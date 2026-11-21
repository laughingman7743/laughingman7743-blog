+++
title = "Parallel S3 file downloads in PyAthena 4.3.0"
description = "Compare S3 file download times in PyAthena 4.2.1 and 4.3.0, and see how the S3 connection pool sets the default worker count for large Athena results."
date = 2026-11-21T21:25:21+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "S3"]
+++

I released **PyAthena 4.3.0** on November 17 (Japan time).
When saving an S3 object to a local file, 4.3.0 now schedules a group of byte ranges for its download workers.
On my WSL 2 client, 4.3.0 downloaded a 96 MiB CSV from S3 in `us-west-2` at 2.5 times the rate of 4.2.1 with eight workers configured in both.
The DB-API's `PolarsCursor` could already read several parts of a CSV at once in 4.2.1.
What changed for that cursor is how many workers it can use by default. [Release notes][release]

## Downloading one S3 object to a local file

I compared the published releases by calling `S3FileSystem.get_file()` on the same uncompressed Athena result.
I fixed the filesystem's `max_workers` and the S3 client's `max_pool_connections` at 8, and passed the same 5 MiB `block_size` to each download.
On the WSL 2 host, I used a separate boto3 client to time `HeadObject` against the same object in `us-west-2`.
After one warm-up call, five consecutive calls took a median of 145 ms.
Both versions used the same Python and AWS SDK, with a fresh process and a new local file for every timed trial.
I timed the `get_file()` call after checking the object's size, alternated which release went first across five pairs, and verified that all ten files had the same SHA-256 digest.

| PyAthena | Median time | Transfer rate |
| --- | ---: | ---: |
| 4.2.1 | 69.6 s | 1.38 MiB/s |
| 4.3.0 | 28.2 s | 3.41 MiB/s |

The old implementation copied one block at a time, and its read cache could fetch ahead.
In a separate trace of the same download, I counted two overlapping range reads on 4.2.1 and eight on 4.3.0, including the time spent reading each response body.
The new implementation asks workers to read a group of ranges in parallel, then writes the group in file order. [Filesystem guide][filesystem]

Keeping more ranges in flight can increase peak memory use.
In the separate 1 GiB download benchmark at commit `1d9e5039`, both runs used 8 MiB blocks and a connection pool of 40 on one EC2 instance in the bucket's region.
Raising the worker count from 8 to 40 increased throughput from 469 to 613 MiB/s and peak process memory from 336 to 938 MiB. [Benchmark results][measurements]

## Reading a large result with the DB-API

A `PolarsCursor` reads an ordinary, unchunked CSV into memory during `execute()`.
The query below produced the benchmark file by generating 4,096 rows with a 24 KiB string in each.
Install `PyAthena[Polars]==4.3.0`, set `AWS_DEFAULT_REGION` and `AWS_ATHENA_S3_STAGING_DIR`, and use a workgroup that writes results to an S3 location you can read.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

with connect(cursor_class=PolarsCursor) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT n, rpad('x', 24 * 1024, 'x') AS payload
            FROM UNNEST(sequence(1, 4096)) AS t(n)
        """)
        print(len(cursor.fetchall()))  # 4096
```

Athena wrote one CSV just over 96 MiB in size.
Each release fetched it in twenty ranges of up to 5 MiB during `execute()`, so `fetchall()` could return the rows from memory.
I counted each range read from the call to `S3Core.get_object()` until it finished reading the response body.
With the default connection, the peak was **20 simultaneous range reads on 4.2.1 and 10 on 4.3.0**.
In 4.2.1 the default was five workers per CPU, so the 24 logical CPUs on this machine allowed all twenty reads to start.
In 4.3.0 the default comes from the S3 client's `max_pool_connections`, normally 10.
For a whole-file Polars CSV read with 5 MiB blocks, that lower default can reduce overlap when the result exceeds 50 MiB and Python reports more than two CPUs. [Polars S3 read options][polars-s3] [S3 client settings][s3-client]

## Tuning the connection after upgrading

If a query feels slower after upgrading, time it from `execute()` through result consumption and compare that with `cursor.total_execution_time_in_millis`, Athena's server-side time.
If the server time grew, check Athena's queue and engine timings first.
If server time is steady, look for extra time spent polling, transferring results, or parsing them.
If the S3 transfer is the bottleneck, try a larger pool.
For this file's twenty ranges, I passed `s3_config=Config(max_pool_connections=20)` to `connect()`, with `Config` imported from `botocore.config`.
Leaving `s3_max_workers` unset let the 4.3.0 cursor use that pool size, and the same query reached 20 simultaneous reads.
Compare elapsed time and peak memory on your own results before keeping the larger pool. [Athena query statistics][query-stats] [S3 client settings][s3-client]

The new limit also applies to `PandasCursor` and to `S3FSCursor` with PyAthena's filesystem.
Their default CSV readers prefetch one 32 MiB block ahead, so raising the pool alone preserves that read pattern.
For pandas, set `block_size` or `cache_type` on the cursor or on `execute()` to change it.
For `S3FSCursor`, passing a positive `block_size` to `execute()` selects that block size and the bytes cache.
Use the connection's `s3_config` to change the S3FS worker limit. [pandas S3 read options][pandas-s3] [S3FSCursor read options][s3fs-s3]

`PandasCursor` and `PolarsCursor`, including their async variants, also accept a per-reader `s3_max_workers`.
If four concurrent Polars readers share a connection with a pool of 20, setting `conn.cursor(s3_max_workers=4)` for each lets them issue up to 16 range reads between them.
When urllib3 logs “Connection pool is full,” count overlapping result reads, including queries on the same async cursor, and compare their worker limits with the shared S3 pool. [S3 client settings][s3-client]

Polars uses the connection's S3 pool for the unchunked CSV above.
A chunked Polars Parquet UNLOAD also uses it when PyArrow is installed and `execute()` receives no Polars read options.
Chunked Polars CSV and unchunked Polars UNLOAD use native Polars S3 access by default, and `ArrowCursor` reads S3 results with PyArrow's client.
The default `Cursor` and `DictCursor` get rows through Athena's `GetQueryResults` API, as do the pandas, Polars, S3FS, and Arrow cursors for this SELECT in a workgroup with Athena-managed results. [Polars S3 read options][polars-s3] [Managed query result storage][managed-results]

## Sharing a connection's S3 settings

Code that constructs `S3FileSystem(connection=conn)` or `AioS3FileSystem(connection=conn)` now gets a `DeprecationWarning`.
The `connection=` argument is scheduled for removal in the next major release.
Use `pyathena.filesystem.s3_filesystem(conn)`, or pass `AioS3FileSystem` as its second argument for the asyncio filesystem.
The helper borrows the connection's S3 client and uses its retry policy.

Check wrappers that used to pass client-building options alongside `connection=`.
4.2.1 ignored those options.
With fsspec's default configuration, 4.3.0 raises `TypeError` for explicit values other than `None`, including `read_timeout=30`, `config_kwargs={}`, or even `anon=False`.
The filesystem ignores values inherited from fsspec's configuration, even when a wrapper passes the same value explicitly.
The same rule applies to `client=`.
When reusing the connection's client, set its region on `connect()` and its S3 timeouts through `s3_config=Config(...)`. [Filesystem guide][filesystem]

For a reader that passes `storage_options` to PyAthena's fsspec filesystem, replace `{"connection": conn}` with `{"client": conn.s3_client, "retry_config": conn.retry_config}` while the connection is open.
Polars readers that use native S3 access take their own storage options. [Polars S3 read options][polars-s3]

For standalone `S3FileSystem` and `AioS3FileSystem` instances, the default `max_workers` changed from five times the CPU count to the S3 client's `max_pool_connections`, normally 10.
Files opened by the same filesystem share its request pool for work such as range reads and multipart uploads.

## Listing and batch failures

In 4.2.1, `fs.ls(path)` could return an empty list both for an empty bucket and for a key path that had neither an object nor a directory.
In 4.3.0 a missing key path raises `FileNotFoundError`.
An existing empty bucket still returns `[]`.
Applications that treat a missing prefix as an expected input can catch `FileNotFoundError` around the listing.
`listdir()`, `tree()`, and the asyncio filesystem's `_ls()` also raise for that path.
A synchronous `walk()` with its default `on_error="omit"` now yields no entry for it.
Code that needs an existence check can continue to use `exists()`.

A synchronous `get`, `put`, `copy`, or dictionary-form `pipe` now raises `ValueError` before transferring data if two entries resolve to the same destination.
A copy also rejects a destination that is another pair's source.
The new overlap checks apply to the synchronous API.
When building lists of paths for `AioS3FileSystem`, check for conflicting destinations and sources in the caller.

A transfer failure can still leave completed objects at the destination.
For example, if a batch copy fails midway, inspect the destination before retrying.
A synchronous `mv()` starts deleting source objects only after every copy succeeds.
For batch deletion, change `rm(max_workers=n)` to `rm(batch_size=n)`.
Here the number controls simultaneous S3 `DeleteObjects` requests, each of which can contain up to 1,000 keys. [Filesystem guide][filesystem]

## Long-lived connections and table replacement

When PyAthena obtains temporary STS credentials through `role_arn` or `serial_number`, it now refreshes them as they approach expiry.
With MFA, the thread making the AWS call can prompt for a new code during refresh.
Passing a boto3 `session=` makes that session responsible for authentication, so PyAthena skips its own STS requests and MFA prompts. [Connection guide][connections]

The upload workers in `pyathena.pandas.util.to_sql()` take a snapshot of PyAthena-managed STS credentials when the uploads start.
They also snapshot credentials supplied by a caller's boto3 session when neither explicit keys nor a `botocore_session` overrides them.
An upload that outlasts those temporary credentials can fail even if the connection itself can refresh them. [pandas guide][pandas-guide]

`to_sql()` now runs its SQL through an internal plain cursor and turns off query result reuse for the table-existence check.
For a connection configured with a DataFrame cursor, this means the lookup uses the connection's workgroup and bypasses cursor options such as `unload`.

With `if_exists="replace"`, `to_sql()` drops the existing table and deletes the objects under the supplied `location` before uploading the new data.
Pass a prefix dedicated to that table.
S3 can report a successful batch request while returning errors for individual keys.
4.2.1 could then write the replacement files alongside the old ones, and Athena could read rows from both.
4.3.0 raises `OperationalError` naming the affected objects before writing any replacement data.

At that point the table has already been dropped.
Resolve the reported deletion errors, remove the old objects that remain under that same `location`, then run the replacement again.
A retry while the table is absent skips the cleanup step because `to_sql()` only deletes old objects when it finds an existing table. [pandas guide][pandas-guide]

## Upgrading from 4.2.1

Keep the extras the application uses.
For a pandas application that writes Parquet through `to_sql()`, install the release with the pandas and Arrow extras.

```sh
python -m pip install --upgrade 'PyAthena[Pandas,Arrow]==4.3.0'
```

The [4.3.0 release notes][release] cover the remaining changes, including asyncio cancellation, S3 retry behavior, and access to versioned objects.

[release]: https://github.com/pyathena-dev/PyAthena/releases/tag/v4.3.0
[measurements]: https://github.com/pyathena-dev/PyAthena/blob/476d68a5c1952626fab9b4e4dc065dc076557554/benchmarks/history/2026-11-15-1d9e5039-filesystem.md#downloads
[filesystem]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/filesystem.md
[polars-s3]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/polars.md#s3-read-options
[pandas-s3]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#s3-read-options
[s3fs-s3]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/s3fs.md#s3-read-options
[s3-client]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#s3-client
[managed-results]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#managed-query-result-storage
[query-stats]: https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html
[connections]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#assume-role-provider-with-mfa
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#to-sql
