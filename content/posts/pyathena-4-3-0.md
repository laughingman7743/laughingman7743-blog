+++
title = "Releasing PyAthena 4.3.0 with parallel S3 transfers"
description = "Follow a Polars query through PyAthena 4.3.0, tune the S3 connection pool, and compare throughput and memory in a 1 GiB filesystem download benchmark."
date = 2026-11-21T21:25:21+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "S3"]
+++

I released **PyAthena 4.3.0** on November 17 (Japan time).
When a cursor reads query results through PyAthena's S3 filesystem, the connection's S3 pool now sets its default worker limit.
How many requests a query actually uses depends on the cursor and the size of the result.
The filesystem can also download a large S3 object to disk with several range requests in flight.

## Reading a query result in parallel

This DB-API example generates 4,096 rows in Athena without needing a table.
Each row contains a 4 KiB string so the result spans several S3 reads even with the default settings.
Install `PyAthena[Polars]==4.3.0`, set `AWS_DEFAULT_REGION` and `AWS_ATHENA_S3_STAGING_DIR`, and use a workgroup that writes query results to an S3 location you can read.

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

with connect(cursor_class=PolarsCursor) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT n, rpad('x', 4096, 'x') AS payload
            FROM UNNEST(sequence(1, 4096)) AS t(n)
        """)
        print(len(cursor.fetchall()))  # 4096
```

**This example used four parallel S3 reads in both PyAthena 4.2.1 and 4.3.0.**
Athena wrote the 4,096 rows to one CSV file a little over 16 MiB in size.
During `execute()`, `PolarsCursor` requested three 5 MiB pieces of that file and a final piece of just over 1 MiB, all at once.
By the time `execute()` returned, Polars had parsed the CSV and `fetchall()` could return the rows from memory. [Polars S3 read options][polars-s3]

I read the same result with four cursors using their default settings.
Each returned all 4,096 rows.
The table shows the peak number of simultaneous S3 download requests (`GetObject`) for the CSV on each PyAthena release.

| Cursor | 4.2.1 | 4.3.0 |
| --- | ---: | ---: |
| `PolarsCursor` | 4 | 4 |
| `PandasCursor` | 1 | 1 |
| `S3FSCursor` | 1 | 1 |
| `Cursor` | 0 | 0 |

`PandasCursor` and the built-in `S3FSCursor` normally read CSV in 32 MiB blocks and prefetch the next block while converting rows.
This entire file fit in their first block, so each issued one S3 request. [pandas S3 read options][pandas-s3] [S3FSCursor read options][s3fs-s3]

The default `Cursor` recorded zero S3 downloads because it gets its rows through Athena's `GetQueryResults` API.
`DictCursor` uses the same API.
With Athena-managed result storage, the Polars cursor also gets the rows from Athena.
The S3 pool setting has no effect on these reads. [S3 client settings][s3-client] [Managed query result storage][managed-results]

## Tuning the S3 connection after upgrading

The upgrade changes the ceiling for parallel reads.
In 4.2.1, `PandasCursor`, `PolarsCursor`, and the built-in filesystem used by `S3FSCursor` defaulted to five workers per CPU reported by Python.
In 4.3.0, reads through PyAthena's filesystem take their default worker count from the S3 client's `max_pool_connections`, normally 10.
For example, a whole-file read of a 100 MiB Polars CSV takes twenty 5 MiB ranges with the defaults.
On an eight-CPU host, the old ceiling of 40 allowed all twenty at once.
With the new default, at most ten can run at once.
The four requests in the example above fit under both limits. [Release notes][release]

`PandasCursor` and `PolarsCursor` expose `s3_max_workers` to override that limit, including on their async variants when they read through PyAthena's filesystem.
For `S3FSCursor` with the built-in filesystem, use the connection's `s3_config` to change the pool size.
This cursor has no `s3_max_workers` option.
Passing a positive `block_size` to its `execute()` method changes the CSV read size and switches the default background cache to the bytes cache. [S3 client settings][s3-client] [S3FSCursor read options][s3fs-s3]

Start with the defaults and time the same representative query from `execute()` through result consumption.
Compare that elapsed time with `cursor.total_execution_time_in_millis`, which reports Athena's server-side time.
If Athena itself took longer, inspect its queue and engine times first.
If Athena's time is steady, the difference also includes polling, network transfer, and local parsing.
Look for time spent reading large S3 results before increasing concurrency. [Athena query statistics][query-stats]

For one whole-file Polars CSV read with the defaults, only a file larger than 50 MiB gives an eleventh worker a range to read.
For pandas and S3FS CSV reads, increasing the pool alone preserves the default one-block read-ahead pattern.
`ArrowCursor` reads S3 result data through PyArrow's own client, and an unchunked Polars Parquet UNLOAD uses the native Polars S3 reader by default.
Those data reads use their respective libraries' settings. [S3 client settings][s3-client] [Polars S3 read options][polars-s3]

For a single transfer-bound reader with more ranges ready than workers, try passing `s3_config=Config(max_pool_connections=20)` to `connect()`, with `Config` imported from `botocore.config`.
Leave `s3_max_workers` unset for this trial so the reader and the reusable HTTP pool grow together.
Repeat the measurement with the same result size and compare elapsed time and peak process memory.
Keep the smaller pool if a larger one buys little throughput. [S3 client settings][s3-client]

If urllib3 logs “Connection pool is full,” check whether an explicit `s3_max_workers` exceeds the S3 client's `max_pool_connections`.
Also count all result reads that overlap, including those from queries running through the same `AsyncPolarsCursor`.
Each result reader using PyAthena's filesystem gets its own worker limit, while the connection shares one S3 HTTP pool per host.
Increasing that pool also raises the limit for every reader that inherits the default.
For example, with a pool of 20 and four Polars cursors reading large CSVs at once, create each cursor with `conn.cursor(s3_max_workers=4)`.
Those readers can issue up to 16 range requests between them, leaving room to reuse connections for other S3 calls to the same host.
Check elapsed time and peak memory with all four readers active.
urllib3 can discard connections opened beyond the pool's capacity after use. [S3 client settings][s3-client]

## What the parallel download benchmark measured

The filesystem also has a separate operation for downloading an S3 object to a local file.
In 4.2.1 its synchronous `get_file()` loop copied one block to the destination at a time.
In 4.3.0 it can request a group of ranges in parallel and write them in order.
The pre-release benchmark at commit `1d9e5039` timed this download through synchronous `get`, copying one 1 GiB object filled with zeros to a memory-backed file.
The timing excludes SQL execution and CSV parsing.
The download ran on one `c8gn.2xlarge` Spot instance in the bucket's region with Python 3.12, 8 MiB ranges, and a connection pool of 40.
Each rate is the median of five trials without warmups. [Benchmark results][measurements]

| Range workers | Download rate | Peak process RSS |
| ---: | ---: | ---: |
| 8 | 469 MiB/s | 336 MiB |
| 40 | 613 MiB/s | 938 MiB |

Going from 8 to 40 workers raised throughput by about 31% on this host and added 602 MiB to peak resident memory (RSS).
The filesystem writes each group of ranges before reading the next one.
A full group carries 64 MiB of payload at eight workers and 320 MiB at 40.
A worker can also briefly retain a range it has just read.
The 256 MiB increase in group payload is smaller than the measured 602 MiB rise in peak RSS.
The benchmark sampled the whole process, including its imports.
The memory-backed destination sits outside that figure. [Filesystem guide][filesystem]

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
