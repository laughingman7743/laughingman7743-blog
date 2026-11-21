+++
title = "Releasing PyAthena 4.3.0 with parallel S3 transfers"
description = "Follow a Polars query through PyAthena 4.3.0, tune the S3 connection pool, and compare throughput and memory in a 1 GiB filesystem download benchmark."
date = 2026-11-21T21:25:21+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "S3"]
+++

I released **PyAthena 4.3.0** on November 17 (Japan time).
If you use a pandas or Polars cursor, the upgrade can change how many requests PyAthena sends to S3 while reading a query result.
The connection's S3 pool now supplies the default worker count for reads through PyAthena's filesystem.
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

`execute()` submits the query and polls Athena until it finishes.
Athena writes the result to one CSV object.
For this unchunked CSV, PyAthena opens the object with the connection's S3 client and hands the file to Polars.
When Polars reads the whole file, PyAthena splits the read at the default 5 MiB block size.
PyAthena joins the bytes in file order and Polars parses them before `execute()` returns.
`fetchall()` then returns the rows as Python tuples. [Polars S3 read options][polars-s3]

In this run, Athena wrote a 16,817,083-byte CSV.
While Polars 1.44.2 read it, I counted each ranged botocore `GetObject` call from entry until it returned the response headers.
Four calls were in flight at once, starting at offsets of 0, 5, 10, and 15 MiB and together covering the object.

The default DB-API `Cursor` pages through Athena's `GetQueryResults` API.
With Athena-managed result storage, the Polars cursor also gets the rows from that API.
Both paths fetch rows through the Athena client, so S3 pool tuning has no effect on those reads. [S3 client settings][s3-client] [Managed query result storage][managed-results]

## Tuning the S3 connection after upgrading

PyAthena 4.2.1 could already read a result file in parallel.
In that release, `PandasCursor` and `PolarsCursor` defaulted to five S3 workers per CPU reported by Python.
On an eight-CPU host that allowed up to 40 parallel requests, even when botocore's HTTP pool could retain only 10 connections for reuse.
In 4.3.0, `s3_max_workers=None` makes the result reader take its worker limit from the S3 client's `max_pool_connections`, normally 10.
This also applies to the thread-based async and native asyncio variants when they read through PyAthena's filesystem. [Release notes][release]

Start with the defaults and time the same representative query from `execute()` through result consumption.
Compare that elapsed time with `cursor.total_execution_time_in_millis`, which reports Athena's server-side time.
If Athena itself took longer, inspect its queue and engine times first.
If Athena's time is steady, the difference also includes polling, network transfer, and local parsing.
Look for time spent reading large S3 results before increasing concurrency. [Athena query statistics][query-stats]

First check which reader handles the result.
For one whole-file Polars CSV read with the defaults, ten 5 MiB ranges cover 50 MiB.
Only a larger CSV gives an eleventh worker a range to read.
The default pandas CSV cache fetches individual 32 MiB blocks and prefetches the next block while pandas parses the current one.
Increasing the pool alone preserves that one-block read-ahead pattern.
An unchunked Polars Parquet UNLOAD uses the native Polars S3 reader by default, with its own connection settings. [pandas S3 read options][pandas-s3] [Polars S3 read options][polars-s3]

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

The pre-release filesystem benchmark at commit `1d9e5039` timed a synchronous `get` of one 1 GiB object filled with zeros, from S3 to a memory-backed local file.
It measured the transfer alone, outside the DB-API query and CSV parser above.
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
[s3-client]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#s3-client
[managed-results]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#managed-query-result-storage
[query-stats]: https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html
[connections]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#assume-role-provider-with-mfa
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#to-sql
