+++
title = "Releasing PyAthena 4.3.0 with parallel S3 transfers"
description = "Follow a DB-API query from Athena to parallel S3 reads, see the measured throughput and memory cost, and tune the connection when upgrading from PyAthena 4.2.1."
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
In a run with Polars 1.44.2, four S3 `GetObject` requests were in flight at once, each reading a different range of the 16,817,083-byte result.
PyAthena joins the bytes in file order and Polars parses them before `execute()` returns.
`fetchall()` then returns the rows as Python tuples. [Polars S3 read options][polars-s3]

The default DB-API `Cursor` pages through Athena's `GetQueryResults` API.
With Athena-managed result storage, the Polars cursor also gets the rows from that API.
An S3 connection-pool setting affects neither of those reads. [Connection guide][s3-client]

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

For a transfer-bound workload, try passing `s3_config=Config(max_pool_connections=20)` to `connect()`, with `Config` imported from `botocore.config`.
Leave the cursor's `s3_max_workers` unset so the reader and the reusable HTTP pool grow together.
Repeat the measurement with the same result size and usual number of simultaneous queries, and compare elapsed time and peak process memory.
Keep the smaller pool if a larger one buys little throughput.
The four ranges in the example already fit in the default pool. [S3 client settings][s3-client]

The reader also determines how much work the pool can do.
For example, the default pandas CSV path reads 32 MiB blocks and prefetches the next block while pandas parses the current one.
Polars uses its native S3 reader by default for an unchunked Parquet UNLOAD. [pandas S3 read options][pandas-s3] [Polars S3 read options][polars-s3]

If urllib3 logs “Connection pool is full,” check explicit worker overrides and how many cursors share the connection.
On the Polars connection above, `conn.cursor(s3_max_workers=4)` limits one reader to four workers while other cursors can use the connection's larger default.
If the application needs the higher concurrency, size the connection's S3 pool for the concurrent requests you expect to reuse.
The HTTP pool keeps connections per host and can discard excess connections after use. [S3 client settings][s3-client]

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
The filesystem writes each group of ranges before reading the next one, so a full group carries 64 MiB of payload at eight workers and 320 MiB at 40.
Peak RSS includes the benchmark process and its imports.
The memory-backed destination sits outside that figure.

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
[query-stats]: https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html
[connections]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#assume-role-provider-with-mfa
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#to-sql
