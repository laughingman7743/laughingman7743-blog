+++
title = "Reading Athena query results with PyAthena 4.3.0"
description = "See how PyAthena 4.3.0 changes parallel S3 reads for DB-API cursors, when to tune the connection pool, and how long-lived connections refresh credentials."
date = 2026-11-21T21:25:21+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "S3"]
+++

I released **PyAthena 4.3.0** on November 16.
If you use the default cursor, `fetchall()` still gets rows through Athena's `GetQueryResults` API, and you can keep your S3 settings.
`PolarsCursor` could already read a large CSV result in parallel in 4.2.1.
In 4.3.0 the size of the connection's S3 pool sets the default limit for those reads. [Release notes][release]

## Reading a large result with Polars

This query generates its own rows and produces a CSV large enough for a parallel read.
To run it, install `PyAthena[Polars]`, set `AWS_DEFAULT_REGION` and `AWS_ATHENA_S3_STAGING_DIR`, and use a workgroup that writes results to S3.

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

Athena writes the result to one CSV file in S3.
During `execute()`, PyAthena reads the file in parallel ranges of up to 5 MiB by default, and Polars parses it into memory.
`fetchall()` then returns the rows as Python tuples. [Polars S3 read options][polars-s3]

In 4.2.1 the reader allowed five workers per logical CPU.
When Python reported three or more logical CPUs, that limit exceeded the ten connections the AWS SDK's default pool could keep per S3 host.
If enough reads ran at once, a read could succeed and the SDK would close its connection afterward because the pool was full.

In 4.3.0 a single reader takes its default worker limit from the S3 client's `max_pool_connections`, normally 10.
When this query has the pool to itself, each finished read has room to return its connection so another part can reuse it without a new TLS handshake.
If 4.2.1 could read more than ten parts at once on your machine, a large result can use fewer simultaneous reads after upgrading. [S3 client settings][s3-client]

## When to change the pool size

Start with the default pool.
If a large result takes longer after upgrading, compare the time from `execute()` through `fetchall()` with `cursor.total_execution_time_in_millis`, the time Athena reports for the query.
If Athena's time is stable, check how much of the remaining time goes to polling, downloading, and parsing.
When the S3 transfer is the bottleneck, try a larger pool. [Athena query statistics][query-stats]

To try a pool of 20 connections, import `Config` from `botocore.config` and add `s3_config=Config(max_pool_connections=20)` to the `connect()` call above.
Leave `s3_max_workers` unset so the reader picks up the new pool size.
Compare elapsed time and peak memory with your own results before keeping that setting.
If several pandas or Polars readers share one connection, set a smaller `s3_max_workers` on each cursor when their combined reads fill the pool.
The same settings apply to their thread-based async and asyncio cursors. [S3 client settings][s3-client]

## Which other cursors use this setting?

`PandasCursor` and `S3FSCursor` with PyAthena's filesystem also inherit the new worker limit.
Their default CSV readers fetch 32 MiB blocks and prefetch one block ahead.
Increasing the pool size alone leaves that one-block read-ahead pattern in place. [pandas S3 read options][pandas-s3] [S3FSCursor read options][s3fs-s3]

For the unchunked CSV above, 4.3.0 uses the connection's S3 client even if another filesystem is registered for `s3` in fsspec.
Passing `storage_options` explicitly to `execute()` uses the registered filesystem for that read.
Polars also uses the connection's pool for chunked Parquet `UNLOAD` reads through PyArrow when `execute()` receives no Polars read options.
Chunked Polars CSV reads use native Polars S3 access by default, and `ArrowCursor` reads S3 data with PyArrow's client.
With Athena-managed results, these cursors get the rows for this query through Athena. [Polars S3 read options][polars-s3] [Managed query results][managed-results]

## Long-lived connections

A long-running job that connects with `role_arn` or `serial_number` could lose access in 4.2.1 when its temporary credentials expired.
In 4.3.0 PyAthena requests fresh credentials before they expire, so the job can keep using the same connection.
PyAthena uses any supplied profile, keys, or `botocore_session` to sign the STS requests and creates a new session for the clients that use the resulting credentials.
With MFA enabled through `serial_number`, the thread making the AWS request can prompt for another code.
If you pass a boto3 `session` to `connect()`, that session manages authentication. [Connection guide][connections]

The credentials already handed to pandas upload workers can expire during a long write. [Credential lifetime for pandas uploads](#writing-tables-with-pandas)

## Writing tables with pandas

`pyathena.pandas.util.to_sql()` now uses a plain cursor for its SQL, so a connection's DataFrame cursor options no longer affect its table lookup.
When `role_arn`, `serial_number` or a caller-supplied boto3 `session` supplies the credentials, `to_sql()` takes one snapshot before starting the upload workers.
All workers in that call use the same snapshot, so later uploads can fail if the credentials expire.
If you pass explicit access keys or a `botocore_session` alongside the boto3 `session` in `connect()`, the workers obtain their credentials from those arguments. [pandas guide][pandas-guide]

With `if_exists="replace"`, `to_sql()` stops if S3 reports that it could not delete some of the old files.
4.2.1 could write the replacement beside them, leaving Athena to read rows from both datasets.
4.3.0 raises `OperationalError` with the affected objects before uploading replacement data.
The table has already been dropped at that point.
Clear the remaining old objects from the table's dedicated S3 location before retrying, because a retry skips cleanup when the table is absent. [pandas guide][pandas-guide]

## S3 filesystem internals

When copying an S3 object to a local file, 4.2.1 copied one block at a time and its cache could fetch ahead.
In 4.3.0 the filesystem's `get()` and `get_file()` download a group of ranges from the object in parallel, then write them to disk in file order.
The DB-API cursors above read their results without making this local copy. [Filesystem guide][filesystem]

[release]: https://github.com/pyathena-dev/PyAthena/releases/tag/v4.3.0
[filesystem]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/filesystem.md
[polars-s3]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/polars.md#s3-read-options
[pandas-s3]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#s3-read-options
[s3fs-s3]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/s3fs.md#s3-read-options
[s3-client]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#s3-client
[managed-results]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#managed-query-result-storage
[query-stats]: https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html
[connections]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#assume-role-provider-with-mfa
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#to-sql
