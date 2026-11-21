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
With `PolarsCursor`, 4.2.1 already read a large CSV result in parallel, but one query could open more S3 connections than the AWS SDK could keep for reuse.
In 4.3.0 the size of the connection's S3 pool sets the default limit on parallel reads for one result. [Release notes][release]

## Reading a large result with Polars

This query generates 4,096 rows with a 24 KiB string in each, enough to see several S3 reads in flight.
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

Athena wrote one CSV file just over 96 MiB in size.
In both releases, `execute()` fetched the CSV in 20 parts of up to 5 MiB each, then Polars parsed it into memory.
`fetchall()` returned the 4,096 rows as Python tuples. [Polars S3 read options][polars-s3]

In 4.2.1 the reader allowed five workers per CPU, so all 20 parts could be in flight at once on my machine with 24 logical CPUs.
The AWS SDK's default pool can keep ten connections per S3 host for reuse.
It can open more, but closes the excess connections when they return to a full pool.
For each setting, I read the same stored result twice on one DB-API connection.
Both reads had the same peak concurrency and the same number of connections discarded because the S3 pool was full.

| Release and pool size | Peak parallel reads | Discarded connections |
| --- | ---: | ---: |
| 4.2.1, default (10) | 20 | 10 |
| 4.3.0, default (10) | 10 | 0 |
| 4.3.0, pool of 20 | 20 | 0 |

With 4.3.0's defaults, the reader worked through the same 20 parts ten at a time.
The pool had room for every returned connection, though the second read still opened nine new HTTPS connections with the default pool and fourteen with the larger pool. [S3 client settings][s3-client]

For a separate timing, I started a fresh process and connection for each run and timed `execute()` through `fetchall()` against the same stored result.
Across three runs per setting, the elapsed times overlapped.[^timings]

## When to change the pool size

Start with the default pool.
If a large result takes longer after upgrading, compare the time from `execute()` through `fetchall()` with `cursor.total_execution_time_in_millis`, the time Athena reports for the query.
If Athena's time is stable, check how much of the remaining time goes to polling, downloading, and parsing.
When the S3 transfer is the bottleneck, try a larger pool. [Athena query statistics][query-stats]

For the third row in the table, I imported `Config` from `botocore.config` and added `s3_config=Config(max_pool_connections=20)` to the same `connect()` call.
I left `s3_max_workers` unset so the reader picked up the new pool size.
Compare elapsed time and peak memory with your own results before keeping that setting.
If several pandas or Polars readers share one connection, set a smaller `s3_max_workers` on each cursor when their combined reads fill the pool.
The same settings apply to their thread-based async and asyncio cursors. [S3 client settings][s3-client]

## Which other cursors use this setting?

`PandasCursor` and `S3FSCursor` with PyAthena's filesystem also inherit the new worker limit.
Their default CSV readers fetch 32 MiB blocks and prefetch one block ahead.
On this same file their peak was one S3 read at a time in both releases, so I would keep the default pool for that workload. [pandas S3 read options][pandas-s3] [S3FSCursor read options][s3fs-s3]

For the unchunked CSV above, 4.3.0 uses the connection's S3 client even if another filesystem is registered for `s3` in fsspec.
Passing `storage_options` explicitly to `execute()` uses the registered filesystem for that read.
Polars also uses the connection's pool for chunked Parquet `UNLOAD` reads through PyArrow when `execute()` receives no Polars read options.
Chunked Polars CSV reads use native Polars S3 access by default, and `ArrowCursor` reads S3 data with PyArrow's client.
With Athena-managed results, these cursors get the rows for this query through Athena. [Polars S3 read options][polars-s3] [Managed query results][managed-results]

## Long-lived connections and pandas writes

A long-running job that connects with `role_arn` or `serial_number` could lose access in 4.2.1 when its temporary credentials expired.
In 4.3.0 PyAthena requests fresh credentials before they expire, so the job can keep using the same connection.
PyAthena uses any supplied profile, keys, or `botocore_session` to sign the STS requests and creates a new session for the clients that use the resulting credentials.
With MFA enabled through `serial_number`, the thread making the AWS request can prompt for another code.
If you pass a boto3 `session` to `connect()`, that session manages authentication.
The upload workers in pandas `to_sql()` take a snapshot of PyAthena-managed credentials when uploads start, so an upload can still outlast those credentials. [Connection guide][connections] [pandas guide][pandas-guide]

For pandas writes, `pyathena.pandas.util.to_sql()` now uses a plain cursor for its SQL, so a connection's DataFrame cursor options no longer affect its table lookup.
With `if_exists="replace"`, it stops if S3 reports that it could not delete some of the old files.
4.2.1 could write the replacement beside them, leaving Athena to read rows from both datasets.
4.3.0 raises `OperationalError` with the affected objects before uploading replacement data.
The table has already been dropped at that point.
Clear the remaining old objects from the table's dedicated S3 location before retrying, because a retry skips cleanup when the table is absent. [pandas guide][pandas-guide]

## S3 filesystem internals

The S3 filesystem's `get()` and `get_file()` now download parts of an object in parallel when copying it to disk.
The DB-API cursors above read their results without making this local copy.
With eight workers, eight pool slots and 5 MiB blocks in both releases, a trace of the same 96 MiB file showed two overlapping reads in 4.2.1 and eight in 4.3.0.
Across five timed copies per release from S3 in `us-west-2` to my WSL 2 machine, median copy time fell from 69.6 s to 28.2 s. [Filesystem guide][filesystem]

[^timings]: The three trials ranged from 12.4 to 65.3 s on 4.2.1 with the default pool, 12.2 to 17.8 s on 4.3.0 with the default pool, and 12.6 to 14.1 s on 4.3.0 with a pool of 20.

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
