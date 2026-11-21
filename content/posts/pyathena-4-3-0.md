+++
title = "Releasing PyAthena 4.3.0 with parallel S3 transfers"
description = "PyAthena 4.3.0 parallelizes S3 downloads and changes the defaults for pandas and Polars result reads. Here is how to migrate from 4.2.1 and recover a failed table replacement."
date = 2026-11-21T21:25:21+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "S3"]
+++

I released **PyAthena 4.3.0** on November 17 (Japan time).
A synchronous `fs.get()` can now download several S3 objects concurrently and read a large object in parallel ranges.
Even applications that never create a filesystem explicitly can see a change after upgrading from 4.2.1, because the pandas and Polars cursors use it to read Athena result files.

## Downloading a prefix

Suppose an Athena export occupies several Parquet files under one S3 prefix.
With `S3FileSystem.get()`, `batch_size` controls how many files are downloaded at once.
For each uncompressed download, the filesystem reads several ranges of the object concurrently and writes them to disk in order.

```python
from botocore.config import Config
from pyathena import connect
from pyathena.filesystem import s3_filesystem

with connect(
    region_name="us-west-2",
    s3_staging_dir="s3://example-bucket/athena-results/",
    s3_config=Config(max_pool_connections=16),
) as conn:
    with s3_filesystem(conn) as fs:
        fs.get(
            "s3://example-bucket/exports/", "exports/", recursive=True, batch_size=4
        )
```

Replace the bucket and prefix with an export the connection can list and read.
The example downloads up to four files together and gets 16 range workers from the connection's S3 client configuration.
Downloading one large file on its own can use all 16 workers.
If several files are large, they share those workers.

I measured the cost of raising the worker count in a pre-release benchmark of a single 1 GiB download.
The synchronous `get` ran on one `c8gn.2xlarge` host with Python 3.12, 8 MiB ranges, and a connection pool of 40.
The rates are medians of five trials at commit `1d9e5039`. [Benchmark results][measurements]

| Range workers | Download rate | Peak process RSS |
| ---: | ---: | ---: |
| 8 | 469 MiB/s | 336 MiB |
| 40 | 613 MiB/s | 938 MiB |

The larger pool of workers keeps more range data in memory while the download waits to write the next group.
The RSS figures include the benchmark process and its imports.
The destination was a memory-backed filesystem whose storage is outside that RSS.

## Sharing a connection's S3 settings

Code that constructs `S3FileSystem(connection=conn)` or `AioS3FileSystem(connection=conn)` now gets a `DeprecationWarning`.
Use `s3_filesystem(conn)` as above, or `s3_filesystem(conn, AioS3FileSystem)` for the asyncio filesystem.
The helper borrows the connection's S3 client and uses its retry policy.
For an API that accepts `storage_options`, replace `{"connection": conn}` with `{"client": conn.s3_client, "retry_config": conn.retry_config}` while the connection is open.
Set the region, endpoint, or credentials when constructing the client.
Passing those options explicitly alongside `client=` or `connection=` raises `TypeError`. [Filesystem guide][filesystem]

For `PandasCursor` and `PolarsCursor`, including their thread-based async and native asyncio variants, the old default for `s3_max_workers` was five times the CPU count.
In 4.3.0 the default is `None`, so result reads through PyAthena's filesystem take their worker count from the client's `max_pool_connections`, normally 10.
The `s3_config` in the download example gives these cursors a default of 16 on the same connection.
If a cursor sets `s3_max_workers` higher than the connection pool size, urllib3 can discard the extra connections after use. [Release notes][release]

## Listing and batch failures

In 4.2.1, `fs.ls(path)` could return an empty list both for an empty bucket and for a key path that had neither an object nor a directory.
In 4.3.0 a missing key path raises `FileNotFoundError`.
An existing empty bucket still returns `[]`.
Applications that treat a missing prefix as an expected input can catch `FileNotFoundError` around the listing.

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

A connection created with `role_arn` or `serial_number` now refreshes its temporary STS credentials as they approach expiry.
With MFA, the thread making the AWS call can prompt for a new code during refresh.
For these connections, the upload workers used by `pyathena.pandas.util.to_sql()` get the current credentials when the uploads begin and keep that snapshot for the duration of the upload. [Connection guide][connections]

`to_sql()` now runs its SQL through an internal plain cursor and turns off query result reuse for the table-existence check.
For a connection configured with a DataFrame cursor, this means the lookup uses the connection's workgroup and bypasses cursor options such as `unload`.

With `if_exists="replace"`, `to_sql()` drops the existing table and deletes its old S3 objects before uploading the new data.
S3 can report a successful batch request while returning errors for individual keys.
4.2.1 could then write the replacement files alongside the old ones, and Athena could read rows from both.
4.3.0 raises `OperationalError` naming the affected objects before writing any replacement data.

At that point the table has already been dropped.
Resolve the reported deletion errors, remove the remaining old objects under the table's S3 location, then run the replacement again.
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
[connections]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/usage.md#assume-role-provider-with-mfa
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.3.0/docs/pandas.md#to-sql
