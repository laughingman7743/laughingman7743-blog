+++
title = "Releasing PyAthena 3.38.0 with cursor and S3 fixes"
description = "PyAthena 3.38.0 fixes S3 appends, parameterized query caching, pandas and Arrow results, SQLAlchemy reflection, and Spark session cleanup."
date = 2026-10-07
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy"]
+++

I have released **PyAthena 3.38.0**, a minor release in the 3.x maintenance line.
It backports bug fixes from master for S3 writes, query caching, result conversion, SQLAlchemy, and Spark sessions.
The [release notes](https://github.com/pyathena-dev/PyAthena/releases/tag/v3.38.0) contain the complete list of changes.

## S3 appends and multipart uploads

Appending through `S3FileSystem` now preserves the existing object's data when it fits within a larger write block.
The fixes also prevent an empty append from truncating an object and prevent existing bytes from being duplicated.
These cases matter when extending an existing object or opening it in append mode without writing anything.

A single large `write()` no longer produces an undersized multipart part that makes the upload fail with `EntityTooSmall`.
Multipart copies, including appends to objects over 5 GiB, now keep parts within S3's size limits.
Listing and object caches are also invalidated after changes that previously left stale entries.

The multipart fixes can change part boundaries when the last part is short.
As a result, identical content can have a different multipart ETag than before.
Applications comparing those ETags should account for this when upgrading.

## Parameterized query caching

For `qmark` queries with parameters, PyAthena now skips the client-side result cache controlled by `cache_size` and `cache_expiration_time`.
Athena does not return the parameters of earlier executions, so matching the SQL text alone could select a result produced with different parameter values.

Even a repeated query with the same parameters now submits a query instead of reusing an execution through this cache.
Applications relying on those cache hits should expect additional query submissions.
Athena's server-side result reuse remains a separate setting.
The [cache documentation for this release](https://github.com/pyathena-dev/PyAthena/blob/v3.38.0/docs/usage.md#cache-configuration) describes the client-side behavior.

## Pandas and Arrow results

On a fresh `PandasCursor` result without an explicit `chunksize`, `as_pandas()` now returns the whole result even when `auto_optimize_chunksize` selected a chunk size.
Previously, it returned only the first chunk in that case.
Collecting all chunks holds the complete DataFrame in memory; use `iter_chunks()` to process a large result incrementally.
When `PandasDataFrameIterator.as_pandas()` joins chunks, it also preserves their index and categorical dtypes, including an index selected with `index_col`.

With `engine="pyarrow"`, `PandasCursor` now uses the C engine for tab-separated DDL result files such as those from `SHOW TABLES` and `DESCRIBE`.
This preserves numeric-looking strings, including leading zeros, exponent notation, and padding.

`ArrowCursor` now keeps NULL rows in single-column CSV results and reads multiline quoted values that cross a read-block boundary.
With managed query result storage and no output location, its fetch methods also stop converting already-converted values a second time, fixing errors for TIME, VARBINARY, and JSON values.
On that path, fetch methods return the values held by `as_arrow()`; a custom Arrow converter mapping is not applied.

## SQLAlchemy and session handling

SQLAlchemy reflection now reports top-level MAP columns as `AthenaMap` with key and value types, replacing `String`.
STRUCT/ROW columns continue to use `AthenaStruct` and now include their field types.
Unrecognized field, key, or value types are reported as `NullType` with a warning.
Selected MAP and STRUCT values still use the cursor's existing conversion behavior.

A bare `awsathena://` URL now selects the REST dialect, matching `awsathena+rest://` and fixing the previous `engine.driver` error.
PyAthena's `to_sql` helper also finds existing tables regardless of the case of the supplied name.

With the default converter, numeric UTC offsets such as `+05:30` in `TIMESTAMP WITH TIME ZONE` results now produce timezone-aware datetimes with a fixed offset.
For Spark, readiness polling raises `OperationalError` when a session is terminated, degraded, or failed.
Failed startup also attempts to terminate a newly created session, and `AsyncSparkCursor.close()` shuts down its executor even if session termination fails.

Thanks to [aminghadersohi](https://github.com/aminghadersohi) for the timezone and reflection reports and for contributing the [bare-URL dialect fix](https://github.com/pyathena-dev/PyAthena/pull/839).

## Upgrading

Install this release with:

```sh
python -m pip install --upgrade 'PyAthena==3.38.0'
```

Keep any extras used by the application, for example `'PyAthena[SQLAlchemy]==3.38.0'`.
The 3.x line continues to support Python 3.10, and dependency requirements, including `sqlalchemy>=1.0.0`, are unchanged.
The [full changelog](https://github.com/pyathena-dev/PyAthena/compare/v3.37.0...v3.38.0) includes the remaining cursor fixes and test workflow improvements.
