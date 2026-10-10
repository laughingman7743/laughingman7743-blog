+++
title = "Releasing PyAthena 3.39.0 with credential and result fixes"
description = "PyAthena 3.39.0 fixes Arrow credentials, array and JSON conversion, pandas and Polars TIME results, and Iceberg detection, and requires tenacity 8.0.1 or later."
date = 2026-10-11
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy"]
+++

I have released **PyAthena 3.39.0**, a minor release in the 3.x maintenance line.
It backports bug fixes from master for Arrow credentials, result conversion, and SQLAlchemy DDL.
The [release notes](https://github.com/pyathena-dev/PyAthena/releases/tag/v3.39.0) contain the complete list of changes.

## Arrow credentials

`ArrowCursor`, `AsyncArrowCursor`, and `AioArrowCursor` now read result files with the connection session's credentials.
This fixes profiles backed by `credential_process`, IAM Identity Center, or an assumed role, which could previously fall back to PyArrow's default credential chain.
An explicitly supplied `session=` also takes precedence over separate credential arguments.

With the `role_arn` connection argument, PyArrow reuses the role credentials already obtained by the connection, including MFA, instead of assuming the role again.
Those credentials expire after `duration_seconds`, as they do for the connection's boto3 clients.
Credential-resolution errors now propagate instead of silently switching to PyArrow's default credential chain.

## Result conversion fixes

Native-format arrays now preserve empty strings and whitespace-only items, and stop splitting a value such as `a,b` into separate elements.
For example, `ARRAY['x', '', 'y']` returns `['x', '', 'y']`, and `ARRAY['a,b', 'c']` returns `['a,b', 'c']`.
Leading and trailing spaces within items are preserved too.

With `array(json)`, `map(..., json)`, or `row(... json)` type hints, JSON string elements such as `"123"` stay strings.
The fixes also prevent `JSONDecodeError` for JSON fields in rows and JSON arrays whose first element is not a string.

For `PandasCursor` with `chunksize`, `PandasDataFrameIterator.get_chunk()` now applies the same TIME conversion as iteration.
NULL TIME values read from result files become `None` instead of `NaT`, including in `as_pandas()`.
With managed query result storage, pandas and Polars results containing TIME columns no longer fail during conversion.
Closing a Polars result set also stops an existing `iter_chunks()` iterator.

## SQLAlchemy table detection

DDL now treats a table as Iceberg only when its `table_type` property is `ICEBERG`, compared case-insensitively.
Unrelated mentions of `table_type` and `iceberg` in table properties no longer cause a Hive table to compile with the Iceberg layout.
On an S3 Tables catalog, such a table now raises `CompileError` instead of compiling as Iceberg.

## Upgrading

Install this release with:

```sh
python -m pip install --upgrade 'PyAthena==3.39.0'
```

Keep any extras used by the application, for example `'PyAthena[SQLAlchemy]==3.39.0'`.
The minimum `tenacity` version is now **8.0.1**, up from 4.1.0.
In `retry_api_call()`, versions before 6.1.0 fail on Python 3.11+, and 8.0.0 raises `TypeError` on the first retry.
The 3.x line continues to support Python 3.10 and retains its `sqlalchemy>=1.0.0` dependency requirement.

The [full changelog](https://github.com/pyathena-dev/PyAthena/compare/v3.38.0...v3.39.0) lists the backported pull requests.
