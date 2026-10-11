+++
title = "Releasing PyAthena 4.0.0"
description = "PyAthena 4.0.0 attributes its copyright to The PyAthena authors, adds a NOTICE file, requires Python 3.11, and makes connection and cursor arguments stricter, with fixes across result conversion, SQLAlchemy, and the S3 filesystem."
date = 2026-10-11
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy"]
+++

I have released **PyAthena 4.0.0**, a major release from `master`, which diverged from the 3.x maintenance line at 3.36.0.
It changes the project's copyright notices, raises the minimum Python and dependency versions, and removes behavior that used to fail silently.
The [release notes](https://github.com/pyathena-dev/PyAthena/releases/tag/v4.0.0) list every change with the pull requests and migration hints.
This post covers the copyright change first, because it affects everyone who redistributes PyAthena, and then the changes most users will notice when upgrading.

## Copyright and license notices

PyAthena is still distributed under the MIT License, and the license terms have not changed.
What changed is who the notices name.
The copyright line in `LICENSE` used to name me alone (`Copyright 2017 laughingman7743`).
It now reads `Copyright 2017 The PyAthena authors`, because the project has received contributions from many people over the years.

The same attribution applies at the file level.
Files that contain only original work by the PyAthena authors carry a short header with `Copyright <year> The PyAthena authors` and `SPDX-License-Identifier: MIT`.
Existing files that I wrote alone received this header, new original files must carry it, and a CI check enforces it.
Files with contributions from other people keep their authorship in the Git history and do not carry the header.

The package metadata follows the same rule.
PyPI now shows "The PyAthena authors" as the author and me as the maintainer, and the distribution uses PEP 639 license metadata with the `MIT` license expression.

The release also adds a [`NOTICE`](https://github.com/pyathena-dev/PyAthena/blob/v4.0.0/NOTICE) file, which is included in both the sdist and the wheel.
It acknowledges [PyHive](https://github.com/dropbox/PyHive), which informed PyAthena's early development, lists the parts that still follow it, and reproduces PyHive's Apache License 2.0 notice.
It also records the other third-party material in the repository and explains which files do not carry the header.
Please read `NOTICE` once, and include it together with `LICENSE` if you redistribute or package PyAthena.

## Python and dependency requirements

PyAthena 4.0.0 requires **Python 3.11 or later**.
The core dependencies now require `boto3` and `botocore` 1.43.31 or later, `fsspec` 2026.9.0 or later, and `tenacity` 8.0.1 or later.
`python-dateutil` is no longer a dependency; time zones come from the standard library's `zoneinfo` with `tzdata` as a dependency.

The extras have new minimum versions too: `sqlalchemy>=2.0.0`, `pandas>=3.0.0`, `pyarrow>=22.0.0`, and `polars>=1.39.0`.
An application that needs Python 3.10, SQLAlchemy 1.x, or pandas 2 can stay on the 3.x maintenance line, whose latest release is 3.39.0.

## Stricter connection and cursor arguments

Several arguments that PyAthena used to accept and then ignore now raise an error.
Connection settings and cursor constructor settings are keyword-only, so a positional setting raises `TypeError`.
An unknown keyword passed to a cursor constructor, directly or through the connection's `cursor_kwargs`, also raises `TypeError` instead of disappearing.
An explicit argument to `connection.cursor()` now overrides the same key in `cursor_kwargs`; before, `cursor_kwargs` silently won.

Some names and values changed meaning.
The S3 read worker setting of the pandas and Polars cursors is now `s3_max_workers`, and `max_workers` of the asynchronous cursors sizes only the cursor's own thread pool.
An `arraysize` passed when a cursor is created now takes effect and is validated, so the default and dict cursors reject values above 1000.
`connect(endpoint_url=...)` no longer applies to S3; setups such as LocalStack that serve Athena and S3 from one endpoint need `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL`.
The usage guide has migration sections for the [keyword arguments](https://pyathena.dev/v4.0.0/usage.html#keyword-arguments-in-pyathena-4-0), the [worker settings](https://pyathena.dev/v4.0.0/usage.html#worker-settings-in-pyathena-4-0), and the [environment fallbacks](https://pyathena.dev/v4.0.0/usage.html#environment-fallbacks-in-pyathena-4-0).

Interrupts behave differently as well.
With the default `kill_on_interrupt=True`, a `KeyboardInterrupt` or task cancellation during `execute()` stops the query and then propagates, instead of surfacing as `OperationalError` or being lost when the query had already succeeded.
Code that caught `OperationalError` after Ctrl-C should catch the interrupt instead, and `asyncio.wait_for()` now raises `TimeoutError` as expected.

## Result conversion

The pandas, Arrow, and Polars cursors now read results from managed query result storage the same way as a CSV result file on S3, with the same types and values.
Their converters, including a custom one, apply on that path too.

Time zone handling also became consistent across cursors.
`time with time zone` values are aware `datetime.time` objects, and `timestamp with time zone` values are aware `datetime` objects in the Arrow and Polars cursors and in every `PandasCursor` column.
Zone names use `zoneinfo.ZoneInfo`, so a pandas dtype such as `datetime64[us, tzfile('/usr/share/zoneinfo/America/New_York')]` becomes `datetime64[us, America/New_York]`; the instants and offsets do not change.

`PandasCursor` received a series of fixes.
Without `chunksize`, repeated `as_pandas()` calls return the whole result and the same object.
With `engine="pyarrow"`, CSV results are read by `pyarrow.csv`, which keeps multi-line values intact and stops turning a string such as `"007"` into `"7.0"`.
Columns that share a name keep their own values and types in the pandas, Arrow, and Polars cursors.

## SQLAlchemy

The dialect requires SQLAlchemy 2.0 and uses its insertmanyvalues support: `executemany` inserts are batched into multi-row `INSERT` statements of 100 rows by default.
A failed statement fails one page of rows, and earlier pages stay inserted.
Very wide rows can exceed Athena's query length limit; lower `insertmanyvalues_page_size` or set `use_insertmanyvalues=False` for such tables.

Type rendering is split into a Hive DDL compiler and a Trino DML compiler, so `CREATE TABLE` renders `STRUCT<name:type>` and `INT` while casts keep the Trino syntax.
Timestamp literals keep microseconds, and `cast()` to `DateTime` renders `TIMESTAMP(6)`.
Hive CTAS and `UNLOAD` reject `timestamp(6)` output columns, so cast those columns to `AthenaTimestamp(precision=3)` there.
Reflection falls back to `information_schema` when Athena throttles metadata requests, and `awsathena_compression` now produces the requested codec for Parquet, text, and JSON tables.
Outside SQLAlchemy, `get_table_metadata()`, `list_table_metadata()`, and `list_databases()` answer throttled requests from the AWS Glue Data Catalog, which needs `glue:GetTable`, `glue:GetTables`, and `glue:GetDatabases`; `connect(glue_metadata_fallback=False)` turns this off.

## S3 filesystem

`S3FileSystem` was audited against fsspec and s3fs and rebuilt around a typed `S3Core`.
Many of the resulting changes prevent data loss.
A failed `pipe()` or `put_file()` leaves the existing object unchanged, `mv()` deletes only the source objects it copied, and a recursive `rm()` of a bucket path on `AioS3FileSystem` raises `ValueError` instead of emptying the bucket.
`open(path, "xb")` and the `mode="create"` options raise `FileExistsError` for an existing object, and keys containing `?` work, with only a `?versionId=` query read as a version.

A few changes break callers.
`find(maxdepth=n)` now lists `n` levels, following fsspec's numbering, which also changes `rm(recursive=True, maxdepth=...)`.
`S3FileSystem.parse_path()` is replaced by `S3Path.parse()`, and the `max_worker` keyword is removed in favor of `max_workers`.

## Spark

The Spark cursors raise `NotSupportedError` for query parameters and no longer accept `work_group` in `execute()`; both used to be ignored.
Closing a Spark cursor created with an existing `session_id` no longer terminates that session.
Pass `terminate_session_on_close=True` to keep the old behavior, as described in the [Spark guide](https://pyathena.dev/v4.0.0/spark.html#breaking-changes-in-pyathena-4-0).

## Tests and CI

The integration tests run against real AWS services, and their daily cost in the test account dropped by about 87%, from $17.6 to $2.3, without reducing what they cover.
Pull requests run the PyAthena, SQLAlchemy, and Spark suites in parallel, only for the suites whose files changed and only once a pull request is ready for review.
The release workflow runs every suite on every supported Python version before it publishes to PyPI, which is how this release was published.

## Upgrading

Install this release with:

```sh
python -m pip install --upgrade 'PyAthena==4.0.0'
```

Keep any extras used by the application, for example `'PyAthena[SQLAlchemy]==4.0.0'`.
Read the **Breaking changes** section of the [release notes](https://github.com/pyathena-dev/PyAthena/releases/tag/v4.0.0) before upgrading; it covers every removed or renamed API with its replacement.
Fixes that were already backported to 3.37.0, 3.38.0, and 3.39.0 are summarized separately there, so users upgrading from 3.39.0 can skip them.

Thanks to [@mvanhorn](https://github.com/mvanhorn) and [@aminghadersohi](https://github.com/aminghadersohi) for their pull requests, and to everyone who reported the issues fixed in this release.
The release notes list all 183 pull requests merged since 3.36.0.
