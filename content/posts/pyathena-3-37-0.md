+++
title = "Releasing PyAthena 3.37.0 with SQLAlchemy ARRAY support"
description = "PyAthena 3.37.0 adds typed SQLAlchemy ARRAY queries and updates, fixes STRUCT DDL and DOUBLE casts, and preserves binary NULLs in CSV results."
date = 2026-10-02
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy"]
+++

I have released **PyAthena 3.37.0**, a minor release in the 3.x maintenance line.
It backports SQLAlchemy ARRAY support and several fixes from master.
The [release notes](https://github.com/pyathena-dev/PyAthena/releases/tag/v3.37.0) contain the complete list of changes.

## SQLAlchemy ARRAY support

PyAthena now supports SQLAlchemy's standard `ARRAY` type alongside `AthenaArray`, with typed reads and writes for native Athena arrays.
Reflection preserves the element types of ARRAY columns instead of reporting them as `String`.
Typed SELECT results preserve nested arrays and strings such as `"a,b"`, `"001"`, and `"null"`, including the distinction between a string and SQL NULL.

ARRAY expressions support indexing, slicing, concatenation, and ANY/ALL comparisons in SELECT and WHERE clauses.
On Iceberg tables, indexed and sliced UPDATE assignments can replace individual elements or resize a range within an array.
Indices are one-based by default, and slice stops are inclusive.

Typed ARRAY results use a JSON projection in the generated SQL.
Queries that need rewriting require explicit SELECT columns and labels on literal SQL expressions; prefer SQLAlchemy expressions for ORDER BY.
Decimal ARRAY binds require an explicit `Numeric(precision, scale)` to preserve fractional values.
Raw SQL queries and arrays with unknown element types retain the cursor's existing conversion behavior.
The [ARRAY guide for this release](https://github.com/pyathena-dev/PyAthena/blob/v3.37.0/docs/sqlalchemy.md#array-type-support) covers examples and the query restrictions in detail.

## Type and cursor fixes

- **STRUCT DDL:** `CREATE TABLE` now renders Hive `STRUCT<name:type>` syntax, including STRUCT values nested inside MAP and ARRAY columns.
  Athena rejected the previous `ROW(...)` output.
  Integer types inside ARRAY, MAP, and STRUCT column definitions now render as `INT`.
- **DOUBLE casts:** Casting to `Double` or `DOUBLE_PRECISION` now produces `DOUBLE`, fixing the previous use of the 32-bit `REAL` type.
- **Binary NULLs:** CSV result sets preserve binary NULL values as Python `None`.

Thanks to [mvanhorn](https://github.com/mvanhorn) for the STRUCT DDL fix and [aminghadersohi](https://github.com/aminghadersohi) for reporting the issue.

## Upgrading

Install this release with:

```sh
python -m pip install --upgrade 'PyAthena[SQLAlchemy]==3.37.0'
```

The 3.x line continues to support Python 3.10 and keeps its `sqlalchemy>=1.0.0` dependency requirement.
The ARRAY changes were checked at compilation level with SQLAlchemy 1.4.54; the test suite runs on SQLAlchemy 2.0.
Applications that inspect reflected types or compare compiled DDL strings should account for the ARRAY, STRUCT, and `INT` changes above.

The release workflow now runs every test suite on every supported Python version before publishing.
The [full changelog](https://github.com/pyathena-dev/PyAthena/compare/v3.36.0...v3.37.0) includes the CI and test coverage improvements as well.
