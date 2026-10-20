+++
title = "Releasing PyAthena 4.2.0"
description = "PyAthena 4.2.0 speeds up SQLAlchemy result reads, supports Python 3.15 and Polars 2.0, and changes cursor lifecycle, asyncio accessors, and temporal types."
date = 2026-10-20T00:25:38+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy"]
+++

I released **PyAthena 4.2.0** on October 19.
It improves result reads through SQLAlchemy, adds support for Python 3.15 and Polars 2.0, and makes cursor lifecycle and temporal values more consistent.
Several changes require updates to calling code, even when upgrading from 4.1.0.
The [release notes][release] list the complete changes and migration details.

## Faster result reads

The pandas and Polars cursors now return Python rows using blocks of column values, without building a dictionary for every row.
This helps `pandas.read_sql_query()`, which asks a SQLAlchemy driver for rows and then builds a DataFrame from them.

The [release benchmark][cursor-benchmark] measured these median total times for **10 million flat rows**, through UNLOAD, without chunking:

| SQLAlchemy driver | 4.1.0 | 4.2.0 |
| --- | ---: | ---: |
| `awsathena+pandas` | 162.60 s | 50.71 s |
| `awsathena+polars` | 92.05 s | 48.71 s |

Both runs used `r7g.2xlarge` hosts and three trials per case, on different hosts and days.
These times include Athena execution and full result consumption.
The default `awsathena+rest` driver still pages through GetQueryResults; selecting a result-file driver and `unload=true` matters for large results.

The benchmark also extended the chunked readers to **100 million flat rows**, about 29 GB of CSV.
With 100,000-row chunks, PandasCursor and PolarsCursor peaked at 462 to 1,051 MiB of sampled process RSS, depending on the reader and transport.
This validates the chunking work across recent releases; chunked CSV reads and the 4.1.0 UNLOAD improvements were already available before 4.2.0.
The measured commit, `9e99caa8`, has the same package code as the release commit, `baee716c`; the difference is one test file.

## Temporal types

PandasCursor now gives temporal columns consistent dtypes based on the Athena type, rather than the particular values returned by a query:

| Athena type | Default pandas dtype |
| --- | --- |
| `timestamp` | `datetime64[us]` |
| `date` | `datetime64[us]` |
| `timestamp with time zone` | `datetime64[us, UTC]` |

Fractional digits beyond microseconds are truncated.
The fetch methods return Python `datetime` or `date` objects, with aware datetimes in UTC for timestamps with a time zone, and `None` for SQL NULL.
Code expecting `pd.Timestamp` or `NaT` from these fetches needs updating.
When `parse_dates`, `dtype`, or `converters` is passed to `execute()`, pandas controls these columns instead; the [pandas guide][pandas-guide] also describes overrides for UNLOAD reads.

An unknown time zone, or a local timestamp that its named zone repeats or skips during a daylight saving transition, now raises `DataError`.
Convert such values to UTC in SQL with `AT TIME ZONE 'UTC'`, or select `to_unixtime()` when epoch seconds suit the application.
The error identifies the column.

UNLOAD timestamp handling also changes.
The pandas and Arrow readers use microseconds at every nesting level, avoiding the nanosecond overflow that corrupted dates outside 1677–2262.
Polars 2.0 supports the corresponding read, but **Polars 1.x still reads UNLOAD timestamps at nanoseconds before casting them**, so out-of-range values remain wrong.
PolarsCursor now logs a warning recommending Polars 2.0.
Chunked Polars reads with pyarrow installed and no read options passed to `execute()` use the path that reads these values correctly.
Custom schemas and timestamp-unit overrides have their own behavior, documented in the [temporal type guide][temporal-guide].

## Cursor lifecycle and failed fetches

A closed cursor now raises `ProgrammingError("Cursor is closed.")` when used for execution, fetching, iteration, cancellation, or metadata methods.
Previously, fetches could appear to be exhausted and `execute()` could start another query.
Create a new cursor after closing one; the new `is_closed` property exposes its state.
Closed result sets also reject fetches and DataFrame or table access.

After a fetch fails, later fetches raise `ProgrammingError` instead of continuing past skipped rows or appearing exhausted.
An invalid argument, such as a non-integer `fetchmany()` size, does not break the result.
After a processing failure, start a new query instead of trying to resume that result set.

A connection may be shared between threads, but one cursor or result set may not be used concurrently from several threads.
That includes a timer thread calling `cursor.cancel()` or another thread closing a cursor during a fetch.
To stop a query from another thread, use its query ID with Athena's cancellation API.

The ThreadPool-based `AsyncCursor` family also has a clearer ownership boundary.
Closing a cursor makes futures still waiting for their query in `execute()`, `description()`, or `poll()` raise `ProgrammingError`; closing does not cancel that query.
A result set returned by a future before the close remains open and belongs to the caller.

## Awaiting DataFrame access

The native asyncio DataFrame and table accessors are now coroutines:

```python
df = await cursor.as_pandas()
```

The same applies to the `as_arrow()` and `as_polars()` methods of `AioArrowCursor` and `AioPolarsCursor`.
`AioPandasCursor.as_pandas()` always returns one DataFrame, collecting remaining chunks when chunking is enabled.
For bounded memory, use the new async `iter_chunks()` instead:

```python
import asyncio

from pyathena import aio_connect
from pyathena.aio.pandas.cursor import AioPandasCursor


async def main():
    async with await aio_connect(
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
        region_name="us-west-2",
    ) as connection:
        async with connection.cursor(
            AioPandasCursor, unload=True, chunksize=100_000
        ) as cursor:
            await cursor.execute("SELECT * FROM your_table")
            async for chunk in cursor.iter_chunks():
                print(len(chunk))


asyncio.run(main())
```

This example needs the `Pandas` and `Arrow` extras, AWS credentials, and a table accessible to the configured Athena workgroup.
`AioPolarsCursor` supports async chunk iteration too.
Cancelling a fetch or an `as_*()` call on the native asyncio result-file cursors detaches the result; subsequent fetches raise `ProgrammingError("No result set.")`.
The [asyncio guide][aio-guide] describes cancellation and cleanup in detail.

## S3 transfers and compatibility

In the [filesystem benchmark][filesystem-benchmark], PyAthena 4.2.0 used much less memory for multipart uploads than 4.1.0.
When writing a 6 GiB object with 40 workers, peak process memory fell from 6,005–6,034 MiB in 4.1.0 to 690–699 MiB in 4.2.0 across the synchronous and asyncio filesystem paths.
The write path now keeps at most `max_workers` upload parts in flight and waits for one to finish before submitting another, limiting the memory held by pending uploads.

`AioS3FileSystem` now sizes its worker pools by `max_workers`, avoiding the event loop's default executor limit.
With 40 workers, its measured write and copy throughput matched the synchronous filesystem's.
This still runs blocking boto3 calls in threads, and `S3FileSystem` remains registered for fsspec's `s3` and `s3a` protocols.

Polars 2.0 is supported, including map columns in UNLOAD results.
There is a known memory regression for whole CSV reads with Polars 1.x: the read holds a copy of the result file while parsing it.
The regression is tracked in [#1307][polars-csv-issue].
A fix has been merged in [#1313][polars-csv-fix] and is expected in the next PyAthena release.
Polars 2.0, chunked reads, and UNLOAD results avoid that particular regression.

## Upgrading

Install the release with the extras used by the application:

```sh
python -m pip install --upgrade 'PyAthena[Pandas,Arrow]==4.2.0'
```

The minimum Python version remains 3.11; this release adds Python 3.15 support and requires `tzdata>=2024.2`.
Before upgrading, read the [breaking changes][release], including the S3 multipart API, custom result-set subclasses, metadata readers, and exception changes not covered here.
For an upgrade from 3.x, read the [4.0.0 post](@/posts/pyathena-4-0-0.md) as well.

[release]: https://github.com/pyathena-dev/PyAthena/releases/tag/v4.2.0
[cursor-benchmark]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-19-9e99caa8.md
[filesystem-benchmark]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-19-9e99caa8-filesystem.md
[polars-csv-issue]: https://github.com/pyathena-dev/PyAthena/issues/1307
[polars-csv-fix]: https://github.com/pyathena-dev/PyAthena/pull/1313
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/docs/pandas.md
[temporal-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/docs/temporal_types.md
[aio-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/docs/aio.md
