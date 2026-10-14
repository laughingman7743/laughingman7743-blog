+++
title = "Releasing PyAthena 4.1.0 with faster reads and lower memory use"
description = "PyAthena 4.1.0 speeds up CSV result reads and reduces memory use for chunked UNLOAD results, with changes to JSON parsing and asyncio SQLAlchemy adapter names."
date = 2026-10-14
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy"]
+++

I have released **PyAthena 4.1.0**, with improvements to CSV result reads and memory use when processing large UNLOAD results in chunks.
The [release notes](https://github.com/pyathena-dev/PyAthena/releases/tag/v4.1.0) contain the complete list of changes and migration notes.

## CSV result reads

By default, `PandasCursor` now downloads the next block of a CSV result from S3 while pandas parses the current one, including when reading in chunks.
`S3FSCursor` also prefetches CSV data, and `ArrowCursor` downloads CSV data through parallel range requests.

The [release benchmark](https://github.com/pyathena-dev/PyAthena/blob/6ea5587dde5aae5a5066a91da1f1231803d471e0/benchmarks/history/2026-10-13-437276e1.md) measured the following times for **10 million flat rows**, without chunking:

| Measurement | 4.0.0 | 4.1.0 |
| --- | ---: | ---: |
| PandasCursor CSV total time | 110.5 s | 74.4 s |
| PandasCursor CSV client result time | 80.9 s | 44.4 s |
| ArrowCursor CSV client result time | 30.4 s | 6.5 s |

These are medians from three trials per case on `r7g.2xlarge` hosts.
Total time includes Athena execution; client result time starts when the client observes query completion.
The two runs used the same instance type and row counts, on different hosts and days.

Prefetching uses additional memory for buffered data.
For the same 10 million flat rows with `chunksize=10_000`, PandasCursor's CSV peak memory rose from 256 MiB to 363 MiB.
The [S3 read options](https://github.com/pyathena-dev/PyAthena/blob/v4.1.0/docs/pandas.md#s3-read-options) describe how to select a different block size or cache type.

## Memory use for UNLOAD results

CSV result chunking was already supported by both `PandasCursor` and `PolarsCursor` before this release.
`PolarsCursor` also supported chunked UNLOAD reads; 4.1.0 improves how it reads those Parquet files to reduce memory use.

With pyarrow installed and no custom Polars read options, it now decodes the files one at a time, prefetches small files, and buffers larger files one row group at a time.
Memory use depends on the chunk size, row groups, and buffers.

The release benchmarks measured the following peak memory for **PolarsCursor with UNLOAD, 10 million rows, and `chunksize=10_000`**:

| Result shape | 4.0.0 | 4.1.0 |
| --- | ---: | ---: |
| Flat | 3,732 MiB | 659 MiB |
| Nested | 6,073 MiB | 642 MiB |

These values are sampled process RSS, including the benchmark's imports.
For the flat case, client result time increased from 1.2 seconds to 2.2 seconds.

For `PandasCursor`, this release adds chunked reading of the Parquet files produced by UNLOAD.
In the 4.1.0 benchmark, 10 million flat rows peaked at 604 MiB with 10,000-row chunks, compared with 5,376 MiB without chunking.

For pandas, use `unload=True`, `chunksize`, and `iter_chunks()` to process one DataFrame at a time:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

with connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2",
) as connection:
    with connection.cursor(PandasCursor, unload=True, chunksize=10_000) as cursor:
        cursor.execute("SELECT * FROM your_table")
        for chunk in cursor.iter_chunks():
            print(len(chunk))
```

This example requires the `Pandas` and `Arrow` extras.
Process each chunk before moving on; collecting the chunks into a single DataFrame holds the whole result in memory.
Additional pandas Parquet options can select the whole-result reader; the [chunking guide](https://github.com/pyathena-dev/PyAthena/blob/v4.1.0/docs/pandas.md#chunksize-options) lists the supported `columns` and `dtype_backend` settings.

## JSON columns in pandas

For CSV results with `json` columns, `PandasCursor` now converts the JSON values after pandas parses the CSV.
It no longer forces `low_memory=False`, allowing pandas' C engine to use its default internal chunking.

This changes how pandas options interact with JSON text.
With `parse_dates` naming a `json` column, pandas attempts date parsing before JSON decoding, so a JSON string such as `"2020-01-01"` is no longer parsed as a date; cast the value in SQL instead.
With `engine="python"` and NA options such as `keep_default_na=True`, a JSON string such as `"NA"` stays a string, while SQL NULL remains `None`.

## Asyncio cleanup and SQLAlchemy names

When `AioPandasCursor` starts another query or exits an `async with` block, it now closes the existing result set in a worker thread.
Closing a result stream can wait for an outstanding read, so moving that work off the event loop lets other coroutines continue.

The SQLAlchemy adapter classes in `pyathena.aio.sqlalchemy.base` have been renamed:

| Old name | New name |
| --- | --- |
| `AsyncAdapt_pyathena_cursor` | `AthenaAioAdaptedCursor` |
| `AsyncAdapt_pyathena_connection` | `AthenaAioAdaptedConnection` |
| `AsyncAdapt_pyathena_dbapi` | `AthenaAioAdaptedDBAPI` |

There are no aliases for the old names, so code importing them must be updated.
Engines created with `awsathena+aio*` URLs are unaffected by the rename.

## Upgrading

Install this release with:

```sh
python -m pip install --upgrade 'PyAthena==4.1.0'
```

Keep the extras used by the application, for example `'PyAthena[Pandas,Arrow]==4.1.0'` for the UNLOAD example above.
Python and dependency requirements are unchanged from 4.0.0.
For an upgrade from 3.x, read the [4.0.0 release post](@/posts/pyathena-4-0-0.md) as well.
The [full changelog](https://github.com/pyathena-dev/PyAthena/compare/v4.0.0...v4.1.0) includes the remaining fixes and benchmark tooling changes.
