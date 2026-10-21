+++
title = "Benchmarking faster result reads and lower memory use in PyAthena 4.2.0"
description = "How PyAthena 4.2.0 speeds up result reads and reduces memory use, with implementation details, awswrangler comparisons, and benchmarks of 100 million rows."
date = 2026-10-20T00:25:39+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy", "Performance"]
+++

PyAthena 4.2.0 has faster result reads and better control over memory use, and the [completed benchmarks][publication] now show what that means for applications.
Direct PandasCursor reads are competitive with awswrangler on the measured workloads.
The pandas and Polars cursors can process 100 million flat rows with roughly 0.5–1 GiB of sampled process memory in 100,000-row chunks.
SQLAlchemy reads also improved substantially, although converting columnar data to Python rows and back still adds overhead.

These results let me close two issues that had been open for years.
[Issue #61][issue-61], opened in January 2019, asked about processing query results larger than memory.
[Issue #254][issue-254], opened in November 2021, reported a large performance gap between SQLAlchemy with pandas and awswrangler.
The new measurements cover a single client's result reader on a new workload, rather than replaying the original queries or demonstrating distributed processing.

The implementation addresses three sources of overhead: waiting for result bytes, keeping too much decoded data alive, and converting columns into Python rows one value at a time.

## What the benchmark measures

The [4.2.0 cursor run][bench-42] used ten Spot `r7g.2xlarge` hosts in `us-west-2`, each with 8 vCPUs and 64 GiB of memory, running Python 3.12.14 and Athena engine version 3.
The libraries included pandas 3.0.6, pyarrow 25.0.1, Polars 1.44.2, awswrangler 3.17.1, and SQLAlchemy 2.0.46.
Polars cases also ran separately with Polars 2.0.0.
Unless a table says otherwise, the Polars results below use 1.44.2.

The input was a fixed partition of PyPI download data, projected into flat, nested, JSON, and temporal workloads.
Snapshots ranged from 100,000 to 100 million rows; the largest scale covered the flat projection only.
The run measured synchronous APIs, with three or five repetitions per case, except for one whole-result read at 100 million rows.
Each trial ran in a fresh process, without warmups; Athena result reuse and client query caches were disabled.
Operating-system and service caches were not flushed.

**Total time** covers execution through full consumption, excluding connection preparation and cleanup.
**Client result time** starts when the client observes query completion and ends after consumption.
The latter helps separate result reading from Athena execution, but still includes network I/O and is not a CPU-only measurement.
Memory is process RSS sampled every 50 ms, including common benchmark imports; brief peaks can be missed.
The [harness documentation][harness] defines these boundaries.

Times below are medians, and the before-and-after comparisons link to the baseline for each reader.
Those runs used the same instance type and row counts, but different hosts, days, and snapshots of the same source partition; they measure the complete readers, not isolated changes under identical conditions.
The measured 4.2.0 commit is `9e99caa8`; the release commit `baee716c` differs only in a test file.
The published run therefore measures the released package code.

## Choosing the result path

Athena execution is only the first part of returning a query to Python.
The client can page through GetQueryResults, read the CSV result directly from S3, or ask Athena to UNLOAD Parquet files and read those.
Afterward, the chosen API determines whether the application receives columnar data or Python rows.

| Interface | Retrieval and output |
| --- | --- |
| Default Cursor | GetQueryResults pages returning Python rows |
| PandasCursor | CSV or UNLOAD; pandas DataFrame, chunks, or rows |
| ArrowCursor | CSV or UNLOAD; Arrow table or rows |
| PolarsCursor | CSV or UNLOAD; Polars DataFrame, chunks, or rows |
| SQLAlchemy + pandas | The selected driver's rows, rebuilt as pandas DataFrames or chunks |

For a large DataFrame, the last distinction matters.
Using `awsathena+pandas` does not make `pandas.read_sql_query()` call `cursor.as_pandas()`.
The driver reads a DataFrame, returns its values as Python rows through `fetchall()` or `fetchmany()`, and pandas builds another DataFrame from those rows.
The Arrow and Polars drivers have the same conversion boundary.

The default REST driver has another cost: GetQueryResults pagination.
In the 4.2.0 run, `awsathena+rest` took about 227 seconds for one million flat rows, while the result-file drivers with UNLOAD took 6.7–7.0 seconds through the same pandas API.
Optimizing local row conversion cannot remove the REST driver's page requests; the application must select the appropriate driver as well.

## Comparing with awswrangler

For ten million flat rows without chunking, 4.2.0 and awswrangler 3.17.1 measured as follows.
All entries return pandas DataFrames, but the SQLAlchemy rows include reconstruction through Python rows.

| Reader and transport | Total | Client result |
| --- | ---: | ---: |
| PandasCursor direct, CSV | 84.17 s | 34.30 s |
| awswrangler, CSV | 108.88 s | 58.25 s |
| PandasCursor direct, UNLOAD | 19.79 s | 2.40 s |
| awswrangler, UNLOAD | 23.44 s | 3.72 s |
| SQLAlchemy pandas, UNLOAD | 50.71 s | 34.35 s |

The direct pandas reads are competitive in these cases.
The SQLAlchemy result is much faster than before, and faster than Wrangler's CSV read, but still takes about twice Wrangler's UNLOAD total time.
Its extra conversion boundary remains relevant when choosing an API.

Nested data needs a separate comparison.
For ten million nested rows through UNLOAD, the default PandasCursor DataFrame took 87.99 seconds, while `dtype_backend="pyarrow"` took 34.22 seconds and reduced RSS from 19,498 to 5,820 MiB.
The latter retains Arrow-backed nested data instead of materializing the same pandas representation, so that gain is not evidence that identical output became faster.
Wrangler's CSV nested cases were unsupported; they must not be treated as slow or failed runs.

Datetime-heavy results also cost more than the flat projection.
The new temporal workload exposes further conversion work tracked in [#1308][temporal-issue].
Its UNLOAD projection omits time and time-zone-bearing columns, so its CSV and UNLOAD results are not interchangeable performance tests.
The benchmark supports choosing a path for a known workload, not a claim that every PyAthena API is faster than every Wrangler mode.

## Overlapping CSV reads and parsing

The earlier PandasCursor CSV path fetched blocks as the parser needed them, leaving the parser waiting on network reads.
The [prefetching implementation][csv-change] overlaps that waiting with useful work.
In 4.2.0, PandasCursor reads a CSV result in 32 MiB blocks by default, with a background cache that holds two blocks.
While pandas parses the current block, another thread fetches the next one.
S3FSCursor also prefetches CSV data, and ArrowCursor downloads CSV data through parallel range requests.

For ten million flat rows without chunking, PandasCursor's client result time fell from 80.9 seconds in the [earlier reader benchmark][reader-baseline] to 34.3 seconds in 4.2.0.
Prefetching gives the parser a better chance of finding the next bytes ready, but the measurements do not isolate how much of that difference comes from prefetching.
Athena execution time also varied between runs, which is why this comparison uses client result time.

The extra buffering trades some memory for less waiting.
With 10,000-row chunks on the same workload, peak RSS rose from 256 MiB in the baseline to 362 MiB in 4.2.0.
The [S3 read options][pandas-guide] allow a different block size or cache type when that tradeoff is unsuitable.

## Bounding Parquet memory below the DataFrame

Returning chunks to the caller is only useful for memory if the reader underneath avoids loading the whole result first.
The earlier PolarsCursor could yield UNLOAD chunks, but its underlying scan still kept substantial decoded data alive.
PandasCursor read UNLOAD results whole even when a chunk size was requested.

The [shared Parquet reader][parquet-change] used by the pandas and Polars paths in 4.2.0 moves chunking into the decoding layer.
It decodes files one at a time and uses `ParquetFile.iter_batches()` for one row group at a time.
Rows carry over between batches and files until an output chunk is complete.

The reader uses different strategies for small and large files.
A file up to 32 MiB is fetched whole, with parallel range requests, and the next file is prefetched while the current one is decoded.
For a larger file, pyarrow buffers coalesced ranges for the current row group; the default filesystem cache does not independently read ahead across the file.
The [implementation][parquet-reader] keeps the decode scope small while still allowing useful I/O overlap.

At ten million rows with 10,000-row chunks, PolarsCursor's sampled peak RSS changed as follows, comparing the [earlier reader benchmark][reader-baseline] with 4.2.0:

| UNLOAD workload | Before | 4.2.0 |
| --- | ---: | ---: |
| Flat rows | 3,732 MiB | 665 MiB |
| Nested rows | 6,073 MiB | 633 MiB |

The reader now limits how much data it decodes at once, so returning smaller chunks can actually reduce memory use.
Memory still depends on row groups, buffers, row width, and data types as well as chunk size; there is no fixed byte limit for every query.
Batching also has a processing cost: in the 4.2.0 flat case, client result time was 1.88 seconds with 10,000-row chunks, compared with 1.19 seconds for the whole-result read.

The shared path has conditions.
Polars uses it with pyarrow installed and no read options passed to `execute()`.
Pandas supports a bounded set of Parquet options for chunked reads, including `dtype_backend` and selection of top-level columns; other options can select a whole-result read.
The [pandas][pandas-guide] and [Polars][polars-guide] guides describe those conditions.

## Converting columns to rows in blocks

SQLAlchemy exposes another cost after the result file has been read: producing Python rows from columns.
The earlier pandas row iterator built a dictionary for each row and converted values individually.
Those allocations and conversions added substantial work between reading the DataFrame and returning it through `read_sql_query()`.

The [pandas row-output change][pandas-rows] removed the intermediate dictionary per row, but profiling found that dictionaries were only part of the cost.
Iterating over column values one at a time, especially pandas string columns backed by Arrow, was expensive.
Simply switching to `DataFrame.itertuples(index=False, name=None)` improved the local synthetic case only modestly.

The new iterator works in **blocks of 10,000 rows** inside each DataFrame chunk.
For NumPy-backed and pandas string columns, it extracts the block's values with one `tolist()` call per column, then zips the columns into tuples.
Other column types retain the conversion appropriate to their values, rather than blindly applying the same fast path.
Temporal columns handled by PyAthena use their defined Python `datetime` or `date` representation, including `None` for missing values.
The [current pandas implementation][pandas-source] shows these choices.

The block bounds the extra Python objects held during conversion, even if the DataFrame chunk itself is large.
It is separate from the application's `chunksize`: a 100,000-row DataFrame can be converted in ten blocks without reading the next DataFrame early.

The [Polars change][polars-rows] follows the same structure, using `Series.to_list()` per column block and avoiding calls to the default identity converter.
Custom conversions still run when needed.
Both cursors let bulk fetches consume slices of the tuple iterator instead of calling `fetchone()` for every row.
For the other buffered result sets, [bulk fetch work][bulk-fetch] similarly moves already-converted rows in batches; it does not remove their paging or per-value conversion costs.

The pandas PR recorded local synthetic row-output gains of roughly six to nine times on an Apple M1 Pro.
Those numbers exclude Athena and S3; they are evidence about the conversion mechanism, not end-to-end query speedups.
The AWS measurements show the improvement visible to callers, comparing the [baseline SQLAlchemy run][row-baseline] with 4.2.0.
All rows below use UNLOAD through `read_sql_query()`, without chunking:

| Driver and flat row count | Before | 4.2.0 |
| --- | ---: | ---: |
| pandas driver, 1 million flat rows | 18.31 s | 7.02 s |
| Polars driver, 1 million flat rows | 11.28 s | 6.66 s |
| pandas driver, 10 million flat rows | 162.60 s | 50.71 s |
| Polars driver, 10 million flat rows | 92.05 s | 48.71 s |

These are total times, including Athena execution and pandas' reconstruction of the output DataFrame.
For ten million flat rows, the pandas driver's total fell by about 69%, and the Polars driver's by about 47%.
The Arrow driver was already close to the current level, at 50.09 seconds in the baseline and 50.93 seconds in 4.2.0; the large gains belong to the pandas and Polars row paths.

## Processing 100 million rows

The largest 4.2.0 cases tested whether memory remained manageable as the result grew tenfold.
With 100,000-row chunks and the flat workload, the published peak RSS values were:

| Reader and transport | 10 million rows | 100 million rows |
| --- | ---: | ---: |
| PandasCursor, CSV | 461 MiB | 462 MiB |
| PandasCursor, UNLOAD | 771 MiB | 986 MiB |
| PolarsCursor 1.44.2, CSV | 978 MiB | 1,001 MiB |
| PolarsCursor 1.44.2, UNLOAD | 817 MiB | 1,051 MiB |
| awswrangler, CSV | 391 MiB | 390 MiB |
| awswrangler, UNLOAD | 820 MiB | 1,134 MiB |

The 100-million-row CSV was about 29 GB.
The one unchunked PandasCursor CSV trial used 49,824 MiB and did fit on the 64 GiB host, close to the harness's 50,506 MiB limit.
This was not an out-of-memory test on a smaller machine.
The chunked cases instead demonstrate that this workload can be consumed without holding its full DataFrame in memory.
The UNLOAD cases wrote eight files, up to about 184 MiB each, and PandasCursor, PolarsCursor, and Wrangler completed their chunked UNLOAD reads in roughly 35–40 seconds.
At the same chunk size, the CSV totals were 699.88 seconds for PandasCursor, 373.57 seconds for PolarsCursor 1.44.2, and 1,233.06 seconds for Wrangler: lower memory use in these cases came with a much longer wait.

## Using the chunk readers

For direct pandas use, process each chunk and release it before moving on:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

with connect(
    s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
    region_name="us-west-2",
) as connection:
    with connection.cursor(
        PandasCursor, unload=True, chunksize=100_000
    ) as cursor:
        cursor.execute("SELECT * FROM your_table")
        for chunk in cursor.iter_chunks():
            print(len(chunk))
```

This needs `PyAthena[Pandas,Arrow]`, AWS credentials, and a table accessible to the Athena workgroup.
Replace the print with processing that does not retain earlier chunks.
Collecting them into a list or concatenating them into one DataFrame restores whole-result memory use.

With SQLAlchemy, configure chunking at both layers:

```python
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

url = URL.create(
    "awsathena+pandas",
    host="athena.us-west-2.amazonaws.com",
    port=443,
    database="default",
    query={"s3_staging_dir": "s3://YOUR_S3_BUCKET/path/to/"},
)
engine = create_engine(
    url,
    connect_args={"cursor_kwargs": {"unload": True, "chunksize": 100_000}},
)
try:
    with engine.connect() as connection:
        for chunk in pd.read_sql_query(
            "SELECT * FROM your_table", connection, chunksize=100_000
        ):
            print(len(chunk))
finally:
    engine.dispose()
```

This example also needs the `SQLAlchemy` extra.
The cursor settings control how the result file is read; pandas' `chunksize` controls the row batches used to build its output DataFrames.
`connect_args["cursor_kwargs"]` replaces cursor options derived from the URL, so the example keeps `unload` and `chunksize` together there.
ArrowCursor reads a whole table, so merely chunking its SQLAlchemy output does not make its underlying read stream in bounded memory.

## Remaining tradeoffs

Polars 1.x has a known regression for whole CSV reads in 4.2.0: the parser receives bytes that keep a copy of the result file alive.
At ten million flat rows, its peak was 9,641 MiB, up from 6,854 MiB in the [preceding benchmark][row-baseline].
Polars 2.0 measured 7,547 MiB for that case; chunking or UNLOAD avoids this particular whole-CSV issue.
The regression is tracked in [#1307][polars-issue].
A fix has been merged in [#1313][polars-csv-fix] and is expected in the next PyAthena release.

For a DataFrame that fits in memory, direct PandasCursor UNLOAD avoids the SQLAlchemy row round trip.
For incremental processing, the pandas or Polars chunk readers keep much less of the result alive.
Applications that need SQLAlchemy can keep that interface and select a result-file driver, while accounting for the remaining conversion cost.

[issue-61]: https://github.com/pyathena-dev/PyAthena/issues/61
[issue-254]: https://github.com/pyathena-dev/PyAthena/issues/254
[publication]: https://github.com/pyathena-dev/PyAthena/pull/1310
[bench-42]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-19-9e99caa8.md
[row-baseline]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-13-437276e1.md
[reader-baseline]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-11-8a69ce03.md
[harness]: https://github.com/pyathena-dev/PyAthena/blob/9e99caa84857f8d29abe63435461fc412e040c43/benchmarks/README.md#measurement-semantics
[csv-change]: https://github.com/pyathena-dev/PyAthena/pull/1134
[parquet-change]: https://github.com/pyathena-dev/PyAthena/pull/1135
[parquet-reader]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/pyathena/arrow/reader.py
[pandas-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/docs/pandas.md
[polars-guide]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/docs/polars.md
[pandas-rows]: https://github.com/pyathena-dev/PyAthena/pull/1167
[polars-rows]: https://github.com/pyathena-dev/PyAthena/pull/1177
[pandas-source]: https://github.com/pyathena-dev/PyAthena/blob/v4.2.0/pyathena/pandas/result_set.py
[bulk-fetch]: https://github.com/pyathena-dev/PyAthena/pull/1187
[temporal-issue]: https://github.com/pyathena-dev/PyAthena/issues/1308
[polars-issue]: https://github.com/pyathena-dev/PyAthena/issues/1307
[polars-csv-fix]: https://github.com/pyathena-dev/PyAthena/pull/1313
