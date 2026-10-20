+++
title = "Improving PyAthena result reads from 4.0 to 4.2"
description = "How S3 prefetching, Parquet chunk readers, and block-based row conversion improved PyAthena's result reads, with awswrangler comparisons and 100-million-row measurements."
date = 2026-10-20T00:25:39+09:00
draft = false

[taxonomies]
tags = ["AWS", "Athena", "Python", "SQLAlchemy", "Performance"]
+++

Two PyAthena issues had been open for years.
[Issue #61][issue-61], opened in January 2019, asked about processing query results larger than memory.
[Issue #254][issue-254], opened in November 2021, reported a large performance gap between SQLAlchemy with pandas and awswrangler.
The [4.2.0 benchmark publication][publication] finally gave me enough evidence to close both.

The result is useful beyond the version number.
PandasCursor now competes with awswrangler for the measured DataFrame reads, and the pandas and Polars cursors processed 100 million flat rows with roughly 0.5–1 GiB of sampled process memory when reading in 100,000-row chunks.
SQLAlchemy reads improved substantially too, although reconstructing a DataFrame from Python rows still costs time and memory.

These measurements use a new workload, not a replay of the queries in those old issues.
Closing #61 also does not mean PyAthena became a distributed execution system: the work here is a single client's result reader.
The changes from 4.0 to 4.2 address three separate costs: fetching bytes, keeping too much decoded data alive, and turning columns into Python rows.

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

The [4.0.0][bench-40] and [4.1.0][bench-41] comparisons used the same instance type and row counts, but different hosts, days, and snapshots of the same source partition.
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

## Overlapping CSV reads and parsing

The [4.1.0 CSV work][csv-change] improved how the client feeds its parser.
By default, PandasCursor reads a CSV result in 32 MiB blocks with a background cache that holds two blocks.
While pandas parses the current block, another thread fetches the next one.
S3FSCursor also prefetches CSV data, and ArrowCursor downloads CSV data through parallel range requests.

Prefetching overlaps network waiting with parsing.
It does not reduce the bytes in the result or make parsing free; it gives the parser a better chance of finding the next bytes ready.
For ten million flat rows without chunking, the published runs measured:

| Client result time | 4.0.0 | 4.1.0 |
| --- | ---: | ---: |
| PandasCursor CSV | 80.9 s | 44.4 s |
| ArrowCursor CSV | 30.4 s | 6.5 s |

The extra buffering has a memory cost.
For PandasCursor CSV with 10,000-row chunks, peak RSS rose from 256 to 363 MiB on that workload.
The [S3 read options][pandas-guide] allow a different block size or cache type when that tradeoff is unsuitable.

Athena execution can hide a client improvement in total time.
In the 4.2.0 run, the flat CSV query at ten million rows took 48.85 seconds in Athena, compared with 28.80 seconds in the 4.1.0 run.
PandasCursor's unchunked client result time fell further, from 44.43 to 34.30 seconds, while its total time grew.
The cause of that further client-time difference was not investigated, so I do not attribute it to a particular 4.2.0 change.

## Bounding Parquet memory below the DataFrame

Returning chunks to the caller is only useful for memory if the reader underneath avoids loading the whole result first.
In 4.0.0, PolarsCursor could yield UNLOAD chunks, but its underlying scan still kept substantial decoded data alive.
At ten million flat rows with 10,000-row chunks, it peaked at 3,732 MiB.

The [4.1.0 Parquet work][parquet-change] introduced a reader shared by the pandas and Polars paths.
It decodes files one at a time and uses `ParquetFile.iter_batches()` for one row group at a time.
Rows carry over between batches and files until an output chunk is complete.
This also added chunked UNLOAD reads to PandasCursor.

The reader uses different strategies for small and large files.
A file up to 32 MiB is fetched whole, with parallel range requests, and the next file is prefetched while the current one is decoded.
For a larger file, pyarrow buffers coalesced ranges for the current row group; the default filesystem cache does not independently read ahead across the file.
The [implementation][parquet-reader] keeps the decode scope small while still allowing useful I/O overlap.

This makes memory depend on chunks, row groups, and buffers, as well as the row width and data types.
It does not impose a fixed byte limit for every possible query.
In the 4.1.0 run, the same Polars flat case peaked at 659 MiB instead of 3,732 MiB; the nested case fell from 6,073 to 642 MiB.
The flat client result time increased from 1.2 to 2.2 seconds, a tradeoff for the lower peak.

The shared path has conditions.
Polars uses it with pyarrow installed and no read options passed to `execute()`.
Pandas supports a bounded set of Parquet options for chunked reads, including `dtype_backend` and selection of top-level columns; other options can select a whole-result read.
The [pandas][pandas-guide] and [Polars][polars-guide] guides describe those conditions.

## Converting columns to rows in blocks

Once UNLOAD reads became inexpensive, the SQLAlchemy path exposed the cost of producing Python rows.
In 4.1.0, PandasCursor returned ten million flat rows as a DataFrame in about 19 seconds, but `read_sql_query()` through the pandas driver took 162.60 seconds.
Both used UNLOAD; they returned their data through different interfaces.

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
The AWS measurements establish the change visible to callers:

All rows below use UNLOAD through `read_sql_query()`, without chunking.

| Driver and flat row count | 4.1.0 total | 4.2.0 total |
| --- | ---: | ---: |
| pandas driver, 1 million flat rows | 18.31 s | 7.02 s |
| Polars driver, 1 million flat rows | 11.28 s | 6.66 s |
| pandas driver, 10 million flat rows | 162.60 s | 50.71 s |
| Polars driver, 10 million flat rows | 92.05 s | 48.71 s |

The Arrow driver was already close to this level: its ten-million-row UNLOAD time was 50.09 seconds in 4.1.0 and 50.93 seconds in 4.2.0.
The large gains belong to the pandas and Polars row paths.

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
At ten million flat rows, its peak rose from 6,854 to 9,641 MiB compared with 4.1.0.
Polars 2.0 measured 7,547 MiB for that case; chunking or UNLOAD avoids this particular whole-CSV issue.
The regression is tracked in [#1307][polars-issue], with a proposed fix in [#1313][polars-csv-fix].
As of October 21, 2026, the fix is still a draft PR with no target release assigned.

The same release also improved S3 writes, separately from query-result reading.
Bounding multipart parts in flight reduced a 6 GiB write's peak from about 6 GiB to 690–699 MiB with 40 workers.
Giving `AioS3FileSystem` pools sized by `max_workers` brought its measured write and copy throughput in line with the synchronous filesystem at that setting.
The [filesystem run][fs-benchmark] used a different host type and workload, so those figures do not explain the cursor speedups.

For a DataFrame that fits in memory, direct PandasCursor UNLOAD avoids the SQLAlchemy row round trip.
For incremental processing, the pandas or Polars chunk readers keep much less of the result alive.
Applications that need SQLAlchemy can keep that interface and select a result-file driver, while accounting for the remaining conversion cost.
Those are concrete choices the completed benchmarks now let me recommend, with measurements behind them.

[issue-61]: https://github.com/pyathena-dev/PyAthena/issues/61
[issue-254]: https://github.com/pyathena-dev/PyAthena/issues/254
[publication]: https://github.com/pyathena-dev/PyAthena/pull/1310
[bench-42]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-19-9e99caa8.md
[bench-41]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-13-437276e1.md
[bench-40]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-11-8a69ce03.md
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
[fs-benchmark]: https://github.com/pyathena-dev/PyAthena/blob/0a58be36ff188fc1dc9541d1d28e5383500a8110/benchmarks/history/2026-10-19-9e99caa8-filesystem.md
