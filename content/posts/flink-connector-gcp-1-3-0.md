+++
title = "Releasing flink-connector-gcp 1.3.0 with client metrics and write ordering"
description = "flink-connector-gcp 1.3.0 adds client RPC metrics and opt-in sink ordering, requires BigQuery CDC sequences, and preserves change coordinates on deletes."
date = 2026-11-01T18:11:01+09:00
draft = false

[taxonomies]
tags = ["Flink", "GCP", "BigQuery", "Bigtable", "Spanner", "Firestore", "CloudTasks"]
+++

I have released **flink-connector-gcp 1.3.0**.
This release brings client-library RPC measurements into Flink's metric reporters, adds optional write ordering to the keyed sinks, and requires a sequence number for BigQuery CDC writes.
It also improves how Bigtable and Spanner Change Streams carry a delete's own commit coordinates through Flink SQL.

- [Release notes and SQL connector downloads](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.3.0)
- [Version 1.3 documentation](https://flink-gcp.github.io/flink-connector-gcp/1.3/)
- [Source at v1.3.0](https://github.com/flink-gcp/flink-connector-gcp/tree/v1.3.0)

The Maven group remains `io.github.flink-gcp`.
Use `1.3.0` with Flink 2.2 or 2.3, and `1.3.0-1.20` with Flink 1.20.
For SQL deployments, put the matching `flink-sql-connector-gcp-*` JAR in the cluster's `lib/` directory.
The connector modules and supported Flink versions are unchanged from [1.2.0](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-2-0/).

## Client RPC metrics in Flink

The connector's counters describe its work: records written, batches sent, retries and failures.
The client library can measure another part of that work, including the time spent in an RPC attempt and, for some methods, timing reported by Google's front ends.
Bigtable, Spanner and Cloud Tasks can now expose those measurements through the configured Flink metric reporters.

The export is off by default.
For a SQL table using one of these connectors, add this option to its `WITH` clause; this is an option fragment, not a complete table definition:

```sql
'client-metrics.export-to-flink' = 'true'
```

The DataStream API exposes `exportClientMetricsToFlink(true)` on the corresponding builders.
Metrics appear under `rpcMethod.METHOD.*` on the group of the component that owns the client, such as a sink writer, source reader or split enumerator.
For the Bigtable and Cloud Tasks exactly-once sinks, that owner is the committer.

The tables below list the exported counters; each name has the prefix `rpcMethod.METHOD.`.
For example, Cloud Tasks reports `rpcMethod.CreateTask.operations` and `rpcMethod.CreateTask.operationTimeMillis` through the gax client framework.
Those measure task creation, not the later HTTP handler execution.

<div style="overflow-x: auto;" tabindex="0" role="region" aria-label="Common RPC counters">

| Counter | Available in | Meaning |
| --- | --- | --- |
| `operations` | Bigtable, Spanner, Cloud Tasks | Completed client operations, including failures. |
| `operationTimeMillis` | Bigtable, Spanner, Cloud Tasks | Total duration of those operations, in milliseconds. |
| `attempts` | Bigtable, Spanner | RPC attempts, including retries within an operation. |
| `attemptTimeMillis` | Bigtable, Spanner | Total duration of those attempts, as observed by the client. |

</div>

Bigtable also reports:

<div style="overflow-x: auto;" tabindex="0" role="region" aria-label="Bigtable RPC counters">

| Counter | Meaning |
| --- | --- |
| `retries` | Attempts after the first attempt of each operation. |
| `connectivityErrors` | Attempts the client judges not to have reached Google, based on missing response headers. |
| `attemptsWithServerTiming` | Attempts whose responses carried Google front-end timing. |
| `serverTimeMillis` | Total time from entering Google's data center to completion, for those attempts. |
| `firstResponseTimeMillis` | Total time to the first response for the operations the client instruments for this measurement. |
| `applicationTimeMillis` | Total time the client attributes to the application consuming responses. |

</div>

Spanner's additional counters distinguish the Google front end (GFE) and Spanner API front end (AFE):

<div style="overflow-x: auto;" tabindex="0" role="region" aria-label="Spanner RPC counters">

| Counter | Meaning |
| --- | --- |
| `attemptsWithGfeTiming` | Attempts whose responses carried GFE timing. |
| `gfeTimeMillis` | Total time from GFE receipt to the first response byte, for those attempts. |
| `gfeConnectivityErrors` | Attempts without GFE timing; not recorded on DirectPath connections. |
| `attemptsWithAfeTiming` | Attempts whose responses carried AFE timing. |
| `afeTimeMillis` | Total time from AFE receipt to the start of its response, for those attempts. |
| `afeConnectivityErrors` | Attempts without AFE timing. |

</div>

Spanner does not record the AFE measurements when `SPANNER_DISABLE_AFE_SERVER_TIMING` is `true`.
Counts and duration sums support means, not latency percentiles: for example, divide the rate of `operationTimeMillis` by the rate of `operations` for a window's mean operation duration.
The server-timing sums use their corresponding `attemptsWith...Timing` counters as denominators.
Bigtable's first-response measurement covers only certain operation types, so `operations` is a valid denominator only when every operation of that method records it.
BigQuery has no equivalent client-metrics export in this release.

The connector records these values directly into Flink counters without running an OpenTelemetry SDK.
Enabling the Flink export leaves Bigtable and Spanner's built-in Cloud Monitoring export enabled.
The separate `client-metrics.export-to-cloud-monitoring` option can disable participation by these clients, but the Bigtable client has a session-based path where doing so also suppresses its Flink measurements.
Bigtable's `FULL` lookup cache has no Flink metric group either.
The [Bigtable](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/datastream/bigtable/#client-library-metrics), [Spanner](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/datastream/spanner/#client-library-metrics) and [Cloud Tasks](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/datastream/cloudtasks/#client-rpc-metrics) references describe the available methods and their limits.

## Ordering repeated writes to one key

A sink can receive two updates to one key in order and still apply them in the opposite order.
Several requests may be in flight, and a batch API need not apply the writes inside one request in their submitted order.
For a changing value, that can leave the destination holding an earlier update.

The new `sink.ordering-mode` option controls this behavior where the connector can preserve the order it receives.
Its default is `none`, retaining the previous behavior.

| Sink | Opt-in mode | What it orders within one sink subtask |
| --- | --- | --- |
| Firestore Native mode | `per-key` | Writes to one document; a later write waits for the earlier write's result. |
| Spanner | `per-key` | Mutations of one row; the writer sends its batch before accepting a second mutation of that row into it. |
| Bigtable | `per-key` | Writes to one row in the supported write modes; later writes wait for earlier ones to be answered. |
| BigQuery `storage-api-at-least-once` | `per-table` | Append requests to one table; a later request waits for the previous request to finish, including its retries. |

For example, this option fragment enables ordering on a Firestore SQL sink:

```sql
'sink.ordering-mode' = 'per-key'
```

For DataStream, configure `orderingMode(...)` on `FirestoreWriterOptions`, `SpannerWriterOptions` or `BigtableWriterOptions`; Bigtable's single-row sinks use `BigtableRequestOptions`, and BigQuery uses `DefaultStreamOptions`.
Bigtable accepts `per-key` for `upsert`, `keep-latest`, `insert-if-absent`, `conditional`, `append` and `increment`; its `aggregate` mode, exactly-once sink and async helpers reject it.
For `upsert` writes that retain earlier cell versions, a read still returns the highest-timestamp version, so a later write with an older explicit timestamp can remain hidden.
The `keep-latest` and map-family `replace` modes delete the earlier versions they replace.

Ordering introduces waiting.
A repeatedly updated key can require a round trip per change, and held writes consume the writer's in-flight budget.
BigQuery's mode permits one append request in flight per table per subtask, so its batch size also affects throughput.
For CDC, it additionally ends a request before a repeated primary key, keeping two changes of that key out of the same request.

The [Bigtable measurements](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/0190-the-bigtable-sinks-keep-per-row-order-on-opt-in.md#evidence) put numbers on this trade-off: with the default batching thresholds, changing 200 rows five times each took about 0.2 seconds under `none` and 1 second under `per-key`.
Across three runs per mode, `none` left 100–200 of the 200 rows at an earlier change, while `per-key` left every row at its last change.
For 2,000 writes to distinct rows, both modes took about 0.4 seconds.

The [BigQuery measurements](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/0192-the-bigquery-default-stream-keeps-arrival-order-per-table-on-opt-in.md#evidence) show the effect of batch size: writing about 50 MB into one table took about 24 seconds with `per-table` at the default 512 KiB request cap, versus about 3 seconds with `none` at that cap.
Raising the cap to 8 MiB reduced the ordered run to about 6 seconds.
No append failed in these runs, so they measure the cost of ordering rather than demonstrate recovery or reordering.

These were service runs from one writer on a workstation outside Google Cloud, so their durations include that network distance.
The linked reports give the setup and repetitions; the [Firestore](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/0187-the-firestore-sink-keeps-per-document-order-on-opt-in.md#evidence) and [Spanner](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/0189-the-spanner-sink-keeps-per-row-order-on-opt-in.md#evidence) reports also compare distinct keys with repeated changes to one key.

New gauges show the writes waiting for their turn: Firestore's `bufferedWrites`, Bigtable's `bufferedEntries` for its batching sink, and Bigtable's `bufferedRequests` for its single-row sinks.
They count held writes rather than requests already sent, and stay at zero under the default ordering mode.

The scope is the order one sink subtask receives.
Changes that need ordering must already arrive in the intended order at the same subtask; DataStream jobs need appropriate key partitioning, and SQL jobs need the corresponding keyed distribution.
A declared SQL primary key does not force that shuffle for every insert-only plan.
These modes do not establish an order across subtasks, restarts or rescales.
An attempt that times out on the client can also be applied late by the service, as the [Bigtable](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/table/bigtable/#two-rows-for-one-key-in-one-batch-have-no-defined-winner), [Spanner](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/datastream/spanner/#order-per-row) and [BigQuery](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/table/bigquery/#commit-order-of-a-tables-rows) references explain.

## BigQuery CDC requires a sequence

Version 1.2.0 accepted CDC writes without a change sequence number.
Version 1.3.0 rejects that configuration.
This requirement applies to both DataStream and Table API / SQL CDC sinks.
This is a connector requirement: BigQuery itself permits sequence-less CDC and resolves it by ingestion time.

That time need not preserve the order in which the sink received a key's changes.
In the recorded service test, 500 keys were each changed twice within one append request, and every key retained its first change.
Waiting for requests one at a time addressed the tested ordering problem at the cost of a round trip per repeated key.
[ADR-0193](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/0193-bigquery-cdc-writes-require-a-change-sequence-number.md) records those measurements and the decision to require a sequence.

For DataStream jobs, configure `sequenceNumberProvider(...)` before building `CdcOptions`.
For SQL, a CDC sink must declare exactly one persisted, writable metadata column for `change-sequence-number`, `debezium-source-properties` or `spanner-change-sequence`.
It must not be `VIRTUAL`, and an `INSERT` with an explicit target-column list must include it.
Each emitted row must supply a valid, non-null value.
The [BigQuery SQL reference](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/connectors/table/bigquery/#change-data-capture) shows the formats and source-specific mappings, including how to add the column to a table derived from a catalog.

An application-provided sequence must come from where the change is made, increase with each change of a key and remain the same when that change is replayed.
Reading `NOW()` in the pipeline does not provide that property.
The `per-table` ordering mode is no substitute for a sequence across recovery.
Equal sequences still fall back to ingestion order, so source-specific ties and changes introduced by Flink's changelog processing need attention too.

## A delete's own coordinates in Flink SQL

This change concerns Flink SQL and Table API jobs reading Bigtable selected-cell or Spanner `upsert` Change Streams sources.
Jobs that consume Change Streams records directly through the DataStream API do not use the planner's `ChangelogNormalize` operator.
If a DataStream job also routes those records through Table API or SQL, check that part of its plan for the operator.

When Flink's `ChangelogNormalize` operator completes a key-only delete, it reconstructs the row from the last value it holds.
The resulting row includes that earlier value's metadata, so a CDC sequence built from it can describe the change being deleted rather than the delete itself.
If the operator has never seen the key, it drops the delete.

The Bigtable selected-cell and Spanner `upsert` Change Streams Table sources can now fill ordinary physical columns with each change's commit coordinates, including on deletes.
Bigtable supplies a commit timestamp and tie breaker; Spanner supplies a commit timestamp, record sequence and mod number.
The [Bigtable-to-BigQuery](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/examples/bigtable/) example uses these columns to build the CDC sequence; the Spanner example below shows the typed sequence route.

On Flink 2.x, a direct projection of those physical columns into a sink accepting key-only deletes can avoid `ChangelogNormalize` and preserve the delete's own coordinates.
Reading metadata, filtering on non-key columns, aggregating or enabling source CDC deduplication can bring the operator back.
Flink 1.20 retains it after these upsert sources, so changing the column declarations there does not remove that limitation.

## Bounded Spanner Change Streams reads

Spanner Change Streams can also finish at an inclusive commit timestamp, through `boundedTimestamp(Instant)` in DataStream or `scan.bounded.timestamp-millis` in SQL.
This supports reading a finite interval of changes with the same bound names used by Bigtable.

## Spanner changes into BigQuery

This is the [native Change Streams SQL example](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/examples/bigquery/#native-change-streams-sql) from the 1.3 documentation.
The three SQL blocks are copied from its source-backed example.
They show how the new physical coordinate columns reach BigQuery's required sequence metadata.

Install the Spanner and BigQuery SQL connector JARs for the cluster's Flink version and configure credentials for both services.
Replace the resource names with an existing Spanner database, an `Orders` table keyed by `OrderId`, and a BigQuery dataset.
The physical Spanner change stream `order_changes` must already watch `Orders` with `value_capture_type` set to `NEW_ROW` or `NEW_ROW_AND_OLD_VALUES`, as the [Spanner setup example](https://flink-gcp.github.io/flink-connector-gcp/1.3/docs/examples/spanner/#comparing-changelog-modes-and-materializing-changes) shows.
That setup example uses `source_orders` and `source_order_changes`; adapt its table, column and stream names to `Orders`, `OrderId`, `Customer`, `Status` and `order_changes` below, or change the Flink DDL consistently.
This job starts at the latest position and reads subsequent changes; it does not load the table's existing rows.

Declare the source with three physical columns for the coordinates:

```sql
SET 'execution.checkpointing.interval' = '1 min';

CREATE TABLE order_changes (
  OrderId BIGINT,
  Customer STRING,
  Status STRING,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL,
  record_sequence STRING NOT NULL,
  mod_number INT NOT NULL,
  PRIMARY KEY (OrderId) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'Orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert',
  -- Physical columns the source fills on every row, a delete included.
  'scan.change-stream.commit-timestamp-column' = 'commit_timestamp',
  'scan.change-stream.sequence-column' = 'record_sequence',
  'scan.change-stream.mod-number-column' = 'mod_number',
  'scan.startup.mode' = 'latest'
);
```

The destination accepts a typed `spanner-change-sequence` metadata value.
The sink can create the BigQuery table in the existing dataset, and `per-table` orders its append requests within each subtask:

```sql
CREATE TABLE current_orders (
  OrderId BIGINT NOT NULL,
  Customer STRING,
  Status STRING,
  change_sequence ROW<commit_timestamp TIMESTAMP_LTZ(9), record_sequence STRING, mod_number INT>
    METADATA FROM 'spanner-change-sequence',
  PRIMARY KEY (OrderId) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_orders',
  'sink.cdc.enabled' = 'true',
  'sink.ordering-mode' = 'per-table',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min'
);
```

Project the coordinates into that metadata value when inserting the changelog:

```sql
INSERT INTO current_orders
SELECT OrderId, Customer, Status,
       ROW(commit_timestamp, record_sequence, mod_number)
FROM order_changes;
```

The checkpoint interval persists the source position and flushes the BigQuery default stream.
Keep the commit timestamp at precision 9; reducing it to milliseconds can make distinct changes share a timestamp.
The non-key business columns remain nullable so key-only deletes can pass through the projection and sink on Flink 2.2 and 2.3, while the source fills the coordinate columns even on a delete.
To export the Spanner source's client metrics too, add `'client-metrics.export-to-flink' = 'true'` to its `WITH` clause.
The Flink 1.20 normalization limitation described above still applies to this example.

## Recovery evidence and smaller fixes

The release includes deployed recovery trials on GKE Autopilot against real Bigtable and Spanner services.
The workloads covered Bigtable's two sink delivery modes and Change Streams source, and Spanner's sink, Change Streams source and batch source.
Trials exercised savepoint upgrades, JobManager and TaskManager replacement, and rescaling in both directions.
Their output checks found no lost record; this does not mean that the at-least-once paths produced no replayed effects.

The [Bigtable findings](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/evidence/0165-bigtable-recovery-findings-1855.md) and [Spanner findings](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/evidence/0165-spanner-recovery-findings-1878.md) document the workloads, recovery timings and unexercised paths.
These were Flink 2.2.1 runs, not verification of every supported version or Table API entry point.
A separate [Cloud Tasks rescale-out rerun](https://github.com/flink-gcp/flink-connector-gcp/blob/v1.3.0/docs/adr/evidence/0165-cloudtasks-rescale-out-findings-1799.md) listed all 180,000 expected tasks with none missing or repeated; it did not measure handler executions.

Two source fixes address progress and observability.
Bigtable's Change Streams enumerator now assigns partitions beyond a reader's free read slots so the reader can rotate through them; previously, excess partitions in an unbounded job could remain unread.
Enumerator metrics, including Flink's `unassignedSplits`, also keep reporting when a restart replaces the enumerator, instead of retaining the first enumerator's values.

## Upgrading from 1.2.0

The compatibility check against 1.2.0, run over the `@Public` and `@PublicEvolving` Java APIs, reports only additions.
That does not make every existing configuration valid: migrate sequence-less BigQuery CDC jobs as described above.

Three other changes affect previously accepted inputs or custom serializers:

- Resource-path components consisting of exactly `.` or `..` are rejected across the connectors. Dots within names remain accepted.
- A schema conflict introduced by a custom BigQuery serializer during record serialization fails the job under all three write methods, rather than being routed as a single record's failure.
- BigQuery's appended protobuf fields, including CDC fields, retain their field numbers as a custom serializer's schema evolves. An evolution that would move those numbers fails the job instead of risking misinterpretation of retained rows.

The Google Cloud `libraries-bom` moves from 26.90.0 to 26.91.0, and the `1.3.0-1.20` artifacts are compiled against Flink 1.20.5 instead of 1.20.4.
Flink itself is still supplied by the cluster.
Use the [1.3 documentation](https://flink-gcp.github.io/flink-connector-gcp/1.3/) when updating jobs; the Development documentation follows later changes.

This is an independent open-source project.
It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.
