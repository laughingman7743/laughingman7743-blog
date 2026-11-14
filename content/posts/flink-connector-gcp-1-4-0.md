+++
title = "Releasing flink-connector-gcp 1.4.0 with scan pushdown and BigQuery write improvements"
description = "flink-connector-gcp 1.4.0 adds Firestore and Datastore scan pushdown, BigQuery background uploads, and Spanner full-cache lookups, with upgrade guidance."
date = 2026-11-14T15:40:41+09:00
draft = false

[taxonomies]
tags = ["Flink", "GCP", "Firestore", "Datastore", "BigQuery", "Spanner", "PubSub", "Bigtable"]
+++

I have released **flink-connector-gcp 1.4.0**.
Firestore and Datastore SQL scans can send more of a query's work to the service, BigQuery file loads can overlap encoding with uploads, and Spanner lookup joins can load a table into a full cache.
The release also fixes incomplete Firestore Enterprise reads and adds an option to suppress a specific class of Pub/Sub redeliveries after recovery.

- [Release notes and SQL connector downloads][release]
- [Version 1.4 documentation][docs]
- [Source at v1.4.0][source]

The Maven group remains `io.github.flink-gcp`.
Use `1.4.0` with Flink 2.2 or 2.3, and `1.4.0-1.20` with Flink 1.20 LTS.
For SQL deployments, put the matching `flink-sql-connector-gcp-*` JAR in Flink's `lib/` directory.
The supported Flink versions and connector modules are unchanged from [1.3.0][previous].
The artifacts remain Java 17 bytecode, including the Flink 1.20 line.

## Reading less from Firestore and Datastore

A query that selects a few documents from a large collection should not have to transfer the whole collection to Flink before rejecting most of it.
In 1.4.0, a Firestore Native-mode table scan sends supported filters to the service by default.
These include comparisons with literals, ranges, `IN`, and `IS NOT NULL`, on supported scalar columns and fields inside `ROW` columns.
Flink still evaluates every predicate after reading the returned documents.

For example, given a `firestore` table named `orders` with an `order_id` primary key and a `total DOUBLE` column, this query can push its range filter:

```sql
SELECT order_id, total
FROM orders
WHERE total >= 100.0;
```

This is a query fragment for an existing table, not a complete setup script.
The [Firestore SQL reference][firestore] provides the table declarations and the supported filter shapes.
A collection-group scan does not push filters, and predicates such as `IS NULL` remain entirely in Flink because a missing field and an explicitly stored null need different treatment at the service boundary.

Index requirements now matter to queries that previously read everything.
On a Standard-edition database, an equality on one field combined with a range on another needs a composite index; ranges on two fields do too.
A missing index makes the job fail at startup with the service's message naming the index to create.
An Enterprise-edition database can answer those queries without an index.
For a Standard-edition table that cannot use the required indexes, this option fragment disables property-filter pushdown:

```sql
'scan.filter-push-down.enabled' = 'false'
```

Document-id equality and `IN` can still be pushed with that setting because they need no index.
The same setting also provides an escape for fields exempted from indexing.

Datastore mode has a different default.
A Datastore filter silently excludes an entity whose filtered property is unindexed, so property-filter pushdown is **opt-in** through `'scan.filter-push-down.enabled' = 'true'`.
Enabling it asserts that every property being filtered is indexed on every entity of the kind.
Key equality and `IN` are pushed without that opt-in.
A key `IN` also uses one split, even when property-filter pushdown is disabled.
With property pushdown enabled, a range, `IN`, `OR`, `<>` or `IS NOT NULL` makes the scan use one split, trading parallelism for fewer entities read.
The [Datastore SQL reference][datastore] describes its index requirements and supported predicates.

### Selecting a field inside a document

Firestore scans can now request individual fields inside a `ROW` column.
For a nullable `details ROW<region STRING, category STRING>` column holding an ordinary map, `SELECT details.region FROM orders` requests that field instead of the whole map.
This is useful when a document holds a large object but a report needs only one value from it.
Arrays and maps declared as `ARRAY` or `MAP` are still read whole, and nullability and special Firestore types can prevent narrowing a path.

The [recorded projection measurement][projection-evidence] used 100 documents with 5,000-key maps on both Standard- and Enterprise-edition databases.
Reading one field took a quarter to a fifth of the time of reading the whole map.
That measures a wide-map workload, not a general speedup for every scan.
This nested projection applies to Firestore Native-mode scans, including collection groups; it is not a new Datastore projection capability.

### Limiting a small inspection query

Both modes can push a limit directly above the scan:

```sql
SELECT * FROM orders LIMIT 10;
```

The limited scan runs one serial query instead of asking every partition for ten rows.
Planning reads and retries can add reads beyond the limit, so ten is not an exact billing promise.
Without `ORDER BY`, SQL does not specify which ten rows come back.

A `WHERE`, `ORDER BY`, `GROUP BY` or `DISTINCT` between the limit and the scan keeps the limit in Flink.
In particular, adding `LIMIT 10` to the earlier filtered query does not limit its service read, because Flink retains that filter even when the service also evaluates it.
For otherwise partitioned Datastore or Firestore collection-group scans, a large limit also gives up parallelism.

### Type checking after pushdown

These optimizations change the coverage of type checking.
A document excluded by a pushed filter, or a field omitted by projection, is no longer checked, so its bad value need not fail a query under `type-mismatch-policy = 'fail'`.
A narrow query therefore does not validate the whole collection.

## BigQuery file loads with background uploads

The `FILE_LOADS` sink writes staging files to Cloud Storage and loads them into BigQuery when the owning checkpoint completes, or at the end of batch input.
Previously, uploading a full chunk or closing a rolled file could hold the task thread while Cloud Storage answered.
Version 1.4.0 enables one background upload thread per writer, allowing the task thread to encode the next chunk while an upload is in progress.

The queue is bounded.
When its configured number of upload operations is occupied, the task thread waits for capacity.
A checkpoint drains the uploader before staged files can proceed to a commit.
Encoding and compression still run on the task thread, so this change helps a writer waiting on uploads rather than one whose CPU is already saturated.

These option fragments belong in the `WITH` clause of an existing BigQuery sink table:

```sql
'sink.write-method' = 'file-loads',
'sink.file-loads.background-staging-upload.enabled' = 'true',
'sink.file-loads.avro-compression' = 'zstd',
'sink.file-loads.zstd-level' = '3'
```

The uploader is already enabled by default; spelling it out makes the example's choice visible.
The new Avro compression option accepts `zstd` or `none`, and `zstd-level` controls the level for either Avro or Parquet staging.
The default remains Zstandard at level 3.
Compression trades CPU for uploaded bytes, so the [file-loads tuning guide][bigquery-fileloads] includes measurements for choosing a different level or turning it off.

The built-in serializers also let `FILE_LOADS` reuse the protobuf row message they construct instead of parsing serialized bytes back into a message.
A custom serializer can expose the same path through the new `ProtoRowMessageProvider` interface.
Its message and byte methods must describe the same row; the [serialization contract][bigquery-serialization] explains when the writer can use the message directly.

An in-region upload probe with one destination, 4 MiB upload chunks and two upload operations measured 1.35 times the median writer throughput with the uploader enabled, relative to the same setup with it disabled.
It drove one writer directly with roughly 9.5 KB nested rows and real Cloud Storage, preparing a commit every 128 MiB within each 512 MiB run, with five interleaved runs per configuration.
That probe measures the staging writer, not end-to-end BigQuery ingestion.
The [full measurements][uploader-evidence] separate destination counts and checkpoint finalization settings, which affect where a writer spends its upload time.

### Nested reads and encryption keys

The BigQuery Table source now pushes predicates on nested fields to the Storage Read API.
It can also read just `r.a` from a `ROW` column `r`, but Flink currently offers that nested projection only for queries without a `WHERE` clause.
A filtered query keeps whole top-level columns for Flink's remaining filter step.
The [BigQuery SQL reference][bigquery-table] describes the projection boundary.

Two new options select customer-managed encryption keys: `sink.table-create.kms-key-name` for tables the sink creates, and `scan.query-result-kms-key-name` for query results or view materialization.
They have corresponding DataStream builder methods.
The table-creation option does not rotate the key of an existing table, and staged Cloud Storage objects use the staging bucket's default key.
There is also a fix for `FILE_LOADS` with `WRITE_TRUNCATE`: replacing jobs now state the destination's existing customer-managed key, preserving it instead of silently switching to default encryption.
The [encryption guide][bigquery-encryption] covers the key's location and required permissions.

For BigQuery tables for Apache Iceberg, the release documents the write paths the service accepts, including Storage Write API writes and direct append loads.
Truncating or empty dispositions, load-job schema update options, overflow commits through temporary tables and CDC are refused by BigQuery.
The connector does not detect Iceberg tables; the [compatibility record][iceberg] explains those boundaries.

## Spanner lookup joins with a full cache

A small reference table that is joined frequently but changes slowly can be cheaper to load periodically than to read once per incoming key.
Spanner tables now accept `'lookup.cache' = 'FULL'`, which 1.3.0 rejected.
Each load reads one snapshot through the bounded scan, including its projection, filters and configured index, and subsequent lookups use the in-memory copy.

For an existing Spanner lookup table, these option fragments select a ten-minute reload interval:

```sql
'lookup.cache' = 'FULL',
'lookup.full-cache.reload-strategy' = 'PERIODIC',
'lookup.full-cache.periodic-reload.interval' = '10 min'
```

Leave `lookup.async` disabled: Flink has no asynchronous full-cache provider.
The cached rows can be indexed by top-level scalar join columns other than floating-point columns, so the join need not include Spanner's primary key.
A key matching several cached rows joins all of them.

The cost moves to reloads and memory.
Each TaskManager holds a copy for each lookup join using the table, shared by that join's subtasks.
The first load blocks lookups; a reload builds a new copy while the old copy serves requests, so memory must accommodate both.
Changes remain invisible until a later load, and a failed load fails subsequent lookups and causes the job to restart.
For larger tables, joins containing the complete Spanner primary key can use `PARTIAL` to load individual keys on demand.
The [Spanner full-cache guide][spanner-cache] covers snapshot settings, key types and reload behavior.

## Recovery and smaller improvements

Pub/Sub now has opt-in restore deduplication through `'scan.restore-deduplication.enabled' = 'true'`, or `restoreDeduplication(true)` on the source builder.
It records message ids covered by a checkpoint so that a restore can acknowledge their redeliveries without emitting them again when the checkpoint's completion notification was lost.
It does not make the source exactly-once: ordinary replay after the restored checkpoint, duplicates outside the restored ledger, its expiry and some partial-failover cases remain outside that protection.
The [Pub/Sub guide][pubsub-dedup] explains the scope, state cost and metrics.

Bigtable Table API scans, lookups and FULL-cache loads now ask the service for only the latest version of each cell, reducing transfer while retaining the table's existing result semantics.
Spanner sinks can tag transactions and exclude writes from change streams that permit transaction exclusion, through `sink.transaction-tag` and `sink.exclude-txn-from-change-streams`.

The Firestore Enterprise fix addresses missing data rather than performance.
In 1.3.0, a scan longer than one page could finish successfully after returning only part of its query because it relied on the service's implicit ordering.
The source now orders queries explicitly, and a 1.3.0 checkpoint resumes in that order.
This affects both DataStream reads and SQL scans; the default page size is 500 documents.

On Standard-edition databases, DataStream queries using `whereNotEqualTo(field, null)` no longer fail on the second page with `INVALID_ARGUMENT`.
On Enterprise-edition databases, DataStream `!=` and `!= null` queries now exclude documents that lack the filtered field, as those queries already did on Standard-edition databases.
The Table API's results are unchanged by this missing-field fix because Flink still evaluates every predicate after the read.

The release's deployed recovery evidence includes:

- **Firestore and Datastore**: one fault-sequence run injected a savepoint upgrade, JobManager deletion, TaskManager deletion, rescale in and rescale out. Both modes retained all 6,000 input keys and copied all 6,000 seed rows; the run also observed at-least-once duplicate deliveries. [Findings][firestore-recovery]
- **BigQuery FILE_LOADS**: four completed uploader-enabled trials covered upgrades, process replacements and rescales. Each run's ten destinations together contained all 1,843,200 expected records with no missing or duplicate sequences. [Findings][bigquery-recovery]
- **Pub/Sub**: three trials forced the lost-notification case during two kinds of scale change and a TaskManager termination. Each processed all 20,000 inputs without a duplicate, with covered redeliveries suppressed. [Findings][pubsub-recovery]

These are results from the recorded workloads and fault sequences.
They do not extend a connector's delivery guarantee or measure how often a production deployment encounters those failures.

## Upgrading from 1.3.0

### Public API compatibility and the Spanner commit delay

From 1.4.0, `@Public` and `@PublicEvolving` APIs may change incompatibly in a minor release, but not a patch release, and release notes identify those changes.
Many public signatures expose Google Cloud or protobuf types, so a compatibility promise also depends on those libraries.
`@Public` still marks the surface intended to remain stable; the [policy decision][compatibility] limits changes to dependency or service requirements, or API that no longer does what it says.

This release removes the Spanner sink's commit-delay API and its SQL option, `sink.buffer-flush.max-commit-delay`.
`SpannerWriterOptions.Builder#maxCommitDelay(Duration)`, `getMaxCommitDelay()` and `MAX_COMMIT_DELAY_LIMIT` are gone.
They never affected writes because the sink uses `BatchWrite`, whose request has no commit-delay field.
Remove those references before upgrading: source code using them will no longer compile, old compiled callers can fail with `NoSuchMethodError`, and SQL using the removed option fails during planning.

### Timestamp precision

Spanner reads now truncate a timestamp to the declared `TIMESTAMP_LTZ(p)` precision.
Version 1.3.0 kept all nine fractional digits regardless of that declaration.
A bare `TIMESTAMP_LTZ` means precision 6, so declare `TIMESTAMP_LTZ(9)` to preserve every digit; catalog-derived tables already do so.
This also affects lookup keys: values differing only beyond the declared precision can become equal, and a lookup can match several stored rows.

### Memory budgets and background work

In 1.3.0, a backlog could cause repeated job failures when flow control allowed more messages than the fixed subscriber-buffer budget and downstream processing was slow, for example with a `FILE_LOADS` sink and two-minute checkpoints.
The reader failed with `the attempted aggregate subscriber buffer was 10001 messages`, then encountered the same failure after restarting.

Version 1.4.0 fixes this mismatch by deriving the subscriber-buffer budget from flow control.
At client-library defaults it is 2,000 messages and 200 MiB per subscriber held by a reader, rather than 1.3.0's fixed 10,000 messages and 64 MiB per reader.
An explicit budget below twice the effective flow-control limit is rejected.
Raise or remove an explicit budget, or lower flow control to reduce the amount of data a slow downstream can hold.
The subscriber budget is not a bound on all source memory; the [tuning guide][pubsub-tuning] also accounts for records already handed to Flink's fetcher queues.

For BigQuery `FILE_LOADS`, the default upload-chunk sizing baseline rises from 64 MiB to 72 MiB per writer subtask, before encoding buffers and the rest of the task.
An upload failure can now surface at the next write, idle close or checkpoint instead of the call that closed the file.
Set `'sink.file-loads.background-staging-upload.enabled' = 'false'` to retain task-thread uploads.
If overriding the upload-operation limit, remove that override when disabling the uploader because the combination is rejected.

The Firestore index and type-checking changes described earlier also apply when upgrading an existing SQL job.

### Restoring state with a new job graph

Upgrade by stopping with a savepoint and resubmitting with the new connector, or use the Flink Kubernetes Operator's `upgradeMode: savepoint` or `last-state` application upgrade.
Version 1.4.0 can restore connector checkpoint state from 1.3.0, but its options objects no longer support older serialized job-graph forms.
Replacing the connector under a JobManager that recovers its persisted HA job graph can therefore fail or read settings incorrectly.
For a session cluster, stop its jobs with savepoints before updating the connector, then resubmit them.
Keep the connector release aligned between the client or SQL Gateway and the cluster, as the [upgrade guide][upgrade] describes.

If Pub/Sub restore deduplication is enabled, its checkpoint state cannot be read by releases before 1.4.0.
Before downgrading, disable it and let one checkpoint complete, then restore that compatible checkpoint.

### Pub/Sub watermark idleness

The Pub/Sub documentation no longer recommends adding `withIdleness(...)`.
The reported Flink timer behavior can mark a split idle while its records are delayed by slow downstream processing, allowing a watermark to pass them and event-time operators to discard them as late.
The [1.4 watermark-alignment guide][pubsub-watermarks] links the upstream issue and describes the alternative configuration and its limits.
Jobs following the earlier recommendation should review that section before retaining the setting.

### Dependencies and Java runtime

The release also updates Google Cloud `libraries-bom` from 26.91.0 to 26.93.0.
The release build now runs on JDK 21 with Java 17 bytecode targeting; this does not make a Java 11 Flink runtime compatible.
For Flink 1.20 containers, use `flink:1.20-java17` rather than the Java 11 `flink:1.20` image.
The [release notes][release] carry the complete change list and upgrade details.

---

This is an independent open-source project.
It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.

[release]: https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.4.0
[docs]: https://flink-gcp.github.io/flink-connector-gcp/1.4/
[source]: https://github.com/flink-gcp/flink-connector-gcp/tree/v1.4.0
[previous]: https://blog.laughingman7743.org/posts/flink-connector-gcp-1-3-0/
[firestore]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/table/firestore/
[datastore]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/table/datastore/
[projection-evidence]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md
[bigquery-fileloads]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/datastream/bigquery/#file-loads
[bigquery-serialization]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/datastream/bigquery/
[uploader-evidence]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/evidence/0195-fileloads-staging-uploader-2021.md
[bigquery-table]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/table/bigquery/#source
[bigquery-encryption]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/datastream/bigquery/#customer-managed-encryption-keys
[iceberg]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/0202-the-bigquery-sink-writes-iceberg-managed-tables-without-detecting-them.md
[spanner-cache]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/table/spanner/#full-lookup-cache
[pubsub-dedup]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/datastream/pubsub/#restore-deduplication
[firestore-recovery]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/evidence/0204-firestore-fault-sequence-findings-2147.md
[bigquery-recovery]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/evidence/0195-fileloads-uploader-recovery-2034.md
[pubsub-recovery]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/evidence/0198-pubsub-restore-deduplication-trial-findings-2127.md
[compatibility]: https://github.com/flink-gcp/flink-connector-gcp/blob/v1.4.0/docs/adr/0199-public-api-compatibility-follows-the-google-cloud-libraries.md
[pubsub-tuning]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/datastream/pubsub/#tuning
[upgrade]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/#upgrading-the-connector
[pubsub-watermarks]: https://flink-gcp.github.io/flink-connector-gcp/1.4/docs/connectors/datastream/pubsub/#watermark-alignment
