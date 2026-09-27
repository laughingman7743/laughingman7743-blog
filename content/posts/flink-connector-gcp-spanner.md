+++
title = "The Spanner connector in flink-connector-gcp, part three of the connector series"
description = "This is the third post in the flink-connector-gcp series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. The release post introduces the project, and..."
date = 2026-09-04T00:16:31+09:00
updated = 2026-09-05T21:15:04+09:00
draft = false

[taxonomies]
tags = ["GCP", "Flink", "Spanner"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/09/04/001631"
+++
This is the third post in the [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp) series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. [The release post](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-0-0/) introduces the project, and the earlier posts cover [BigQuery](https://blog.laughingman7743.org/posts/flink-connector-gcp-bigquery/) and [Pub/Sub](https://blog.laughingman7743.org/posts/flink-connector-gcp-pubsub/).

This post follows Spanner changes into BigQuery, both as a current-row replica and as append-only history. It also covers the move from JDBC to the native client, change-stream state, mutation writes, snapshot reads, lookup joins, and batch limits.

- Documentation: <https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/spanner/>
- Maven: `io.github.flink-gcp:flink-connector-gcp-spanner:1.0.0` (`1.0.0-1.20` for Flink 1.20); for the SQL client, the `flink-sql-connector-gcp-spanner` uber-jar from the [release page](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.0.0) goes into `lib/`

## From a JDBC dialect to the native client

I first tried to support Spanner through Flink's JDBC connector. In February 2025, I submitted a dialect and catalog upstream ([apache/flink-connector-jdbc#156](https://github.com/apache/flink-connector-jdbc/pull/156), FLINK-37288). I appreciate the review feedback it received, but at the time of writing it remains open and unmerged. I understand that committer time is scarce; I still needed a working pipeline.

The native Java client began as a way around that delay, but it enabled two features that could not have fit Flink's JDBC connector. The first is change streams. Spanner serves them through long-lived partition queries carrying changes, heartbeats and child-partition tokens. A source must manage concurrent queries and their lineage, starting children after their parents finish. That lifecycle fits a FLIP-27 coordinator rather than a bounded JDBC scan.

The second is snapshot partitioning. Flink's JDBC source divides a scan using a configured column and bounds, then runs separate queries without a shared snapshot. Spanner's native batch read API lets the enumerator open one batch read-only transaction and ask the service to partition it. Each server-planned partition becomes a Flink split, and every subtask rejoins the same transaction.

All subtasks therefore read one consistent snapshot. Spanner chooses the partitions from its physical storage layout, removing the need to select a split column and bounds in the job. I value having fewer ways to misconfigure a scan.

The module provides a bounded snapshot source, an unbounded change-stream source and an at-least-once mutation sink through both DataStream and Flink SQL APIs. SQL also supports primary-key lookup joins. The same code supports GoogleSQL and PostgreSQL; in the DataStream API, the database determines the dialect rather than a builder option. These examples use GoogleSQL.

## Spanner to BigQuery replication in one SQL job

The pipeline promised in the Pub/Sub post connects operational data in Spanner to analytics in BigQuery. One Flink SQL job consumes a Spanner change stream and writes its upsert changelog through the BigQuery CDC sink. Current row contents flow continuously into BigQuery, with checkpoints governing progress instead of a batch export schedule. No intermediate Kafka pipeline is required.

The Spanner table needs a change stream that captures new row values. I wrote this DDL to match the Flink schema below:

``` sql
CREATE TABLE Orders (
  OrderId  INT64 NOT NULL,
  Customer STRING(128),
  Status   STRING(32)
) PRIMARY KEY (OrderId);

CREATE CHANGE STREAM order_changes FOR Orders
OPTIONS (value_capture_type = 'NEW_ROW_AND_OLD_VALUES');
```

The Flink job uses three statements copied verbatim from the [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigquery/#spanner-cdc-from-either-route). CI validates the site's Flink SQL through the planner and executes its GoogleSQL examples against a Spanner emulator. The source table exposes three ordering coordinates as metadata:

``` sql
SET 'execution.checkpointing.interval' = '1 min';

CREATE TABLE order_changes (
  OrderId BIGINT,
  Customer STRING,
  Status STRING,
  commit_timestamp TIMESTAMP_LTZ(9) METADATA FROM 'commit-timestamp' VIRTUAL,
  record_sequence STRING METADATA FROM 'sequence' VIRTUAL,
  mod_number INT METADATA FROM 'mod-number' VIRTUAL,
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
  'scan.startup.mode' = 'latest'
);
```

The BigQuery destination accepts those coordinates as one row of writable metadata:

``` sql
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
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min'
);
```

The `INSERT` connects them:

``` sql
INSERT INTO current_orders
SELECT OrderId, Customer, Status,
       ROW(commit_timestamp, record_sequence, mod_number)
FROM order_changes;
```

The DDL controls how the source represents changes and how BigQuery orders them.

This pipeline requires `upsert` mode. It emits keyed `INSERT` and `UPDATE_AFTER` rows and key-only `DELETE` rows, matching the BigQuery CDC sink's contract. The alternative, `full`, reconstructs complete retract rows and emits `UPDATE_BEFORE` / `UPDATE_AFTER` pairs. BigQuery's CDC sink rejects update-before rows. Other downstream operators, such as aggregations over a changing table, can use them.

The Spanner capture type must also match. `upsert` needs complete after-images, supplied by `NEW_ROW` and `NEW_ROW_AND_OLD_VALUES`. `OLD_AND_NEW_VALUES` contains only changed columns and cannot supply the complete row. The source validates each data-change record against the DDL before emitting rows. An incompatible capture type therefore fails deserialization instead of producing partial rows.

Checkpointing persists the change-stream position and flushes the BigQuery default stream. It therefore carries the pipeline's at-least-once guarantee. Without it, there is no saved position, and a restart begins at the configured start position again.

The commit timestamp uses `TIMESTAMP_LTZ(9)` to retain nanosecond precision, so this table declares no watermark. BigQuery's `_CHANGE_SEQUENCE_NUMBER` encodes the three source coordinates, beginning with that timestamp. Flink watermark columns support only millisecond precision; truncating here could make two changes within one millisecond equal in the first sequence section.

For jobs that prioritize event-time processing over replication fidelity, the docs instead show `TIMESTAMP_LTZ(3)` with `SOURCE_WATERMARK()`. This replication example preserves nanoseconds.

The sequence coordinates let BigQuery handle replays and out-of-order arrival. Where they order two changes, an older replay cannot overwrite the newer value. This makes an at-least-once transport usable for the replica.

They cannot create a total order that Spanner does not expose. Transactions updating disjoint column sets of one row can have equal coordinates. BigQuery resolves those ties by ingestion order.

The source starts at `latest`, so a fresh job captures changes committed after startup. An already populated table needs an initial snapshot followed by a handoff to a timestamp start; that bootstrap is outside this example. The result is an analytics replica of current row contents, without a byte-for-byte or transactionally consistent copy of the Spanner database.

Install `flink-sql-connector-gcp-spanner` and `flink-sql-connector-gcp-bigquery` in the SQL client's classpath. Their dependencies are relocated so both uber-jars can share one `lib/`.

## Keeping every change instead of the latest row

The replica replaces each key's previous state. Other analytics needs that history: what changed and when, how often an order changed status, how long transitions took, or the input for a slowly changing dimension. The change stream already carries those events. To keep them, the job appends them to BigQuery instead of overwriting the previous row.

This history pattern requires a DataStream job in this connector. SQL's `scan.mode = 'change-stream'` exposes a changelog, but SQL expressions cannot access row kind to turn each change into a plain inserted log record. An append-only destination therefore rejects the updating query during planning.

The DataStream deserializer receives changes before the Table layer interprets them as a changelog. Each typed `DataChangeRecord` contains the commit timestamp, table, modification type, transaction id and record sequence, plus each mod's keys, new values and old values as normalized JSON. Emitting one row per mod makes these fields ordinary data columns.

Those rows can use BigQuery's append path from the first post: the at-least-once Storage Write API method, or `FILE_LOADS` for free ingestion at higher volumes. The commit timestamp is a natural time-partitioning column.

Capture semantics determine the contents. With `NEW_ROW_AND_OLD_VALUES`, inserts and updates include the complete new row, preserving successive states. Old values cover the changed columns, so finding what changed can use a column filter instead of a self-join with the previous row. Deletes have their own modification type and close that key's history.

Delivery remains at-least-once, so the history can contain duplicates. Each output row has an identity: server transaction id, record sequence and mod number, the mod's zero-based position in the record. Queries or a scheduled `MERGE` can deduplicate on that identity. This path needs neither the CDC sink nor sequence metadata because it does not overwrite earlier records.

## Where the change-stream source keeps its state

Spanner divides a change stream into partitions that split and merge over time. Each partition is a streaming query whose child-partition records identify the tokens to read next. The source must track this lineage, retaining every token and waiting for all parents before reading a child. Where that state lives determines much of the connector's operational work.

This connector keeps the partition ledger entirely in Flink checkpoints. The coordinator begins with the null partition token and schedules a child only after every parent naming it has finished. It checkpoints the unfinished topology. Finished parents remain as compact proofs until their children can be scheduled, then are removed.

No metadata table is created, and progress tracking needs no write permission. The workload principal needs database read access, `spanner.databases.select` and the client's session permissions. A checkpoint restore brings the partition ledger back with the job.

Apache Beam's `SpannerIO.readChangeStream`, used by Google's Dataflow templates, instead creates a partition metadata table in a designated Spanner database. I checked this against Google's Dataflow connector documentation on 2026-09-03. That table is an additional stateful resource to provision, grant write access to and clean up.

The source uses each record's Spanner commit timestamp as its Flink event timestamp. The coordinator owns one watermark: the minimum across all unfinished partitions, including discovered, queued and running partitions. It broadcasts that value to every reader.

Quiet partitions advance through heartbeats, with a default interval of two seconds and a configurable range from one second to five minutes. They are not marked idle, because removing them from the minimum could make later records from those partitions late.

Heartbeat conversion subtracts one millisecond after truncation. A later nanosecond timestamp can fall in the same Flink millisecond, so the subtraction prevents an on-time record from being classified as late. These are the source-provided timestamps and watermarks. A job that supplies its own watermark strategy chooses that strategy's behavior instead.

The source provides at-least-once delivery. A reader checkpoints each partition's greatest consumed record timestamp and resumes inclusively at that timestamp. Several records can share it; starting after it could skip one. Records at the recovery boundary can therefore repeat.

Deduplicate on server transaction id and record sequence where uniqueness is required. Commit timestamps alone are not unique. In the earlier replication example, sequence metadata handles those boundary replays.

The change stream retains records for the period configured in its DDL. Restoring an expired partition position fails the job by default. The explicit opt-in `scan.resume-fallback.mode` discards the entire stale ledger and starts fresh, accepting loss of the unavailable interval. I prefer to make that availability-versus-history decision in the job configuration.

Before releasing readers, the coordinator reads stream metadata and logs its watch scope, effective retention and value-capture type. It warns about an explicit watched-column list, since newly added columns will not be included automatically, and rejects unsupported partition modes at startup.

The change-stream API is `@PublicEvolving` and may change in a minor release, with changes announced in release notes. The rest of the connector is frozen by the japicmp gate described in the release post.

## A sink where the mutation names its own table

The sink is configured with a database. Its serializer returns a Spanner `Mutation`, which names the target table, so one sink can write to every table the serializer chooses. The other four connectors take destination resolvers on their builders. Spanner needs none because the mutation already carries the routing information.

The sink calls `batchWriteAtLeastOnce` with one mutation per mutation group. Spanner reports status per group, so each rejection identifies one input record. The sink can retry, route or fail that record individually. A plain commit would reject the entire batch for one bad row.

The Spanner client does not retry the batch-write RPC: its generated settings have an empty retryable-code set. The sink therefore owns the retry loop. It retries `ABORTED`, `UNAVAILABLE`, `DEADLINE_EXCEEDED` and `RESOURCE_EXHAUSTED` within its own budget.

A retry includes groups with transient failures and groups for which the service returned no status, as can happen when the response stream fails partway through. Groups confirmed as applied are not resent. The sink exposes this work through `mutationsRetried` and transient-error counters, which is less visible when sibling connectors rely on SDK retries.

The configurable failure handler normally receives only `ALREADY_EXISTS`, such as a replayed insert or unique-index collision, and `INVALID_ARGUMENT`. Schema violations fail the job by default: a `NULL` in a `NOT NULL` column, an over-long value, or a foreign-key or `CHECK` failure. These often indicate a faulty mapping affecting every record of that shape. Dropping records individually could conceal the mapping bug.

System state can also cause the same status family. If a database's CMEK key is disabled or unreachable, writes fail with `FAILED_PRECONDITION`. Routing that status to a dropping handler could discard the whole stream during a key incident.

For occasional schema-invalid input, `constraintViolationPolicy(ROUTE_TO_FAILURE_HANDLER)` makes those statuses eligible for dropping or dead-lettering. The default preserves records for source replay after restart. The opt-in allows progress by routing the rejected records.

Spanner's batch write has no replay protection: the service documents that a mutation may be applied more than once. The serializer's chosen operation determines the effect:

| Operation | Same mutation replayed |
|----|----|
| `insertOrUpdate`, `replace` | Idempotent |
| `delete` | Idempotent; deleting an absent row is simply applied |
| `insert` | Refused with `ALREADY_EXISTS`, routed to the failure handler |
| `update` | Idempotent, unless the row was deleted in between: then `NOT_FOUND` fails the job |

`NOT_FOUND` cannot distinguish a missing row from a missing table. Routing it could silently discard every record when the table name is wrong. For updates that may target absent rows, I use `insertOrUpdate`.

SQL selects the operation from the DDL. A declared primary key produces an upsert sink using `insertOrUpdate`, with deletes built from the declared key columns. A keyless table accepts only insert input and uses `insert`, preserving duplicate-key errors rather than assuming an unknown physical key supports upserts.

Idempotence of an individual mutation does not guarantee the latest value wins. The writer uses separate mutation groups, which Spanner may apply in an unspecified order. Successive writes to one key can therefore finish out of input order. This affects pipelines writing to Spanner; the earlier replication example writes to BigQuery.

## Reading a table at one snapshot

The bounded source reads one database snapshot and finishes. It can run inside a streaming pipeline, for example to load a Spanner table for a join with an unbounded stream. Server-planned partitions all rejoin the same batch transaction, so every subtask reads the same consistent snapshot.

Snapshot reads provide at-least-once delivery with a duplicate window of one partition. Partitioned-query row order is not contractual, so the source cannot resume at a position within a partition. Checkpoints record the partitions still held by a reader, and recovery rereads them from the start.

A partition cancelled by Flink during a running job is also reread. The `partitionsReread` metric can therefore explain duplicates even when the job has not failed. A pipeline requiring unique rows should deduplicate downstream on the primary key.

The snapshot expires after the database's `version_retention_period`, one hour by default and configurable up to a week. A backfill must finish within that period or use a database whose retention was raised beforehand. A savepoint cannot resume an expired snapshot.

Two options reduce a backfill's impact on serving traffic. `rpcPriority(LOW)` costs nothing extra: reads stay on the instance's compute but are shed first at capacity. Data Boost, enabled with `dataBoostEnabled(true)`, uses separate billed compute. It needs `spanner.databases.useDataBoost`, which is absent from `roles/spanner.databaseReader`, and has its own concurrency quota.

In August 2026, the gated suite exercised Data Boost end to end on a 100-processing-unit `STANDARD` instance, the cheapest edition. That run confirmed that a Data Boost backfill does not require an edition upgrade from `STANDARD`.

SQL scans push projection into the requested columns and convert predicates on consecutive primary-key columns to exact key ranges. Flink evaluates the remaining predicates. `scan.index` selects a secondary index, checked against live metadata at the scan snapshot. Broader filter pushdown is planned for the [v1.1.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/10).

## Lookup joins from plain SQL

Lookup joins are the read feature I use most often. An event carries an account id, and a Spanner table holds the account's current data. A processing-time temporal join enriches the event through a point read, with a cache in front, without exporting the table. The Spanner dimension table has a composite primary key:

``` sql
CREATE TABLE accounts (
  region STRING(16) NOT NULL,
  account INT64 NOT NULL,
  name STRING(128)
) PRIMARY KEY (region, account);
```

The Flink job declares that table and joins it to events from Pub/Sub. This combines two connectors from the series. The example adapts the [source-backed lookup example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/spanner/#joining-a-composite-key-lookup-table), validated in CI; I added the Pub/Sub event stream:

``` sql
CREATE TABLE order_events (
  account   BIGINT,
  region    STRING,
  amount    INT,
  proc_time AS PROCTIME()
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'account-events-sub',
  'format'       = 'json'
);

CREATE TABLE accounts (
  region  STRING,
  account BIGINT,
  name    STRING,
  PRIMARY KEY (region, account) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project'   = 'my-project',
  'instance'  = 'my-instance',
  'database'  = 'orders-db',
  'table'     = 'accounts',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

SELECT e.region, e.account, e.amount, a.name
FROM order_events AS e
LEFT JOIN accounts FOR SYSTEM_TIME AS OF e.proc_time AS a
  ON e.account = a.account AND e.region = a.region;
```

The join equality must include every declared primary-key column because a point read needs the complete key. Composite keys are encoded in `PRIMARY KEY` declaration order, regardless of the join-predicate order. That declaration must match the physical Spanner primary key or the lookup will address a different key.

The `WITH` options control lookup behavior. `lookup.async` selects asynchronous rather than synchronous point reads. The lookup cache uses Flink's standard `NONE` and `PARTIAL` modes, with expiry, size and missing-key settings. I generally use `PARTIAL` with a write expiry such as the example's ten minutes. Frequently read keys stay cached, at the cost of up to that much staleness.

The source rejects `FULL` because a scan-backed cache would require snapshot and refresh semantics it does not provide. The next connector in the series, Bigtable, offers that mode.

`lookup.max-retries` retries `ABORTED`, `DEADLINE_EXCEEDED` and `UNAVAILABLE`. Of these, the client library retries only `UNAVAILABLE`, so the connector option supplies further attempts for the other two. It excludes `RESOURCE_EXHAUSTED`: the client already retries that status while respecting the server-requested delay, which an immediate connector retry would undermine.

## Batch limits that are correctness bounds first

A request-level rejection affects every mutation in the request, so the sink must bound each batch. It flushes when any of three limits is reached. The defaults, inherited from Apache Beam, are 5,000 cells, 500 mutations and 1 MiB, all well below service limits.

`maxBatchCells` accounts for secondary indexes. Spanner counts one cell for the table column plus one for each index containing it. The sink reads `INFORMATION_SCHEMA` when the writer opens and uses that schema to estimate each mutation's cost. This requires `spanner.databases.select` in addition to write access.

The 5,000-cell default is sixteen times below Spanner's published 80,000 per-group figure. That headroom covers undercounting for tables created after startup or hidden from the writer's role, whose index entries cannot be included. Raising the limit consumes that margin.

`maxBatchBytes` protects the request-level size limit. Google's documentation can be read as specifying either 10 MiB or 100 MiB, so the gated suite tested oversized requests. The service accepted roughly 12 MiB and rejected roughly 110 MiB, naming a limit of 104,857,600 bytes, exactly 100 MiB.

The connector estimates size because the client cannot report a mutation's wire size, and the estimate generally reads low. `BYTES` is counted at its base64 length: Spanner wraps it in a protobuf `Value`, which has no bytes kind. The measurement showed 83,886,080 raw bytes becoming 111,852,884 on the wire, roughly four thirds of the input.

Raising one limit may have no effect if another still binds first. For example, `maxBatchMutations` cannot be reached above `maxBatchCells`, because every mutation costs at least one cell. Building those options logs a warning naming both values where the job's `main` runs. Compare `bufferedCells` and `bufferedBytes` when tuning to see which limit triggers the flush.

`batchWriteTimeout` bounds a complete attempt. Its default is 30 seconds, replacing the client's one-hour timeout for this RPC only. Including retries, the default worst case is 369.375 seconds: ten 30-second attempts and up to 69.375 seconds of jittered backoff.

A checkpoint may encounter that loop twice. A record-triggered flush can already be running when the barrier arrives, followed by another invocation for the checkpoint flush. Choose the checkpoint timeout to cover that work, alignment and the rest of the job.

Change-stream capacity is source parallelism multiplied by `scan.max-concurrent-queries-per-subtask`, default 8. This bounds the connector's open partition queries; it is not a Spanner quota.

If `activeChangeStreamQueries` stays at that product while queued or unassigned partition lag grows, increase parallelism or the per-subtask bound. Alert when either lag approaches stream retention, since expired records cannot be recovered. The per-subtask bound changes reader capacity. Changing parallelism requires restarting from a checkpoint or savepoint so Flink can redistribute ownership.

## Testing, and what the emulator cannot show

The weekly gated real-GCP suite establishes service behavior. Spanner's emulator is useful but has specific differences. It supports the sink's `BatchWrite` RPC only from v1.5.31; older versions return `UNIMPLEMENTED`. The repository therefore pins its image separately from the other connectors' emulator bundle.

The emulator also serializes concurrent read-write transactions. In measured reads, it planned exactly two partitions for every table, ignored both partition hints, and applied a stricter partitionability check with a less useful error message than the service. The [connector page](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/spanner/#reading-against-the-emulator) records the full deviation table measured on 2026-08-10.

The sink's rejection-status table was measured against both environments, including `NOT NULL` violations, missing tables and replayed inserts. The gated suite asserts each result so changes on either side are detected.

Each test class creates a regional 100-processing-unit `STANDARD` instance and deletes it afterwards. Names include creation time, allowing the next run and a scheduled sweep to reclaim instances left by interrupted tests. Instances are not retained between runs because they bill for their entire lifetime.

The measured change-stream recovery run delivered all 5,000 unique mutations after an intentional failure following a checkpoint. It also delivered 500 repeats at the inclusive recovery boundary, matching the documented duplicate window. A subsequent savepoint restore consumed a mutation written while the job was stopped.

## What's next for this connector

The [v1.1.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/10) extends bounded-scan filter pushdown. Further work is driven by reports, especially from workloads unlike mine: <https://github.com/flink-gcp/flink-connector-gcp/issues.>

The next post covers Bigtable: lookup joins with a full-table cache, change streams, and the write extensions planned for v1.1.0.

------------------------------------------------------------------------

*This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.*
