+++
title = "The BigQuery connector in flink-connector-gcp, part one of the connector series"
description = "This is the first of five posts about flink-connector-gcp, the Apache Flink connectors for Google Cloud I released as 1.0.0. The release post explains the motivation, supported..."
date = 2026-09-02T00:11:29+09:00
updated = 2026-09-05T20:55:15+09:00
draft = false

[taxonomies]
tags = ["GCP", "Flink", "BigQuery"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/09/02/001129"
+++
This is the first of five posts about [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp), the Apache Flink connectors for Google Cloud I released as 1.0.0. [The release post](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-0-0/) explains the motivation, supported Flink versions, and testing.

This post covers BigQuery's three write methods, a multi-tenant ingestion pipeline, the bounded source, and defaults informed by measurements against the real service.

- Documentation: <https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/bigquery/>
- Maven: `io.github.flink-gcp:flink-connector-gcp-bigquery:1.0.0` (`1.0.0-1.20` for Flink 1.20)

When I surveyed the [Dataproc-maintained BigQuery connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector) in July 2026, it was actively developed but lacked two features I needed. Each sink wrote to one fixed table, and schema changes required a job redeployment. Dynamic destinations and schema evolution became central to this connector.

## One builder, three write methods

The sink uses one builder, following the style of Beam's `BigQueryIO`. A call to `writeMethod(...)` on `BigQuerySink.builder()` selects one of three implementations when the job graph is built. Destination, serializer, table creation, schema updates, and failure-handler settings stay on that builder. Switching from streaming inserts to batch loads therefore changes configuration without rewriting the pipeline.

``` java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

The three methods trade visibility latency against delivery guarantee and ingestion cost:

|  | Visibility | Delivery | Ingestion price |
|----|----|----|----|
| `STORAGE_API_AT_LEAST_ONCE` | seconds (as appends succeed) | at least once | volume-based Storage Write API pricing |
| `STORAGE_API_EXACTLY_ONCE` | per checkpoint | exactly once | volume-based Storage Write API pricing |
| `FILE_LOADS` | per checkpoint | exactly once | load jobs are free on the shared slot pool |

`STORAGE_API_AT_LEAST_ONCE` writes through the Storage Write API's default stream. At every checkpoint, the writer flushes pending batches and waits for all in-flight appends before passing the barrier. A completed checkpoint therefore means BigQuery has acknowledged every preceding record. No sink buffer remains in Flink state.

The project evaluated [FLIP-171](https://cwiki.apache.org/confluence/spaces/FLINK/pages/181309759/FLIP-171+Async+Sink)'s `AsyncSinkBase` for this sink and the other connectors. Its `AsyncSinkWriter` stores unflushed buffers in writer state rather than flushing them at the barrier. Redeploying without that state loses those buffered records. I chose to flush at the barrier so a savepoint-less redeploy risks duplicates instead of loss. The Storage Write API SDK already provides the in-flight window needed for backpressure, so the sink did not need that machinery from the base class.

`STORAGE_API_EXACTLY_ONCE` uses application-created buffered streams and a two-phase protocol. Writers append rows at explicit offsets, where they remain invisible. When a checkpoint completes, the committer calls `FlushRows` up to that checkpoint's offset.

Each writer reuses one buffered stream per active destination across checkpoints. Google's guidance discourages calling `CreateWriteStream` at checkpoint frequency, and the API has a quota of 10,000 calls per hour per project and region.

`FlushRows` is naturally idempotent: flushing an already-flushed offset returns `ALREADY_EXISTS`. A restarted committer can therefore retry without deterministic-ID machinery and can run at the sink's full parallelism.

The two modes have different failure risks. At-least-once keeps the sink ahead of source acknowledgment, so discarding operator state can duplicate rows but cannot lose them. Exactly-once puts visibility and source acknowledgment in the same commit phase without an atomic operation spanning both. Discarding state during a redeploy can then lose up to one checkpoint's data.

This limitation is inherent to two-phase commit; a Kafka exactly-once producer has the same risk. The sink cannot detect discarded state because a writer restored without state looks like a new job. Redeploy exactly-once jobs through savepoints. With the Flink Kubernetes Operator, use `upgradeMode: savepoint` or `last-state`, never `stateless`.

The later `FILE_LOADS` section covers its ingestion costs and checkpoint-related quotas.

## A multi-tenant pipeline, end to end

The pipeline that motivated these features receives events from many tenants and writes to one BigQuery table per tenant. Tenants can appear or disappear without a job redeployment. That requires per-record routing, table creation on first write, schema evolution, and a destination for unroutable records. The sink combines all four:

``` java
Map<String, TableDestination> tablesByTenant = new HashMap<>();

BigQuerySink.<OrderEvent>builder()
        .destinationResolver(
                (event, context) -> {
                    if (!event.hasKnownTenant()) {
                        return UnroutableRecord.of(event.deadLetterPayload(), "Unknown tenant");
                    }
                    return tablesByTenant.computeIfAbsent(
                            event.tenantId(),
                            id -> TableDestination.of("my-project", "my_dataset", "orders_" + id));
                })
        .serializer(new OrderEventProtoSerializer())
        .tableCreateOptions(
                TableCreateOptions.builder()
                        .timePartitioning(TimePartitioningType.DAY, "created_at")
                        .timePartitioningExpiration(Duration.ofDays(90))
                        .clusteredFields(List.of("customer_id"))
                        .build())
        .schemaUpdateOptions(SchemaUpdateOptions.builder().allowNewFields().build())
        .failureHandler(
                FailureHandler.sendToDeadLetterQueue(
                        PubSubDeadLetterQueue.builder()
                                .topic(TopicDestination.of("my-project", "dead-letters"))
                                .build()))
        .build();
```

This composite adapts the [source-backed examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/dynamic-destinations/), which CI compiles. The combination is mine.

**Routing.** The resolver runs once per record, before serialization, on the writer's hot path. It must be serializable, deterministic and cheap. Caching `TableDestination` values, as above, is the documented pattern.

Each active destination has its own writer state, including a stream writer for the Storage Write API methods. Destinations are evicted after an idle timeout, one hour by default, so tenant churn does not leave connections accumulating indefinitely.

For a record it cannot place, the resolver returns an `UnroutableRecord` containing a payload and reason. That result reaches the failure handler. A bare `null` is always fatal, preventing a drop policy from concealing a routing bug.

**Table creation.** With the default `CREATE_IF_NEEDED`, the first record for a missing table creates it. The serializer supplies the schema; `tableCreateOptions(...)` supplies partitioning and clustering. HTTP 409 is treated as success when parallel subtasks race to create the table.

Heavy contention can also hit BigQuery's per-table metadata-update quota and return 403 `rateLimitExceeded`. In August 2026, the project raced sixteen creations against one absent table and saw five rate-limited responses. The connector retries these within its recovery backoff budget, allowing concurrent creation to back off instead of failing the job immediately.

A missing table returns an unexpected status. Opening a Storage Write API stream against it gives `PERMISSION_DENIED`, because BigQuery masks table existence from callers who might be probing names. The permission named in the message varies by path, so the connector checks the status code, never the text.

The [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) returns `NOT_FOUND` instead. Emulator tests therefore do not exercise this recovery path. In this project, auto-creation had never worked against the real service until direct measurements exposed the difference in August 2026. The documentation records dates and trial counts. That experience is why I use a weekly real-GCP suite to establish service behavior.

**Schema evolution.** The sink handles changes in either direction without a restart. If someone changes the destination schema through DDL, the default stream's append response reports the new schema. The writer rebuilds its connection with a fresh descriptor.

If the serializer's schema extends the table's schema, `schemaUpdateOptions(...)` can allow the sink to update the table. It reads the live schema, combines it with the serializer's schema, and submits an etag-conditioned update. The union only widens: new fields are appended as `NULLABLE`; existing fields are not removed, reordered or retyped. Concurrent unions converge, so parallel subtasks need no additional coordination.

In six of seven instrumented real-service runs, an update reached the write backend in about 35 seconds. The writer keeps retrying affected batches during propagation. One run took much longer. In a checkpointed job, the checkpoint timeout bounds that wait by triggering a restart.

Schema updates are opt-in because an unexpected field in the serializer's schema can alter a live table. Enabling `allowNewFields()` means trusting that schema. The connector's additive schema union does not remove fields, so unwanted additions require a separate cleanup. BigQuery supports [explicit column deletion with `ALTER TABLE DROP COLUMN`](https://docs.cloud.google.com/bigquery/docs/managing-table-schemas#delete_a_column). By default, a schema-mismatch append fails the job for investigation.

**The dead letter path.** `PubSubDeadLetterQueue` publishes failed elements to a Pub/Sub topic. Attributes identify the connector, intended destination or `unresolved` for routing failures, error, timestamp and subtask.

All connectors share the failure-handler contract, so one dead-letter implementation can serve a job's BigQuery, Bigtable and Pub/Sub sinks. Delivery is at-least-once. The implementation deliberately does not create the topic: a newly created dead-letter destination may have no consumer.

## The same pipeline on free ingestion

Both Storage Write API methods use volume-based ingestion pricing. With `FILE_LOADS`, writers stage each destination's rows in Cloud Storage, using zstandard-compressed Avro by default. At every checkpoint, a committer submits BigQuery load jobs. These jobs are free on the shared slot pool, although Cloud Storage staging still costs money and shared-pool capacity is not guaranteed. Paid `PIPELINE` slots are available when capacity must be reserved.

Rows become visible after the checkpoint's load completes, typically minutes rather than seconds. A high-volume pipeline that accepts that latency can switch with two builder calls and a suitable checkpoint interval:

``` java
env.enableCheckpointing(300_000); // 5 minutes; see the quota arithmetic below

BigQuerySink.<OrderEvent>builder()
        .writeMethod(WriteMethod.FILE_LOADS)
        .destinationResolver(...)   // unchanged
        .serializer(serializer)     // unchanged
        .fileLoadsOptions(
                FileLoadsOptions.builder()
                        .stagingPath("gs://my-staging-bucket/flink-loads")
                        .build())
        .build();
```

`FILE_LOADS` provides exactly-once delivery through precise file references and deterministic job IDs. Each load names the exact file URIs emitted by the writers, never a bucket prefix that could include files from failed attempts. The job ID hashes the destination and file list. After a crash, a retry reattaches to the existing load job instead of submitting a second load.

Committables remain in Flink's committer state until their loads succeed. The referenced files are therefore part of the recoverable data. Use a dedicated bucket for `stagingPath`, with a lifecycle age longer than the longest outage the job must recover from. If the rule deletes files still referenced by a checkpoint, restoring that checkpoint leaves the loads permanently failing.

`FileLoadsOptions` selects the staging format. Avro is the default; Parquet is opt-in and requires `parquet-avro` plus a Hadoop runtime for any compression. The connector does not ship these dependencies. It checks for them when building the job graph and names any missing artifact in the client-side error.

BigQuery rejects Parquet load jobs whose schema contains a `JSON` column, regardless of the file contents. For such destinations, the connector overrides the format to Avro and logs that choice once per destination.

Both formats use the same derived Avro schema, so both reject `INTERVAL`, `RANGE` and BigQuery flexible column names. The tuning section includes the measurement used to evaluate Parquet's performance.

The checkpoint interval determines load-job quota use. A standard table allows 1,500 modifications per day from load, copy and query jobs combined. Each checkpoint that commits files uses at least one modification per active destination:

| Checkpoint interval | Modifications per destination per day        |
|---------------------|----------------------------------------------|
| 1 min               | 1,440, too close to the ceiling to be viable |
| 2 min               | 720                                          |
| 5 min               | 288                                          |

Multiple destinations also consume the project quota. At a 3-minute interval, one destination needs 480 load jobs per day. Two hundred active destinations use about 96,000 of the project's 100,000 daily load jobs, leaving little room for retries or other workloads.

The connector rejects intervals below 2 minutes when building the graph and warns below 5 minutes. Lowering that guard requires an explicit opt-in for short-lived jobs. Pipelines needing visibility within seconds should use the Storage Write API methods; `FILE_LOADS` accepts minutes of latency for free ingestion.

## Reading a table back out

The Storage Read API source is a bounded FLIP-27 source. It can run inside a streaming job, for example to load a dimension table for a broadcast join, and finishes when it has read the table. There is no unbounded or CDC source, and none is planned: BigQuery exposes no changelog primitive on which to build one.

A split stores one session read stream and the number of rows already consumed. Recovery resumes the read at that offset using the API's own mechanism. Readers request another stream as soon as they finish one, so requesting more streams than subtasks can help balance the work.

BigQuery decides the actual stream count. Measurements in August 2026 produced 936 streams for a 910 GB table and always 1 for a small table, regardless of how many were requested.

BigQuery charges for bytes scanned by a read session and stores columns separately. `selectedFields`, the equivalent of Table API projection pushdown, avoids scanning unused columns. `rowRestriction` filters rows before transfer. Reading a view first executes it as a query, incurring both query-scan and result-read charges, so pruning needs to happen inside that query.

The source uses Avro on the wire. In the comparison with Arrow, constructing individual records from Arrow was 34% slower than decoding Avro directly, and the Arrow representation was 84% larger on the wire. Arrow's advantage depends on avoiding individual row materialization, while Flink requires the source to produce records. The connector therefore retained Avro.

## Change data capture

With `sink.cdc.enabled`, the Table API sink accepts an upsert changelog and writes `UPSERT` and `DELETE` mutations through the Storage Write API's CDC support. The `_CHANGE_SEQUENCE_NUMBER` pseudocolumn controls ordering. It contains one to four slash-separated hexadecimal sections, compared as unsigned numbers by BigQuery.

Built-in sequence profiles derive these values from Debezium PostgreSQL and MySQL metadata, TiCDC commit timestamps, and Spanner commit-timestamp coordinates. The API remains experimental while an upstream question about exposing Debezium source metadata to sequence providers is resolved.

The worked example appears in the Spanner post: its change-stream source and this CDC sink form a replication pipeline in one Flink SQL job.

## Defaults with measurements behind them

The [configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/bigquery/) lists every option. Where a default needed empirical evidence, the project measured it and recorded the result beside the setting. Examples include:

- **Staging file roll size (16 MiB).** In the August 2026 measurement, loading 769 MiB took 15.0 s with 2 MiB files, 8.3 s with 8 MiB files, and 16.9 s with 128 MiB files. Load times were lowest near 8 MiB; making files smaller did not keep improving them. The default is 16 MiB because file size also determines how much one destination can load within the 10,000-URI job limit: about 156 GiB at 16 MiB, compared with 78 GiB at 8 MiB.
- **Zstandard staging compression.** Compression runs on the task thread and directly affects throughput. Across 2,000,000 rows, deflate took 11,436 ms and zstandard 3,182 ms. Zstandard's output was only 1.8% larger, so the choice mainly reduced CPU cost.
- **Parquet staging as opt-in.** Parquet used 0.785x the bytes of Avro across a 64x range of file sizes. Below 256 MiB of input per load job, however, it loaded several times slower, with a sharp improvement at that threshold. A streaming checkpoint's load is usually smaller. Parquet is therefore suited to batch workloads where each destination's per-commit volume clearly exceeds 256 MiB.
- **`maxInflightRequests` at 100 where the SDK defaults to 1000.** The default stream multiplexes appends over a shared pool that grows when connections appear busy. At the SDK default, a connection needs more than 200 queued requests to count as busy, so the pool rarely grows and throughput plateaus. The connector follows Google's guidance to lower the threshold and documents how to restore the SDK default.
- **Append batch size at 512 KiB.** The default bounds memory and per-record latency. Throughput-oriented jobs can increase it toward the API's 10 MB request cap, at the cost of more buffered bytes per destination and larger retry units.

The newest option, added days before release, limits staging files finalized concurrently by one writer at a checkpoint: `maxConcurrentCheckpointFinalizations`, default 1 and maximum 8. In its sizing measurements, increasing sink parallelism reduced upload time by 77-85%, while writer-local close concurrency of 8 reduced close time by roughly 79-84%. Parallelism remains the first adjustment to try; local concurrency helps when more slots are unavailable.

These numbers describe measured workloads, not fixed service behavior. The documentation records the experiments so they can be repeated when a default does not suit another workload.

## What's next for this connector

The [v1.1.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/10) extends the Table API source's filter pushdown. The CDC API will also continue to evolve while the Debezium-format question is resolved.

The next post covers Pub/Sub: metadata columns in both directions, seek-on-start behavior, and ordered delivery. Reports from workloads unlike mine help guide the project: <https://github.com/flink-gcp/flink-connector-gcp/issues>

------------------------------------------------------------------------

*This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.*
