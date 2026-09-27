+++
title = "The Bigtable connector in flink-connector-gcp, part four of the connector series"
description = "This is the fourth post in the flink-connector-gcp series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. The release post introduces the project; the..."
date = 2026-09-05T16:33:07+09:00
updated = 2026-09-05T21:29:48+09:00
draft = false

[taxonomies]
tags = ["GCP", "Flink", "Bigtable"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/09/05/163307"
+++
This is the fourth post in the [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp) series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. [The release post](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-0-0/) introduces the project; the previous posts cover [BigQuery](https://blog.laughingman7743.org/posts/flink-connector-gcp-bigquery/), [Pub/Sub](https://blog.laughingman7743.org/posts/flink-connector-gcp-pubsub/) and [Spanner](https://blog.laughingman7743.org/posts/flink-connector-gcp-spanner/).

The Bigtable connector grew out of a need to enrich streams with data from Bigtable. This post follows that SQL pipeline, then covers the full-table cache, how cell timestamps affect replayed writes, change streams, and the write extensions planned for v1.1.0.

- Documentation: <https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/bigtable/>
- Maven: `io.github.flink-gcp:flink-connector-gcp-bigtable:1.0.0` (`1.0.0-1.20` for Flink 1.20); for the SQL client, the `flink-sql-connector-gcp-bigtable` uber-jar from the [release page](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.0.0) goes into `lib/`

## What existed, and what I needed

Google's [flink-connector-gcp](https://github.com/google/flink-connector-gcp) is a separate project with the same name. It publishes a Bigtable sink to Maven Central, with DataStream and Table APIs and a custom-serializer SPI. It has neither a scan source nor a lookup source. Its change-stream source has been an open pull request since March 2026, last updated in April. When I checked on 2026-09-04, its only commit in the preceding five months was a Flink version bump.

Two smaller concerns also made me hesitate to build on it. The connectors directory contains Bigtable and a BigQuery module that the README describes as catalog support for the separately maintained Dataproc connector. The examples are spread across several top-level directories. I saw no basis to expect those gaps or the layout to change, so I built my own connector.

My pipelines mostly use Bigtable as a low-latency attribute store. An event carries a user id, and a Bigtable row holds that user's current attributes. A lookup join adds those attributes to the event without exporting the table. I also wanted to consume a change stream from the same table.

I also considered Flink's HBase connector over Google's HBase-compatible client library, but its releases did not cover the Flink versions I needed. As of 2026-09-04, the newest `apache/flink-connector-hbase` artifacts on Maven Central were 4.0.0 builds for Flink 1.18 and 1.19, published in November 2024. There were none for 1.20 or 2.x.

I did adopt its DDL model: one plain column for the row key, one `ROW` per column family, and the HBase ecosystem's byte encodings for cells. An HBase connector table definition can therefore keep its schema when moved to this connector, and each connector can read tables written by the other.

The resulting module has a bounded scan source, a change-stream source and an at-least-once sink in the DataStream API. The SQL connector adds lookup joins with three cache modes and a changelog interpretation of the change stream. I also adopted Google's serializer interface shape, so porting a serializer requires changing the interface name.

## An enrichment pipeline in Flink SQL

JSON events arrive over Pub/Sub with a user id. A Flink SQL job looks up each user's Bigtable row through a cache, then creates an HTTP task in Cloud Tasks for each enriched event. The next post covers Cloud Tasks in detail.

The job consists of three tables and one `INSERT`. This code is copied byte for byte from the [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigtable/#enriching-pubsub-events-before-creating-tasks), which CI validates through Flink's planner:

``` sql
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE incoming_events (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  message_id STRING METADATA FROM 'message-id' VIRTUAL,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'events-sub',
  'format' = 'json'
);

CREATE TABLE user_attributes (
  rowkey STRING,
  profile ROW<tier STRING, api_path STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'user-attributes',
  'scan.row-prefix' = 'user#',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

CREATE TABLE api_tasks (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  user_tier STRING,
  api_path STRING,
  source_message_id STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'events',
  'http.url' = 'https://api.example.com/events',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'format' = 'json'
);

INSERT INTO api_tasks
SELECT
  e.event_id,
  e.user_id,
  e.event_type,
  a.profile.tier,
  a.profile.api_path,
  e.message_id,
  MAP['X-Source-Message-Id', e.message_id]
FROM incoming_events AS e
JOIN user_attributes FOR SYSTEM_TIME AS OF e.proc_time AS a
  ON e.user_id = a.rowkey;
```

The Bigtable DDL follows the HBase connector's model. Exactly one column is not a `ROW`: the row key. Its type determines the key's byte encoding. A `STRING` key uses UTF-8; a `BIGINT` key uses eight big-endian bytes.

Each `ROW` column represents a column family, and its fields name the qualifiers, with one cell per field. Here, `profile ROW<tier STRING, api_path STRING>` defines the `profile` family with two qualifiers.

Cell encodings also follow HBase conventions, allowing the connector to read tables written by other systems that use those conventions. For keys or cells written with a different encoding, declaring `BYTES` or `STRING` is safer than decoding them as numbers.

The selected columns determine which families Bigtable returns. This query reads `a.profile.tier` and `a.profile.api_path`, so the point read includes a filter for the `profile` family. Families declared in the DDL but unused by the query are not transferred.

That filter also affects which rows the lookup finds. A Bigtable row exists only while it has a cell. If none of its cells belong to a family the query reads, the filter returns nothing. The inner join then produces no task for that event. The same happens when the user id falls outside `user#`, because `scan.row-prefix` bounds point reads as well as scans. A `LEFT JOIN` would retain these events with null attributes.

The join must use equality on the single row-key column. Unlike the Spanner example, it cannot join on composite keys or nested family fields; Flink rejects those joins during planning. A Bigtable point read addresses one atomic row key. To look up a row by two values, encode both into that key.

The example uses Flink's standard lookup cache. With `lookup.async = 'true'`, point reads can overlap instead of blocking the operator for each event. With `lookup.cache = 'PARTIAL'`, the cache loads entries on demand and retains them under at least one configured bound. Here the bounds are 10,000 rows and a ten-minute write expiry. A frequently accessed user that stays cached is read once per ten minutes rather than once per event, at the cost of up to ten minutes of stale attributes.

The checkpoint interval serves the Pub/Sub source: it acknowledges messages when a checkpoint completes. Both ends of this pipeline provide at-least-once delivery. The lookup join does not strengthen that guarantee.

Deployment requires three uber-jars in the SQL client's `lib/`, one per connector. Their dependencies are relocated so the jars can coexist.

Google's Bigtable connector also registers the `bigtable` identifier. Installing both Bigtable connectors as separate jars makes factory discovery fail with an ambiguity error naming that identifier. Merging them into one fat jar can instead let whichever registration survives silently control the DDL. Keep the jars separate so the conflict remains visible.

## The full-table cache, and when to reach for it

The Spanner post mentioned that its lookup source rejects a `FULL` cache, while Bigtable offers one. The choice here depends on how large the lookup table is and how often it changes.

The three cache modes differ in where reads happen. `NONE` performs a point read for every event. `PARTIAL` reads on a cache miss and retains the result within its configured bounds.

With `FULL`, each lookup task loads every projected row within the configured key bounds through a bounded scan. All joins use that local copy, with no point reads. Reloads can run periodically through `lookup.full-cache.periodic-reload.interval`, in fixed-delay or fixed-rate mode, or at a time of day through `lookup.full-cache.timed-reload.iso-time`, at a configured interval in days.

A full-cache lookup is synchronous, so combining it with `lookup.async = 'true'` fails during planning. The `scan.row-prefix` and range bounds apply to the cache load just as they do to point reads.

A full cache suits a dimension table that fits comfortably in a subtask's heap, receives frequent lookups, and changes on a schedule. Consider a few hundred thousand user attributes rebuilt nightly. A timed reload after the rebuild makes every join local, with no Bigtable traffic between reloads. The cost is one copy of the projected table per lookup task, so memory use scales with lookup parallelism.

My reading of the two designs is that the difference starts with what each service's scan promises. A Spanner snapshot read runs at one timestamp in a batch transaction whose lifetime is bounded by version retention. A full cache would need to define the snapshot it holds and how successive reloads relate, a contract that source deliberately does not offer.

A Bigtable scan returns the latest cell versions as it reads each row, without a single snapshot timestamp. The cache uses the connector's bounded scan source as its loader and retains those scan semantics.

A `FULL` cache reload is a scan, so it can use a Data Boost application profile. Data Boost is Bigtable's read-only serverless compute and keeps the reload off the serving cluster. Point-read modes cannot use it.

Two limits matter here. Bigtable gives no guarantee for data written less than 35 minutes before a Data Boost read, so a reload may omit the newest rows. A parallel reload above 1,000 read requests per second per cluster is reported as ineligible and billed accordingly, without an error.

This project has not exercised Data Boost, which requires an Enterprise-edition instance. The gated suite checks only that the configured profile id reaches the client. I therefore consider the full-cache combination plausible but unproven.

## Keeping the lookup table current

Writing the lookup table raises two questions: what happens when an update is replayed, and what happens when two updates target the same key? A second SQL job maintains the table from a stream of profile updates. It adapts the [source-backed cell-timestamp example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigtable/#writing-a-stable-cell-timestamp); I added the Pub/Sub source:

``` sql
CREATE TABLE profile_updates (
  user_id    STRING,
  tier       STRING,
  api_path   STRING,
  updated_at TIMESTAMP_LTZ(3)
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'profile-updates-sub',
  'format'       = 'json'
);

CREATE TABLE user_attributes_out (
  rowkey         STRING,
  profile        ROW<tier STRING, api_path STRING>,
  cell_timestamp TIMESTAMP_LTZ(6) METADATA FROM 'timestamp',
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project'   = 'my-project',
  'instance'  = 'my-instance',
  'table'     = 'user-attributes',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO user_attributes_out
SELECT user_id, ROW(tier, api_path), CAST(updated_at AS TIMESTAMP_LTZ(6))
FROM profile_updates;
```

The sink advertises an upsert contract by default because `setCell` overwrites cells under the row key. A changelog `DELETE` removes the whole row. Declaring the row key as a primary key is optional, as with the HBase connector. On Flink 2.x, declaring it can make updating queries cheaper: a delete may carry only the key, avoiding the stateful `ChangelogNormalize` operator that would otherwise complete the row.

The `insert-only` option addresses a planning rule in Flink 2.3. A keyed upsert sink requires `ON CONFLICT` when the input's upsert key differs from the sink's or cannot be inferred, as with this append stream. The option narrows an insert-only input to an insert-only contract, allowing this statement to plan without the clause on Flink 1.20, 2.2 and 2.3. The physical write is unchanged: it still overwrites cells under an existing row key.

The `cell_timestamp` column determines what a replay does to a cell. This is an at-least-once sink, so a restart replays records written since the last completed checkpoint. With a stable explicit timestamp, replay writes the same value to the same cell version. Without one, the sink uses the writer's clock, and replay adds another version. The table's garbage-collection policy determines how long those versions remain.

The query takes its timestamp from the record's `updated_at`, making a replay of that cell write a no-op. Bigtable's storage model absorbs the duplicate; the sink does not track or reject replayed records. This does not cover writes using the writer's clock, non-idempotent mutations, or collisions between distinct events that target the same cell version.

On a table with the default timestamp granularity, an explicit timestamp must be millisecond-aligned or Bigtable rejects it with `INVALID_ARGUMENT`. The opt-in `sink.cell-timestamp.truncate-to-millis` drops the extra precision. An absent or null timestamp uses a millisecond-aligned writer clock.

The sink does not guarantee the order of writes to one key. Bigtable's bulk mutation contract says entries "may be applied in arbitrary order (even between entries for the same row)". Requests can also overlap in flight. Two updates to one user may therefore be applied in a different order from the order in which the job submitted them.

Before deciding whether to add ordering machinery, the project measured the service. On 2026-08-11, a campaign submitted 86,196 same-row pairs in mirrored arms against a one-node SSD instance. Request sizes ranged from 2 through 19,998 entries, and the campaign observed zero reversals. That observation does not change the service contract. The connector documents the caveat rather than adding a per-key queue for reversals the campaign did not observe.

For timestamped profile updates, a latest-cell read selects by the record-supplied cell timestamp, regardless of mutation arrival order. Two updates within the same millisecond can still target one version with no defined winner, and a row delete carries no timestamp. Those cases remain subject to the ordering caveat.

In the DataStream API, the serializer makes the same choices. It returns the client's `RowMutationEntry`, which can combine several cell writes and a delete into one atomic operation on a row. This example is adapted from the [source-backed examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigtable/#several-cells-and-a-delete-per-record), which CI compiles:

``` java
BigtableSink.<OrderEvent>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .serializer(
                (event, context) -> {
                    long timestampMicros = event.updatedAtMillis() * 1_000;
                    RowMutationEntry entry = RowMutationEntry.create("order#" + event.id());
                    entry.setCell("cf", "status", timestampMicros, event.status());
                    entry.setCell("cf", "total", timestampMicros, event.totalCents());
                    if (event.isCancelled()) {
                        entry.deleteCells("cf", "reserved_stock");
                    }
                    return entry;
                })
        .build();
```

Multiplying by 1,000 converts the record's epoch milliseconds to the microseconds `setCell` expects. A serializer can also return `null` to skip a record, for example when a filter depends on the mutation being built. Nothing is written, and only the `recordsSkipped` counter reports the skip.

Dynamic destinations work as in the earlier posts. A resolver returns a `TableDestination` for each record, allowing a table per tenant or per day. The writer maintains one bulk mutation batcher per table and one client per instance.

Auto-creation requires an explicit schema: column families and their garbage-collection policies. The sink cannot infer these, so `CREATE_IF_NEEDED` requires table-create options naming at least one family. As in the Pub/Sub sink, creation is reactive. No admin client exists until a mutation fails with `NOT_FOUND`. The sink then ensures the table and its declared families exist and reapplies the failed mutations.

Garbage collection needs a deliberate choice. A family without a rule keeps Bigtable's default of collecting nothing. If the sink uses wall-clock timestamps, duplicate versions from replays can then accumulate forever. The SQL layer therefore requires a `max-versions` or `max-age` rule under `create-if-needed`. A typical rule for retaining only the latest cell is `GcRule.union(GcRule.maxVersions(1), GcRule.maxAge(...))`.

Google's connector calls timestamp-based idempotence "Exactly Once out of the box". Its writer flushes at the checkpoint barrier without a committer, and most of its built-in serializers use the Flink record timestamp for each cell. The underlying mechanism is the same, but this connector does not label it exactly-once.

Stable timestamps cannot make an increment or append idempotent. Bigtable's primitive for protecting such a write from replay is a conditional single-row mutation: apply the data only if a marker in the row says the write has not already happened.

The project benchmarked this primitive against the real service under conditions fixed in advance. Two runs were inconclusive because throughput varied too widely between repetitions. On 2026-09-05, a run that isolated each repetition in its own JVM passed: about 1.5 times the bulk baseline's throughput, at two thirds of its tail latency, with a clean replay read-back. A separate probe showed that the conditional branch also absorbed a replayed increment.

That result supports a planned exactly-once mode for v1.1.0 ([\#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)). It is not implemented yet. The proposal uses Flink's two-phase commit: stage records during a checkpoint interval, then apply them at checkpoint completion through conditional writes keyed on a checkpoint-id cell in each row. The intended guarantees are idempotent commit retries and no later data left visible after restoring an earlier checkpoint.

The expected costs are one conditional request per row and loss of staged records if Flink state is discarded, as with BigQuery's buffered streams. The issue's design record will settle the remaining details. Until this mode ships, BigQuery is the only connector in the family with a supported exactly-once sink mode.

## Change streams without a metadata table

A Bigtable change-stream record describes one atomic row mutation as the service applied it. It contains the row key and an ordered list of entries: `SetCell`, `DeleteCells`, `DeleteFamily`, and the aggregate kinds `AddToCell` and `MergeToCell`. It also carries a commit timestamp, a tie breaker, the source cluster and the partition's estimated low watermark.

Unlike Spanner's change stream, it supplies neither a before image nor a complete after image of the row. That difference determines how the connector's two SQL modes interpret the stream.

The envelope mode preserves the mutation as one insert-only row, with its entries in an array in service order. This example is copied verbatim from the [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigtable/#consuming-change-streams-with-sql):

``` sql
CREATE TABLE profile_mutations (
  row_key BYTES,
  entries ARRAY<ROW<
    entry_index INT,
    kind STRING,
    family STRING,
    qualifier ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    `timestamp` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    `value` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    delete_range ROW<
      start_bound STRING,
      start_micros BIGINT,
      end_bound STRING,
      end_micros BIGINT
    >
  >>,
  mutation_type STRING NOT NULL
    METADATA FROM 'mutation-type' VIRTUAL,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'commit-timestamp' VIRTUAL,
  source_cluster_id STRING
    METADATA FROM 'source-cluster-id' VIRTUAL
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'envelope',
  'scan.app-profile-id' = 'single-cluster-profile'
);

SELECT
  row_key,
  mutation_type,
  commit_timestamp,
  entry_index,
  kind,
  family,
  qualifier,
  entry_timestamp,
  entry_value,
  delete_range
FROM profile_mutations
CROSS JOIN UNNEST(entries) AS entry_table(
  entry_index,
  kind,
  family,
  qualifier,
  entry_timestamp,
  entry_value,
  delete_range
);
```

In envelope mode, a deletion is an inserted log record, just like any other mutation. There is no row image to turn into a Flink `DELETE`. The stream also includes garbage-collection mutations alongside application writes across all column families; `mutation_type` distinguishes them.

An envelope table rejects a primary key. Two mutations to the same Bigtable row are two log records, so feeding a keyed upsert sink requires the job to reconstruct rows in its own stateful operator.

The envelope is a useful history format: it records which cells changed and when. `entry_index` preserves each entry's position through `UNNEST`. These records can feed BigQuery's append path, like the history pattern in the Spanner post.

The `selected-cell` mode produces a keyed changelog, but requires a specific producer protocol. One configured cell must contain the complete serialized non-key part of a logical row. The source decodes it with a Flink format and emits keyed `UPDATE_AFTER` and `DELETE` rows.

The resulting changelog can feed the BigQuery CDC sink from the first post. This code is copied verbatim from the [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigtable/#replicating-a-selected-cell-into-bigquery):

``` sql
SET 'execution.checkpointing.interval' = '1 min';

CREATE TABLE current_profiles (
  profile_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'selected-cell',
  'scan.app-profile-id' = 'single-cluster-profile',
  'scan.change-stream.selected-cell.family' = 'state',
  -- Base64 for the qualifier "current".
  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',
  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-a',
  'value.format' = 'json'
);

CREATE TABLE analytics_profiles (
  profile_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_profiles',
  'sink.cdc.enabled' = 'true',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min'
);

INSERT INTO analytics_profiles
SELECT profile_id, name, tier FROM current_profiles;
```

The producer must follow the documented replacement protocol. An upsert must atomically delete the selected column across all timestamps, or its entire family, and then set exactly one replacement cell. The same delete without a following set becomes a key-only delete. Entries for other cells produce no row.

The source cannot infer this protocol from arbitrary traffic. A standalone, repeated or out-of-order selected write fails the job, as does writing from a second cluster. The writer must replace the complete value using the required mutation shape.

Compared with the Spanner replication example, the producer supplies the row contents that Bigtable itself does not provide. This pipeline also sends BigQuery no sequence metadata, so colliding changes to one key resolve by arrival order. It is an analytics replica starting at `latest`; an already populated source table needs a separate initial snapshot.

Both SQL modes use the partition machinery described in the Spanner post. Bigtable change-stream partitions split and merge over time. Each open partition read is a streaming RPC carrying mutations, heartbeats and continuation tokens. The coordinator stores assigned and unassigned partitions, pending merges and resume tokens in Flink checkpoints.

No metadata table is created, so there is no additional stateful resource to provision, grant write access to or clean up. By comparison, Apache Beam's `BigtableIO.readChangeStream` "creates and manages a metadata table to manage the state of the connector", by default in the streamed table's instance. This quote is from the Beam Java SDK documentation, checked 2026-09-04, and describes the same arrangement as the Spanner post.

Retention handling also follows the Spanner pattern. Restoring a position outside the stream's retention fails the job by default. The explicit opt-in `scan.resume-fallback.mode` discards the stale token and restarts from `earliest`, `latest` or a timestamp, accepting the resulting gap.

This source emits no watermarks, so its DDL must not declare `SOURCE_WATERMARK()`. Each record does carry its commit timestamp as the Flink timestamp, and the partition's estimated low watermark is available as metadata. But Bigtable permits later records to have older commit timestamps, with no published finite bound on how much older. That estimate cannot support a source-watermark guarantee.

Spanner heartbeats, by contrast, guarantee that every change at or before the heartbeat timestamp has been delivered. That lets the Spanner source own its watermark. Beam's Bigtable documentation takes a different approach: it says the connector "outputs all data with an output timestamp of zero, making all data late" (checked 2026-09-04).

A Flink job can instead define its own watermark policy over the commit timestamp:

``` sql
commit_timestamp TIMESTAMP_LTZ(3) NOT NULL
  METADATA FROM 'commit-timestamp' VIRTUAL,
WATERMARK FOR commit_timestamp AS commit_timestamp - INTERVAL '5' MINUTE
```

The five-minute delay is an example policy. Records behind that watermark are late according to the job's own choice. Downstream allowed lateness or late-data routing must account for them.

The maximum number of open partition reads is source parallelism multiplied by `scan.max-concurrent-streams-per-subtask`, whose default is two. This is a connector capacity bound, not a service quota. Monitor the positions of active, queued and unassigned partitions against the table's change-stream retention: once records fall outside retention, the source cannot recover them.

Change streams require a single-cluster-routing application profile. The API is marked `@PublicEvolving` because its record model follows a client surface the vendor is still evolving. There is no emulator option, since the Bigtable emulator implements neither change-stream RPC.

## The knobs that matter

The sink exposes batch thresholds and in-flight bounds. Both count entries, one per record returned by the serializer. Bigtable's separate service limit counts mutations: at most 100,000 per batch. The client enforces that limit by flushing before another entry would exceed it, regardless of the configured entry threshold.

Batch thresholds (`sink.batching.*` in SQL) are unset by default, retaining the client's defaults of 100 entries, 20 MiB and a one-second timer. Lowering the entry threshold reduces the wait before a low-volume batch is sent, useful for a table-per-day job receiving only a trickle of records.

The writer's in-flight bounds control backpressure: `maxInFlightEntries` defaults to 1,000 and `maxInFlightBytes` to 64 MiB. At either cap, `write()` yields to the task mailbox until requests complete and the counters fall. This lets checkpoint barriers continue to run.

The client has its own flow controller, capped at 20,000 outstanding entries and 100 MiB. Reaching either limit blocks Flink's task thread, which also needs to process checkpoint barriers. Raising the writer's bounds far above the client's limits therefore shifts backpressure into a blocking client call instead of increasing effective capacity.

The connector exposes no retry knobs. The client retries transient `MutateRows` failures per entry, with its own backoff and a ten-minute total budget. A failure reaches the writer after the client has given up. An `UNAVAILABLE` at that point represents an outage that outlasted the retry budget, rather than a single slow call.

Only `INVALID_ARGUMENT` is eligible for row-level failure routing, where a configured handler can drop or dead-letter it. gRPC defines that status as a problem independent of system state. Under `CREATE_IF_NEEDED`, a `NOT_FOUND` can be repaired for a missing table or a declared missing family; otherwise it is fatal. All other statuses, including outages, fail the job.

Bigtable can reject an entire batch because of one bad entry. A measurement confirmed this: a good record and a bad record sent together both failed with the same status. The writer therefore resubmits each parked mutation alone to confirm a row-level rejection before invoking the handler.

The `maxConsecutiveRejections` limit, 100 by default, fails the job after that many confirmed rejections without an applied mutation between them. This prevents a dropping policy from silently draining a stream that the service refuses wholesale. SQL tables expose no failure-policy option and always fail the job on the first routed failure.

Scan parallelism depends on the table's tablets. Each split is a row-key range cut at boundaries Bigtable reports, and the read path cannot subdivide a tablet. A table with few tablets therefore uses few reading subtasks even when the job has higher parallelism.

Each fetch hands Flink at most 1,000 rows and targets at most 8 MiB of decoded input. Server-side filters reduce memory use before data reaches the SDK. SQL family projection is one such filter.

## Testing, and what the emulator cannot show

The Bigtable emulator is useful for development, but its validation differs substantially from the real service. The differences can change failure routing. For a timestamp finer than the table's granularity or an empty row key, the service returns `INVALID_ARGUMENT`. For a missing column family, it returns `NOT_FOUND`. The emulator returns `INTERNAL` in these cases.

This sink treats `INTERNAL` as fatal but can route `INVALID_ARGUMENT` to a failure handler. An emulator-only test can therefore report a job failure for an error that is droppable on the service.

The emulator also models no tablets, so a scan plan has one split regardless of parallelism. It ignores application profiles and implements neither change-stream RPC. The [connector page](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/bigtable/#where-the-emulator-differs-from-the-service) contains the full deviation tables measured against both environments.

The gated suite creates real instances for its tests. A standing one-node instance costs roughly \$470 a month, so each gated class creates an instance and deletes it afterwards. A scheduled sweep reclaims instances left by runs that die before teardown.

These tests found the batch-wide rejection that led to individual confirmation of failed entries. The same-row ordering campaign also ran against the real service. It was deliberately not retained as a regression test: requiring zero reversals would turn an observation into a guarantee the service does not make.

For change-stream recovery, the run on 2026-08-12 observed all 100 seeded rows through a checkpoint, a controlled failure and recovery, without loss. It reached its bounded end time in 132.3 seconds.

## What's next for this connector

The [v1.1.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/10) mainly extends Bigtable writes beyond `MutateRows`. Its first piece merged on 2026-09-03: a request-response runtime for the single-row transactions `CheckAndMutateRow` and `ReadModifyWriteRow`. It sits alongside the batching sink because each operation sends one request for one row and returns a response the caller needs.

The runtime follows the client's classification of both RPCs as non-idempotent. It makes one attempt under a 20-second deadline, with no retries on either side. If the request ends before the service answers, the outcome is ambiguous and the job fails: replaying an increment could apply it again.

The runtime and its options are present, but public entry points remain open work under [\#1174](https://github.com/flink-gcp/flink-connector-gcp/issues/1174). These include per-operation sinks and functions and a Table write mode. The issue also tracks aggregate column families and an atomic keep-latest mode that deletes a cell's versions and sets its replacement in one mutation.

Response-bearing async SQL functions for Flink 2.2 and later follow. The committer-based exactly-once mode described above is currently planned as the milestone's last item.

The next post covers Cloud Tasks, the sink at the end of this pipeline and the one service for which I found no Flink connector. Reports from workloads unlike mine help decide what to build next: <https://github.com/flink-gcp/flink-connector-gcp/issues>

------------------------------------------------------------------------

*This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.*
