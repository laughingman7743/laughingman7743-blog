+++
title = "Releasing flink-connector-gcp 1.1.0: Bigtable write modes, checkpointed sinks, and lineage"
description = "I have released flink-connector-gcp 1.1.0. This release adds several ways to write Bigtable rows, experimental checkpoint-owned delivery for Bigtable and Cloud Tasks, and..."
date = 2026-09-26T23:47:14+09:00
draft = false

[taxonomies]
tags = ["Flink", "GCP", "Bigtable", "CloudTasks"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/09/26/234714"
+++
I have released **flink-connector-gcp 1.1.0**. This release adds several ways to write Bigtable rows, experimental checkpoint-owned delivery for Bigtable and Cloud Tasks, and FLIP-314 lineage metadata across all five connectors. It also extends BigQuery and Spanner filter pushdown and fixes several source and sink edge cases.

- [Release and SQL connector downloads](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.1.0)
- [Version 1.1 documentation](https://flink-gcp.github.io/flink-connector-gcp/1.1/)
- [Source](https://github.com/flink-gcp/flink-connector-gcp/tree/v1.1.0)

The Maven coordinates remain under `io.github.flink-gcp`. Use version `1.1.0` with Flink 2.2 or 2.3, and `1.1.0-1.20` with Flink 1.20. For SQL jobs, install the matching `flink-sql-connector-gcp-*` jar from the release downloads in Flink's `lib/` directory. The supported Flink versions have not changed since 1.0.0. No public API or configuration option was removed or renamed, and the new write and delivery modes are opt-in. An existing job therefore keeps its 1.0.0 behavior when its connector dependency is upgraded.

## Bigtable writes beyond upsert

Bigtable's Table API sink now selects its operation through `sink.write-mode`. The default remains `upsert`. The new choices select different operations for each input row:

| Mode | Operation |
|----|----|
| `insert-if-absent` | Insert cells atomically only if the row has no stored cells. |
| `keep-latest` | Delete all versions of each targeted cell and write its replacement in one row operation. |
| `append` and `increment` | Apply read-modify-write rules to the latest stored cell values. |
| `aggregate` | Contribute values to typed INT64 SUM, MIN, MAX, or HLL aggregate families. |
| `conditional` | Use a DDL-defined predicate and mutation branches for `CheckAndMutateRow`. |

The `conditional` mode needs the most configuration because the DDL defines a complete row command. This example changes a user's status only if the latest stored status is `pending`. On a match it also adds one to an aggregate counter; on a mismatch it writes an audit reason.

``` sql
CREATE TABLE conditional_updates (
  row_key STRING,
  expected_status BYTES,
  new_status BYTES,
  activation_delta BIGINT,
  mismatch_reason BYTES
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'users',
  'sink.app-profile-id' = 'single-cluster',
  'sink.write-mode' = 'conditional',
  'sink.conditional.row-key-column' = 'row_key',
  'sink.conditional.predicate' = 'latest-cell-value-equals',
  'sink.conditional.predicate.family' = 'profile',
  'sink.conditional.predicate.qualifier' = 'status',
  'sink.conditional.predicate.value-column' = 'expected_status',
  'sink.conditional.then.0.operation' = 'set-cell',
  'sink.conditional.then.0.family' = 'profile',
  'sink.conditional.then.0.qualifier' = 'status',
  'sink.conditional.then.0.value-column' = 'new_status',
  'sink.conditional.then.1.operation' = 'add-to-cell',
  'sink.conditional.then.1.family' = 'stats',
  'sink.conditional.then.1.qualifier' = 'activated',
  'sink.conditional.then.1.timestamp-micros' = '0',
  'sink.conditional.then.1.value-column' = 'activation_delta',
  'sink.conditional.otherwise.0.operation' = 'set-cell',
  'sink.conditional.otherwise.0.family' = 'audit',
  'sink.conditional.otherwise.0.qualifier' = 'reason',
  'sink.conditional.otherwise.0.value-column' = 'mismatch_reason'
);

INSERT INTO conditional_updates VALUES (
  'u1',
  CAST('pending' AS BYTES),
  CAST('active' AS BYTES),
  1,
  CAST('status mismatch' AS BYTES)
);
```

This is a write-only **command-input table**. Its five physical columns supply arguments to the command; they are not a description of the stored Bigtable row. `sink.conditional.row-key-column` selects the input column containing the row key, and each `value-column` names another top-level input column exactly. The `CAST(... AS BYTES)` expressions supply the byte values compared with or written to Bigtable.

The `predicate` settings compare the latest `profile:status` cell with `expected_status`. If they match, `then.0` writes `new_status` to that cell and `then.1` adds `activation_delta` to `stats:activated`. The numeric branch indexes set the order of those operations. If the predicate misses, `otherwise.0` writes `mismatch_reason` to `audit:reason` instead. The explicit timestamp `0` is required for `add-to-cell` and addresses the aggregate cell at that timestamp.

Create the `profile` and `audit` families and an INT64 SUM `stats` family before running the statement. The application profile must use single-cluster routing with single-row transactions enabled. All referenced inputs must be non-null, including the value used by the branch the service does not select. The mode accepts insert-only input and has at-least-once delivery: recovery may repeat an applied request and select a different branch. The sink discards the Boolean match result. A query that needs it can use `BigtableCheckAndMutateFunction` on Flink 2.x, with a separate named-request configuration.

The other modes cover simpler cases. `insert-if-absent` checks whether the entire row has any cell before inserting. `keep-latest` replaces the versions of the targeted cells immediately, whereas a one-version garbage-collection rule removes old versions asynchronously. `append` and `increment` operate on raw cells and can repeat their effect after replay. For counters stored in Bigtable aggregate families, `aggregate` exposes SUM, MIN, MAX, and HLL contributions through SQL. The [Bigtable Table reference](https://flink-gcp.github.io/flink-connector-gcp/1.1/docs/connectors/table/bigtable/) has complete, planner-checked examples for every mode and their type and replay rules.

DataStream jobs can also send `CheckAndMutateRow` and `ReadModifyWriteRow` requests through the new single-row request sinks. When a SQL query needs the outcome of a conditional write or the cells returned by a read-modify-write request, Flink 2.x has two new asynchronous functions: `BigtableCheckAndMutateFunction` and `BigtableReadModifyWriteFunction`. These functions are unavailable in the `-1.20` artifacts; the [async SQL function guide](https://flink-gcp.github.io/flink-connector-gcp/1.1/docs/connectors/table/bigtable-functions/) shows their registration and result types.

## Checkpoint-owned Bigtable delivery

Bigtable now has an **experimental `EXACTLY_ONCE` delivery mode**. Set `sink.delivery-guarantee = 'exactly-once'` for a Table sink using `upsert`, `keep-latest`, or `aggregate`. The other new write modes reject that setting. The equivalent DataStream choice is `BigtableDeliveryGuarantee.EXACTLY_ONCE`.

The writer holds mutations until their owning Flink checkpoint completes. A committer then applies each mutation with a retained marker that prevents a restored checkpoint from applying that mutation again. This matters for aggregate contributions: a stable cell timestamp alone does not prevent a replayed SUM contribution from being added twice. Recover from the latest completed checkpoint with all committer state; if a stop-with-savepoint produced a savepoint, follow the guide's rule for restoring it. Discarding that state after the source has checkpointed records but before their staged mutations reach Bigtable can lose those writes.

The delay is part of the mode's cost. A row becomes readable no earlier than one checkpoint interval later, followed by the time needed to commit the staged writes. Each staged mutation uses a conditional request instead of sharing a bulk `MutateRows` request, so the drain costs more per row than the default sink. The [checkpoint-owned delivery guide](https://flink-gcp.github.io/flink-connector-gcp/1.1/docs/connectors/datastream/bigtable/#checkpoint-owned-writes) describes the required marker family, transactional routing, recovery conditions, and measured latency. The mode ships as experimental; the assessment did not establish a supported workload for its performance gate.

## Checkpointed Cloud Tasks creation

Cloud Tasks has a separate **experimental `EXACTLY_ONCE` task-creation mode**. It checkpoints a named task before calling `CreateTask` and reuses the same name and serialized task on recovery. Within the documented recovery window, this protects creation of each staged envelope against a replay. The default remains at-least-once creation.

The Table API selects the mode through `sink.delivery-guarantee`:

``` sql
SET 'execution.runtime-mode' = 'STREAMING';
SET 'execution.checkpointing.interval' = '1 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.checkpoints-after-tasks-finish' = 'true';
SET 'execution.checkpointing.storage' = 'filesystem';
SET 'execution.checkpointing.dir' = 'file:///shared/flink/checkpoints/cloudtasks';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.timeout' = '5 min';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '3';
SET 'restart-strategy.fixed-delay.delay' = '1 s';

CREATE TABLE generated_tasks (
  payload STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10'
);

CREATE TABLE checkpointed_tasks (
  payload STRING
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'webhooks',
  'http.url' = 'https://api.example.com/tasks',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.staged.max-tasks' = '1000'
);

INSERT INTO checkpointed_tasks SELECT payload FROM generated_tasks;
```

Replace the sample endpoint and checkpoint directory, provision the queue, and use a durable checkpoint filesystem shared by the job's processes. The queue must retain task names through the configured recovery window; the creating identity also needs permission to read queue settings for the default retention check. The synthetic `datagen` input shows the configuration, not event deduplication. The [Cloud Tasks checkpointed-creation guide](https://flink-gcp.github.io/flink-connector-gcp/1.1/docs/connectors/table/cloudtasks/#checkpointed-task-creation) covers the recovery prerequisites and the matching DataStream options. Recovery must retain the latest checkpoint and the sink's mapped committer state. Dropping that state, including by restoring with `allowNonRestoredState`, can lose checkpointed tasks that have not yet been created.

This mode protects **task creation**, not handler execution. Cloud Tasks can dispatch a created task more than once, so the handler still needs an idempotent operation or its own deduplication record. Tasks become visible after their owning checkpoint completes; the measured DataStream runs kept up at the rates tested but added most of a checkpoint interval to visibility latency. If records already have stable task IDs, the default writer can collapse duplicate creations within the queue's name-retention window without waiting for a checkpoint.

## Lineage across the connectors

All five connectors now report configured Google Cloud resources through Flink's FLIP-314 lineage metadata. For example, a BigQuery sink can identify a configured table, a Pub/Sub source can identify its subscriptions, and a Cloud Tasks sink can identify a fixed queue. These identities describe resources known when the job graph is built; a per-record destination chosen later cannot be inferred from that graph.

On Flink 2.2 and 2.3, a configured job-status listener can receive the metadata. On Flink 1.20, the vertices can be inspected directly but are not delivered to listeners automatically. The connectors provide resource metadata; an exporter or lineage service is a separate component. The [lineage guide](https://flink-gcp.github.io/flink-connector-gcp/1.1/docs/connectors/lineage/) lists the resource identities and coverage limits.

## Other changes

BigQuery's Table source pushes more bounded-scan filters to the service: binary values, `TIME` with precision 0–3, and `TIMESTAMP` or `TIMESTAMP_LTZ` with precision 0–5. Deeply nested predicates no longer risk a stack overflow during filter handling. Spanner bounded reads now push ordered `FLOAT64`/`FLOAT8` key predicates down to the service.

Bigtable Change Streams selected-cell mode now accepts full-column deletes. Bigtable's async function now completes a result when its timeout fires between retry attempts, and Bigtable and Spanner model constructors accept immutable lists. Spanner's `ChildPartitionsEvent` also reports a null child as a clear argument error.

The documentation site now keeps separate [1.1](https://flink-gcp.github.io/flink-connector-gcp/1.1/) and [1.0](https://flink-gcp.github.io/flink-connector-gcp/1.0/) pages alongside Development. Use the 1.1 pages for the code in this release: Development follows `main` and can describe features that have not shipped. The [release notes](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.1.0) have the complete highlight list and downloads.

This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.
