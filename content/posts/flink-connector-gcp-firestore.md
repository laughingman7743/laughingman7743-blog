+++
title = "The Firestore connector in flink-connector-gcp"
description = "Using Firestore with Apache Flink 1.20 and 2.x: SQL writes, snapshot scans, lookup joins, and DataStream sinks, with support for Native and Datastore modes."
date = 2026-10-17
draft = false

[taxonomies]
tags = ["Flink", "GCP", "Firestore", "Datastore"]
+++

Firestore joins [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp) in [version 1.2.0](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.2.0).
It is the sixth connector module, following the five covered by the [original series](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-0-0/).
The module supports both Firestore Native mode and Datastore mode, with DataStream sources and sinks and Table API connectors for SQL.

The Native-mode connector covers three useful jobs: writing pipeline output into documents, reading a collection as a snapshot, and looking up a document while processing an event.
A SQL table supplies the document schema and collection; a DataStream serializer can choose the document path and write operation for each record.
The examples below focus on Native mode, followed by the differences for Datastore mode.

## Installing the connector

For a DataStream application, use `io.github.flink-gcp:flink-connector-gcp-firestore:1.2.0` on Flink 2.2 or 2.3, or version `1.2.0-1.20` on Flink 1.20.
For SQL jobs, install the corresponding `flink-sql-connector-gcp-firestore` JAR from the [release downloads](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.2.0) in Flink's `lib/` directory before starting the cluster.
One SQL JAR provides both identifiers, `firestore` for Native mode and `datastore` for Datastore mode.

Create the database before running a job.
The connector writes to an existing database and does not provision one; a Native-mode collection appears when its first document is written.
The examples use `my-project` and its `(default)` database.
Set `database` for another database and provide Application Default Credentials on the job's processes.
The [credentials guide](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/datastream/firestore/#credentials) describes the permissions for reads and writes and the emulator configuration.

For a streaming sink, enable checkpointing, for example with `SET 'execution.checkpointing.interval' = '60 s'` in SQL.
The sink waits for pending writes at each checkpoint barrier.
Checkpointing and replay are part of its at-least-once delivery contract.

## Writing rows as documents

A SQL table names one collection, and its primary key becomes the document ID.
This excerpt is the [SQL reference's write example](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/firestore/), backed by a planner-tested snippet in the connector repository:

```sql
CREATE TABLE orders (
  order_id STRING NOT NULL,
  customer STRING,
  total DOUBLE,
  updated_at TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'orders'
);

INSERT INTO orders SELECT order_id, customer, total, updated_at FROM staged_orders;
```

`staged_orders` is an input table or view supplied by the application, with `order_id STRING`, `customer STRING`, `total DOUBLE` and `updated_at TIMESTAMP_LTZ(6)` columns.
The repository's planning test supplies a one-row view; this excerpt does not define an upstream source.
For a load with one row per order ID, a row keyed `o-1001` writes `orders/o-1001`.
The key is not also stored as a field: the document contains `customer`, `total` and `updated_at`.
The connector accepts one `STRING` primary-key column.
A row whose key is empty or contains `/` fails the job because it does not identify a document in that collection.

The default write mode is **`merge`**.
An application may already store fields such as `fulfillment` in these documents, and a table that declares only the fields above leaves `fulfillment` in place.
The choice is made through `sink.write-mode`:

| Mode | Effect of an insert or update |
| --- | --- |
| `merge` | Merge the row's fields, creating the document if needed; preserve undeclared fields. |
| `replace` | Replace the document with the row, creating it if needed; remove undeclared fields. |
| `update` | Replace the declared top-level fields of an existing document; a missing document fails the job. |

Merge also works inside `ROW` and `MAP` values, keeping nested keys the incoming value does not contain.
Removing a key from a map in Flink therefore does not remove that key from the stored map.
An empty map leaves the stored map unchanged, while an array or SQL `NULL` replaces the stored field.
Choose `replace` when the row is intended to describe the complete document.

A keyed table accepts an upsert changelog.
Under `merge` and `replace`, an input delete deletes the document; `update` rejects deletes.
These modes do not preserve the order of repeated writes to a document, so an updating aggregation is not enough by itself to keep Firestore at the latest aggregate value.
The ordering section below explains the limit.

Without a primary key, the table accepts inserts only and creates each row under a newly drawn document ID.
It does not accept `sink.write-mode` in that form.
This is useful when the stored records need independent identities, but a restart can create another document for a replayed row.
Even without a restart, a retry after a successful create whose response was lost can lead to another ID and a second copy.
Use a stable key when repeated delivery should address the same document.

## Reading a collection snapshot

The same `orders` table can be read with a `SELECT`.
A scan reads the documents directly in that collection at one snapshot time and then finishes.
It can run in a batch or streaming job, but it does not keep watching for changes.
Version 1.2.0 has no Firestore CDC source.

A regular collection scan is one query on one split.
For documents spread across subcollections, `scan.collection-group = 'true'` reads every collection whose ID is the last segment of `collection`, at any depth of the database, and uses service-planned partitions.
For example, an `audit` group can include both `users/alice/audit` and `users/bob/audit`.
Such a table cannot declare a primary key because the same document ID can occur under different parents.
The `document-path` metadata column identifies the full path instead.

By default, the read time comes from the service when the source plans the read.
`scan.read-time` selects a particular snapshot, and the source checkpoints that time with its split positions.
A restored scan continues at the saved time.
A restart before the first completed checkpoint plans the read again from the start, taking a new service time unless `scan.read-time` fixes one.

That time must remain readable for the duration of the job and any recovery.
The default microsecond-resolution snapshot is usable for about an hour.
Longer reads need point-in-time recovery enabled and an explicitly chosen whole-minute read time within its retention window.
A checkpoint cannot recover a snapshot that Firestore no longer retains.
The [snapshot guide](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/datastream/firestore/#one-snapshot-for-the-whole-read) gives the retention and timestamp rules.

SQL projections limit which top-level fields are fetched, but SQL filters are not pushed down to Firestore.
The DataStream source also accepts an application-built query, including its filters and ordering; that query is read as one split.

## Looking up customer data for an event

A lookup join reads one document by ID while processing a row from another source.
This is separate from a snapshot scan: an uncached lookup reads the document at the time of the request.
The following [reference excerpt](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/firestore/#lookup) assumes an `order_events` input with `order_id STRING`, `customer_id STRING` and a processing-time column declared as `proc_time AS PROCTIME()`:

```sql
CREATE TABLE customers (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'customers',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

SELECT e.order_id, c.name, c.tier
FROM order_events AS e
LEFT JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c
  ON e.customer_id = c.customer_id AND c.tier = 'gold';
```

Only `customer_id` addresses the Firestore read.
The extra equality on `tier` is checked against the converted row after the document is read.
A missing document or one whose tier does not match produces NULL customer columns in this left join.
The `ON` clause does not make Firestore search the collection for gold customers.

`lookup.async = 'true'` allows several reads in flight per lookup subtask.
The connector supports `NONE` and `PARTIAL` caching; a full-table cache is not supported.
With the example's partial cache, a hit can return a previously read row until its ten-minute expiry, even if the document has changed.
The cache uses the complete lookup-key tuple, including additional equality values.
`scan.read-time` has no effect on lookup joins, so this syntax does not perform an event-time historical lookup.

## Mapping document values

The Flink schema is an explicit interpretation of a schemaless collection.
`BIGINT` maps to an integer, `DOUBLE` to a floating-point number, and `TIMESTAMP_LTZ` to a timestamp stored at microsecond precision.
`ROW` and `MAP<STRING, T>` represent maps, and arrays use `ARRAY<T>`.
An `INT` or `DECIMAL` column is not mapped; cast to a supported type deliberately instead of expecting the connector to choose a representation.

Two marker options distinguish values that share a Flink type with ordinary data.
`geo-point-field-paths` marks `ROW<latitude DOUBLE, longitude DOUBLE>` fields as geographical points.
`reference-field-paths` marks strings as document references in the same database, such as `staff/alice`.
The [type-mapping reference](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/firestore/#type-mapping) covers nested paths and arrays.

Reads fail by default if a stored value does not fit its declared type.
`type-mismatch-policy = 'null'` instead makes the nearest enclosing nullable field NULL.
It cannot turn an incompatible top-level `NOT NULL` column into a nullable one, so that case still fails.
This choice is useful when a collection contains older documents written with another field representation, but it should be made knowing which values will be lost from the resulting rows.

## Choosing writes in a DataStream serializer

The DataStream sink fixes the project and database, while each serialized `FirestoreWrite` supplies a document path and operation.
One sink can therefore write to multiple collections in that database.
The operations are `set`, `setMerge`, `create`, `add`, `update` and `delete`.

This abbreviated builder example comes from the [compiled merge example](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/examples/firestore/#merging-fields-into-existing-documents).
It assumes an application's `OrderEvent` type with string-returning `getId()` and `getStatus()` methods.
Imports, the input stream and job execution are omitted; attach the resulting sink with `sinkTo(...)` and enable checkpointing on the environment.

```java
FirestoreSink.<OrderEvent>builder()
        .database(DatabaseDestination.of("my-project"))
        .serializer(
                (event, context) ->
                        FirestoreWrite.setMerge(
                                "orders/" + event.getId(),
                                Map.of(
                                        "status", event.getStatus(),
                                        // A nested map is merged key by key, so other
                                        // keys of "audit" keep their values.
                                        "audit", Map.of("lastStatus", event.getStatus()))))
        .build();
```

The write changes `status` and `audit.lastStatus`, preserving the other keys inside `audit` and the document's other fields.
It creates the document if it is absent.
An `update` would instead require an existing document and replace a named map as a whole.
The serializer can choose `delete` for another record type, or a different collection path for another destination.

`update` and `delete` can also carry a `lastUpdateTime` precondition, so an operation applies only to the document version from which it was computed.
A replay of an already successful conditional write fails that precondition too.
The [conditional-write example](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/examples/firestore/#updating-a-document-only-if-it-has-not-changed) explains how to route those failures rather than repeatedly fail the job on recovery.
Server-side transforms such as `serverTimestamp()` and `increment()` are not supported in 1.2.0.

## Replay and write ordering

The Native-mode sink writes through the client library's `BulkWriter`.
At a checkpoint barrier it waits until every submitted write has an answer.
With the default failure handler, an unhandled write failure fails the job; a configured handler that drops or routes failures changes what a completed checkpoint says about stored output.

After recovery, a write may be submitted again.
Repeating an identical `set`, `setMerge` or `delete` is idempotent for that write.
A repeated `create` is refused if the first attempt already created the document, while `add` can store another copy under a new ID.
There is no exactly-once sink mode.

Idempotence does not preserve the order of different writes to one document.
Concurrent requests and retries can apply an earlier value after a later value, even when both came from the same subtask.
A `set` followed by a `delete` can leave the document present.
Keying the stream does not remove this behavior inside the writer.
This makes one-row-per-key loads a simpler fit than a stream whose correctness requires each document to retain its newest update.
The [delivery reference](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/datastream/firestore/#delivery-guarantee) describes the replay behavior operation by operation.

Two SQL planner details matter for updating jobs too.
On Flink 2.3, an updating statement may require an `ON CONFLICT` clause if the planner cannot establish the required relationship between the query's upsert key and the sink's primary key.
On Flink 2.2 and 2.3, key-only deletes encounter Flink bugs when non-key columns are declared `NOT NULL`; the documented workaround is to keep those columns nullable in both source and sink tables.
The [changelog reference](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/firestore/#changelog-and-ordering) covers both cases.
Neither planner setting supplies ordering inside the Firestore writer.

## Datastore mode

A Datastore-mode database uses the `datastore` SQL identifier from the same JAR.
It has a bounded scan, a lookup join and a sink, with its own DataStream types under `io.github.flink.gcp.connector.datastore`.
The API speaks in entities, kinds and namespaces rather than documents and collections.

| Native-mode SQL | Datastore-mode SQL |
| --- | --- |
| `collection` identifies the collection. | `kind` and optional `namespace` identify the entities. |
| One `STRING` primary key is the document ID. | One `STRING` key is an entity name, or one `BIGINT` key is a numeric ID. |
| A keyless table draws document IDs. | A keyless table allocates numeric IDs through the service. |
| `ROW` and `MAP<STRING, T>` can represent maps. | `ROW` represents an embedded entity; `MAP` has no SQL mapping. |
| Reference markers use document-path strings. | Reference markers use rows holding key components. |

Datastore SQL also defaults to `sink.write-mode = 'merge'`, preserving properties the row does not supply and merging embedded entities recursively.
`replace` replaces the entity, and `update` changes declared properties of an existing entity.
The DataStream sink exposes `upsert`, `upsertMerge`, `insert`, `update`, `updateProperties` and `delete` operations.
Its delivery is at-least-once.
One sink subtask applies writes to one key in received order, using one non-transactional commit at a time with each key included at most once.
If an attempt times out on the client, the service may still apply it after a later commit to the same key.
Writes from different subtasks are not ordered against each other.
The [Datastore SQL guide](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/datastore/) and [DataStream guide](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/datastream/firestore/#datastore-mode) describe those differences.

Neither mode has a catalog that infers the SQL schema.
Declare the fields the job reads and writes, choose a write mode that matches who owns them, and account for the operation's behavior when it is repeated.
The [1.2 quickstart](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/quickstart/firestore/) provides a complete DataStream job to start with.

## What's next for this connector

The [v1.3.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/12) currently includes [opt-in write ordering for Native mode](https://github.com/flink-gcp/flink-connector-gcp/issues/1556), intended to preserve the order in which one sink subtask receives writes to the same document.
A [sampling catalog for both modes](https://github.com/flink-gcp/flink-connector-gcp/issues/1732) is also being considered for that milestone, with schemas inferred from sampled documents or entities; the design may still be declined.

The [v1.4.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/13) now tracks [filter pushdown for Table API scans](https://github.com/flink-gcp/flink-connector-gcp/issues/1829), starting with Native mode, to let the service apply eligible `WHERE` conditions and reduce the data returned to Flink.
For Datastore mode, [the proposal](https://github.com/flink-gcp/flink-connector-gcp/issues/1831) would leave pushdown disabled by default: users would explicitly enable it only when every filtered property is indexed on every entity of the kind.
Datastore filters cannot match unindexed values, so enabling pushdown without that guarantee could omit matching entities; this design may still be declined.
Neither milestone has a release date, and the scope and target versions may change.

This is an independent open-source project.
It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.
