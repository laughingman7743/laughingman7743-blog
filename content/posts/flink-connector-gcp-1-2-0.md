+++
title = "Releasing flink-connector-gcp 1.2.0 with Firestore and SQL catalogs"
description = "flink-connector-gcp 1.2.0 adds Firestore Native and Datastore mode connectors, read-only SQL catalogs, and Bigtable map families, with migration notes for Spanner."
date = 2026-10-17
draft = false

[taxonomies]
tags = ["Flink", "GCP", "Firestore", "BigQuery", "Bigtable", "Spanner"]
+++

I have released **flink-connector-gcp 1.2.0**.
The largest addition is a Firestore connector covering both Native mode and Datastore mode.
The release also adds SQL catalogs for BigQuery, Spanner and Bigtable, and lets a Bigtable column family be represented as a map when its qualifiers are not known in advance.

- [Release notes and SQL connector downloads](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.2.0)
- [Version 1.2 documentation](https://flink-gcp.github.io/flink-connector-gcp/1.2/)
- [Source at v1.2.0](https://github.com/flink-gcp/flink-connector-gcp/tree/v1.2.0)

The Maven group remains `io.github.flink-gcp`.
Use version `1.2.0` with Flink 2.2 or 2.3, and `1.2.0-1.20` with Flink 1.20.
For SQL deployments, put the matching `flink-sql-connector-gcp-*` JAR in the cluster's `lib/` directory.
The supported Flink versions are unchanged from 1.1.0.

## Firestore in both database modes

The new `flink-connector-gcp-firestore` module reads and writes Firestore databases in either mode.
Each mode has a DataStream sink and a bounded source, plus a Table API connector with a sink, a bounded scan and lookup joins.
The SQL identifiers are `firestore` and `datastore`; both are included in the same `flink-sql-connector-gcp-firestore` JAR.

In Native mode, a SQL table maps documents in one collection to rows.
A primary key supplies the document ID, while a table without a primary key creates a new document for every row.
The default write mode, `merge`, preserves stored fields the table does not declare.
`replace` makes the row the whole document, and `update` changes the declared fields of an existing document.
DataStream jobs can choose the operation and document path per record, including paths in different collections of the configured database.

Datastore mode has the same three SQL write-mode names, with entities and properties in place of documents and fields.
A table addresses one kind and namespace; its primary key is a string name or a numeric ID.
The DataStream API exposes Datastore operations through its own types under `io.github.flink.gcp.connector.datastore`.

Both sources read a snapshot and finish; neither is a change-stream source.
Both sinks provide at-least-once delivery with checkpointing.
Replay behavior depends on the write operation and key, and Native-mode writes to the same document are not ordered.
Those distinctions matter when choosing between loading one row per key, maintaining changing values, and appending records under generated IDs.
The [Firestore SQL reference](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/firestore/) and [Datastore SQL reference](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/datastore/) describe the supported types and delivery constraints.
The [Firestore connector article](https://blog.laughingman7743.org/posts/flink-connector-gcp-firestore/) covers SQL and DataStream usage in more detail.

## SQL catalogs from service metadata

Previously, using an existing BigQuery, Spanner or Bigtable table required declaring its schema again in Flink.
The new catalogs derive that declaration from service metadata.
For example, the following [BigQuery reference example](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/bigquery/#catalog) makes the datasets of `my-project` available as Flink databases:

```sql
CREATE CATALOG bq WITH (
  'type' = 'bigquery',
  'project' = 'my-project',
  'default-database' = 'analytics'
);

USE CATALOG bq;
```

Install the BigQuery SQL connector JAR and configure credentials for the project's existing datasets first.
`SHOW DATABASES`, `SHOW TABLES` and `DESCRIBE` then discover their tables and schemas.
With `analytics` selected by the catalog's default, `events` refers to `my-project.analytics.events`.

The three catalogs follow the services' resource structure:

| Catalog | Scope and table mapping |
| --- | --- |
| BigQuery | One project, with datasets as databases and schemas derived from table metadata. |
| Spanner | One instance, with its databases and their tables, supporting both database dialects. |
| Bigtable | One instance, with a `_key` column and one map column per column family. |

“Read-only” describes catalog metadata operations.
The catalogs refuse table and database creation, alteration and deletion.
Resolved tables still support the connector's normal reads and writes, and Spanner and Bigtable tables can be used in lookup joins.
Per-statement `OPTIONS` hints select tuning or other connector behavior without putting those choices into catalog-wide defaults.

Firestore and Datastore do not have catalogs in this release.
Firestore collections have no stored schema to derive a Flink table from, and Datastore's property metadata cannot distinguish an array from a scalar value.
Their SQL tables therefore keep explicit column declarations.

## Bigtable families with variable qualifiers

A Bigtable column family can now be declared as `MAP<STRING, V>` or `MAP<BYTES, V>` alongside the existing `ROW` form.
The map's keys are cell qualifiers.
This lets a table read qualifiers that vary between rows without listing every qualifier in its DDL, and gives the Bigtable catalog a representation for families whose qualifiers are absent from service metadata.

For `upsert` and `keep-latest` writes, `sink.map-family.update-mode` chooses what happens to qualifiers absent from the incoming map.
The default, `merge`, leaves them stored.
`replace` deletes the family and writes the incoming entries in the same row mutation.
An empty map under `replace` therefore clears the family; under `merge` it contributes no writes, and a row left with nothing to write is rejected rather than skipped.
The `aggregate`, `append` and `increment` write modes also accept map families, with their own operation semantics.
The [map-family reference](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/bigtable/#map-column-families) covers value types and write-mode restrictions.

## BigQuery loading and table metadata

For `FILE_LOADS` whose final step copies into a column- or integer-range-partitioned or clustered destination, temporary tables now carry the destination's partitioning and clustering layout.
This fixes final copy jobs that BigQuery refused when those layouts differed.
The new `sink.file-loads.temp-table-expiration` option, `tempTableExpiration(...)` in Java, gives these layout-preserving temporary tables an expiration of one day by default; each commit attempt refreshes it.
Temporary tables without that layout, including the query-based `WRITE_TRUNCATE_DATA` path, continue to rely on the dataset's default table expiration.

When the sink creates a table, `sink.table-create.description` and `sink.table-create.labels` can supply its description and labels.
These options leave existing tables unchanged.
A separate fix makes schema evolution patch only the schema, conditioned on the table's etag, so it does not write back stale descriptions, labels or expiration settings after another client changes them.

Parquet staging files now roll at the row-group boundary expected to be nearest `maxStagingFileBytes`, correcting the previous behavior of rolling at about twice that setting.
The estimate uses the preceding row group's size, so changing compressibility within a file can still carry it further past the target.
The [BigQuery DataStream guide](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/datastream/bigquery/) explains the load path and its tuning options.

## Recovery evidence and smaller fixes

This release includes results from deployed recovery trials for BigQuery, Pub/Sub and Cloud Tasks sinks on GKE Autopilot.
The runs exercised savepoint upgrades and JobManager failovers; the BigQuery exactly-once and Pub/Sub sinks were also tested through TaskManager loss and rescaling in both directions.
Each trial's output check found no lost record.
These are results for the recorded workloads and faults, with the limits documented in the findings, rather than a guarantee for every deployment.
The [release notes](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.2.0) link the evidence and describe the separate `FILE_LOADS` checkpoint-finalization measurements.

Cloud Tasks now retries `INTERNAL` and `CANCELLED` responses from `CreateTask` within the connector's recovery budget in both delivery modes.
As with a timeout, retrying an unnamed task after an uncertain result can create it twice.
Cancellation initiated by the sink itself does not enter that retry path.

The release also rejects duration values that cannot be expressed in milliseconds with an error naming the option, and updates the Google Cloud `libraries-bom` from 26.87.0 to 26.90.0.

## Upgrading from 1.1.0

There is one incompatible Java API change: five constants in the `@PublicEvolving` class `SpannerConnectorOptions` were renamed along with their SQL option keys.

| Former key | New key | Former Java constant | New Java constant |
| --- | --- | --- | --- |
| `schema` | `named-schema` | `SCHEMA` | `NAMED_SCHEMA` |
| `schema.json-field-paths` | `json-field-paths` | `SCHEMA_JSON_FIELD_PATHS` | `JSON_FIELD_PATHS` |
| `schema.uuid-field-paths` | `uuid-field-paths` | `SCHEMA_UUID_FIELD_PATHS` | `UUID_FIELD_PATHS` |
| `schema.proto-type-names` | `proto-type-names` | `SCHEMA_PROTO_TYPE_NAMES` | `PROTO_TYPE_NAMES` |
| `schema.enum-type-names` | `enum-type-names` | `SCHEMA_ENUM_TYPE_NAMES` | `ENUM_TYPE_NAMES` |

Java code referring to an old constant needs to use its replacement.
The connector still accepts the former SQL keys as deprecated aliases, with a warning naming the new key.
Use the current keys when updating saved table definitions too: Flink catalogs that serialize table options as properties can treat `schema` and `schema.*` as reserved keys and lose them.
The [Spanner migration notes](https://flink-gcp.github.io/flink-connector-gcp/1.2/docs/connectors/table/spanner/#former-option-keys) explain that distinction.

Two configurations also fail earlier than they did in 1.1.0.
Pub/Sub sink retry settings that the SDK publisher would reject on a TaskManager are now rejected before job submission.
Spanner and Bigtable lookup tables reject a negative `lookup.partial-cache.max-rows` during planning.

For SQL jobs with updates or deletes, the Table connector references also document Flink 2.3's `ON CONFLICT` requirements and the Flink 2.2/2.3 issues affecting key-only deletes.
Check those notes when adapting a query to one of the new connectors or catalogs.
Use the [1.2 documentation](https://flink-gcp.github.io/flink-connector-gcp/1.2/) for this release; Development follows changes after it.

This is an independent open-source project.
It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.
