+++
title = "Releasing flink-connector-gcp 1.0.0, Apache Flink connectors for BigQuery, Pub/Sub, Cloud Tasks, Bigtable, and Spanner"
description = "I have released flink-connector-gcp 1.0.0: five independent Apache Flink connectors for Google Cloud, covering BigQuery, Cloud Pub/Sub, Cloud Tasks, Bigtable, and Spanner. Each..."
date = 2026-08-31T19:06:04+09:00
updated = 2026-09-05T20:41:12+09:00
draft = false

[taxonomies]
tags = ["GCP", "Flink"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/08/31/190604"
+++
I have released [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp) 1.0.0: five independent [Apache Flink](https://flink.apache.org/) connectors for Google Cloud, covering BigQuery, Cloud Pub/Sub, Cloud Tasks, Bigtable, and Spanner. Each offers DataStream and Flink SQL / Table APIs. The artifacts are available on Maven Central under [`io.github.flink-gcp`](https://central.sonatype.com/namespace/io.github.flink-gcp).

- Documentation: <https://flink-gcp.github.io/flink-connector-gcp/>
- Source: <https://github.com/flink-gcp/flink-connector-gcp>
- Release: <https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.0.0>

This post explains why I built the project and what it provides. The following posts cover one connector each, with feature details, pipeline examples, and tuning.

## Why I built it

The existing connectors lacked features my pipelines needed, and adding those features upstream was either slow or not an available route. These were the options for each service when I started.

**Pub/Sub.** Apache's [flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub) has seen sparse activity. At the time of writing, its last main-branch commit was in November 2024. That commit added a Table API sink, but a Table API source is still missing. Google's connector in [GoogleCloudPlatform/pubsub](https://github.com/GoogleCloudPlatform/pubsub/tree/master/flink-connector) is well built on the StreamingPull API and moved to Flink 2.2 in June 2026. It remains DataStream-only and, at the time of writing, is not published to a Maven repository. Neither provides the Table API source or the full set of source and sink features I needed.

**BigQuery.** The [Dataproc-maintained connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector) is actively developed, but my July 2026 survey found two gaps. Each sink writes to one fixed table, so it cannot route records to destinations chosen at runtime. It also cannot follow schema changes without redeploying the job.

**Bigtable.** Google's [flink-connector-gcp](https://github.com/google/flink-connector-gcp), a separate project with the same name, provides an exactly-once Bigtable sink with Table API support. I also needed lookup joins for enrichment and a change-stream source.

**Spanner.** I submitted a JDBC dialect and catalog to Flink's JDBC connector ([apache/flink-connector-jdbc#156](https://github.com/apache/flink-connector-jdbc/pull/156), FLINK-37288). It received review feedback, which I appreciate, but has remained open and unmerged since February 2025. Committer time is scarce in a large project. I understand the delay, but needed a working pipeline, and a JDBC dialect would not have covered change streams anyway.

**Cloud Tasks.** I did not find an existing Flink connector at all.

Each project has its own priorities and review queue. I needed features I could not get from those projects, so I built connectors for all five services around a shared design.

## What is in the box

Five connectors, each with two API surfaces:

| Connector     | DataStream    | Flink SQL / Table API       |
|---------------|---------------|-----------------------------|
| BigQuery      | source + sink | scan source (bounded), sink |
| Cloud Pub/Sub | source + sink | source, sink                |
| Cloud Tasks   | sink          | sink                        |
| Bigtable      | source + sink | scan source, lookup, sink   |
| Spanner       | source + sink | scan source, lookup, sink   |

Each release has two version lines: `1.0.0` for the supported Flink 2.x range and `1.0.0-1.20` for Flink 1.20 LTS. The next section explains that policy. Each connector also has a `flink-sql-connector-gcp-*` uber-jar for SQL, available from the release page and installed in Flink's `lib/`.

``` xml
<dependency>
  <groupId>io.github.flink-gcp</groupId>
  <artifactId>flink-connector-gcp-bigquery</artifactId>
  <version>1.0.0</version> <!-- 1.0.0-1.20 for a Flink 1.20 job -->
</dependency>
```

## Which Flink versions are supported, and how

The support policy follows Flink's current and previous minor releases, currently 2.2 and 2.3, plus the 1.20 long-term support line. Supporting both major versions requires two artifact lines.

One artifact covers both supported 2.x minors. The connectors compile against the oldest, 2.2, and run on the newer version. A weekly job verifies this compatibility: it builds the jars once against 2.2, then reruns the test suite against those same jars on the newer minor. If that check finds a break, the documented fallback is separate artifacts per minor, as the Kafka connector publishes. So far, it has found none.

Flink 1.20 uses the same source tree. Before choosing whether to maintain another branch, I measured the API differences affecting these connectors and found two. Each has a same-named file in small per-major source roots, selected by a Maven property at build time. All other source is shared.

The `-1.20` artifacts compile that code for Flink 1.x. Binary compatibility does not extend across majors: a 2.x jar is not expected to load on a 1.20 cluster, or vice versa. The LTS line therefore has a separate compilation and version suffix.

Expanding the supported range requires an explicit change. A scheduled check reports new Flink minors and lists the required edits. Support is claimed only after the compatibility job passes. The weekly matrix also builds against the next unreleased Flink snapshot to catch upstream breaks before release.

## The features I built it for

Everything below is documented with runnable examples on the [documentation site](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/).

**Dynamic destinations.** Every DataStream sink can derive its destination from each record: a BigQuery table, Pub/Sub topic, Cloud Tasks queue, Bigtable table, or Spanner table. A multi-tenant pipeline can route to per-tenant tables through one sink and accept new tenants without restarting.

**Auto-creation.** The BigQuery and Pub/Sub sinks can create a table or topic on first write. A new tenant's first record can therefore create its destination and land in it within the same job. Bigtable also supports opt-in auto-creation when a table schema is declared.

**Schema evolution.** The BigQuery sink can follow changes to the destination table's schema without restarting the job. Adding a column no longer requires a redeployment.

**BigQuery batch ingestion from a streaming job.** Alongside its at-least-once and exactly-once Storage Write API paths, the sink offers `FILE_LOADS`. The streaming job stages Avro files, or optionally Parquet, in Cloud Storage and commits them through load jobs at checkpoints. Load jobs have no streaming-insert cost, making this economical for high-volume pipelines that can tolerate checkpoint-aligned latency. The job retains the same sink API and dynamic destinations.

**CDC into BigQuery.** The BigQuery Table API sink accepts changelog streams and uses the Storage Write API's CDC support to write upserts and deletes with explicit ordering. Flink SQL can therefore materialize a changing table in BigQuery.

**Change streams out of Spanner (and Bigtable).** Both connectors provide change-stream sources. Combining the Spanner source with the BigQuery CDC sink makes Spanner-to-BigQuery replication a single Flink SQL job.

**Pub/Sub attributes, ordering, and seek in SQL.** Message attributes and ordering keys are metadata columns on both the source and sink. A source table reads `attributes` as `MAP<STRING, STRING>` and can use the publish timestamp as event time. A sink writes attributes and ordering keys from columns.

The start position is also a table option. Declaring `earliest-retained`, `latest`, or a timestamp makes the connector seek the subscription on startup, replacing a manual `gcloud pubsub subscriptions seek` before a backfill. Neither existing Pub/Sub connector offers a Table API source.

**Lookup joins against Bigtable and Spanner.** Both provide Table API lookup sources with caching options. A Flink SQL enrichment join can use `FOR SYSTEM_TIME AS OF` to look up a Bigtable row or query a Spanner table.

## How it is tested

The test coverage matters as much as the feature list, especially where emulators behave differently from the real services.

Every pull request runs integration tests for the affected connectors against service emulators. The weekly matrix builds against every supported Flink version. A [japicmp](https://siom79.github.io/japicmp/) gate compares each build's public API with the published 1.0.0 release.

Documentation checks compare option and metric tables with the source. CI also compiles the Java examples, validates Flink SQL examples through the planner, and executes GoogleSQL examples against a Spanner emulator.

**A weekly E2E suite also runs against real Google Cloud services.** It covers behavior emulators cannot establish: BigQuery load jobs and default-stream fan-out, serializer fidelity against real column types, ordered Pub/Sub subscription delivery and dead-letter forwarding, Cloud Tasks dispatch to an App Engine handler, and Bigtable and Spanner rejection statuses.

A separate gated manual probe checks BigQuery schema evolution on the real service. It stays outside the weekly suite because schema propagation can outlast a CI runner.

Tests create and delete real instances where needed. The harness also checks that the gated test classes actually ran. Missing credentials must not produce a green result from a suite that skipped its tests.

## How it was built

Claude and Codex helped write much of this repository under the support programs acknowledged below. I make the architecture decisions and give every change its final review. Two practices make that process inspectable.

First, the repository records its decisions in close to 150 architecture decision records. Changes to a settled design are expected to read and update the relevant records. This preserves the reasoning for the next contributor, whether human or model.

Second, every pull request follows a fixed review sequence. The first self-review asks whether the code does what the description says. The second asks whether the description itself is true. A different model then performs an independent review: Codex reviews Claude's work, and Claude reviews Codex's.

The independent reviewer receives the diff without the pull-request description or author notes. Its findings are hypotheses to verify. Using another model mattered more than I expected: one measured case in the decision records showed repeated same-model reviews stopping at a shallow fix, while the other model identified the underlying race condition. The pull-request review threads are public.

## What's next

As of August 2026, the tracker groups planned features into these milestones:

- **[v1.1.0](https://github.com/flink-gcp/flink-connector-gcp/milestone/10)** is mostly about extending Bigtable writes: check-and-mutate, read-modify-write, aggregate column families, and response-bearing writes exposed up to Flink SQL, plus wider filter pushdown for BigQuery and Spanner and release-pipeline automation.
- **[v1.2.0](https://github.com/flink-gcp/flink-connector-gcp/milestone/11)** plans a sixth connector, for Firestore / Datastore.
- **[v1.3.0](https://github.com/flink-gcp/flink-connector-gcp/milestone/12)** plans observability work: exporting the Google client libraries' own metrics through Flink's metric reporters, so the connector-internal RPC behavior shows up on the same dashboards as the job.

Issues and feedback are welcome on the [tracker](https://github.com/flink-gcp/flink-connector-gcp/issues). Reports from use cases unlike mine are especially useful because they expose gaps in what I can test.

The following posts cover one connector each, with feature details, an end-to-end pipeline, and tuning guidance.

## Acknowledgements

This project receives support from Anthropic through [Claude](https://claude.com/) and [Claude for OSS](https://claude.com/contact-sales/claude-for-oss), its program for open-source developers, and from OpenAI through [Codex](https://openai.com/codex/) and [Codex for Open Source](https://openai.com/form/codex-for-oss/), its program for open-source maintainers.

Building five connectors with this review process takes substantial model time. These programs made it viable as a personal project outside an employer. I am grateful to both.

------------------------------------------------------------------------

*This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.*
