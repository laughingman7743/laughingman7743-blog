+++
title = "The Pub/Sub connector in flink-connector-gcp, part two of the connector series"
description = "This is the second post in the flink-connector-gcp series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. The release post introduces the project, and..."
date = 2026-09-03T00:13:09+09:00
updated = 2026-09-05T21:04:38+09:00
draft = false

[taxonomies]
tags = ["GCP", "Flink", "PubSub"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/09/03/001309"
+++
This is the second post in the [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp) series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. [The release post](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-0-0/) introduces the project, and the [previous post](https://blog.laughingman7743.org/posts/flink-connector-gcp-bigquery/) covers BigQuery.

This post follows a Pub/Sub order pipeline in Flink SQL and a DataStream sink that routes records to different topics. It also explains the source's streaming-pull design, seek-on-start behavior, and tuning for checkpoint-based acknowledgment.

- Documentation: <https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/pubsub/>
- Maven: `io.github.flink-gcp:flink-connector-gcp-pubsub:1.0.0` (`1.0.0-1.20` for Flink 1.20); for the SQL client, the `flink-sql-connector-gcp-pubsub` uber-jar from the [release page](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.0.0) goes into `lib/`

Two Pub/Sub connectors already existed when I started. Apache's [flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub) uses the legacy `SourceFunction` API over unary `Pull`. At the time of writing, its last main-branch commit was in November 2024. That commit added a Table API sink (FLINK-36077), but a Table API source is still missing.

Google's connector in its [pubsub repository](https://github.com/GoogleCloudPlatform/pubsub/tree/master/flink-connector) is well built on the StreamingPull API and moved to Flink 2.2 in June 2026. It remains DataStream-only and, at the time of writing, is not published to a Maven repository.

I needed complete Flink SQL support, with both source and sink tables, and message attributes queryable as columns. I also wanted a start-position table option, a policy that sends poison messages through Pub/Sub's dead-lettering, and a sink that chooses a topic for each record at runtime.

## Why the source consumes through streaming pull

Apache's connector chose unary pull during its review ([FLINK-9311](https://github.com/apache/flink/pull/6594)). An earlier implementation used the high-level `Subscriber`, but the final design called unary `Pull` on the blocking gRPC stub. The review gave three reasons: backpressure blocking `sourceContext.collect()` naturally stops the pull loop; users avoid tuning flow control; and removing the intermediate queue reduces memory use and latency. I think that choice suited its source framework.

Two of those reasons depend on `SourceFunction`. Its hand-written `run()` loop needs queues and lock coordination to bridge an asynchronous client. In FLIP-27, Flink provides that bridge: `SplitReader.fetch()` is a pull loop, while `SourceReaderBase` owns the element queue and backpressure between the fetcher and task thread.

Flow-control tuning remains a concern. This connector exposes the subscriber settings rather than hiding them, and the tuning section explains how to size them.

Lease extension and ordering led me to the high-level client. Unary `Pull` returns acknowledgment ids without managing their leases. Apache's connector does not extend deadlines, so messages must be acknowledged within the subscription deadline, at most 600 seconds. Its documentation accordingly requires a much shorter checkpoint interval. The high-level client extends leases automatically, up to an hour by default, which better suits checkpoint-based acknowledgment.

The high-level client also provides sequential dispatch per ordering key. Using unary pull would require implementing that behavior separately. Streaming pull does cost more gRPC CPU. The FLINK-9311 review measured the synchronous design at 3,000 messages per second with one-second checkpoints and 20,000 with 50-millisecond checkpoints, showing its dependence on checkpoint frequency.

Each split represents one streaming-pull connection to a subscription. It stores no progress position because Pub/Sub has no offset to resume from. In default unordered mode, the plan creates `max(|subscriptions|, parallelism)` splits so every subscription is consumed and every subtask can work. Per-key ordering uses exactly one split per subscription.

The plan is deterministic from the subscription list, ordering mode and parallelism, and is recomputed on each start. Restoring a savepoint with a different parallelism therefore reassigns the splits. A MiniCluster test verifies rescaling in both directions.

Pub/Sub holds delivery state on the server. The source acknowledges a message when the checkpoint covering its emission completes. A failure before that leaves the message unacknowledged for redelivery, giving at-least-once delivery. Checkpoints contain no message data, so source recovery does not require retaining one.

Checkpointing is still mandatory. Without it, messages remain unacknowledged until subscriber flow control fills and consumption stalls. If messages are waiting and no checkpoint arrives within ten minutes, the reader fails the job with an error naming `execution.checkpointing.interval`.

The reader checks behavior because it cannot reliably inspect the configured interval. `env.enableCheckpointing(...)` updates the job configuration, while the reader receives the TaskManager configuration. An absent interval there does not prove checkpointing is disabled.

## The order pipeline in Flink SQL

The example consumes JSON order events from a topic, uses the publish timestamp as event time, calculates hourly totals per customer, and publishes them to another topic. Pub/Sub-specific behavior is expressed through columns and options. This example assumes the subscription exists; a later section covers resource creation. The source table is:

``` sql
CREATE TABLE incoming_orders (
  order_id     STRING,
  customer_id  STRING,
  amount       INT,
  message_id   STRING              METADATA FROM 'message-id'   VIRTUAL,
  publish_time TIMESTAMP_LTZ(3)    METADATA FROM 'publish-time' VIRTUAL,
  attrs        MAP<STRING, STRING> METADATA FROM 'attributes'   VIRTUAL,
  ordering_key STRING              METADATA FROM 'ordering-key' VIRTUAL,
  subscription STRING              METADATA FROM 'subscription' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'orders-sub',
  'format'       = 'json'
);
```

A Pub/Sub message contains a payload, attributes and an ordering key. `format` decodes the payload into physical columns. The remaining fields are exposed as metadata.

`publish-time` records when the service received the message, truncated to milliseconds and never rounded up. It provides an event-time column for the watermark without requiring a timestamp in the payload.

`attributes` is always a map, empty when the message has none. Queries can filter with `WHERE attrs['channel'] = 'mobile'` or group by an attribute. This avoids writing a custom deserializer, one of my original requirements.

`ordering-key` is `NULL` when no key is set. Pub/Sub represents that absence as an empty string; the connector maps it to SQL null.

`subscription` contains the full resource name, such as `projects/my-project/subscriptions/orders-sub`. The configuration option accepts a bare id, but the metadata uses the API's resource-name form, suitable for joins with audit logs or Cloud Asset Inventory. Consequently, `WHERE subscription = 'orders-sub'` matches nothing, a distinction emphasized in the documentation.

The sink maps a column back to message attributes:

``` sql
CREATE TABLE customer_totals (
  customer_id STRING,
  total       INT,
  attrs       MAP<STRING, STRING> METADATA FROM 'attributes'
) WITH (
  'connector' = 'pubsub',
  'project'   = 'my-project',
  'topic'     = 'customer-totals',
  'format'    = 'json'
);

INSERT INTO customer_totals
SELECT customer_id,
       CAST(SUM(amount) AS INT),
       MAP['window-start', CAST(window_start AS STRING)]
FROM TABLE(TUMBLE(TABLE incoming_orders, DESCRIPTOR(publish_time), INTERVAL '1' HOUR))
GROUP BY window_start, window_end, customer_id;
```

Both tables adapt the [source-backed examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/pubsub/), which CI validates through Flink's planner. The combination is mine.

The window TVF makes the totals insert-only, which this sink requires. Pub/Sub cannot express retractions. An updating query such as a plain `GROUP BY customer_id` therefore fails during planning, preventing update and delete rows from being published as ordinary messages.

The source instead adopts the format's changelog mode. A changelog format can therefore travel over Pub/Sub, although the at-least-once transport may redeliver a retraction.

This pipeline uses event time without requesting ordered delivery. The watermark permits five seconds of lateness in this example, a bound to size for the topic. Arrival order within that bound does not change the hourly totals. Transport-level ordering addresses a narrower requirement and is covered below.

A null key or value in the attributes map fails the write. Pub/Sub supports neither, and silently removing an entry would lose data without the query knowing. Filter such entries in the query before writing.

Install the `flink-sql-connector-gcp-pubsub` uber-jar in Flink's `lib/` or load it with `ADD JAR`. Its bundled dependencies are relocated so it can coexist with the BigQuery and Bigtable uber-jars.

## A fan-out sink that routes on record content

Dynamic destinations were my main reason for building this sink. A resolver chooses a topic from each record's values or attributes, allowing one input stream to fan out to topics selected at runtime. The release post introduced this feature across all five connectors. On Pub/Sub, the sink is:

``` java
DataStream<OrderEvent> orders = ...;

orders.sinkTo(
        PubSubSink.<OrderEvent>builder()
                .destinationResolver(
                        (event, context) ->
                                TopicDestination.of("my-project", "orders-" + event.region()))
                .serializer(
                        PubSubSerializationSchema.payload(new OrderEventSchema())
                                .withAttributes(e -> Map.of("tenant", e.tenantId())))
                .topicCreateOptions(
                        TopicCreateOptions.builder()
                                .messageRetention(Duration.ofDays(7))
                                .build())
                .build());
```

This composite adapts the [source-backed examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/pubsub/), which CI compiles. The combination is mine.

The resolver runs once per record, before serialization. Any retained field can determine the route, including message attributes preserved by a passthrough deserializer. `TopicDestination` holds only project and topic identity, with equality over both. It is cheap to allocate and keys the writer's publisher map.

The writer manages publishers as destinations appear and disappear. It creates an SDK publisher lazily for each active topic and retains at most 100 (`maxActivePublishers`). At capacity, a new topic evicts the least-recently-used publisher with no pending messages. If none is clean, the writer drains all publishers first. It never discards an in-flight message.

After a successful checkpoint flush, publishers idle for over an hour (`destinationIdleTimeout`) are released. A later record recreates the publisher. Releases use bounded two-phase shutdowns with overlapping waits, so several releases share one timeout budget. A shutdown that times out during eviction fails the running task, preventing abandoned gRPC channels from accumulating. At final close, teardown continues even if shutdown times out.

The writer flushes all records at each checkpoint barrier instead of storing them in Flink state. A savepoint-less redeploy can duplicate messages but cannot lose messages covered by a completed checkpoint. The next section explains how a newly routed topic is created, allowing a new region in this example to receive its stream without redeployment.

SQL sinks write to the single topic named in their DDL. A SQL job can fan out to topics known at planning time using multiple `INSERT` statements in a `STATEMENT SET`. A topic computed from record contents requires the DataStream API shown above. A Kafka-style `topic` metadata column was considered and declined because `INSERT` branching covers the SQL cases the planner can express.

## Creating what is missing, on both sides

The sink creates missing topics under the default `CREATE_IF_NEEDED`. Creation begins only after a publish returns `NOT_FOUND`: the sink parks the messages, creates the topic, and republishes with bounded backoff. Existing topics incur no admin call.

A topic can use service defaults, so the disposition alone authorizes creation. `TopicCreateOptions` optionally adds retention, a CMEK key and storage regions.

Source creation is authorized by supplying settings for each subscription; there is no disposition option. A subscription needs a topic binding that only the pipeline author can choose. Those settings must be per subscription. Sharing one topic binding across several subscriptions would make the job consume a full copy of the topic's stream from each.

In SQL, the mapping looks like `'scan.auto-create.topics.orders-sub' = 'orders'`. Its keys must match the subscription list exactly. The source creates subscriptions only: every mapped topic must already exist.

Creation is idempotent on both sides. `ALREADY_EXISTS` counts as success, so concurrent jobs need no coordination. Existing resources are left as they are; creation settings are neither applied nor compared.

A new subscription normally has no backlog from before its creation. To replay that history, enable topic-level retention, exposed as `messageRetention` in the sink's creation settings. The start-position options below then determine where to begin within the retained history.

## What a start position does to a subscription

`scan.startup.mode`, or `startPosition(...)` in the DataStream API, selects the starting position. Every non-default value performs a seek:

| Mode | Behavior |
|----|----|
| `continue-from-subscription` (default) | Starts wherever the subscription already is; the only mode that issues no seek |
| `earliest-retained` | Replays the whole retained backlog |
| `latest` | Discards the existing backlog |
| `timestamp` (with `scan.startup.timestamp-millis`) | Everything published before the instant is marked acknowledged, everything after unacknowledged |

These options replace a manual `gcloud pubsub subscriptions seek` before submitting a backfill. A seek changes server-side subscription state shared by all consumers; there is no per-consumer offset. A job using a non-default start position should therefore own its subscription.

The enumerator performs the seek on the first start and records it in checkpointed state. Restoring that state resumes consumption without seeking again.

A redeploy without a savepoint seeks again because the record of the previous seek is gone. A job that repeatedly crashes before its first completed checkpoint also repeats the seek. `latest` resolves against the clock each time and is therefore not reproducible. Use `timestamp` for an exact boundary.

Retention determines what a backward seek can recover. Already-acknowledged messages are available only if the subscription retains acknowledged messages or the topic retains messages. With neither enabled, only unacknowledged messages can be recovered. The startup check warns about that combination.

Before assigning splits, the enumerator checks every subscription. It rejects per-key ordering on a subscription without message ordering, which would otherwise emit unordered messages. It also rejects subscriptions with exactly-once delivery enabled: their acknowledgment ids expire with the deadline and are invalidated on redelivery, while this source retains ids for a whole checkpoint interval.

All subscriptions are checked before any seek runs, so a rejected configuration cannot leave another subscription already rewound. These checks capture startup settings only. They do not detect settings changed while the job is running.

## Per-key ordering, and where it stands

The connector supports per-key ordering end to end, but I expect most pipelines, including mine, to leave it disabled. Google's [ordering documentation](https://cloud.google.com/pubsub/docs/ordering) describes the costs: lower publish availability, higher end-to-end latency, a 1 MB/s publish limit per ordering key, and at most one outstanding batch per key for pull subscriptions. Checkpoint-based acknowledgment adds another constraint here: roughly one batch per key per checkpoint interval.

In my experience, many apparent ordering requirements are event-time requirements. The example's watermark handles those within its declared lateness bound without requiring ordered transport. Pub/Sub serves message fan-out rather than a log, and transport ordering is a separate choice.

For jobs that need ordering, both APIs support it on the source and sink. A source table with `scan.ordering-mode = 'per-key'` assigns each subscription to one subtask over one streaming-pull connection. It guarantees per-key order at the source output; downstream exchanges must partition by the key to preserve it.

On a sink table, `sink.message-ordering.enabled` and an `ordering-key` metadata column automatically route keyed rows to one writer subtask. The pair is validated during planning. The [ordering documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/pubsub/#message-ordering) details the costs and conditions.

The Pub/Sub emulator cannot validate ordered consumption. The client's per-key callback serialization depends on a subscription property the emulator does not set. Callbacks can therefore arrive out of order even without Flink involved. The connector verifies ordered consumption in its weekly real-service suite.

Enabling publish ordering also changes retry behavior. With `enableMessageOrdering`, the SDK replaces its retry settings with unlimited retries, including for unkeyed messages. During an outage, even a configured retry timeout no longer ends the wait.

The connector rejects `retryTotalTimeout` and `retryMaxAttempts` when ordering is enabled, since the SDK would ignore them. It also adds `publishProgressTimeout`, a watchdog that fails the job when no publish completes within that interval. In one August 2026 measurement against an unreachable endpoint, an unordered publish failed after 591 seconds, while an ordered flush was still waiting at 700 seconds. That gap informed the watchdog's default.

## Failure routing on both sides

On the source, `scan.deserialization-failure-policy` handles messages the format cannot decode. The default, `fail`, leaves the message unacknowledged and fails the job. A permanently bad message can then fail each restart. `drop` acknowledges and discards it, with a counter and rate-limited logging. `nack` returns it for redelivery so the subscription's dead-letter policy can handle it.

I wanted `nack` so a poison message could reach a dead-letter topic for later replay while the pipeline kept running. It uses Pub/Sub's service-side machinery without adding dead-letter code to the job. Startup rejects `nack` on a subscription with no dead-letter policy, since an undecodable message would otherwise keep being redelivered without failing the job.

Dead-lettering counts deliveries regardless of cause, so unrelated job restarts contribute to the same counter. The Pub/Sub service account also needs publish and subscribe grants. Without them, the service continues redelivering instead of forwarding.

For more complex handling, a DataStream deserializer receives the entire message and writes to a `Collector`. It can emit a bad-record variant instead of throwing, then use a side output to route it within the checkpointed pipeline.

The sink routes two failure types to its configurable handler: serializer rejections and messages individually rejected by the service with `INVALID_ARGUMENT`. Other failures, including outages, remain fatal. Routing an outage to a dropping handler would lose messages instead of applying backpressure and restarting.

Individual confirmation matters because `Publish` is an all-or-nothing batch RPC. The SDK reports the batch status for every message, and real-service measurements found no indication of which message caused it. The sink republishes the failed batch one message per request. Only messages rejected individually reach the handler; valid neighbors are published.

After 100 confirmed rejections in a row without a success, `maxConsecutiveRejections` fails the job even under a dropping policy. Dropping occasional anomalous records should not allow a wholly rejected stream to drain silently.

The handler can use the same `PubSubDeadLetterQueue` as the BigQuery post. The shared failure contract lets this implementation serve every connector. Each published element carries attributes identifying the connector, destination, error, timestamp and subtask. It does not create its topic, since an automatically created dead-letter destination may have no consumer.

## Tuning around checkpoint-shaped acknowledgement

Google's documentation gives no recommended flow-control values beyond sizing them to the client machines. The SDK defaults protect memory; they are not throughput targets. The connector leaves these settings at their SDK defaults and documents a sizing rule for checkpoint-based acknowledgment. All messages received since the last completed checkpoint count against the outstanding-message limit:

```
flowControlMaxOutstandingElementCount ≳ peak messages/s × checkpoint interval
```

Below that bound, the client stops pulling before the checkpoint completes, making throughput depend on checkpoint frequency. The SDK default of 1,000 outstanding messages saturates at a hundred messages per second with a 10-second checkpoint interval. Higher limits increase both reader memory use and the number of messages replayed after failure.

`maxAckExtensionPeriod`, one hour by default, must also comfortably exceed the checkpoint interval. Otherwise leases expire before the covering checkpoint can acknowledge the messages, causing redelivery.

The connector adds a hard buffer budget of 10,000 messages or 64 MiB of serialized data per source reader, whichever is exceeded first. It covers cases where SDK flow control is insufficient, such as a split paused by watermark alignment or a downstream operator that stops consuming.

Measurements showed why the extra bound is needed. When a buffered message exhausts its lease-extension budget, the client stops extending it and releases its flow-control permit. Pub/Sub redelivers the message, but the connector still holds the original. Released permits allow another wave of intake, mostly duplicates, growing the buffer by roughly one flow-control window per wave. The emulator stopped after two waves; the real service continued.

The connector bounds the buffer and stops a paused split's subscriber entirely if it outgrows that bound. On resume, it reopens the subscriber. Returning the leases lets Pub/Sub redeliver to a consumer that can make progress.

The sink caps unacknowledged publishes at 1,000 messages and 64 MiB per writer subtask. Count alone is insufficient: Pub/Sub accepts messages up to 10 MiB, so 1,000 could occupy about 10 GiB. Retries retain them for up to the SDK's default 600-second timeout, or indefinitely with ordering enabled. A partial outage is therefore when retained data can peak.

The 64 MiB default is below the Java subscriber's 100 MB SDK default, which applies per client rather than per subtask. Size `maxInFlightBytes` so its total across sink subtasks on a TaskManager fits the heap budget. With default limits, bytes become the binding cap only above roughly 64 KiB per message.

The connector uses its own bound because the SDK flow controller blocks the task thread instead of yielding to Flink's mailbox. With ordering enabled, the SDK also leaks permits on its per-key cancellation path, eventually exhausting the budget and hanging the thread without an exception. The design record documents that behavior.

As with BigQuery, the [connector documentation](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/pubsub/) records measurements beside the design decisions. The [configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/pubsub/) lists all options. Those experiments provide a starting point for adjusting defaults to another workload.

## What's next

Near-term milestones focus on other connectors, including Bigtable write extensions in v1.1.0. Pub/Sub's roadmap is driven by workload reports on the [issue tracker](https://github.com/flink-gcp/flink-connector-gcp/issues).

The next post covers Spanner, whose change-stream source feeds the BigQuery CDC sink from the first post. Together they provide Spanner-to-BigQuery replication in one Flink SQL job.

------------------------------------------------------------------------

*This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.*
