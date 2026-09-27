+++
title = "The Cloud Tasks connector in flink-connector-gcp, part five of the connector series"
description = "This is the final post in the flink-connector-gcp series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. The release post introduces the project; the..."
date = 2026-09-06T12:31:17+09:00
draft = false

[taxonomies]
tags = ["GCP", "Flink", "CloudTasks"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2026/09/06/123117"
+++
This is the final post in the [flink-connector-gcp](https://github.com/flink-gcp/flink-connector-gcp) series, about the Apache Flink connectors for Google Cloud I released as 1.0.0. [The release post](https://blog.laughingman7743.org/posts/flink-connector-gcp-1-0-0/) introduces the project; the previous posts cover [BigQuery](https://blog.laughingman7743.org/posts/flink-connector-gcp-bigquery/), [Pub/Sub](https://blog.laughingman7743.org/posts/flink-connector-gcp-pubsub/), [Spanner](https://blog.laughingman7743.org/posts/flink-connector-gcp-spanner/) and [Bigtable](https://blog.laughingman7743.org/posts/flink-connector-gcp-bigtable/).

This post uses Cloud Tasks to turn enriched orders into rate-limited calls to a partner API. It covers request construction in SQL, task-id deduplication, authentication, body formats, queue sharding, failure handling and tuning, as well as the exactly-once mode planned for v1.1.0.

- Documentation: <https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/cloudtasks/>
- Maven: `io.github.flink-gcp:flink-connector-gcp-cloudtasks:1.0.0` (`1.0.0-1.20` for Flink 1.20); for the SQL client, the `flink-sql-connector-gcp-cloudtasks` uber-jar from the [release page](https://github.com/flink-gcp/flink-connector-gcp/releases/tag/v1.0.0) goes into `lib/`

## What Cloud Tasks does that a stream needs

Cloud Tasks queues HTTP requests for dispatch. It holds each task until the queue's pacing allows it, retries responses outside the 2xx range under the queue's policy, and can schedule tasks up to 30 days ahead. My pipelines use it to deliver work to handlers, so this connector provides a sink. It has no source for reading a queue back.

I built it for streams from fast sources such as Pub/Sub or Kafka that need to call a slower endpoint, usually a third-party API with a rate limit. Enforcing that limit in Flink requires a stateful throttle; a plain HTTP sink supplies no such control. With Cloud Tasks, the job enqueues records as they arrive and the queue paces requests, retries failures and holds the backlog. The sink therefore has no rate controls of its own. Queue settings determine dispatch speed.

I found no Flink connector for Cloud Tasks when I started, and still found none on GitHub as of 2026-09-05. Apache Beam's built-in I/O list also had no Cloud Tasks entry when checked that day. I used this repository's Pub/Sub sink as the design reference, sharing its serializer boundary and stateless writer that flushes at checkpoints.

In 1.0.0, the DataStream sink provides at-least-once task creation for HTTP and App Engine targets, fixed or per-record queue routing, opt-in named-task deduplication, and a pluggable failure policy. The SQL sink builds requests from a table: a Flink format serializes the body, while writable metadata supplies the URL, method, headers, schedule time and task id.

## Enriched orders into a partner API

Order events arrive over Pub/Sub with a customer id. The SQL job looks up the customer's partner endpoint and tenant in Bigtable, then creates an HTTP task on a queue paced to the partner API's limit. The Bigtable post used a fixed target URL. Here each row chooses its URL, method, headers, schedule time and task id.

This code is copied byte for byte from the [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/cloudtasks/#pubsub-events-enriched-from-bigtable), which CI validates through Flink's planner:

``` sql
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE incoming_orders (
  event_id    STRING,
  customer_id STRING,
  order_id    STRING,
  amount      DECIMAL(12, 2),
  dispatch_at TIMESTAMP_LTZ(6),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json'
);

CREATE TABLE customer_routes (
  rowkey  STRING,
  routing ROW<endpoint STRING, tenant STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'customer-routes',
  'lookup.async' = 'true'
);

CREATE TABLE enriched_order_tasks (
  event_id       STRING,
  order_id       STRING,
  amount         DECIMAL(12, 2),
  customer_id    STRING,
  target_url     STRING NOT NULL    METADATA FROM 'url',
  request_method STRING             METADATA FROM 'http-method',
  request_headers MAP<STRING, STRING> METADATA FROM 'headers',
  schedule_at    TIMESTAMP_LTZ(6)   METADATA FROM 'schedule-time',
  dedupe_key     STRING             METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'partner-api',
  'format' = 'json'
);

INSERT INTO enriched_order_tasks
SELECT e.event_id,
       e.order_id,
       e.amount,
       e.customer_id,
       r.routing.endpoint || '/orders/' || e.order_id,
       'POST',
       MAP['Content-Type', 'application/json', 'X-Tenant', r.routing.tenant],
       e.dispatch_at,
       e.event_id
FROM incoming_orders AS e
JOIN customer_routes FOR SYSTEM_TIME AS OF e.proc_time AS r
  ON e.customer_id = r.rowkey
WHERE e.event_id IS NOT NULL
  AND e.order_id IS NOT NULL
  AND r.routing.endpoint IS NOT NULL
  AND r.routing.tenant IS NOT NULL;
```

Physical columns form the request body. The format selected in `WITH`, JSON here, receives exactly the four columns from `event_id` through `customer_id` and produces the body bytes. The connector removes the five `METADATA` columns before serialization, so request settings cannot accidentally enter the JSON.

This separation lets the sink use JSON, CSV, Avro or raw formats with a table-supplied `Content-Type`. The form format described later supplies its own content type.

A non-null row URL, method or header value overrides its fixed-option counterpart. This table has no `http.url`, so `target_url` must be `STRING NOT NULL`; otherwise planning fails. Every row needs a URL when there is no fixed fallback. Row headers override fixed headers by case-insensitive name.

Schedule time and task id are metadata-only. `schedule_at` sets a time at most 30 days ahead, with null leaving the service default. The query takes it from `dispatch_at`, allowing an order due in an hour to be enqueued now without timer state in Flink. The queue's pacing determines when it is dispatched after that time.

The Bigtable lookup is the same kind of join as in the previous post. With this inner join, an event without a customer route produces no task. The `WHERE` also drops events or route rows missing any of the four required request values. The sink sees neither case, so production jobs should count these rejected inputs separately from successful dispatches.

The route table is trusted configuration. As the documentation advises, enforce an endpoint allowlist before the insert and validate `order_id` as one path segment before concatenating it. The connector forwards URLs without restricting target hosts or paths.

Checkpointing is required; the example chooses ten seconds. Pub/Sub acknowledges messages when a checkpoint completes. At the barrier, the Cloud Tasks sink waits for every outstanding create, including requests in retry backoff. Under the default failure policy, a completed checkpoint therefore means the service has durably accepted every preceding record except serializer skips.

Without checkpoints, that flush does not run during the stream, and outstanding creates can be lost on failure. Both ends provide at-least-once delivery, which the lookup join does not strengthen.

Successful creation means Cloud Tasks has stored the task. Dispatch and handler success are separate: the queue controls them through its retry policy, outside the sink's view. Handler delivery is at-least-once even when creation is deduplicated. The endpoint still needs an idempotent operation or its own durable business key.

## Deduplicating task creation by task id

Tasks are unnamed by default, letting Cloud Tasks assign their names and create them at full speed. A replay can create a second task and call the endpoint again. For handlers that are already idempotent, I keep this at-least-once default.

The `task-id` metadata column, or `taskIdExtractor(...)` in DataStream, derives task names from row keys. Once enabled, every row must supply a non-null key. Recreating a name the service still remembers returns `ALREADY_EXISTS`. The sink counts this as success in `tasksDeduplicated`, so replay sends another create request without creating another task to dispatch.

The documentation calls this bounded effectively-once task creation. The bound comes from name retention after execution or deletion. Google's sources disagree on that duration: the REST reference says a released id can take up to 24 hours, while the v2 proto comment says about an hour. The connector advises designing against the shorter window. A scheduled task retains its id for its lifetime plus that window.

The guarantee covers creation only. Cloud Tasks still delivers the handler at least once.

A task name identifies an immutable logical task. Cloud Tasks cannot update a created task, and the sink accepts `ALREADY_EXISTS` without comparing its payload or schedule. If changed data should create another task, include a content or schedule version in the key. That is the role of `-v1` in the documentation's external-API example.

The sink uses the key's SHA-256 digest as the task id. Google's `tasks.create` reference recommends hashed strings and warns that sequential ids, "for example using a timestamp", increase latency and error rates across task commands. Hashing keeps deterministic deduplication while allowing the job to supply an event id or offset without creating sequential task names. The tradeoff is readability: the business key alone does not reveal the task's console name.

Naming is opt-in because of its service-side cost. The same reference says the additional lookup causes "significantly increased latency", without quantifying it. The project's benchmark compared deterministic ids with unnamed creation on a paused queue, using the preregistered exactly-once performance gate applied to candidate modes.

The averages met the general gate, but the run-to-run throughput range was about 11 percent, above the 10 percent limit. The result remains inconclusive. For the current mode, I enable naming when duplicate-call costs justify the measured latency on that queue.

For v1.1.0, I am planning an opt-in **exactly-once mode** using Flink's two-phase commit ([\#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238)). It is not implemented yet. The sink would stage tasks in checkpointed state and create them only after checkpoint completion, preserving each task's identity and contents for commit retries.

The guarantee would cover task creation within a verified recovery window; handler delivery would remain at least once. That window needs a safety margin inside a verified minimum name-retention period. Neither the one-hour nor the 24-hour description above establishes that minimum. After the recovery deadline, the job would stop creating tasks and retain pending work in Flink state for operator recovery.

The initial scope is checkpointed streaming jobs writing to one fixed, pre-provisioned queue. Shipping the mode depends on verifying the service contract and passing recovery and performance tests; the existing at-least-once mode would remain the default.

## Two identities behind an authenticated call

An authenticated pipeline uses two identities. The first calls `CreateTask`, using application-default credentials or a service-account key file. With a key file, only the path travels in the job graph; each TaskManager reads it when its writer starts.

The second identity authenticates the HTTP request when Cloud Tasks later dispatches it. Its token settings are configured on the table or serializer. They do not authenticate Flink or grant permission to enqueue tasks.

For a Cloud Run service or function protected by IAM, the dispatch token is OIDC. The documentation provides this [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/cloudtasks/#an-authenticated-cloud-run-function):

``` sql
CREATE TABLE function_tasks (
  order_id     STRING,
  amount       DECIMAL(12, 2),
  trace        MAP<STRING, STRING> METADATA FROM 'headers',
  schedule_at  TIMESTAMP_LTZ(6)    METADATA FROM 'schedule-time',
  dedupe_key   STRING              METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'functions',
  'http.url' = 'https://process-order-abc-an.a.run.app/tasks',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'http.oidc.service-account-email' =
    'dispatcher@my-project.iam.gserviceaccount.com',
  'http.oidc.audience' = 'https://process-order-abc-an.a.run.app',
  'format' = 'json'
);

INSERT INTO function_tasks
VALUES (
  'o-42',
  CAST(19.95 AS DECIMAL(12, 2)),
  MAP['X-Trace-Id', 'trace-42'],
  CAST(CURRENT_TIMESTAMP + INTERVAL '5' MINUTE AS TIMESTAMP_LTZ(6)),
  'order-o-42'
);
```

The audience uses the service's stable root `run.app` URL. Without an explicit audience, Cloud Tasks uses the full target URL, including its path. The handler must accept whichever audience is configured.

The deployment must create the IAM bindings; the connector does not. The task creator needs enqueue permission and `iam.serviceAccounts.actAs` on the dispatch service account. That account must belong to the queue's project and have the target's invoker role. OIDC settings are fixed table options because SQL exposes no per-row dispatch identity.

OAuth supplies an access token for Google APIs on `*.googleapis.com`, the use Google generally documents for it. OIDC and OAuth occupy one protobuf `oneof`, so the builder rejects setting both. A partner endpoint that validates Google-issued tokens can use OIDC. An endpoint using its own credentials can receive them in a row-supplied or fixed header.

The endpoint must also be reachable. Google documents HTTP targets as endpoints with external IP addresses. One supported exception is Cloud Run with internal ingress: Cloud Tasks can reach it in the same project or VPC Service Controls perimeter through its default `run.app` URL. A valid token alone does not establish network reachability.

Before relying on per-row URLs, check for a queue-level URI override. Its enforcement mode defaults to always, so it can silently replace every task's URL. The sink's v2 client cannot read that field and therefore cannot guard against it. The documentation records the interaction; I check the queue configuration before using row URLs.

The connector also supports App Engine targets, although I do not use this integration in my own pipelines. In SQL, `target.type = 'app-engine'` selects a relative URI with optional service, version and instance routing. Tasks reach the application through Google's internal transport, so this target does not accept OIDC or OAuth options. The project's gated tests validate this support against real App Engine.

## Bodies the target API expects

The target API determines the body format. SQL can use any serialization format on the classpath whose schema requirements match the physical columns. The documentation shows the resulting bytes for nested JSON, CSV with configurable delimiters, quoting and null literals, raw single-column data in a chosen charset, and binary Avro. Avro's writer schema comes from the physical columns and must be shared with the handler.

The connector passes these bytes through without inspecting them, so the table must set the appropriate `Content-Type`.

The module also provides `form-urlencoded` for `application/x-www-form-urlencoded` bodies. It accepts `STRING` and `ARRAY<STRING>` columns and sets its own `Content-Type`. Non-null columns produce fields in physical schema order; array elements produce repeated fields. Null columns and empty arrays add nothing, while a null array element fails the row.

The format does not invent encodings for numbers, booleans, nested rows or maps. SQL must cast scalars to strings to make the wire representation explicit. Quoted column names such as `` `items[]` `` can express bracket or dotted names expected by the server.

Dynamic field names, indexed arrays and arbitrary maps require a scalar function to build the complete body and the raw format to pass it through. Sending pre-encoded content through `form-urlencoded` would encode it again.

HTTP targets serialize bodies only for `POST`, `PUT` and `PATCH`; App Engine targets do so only for `POST` and `PUT`. Other methods skip body serialization. A `GET` request therefore takes its query string from the URL. Use the `url` metadata column with Flink 2.x's `URL_ENCODE`, or a UDF on 1.20, to escape values before concatenation. The connector does not repair an unescaped query string.

A DataStream serializer can return a complete `Task` proto, including a per-record dispatch deadline and multipart body, neither of which has a SQL representation. The `httpTarget(url)` and `appEngineTarget(relativeUri)` helpers bind a body `SerializationSchema` and provide URL, header and routing resolvers alongside fixed token settings. Per-record authorization requires the full `Task` serializer.

Returning `null` from the task serializer skips a record. Nothing is written, and the `recordsSkipped` counter reports the skip. The body-serialization helpers cannot skip this way: null from their wrapped Flink serializer is a serialization failure.

The task serializer must leave the name unset. The sink constructs it from the resolved queue and hashed key, keeping naming on one path.

## Sharding across queues from the DataStream API

Each SQL table writes to one fixed queue. DataStream can resolve a queue for each record through the same resolver pattern used for tables and topics in the earlier connectors. Sharding across queues increases aggregate dispatch capacity when one queue's service limits are insufficient. This [source-backed example](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/cloudtasks/#sharding-across-queues) keeps each customer on one queue:

``` java
CloudTasksSink.<OrderEvent>builder()
        .destinationResolver(
                (element, context) ->
                        QueueDestination.of(
                                "my-project",
                                "asia-northeast1",
                                "webhooks-"
                                        + Math.floorMod(
                                                element.customerId().hashCode(), 4)))
        .serializer(
                CloudTasksSerializationSchema.httpTarget(
                                "https://api.example.com/v1/orders")
                        .withBody(new OrderEventSchema()))
        .build();
```

One client serves all queues. Routing creates no per-queue client, stream or batcher to cache or evict. A destination includes its location because queues are regional.

The sink holds no per-queue state except optional per-destination metrics. Those metrics are off by default because Flink cannot unregister them: every queue used would retain counters for the task's lifetime. Any resolver cache belongs to the resolver, travels in the job graph and should also be bounded.

Every destination queue must already exist. The sink offers no create disposition because queue settings define the pacing this pipeline needs. Automatic creation would use defaults of 500 dispatches per second and 1,000 concurrent dispatches, potentially exceeding the endpoint's capacity. A deleted queue name also cannot be reused for three days, making a mistaken creation slow to undo.

Queues are infrastructure the job references. The quickstart begins by creating one with limits the endpoint can sustain.

## What a failed task means

Three failure types reach `failedTaskHandler`: serializer rejection, an exception from the task-id extractor, and a service `INVALID_ARGUMENT` response, such as a malformed target, oversized body or rejected header. The default handler fails the job; alternatives can log and drop or forward to a dead-letter queue.

The shared Pub/Sub dead-letter implementation works unchanged. When serialization succeeded, its message contains the full serialized task. A consumer can call `Task.parseFrom` to recover the target, method, headers and authorization settings. A serialization failure produces empty message data. SQL exposes no failure-policy option and fails the job on the first routed failure.

An exhausted retry budget, missing queue or `PERMISSION_DENIED` fails the job without reaching a dropping handler. These failures require backpressure and restart rather than discarding a stream during an outage. A transient status anywhere in the cause chain takes precedence over `INVALID_ARGUMENT`, preventing instability from being classified as a dead letter.

Configuration errors also remain fatal. A null resolver result or empty extracted key would otherwise drop every record while the job appeared healthy. A serializer bug that makes every task invalid can still reach a dropping handler one record at a time. Monitor `numRecordsSendErrors`, which counts all tasks delivered to the handler, whether dropped or not.

A task dropped during creation never entered the queue, so the queue's retry policy never applied. Once created, a task whose handler fails is retried by Cloud Tasks outside the sink's view. App Engine has one documented distinction: a handler's `503` throttles the queue, while `429` does not trigger congestion control.

Paused and disabled queues need separate monitoring. Both accept new tasks while stopping dispatch; the paused-queue contract says it "will stop delivering tasks from it, but more tasks can still be added to it". The sink can therefore appear healthy while the backlog grows.

## The knobs that matter

The writer has few options because dispatch pacing belongs to the queue. Configure `maxDispatchesPerSecond`, `maxConcurrentDispatches` and the handler retry policy there.

The sink owns create retries. The generated client sets a 20-second timeout for `CreateTask` and no retryable status codes; an unnamed create is non-idempotent. Failed creates are parked with a due time and retried from a subsequent write or flush.

`UNAVAILABLE`, `DEADLINE_EXCEEDED` and `RESOURCE_EXHAUSTED` use up to eight attempts including the first. Backoff starts at 100 ms, doubles and caps at 10 s. `NOT_FOUND` has a separate three-attempt budget with delays from 500 ms to 2 s. Both schedules use up to 25 percent jitter in either direction.

The shorter `NOT_FOUND` budget balances two cases. A queue idle for 30 days can take minutes to reactivate and return `NOT_FOUND` meanwhile, so that status alone does not prove the name is wrong. But a mistyped queue should fail without exhausting the longer budget for every record. Reactivation can outlast the short budget; the job's restart strategy handles that case.

Retrying `DEADLINE_EXCEEDED` can duplicate an unnamed task if the first create succeeded. At-least-once favors that over loss. Named-task deduplication removes this ambiguity within its retention window.

`maxInFlightTasks`, default 1,000, bounds outstanding creates, including parked retries. At the cap, a write yields to the task mailbox until completions free capacity.

Transport capacity can bind first. The client opens one gRPC channel by default, and one HTTP/2 channel supports about 100 concurrent streams. A subtask therefore runs about 100 concurrent creates regardless of a higher in-flight cap. Measurements on 2026-08-22 against a real paused queue reached about 210 creates per second per subtask with one channel and 1,271 with eight.

`channelPoolSize` allows more channels but defaults to one. Automatically sizing the pool from the in-flight cap could exceed Google's guidance of roughly 1,000 tasks per second per queue, counting creates and dispatches, as the eight-channel measurement already does. It could also violate the ramp rule of increasing traffic by no more than 50 percent every five minutes.

Increase the pool deliberately, using roughly one channel per 100 concurrent creates needed. A pipeline paced to a partner API usually does not need more channels unless ingestion bursts alone approach the queue guidance.

The project evaluated batch creation and declined it in [ADR-0129](https://github.com/flink-gcp/flink-connector-gcp/blob/main/docs/adr/0129-the-cloud-tasks-sink-keeps-one-create-rpc-per-record-and-declines-v2beta3-batchcreatetasks.md). The API exists only in v2beta3 as a long-running, explicitly non-atomic operation accepting up to 100 tasks. On the same measurement day, it was no faster at the median than 100 concurrent single creates and about twice as slow at the tail.

Its failure semantics ruled it out. A batch containing existing named tasks returns one `ALREADY_EXISTS` for the whole batch, without per-task results, while still creating its non-duplicate tasks. A sink cannot map that outcome reliably to per-task results while treating deduplication as success. Creation therefore remains one RPC per record, with the sink managing buffering, backpressure and concurrency.

## Testing without an official emulator

Google provides no official Cloud Tasks emulator. Integration tests use the MIT-licensed community [`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator) through testcontainers, without cloud credentials, on pull requests affecting the module. It dispatches over real HTTP, allowing a server in the test JVM to record each request's method, path, body and headers. The tests also inspect OIDC bearer JWTs for the configured account and audience.

Stored-task assertions use paused queues. Running queues delete tasks as soon as they complete, which would race inspections of the task itself. Separating these checks verifies both stored request settings and what reaches the handler.

The emulator never garbage-collects task names, so it can test `ALREADY_EXISTS` but not the deduplication window. It also lacks queue-level URI overrides, App Engine dispatch, OAuth tokens, failure injection and task-size enforcement. Transient retries are therefore unit-tested with a fake creator and injected clock.

The connector leaves size enforcement to the service. As of 2026-09-05, Google's create reference still specified 100 KB while its quotas page specified 1 MiB. The connector validates against neither conflicting figure, and the documentation advises staying within the smaller one.

The gated App Engine suite checks request construction and routing on paused queues, reads a queue-level routing override and observes failed handler attempts against a real App Engine Standard fixture. The fixture starts for the test class and returns to zero instances afterwards, with a scheduled sweep restoring the idle state after hard cancellation.

## Closing the series

This completes the five-connector series, with each post built around a pipeline I needed. Cloud Tasks illustrates the shared design approach: expose service capabilities and add the Flink behavior around them.

The queue controls handler pacing, scheduling and retries. The sink retries only creation requests, hashes business keys for service-side deduplication, and makes `INVALID_ARGUMENT` eligible for failure routing. Its Flink-specific responsibility is to wait at a checkpoint until every non-skipped create is durable under the default failure policy. The SQL API expresses the request as a table.

The [v1.1.0 milestone](https://github.com/flink-gcp/flink-connector-gcp/milestone/10) now includes the Cloud Tasks exactly-once mode alongside Bigtable's write extensions. The [Cloud Tasks tracker](https://github.com/flink-gcp/flink-connector-gcp/issues/1238) covers the service investigation, staged writer and committer, DataStream and Table APIs, and recovery and performance validation. [RPC-level metrics through the client tracer](https://github.com/flink-gcp/flink-connector-gcp/issues/715) remain planned for v1.3.0. Reports about targets or body formats the SQL API cannot express will help guide further work: <https://github.com/flink-gcp/flink-connector-gcp/issues>

------------------------------------------------------------------------

*This is an independent open-source project. It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.*
