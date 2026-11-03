+++
title = "Native Protobuf types and state in Apache Flink with flink-datastream-protobuf 1.0.0"
description = "Why I built native Protobuf type integration for Flink DataStream, how it handles saved state, and how its serializer compares with Chill, Avro, POJO, and Tuple."
date = 2026-11-03T16:00:00+09:00
draft = false

[taxonomies]
tags = ["Flink", "Protobuf", "Java", "Performance"]
+++

I have released [flink-datastream-protobuf 1.0.0][release], a library for using Protocol Buffers messages as native Apache Flink DataStream types and managed-state values.
It is the first release published to Maven Central.
It provides explicit type information, configuration-based type registration, and descriptor-based checks when restoring state after a supported schema change.

My performance goal was to match Chill and improve on it where native integration could avoid work.
After several rounds of optimization, the native serializer reached that goal for ordinary serialization and deserialization on the Protobuf 4 runtime in the [release-candidate measurements][benchmarks]: it took 0.34–0.88 times Chill's serialization time and 0.54–0.88 times its deserialization time across all five workloads.
Those are serializer measurements on one machine configuration; other representations win some comparisons, and none of these numbers measures a complete Flink job.

## Why I built it

I build, operate, and maintain Flink pipelines on Google Cloud that carry Protobuf messages between operators.
Protobuf is a frequent part of that work: many [Cloud APIs][cloud-apis] offer gRPC interfaces, and [BigQuery's Storage Write API][bigquery-write] accepts Protobuf records for gRPC streaming.
For these pipelines, working with Protobuf objects throughout a Flink job is a practical requirement.

I have also contributed to Flink's Protobuf format through [Apache Flink #21613][flink-protobuf-contribution].
[That change][flink-protobuf-fix] fixed code-generation failures for imported message types such as `google.protobuf.Timestamp`, with tests for conversion in both directions.
That Table/SQL format converts connector data into Flink's table representation.
For DataStream jobs that pass generated Protobuf objects between operators and keep them in state, I wanted a native integration I could maintain and improve over time.

A source's `DeserializationSchema` turns incoming bytes into Java objects; Flink then needs a separate `TypeSerializer` as those objects cross operator boundaries or enter managed state.
When Flink cannot select a dedicated serializer for a type, it can treat the object as generic and fall back to Kryo.
Its documentation shows how to register Twitter Chill's [Protobuf adapter][flink-third-party] on that path.
Encoding Protobuf at a connector boundary does not select this internal serializer.

Chill's long pause in releases and development was the main reason I wanted to replace that dependency.
As of November 1, 2026, its latest GitHub release is [0.10.0, published on June 4, 2021][chill-release].
The latest commit on its default branch, `develop`, is a [dependency update from March 19, 2023][chill-last-commit].
For pipelines I expect to maintain through future Flink and Protobuf upgrades, relying on a library with that maintenance history is a risk I do not want to keep carrying.

The dependency versions show how far the components have drifted apart:

| Component | Kryo version in its build |
| --- | --- |
| [Chill 0.7.6][chill-old-build] | 2.21 |
| [Chill `develop` at its latest commit][chill-build] | 4.0.2 |
| [Flink 1.20.5][flink-lts-build] | 2.24.0, with Chill 0.7.6 |
| [Flink 2.3.0][flink-current-build] | 5.6.2 |

The Flink 2.3 documentation still illustrates Protobuf registration with `chill-protobuf:0.7.6` and `protobuf-java:3.7.0`.
That puts a Chill adapter built against Kryo 2 in the instructions for a Kryo 5 runtime.
My [initial experiment][spike] transported a message with Chill 0.7.6 on Flink 2.3.0's Kryo 5.6.2, using Protobuf 3.25.8 rather than the documented 3.7.0.
Later checks exercised Chill 0.10.0 on the supported Flink 2.x minors.
Those results establish that the tested paths work today; they leave the question of who will maintain the integration as its dependencies evolve.

If I was going to take responsibility for that maintenance, I preferred to build the Protobuf integration I wanted directly against Flink's native type APIs instead of maintaining a Chill fork.
A replacement Kryo adapter would still leave messages as generic types from Flink's perspective.
A native `TypeInformation` and `TypeSerializer` let me use generated messages with `pipeline.generic-types: false`, control allocation and copying, and define the state-restore policy in the library itself.

That restore policy matters for the stateful jobs I maintain.
Being able to parse old Protobuf bytes alone does not tell me whether a new message schema can restore a savepoint: Flink must accept the serializer metadata, and the application must handle the restored values correctly.
Flink already provides native type and serializer-snapshot integration for Avro; I wanted the equivalent foundation for Protobuf, with explicit compatibility rules and recovery tests.

## What 1.0.0 provides

The application entry points are `ProtobufTypeInformation` and `ProtobufTypeInfoFactory`.
The library handles full-runtime proto3 generated messages and descriptor-bound `DynamicMessage` values.
Lite messages, proto2 payload types, and editions payload types are outside its supported scope, including when reached through a nested message.
The [usage guide][usage] describes these boundaries and the supported nested Flink types.

Both artifact lines use the same Maven group and artifact name:

```xml
<dependency>
    <groupId>io.github.flink-gcp</groupId>
    <artifactId>flink-datastream-protobuf</artifactId>
    <version>1.0.0</version>
</dependency>
```

Version `1.0.0` runs on Flink 2.2 and 2.3 with Java 17 or 21.
Use `1.0.0-1.20` for Flink 1.20 LTS with Java 17.
Each line's jar is built with Protobuf 4 and runs unchanged with the supported Protobuf 4.x or 3.25.x runtime and matching generated code.
The release was checked with Flink 2.2.1, 2.3.0, and 1.20.5, and Protobuf/protoc 4.36.2 and 3.25.9.
The application supplies Flink and `protobuf-java`; the library declares them as provided dependencies.

Stay on the artifact line matching the Flink runtime.
Maven orders `1.0.0-1.20` after `1.0.0`, so an update tool can otherwise propose the LTS artifact to a Flink 2.x application.
The [compatibility guide][compatibility] includes update-tool configuration.

### Selecting the native type

The following fragment uses `Event` from the project's [standalone example][explicit-example], generated from a proto3 schema with `account` and `amount` fields.
The partitioning line follows the [stateful example][stateful-example], and the [quickstart][quickstart] provides the build configuration and complete entry points.

```java
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.protobuf.ProtobufTypeInformation;
import io.github.flink.gcp.protobuf.examples.generated.Event;

Configuration config = new Configuration();
config.set(PipelineOptions.GENERIC_TYPES, false);
var env = StreamExecutionEnvironment.getExecutionEnvironment(config);

var eventType = ProtobufTypeInformation.of(Event.class);
var events = env.fromData(
        eventType,
        Event.newBuilder().setAccount("alice").setAmount(2).build(),
        Event.newBuilder().setAccount("bob").setAmount(7).build());

var updated = events
        .map(event -> event.toBuilder().setAmount(event.getAmount() + 1).build())
        .returns(eventType);

var keyed = updated.keyBy(Event::getAccount, Types.STRING);
```

Disabling generic types makes missing native type information fail instead of silently selecting Kryo.
It does not register the Protobuf factory by itself.
For configuration-based registration, apply these options when constructing the environment, before type extraction:

```yaml
pipeline.generic-types: false
pipeline.serialization-config:
  - com.google.protobuf.AbstractMessage: {type: typeinfo, class: io.github.flink.gcp.protobuf.ProtobufTypeInfoFactory}
```

The superclass matters: Flink's factory lookup follows the superclass chain, so registering the `Message` interface does not cover generated message classes.
Factory registration also covers message fields inside POJOs and tuple elements; it is process-global, while explicit type information avoids that registration.
The [registration example][registration] shows how to load the configuration.

The `keyBy` above extracts an account string deliberately.
Messages are supported as values, not as partitioning keys or MapState user keys.
Deterministic Protobuf serialization does not make message bytes canonical across builds or make generated-message hashing stable across JVMs.

### Restoring state after a schema change

The serializer snapshot retains descriptors alongside the type identity and settings.
At restore time, the library compares those descriptors with the new type and accepts unchanged schemas or its supported forward changes.
The policy is deliberately narrower than everything Protobuf's wire format can parse:

| Change | State restore in 1.0.0 |
| --- | --- |
| Add a field with a fresh, unreserved number and name | Accepted |
| Remove a field and reserve its number | Accepted |
| Add an enum value with a new, unreserved name and number, preserving existing order | Accepted |
| Change an existing field from `int32` to `int64` | Rejected |
| Remove an enum value | Rejected |

These examples assume unchanged type names and other declarations, supported snapshot formats, and nondecreasing size/depth limits; the [full rules][schema-changes] specify the remaining conditions.
For generated messages, an added field returns its default when the saved payload contains no value for it.
A value retained as an unknown field can instead populate the added field, so application logic must account for both cases.
The [recovery tests][recovery] exercise checkpoint recovery and savepoint restoration with HashMap and RocksDB backends, including jobs whose application jar contains only the new generated classes.

Version 1.0.0 begins the 1.x stable API and forward-restore contract.
Later 1.x releases must restore earlier 1.x state for the supported schema changes with the same artifact line, Flink minor, and Protobuf runtime and generated code.
That contract does not promise downgrades or cross-Flink-minor restores.
The earlier 0.x milestones were unpublished development versions, and 1.0.0 does not read their state.

## What the performance comparison measures

The [release-candidate run][benchmarks] used temporary Google Compute Engine `c3-standard-8` VMs with Intel Sapphire Rapids CPUs, 8 vCPUs, and 32 GB of memory.
Each Protobuf profile ran on its own VM, using Flink 2.3.0, Temurin 17.0.21, G1 GC, and a fixed 1 GiB heap.
The comparators were Chill 0.10.0, Kryo 5.6.2, Avro 1.11.4, and Thrift 0.25.0.

JMH 1.37 measured each cell in five independent JVM forks, with five one-second warmup iterations and five one-second measurement iterations per fork, using one benchmark thread.
Native Protobuf and Chill ran in adjacent blocks with alternating order.
The GC profiler recorded allocated bytes per operation, and correctness checks verified the selected serializers and round-tripped values before timing.

All representations carry the same nine logical fields, with 64 records per workload:

| Workload | Populated data |
| --- | --- |
| scalar | Scalar values, with empty/default structured fields |
| text | ASCII, Japanese, and accented text, plus 1 KiB of bytes |
| nested | A child record containing an integer and a Unicode string |
| collections | 16 long values and 16 string-to-long map entries |
| large | 64 KiB of bytes |

This is one schema with different populated shapes, not a minimal schema separately tuned for each format.
Fixed-field representations still encode their empty/default fields.
Ordinary `serialize` cycles through prebuilt, previously encoded messages, so Protobuf's memoized sizes can be warm.
`buildAndSerialize` separately includes constructing fresh records from prebuilt logical values.
Class initialization and descriptor discovery are outside the timed operations.

The measured source is [`9e7d175`][measured-source].
Its production source and POM are unchanged at the `v1.0.0` tag, but the benchmark built a jar per Protobuf profile at Flink 2.3.0; publication builds one Protobuf 4 jar per artifact line, with a Flink 2.2.1 floor for the 2.x line.
The report records a local class/resource equivalence check, not a direct measurement of the Maven Central jar.
It links the [retained CSV and JMH evidence][evidence] for checking the results.

### Native Protobuf and Chill

The figure uses **native/Chill time ratios** for Protobuf 4.36.2.
A value of 0.5 means native took half the time for that operation.
Points are paired geometric ratios; bars are per-cell 95% intervals calculated from the five log ratios.
They are not a simultaneous confidence statement about all operations.

![Native/Chill time ratios for Protobuf 4.36.2, with 95% intervals. Serialize ranges from 0.337 for large to 0.880 for collections; deserialize ranges from 0.539 for large to 0.881 for collections.](/images/flink-datastream-protobuf-1-0-0/native-chill.svg)

[Open the figure at full size.](/images/flink-datastream-protobuf-1-0-0/native-chill.svg)

The practical target was set before measurement: an interval entirely at or below 1.10 establishes that cell's 10% tolerance.
All 40 Protobuf 4 cells met it.
This meets the project's original performance target on that configuration, with ordinary serialization and deserialization faster than Chill in every workload shown above: each of those ten intervals is entirely below 1.0.
Protobuf 3.25.9 met it in 38 of 40 cells; text `buildAndSerialize` and collections `serializeSameInstance` crossed the threshold and remained inconclusive.
No interval was wholly above the threshold.
The inconclusive text result had substantial fork-to-fork variation, so a favorable point estimate alone would overstate the evidence.

For Protobuf 4, the scalar serialize ratio was 0.611 (0.588–0.634), and the large ratio was 0.337 (0.334–0.339).
That corresponds to about 39% and 66% less serialization time in those cases.
Collections had the smallest margin, at 0.880 (0.865–0.894).
The fresh-construction measurement also favored native, but by less: `buildAndSerialize` ratios ranged from 0.647 for large to 0.886 for collections.

Allocation and encoded size answer different questions.
The table below reports ordinary serialize on Protobuf 4; each pair is **native / Chill**.
Allocation is the arithmetic mean of five fork scores, and encoded bytes are the mean over the corpus, including each serializer's framing.

| Workload | Allocated bytes/op, native / Chill | Encoded bytes, native / Chill |
| --- | ---: | ---: |
| scalar | 149.1 / 187.6 | 23.9 / 22.9 |
| text | 1,957.1 / 3,642.0 | 1,663.8 / 1,663.8 |
| nested | 245.1 / 307.6 | 46.4 / 45.4 |
| collections | 533.1 / 1,047.3 | 498.1 / 498.1 |
| large | 4,519.3 / 65,729.4 | 65,563.9 / 65,564.9 |

Large-message serialization allocates about 4.5 KB per operation here, compared with Chill's 65.7 KB.
Encoded sizes remain close because both carry Protobuf payloads, though their framing differs.
Neither the allocation column nor the encoded-size column measures peak heap usage, checkpoint size, or Flink network overhead.

Copy operations need their own interpretation.
Native object copy returns the immutable message reference, whereas Chill serializes and parses another value.
Native stream copy transfers framed bytes through reusable storage; Flink's Kryo stream copy deserializes and serializes.
Those operations can therefore differ by much more than ordinary encoding, but their ratios are not encoding speedups.

### Comparing other representations

Choosing Protobuf also means choosing its wire representation and generated-object model.
The following table compares ordinary serialize across the same five workloads on the Protobuf 4 VM.
Times are arithmetic means of five fork scores in **ns/op**, not the paired geometric ratios above.
These descriptive means do not carry a statistical ranking of every pair.

<div role="region" aria-label="Serialization times by workload; scroll horizontally for all columns" tabindex="0" style="overflow-x: auto;">

| Serializer | scalar | text | nested | collections | large |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native Protobuf | 54.5 | 808.1 | 88.2 | 777.3 | 3,632.8 |
| Chill Protobuf | 89.3 | 1,116.3 | 123.7 | 883.5 | 10,792.0 |
| Flink POJO | 111.4 | 1,383.0 | 133.4 | 388.6 | 2,774.5 |
| Flink Tuple | 71.9 | 1,271.3 | 98.3 | 309.9 | 2,696.8 |
| Avro Specific | 120.9 | 833.0 | 152.3 | 548.6 | 2,847.6 |
| Avro Generic | 137.6 | 851.9 | 165.9 | 587.6 | 2,892.2 |
| Chill Thrift | 1,039.7 | 1,864.9 | 1,053.0 | 2,678.0 | 13,778.6 |
| Kryo | 255.0 | 1,020.9 | 303.7 | 1,206.3 | 3,314.2 |

</div>

The POJO and Tuple lanes explicitly assign native field serializers and reject nested Kryo fallback.
Avro uses its specific-record and generic-record serializers.
The standalone Kryo lane uses a separate Java representation with registered root, child, and collection classes; it is not unconfigured Kryo applied to a Protobuf message.
Chill Protobuf and Chill Thrift use their format adapters through Flink's generic serializer.

Native Protobuf has the lowest mean serialize time for scalar, nested, and text in this Protobuf 4 run; its text mean is about 3% below Avro Specific's.
For collections, however, it takes about twice POJO's time and 2.5 times Tuple's time; both Avro lanes are faster too.
Protobuf encodes each map entry as a length-delimited message and long values as varints, whereas the POJO and Tuple serializers use fixed-width longs without per-entry message lengths.
Those are structural differences; this benchmark does not isolate how much each contributes.
The native collections record also averages 498.1 encoded bytes, versus 420.9 for POJO and 409.2 for Tuple.

Large records give another useful boundary.
Native Protobuf is much faster than Chill Protobuf, but slower than POJO, Tuple, Avro, and the standalone Kryo lane.
An application already using Protobuf can benefit from the native serializer without that making Protobuf the best format for every workload.

### Where the implementation saves work

The native implementation was not automatically faster than Chill.
The early benchmarks exposed expensive buffering and size checks, and I tracked the improvements separately so each could be measured against an unchanged baseline.
The four completed issues below are included in 1.0.0.

**Reusable buffers ([#42][buffer-issue]).**
The original stream encoders and decoders allocated a fresh 4 KiB buffer for each record.
The serializer now retains a 4 KiB scratch array per instance and uses array encoders and decoders for payloads that fit.
Small reads copy decoded values out of that storage, so a later record cannot overwrite bytes retained by an earlier message.
The [controlled buffer comparison][buffers] separated the read and write changes: each removed roughly 4 KB of allocation per operation on the small workloads.

**Cheaper size traversal ([#43][sizing-issue]).**
Before writing, the library checks payload size and nesting depth; that traversal originally built maps of populated fields and allocated bookkeeping objects for every record.
The implementation now caches field layouts with the payload class, reuses traversal frames, skips empty unknown-field sets, and reads packed primitive lists without boxing.
It keeps the overflow, size, depth, and exact-output-size checks.
In the [Protobuf 4 controlled comparison][sizing], collections serialization allocation fell from 12,919 to 660 B/op, although small writes still took longer than Chill's.

**Guarded generated sizes ([#112][generated-size-issue]).**
The next investigation isolated the remaining small-write cost in safe sizing.
For eligible acyclic message types and values, the serializer uses Protobuf's memoized `getSerializedSize()` and confirms it while encoding into the scratch array.
The [guard conditions][generated-size] preserve the size and depth limits; other values, including payloads above 4 KiB, still use the traversal.
In that controlled comparison, scalar and nested `serialize` and `serializeSameInstance` took 0.20–0.28 times the previous implementation's time across the two Protobuf profiles.
This comparison is against the preceding native implementation, not against Chill.

**Fewer large-read copies ([#113][large-read-issue]).**
The old stream decoder path allocated about 132 KB to read the benchmark's 64 KiB payload.
For payloads above 4 KiB, the reader now uses a dedicated payload array and enables aliasing by default, allowing decoded `bytes` and unknown length-delimited values to share it.
The [controlled large-read comparison][large-read] measured both implementations on the same VM for each Protobuf profile: large `deserialize` and `deserializeReuse` took 0.433–0.436 times the baseline's time, with allocation reduced to about 66 KB/op.

Aliasing has a retention cost: keeping a small `ByteString` from a large decoded message can keep the entire payload array alive.
Applications retaining such values in heap state or caches can copy the value, or set `largePayloadAliasing(false)` on the type-information builder.
That setting preserves wire bytes and decoded values; its disabled-path performance was not measured by this protocol.
The [memory guidance][aliasing] explains the tradeoff.

These are results from four separate controlled experiments, each with its own baseline, runtime pins, and uncertainty intervals in the linked report.
They should not be multiplied together into an overall speedup.
The first cross-format baseline ran on an Apple M1 Pro, while the release candidate ran on GCE with some newer dependencies.
Comparing their absolute times would mix implementation, machine, and runtime changes, so I do not use that comparison as an optimization speedup.

### The cost of DynamicMessage

Applications without generated classes can bind type information to a descriptor with `ProtobufTypeInformation.of(descriptor)`.
That type also has its own serializer and state snapshot.
It does not provide a migration between generated-message state and DynamicMessage state; the [DynamicMessage guide][dynamic] describes the separate contracts.

The release benchmark includes DynamicMessage over the same schema and content.
Across the two Protobuf profiles, decoding scalar, nested, and collections messages took roughly 5.3–6.3 times generated-message decoding time.
The difference was about 1.6 times for text and 1.1 times for large.
The implementation uses protobuf-java's respective parsers, so choosing runtime descriptors also chooses that parsing cost.
For schemas known when building an application, generated messages remain the natural starting point.

### What remains for 1.1.0

The remaining benchmark findings were not critical blockers for the 1.0.0 release.
The [release assessment][benchmarks] found no material performance regression and no performance objection to releasing the candidate.
I have scheduled the four open follow-ups for [1.1.0][next-milestone]:

- [Generated-message allocation (#175)][fixed-allocation-issue]: identify the fixed allocation that generated-message writes make and DynamicMessage writes avoid, and remove it where the serializer's guarantees permit.
- [Large-write buffering (#176)][large-write-issue]: remove the per-record encoder buffer still allocated when writing frames above 4 KiB.
- [Release-jar measurements (#177)][release-jar-issue]: run both Protobuf profiles against the unchanged jar built by the release configuration, recording that artifact directly in the benchmark evidence.
- [Protobuf 3 text timings (#178)][text-forks-issue]: explain why JVM forks fall into two timing groups, then adjust the measurement protocol or its reporting rule based on that investigation.

These follow-ups are scoped as compatible improvements; none requires a public-API, saved-state-format, or default-setting change.

## Trying it in a job

The [quickstart][quickstart] provides standalone generated-message, registration, state, and DynamicMessage applications.
The [release notes][release] record verification of both published artifact lines against Maven Central and link the retained state fixtures for future compatibility checks.

For a new Protobuf DataStream job, start with explicit type information and disabled generic types, then exercise the application's actual message shapes and state usage.
The serializer measurements help explain where time and allocations go; the job's operators, state backend, network traffic, and checkpoint behavior determine the benefit visible in production.

This is an independent open-source project.
It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.

[release]: https://github.com/flink-gcp/flink-datastream-protobuf/releases/tag/v1.0.0
[flink-third-party]: https://github.com/apache/flink/blob/release-2.3.0/docs/content/docs/dev/datastream/fault-tolerance/serialization/third_party_serializers.md
[flink-protobuf-contribution]: https://github.com/apache/flink/pull/21613
[flink-protobuf-fix]: https://github.com/apache/flink/commit/7ea4476c0544e17798cbb1e39609827954f6c266
[cloud-apis]: https://docs.cloud.google.com/apis/docs/overview
[bigquery-write]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc
[chill-release]: https://github.com/twitter/chill/releases/tag/v0.10.0
[chill-last-commit]: https://github.com/twitter/chill/commit/f69dfa0edce9442a1f5c0413d8b797957b8b27d7
[chill-old-build]: https://github.com/twitter/chill/blob/v0.7.6/build.sbt
[chill-build]: https://github.com/twitter/chill/blob/f69dfa0edce9442a1f5c0413d8b797957b8b27d7/build.sbt
[flink-lts-build]: https://github.com/apache/flink/blob/release-1.20.5/pom.xml
[flink-current-build]: https://github.com/apache/flink/blob/release-2.3.0/pom.xml
[spike]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/spike-0/
[usage]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/usage/
[compatibility]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/compatibility/
[schema-changes]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/compatibility/#schema-changes
[explicit-example]: https://github.com/flink-gcp/flink-datastream-protobuf/blob/v1.0.0/examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/ExplicitExample.java
[stateful-example]: https://github.com/flink-gcp/flink-datastream-protobuf/blob/v1.0.0/examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/StatefulExample.java
[registration]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/usage/#load-the-configuration-before-constructing-the-job
[recovery]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/runtime-recovery/
[benchmarks]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/serializer-benchmarks-v1.0.0/
[measured-source]: https://github.com/flink-gcp/flink-datastream-protobuf/tree/9e7d175341fc0e418a97f199168d63c1a0f8cb33
[evidence]: https://github.com/flink-gcp/flink-datastream-protobuf/tree/v1.0.0/docs/validation/data/serializer-benchmarks-v1.0.0
[buffers]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/serializer-buffer-allocation/
[buffer-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/42
[sizing-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/43
[sizing]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/serializer-sizing-allocation/
[generated-size]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/serializer-generated-size/
[generated-size-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/112
[large-read]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/validation/serializer-large-read/
[large-read-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/113
[fixed-allocation-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/175
[large-write-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/176
[release-jar-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/177
[text-forks-issue]: https://github.com/flink-gcp/flink-datastream-protobuf/issues/178
[next-milestone]: https://github.com/flink-gcp/flink-datastream-protobuf/milestone/5
[aliasing]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/usage/#memory-of-large-decoded-messages
[dynamic]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/dynamic-messages/
[quickstart]: https://flink-gcp.github.io/flink-datastream-protobuf/1.0/docs/quickstart/
