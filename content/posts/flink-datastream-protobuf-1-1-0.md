+++
title = "Faster large-message writes in flink-datastream-protobuf 1.1.0"
description = "flink-datastream-protobuf 1.1.0 reuses its large-message write buffer, cutting allocation by about 93% in the controlled 64 KiB benchmark."
date = 2026-11-12T02:08:03+09:00
draft = false

[taxonomies]
tags = ["Flink", "Protobuf", "Java", "Performance"]
+++

I have released [flink-datastream-protobuf 1.1.0][release], a library for using Protocol Buffers messages as native Apache Flink DataStream types and managed-state values.
The main change is a reusable buffer for large-message writes.
In the controlled benchmark of a message containing a 64 KiB byte field, this removed about 4.2 KB of allocation per write and reduced write time by 11–12%, using Chill as the timing control.

Applications on 1.0.0 get the improvement by updating the dependency version on their existing Flink artifact line.
The API, configuration defaults, runtime requirements, and state format stay the same.
The [1.0.0 article][previous-post] covers the library's native type integration and schema checks; this post focuses on the write-buffer change and its measured effect.

## Reusing the large-write buffer

Version 1.0.0 already reused a scratch array for payloads of at most 4 KiB.
For larger payloads, it created a Protobuf `CodedOutputStream` for each record and streamed the encoded bytes into Flink's output view.
Each new stream allocated a 4,096-byte buffer.
Writing a sequence of large records therefore repeatedly allocated and discarded buffers of the same size.

Version 1.1.0 creates that stream on the first large write and reuses it for subsequent records.
An adapter points it at the current output view, checks the expected payload length, and releases the view after the write.
The stream retains one 4 KiB buffer regardless of the message's size.
Each serializer instance owns its buffer, and a failed write discards the stream before the next record is written.

This removes a recurring allocation from the path Flink uses to write records across operator boundaries and into managed state.
The first large write pays for the buffer; later writes reuse it.
Small writes and reads keep their existing implementations, and the length prefix and encoded Protobuf payload remain unchanged.
The [implementation report][large-write] describes the change and its correctness checks.

## Comparing the old and new write paths

To measure the buffer change, I compared the previous and updated serializer implementations on the same VM for each Protobuf runtime.
Both used Flink 2.3.0 and Temurin 17.0.21 on Google Compute Engine `c3-standard-8` instances with Intel Sapphire Rapids CPUs.
The workload serialized prebuilt messages containing a 65,536-byte `bytes` field.

Each pair in the table shows the value before and after buffer reuse.
Write time is normalized so that Chill's serialization time is 100 in each run.[^controlled]

<div role="region" aria-label="Write-buffer benchmark results; scroll horizontally for all columns" tabindex="0" style="overflow-x: auto;">

| Protobuf | Allocation (B/op) | Write time (Chill = 100) | Time saved |
| --- | ---: | ---: | ---: |
| 3.25.9 | 4,506 → 322 | 34.6 → 30.9 | 10.7% |
| 4.36.2 | 4,519 → 335 | 34.4 → 30.3 | 11.9% |

</div>

Allocation fell by about **93%**, and the write took about **11–12% less time** in both profiles.
The time saving is relative to the previous native implementation, using Chill as the control.
The previous implementation was already faster than Chill on this workload; buffer reuse extends that advantage.

The benefit also applies when a record is constructed immediately before serialization.
That operation saved about 4.2 KB per record too, while total allocation remained around 66.5 KB because it includes creating the 64 KiB message.
Reusing the serializer's buffer removes the same repeated work in both cases; message construction still accounts for most of the allocation in the second case.

These results cover a large byte-field message, a common shape for carrying an opaque payload.
Large messages made mostly of strings or many small fields use the same write path, but their performance was not measured in this comparison.
For an application carrying large byte payloads, the result gives a concrete reason to update: less temporary allocation and faster serialization without changing how the application uses the library.

The separate [release-candidate benchmark report][benchmarks] records the full suite measured at commit [`8565715`][candidate].

## Updating from 1.0.0

An existing application can adopt 1.1.0 by changing the library dependency version.
For Flink 2.2 or 2.3, use:

```xml
<dependency>
    <groupId>io.github.flink-gcp</groupId>
    <artifactId>flink-datastream-protobuf</artifactId>
    <version>1.1.0</version>
</dependency>
```

For Flink 1.20, use `1.1.0-1.20` instead.
The supported runtime combinations remain:

| Library version | Flink runtime | Supported Java |
| --- | --- | --- |
| `1.1.0` | 2.2 and 2.3 | 17 and 21 |
| `1.1.0-1.20` | 1.20 LTS | 17 |

Each line's jar is built with Protobuf 4.36.2 and also runs unchanged with the supported Protobuf 3.25.x runtime and matching generated code.
The verification matrix pairs runtime and generated code at 4.36.2 or 3.25.9.
Flink and `protobuf-java` remain application-supplied dependencies.
Existing explicit type information and configuration-based factory registration continue to work unchanged.
The [quickstart][quickstart] provides complete examples for applications adopting the library for the first time.

Keep the update on the application's existing artifact line.
Both lines share the same group and artifact ID, and Maven orders `1.1.0-1.20` after `1.1.0`.
The [compatibility guide][artifact-lines] shows how to keep Dependabot and Renovate on the line that matches the Flink runtime.

Version 1.1.0 preserves the 1.x API and forward-state-restore contract introduced in 1.0.0.
Before release, CI compared the annotated API with 1.0.0, ran its compiled consumers against the candidate, and restored snapshots and checkpoint/savepoint fixtures written by the published 1.0.0 jars.
The [restore contract][compatibility] covers unchanged schemas and documented supported schema changes on the same artifact line, Flink minor, and Protobuf runtime and generated code.
Downgrades and restores across Flink minors or artifact lines remain outside that contract.

For a stateful deployment, retain a savepoint and the old application artifacts and settings, then verify restoration and continued processing with the updated application.
Take a new savepoint after that processing and verify restoration from it too.
This follows the [upgrade procedure][upgrade] and checks the application's own schemas and state alongside the release's retained-fixture tests.

This is an independent open-source project.
It is not affiliated with, endorsed by, or supported by the Apache Software Foundation or Google.
Apache Flink, Flink, and the Flink logo are trademarks of the Apache Software Foundation.

[^controlled]: The comparison used five JVM forks per cell, each with five one-second warmup iterations and five one-second measurement iterations.
    Allocation is the mean normalized allocation reported by JMH's GC profiler.
    The candidate/baseline ratios of geometric native/Chill times are 0.893 (95% interval 0.882–0.904) for Protobuf 3 and 0.881 (0.871–0.890) for Protobuf 4, using the report's pooled two-sample Student-t method with eight degrees of freedom on the [retained measurements][large-write-data].
    Baseline and candidate ran sequentially on one VM per profile; the control reduces sensitivity to host drift.

[release]: https://github.com/flink-gcp/flink-datastream-protobuf/releases/tag/v1.1.0
[previous-post]: @/posts/flink-datastream-protobuf-1-0-0.md
[large-write]: https://github.com/flink-gcp/flink-datastream-protobuf/blob/v1.1.0/docs/validation/serializer-large-write.md
[large-write-data]: https://github.com/flink-gcp/flink-datastream-protobuf/blob/v1.1.0/docs/validation/data/serializer-large-write-v1.1.0/README.md
[candidate]: https://github.com/flink-gcp/flink-datastream-protobuf/commit/85657151ecb69a7a8c44ffae651d8150aef99dcf
[benchmarks]: https://github.com/flink-gcp/flink-datastream-protobuf/blob/v1.1.0/docs/validation/serializer-benchmarks-v1.1.0.md
[compatibility]: https://flink-gcp.github.io/flink-datastream-protobuf/1.1/docs/compatibility/#compatibility-from-100
[artifact-lines]: https://flink-gcp.github.io/flink-datastream-protobuf/1.1/docs/compatibility/#artifact-lines-and-version-order
[upgrade]: https://flink-gcp.github.io/flink-datastream-protobuf/1.1/docs/compatibility/#verified-coverage-and-upgrade-procedure
[quickstart]: https://flink-gcp.github.io/flink-datastream-protobuf/1.1/docs/quickstart/
