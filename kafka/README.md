---
title: Kafka
description: Read Kafka topics as streams, and write streams into topics in one transaction per epoch, in JSON, Avro or Protobuf.
---

The `kafka` package has a source and a destination. The source reads each topic as a stream, from where the last read stopped. The destination writes each stream into a topic, a record per row, and commits each epoch in one Kafka transaction, so every row lands exactly once.

Both are tested against Apache Kafka 4.3, without TLS or SASL, and the Karapace schema registry. TLS, SASL and a registry's basic authentication are supported, but no test covers them.

## Connecting

The source and the destination share their connection settings:

- `brokers`: the bootstrap brokers, as `host:port`.
- TLS: `tls: true`, and `tlsCa`, `tlsCert` and `tlsKey` for your own certificate authority and for mutual TLS. Setting `tlsCa` or `tlsCert` turns TLS on.
- SASL: `saslMechanism` (`plain`, `scram-sha-256` or `scram-sha-512`), with `saslUsername` and `saslPassword`.
- `format`: how values and keys are encoded: `json` (the default), `avro` or `protobuf`. Avro and Protobuf use a schema registry's wire format, so they need `schemaRegistryUrl`, and `schemaRegistryUsername` and `schemaRegistryPassword` if it asks for basic authentication.

Keep `tlsKey`, `saslPassword` and `schemaRegistryPassword` in secrets, as `{ secret: NAME }`.

## Source

### What it reads

Each topic is a stream, read with the `stream` sync mode: every topic the credentials can describe, except Kafka's internal ones, or those `topics` lists, matching `topicPattern` if it's set.

The source reads each partition from its next offset, as the last checkpoint recorded it, with `read_committed` isolation, so records of aborted transactions are never read. A topic it hasn't read before starts at `startOffset`: `earliest` (the default) or `latest`.

- A **scheduled** pipeline reads each partition up to the end it had when the sync started, then stops.
- A **continuous** pipeline keeps reading, and picks up partitions added to its topics within 30 seconds.

Once a checkpoint is committed, the source commits its offsets to the consumer group `group` (`openetl-<pipeline>` unless set), so Kafka's tools show the read's lag. The checkpoint, not the group, is where the next read starts.

If the topic's retention deleted records the read hadn't reached, the read stops and the pipeline gets an incident: the position it would resume from is gone.

### Rows

Each record becomes a row. A value that decodes to an object gives its fields as columns; any other value goes in a column named `value`. Every row also has:

| Column | What it holds |
|---|---|
| `_kafka_partition` | The record's partition |
| `_kafka_offset` | Its offset in the partition |
| `_kafka_timestamp` | Its timestamp, in UTC |
| `_kafka_key` | Its key as text: a string key's text, or a structured key as JSON |
| `_kafka_headers` | Its headers, as a JSON object |

With `avro` or `protobuf`, the catalog declares the value's fields as the latest schema under `<topic>-value` has them; with `json`, columns and their types come from the records. `format: string` reads each value as text, into `value`.

A value that doesn't decode in the configured format stops the read, with an error naming its topic, partition and offset.

### Example

The [config schema](./source.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: events
spec:
  connector: kafka
  config:
    brokers: ["kafka-1.internal:9092", "kafka-2.internal:9092"]
    tls: true
    saslMechanism: scram-sha-512
    saslUsername: openetl
    saslPassword: { secret: kafka-password }
    format: avro
    schemaRegistryUrl: https://registry.internal
    topicPattern: "^orders\\."
```

### Delivery

Reads replay exactly: a read resumed from a checkpoint yields the same records, in the same order, as long as the topic still has them.

From 0.2.0, a read that OpenETL gives an end stops there: it reads each partition up to the offset it was given, sends a resume point there, and completes. OpenETL does this when it reads a writer lane's part of an epoch again on [several workers](#jobs-on-several-workers), so the second read returns the same records as the first, as long as the topic still has them.

## Destination

### What it writes

Each stream goes to a topic named after its table, prefixed with the pipeline's `destination.namespace` or the config's `namespace` if either is set: `NAMESPACE.TABLE`. Missing topics are created, with `partitions` and `replicationFactor` or the brokers' defaults, unless `createTopics` is `false`.

Each row is one record:

- Its value is the row, in `format`.
- Its key is the row's primary key, in the same format, if the stream has one.
- Its header `openetl-id` holds the row's `_openetl_id`.

With `avro` or `protobuf`, the destination registers each stream's schemas under `<topic>-value` and `<topic>-key`. Only append is supported: deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`. With `tombstones: true`, each delete is followed by a tombstone for its key, so a compacted topic drops the key.

### Exactly once

Each epoch is one Kafka transaction across all its topics, on one of three transactional producers, named `transactionalIdPrefix`, the pipeline and the producer's number. The transaction also writes the epoch's marker to the compacted topic `markerTopic` (`_openetl_markers` unless set), keyed by the pipeline. A replaced attempt's producers are fenced, and the marker shows whether an epoch committed, so no epoch lands twice. Consumers that read with `read_committed` isolation see an epoch's rows only once it commits. On [several workers](#jobs-on-several-workers), each writer lane's part of an epoch is a transaction of its own.

Streams whose source replays exactly land exactly once. A stream that may repeat rows, such as a Postgres table read incrementally, gets at least once: the destination has no way to drop the rows sent again.

A transaction must stay open for less than the brokers' `transaction.max.timeout.ms`, 15 minutes by default, so a pipeline's commit interval can be at most 7 minutes. The destination checks the brokers' setting when it opens, if the credentials may describe their configs. A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Jobs on several workers

From 0.2.0, the destination declares lane commits and up to 64 writer lanes, so the jobs of a pipeline that sets `resources.workers` can run on workers, as [Run a job on several pods](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md) explains. On one pod, a job has one writer lane, and writes as above.

- **A transaction per lane.** Each writer lane writes its part of an epoch in a transaction of its own, and commits it on its own. Consumers that read with `read_committed` isolation can see one lane's part of an epoch before another's.
- **Producers per lane.** Writer lane L has three transactional producers of its own, numbered 3L to 3L+2, so lane 0's are those of a job on one pod. A lane's transactional IDs are the same in every attempt: a new attempt's writer of a lane fences the old writer of that lane, and no other.
- **A marker per lane.** Each lane's transaction writes its marker of the epoch to `markerTopic`, keyed by the pipeline for lane 0 and by `PIPELINE/L` for lane L.
- **Recovery.** If a pod fails after a checkpoint and before every lane committed, the next attempt settles each lane of that epoch before it reads anything new. A lane whose marker committed is done. The destination keeps no copy of a lane's part, so for any other lane OpenETL reads the lane's part of the epoch again from the source and commits it in a new transaction of that lane. That is exact only if the source returns the same records, as this package's source does while the topic still has them. [When a pod fails](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md#when-a-pod-fails) has what else it depends on.

Do not add partitions to the marker topic. Recovery looks for a lane's marker of an epoch in the partition where the lane's first marker of that epoch went. With more partitions, a lane's part that was written again after a failure can be written once more.

A transaction that a lost worker left open stays open until the brokers abort it after the transaction's timeout, unless a later attempt opens the same writer lane, which aborts it at once. An attempt opens every lane that the checkpoint it restores names. So this happens only when an attempt fails before its first checkpoint, and the next attempt has fewer writer lanes. Until the transaction ends, consumers that read with `read_committed` isolation do not get past it in the partitions it wrote to.

### Set it up

The credentials need to write to the topics and create them unless `createTopics` is `false`, to read and write the marker topic, and to use transactional IDs that start with `transactionalIdPrefix`. The marker topic is created with `cleanup.policy=compact` if it doesn't exist.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: changes
spec:
  connector: kafka
  config:
    brokers: ["kafka-1.internal:9092"]
    format: json
    namespace: cdc
```

### Schema changes

With `json`, nothing is registered, and a column whose type changes is written with its new type.

With `avro` or `protobuf`, the destination doesn't register a schema that changes a field's type from the subject's latest. The stream stops with a schema drift error, and the attempt's log shows the new schema: register it under the subject yourself, then resume the pipeline if OpenETL paused it, and the next attempt uses that schema. A schema the registry's compatibility setting refuses stops the stream the same way. The pipeline's `destination.typeChanges: allow` lets the destination register schemas that change a field's type itself, as far as the registry's compatibility setting allows.
