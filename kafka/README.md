---
title: Kafka
description: Read Kafka topics as streams, and write streams into topics in one transaction per epoch, in JSON, Avro or Protobuf.
---

The `kafka` package has a source and a destination. The source reads each topic as a stream, from where the last read stopped. The destination writes each stream into a topic, a record per row, and commits each epoch in one Kafka transaction, so every row lands exactly once.

Both are tested against Apache Kafka 4.3 and the Karapace schema registry: without TLS or SASL, and through a SASL_SSL listener with PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER (from 0.2.0). The tests also show that the broker refuses a client certificate its CA didn't sign, but no test signs in with a client certificate the broker accepts. A registry's basic authentication and AWS MSK IAM (from 0.2.0) are supported, but no test runs them against a server: MSK IAM's signed request is tested on its own.

## Connecting

The source and the destination share their connection settings:

- `brokers`: the bootstrap brokers, as `host:port`. The client connects to each broker at the address the cluster advertises for it, so the runner must reach those too.
- TLS: `tls: true`, and `tlsCa` for your own certificate authority, as PEM text. Setting `tlsCa` or `tlsCert` turns TLS on. For mutual TLS, `tlsCert` and `tlsKey` are the client's certificate and its unencrypted key, also PEM text.
- SASL: `saslMechanism`, and what it needs:
  - `plain`, `scram-sha-256` or `scram-sha-512`: `saslUsername` and `saslPassword`. PLAIN sends the password as it is, so use it with TLS.
  - `oauthbearer` (from 0.2.0): `oauthTokenUrl`, `oauthClientId` and `oauthClientSecret`. The connector gets a token from the token endpoint with the OAuth client-credentials grant, asking for `oauthScopes` if set, and gets a new one before it expires. `oauthExtensions` are SASL extensions sent with the token, such as Confluent Cloud's `logicalCluster` and `identityPoolId`.
  - `aws-msk-iam` (from 0.2.0): Amazon MSK's IAM access control. The connector signs in with `awsAccessKeyId` and `awsSecretAccessKey` if set, or the AWS SDK's default credentials: the environment, a web identity token, a profile or the instance's role. The region is the one in the brokers' host names.
- `format`: how values and keys are encoded: `json` (the default), `avro` or `protobuf`. Avro and Protobuf use a schema registry's wire format, so they need `schemaRegistryUrl`, and `schemaRegistryUsername` and `schemaRegistryPassword` if it asks for basic authentication. Basic authentication sends the password as it is, so use an `https` URL: over `http`, the credentials cross the network unencrypted, and from 0.2.0 the check that signs in to the registry warns. Credentials in the URL, as `https://user:password@registry.internal`, sign in the same way; from 0.2.0, errors don't show them.

Keep `tlsKey`, `saslPassword`, `oauthClientSecret`, `awsAccessKeyId`, `awsSecretAccessKey` and `schemaRegistryPassword` in secrets, as `{ secret: NAME }`.

### Checks

From 0.2.0, `openetl connector check`, and the check OpenETL runs before a pipeline's first sync, report each step on its own:

- Reaching each bootstrap broker, and its TLS handshake with the configured certificates. With several brokers, one that can't be reached only warns.
- Signing in, as the configured user or OAuth client, and the cluster answering.
- Reaching every broker the cluster advertises, which is where reads and writes go.
- Reaching the schema registry and signing in to it, with `avro` or `protobuf`.
- The source: that each topic in `topics` exists and the credentials may read it, or that at least one topic is offered.
- The destination: that the credentials may read and write the marker topic, which is meant to be compacted, and the brokers' `transaction.max.timeout.ms`, which bounds the commit interval.

A failed step names what is missing: a wrong password, a certificate the client or the broker doesn't trust, or the operation a principal has no ACL for, such as `User:openetl has no READ on the topic orders`. The checks read the topics' authorized operations, so they need no more than the read or write itself does.

Before a run, the preflight checks each stream:

- The source: that its topic exists and may be read, that the topic keeps records longer than the time between a scheduled pipeline's syncs (a warning otherwise), that the schema registry has the topic's value subject with `avro` or `protobuf` (a warning otherwise), and that the credentials may commit offsets to the consumer group.
- The destination: that the stream is appended to, that the topic exists and may be written, or may be created; and that the pipeline's transactional IDs may be used. The preflight describes the IDs' transactions and never starts a producer with them, which would fence the pipeline's writers.

## Source

### Set it up

The credentials need DESCRIBE and READ on the topics, and READ on the consumer group `group`, to commit its offsets. With `avro` or `protobuf`, they need to read the topics' subjects in the schema registry.

### What it reads

Each topic is a stream, read with the `stream` sync mode: every topic the credentials can describe, except Kafka's internal ones, or those `topics` lists, matching `topicPattern` if it's set.

The source reads each partition from its next offset, as the last checkpoint recorded it, with `read_committed` isolation, so records of aborted transactions are never read. A topic it hasn't read before starts at `startOffset`: `earliest` (the default) or `latest`.

- A **scheduled** pipeline reads each partition up to the end it had when the sync started, then stops.
- A **continuous** pipeline keeps reading, and picks up partitions added to its topics within 30 seconds.

Once a checkpoint is committed, the source commits its offsets to the consumer group `group` (`openetl-<pipeline>` unless set), so Kafka's tools show the read's lag. The checkpoint, not the group, is where the next read starts.

If the topic's retention deleted records the read hadn't reached, the read stops and the pipeline gets an incident: the position it would resume from is gone. From 0.2.0, so does a read whose position is past its partition's end, or in a partition the topic no longer has: the topic was deleted and created again, or an unclean leader election lost records the read had read. Before 0.2.0, a scheduled read skipped such a partition's records up to the old position. A topic created again that has already grown past the read's position isn't noticed: the read carries on from the old offsets in the new topic.

### Rows

Each record becomes a row. A value that decodes to an object gives its fields as columns; any other value goes in a column named `value`. A record with no value, such as a compacted topic's tombstone, is a row of the columns below alone: the source doesn't read it as a delete. Compaction may also have removed a key's earlier records, and the tombstone itself once the topic's `delete.retention.ms` has passed. Every row also has:

| Column | What it holds |
|---|---|
| `_kafka_partition` | The record's partition |
| `_kafka_offset` | Its offset in the partition |
| `_kafka_timestamp` | Its timestamp, in UTC |
| `_kafka_key` | Its key as text: a string key's text, or a structured key as JSON. A key that isn't in the configured format is its bytes' text, or their base64 if they aren't UTF-8 |
| `_kafka_headers` | Its headers, as a JSON object of their values' text: from 0.2.0, the base64 of a value that isn't UTF-8, whose bytes that aren't were replaced before. A header key that the record repeats keeps only its last value, and a null value is an empty string |

With `avro` or `protobuf`, the catalog declares the value's fields as the latest schema under `<topic>-value` has them; with `json`, columns and their types come from the records. `format: string` reads each value as text, into `value`.

A value that doesn't decode in the configured format stops the read, with an error naming its topic, partition and offset. From 0.2.0, a value or key whose schema the registry doesn't serve for now, as when it doesn't answer, answers with a server error or refuses the credentials, stops the read with the registry's error, as a transient failure that OpenETL tries again, or as refused credentials. Before 0.2.0, such a key landed in `_kafka_key` as its encoded bytes.

### Types

With `avro` or `protobuf`, each value is decoded with the schema whose ID it carries, and each field of the latest schema is declared as:

| Avro | Protobuf | Declared as |
|---|---|---|
| `boolean` | `bool` | boolean |
| `int` | `int32`, `sint32`, `sfixed32` | 32-bit integer |
| `long` | `int64`, `sint64`, `sfixed64`, `uint32`, `fixed32` | 64-bit integer |
| | `uint64`, `fixed64` | decimal(20, 0) |
| `float`, `double` | `float`, `double` | 32-bit or 64-bit float, NaN and the infinities kept |
| `string`, `enum` | `string`, `enum` | text; a Protobuf enum value the schema doesn't name is its number |
| `bytes`, `fixed` | `bytes` | binary |
| `decimal` | | decimal of its precision and scale, up to precision 38; wider, text |
| `date` | | date |
| `time-millis`, `time-micros` | | time of day, in microseconds |
| `timestamp-millis`, `-micros`, `-nanos` | `google.protobuf.Timestamp` | timestamp in UTC, in microseconds |
| `local-timestamp-millis`, `-micros`, `-nanos` | | timestamp without a time zone, in microseconds |
| `uuid` on a `string`, or from 0.2.0 on a `fixed` of 16 | | UUID |
| `duration` (from 0.2.0) | | JSON: `months`, `days` and `milliseconds` |
| a union of `null` and one type, in either order | | that type |
| other unions, `array`, `map`, `record` | `repeated`, `map`, messages, other well-known types | JSON, as Protobuf's JSON mapping spells it for Protobuf |

Before 0.2.0, a `uuid` on a `fixed` and a `duration` were declared binary, which their values didn't fit. A Protobuf `google.protobuf.Any` decodes when its message is in the schema or a file it imports, from 0.2.0; one of a message neither has stops the read, as every `Any` did before.

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

From 0.2.0, a read that OpenETL gives an end stops there: it reads each partition up to the offset it was given, sends a resume point there, and completes there. OpenETL does this when it reads a writer lane's part of an epoch again on [several workers](#jobs-on-several-workers), so the second read returns the same records as the first, as long as the topic still has them. The end is a resume point the first read sent, or, for a partition whose read completed in that epoch, as a scheduled pipeline's does in its last epoch, the offset it completed at. Records added since are left for the next sync. Where the offsets just below the end no longer hold records the read returns, as once compaction has removed an aborted transaction and its marker, the read still completes at the end, once the brokers show that nothing is left below it: a record past the end, or a fetch that returns nothing below it. A partition whose brokers show neither for 2 minutes fails the read with a transient error, and OpenETL tries the attempt again, rather than completing at the end with records below it unread.

## Destination

### What it writes

Each stream goes to a topic named after its table, prefixed with the pipeline's `destination.namespace` or the config's `namespace` if either is set: `NAMESPACE.TABLE`. Missing topics are created, with `partitions` and `replicationFactor` or the brokers' defaults, unless `createTopics` is `false`.

Each row is one record:

- Its value is the row, in `format`.
- Its key is the row's primary key, in the same format, if the stream has one.
- Its header `openetl-id` holds the row's `_openetl_id`.

A record, with its key and headers, must fit in the client's batch limit of 1,000,012 bytes and in its topic's `max.message.bytes`. A row whose record doesn't fails its epoch, and the attempt; from 0.2.0 the error names the topic and the limits. Raising the topic's limit doesn't raise the client's.

With `avro` or `protobuf`, the destination registers each stream's schemas under `<topic>-value` and `<topic>-key`. Only append is supported: deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`. With `tombstones: true`, each delete is followed by a tombstone for its key, so a compacted topic drops the key.

### Exactly once

Each epoch is one Kafka transaction across all its topics, on one of three transactional producers, named `transactionalIdPrefix`, the pipeline and the producer's number. The transaction also writes the epoch's marker to the compacted topic `markerTopic` (`_openetl_markers` unless set), keyed by the pipeline. A replaced attempt's producers are fenced, and the marker shows whether an epoch committed, so no epoch lands twice. Consumers that read with `read_committed` isolation see an epoch's rows only once it commits. On [several workers](#jobs-on-several-workers), each writer lane's part of an epoch is a transaction of its own.

Streams whose source replays exactly land exactly once. A stream that may repeat rows, such as a Postgres table read incrementally, gets at least once: the destination has no way to drop the rows sent again.

A transaction must stay open for less than the brokers' `transaction.max.timeout.ms`, 15 minutes by default, so a pipeline's commit interval can be at most 7 minutes. The destination reads the brokers' setting when it opens, and from 0.2.0 in its check too, if the credentials may describe their configs. A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Jobs on several workers

From 0.2.0, the destination declares lane commits and up to 64 writer lanes, so the jobs of a pipeline that sets `resources.workers` can run on workers, as [Run a job on several pods](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md) explains. On one pod, a job has one writer lane, and writes as above.

- **A transaction per lane.** Each writer lane writes its part of an epoch in a transaction of its own, and commits it on its own. Consumers that read with `read_committed` isolation can see one lane's part of an epoch before another's.
- **Producers per lane.** Writer lane L has three transactional producers of its own, numbered 3L to 3L+2, so lane 0's are those of a job on one pod. A lane's transactional IDs are the same in every attempt: a new attempt's writer of a lane fences the old writer of that lane, and no other.
- **A marker per lane.** Each lane's transaction writes its marker of the epoch to `markerTopic`, keyed by the pipeline for lane 0 and by `PIPELINE/L` for lane L.
- **Recovery.** If a pod fails after a checkpoint and before every lane committed, the next attempt settles each lane of that epoch before it reads anything new. A lane whose marker committed is done. The destination keeps no copy of a lane's part, so for any other lane OpenETL reads the lane's part of the epoch again from the source and commits it in a new transaction of that lane. That is exact only if the source returns the same records, as this package's source does while the topic still has them, with the exception its [Delivery](#delivery) gives. [When a pod fails](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md#when-a-pod-fails) has what else it depends on.

Do not add partitions to the marker topic. Recovery looks for a lane's marker of an epoch in the partition where the lane's first marker of that epoch went. With more partitions, a lane's part that was written again after a failure can be written once more.

A transaction that a lost worker left open stays open until the brokers abort it after the transaction's timeout, unless a later attempt opens the same writer lane, which aborts it at once. An attempt opens every lane that the checkpoint it restores names. So this happens only when an attempt fails before its first checkpoint, and the next attempt has fewer writer lanes. Until the transaction ends, consumers that read with `read_committed` isolation do not get past it in the partitions it wrote to.

### Set it up

The credentials need to write to the topics and create them unless `createTopics` is `false`, to read and write the marker topic, and to use transactional IDs that start with `transactionalIdPrefix`: WRITE on those transactional IDs. The marker topic is created with `cleanup.policy=compact` if it doesn't exist.

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
