---
title: Amazon Kinesis
description: Read Kinesis data streams shard by shard, by polling or with enhanced fan-out, and write streams into Kinesis, a record per row, at least once.
---

The `kinesis` package has a source and a destination for Amazon Kinesis Data Streams. The source reads each Kinesis stream as a stream, shard by shard, from where the last read stopped, by polling or with enhanced fan-out. The destination writes each stream into a Kinesis stream, a record per row, and delivers at least once: Kinesis has no transactions.

Both are tested against kinesis-mock, an emulator, and against the Karapace schema registry for Avro and Protobuf. Both have also run against Amazon Kinesis with JSON records, which CI does not test. Enhanced fan-out's subscriptions are tested against a fake of Kinesis's event stream only: kinesis-mock doesn't send the first event the AWS SDK for Go waits for.

The [source's config schema](./source.schema.json) and the [destination's](./destination.schema.json) have every setting and its default. This page explains the ones that need judgment. Settings marked "From 0.2.0" arrive in that release.

## Connecting

The source and the destination share their connection settings:

- `region`: the streams' AWS region. Unset, the AWS default configuration's.
- `accessKeyId` and `secretAccessKey`, together. Without them, the AWS SDK's default credential chain, which finds only what the runner gives the connector: by default, nothing. Name an IAM role in the source's or destination's `aws.role`, which the runner assumes for the connector, on EC2 and EKS alike. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup. Keys in the config are used even if a role is named.
- `endpoint`: a Kinesis-compatible endpoint, such as a VPC endpoint or an emulator.
- `format`: how each record's data is encoded: `json` (the default), `avro` or `protobuf`, the same as the [Kafka connectors](../kafka/README.md). Avro and Protobuf need `schemaRegistryUrl`.

Keep `accessKeyId`, `secretAccessKey` and `schemaRegistryPassword` in secrets, as `{ secret: NAME }`.

### Retries

From 0.2.0. Kinesis throttles a shard or an account that goes over its limits. Both connectors retry a throttled call, and one that fails for a moment, after a random wait up to `retry.initialBackoff` (100 ms), doubled for each retry in a row, up to `retry.maxBackoff` (10 s). The AWS SDK tries each call up to `retry.maxAttempts` (10) times. After that, the connector keeps waiting and trying again while Kinesis keeps throttling, for up to `retry.maxElapsed` (5 minutes) without a call getting through. Then the read or the write fails with `RATE_LIMITED`, and OpenETL retries the attempt later. A call that gets through starts the count again.

### Check

From 0.2.0, checking a source or a destination runs a pre-flight. It says what it checked, item by item, and what to do about each item that fails, such as the IAM action to allow. It reads no records and writes nothing:

- Both reach Kinesis by listing streams, and the schema registry by listing its subjects if `schemaRegistryUrl` is set.
- The source then looks at each stream in `streams`, or the first five that match `streamPattern`. It describes the stream (its status, open shards, retention and encryption), lists its shards, and reads one shard from its newest record with `GetShardIterator` and `GetRecords`, which returns nothing. It warns if `startTimestamp` is older than the stream's retention. With enhanced fan-out, it lists the stream's consumers, or describes `consumer.name`, in place of the read. It doesn't register a consumer, so it lists what registering and subscribing need.
- The destination compares the streams already named `NAMESPACE.*`, up to ten, with what it would create, and warns of each that differs. It can't check creating streams or putting records without doing it, so it skips those items and lists the IAM actions they need.

## Source

### What it reads

Each Kinesis stream is a stream, read with the `stream` sync mode: every stream the credentials can list, or those `streams` lists, matching `streamPattern` if it's set.

The source reads each shard from its sequence number, as the last checkpoint recorded it. A parent shard is read to its end before its children, so a key's records stay in order across a resharding. A stream it hasn't read before starts at `startPosition`:

- `earliest` (the default): each shard's oldest record.
- `latest`: the records that arrive after the first read starts.
- `timestamp`, from 0.2.0: the records that arrived at or after `startTimestamp`, in RFC 3339, such as `2026-11-01T00:00:00Z`. If that's older than the stream's retention, the read starts at the oldest record the stream still holds.

A **scheduled** pipeline reads each shard until it has caught up, then stops. A **continuous** pipeline keeps reading.

Before it reads, the source checks that each shard still holds the record it would resume from. If the stream's retention period has passed it, the read stops and the pipeline gets an incident: the position it would resume from is gone.

### Polling or enhanced fan-out

From 0.2.0, `consumer.type` picks how shards are read:

- `polling` (the default) calls `GetRecords` on each shard. Every reader of a shard shares its 2 MB a second and five calls a second. `polling.interval` (200 ms, Kinesis's least) is the least time between two calls on a shard, and `polling.idleInterval` (1 s) the wait after a call that returned nothing. `polling.maxRecords` (10,000) caps the records a call returns. When other readers share the streams, raise `polling.interval` so that together they stay under five calls a second.
- `efo` reads with enhanced fan-out: a consumer registered on each stream, with 2 MB a second of its own per shard, which Kinesis pushes records to over `SubscribeToShard`. The first read registers the consumer, named `consumer.name` or else `openetl-` and the pipeline's name, and waits until it's active. The source subscribes to each shard again each time Kinesis ends a subscription, every five minutes. Once the pipeline is deleted, it deregisters the consumer from every stream it offers, unless `consumer.deregisterOnTeardown` is `false`. Kinesis charges for enhanced fan-out and allows 20 consumers a stream. Give each pipeline its own consumer: a shard takes one subscription per consumer at a time.

### Shards, lanes and backpressure

Each shard is a split, which OpenETL can read on its own and spread over a job's lanes and [workers](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md). From 0.2.0, a read fetches and emits for no more splits at a time than it has lanes. The others wait for a lane between fetches, so every shard still moves. A stream read whole counts as one lane, and its shards fetch together.

When OpenETL can't take rows as fast as the source reads them, sending them waits, and so does the shard's next fetch. With enhanced fan-out, the source takes the next event only once it has sent the last, so Kinesis's flow control holds the subscription back. Nothing is buffered beyond one fetch a shard.

### Aggregated records

From 0.2.0, records the Kinesis Producer Library aggregated are split into their user records, a row each, with the partition key each was put with. They share their Kinesis record's sequence number, and `_kinesis_subsequence` numbers them from 0. A checkpoint always falls between Kinesis records, never inside one. With `deaggregate: false`, an aggregated record fails the read, as data the format can't decode.

### Rows

Each record becomes a row, as in the [Kafka source](../kafka/README.md#rows): an object's fields as columns, any other value in `value`. Every row also has:

| Column | What it holds |
|---|---|
| `_kinesis_shard` | The record's shard |
| `_kinesis_sequence` | Its sequence number |
| `_kinesis_subsequence` | From 0.2.0: its user record's number within an aggregated record; 0 for a record that isn't one |
| `_kinesis_arrival` | When Kinesis received it, in UTC |
| `_kinesis_partition_key` | Its partition key |

### Set it up

Allow these actions on the streams the source reads, `arn:aws:kinesis:REGION:ACCOUNT:stream/NAME`:

- Polling: `kinesis:ListStreams`, `kinesis:DescribeStreamSummary`, `kinesis:ListShards`, `kinesis:GetShardIterator` and `kinesis:GetRecords`.
- Enhanced fan-out: the same, as the source still reads a shard to check its retention, and `kinesis:ListStreamConsumers`, `kinesis:DescribeStreamConsumer`, `kinesis:RegisterStreamConsumer` and `kinesis:DeregisterStreamConsumer`. Allow `kinesis:SubscribeToShard` on the consumers, `arn:aws:kinesis:REGION:ACCOUNT:stream/NAME/consumer/*`.
- A stream encrypted with a customer-managed KMS key also needs `kms:Decrypt` on the key.

### Example

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: clicks
spec:
  connector: kinesis
  config:
    region: eu-west-1
    streams: ["clicks"]
    startPosition: earliest
```

### Delivery

Reads replay exactly within the stream's retention period: a read resumed from a checkpoint yields the same records in the same order.

## Destination

### What it writes

Each stream goes to a Kinesis stream named after its table, prefixed with the pipeline's `destination.namespace` or the config's `namespace` if either is set: `NAMESPACE.TABLE`. A missing stream is created unless `createStreams` is `false`: on-demand by default, or with `streamMode: provisioned` and `shards` shards. From 0.2.0, `streamMode` picks the mode, which `shards` alone did before; `retentionHours` (24 to 8,760; 24 by default) sets its retention, and `kmsKeyId` encrypts it. These settings apply only to streams the destination creates. An existing stream is written to as it is, and a check warns of one that differs.

Each row is one record. Its data is the row in `format`, with Avro and Protobuf schemas registered under `<stream>-value`. A row over Kinesis's 1 MiB per record, once encoded, fails the write.

Only append is supported: deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`.

### Partition keys and order

From 0.2.0, `partitionKey.mode` picks each record's partition key, and so its shard:

- `key` (the default): the row's primary key as text, or its `_openetl_id` for a stream without one, so a key's rows go to one shard and stay in order.
- `fields`: the columns in `partitionKey.fields`, as text, so rows with the same values go to one shard, in order.
- `random`: a random key, which spreads rows evenly over shards, with no order between a key's rows.

A partition key longer than Kinesis's 256 characters is replaced by its SHA-256 hash.

Kinesis can accept part of a `PutRecords` request and refuse the rest, and the destination sends the refused records again. From 0.2.0, with `ordering: key` (the default), a request holds one record of each partition key, and a key's later records wait for a later request. So a record sent again can't land behind a later record of its key. That costs throughput when a few keys have most of the rows. `ordering: none` fills requests freely, which is faster for such keys, but a record sent again can land after its key's later records.

Each of a job's writer lanes, up to 16, sends its own requests, one at a time, and a key's rows stay on one lane. From 0.2.0, `rateLimit.recordsPerSecond` and `rateLimit.bytesPerSecond` cap what one destination process sends across its lanes, on average.

### At least once

The destination sends each epoch's records with `PutRecords` as they come, sends the ones Kinesis fails again until it has accepted all of them, and the epoch commits once it has. A record can't be taken back, so a retried epoch sends its records again: every stream gets at least once, and readers drop the repeats by `_openetl_id`. A pipeline into Kinesis has to accept that, with `delivery: { require: at_least_once }`.

### Set it up

The destination calls `DescribeStreamSummary` and `PutRecords`. To create streams, it calls `CreateStream` and `DescribeStream`, with `IncreaseStreamRetentionPeriod` for `retentionHours` over 24 and `StartStreamEncryption` for `kmsKeyId`. Its check calls `ListStreams`. Allow those actions on the streams it writes, `arn:aws:kinesis:REGION:ACCOUNT:stream/NAMESPACE.*`. Streams encrypted with a customer-managed KMS key also need `kms:GenerateDataKey` on the key.

### Example

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: click-stream
spec:
  connector: kinesis
  config:
    region: eu-west-1
    namespace: cdc
    format: json
```

### Schema changes

The same as the [Kafka destination's](../kafka/README.md#schema-changes): JSON registers nothing, and with Avro or Protobuf the destination doesn't register a schema that changes a field's type unless the pipeline's `destination.typeChanges` allows it.
