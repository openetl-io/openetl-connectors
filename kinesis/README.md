---
title: Amazon Kinesis
description: Read Kinesis data streams shard by shard, and write streams into Kinesis, a record per row, at least once.
---

The `kinesis` package has a source and a destination for Amazon Kinesis Data Streams. The source reads each Kinesis stream as a stream, shard by shard, from where the last read stopped. The destination writes each stream into a Kinesis stream, a record per row, and delivers at least once: Kinesis has no transactions.

Both are tested against kinesis-mock, an emulator, and against the Karapace schema registry for Avro and Protobuf. Both have also run against Amazon Kinesis with JSON records, which CI does not test.

## Connecting

The source and the destination share their connection settings:

- `region`: the streams' AWS region. Unset, the AWS default configuration's.
- `accessKeyId` and `secretAccessKey`, together. Without them, the AWS SDK's default credential chain, which finds only what the runner gives the connector: by default, nothing. Name an IAM role in the source's or destination's `aws.role`, which the runner assumes for the connector, on EC2 and EKS alike. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup. Keys in the config are used even if a role is named.
- `endpoint`: a Kinesis-compatible endpoint, such as a VPC endpoint or an emulator.
- `format`: how each record's data is encoded: `json` (the default), `avro` or `protobuf`, the same as the [Kafka connectors](../kafka/README.md). Avro and Protobuf need `schemaRegistryUrl`.

Keep `accessKeyId`, `secretAccessKey` and `schemaRegistryPassword` in secrets, as `{ secret: NAME }`.

## Source

### What it reads

Each Kinesis stream is a stream, read with the `stream` sync mode: every stream the credentials can list, or those `streams` lists, matching `streamPattern` if it's set.

The source polls each shard from its sequence number, as the last checkpoint recorded it. A parent shard is read to its end before its children, so a key's records stay in order across a resharding. A stream it hasn't read before starts at `startPosition`: `earliest` (the default), each shard's oldest record, or `latest`, the records that arrive after the read starts.

- A **scheduled** pipeline reads each shard until it has caught up, then stops.
- A **continuous** pipeline keeps reading.

Before it reads, the source checks that each shard still holds the record it would resume from. If the stream's retention period has passed it, the read stops and the pipeline gets an incident: the position it would resume from is gone.

### Rows

Each record becomes a row, as in the [Kafka source](../kafka/README.md#rows): an object's fields as columns, any other value in `value`. Every row also has:

| Column | What it holds |
|---|---|
| `_kinesis_shard` | The record's shard |
| `_kinesis_sequence` | Its sequence number |
| `_kinesis_arrival` | When Kinesis received it, in UTC |
| `_kinesis_partition_key` | Its partition key |

### Set it up

The source calls `ListStreams`, `DescribeStreamSummary`, `ListShards`, `GetShardIterator` and `GetRecords`, so its credentials' IAM policy has to allow those actions on the streams it reads.

### Example

The [config schema](./source.schema.json) has every setting.

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

Each stream goes to a Kinesis stream named after its table, prefixed with the pipeline's `destination.namespace` or the config's `namespace` if either is set: `NAMESPACE.TABLE`. A missing stream is created, on-demand, or provisioned with `shards` shards if that's set, unless `createStreams` is `false`.

Each row is one record. Its data is the row in `format`, with Avro and Protobuf schemas registered under `<stream>-value`. Its partition key is the row's primary key as text, or its `_openetl_id` for a stream without one, so a key's rows go to one shard and stay in order. A partition key longer than Kinesis's 256 characters is replaced by its SHA-256 hash. A row over Kinesis's 1 MiB per record, once encoded, fails the write.

Only append is supported: deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`.

### At least once

The destination sends each epoch's records with `PutRecords` as they come, sends the ones Kinesis fails again until it has accepted all of them, and the epoch commits once it has. A record can't be taken back, so a retried epoch sends its records again: every stream gets at least once, and readers drop the repeats by `_openetl_id`. A pipeline into Kinesis has to accept that, with `delivery: { require: at_least_once }`.

### Set it up

The destination calls `DescribeStreamSummary` and `PutRecords`, and to create streams, `CreateStream` and `DescribeStream`; its check calls `ListStreams`. Its IAM policy has to allow those actions on the streams it writes.

### Example

The [config schema](./destination.schema.json) has every setting.

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
