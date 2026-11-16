---
title: Amazon Kinesis
description: Read Kinesis data streams shard by shard, and write streams into Kinesis, a record per row, at least once.
---

The `kinesis` package has a source and a destination for Amazon Kinesis Data Streams. The source reads each Kinesis stream as a stream, shard by shard, from where the last read stopped. The destination writes each stream into a Kinesis stream, a record per row, and delivers at least once: Kinesis has no transactions.

Both are tested against kinesis-mock, an emulator, and against the Karapace schema registry for Avro and Protobuf. Both have also run against Amazon Kinesis with JSON records, which CI does not test. kinesis-mock takes any credentials and allows every action, so no test covers rejected credentials or a denied action, nor the checks' STS lookup, which they skip with an `endpoint`.

## Connecting

The source and the destination share their connection settings:

- `region`: the streams' AWS region. Unset, the AWS default configuration's.
- `accessKeyId` and `secretAccessKey`, together. Without them, the AWS SDK's default credential chain, which finds only what the runner gives the connector: by default, nothing. Name an IAM role in the source's or destination's `aws.role`, which the runner assumes for the connector, on EC2 and EKS alike. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup. Keys in the config are used even if a role is named.
- `endpoint`: a Kinesis-compatible endpoint, such as a VPC endpoint or an emulator.
- `format`: how each record's data is encoded: `json` (the default), `avro` or `protobuf`, the same as the [Kafka connectors](../kafka/README.md). Avro and Protobuf need `schemaRegistryUrl`.

Keep `accessKeyId`, `secretAccessKey` and `schemaRegistryPassword` in secrets, as `{ secret: NAME }`.

### Checks

From 0.2.0, `openetl connector check`, and the check OpenETL runs before a pipeline's first sync, report each step on its own:

- Reaching Kinesis: the `endpoint`, or with `region` set, the region's Kinesis endpoint. Reaching AWS's own endpoint only warns, since the runner may reach it only through a proxy, which the AWS SDK uses and the check's dial doesn't.
- Signing in: listing streams shows that Kinesis takes the credentials. Without an `endpoint`, the checks and the preflight first ask STS for the credentials' identity, to name it in a failure.
- Reaching the schema registry and signing in to it, if `schemaRegistryUrl` is set.
- The source: that each stream in `streams` exists, or that at least one stream the credentials can list is offered.

Before a run, the preflight checks each stream:

- The source: that the Kinesis stream exists and is active, with its capacity mode, open shards and retention; that the credentials may list its shards and read one, from its latest record, which reads nothing; that the stream keeps records longer than the time between a scheduled pipeline's syncs (a warning otherwise); and with `avro` or `protobuf`, that the registry has its value subject (a warning otherwise).
- The destination: that the stream is appended to, and that its Kinesis stream exists and is active, or will be created. The preflight doesn't test `PutRecords`, which would add a record to the stream: a denied `PutRecords` shows only when the pipeline writes.

A denied action names the action, and the identity if STS gave it, such as `arn:aws:iam::123456789012:role/openetl has no kinesis:GetRecords on clicks`.

## Source

### What it reads

Each Kinesis stream is a stream, read with the `stream` sync mode: every stream the credentials can list, or those `streams` lists, matching `streamPattern` if it's set.

The source polls each shard from its sequence number, as the last checkpoint recorded it. A parent shard is read to its end before its children, so a key's records stay in order across a resharding. A stream it hasn't read before starts at `startPosition`: `earliest` (the default), each shard's oldest record, or `latest`, the records that arrive after the read starts.

- A **scheduled** pipeline reads each shard until it has caught up, then stops.
- A **continuous** pipeline keeps reading.

Each read records, for each shard, when it last found nothing after its position: the last record's arrival, or when it caught up. A shard read within the stream's retention period of that time can't have lost anything, idle or not. Past it, the source checks that the shard still holds the record it would resume from; if it doesn't, the read stops and the pipeline gets an incident, because records that arrived after the position may have expired unread.

> [!NOTE]
> Kinesis can't tell whether records after a position expired: its sequence numbers aren't contiguous, and the API reports no trimmed range. So the source decides from when it last found the shard caught up. A pipeline that doesn't sync a stream within its retention period gets the incident for a shard whose records have all expired, even if none arrived after its position.

### Rows

Each record becomes a row, as in the [Kafka source](../kafka/README.md#rows): an object's fields as columns, any other value in `value`. Every row also has:

| Column | What it holds |
|---|---|
| `_kinesis_shard` | The record's shard |
| `_kinesis_sequence` | Its sequence number |
| `_kinesis_arrival` | When Kinesis received it, in UTC |
| `_kinesis_partition_key` | Its partition key |

### Set it up

The source calls `ListStreams`, `DescribeStreamSummary`, `ListShards`, `GetShardIterator` and `GetRecords`, so its credentials' IAM policy has to allow those actions on the streams it reads. Its check also calls `sts:GetCallerIdentity`, which needs no permission, when no `endpoint` is set.

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

From 0.2.0, a read that OpenETL gives an end stops there: it reads each shard up to and including the sequence number it was given, reads nothing of a shard the end doesn't have, sends a resume point there, and completes. OpenETL does this when it reads a writer lane's part of an epoch again on several workers, so the second read returns the same records as the first, as long as the stream still has them. A read across a resharding still reads a parent to the end it was given before its children.

OpenETL gives no end for a shard whose read completed in that epoch, as a scheduled pipeline's does in its last epoch. The second read then goes to where the shard has caught up, as it is then. It returns the same records unless records were added since. It completes at the same position too, unless the shard was idle for more than a minute before the epoch: then each read records when it found the shard caught up, which differs between the two.

The source has no consumer position in Kinesis to acknowledge, as Kafka's consumer groups are: Kinesis keeps one only for the Kinesis Client Library's lease table or an enhanced fan-out consumer, and the source uses neither. Where it resumes is the checkpoint alone, and nothing in AWS shows the read's position.

## Destination

### What it writes

Each stream goes to a Kinesis stream named after its table, prefixed with the pipeline's `destination.namespace` or the config's `namespace` if either is set: `NAMESPACE.TABLE`. A missing stream is created, on-demand, or provisioned with `shards` shards if that's set, unless `createStreams` is `false`.

Each row is one record. Its data is the row in `format`, with Avro and Protobuf schemas registered under `<stream>-value`. Its partition key is the row's primary key as text, or its `_openetl_id` for a stream without one, so a key's rows go to one shard and stay in order, except across a retry of records Kinesis failed, as [At least once](#at-least-once) says. A partition key longer than Kinesis's 256 characters is replaced by its SHA-256 hash. A row over Kinesis's 1 MiB per record, once encoded, fails the write.

Only append is supported: deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`.

### At least once

The destination sends each epoch's records with `PutRecords` as they come, sends the ones Kinesis fails again until it has accepted all of them, and the epoch commits once it has. A record can't be taken back, so a retried epoch sends its records again: every stream gets at least once, and readers drop the repeats by `_openetl_id`. A pipeline into Kinesis has to accept that, with `delivery: { require: at_least_once }`.

Nothing stronger is possible with Kinesis's API: it has no transactions, `PutRecords` has no idempotency key, and a record's sequence number is known only once Kinesis has accepted it. A retry can't learn which of an epoch's records landed before a failure without reading the whole stream back, so the destination doesn't try. One `PutRecords` call can also land some of its records and fail others, when a shard is over its throughput or Kinesis fails inside. The destination sends the failed ones again, so each lands after the records of its key that followed it in that call: a key's order holds except across such a retry.

### Set it up

The destination calls `DescribeStreamSummary` and `PutRecords`, and to create streams, `CreateStream` and `DescribeStream`; its check calls `ListStreams`, and without an `endpoint`, `sts:GetCallerIdentity`. Its IAM policy has to allow those actions on the streams it writes.

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
