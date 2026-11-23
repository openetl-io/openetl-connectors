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
- `format`: how each record's data is encoded: `json` (the default), `avro` or `protobuf`, the same as the [Kafka connectors](../kafka/README.md). Avro and Protobuf need `schemaRegistryUrl`, and `schemaRegistryUsername` and `schemaRegistryPassword` if it asks for basic authentication. Basic authentication sends the password as it is, so use an `https` URL: over `http`, the credentials cross the network unencrypted, and from 0.2.0 the `registry.reach` check warns. Credentials in the URL, as `https://user:password@registry.internal`, sign in the same way; from 0.2.0, errors and the `registry.reach` check's title don't show them, even a password with a `#`, `?` or `/` in it.

Keep `accessKeyId`, `secretAccessKey` and `schemaRegistryPassword` in secrets, as `{ secret: NAME }`.

### Retries

From 0.2.0. Kinesis throttles a shard or an account that goes over its limits. Both connectors retry a throttled call, and one that fails for a moment, after a random wait up to `retry.initialBackoff` (100 ms), doubled for each retry in a row, up to `retry.maxBackoff` (10 s). The AWS SDK tries each call up to `retry.maxAttempts` (10) times. After that, the connector keeps waiting and trying again while Kinesis keeps throttling, for up to `retry.maxElapsed` (5 minutes) without a call getting through, and no wait runs past it. Then the read or the write fails with `RATE_LIMITED`, and OpenETL retries the attempt later. A call that gets through starts the count again.

### Check and preflight

From 0.2.0, both report what they looked at, item by item. Each item that fails or warns says what to do, such as the IAM action to allow and the resource AWS named. Neither reads records or writes anything. Item names stay the same from one release to the next.

A **check** is about the connection alone, and runs from a source's or a destination's page:

| Item | What it checks |
|---|---|
| `net.dns`, `net.connect` | With `endpoint` set, and no proxy to reach it: its host resolves, and its port takes a connection. |
| `kinesis.sign-in` | The connector finds AWS credentials, and Kinesis takes them to list a stream. The destination doesn't list streams, so for it, Kinesis refusing the list for want of a permission passes. |
| `registry.reach` | With `format` `avro` or `protobuf`: the schema registry lists its subjects. From 0.2.0, it warns if credentials for the registry, from the settings or the URL, go over `http`. |
| `streams.offered` | The source only: the streams it offers. It warns if `streams` and `streamPattern` match none, or if `streams` names one that isn't among them. |

A **preflight** is about a pipeline: OpenETL runs it with the streams a sync of the pipeline would read and write, after both checks pass. Its items for a stream start with `stream.NAME.`, where NAME is the stream's name in lowercase with each run of other characters than letters and digits replaced by `-`, followed by a hash if two streams' NAMEs would be the same.

For the source, `start-timestamp` fails a `startTimestamp` in the future. Then, for each stream the pipeline reads:

- `mode` fails a sync mode other than `stream`.
- `describe` fails a stream that doesn't exist or is being deleted, and warns of one still being created. Otherwise it reports the stream's open shards, retention and encryption.
- `start` warns if `startTimestamp` is older than the stream's retention. `schedule` warns if the pipeline's schedule can leave more time between syncs than the stream keeps records.
- `shards` lists the stream's shards, and `read` reads one from its newest record with `GetShardIterator` and `GetRecords`, which returns nothing.
- With enhanced fan-out, `consumer` finds the pipeline's consumer, by the name the read gives it, and says if the first read registers it. Without the pipeline's name, it warns, as it can't tell the consumer's. If the consumer isn't registered, `consumer-limit` warns when the stream already has 20 consumers, as registering it may then fail. `subscribe` is skipped, as registering a consumer changes the stream, and names the IAM actions that registering and subscribing take.

For the destination, for each stream the pipeline writes:

- `mode` fails a write mode other than append.
- `keys` fails if the stream lacks a column that partition keys are made of, from its primary key or `partitionKey.fields`.
- `target` looks up the Kinesis stream the writer would use, `NAMESPACE.TABLE`. If it exists, the item warns if the stream isn't active, or differs from `streamMode`, `shards`, `retentionHours` or `kmsKeyId`, unless the destination created it and its next write finishes setting it up. If it doesn't exist, the item fails when `createStreams` is `false`; otherwise it's skipped, and says how the first write creates the stream and the IAM actions that takes.

Then `put` is skipped, as putting a record would write to a stream, and names the IAM actions that writing takes.

## Source

### What it reads

Each Kinesis stream is a stream, read with the `stream` sync mode: every stream the credentials can list, or those `streams` lists, matching `streamPattern` if it's set.

The source reads each shard from its sequence number, as the last checkpoint recorded it. A parent shard is read to its end before its children, so a key's records stay in order across a resharding. Once a shard is read to its end, the source lists the stream's shards every second until the listing has the shard's children, which Kinesis can list late. A scheduled read tries five times, and the next sync reads the children it didn't find. From 0.2.0, a continuous read keeps listing until it finds them, and reads them without the pipeline starting again; before, it gave up after five tries too, and read them only once its job started again. While the read waits, its other shards' ends wait too, and a stream read whole sends nothing. A stream it hasn't read before starts at `startPosition`:

- `earliest` (the default): each shard's oldest record.
- `latest`: the records that arrive after the first read starts.
- `timestamp`, from 0.2.0: the records that arrived at or after `startTimestamp`, in RFC 3339, such as `2026-11-01T00:00:00Z`. If that's older than the stream's retention, the read starts at the oldest record the stream still holds. The time must have passed: Kinesis starts a read only at a time that has. A time in the future fails the preflight, and the first read fails with `CONFIG_INVALID`.

`latest` starts each shard at the time the first read started, by the runner's clock. From 0.2.0, where Kinesis refuses a start at a time the runner's clock has passed, as Kinesis's clock is behind it, the read tries again every second, for up to 5 minutes after that time, rather than failing with `CONFIG_INVALID`. The same holds for a `startTimestamp` that has just passed.

A **scheduled** pipeline reads each shard until it has caught up, then stops. A **continuous** pipeline keeps reading.

Before it reads, the source checks that each shard still holds the record it would resume from. If the stream's retention period has passed it, the read stops and the pipeline gets an incident: the position it would resume from is gone.

From 0.2.0, a read further behind than the retention period checks again each time it needs a new shard iterator, its last having expired after 5 minutes unused, or a new enhanced fan-out subscription: Kinesis would otherwise go on from the oldest record left, past the ones it removed, without an error. Between those checks, a read that stays further behind than the retention period can still miss records Kinesis removes before the read reaches them, so keep reads well within it.

### Polling or enhanced fan-out

From 0.2.0, `consumer.type` picks how shards are read:

- `polling` (the default) calls `GetRecords` on each shard. Every reader of a shard shares its 2 MB a second and five calls a second. `polling.interval` (200 ms, Kinesis's least) is the least time between two calls on a shard, and `polling.idleInterval` (1 s, or `polling.interval` if that's longer) the wait after a call that returned nothing. `polling.maxRecords` (10,000) caps the records a call returns. When other readers share the streams, raise `polling.interval` so that together they stay under five calls a second.
- `efo` reads with enhanced fan-out: a consumer registered on each stream, with 2 MB a second of its own per shard, which Kinesis pushes records to over `SubscribeToShard`. The first read registers the consumer, named `consumer.name` or else `openetl-` and the pipeline's name, and waits until it's active. Where the pipeline's name has characters a consumer's can't, or is longer than 120 characters, those are replaced with `-` or cut, and `-` and a hash of the whole name follow, so two pipelines don't share a consumer. The source subscribes to each shard again each time Kinesis ends a subscription, every five minutes. Once the pipeline is deleted, it deregisters the consumer from every stream it offers, unless `consumer.deregisterOnTeardown` is `false`. Kinesis charges for enhanced fan-out, and allows 20 consumers a stream, or 50 for an On-demand Advantage stream. If Kinesis refuses to register the consumer for want of room until the read stops waiting, after 5 minutes, the read fails with `RESOURCE_EXHAUSTED`. Give each pipeline its own consumer: a shard takes one subscription per consumer at a time.

### Shards, lanes and backpressure

Each shard is a split, which OpenETL can read on its own and spread over a job's lanes and [workers](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md). From 0.2.0, a scheduled pipeline's read fetches and emits for no more splits at a time than it has lanes. The others wait for a lane between fetches, so every shard still moves. A stream read whole counts as one lane, and its shards fetch together. A continuous pipeline's read fetches from every split as it goes, whatever its lanes: its splits don't end, so none waits for another's lane.

> [!WARNING]
> In a continuous pipeline on more than one worker, the shard that Kinesis merges two shards into can be read twice, and before the last records of one of them, when the two are read on different workers. A read takes a listed shard that isn't one of its splits, nor below one, as read to its end, so each worker reads the merged shard as soon as its own parent ends. A scheduled pipeline's read leaves the merged shard for the next sync, which reads it once, after both parents, and the shards a split makes are read only by the worker that reads their parent. Run a continuous pipeline on a stream whose shards are merged, as `UpdateShardCount` does to lower the count, or that still lists a merge's parents, with `resources.workers: 1`.

When OpenETL can't take rows as fast as the source reads them, sending them waits, and so does the shard's next fetch. With enhanced fan-out, the source takes the next event only once it has sent the last, so Kinesis's flow control holds the subscription back. Nothing is buffered beyond one fetch a shard.

### Aggregated records

From 0.2.0, records the Kinesis Producer Library aggregated are split into their user records, a row each, with the partition key each was put with. They share their Kinesis record's sequence number, and `_kinesis_subsequence` numbers them from 0. A checkpoint always falls between Kinesis records, never inside one. With `deaggregate: false`, an aggregated record fails the read with `DATA_INVALID`, whatever the `format`. So does an aggregated record that breaks the Kinesis Producer Library's format, such as one with a user record without data.

### Rows

Each record becomes a row, as in the [Kafka source](../kafka/README.md#rows): an object's fields as columns, any other value in `value`. Every row also has:

| Column | What it holds |
|---|---|
| `_kinesis_shard` | The record's shard |
| `_kinesis_sequence` | Its sequence number |
| `_kinesis_subsequence` | From 0.2.0: its user record's number within an aggregated record; 0 for a record that isn't one |
| `_kinesis_arrival` | When Kinesis received it, in UTC |
| `_kinesis_partition_key` | Its partition key |

With `avro` or `protobuf`, each record's data is decoded with the schema whose ID it carries, and each field of the latest schema under `<stream>-value` is declared as in the [Kafka source](../kafka/README.md#types). From 0.2.0, a `uuid` on a `fixed` of 16 is declared a UUID, and a `duration` JSON of its `months`, `days` and `milliseconds`; before, both were declared binary, which their values didn't fit. From 0.2.0 too, a Protobuf `google.protobuf.Any` decodes when its message is in the schema or a file it imports; one of a message neither has stops the read, as every `Any` did before.

Data that doesn't decode in the configured `format` stops the read with `DATA_INVALID`, naming its shard and sequence number. From 0.2.0, data whose schema the registry doesn't serve for now, as when it doesn't answer, answers with a server error or too many requests, or refuses the credentials, stops the read with the registry's error instead: `TRANSIENT`, or `AUTH_INVALID` or `PERMISSION_DENIED` for credentials it refuses. Before 0.2.0, that was `DATA_INVALID` too. A schema ID the registry doesn't have is still `DATA_INVALID`.

### Set it up

Allow these actions in an IAM policy of the source's identity:

- `kinesis:ListStreams` on `"Resource": "*"`, in a statement of its own: IAM can't limit it to some streams.
- On the streams the source reads, `arn:aws:kinesis:REGION:ACCOUNT:stream/NAME`: `kinesis:DescribeStreamSummary`, `kinesis:ListShards`, `kinesis:GetShardIterator` and `kinesis:GetRecords`. Enhanced fan-out needs them too, as the source still reads a shard to check its retention.
- With enhanced fan-out, also `kinesis:RegisterStreamConsumer` on the streams, and `kinesis:DescribeStreamConsumer`, `kinesis:SubscribeToShard` and `kinesis:DeregisterStreamConsumer` on their consumers, `arn:aws:kinesis:REGION:ACCOUNT:stream/NAME/consumer/*`.
- On a stream encrypted with a customer-managed KMS key, `kms:Decrypt` on the key.

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

From 0.2.0, a redo that OpenETL asks for reads each shard up to the sequence number where the recorded read sent its resume point, or completed, and no further. It reads no shard the recorded read hadn't reached by then, adds no shards, and ends with a resume point there and completes there. If retention has removed that record, the redo fails with `POSITION_LOST`, as it can't read the same records again.

A bounded read that finds a shard caught up records when it did, unless it recorded that within the last minute. So a read again from where one stopped, within that minute, completes where it did.

## Destination

### What it writes

Each stream goes to a Kinesis stream named after its table, prefixed with the pipeline's `destination.namespace` or the config's `namespace` if either is set: `NAMESPACE.TABLE`. A missing stream is created unless `createStreams` is `false`: on-demand by default, or with `streamMode: provisioned` and `shards` shards. From 0.2.0, `streamMode` picks the mode, which `shards` alone did before; `retentionHours` (24 to 8,760; 24 by default) sets its retention, and `kmsKeyId` encrypts it. These settings apply only to streams the destination creates. An existing stream is written to as it is, and a preflight warns of one that differs.

Kinesis creates a stream with 24 hours of retention and no encryption, so the destination sets `retentionHours` and `kmsKeyId` once the stream is active. Until it has, the stream has the tag `openetl.io/setup`. If setting them fails, the next write sets them and removes the tag. The destination doesn't change a stream without that tag. If it isn't allowed to list an existing stream's tags, it treats the stream as one it didn't create, unless `kmsKeyId` is set and the stream isn't encrypted with that key: then the write fails, so records never go unencrypted to a stream whose setup didn't finish.

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

The destination sends each epoch's records with `PutRecords` as they come, sends the ones Kinesis fails again until it has accepted all of them, and the epoch commits once it has. From 0.2.0, what's left of an epoch's records goes before it commits without the next epoch's that have come: those go once a request fills, or before their own epoch commits. A record can't be taken back, so a retried epoch sends its records again: every stream gets at least once, and readers drop the repeats by `_openetl_id`. A pipeline into Kinesis has to accept that, with `delivery: { require: at_least_once }`.

### Set it up

Allow these actions in an IAM policy of the destination's identity, on the streams it writes, `arn:aws:kinesis:REGION:ACCOUNT:stream/NAMESPACE.*`:

- `kinesis:DescribeStreamSummary` and `kinesis:PutRecords`.
- To create streams, `kinesis:CreateStream` and `kinesis:DescribeStream`. With `retentionHours` over 24 or `kmsKeyId`, also `kinesis:AddTagsToStream`, `kinesis:ListTagsForStream` and `kinesis:RemoveTagsFromStream`, for the setup tag, and `kinesis:IncreaseStreamRetentionPeriod` for `retentionHours`, `kinesis:StartStreamEncryption` for `kmsKeyId`.
- On a stream encrypted with a customer-managed KMS key, `kms:GenerateDataKey` on the key.

Its check lists streams with `kinesis:ListStreams`. The destination doesn't need it: without it, the check passes once Kinesis takes the credentials. To allow it, give it `"Resource": "*"` in a statement of its own, as IAM can't limit it to some streams.

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
