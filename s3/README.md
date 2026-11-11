---
title: Amazon S3
description: Write streams as Parquet files on S3, an S3-compatible store or a local disk, appending each epoch's files exactly once.
---

The `s3` package has a destination. It writes each stream as a directory of Parquet files, on Amazon S3, on another S3-compatible store, or on a local filesystem with a `file://` URL. Each epoch's files appear once, under names that can't collide, and a marker records that the epoch landed.

It is tested on a local filesystem and against SeaweedFS's S3 API. It has also run against Amazon S3, which CI does not test.

## What it writes

Each stream's table is a directory under `url`, named after the table, below the pipeline's `destination.namespace` if it sets one: `URL/NAMESPACE/TABLE/`. Each epoch adds one or more Parquet files to it, named after the pipeline's ID, the epoch, its generation and the file's number, with the writer lane's before it for lanes after the first:

```text
s3://my-bucket/raw/orders/9af7d67a-fb93-45a8-9061-af62ce29978d-00000000000000000042-g00000000000000000001-00000.parquet
```

A file closes at `fileSize` bytes, 256 MiB by default, and the next one starts. Files are compressed with `compression`: `zstd` (the default), `snappy`, `gzip` or `none`.

The destination keeps what it needs besides the tables under `URL/_openetl/`: files being staged, a marker per stream and epoch, and a record of each table's columns. Leave it in place, and point readers at the table directories.

Only append is supported. Deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`, and a reader that wants the latest version of each row picks it by `_openetl_seq`.

## Exactly once

Writes go to staging first. To commit an epoch, the destination copies each staged file to its final name, then writes the stream's marker. An epoch is committed only once OpenETL's checkpoint has recorded it, under the generation in its file names, and a repeated commit copies the same files to the same names, so a file never appears twice. The marker shows that an epoch is done.

Streams whose source replays exactly land exactly once. A stream whose source may send rows again, such as a Postgres table read incrementally, gets at least once: the destination can't drop the rows sent again, so a pipeline with such a stream has to accept that with `delivery: { require: at_least_once }`, and readers drop the repeats by `_openetl_id`.

Each file appears when it's copied, so readers can see part of an epoch while it commits, and different streams' files land at different times.

A continuous pipeline commits every minute unless it sets its own `delivery.commit.interval`. Each commit adds at least one file per stream, so shorter intervals make more, smaller files.

## Set it up

`url` is `s3://BUCKET/PREFIX`, or `file://` and an absolute path. For S3, set `region`, and `accessKeyId` and `secretAccessKey`. For another S3-compatible store, set `endpoint` too, which uses path-style addressing.

Without `accessKeyId` and `secretAccessKey`, the destination uses the AWS SDK's default credential chain, which finds only what the runner gives it. By default, the runner gives it no AWS credentials. Name an IAM role in the destination's `aws.role`, which the runner assumes for it, on EC2 and EKS alike. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup, and the runner's `inherit` setting. With keys, keep them in secrets, as `{ secret: NAME }`. Keys in the config are used even if the destination names a role.

The credentials need to read, write, copy, delete and list objects under the URL. The destination's check writes and deletes an object under `_openetl/`. If the destination has no AWS credentials, or S3 refuses them, the check and syncs fail with `AUTH_INVALID`, `AUTH_EXPIRED` or `PERMISSION_DENIED`. OpenETL retries only `AUTH_EXPIRED`.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: lake-files
spec:
  connector: s3
  config:
    url: s3://my-bucket/openetl
    region: eu-west-1
```

The [config schema](./destination.schema.json) has every setting.

## Types

Columns keep their Arrow types in Parquet, timestamps to the nanosecond if the pipeline asks for nanoseconds, with one exception: intervals, which Parquet can't hold to the nanosecond, become a struct of `months` (32-bit integer), `days` (32-bit integer) and `nanoseconds` (64-bit integer), the parts JSON destinations spell them with.

## Schema changes

New columns are added: later files have them, and earlier files don't.

Readers expect a column to have one type across a table's files, so the destination keeps a record of each table's columns in `URL/_openetl/schemas/TABLE.json`, and doesn't change a column's type by itself. When a stream's column changes type, the destination refuses the change, the stream stops with a schema drift error, and the refusal says to remove that record. Once it's removed and the pipeline resumed, the next attempt writes the new type, and the table's files then hold the column with two types: make sure its readers can take that first. The pipeline's `destination.typeChanges: allow` lets the destination change the record itself.
