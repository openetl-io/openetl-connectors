---
title: Apache Iceberg
description: Write streams into Iceberg tables through a REST catalog or AWS Glue, by appending, overwriting or merging, with each epoch committed once.
---

The `iceberg` package has a destination. It writes each stream into an Iceberg table, as Parquet data files, and commits each epoch through the table's catalog: an Iceberg REST catalog, or AWS Glue. The catalog's own compare-and-swap keeps an epoch from being committed twice.

It's tested against Apache Iceberg's REST catalog fixture 1.10, with its tables in an S3-compatible store, and against moto's emulation of Glue. It has also run against AWS Glue with Amazon S3, which CI does not test.

## What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, or else the config's `namespace` (`default` unless set). If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. The namespace is created if it's missing. New tables are Iceberg format version 2, unpartitioned, with every column optional, and write Parquet compressed with zstd at level 1.

A table that already exists keeps its own settings: the destination writes with the table's properties, such as `write.parquet.compression-codec`, and doesn't change them. A running job picks up a changed property at its next attempt, or when the stream's schema next changes.

All three write modes work:

- **Append** adds each epoch's data files to the table.
- **Overwrite** keeps a read's files out of the table until the read ends, then replaces the table's files with them.
- **Merge** writes equality deletes for the keys an epoch changes, with the keys' new rows, so the table holds each key's latest version. Query engines that read Iceberg v2 apply the deletes.

Each epoch is one snapshot per table, with the epoch's marker in the snapshot's summary and in a table property. The commit checks the fence and the marker on the snapshot it builds on, and the catalog refuses it if another commit landed first, so a replaced attempt can't commit and no epoch lands twice.

On a REST catalog, an epoch's tables commit together, in one multi-table commit. Glue commits one table at a time, so a pipeline into Glue has to say so:

```yaml
spec:
  delivery:
    commit: { scope: stream }
```

A continuous pipeline commits every minute unless it sets its own `delivery.commit.interval`. Each commit is a snapshot, so shorter intervals make more, smaller files.

## Delivery

Streams whose source replays exactly land exactly once. A stream whose source may send rows again, such as a Postgres table read incrementally, lands effectively once when it's merged: the repeat lands on its key's row. Appended or overwritten, such a stream gets at least once, because the destination can't drop the rows sent again.

## Set it up

### A REST catalog

Set `uri`, and `warehouse` if the catalog serves more than one. For authentication, give a bearer `token`, or OAuth2 client credentials as `credential`, in the form `CLIENT_ID:CLIENT_SECRET`.

The destination writes data files to the tables' locations itself. If the catalog doesn't hand out storage credentials, give them: for S3, `region`, and `accessKeyId` and `secretAccessKey` or the AWS default credentials; for another S3-compatible store, `s3Endpoint` too, which uses path-style addressing. Any other catalog or file IO property, by the name Iceberg gives it, goes in `properties`.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: lake
spec:
  connector: iceberg
  config:
    catalog: rest
    uri: https://catalog.internal
    warehouse: analytics
    credential: { secret: lake-catalog-credential }
    namespace: raw
```

### AWS Glue

Set `catalog: glue`, `region`, and `warehouse`: the S3 location new tables go under, such as `s3://bucket/prefix`, each in `NAMESPACE/TABLE` below it. Without `accessKeyId` and `secretAccessKey`, the destination uses the AWS default credentials, which need access to Glue's databases and tables and to the S3 location.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: glue-lake
spec:
  connector: iceberg
  config:
    catalog: glue
    region: eu-west-1
    warehouse: s3://my-lake/iceberg
    namespace: raw
```

The [config schema](./destination.schema.json) has every setting.

## Types

Columns take the Iceberg type of their Arrow type, with two exceptions:

- Timestamps are kept in microseconds.
- Intervals, which Iceberg has no type for, become a struct of `months` (32-bit integer), `days` (32-bit integer) and `nanoseconds` (64-bit integer), the parts JSON destinations spell them with.

## Schema changes

New columns are added to the table, and new fields to a struct column.

The destination doesn't change an existing column's type by itself. When a stream's column widens in a way Iceberg can promote (an `int` to a `long`, a `float` to a `double`, or a decimal to one of greater precision and the same scale), the destination refuses the change, the stream holds the column where it can, and the refusal gives the Spark SQL that makes the change:

```sql
ALTER TABLE `raw`.`orders` ALTER COLUMN `amount` TYPE DECIMAL(20, 2)
```

Run it in an engine that can change the table, then release the hold. Or let the destination make those promotions itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. A change Iceberg can't make, such as a `long` to a `string`, is refused either way, with no statement to run.

Columns of the table's partition spec can't be held, and nor can a required column take the nulls a hold leaves; a table the destination created has neither.
