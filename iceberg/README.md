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
- **Overwrite** keeps a read's files out of the table until the read ends, then replaces the table's files with them. From 0.2.0, a stream paused in the middle of a read goes on with it once it's resumed: the read's end replaces the table's files with those written before the pause too.
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

Streams whose source replays exactly land exactly once. A stream whose source may send rows again, such as a Postgres table read incrementally, lands effectively once when it's merged: the repeat lands on its key's row. From 0.2.0, appended or overwritten, such a stream is deduped at commit and lands exactly once, unless the pipeline requires only `at_least_once`, or the stream has no primary key and doesn't collapse: the commit leaves out each row whose `_openetl_id` an earlier row of the epoch has, and each one the table already has. An overwrite's target is its own read, so it keeps one row per `_openetl_id` across the read. With `at_least_once`, a stream without a primary key that collapses is deduped within each epoch only.

Deduping costs reads, since Iceberg has no index to look IDs up in: the commit reads the `_openetl_id` column of the epoch's staged files, and against the table, scans the table's `_openetl_id` column, filtered by the epoch's IDs when there are at most 1,000 of them and in full when there are more. A staged file that loses rows is written again without them. The pipeline's status counts the rows it left out as collapsed.

## Set it up

### A REST catalog

Set `uri`, and `warehouse` if the catalog serves more than one. For authentication, give a bearer `token`, or OAuth2 client credentials as `credential`, in the form `CLIENT_ID:CLIENT_SECRET`.

From 0.2.0, these `properties` apply from the first request, for the catalog's config, so a catalog that needs them can be signed in to:

- `scope`: the scope a token is asked for, `catalog` unless set. Polaris takes `PRINCIPAL_ROLE:ALL`.
- `oauth2-server-uri`: the token endpoint of an identity provider of the catalog's own, such as Keycloak's, which `credential` goes to instead of the catalog's `v1/oauth/tokens`. `audience` and `resource` go with the token request.
- `rest.sigv4-enabled: "true"`: requests are signed with the AWS credentials, for the service `rest.signing-name` names, such as `glue` for Glue's Iceberg REST endpoint, in `rest.signing-region`, or else `region`.

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

### AWS credentials

Without `accessKeyId` and `secretAccessKey`, the destination uses the AWS SDK's default credential chain, which finds only what the runner gives it. By default, the runner gives it no AWS credentials. Name an IAM role in the destination's `aws.role`, which the runner assumes for it, on EC2 and EKS alike. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup, and the runner's `inherit` setting. With keys, keep them in secrets, as `{ secret: NAME }`. Keys in the config are used even if the destination names a role.

The credentials need these actions:

- **S3**, whenever the destination writes to S3 with them: `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on the tables' locations, and `s3:ListBucket` on their bucket, which an overwrite needs to clean up after a read.
- **Glue**: `glue:GetDatabase`, `glue:CreateDatabase` for a namespace that doesn't exist yet, `glue:GetTable`, `glue:CreateTable` and `glue:UpdateTable`. From 0.2.0, the checks also use `glue:DeleteTable`, `glue:GetTables` and `glue:DeleteDatabase` when they created the namespace, and `glue:GetDatabases`, though they pass without it.

The [config schema](./destination.schema.json) has every setting.

### Checks

From 0.2.0, `openetl check` runs the destination's checks, in order: it resolves and connects to the catalog's host, and, over HTTPS, completes a TLS handshake (`net.dns`, `net.connect`, `net.tls`; for Glue at AWS's own endpoint, and for a host that the `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` environment variables send through a proxy, these only advise, since the client may reach it through a proxy a dial doesn't take); signs in and lists namespaces (`catalog.auth`); finds the namespace, which the first write creates if it's missing (`catalog.namespace`); creates a table `_openetl_check_…` there, creating the namespace if needed (`catalog.create`); commits a property change to it (`catalog.commit`); with `s3Endpoint`, reaches the store (`store.dns`, `store.connect`, `store.tls`, which only advise for a host behind a proxy too); and writes and deletes a file in the table's location (`store.write`, `store.delete`). It then purges the table, and drops a namespace it created.

From 0.2.0, a refusal of the credentials or of access, by the catalog or the store, fails a run with the refusal's code, as it fails the checks, rather than as an internal error the platform retries.

From 0.2.0, a pipeline's preflight reaches the catalog, signs in and finds the namespace, without the checks that create a table or write files, and checks each stream: that its write mode is append, overwrite or merge and that a merge has a primary key of columns that aren't floating-point (`stream.NAME.mode`), and that its table, if it exists, takes the stream's columns without a type change the stream doesn't allow, and, for a merge, is format 2 or later, partitioned by nothing outside the key, and holds no files of an earlier partitioning (`stream.NAME.table`). For a stream whose schema OpenETL doesn't know yet, it checks only the write mode, that a merge has a primary key, and whether the table named after the stream exists.

## Types

| Arrow type | Iceberg type | Notes |
| --- | --- | --- |
| `boolean` | `boolean` | |
| `int8`, `int16`, `int32`, `uint8`, `uint16` | `int` | |
| `int64`, `uint32` | `long` | |
| `float16`, `float32` | `float` | |
| `float64` | `double` | |
| `decimal128(p, s)` | `decimal(p, s)` | |
| `string`, `large_string`, `string_view`, `null`, JSON | `string` | |
| `binary`, `large_binary`, `binary_view` | `binary` | |
| `fixed_size_binary(16)` with `arrow.uuid` | `uuid` | |
| other `fixed_size_binary(N)` | `fixed[N]` | |
| `date32`, `date64` | `date` | |
| `time32`, `time64` | `time` | Microseconds |
| `timestamp` | `timestamp`, or `timestamptz` with a time zone | Microseconds; `timestamptz` in UTC |
| `list`, `large_list`, `fixed_size_list`, `list_view`, `large_list_view` | `list` | |
| `struct` | `struct` | |
| `month_day_nano_interval` | `struct<months: int, days: int, nanoseconds: long>` | Iceberg has no interval type; these are the parts JSON destinations spell an interval with |

Every column, and every field of a struct, is optional, and other extension types take their storage type's. Values are cast safely: a time or timestamp with nanoseconds that microseconds can't hold, or a `date64` that isn't midnight, fails the write rather than being cut short. A table can't be made for a column of another type, such as `uint64`, `decimal256`, `duration`, a dictionary or a union.

## Schema changes

New columns are added to the table, and new fields to a struct column. A column narrower than the table's, in a way Iceberg promotes, such as an `int` into a `long`, is written as it is, without a change to the table.

The destination doesn't change an existing column's type by itself. When a stream's top-level column widens in a way Iceberg can promote (an `int` to a `long`, a `float` to a `double`, or a decimal to one of greater precision and the same scale), the destination refuses the change, the stream [holds](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/holds-and-quarantine.md) the column where it can, and the refusal gives the Spark SQL that makes the change:

```sql
ALTER TABLE `raw`.`orders` ALTER COLUMN `amount` TYPE DECIMAL(20, 2)
```

Run it in an engine that can change the table, then release the hold. Or let the destination make those promotions itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. Any other change, such as a `long` to a `string`, or a widening inside a struct or a list, is refused either way, with no statement to run.

Until the stream holds a refused column, the epoch's rows are written without its values. If the table changes while the epoch is written, so that it takes them, the attempt fails, and the epoch is read again.

Columns of the table's partition spec can't be held, and nor can a required column take the nulls a hold leaves; a table the destination created has neither.

## Partitioned tables

From 0.2.0.

The destination creates its tables unpartitioned, and doesn't change a table's partitioning. It writes into a table that's already partitioned, by any of Iceberg's transforms, with each data file in one partition. At commit, it reads each file's partition columns to find its partition; if the table's partitioning changed after the file was written, so that its rows span partitions, it writes the rows again, by partition.

A merge writes equality deletes in the partition of the key's new row, so it needs every column the table is partitioned by in the stream's primary key. It refuses a table partitioned by another column, and the preflight says so.

A delete written in a table's current partitioning doesn't reach files of an earlier one. So a merge, or a patch of an appended stream's held cells, fails on a partitioned table that still holds files written before its partitioning last changed, until they're rewritten into the current one, as Spark's `rewrite_data_files` procedure does.

## Maintenance

Each commit adds a snapshot, and a metadata file, to each table it writes, and the destination neither expires snapshots, nor compacts data files, nor deletes old metadata files. Run them in an engine that can, such as Spark's `expire_snapshots` and `rewrite_data_files` procedures, and set a table's `write.metadata.delete-after-commit.enabled` to `true` to keep only its `write.metadata.previous-versions-max` newest metadata files.

An epoch's data files are in its tables' locations from when they're written until the epoch commits, which can be long after for a pipeline that stops with an epoch staged, and a table's catalog knows nothing of them until then. A `remove_orphan_files` that runs with an `older_than` shorter than that deletes them, and the epoch can't commit: its recovery reports it lost. An overwrite read's files are there too, out of the table, until the read ends, with its manifests under the table's `_openetl/reads`; from 0.2.0, a paused stream's read goes on once the stream is resumed. Keep `older_than` longer than a pipeline may stay stopped or a stream paused, and longer than an overwrite read takes.

## Limits

- New tables are unpartitioned and unsorted: the destination has no setting for a new table's partitioning or sort order. Partition a table with an engine that can, such as Spark's `ALTER TABLE … ADD PARTITION FIELD`, and from 0.2.0, the destination writes into its partitions from then on.
- New tables are format 2, and timestamps are kept in microseconds: the destination neither creates format 3 tables nor writes `timestamp_ns`. From 0.2.0, it appends to and overwrites a format 3 table that exists, giving its rows their row IDs; merges and held cells' patches into a format 3 table are untested.
- From 0.2.0, deduping holds every `_openetl_id` of the epoch in memory, and of the whole read for an overwrite, and a staged file that loses rows is read whole to be written again.
- A merge, and a patch of held cells, holds in memory at commit the epoch's staged rows or patches of one stream at a time, with the table's rows they match. It finds those by up to 1,000 distinct values of each column it matches on, by the range between the lowest and highest beyond that, and by no value of a column with a null among them, so a match on one column with a null reads the whole table.
- The destination writes; it doesn't read tables back as a source.
- Glue commits one table at a time, so a pipeline into Glue needs stream scope, and an epoch's tables may land a few moments apart.
- From 0.2.0, an epoch that can't be recovered, as some of its staged files are gone, leaves the rest in its tables' locations as orphan files, which no snapshot holds.
- From 0.2.0, the destination can't tell a stream removed from the pipeline from a paused one, so an overwrite read that a removed stream left unfinished keeps its files in the table's location, out of the table, until the stream is added back or the table is dropped.
