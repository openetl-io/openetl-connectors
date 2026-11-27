---
title: Apache Iceberg
description: Write streams into Iceberg tables through a REST catalog or AWS Glue, by appending, overwriting or merging, with each epoch committed once.
---

The `iceberg` package has a destination. It writes each stream into an Iceberg table, as Parquet data files, and commits each epoch through the table's catalog: an Iceberg REST catalog, or AWS Glue. The catalog's own compare-and-swap keeps an epoch from being committed twice.

## What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, or else the config's `namespace` (`default` unless set). If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. The namespace is created if it's missing. New tables are Iceberg format version 2, unpartitioned, with every column optional, and write Parquet compressed with zstd at level 1.

A table that already exists keeps its own settings: the destination writes with the table's properties, such as `write.parquet.compression-codec`, and doesn't change them. A running job picks up a changed property at its next attempt, or when the stream's schema next changes.

All three write modes work:

- **Append** adds each epoch's data files to the table.
- **Overwrite** keeps a read's files out of the table until the read ends, then replaces the table's files with them. A stream paused in the middle of a read goes on with it once it's resumed: the read's end replaces the table's files with those written before the pause too.
- **Merge** writes equality deletes for the keys an epoch changes, with the keys' new rows, so the table holds each key's latest version. Query engines that read Iceberg v2 apply the deletes.

Each epoch is one snapshot per table, with the epoch's marker in the snapshot's summary and in a table property. The commit checks the marker on the snapshot it builds on, and the catalog refuses it if another commit landed first, so no epoch lands twice. A marker property, `openetl.loaded.PIPELINE`, that isn't one the destination wrote fails the commit, and the run without a retry, rather than having the epoch applied again: set it to `EPOCH:GENERATION`, the `openetl.epoch` and `openetl.generation` of the last snapshot the pipeline committed.

The commit checks the fence too, a table property that each attempt's recovery raises. An Iceberg commit can't be made to depend on a property, though, and raising it changes no snapshot, so a replaced attempt that checked the fence before it was raised can still commit, and an older recovery can lower it again. What keeps a replaced attempt from committing an epoch the newer one doesn't know of is the [checkpoint log](https://github.com/openetl-io/openetl/blob/main/docs/internals/commits.md#checkpoints), in which it can't record one; an epoch they both apply lands once, by its marker.

On a REST catalog, an epoch's tables commit together, in one multi-table commit. Glue commits one table at a time, so a pipeline into Glue has to say so:

```yaml
spec:
  delivery:
    commit: { scope: stream }
```

A continuous pipeline commits every minute unless it sets its own `delivery.commit.interval`. Each commit is a snapshot, so shorter intervals make more, smaller files.

## Delivery

Streams whose source replays exactly land exactly once. A stream whose source may send rows again, such as a Postgres table read incrementally, lands effectively once when it's merged: the repeat lands on its key's row. Appended or overwritten, such a stream is deduped at commit and lands exactly once, unless the pipeline requires only `at_least_once`, or the stream has no primary key and doesn't collapse: the commit leaves out each row whose `_openetl_id` an earlier row of the epoch has, and each one the table already has. An overwrite's target is its own read, so it keeps one row per `_openetl_id` across the read. With `at_least_once`, a stream without a primary key that collapses is deduped within each epoch only.

Deduping costs reads, since Iceberg has no index to look IDs up in: the commit reads the `_openetl_id` column of the epoch's staged files, and against the table, scans the table's `_openetl_id` column, filtered by the epoch's IDs when there are at most 1,000 of them and in full when there are more. A staged file that loses rows is written again without them. The pipeline's status counts the rows it left out as collapsed.

## Set it up

### A REST catalog

Set `uri`, and `warehouse` if the catalog serves more than one. For authentication, give a bearer `token`, or OAuth2 client credentials as `credential`, in the form `CLIENT_ID:CLIENT_SECRET`.

These `properties` apply from the first request, for the catalog's config, so a catalog that needs them can be signed in to:

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
- **Glue**: `glue:GetDatabase`, `glue:CreateDatabase` for a namespace that doesn't exist yet, `glue:GetTable`, `glue:CreateTable` and `glue:UpdateTable`. The checks also use `glue:DeleteTable`, to drop the table they create, and `glue:GetTables` and `glue:DeleteDatabase`, to drop a namespace they created; Glue lets an identity delete a database only with access to its tables and user-defined functions too, `table/DATABASE/*` and `userDefinedFunction/DATABASE/*`. They also use `glue:GetDatabases`, though they pass without it.

The [config schema](./destination.schema.json) has every setting.

### Checks

`openetl check` runs the destination's checks, in order: it resolves and connects to the catalog's host, and, over HTTPS, completes a TLS handshake (`net.dns`, `net.connect`, `net.tls`; for Glue at AWS's own endpoint, and for a host that the `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` environment variables send through a proxy, these only advise, since the client may reach it through a proxy a dial doesn't take); signs in and lists namespaces (`catalog.auth`); finds the namespace, which the first write creates if it's missing (`catalog.namespace`); creates a table `_openetl_check_…` there, creating the namespace if needed (`catalog.create`); commits a property change to it (`catalog.commit`); with `s3Endpoint`, reaches the store (`store.dns`, `store.connect`, `store.tls`, which only advise for a host behind a proxy too); writes and deletes a file in the table's location (`store.write`, `store.delete`); and purges the table, and drops the namespace if it created it (`catalog.drop`). If the catalog refuses a drop, `catalog.drop` warns and names what's left, which stays in the catalog.

A refusal of the credentials or of access, by the catalog or the store, fails a run with the refusal's code, as it fails the checks, rather than as an internal error the platform retries.

A pipeline's preflight reaches the catalog, signs in and finds the namespace, without the checks that create a table or write files, and checks each stream: that its write mode is append, overwrite or merge and that a merge has a primary key of columns that aren't floating-point (`stream.NAME.mode`), and that its table, if it exists, takes the stream's columns without a type change the stream doesn't allow, and, for a merge, is format 2 or later, partitioned by nothing outside the key, and holds no files of an earlier partitioning (`stream.NAME.table`). For a stream whose schema OpenETL doesn't know yet, it checks only the write mode, that a merge has a primary key, and whether the table named after the stream exists.

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

The destination creates its tables unpartitioned, and doesn't change a table's partitioning. It writes into a table that's already partitioned, by any of Iceberg's transforms, with each data file in one partition. At commit, it reads each file's partition columns to find its partition; if the table's partitioning changed after the file was written, so that its rows span partitions, it writes the rows again, by partition.

A merge writes equality deletes in the partition of the key's new row, so it needs every column the table is partitioned by in the stream's primary key. It refuses a table partitioned by another column, and the preflight says so.

A delete written in a table's current partitioning doesn't reach files of an earlier one. So a merge, or a patch of an appended stream's held cells, fails on a partitioned table that still holds files written before its partitioning last changed, until they're rewritten into the current one, as Spark's `rewrite_data_files` procedure does.

## Maintenance

Each commit adds a snapshot, and a metadata file, to each table it writes, and the destination neither expires snapshots, nor compacts data files, nor deletes old metadata files. Run them in an engine that can, such as Spark's `expire_snapshots` and `rewrite_data_files` procedures, and set a table's `write.metadata.delete-after-commit.enabled` to `true` to keep only its `write.metadata.previous-versions-max` newest metadata files.

An epoch's data files are in its tables' locations from when they're written until the epoch commits, which can be long after for a pipeline that stops with an epoch staged, and a table's catalog knows nothing of them until then. A `remove_orphan_files` that runs with an `older_than` shorter than that deletes them, and the epoch can't commit: its recovery reports it lost. An overwrite read's files are there too, out of the table, until the read ends, with its manifests under the table's `_openetl/reads`; a paused stream's read goes on once the stream is resumed. Keep `older_than` longer than a pipeline may stay stopped or a stream paused, and longer than an overwrite read takes.

## Throughput

OpenETL 0.2.0-alpha, with the `postgres` and `iceberg` packages at 0.2.0-alpha, ran three full refreshes of one Postgres table into an Iceberg table in AWS Glue and Amazon S3. Each run succeeded at its first attempt.

| Run | Sync time | Rows/s | MB/s | GB/hour | CPU-seconds per GB | Job peak memory |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 31.539 s | 1,204,733 | 340.3 | 1,225.2 | 11.76 | 5,857 MiB |
| 2 | 32.229 s | 1,178,945 | 333.0 | 1,199.0 | 11.78 | 4,902 MiB |
| 3 | 30.092 s | 1,262,685 | 356.7 | 1,284.1 | 11.62 | 5,845 MiB |
| Mean | 31.29 s | 1,215,454 | 343.3 | 1,236.1 | 11.72 | 5,857 MiB, the highest |

The sync time is the sync's own record, its end time less its start time. It includes placing the job, starting the connectors and the engine, planning, reading, writing and every commit. The spread of the three times, the slowest less the fastest over the mean, is 6.8%. The rates are of the source table's heap without its indexes: 10,733,748,224 bytes and 37,996,289 rows. MB is 1,000,000 bytes and GB is 1,000,000,000 bytes. The CPU time and the peak memory are the job's, for the engine and both connectors, from the sync's record.

| Part | Set-up |
|---|---|
| Data | One table of 16 columns and 37,996,289 rows, 267 bytes a row on average, stored in key order, with no nulls: `id bigint` primary key, `f1` and `f2 bigint`, `f3 integer`, `f4` and `f5 double precision`, `f6 numeric(18,4)`, `f7 boolean`, `f10` and `f11 date`, `f12 timestamptz`, and five `text` columns, `f8`, `f9` and `f13` to `f15`. Each text value is 32 characters of base64 cut from SHA-512 digests of the key, so it compresses poorly. |
| Source | Postgres 18.7 (`postgres:18`) with `shared_buffers=12GB`. `pg_prewarm` read the table and its key's index into the buffers before each run. The source read on up to 8 connections, without TLS. |
| Runner | EC2 `c8g.2xlarge` in `ap-southeast-2a`: 8 vCPUs (Graviton4), 16 GiB, network up to 15 Gbit/s, and a 60 GB gp3 disk at 6,000 IOPS and 400 MB/s. It ran OpenETL's Compose stack from the release's `compose.yml`, with `OPENETL_RESERVE=0,1Gi` so that a job of 8 vCPUs fits. The job had 8 vCPUs, 13 GiB and 8 lanes. |
| Database machine | EC2 `c8g.4xlarge` in the same zone: 16 vCPUs, 32 GiB, and a 300 GB gp3 disk at 16,000 IOPS and 1,000 MB/s. Postgres ran there alone, in Docker with host networking, on the private network. |
| Catalog and table | AWS Glue, with the warehouse in an S3 bucket, both in `ap-southeast-2`. The connectors used the runner's instance role (`OPENETL_CONNECTOR_AWS_CREDENTIALS=inherit`). Each run wrote into a Glue database of its own, to a table that the destination created with its defaults: format 2, unpartitioned, Parquet with zstd at level 1. |
| Packages | `postgres` and `iceberg`, at the version above, from `openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/NAME:VERSION`, on linux/arm64. |

Each run used these specs, with a pipeline and a namespace of its own. The source's DSN had `sslmode=disable`. The yearly schedule starts no sync during a run, so `openetl sync` started each one.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata: { name: pg-full }
spec:
  connector: postgres
  config: { dsn: { secret: pg-src-dsn }, schema: data, tables: [bench], maxConnections: 8 }
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata: { name: ice }
spec:
  connector: iceberg
  config: { catalog: glue, region: ap-southeast-2, warehouse: s3://BUCKET/iceberg }
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata: { name: pg-to-iceberg }
spec:
  schedule: { cron: "0 0 1 1 *" }
  heal: { mode: "off" }
  resources: { size: { cpu: 8, memory: 13Gi } }
  delivery: { commit: { scope: stream } }
  source:
    ref: pg-full
    streams:
      - { name: bench, sync: full_refresh }
  destination:
    ref: ice
    namespace: NAMESPACE
```

Before each timed sync, a sync of a one-row copy of the table, under the same stream name, created the Glue table. The timed sync then replaced the table's rows, as `overwrite` is the default write mode for a full refresh. Delivery was exactly once, with a commit every 256 MB that the source read (`delivery.commit.maxBytes`), the defaults. The time does not include making the data, the one-row sync, or reading the table into Postgres's cache.

The runner's CPU limited the runs. The read, until 99% of the source's bytes had reached the runner, took 19.4 to 20.1 s. In it, the runner was 88 to 90% busy: `destination-iceberg` used 3.2 to 3.4 cores and `source-postgres` 1.6 to 1.7. The runner's network received about 625 MB/s and sent about 360 MB/s. In each run, its ENA driver counted 0.9 to 1.3 million packets over the outbound bandwidth allowance (`bw_out_allowance_exceeded`) and 55,000 to 126,000 over the inbound one. These counters do not show how much time that cost. Postgres used about 1.9 cores and had room. The commits to Glue then took 10 to 12 s.

After each run, Amazon Athena queried the Glue table. Its row count, its count of distinct `id`s, and its sums of `f1`, `f6` and the lengths of `f8` were equal to the source's in all three runs.

These numbers are for one case:

- One row shape: 16 columns, 267 bytes a row, with five random strings that compress poorly. Wider or narrower rows, other types and text that compresses change every number.
- One machine size: a runner of 8 vCPUs and 16 GiB, with 8 lanes, whose CPU was the limit. A larger runner, or several workers (`resources.workers`), changes the numbers.
- A cached source: Postgres read the table from its shared buffers. A source that reads from disk is slower.
- A full refresh of one stream. No CDC, no incremental reads, no merges and no transforms.
- Postgres in Docker on a neighboring EC2 machine, not a managed service, whose storage, network and CPU differ.
- Three runs, one after another: the spread is that of three runs, not a distribution.

## Limits

- New tables are unpartitioned and unsorted: the destination has no setting for a new table's partitioning or sort order. Partition a table with an engine that can, such as Spark's `ALTER TABLE … ADD PARTITION FIELD`, and the destination writes into its partitions from then on.
- New tables are format 2, and timestamps are kept in microseconds: the destination neither creates format 3 tables nor writes `timestamp_ns`. It appends to and overwrites a format 3 table that exists, giving its rows their row IDs.
- Deduping holds every `_openetl_id` of the epoch in memory, and of the whole read for an overwrite, and a staged file that loses rows is read whole to be written again.
- A merge, and a patch of held cells, holds in memory at commit the epoch's staged rows or patches of one stream at a time, with the table's rows they match. It finds those by up to 1,000 distinct values of each column it matches on, by the range between the lowest and highest beyond that, and by no value of a column with a null among them, so a match on one column with a null reads the whole table.
- The destination writes; it doesn't read tables back as a source.
- Glue commits one table at a time, so a pipeline into Glue needs stream scope, and an epoch's tables may land a few moments apart.
- An epoch that can't be recovered, as some of its staged files are gone, leaves the rest in its tables' locations as orphan files, which no snapshot holds.
- The destination can't tell a stream removed from the pipeline from a paused one, so an overwrite read that a removed stream left unfinished keeps its files in the table's location, out of the table, until the stream is added back or the table is dropped.
