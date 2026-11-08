---
title: PostgreSQL
description: Read Postgres tables in full, by a cursor column, or change by change through logical replication, and write into Postgres by appending, overwriting or merging.
---

The `postgres` package has a source and a destination. The source reads the tables of one schema: whole, from where a cursor column last got to, or as a stream of every insert, update and delete through logical replication (CDC). The destination writes each stream into a table of its own, and commits each epoch exactly once.

Both are tested against Postgres 18.

## Source

### What it reads

Each table in the configured `schema` is a stream, or only the ones `tables` lists. A partitioned table is one stream, read through its root; views aren't offered.

A stream can be read three ways, which the pipeline picks with each stream's `sync`:

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table | The whole table each time. A table with a primary key is read in key order, `chunkRows` rows a query, and a stopped read resumes after its last chunk. A table without one is read in a single query. |
| `incremental` | Tables with a column of type `smallint`, `integer`, `bigint`, `numeric`, `date`, `timestamp`, `timestamptz`, `text` or `varchar` | The rows whose `cursor` column is at or above where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the key. |
| `cdc` | Tables with a primary key, whose replica identity is the default or `FULL` | First a snapshot of the table, in chunks of `snapshotChunkRows`, then every change from the write-ahead log, through a replication slot with the `pgoutput` plugin. |

An incremental read starts at the last read's highest cursor value, not after it, so the rows that had that value are read again. Rows whose cursor is `NULL` are never read, and deleted rows aren't seen: CDC is the mode that sees deletes.

A read holds up to `maxConnections` connections, one for each lane it runs, and CDC holds one replication connection besides.

### Set it up

The user in `dsn` needs `USAGE` on the schema and `SELECT` on every table and view in it: the source's check fails otherwise, naming what it can't read.

For CDC, the server and the user need more:

- `wal_level = logical` in the server's configuration, which takes a restart. Without it, creating the slot fails with an error that says so.
- A role with the `REPLICATION` attribute, to create the slot and stream from it.
- A publication of the tables read with CDC, named by `publication` (`openetl` unless set). The source creates it, or adds the tables it lacks, if the user may. If not, create it yourself:

  ```sql
  CREATE PUBLICATION openetl FOR TABLE public.orders, public.customers WITH (publish_via_partition_root = true);
  ```

Each pipeline reads through a replication slot of its own: `slot`, or else `openetl_` and the pipeline's name in lowercase letters and digits, such as `openetl_appdbtowarehouse`. The source creates it on the first read; on Postgres 17 and later it creates it as a failover slot, so a standby that synchronizes slots keeps it. Deleting the pipeline drops the slot.

> [!WARNING]
> A replication slot keeps the server's write-ahead log until the pipeline has read it. A pipeline that's paused or failing for long makes the log grow on the source's disk. Set `max_slot_wal_keep_size` to cap it: OpenETL then warns as a slot nears the cap, and a slot the server invalidates has to be read again from a new snapshot.

### Example

The [config schema](./source.schema.json) has every setting. This source offers the tables of `public`, and its pipeline reads `orders` by CDC and `customers` by its `updated_at` column:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-db
spec:
  connector: postgres
  config:
    dsn: { secret: app-db-dsn }
    schema: public
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: app-db-to-warehouse
spec:
  schedule: { every: 1h }
  source:
    ref: app-db
    streams:
      - name: orders
        sync: cdc
      - name: customers
        sync: incremental
        cursor: updated_at
  destination: { ref: warehouse }
```

`dsn` is a connection URL, such as `postgres://USER:PASSWORD@HOST:5432/DATABASE?sslmode=require`, or a keyword/value string. It's a secret: store it with `openetl secret set app-db-dsn`.

### Types

| Postgres | Arrives as |
|---|---|
| `boolean` | boolean |
| `smallint`, `integer`, `bigint` | 16-, 32- and 64-bit integers |
| `oid` | 64-bit integer |
| `real`, `double precision` | 32- and 64-bit floats |
| `numeric(p,s)` | a decimal of the same precision and scale, up to a precision of 38; otherwise, and without a precision, `decimal(38, 9)` |
| `text`, `varchar`, `char`, `name` | string |
| `bytea` | binary |
| `date`, `time` | date, time of day in microseconds |
| `timestamp`, `timestamptz` | timestamp in microseconds, without and with a time zone (UTC) |
| `uuid` | UUID |
| `json`, `jsonb` | JSON |
| Anything else, such as `interval`, arrays, enums and network types | string, in Postgres's text form (intervals in ISO 8601) |

`openetl discover` lists a column as nullable unless it's `NOT NULL`. In a table CDC can read, only the key's columns are listed as not nullable, since a deleted row carries only its key.

### Delivery

Full refresh and CDC streams replay exactly: a read resumed from a checkpoint yields the same rows in the same order. OpenETL counts incremental streams as ones that may repeat rows, because of the rows at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

## Destination

### What it writes

Each stream becomes a table, named after the stream in lowercase, in the pipeline's `destination.namespace`, or else in the config's `schema` (`public` unless set). The schema is created if it's missing.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true.

Each epoch is staged into tables of its own, then applied in one transaction behind a fence row in `_openetl_fence`, with a marker per stream in `_openetl_loads`. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` and `_openetl_loads` in place.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Set it up

The user in `dsn` needs `CREATE` on the schema, or, if the schema doesn't exist yet, `CREATE` on the database.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: postgres
  config:
    dsn: { secret: warehouse-dsn }
    schema: raw
```

### Types

| Arrives as | Postgres column |
|---|---|
| boolean | `boolean` |
| 8- and 16-bit integers, unsigned 8-bit | `smallint` |
| 32-bit integers, unsigned 16-bit | `integer` |
| 64-bit integers, unsigned 32-bit | `bigint` |
| 32- and 64-bit floats | `real`, `double precision` |
| decimal(p, s) | `numeric(p,s)` |
| string | `text` |
| binary | `bytea` |
| date, time of day | `date`, `time without time zone` |
| timestamp | `timestamp without time zone`, or `timestamp with time zone` if it has one |
| interval | `interval` |
| UUID | `uuid` |
| JSON, lists, structs and maps | `jsonb` |

### Schema changes

New columns are added to the table, until it has the 1,600 columns Postgres allows, dropped ones included; past that, the destination refuses the column.

The destination doesn't change an existing column's type by itself, since `ALTER COLUMN … TYPE` can rewrite and lock the table. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change:

```sql
ALTER TABLE "raw"."orders" ALTER COLUMN "amount" TYPE numeric(20,2)
```

Run it, then release the hold. Or let the destination make such changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. It then widens `smallint` to `integer` to `bigint`, any of them or `real` to `double precision`, integers and decimals to a `numeric` that holds them, and anything to `text`. A change that isn't a widening is refused either way.

A schema change waits up to 5 seconds for its table's lock, and tries again up to 5 times, so queries on the table don't queue behind it.
