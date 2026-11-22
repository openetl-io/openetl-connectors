---
title: PostgreSQL
description: Read Postgres tables in full, by a cursor column, or change by change through logical replication, and write into Postgres by appending, overwriting or merging.
---

The `postgres` package has a source and a destination. The source reads the tables, and from 0.2.0 views and materialized views, of one schema: whole, from where a cursor column last got to, or as a stream of every insert, update and delete through logical replication (CDC). The destination writes each stream into a table of its own, and commits each epoch exactly once.

Both are tested against Postgres 18.

## Source

### What it reads

Each table in the configured `schema` is a stream, or only the ones `tables` lists. A partitioned table is one stream, read through its root. From 0.2.0, views and materialized views are streams too, read with `full_refresh` or `incremental`.

A stream can be read three ways, which the pipeline picks with each stream's `sync`:

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table, and from 0.2.0 every view | The whole table each time. A table with a read order (below) is read in that order, `chunkRows` rows a query, and a stopped read resumes after its last chunk. One without is read in a single query. |
| `incremental` | Tables, and from 0.2.0 views, with a column of type `smallint`, `integer`, `bigint`, `numeric`, `date`, `timestamp`, `timestamptz`, `text` or `varchar` | The rows whose `cursor` column is at or above where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the read order. |
| `cdc` | Tables with a primary key, whose replica identity is the default or `FULL` | First a snapshot of the table, in chunks of `snapshotChunkRows`, then every change from the write-ahead log, through a replication slot with the `pgoutput` plugin. |

A table's read order is its primary key. From 0.2.0, a table without one is read in the order of a unique index instead, if it has one whose columns are all `NOT NULL`, each in its column's collation, with no `WHERE` and no expressions, and that has finished building; of several, the one with the fewest columns, then the first by name. A table that other tables inherit from has no such index, since a read of it reads their rows too, which its indexes don't cover; a partitioned table's unique index covers its partitions. Nor does such a table's primary key cover them, though it's still the read order and the stream's key: their rows can repeat a key, and then a chunked read misses a row whose key the chunk before it ended on, and a merge keeps one row of each key. A CDC read of such a table reads their rows in its snapshot, but none of their changes after it. No two rows share such a key, so a chunked read is exact as it is with a primary key. The index doesn't make the stream keyed: the catalog gives it no primary key, and it can't be read with `cdc`. A read resumed in another order than it started in, after the index is dropped or another takes its place, fails with `POSITION_LOST`: only a read from the start goes on from there. So does a split of the table that hasn't started yet, since its range is of the old order's first column. A table or view with neither is read in one query, and a stopped read reads it again from the start; that includes every view and materialized view, whose columns are never `NOT NULL`. Reading by physical position (`ctid`) instead isn't sound: an update can move a row to a page the read has passed, and it's then missed.

The catalog names no default cursor: nothing in Postgres's catalog shows that a column orders new and changed rows, so a pipeline names its `cursor`. An incremental read starts at the last read's highest cursor value, not after it, so the rows that had that value are read again. Rows whose cursor is `NULL` are never read, nor, from 0.2.0, those whose cursor is `NaN` or an infinity, in a `numeric`, `date`, `timestamp` or `timestamptz`. Before 0.2.0, such a value became the next read's floor, above every value written after it, so later reads read nothing new; a read from such a floor now starts from the beginning. Deleted rows aren't seen: CDC is the mode that sees deletes. A value from a sequence or the clock is taken before its transaction commits, so a row can commit with a value below one a read has already passed, and be missed.

A table with a read order, read in full or incrementally, is split into ranges of its order's first column from the server's histogram statistics (`pg_stats`), which OpenETL reads in parallel on up to `maxConnections` connections; a CDC table's initial snapshot is split in the same way when more than one connection may run. Small tables, and tables without statistics, are read whole. A range being read can be split again while it's read.

From 0.2.0, a column added to a table since the run discovered it fails the read with `TRANSIENT` before it reads, in any mode: the next attempt discovers the column and declares its type. Before 0.2.0, a full or incremental read sent such a column without a declared type, so a `numeric` landed as a string, and a CDC read left it out of the table's snapshot, so the rows the snapshot read arrived without its values. A column added while a CDC read runs fails the read the same way, at the first change that has it.

`TRUNCATE` isn't carried as a CDC read's deletes: the source logs a warning, and the destination keeps the rows; a new snapshot of the table brings it back in step.

A read holds up to `maxConnections` connections, one for each lane it runs, and CDC holds one replication connection besides.

### Set it up

The user in `dsn` needs `USAGE` on the schema and `SELECT` on every table, and from 0.2.0 view, the source offers: the source's check fails otherwise, naming what it can't read. From 0.2.0, the check looks only at those `tables` lists, where it's set; before, it asked for `SELECT` on every table and view in the schema.

For CDC, the server must be Postgres 13 or later, and the server and the user need more:

- A `dsn` that reaches the primary: the read takes the log's position with `pg_current_wal_lsn()`, which a standby can't give.
- `wal_level = logical` in the server's configuration, which takes a restart. Without it, creating the slot fails with an error that says so.
- A role with the `REPLICATION` attribute, to create the slot and stream from it, or on Amazon RDS, the `rds_replication` role.
- A publication of the tables read with CDC, named by `publication` (`openetl` unless set). The source creates it, or adds the tables it lacks, if the user may. If not, create it yourself:

  ```sql
  CREATE PUBLICATION openetl FOR TABLE public.orders, public.customers WITH (publish_via_partition_root = true);
  ```

Each pipeline reads through a replication slot of its own: `slot`, or else `openetl_` and the pipeline's name in lowercase letters and digits, such as `openetl_appdbtowarehouse`. The source creates it on the first read; on Postgres 17 and later it creates it as a failover slot, so a standby that synchronizes slots keeps it. Deleting the pipeline drops the slot.

> [!WARNING]
> A replication slot keeps the server's write-ahead log until the pipeline has read it. A pipeline that's paused or failing for long makes the log grow on the source's disk. Set `max_slot_wal_keep_size` to cap it: OpenETL then warns as a slot nears the cap, and a slot the server invalidates has to be read again from a new snapshot.

### Check and preflight

From 0.2.0, the source's check and a pipeline's preflight report an item for each thing they look at, with a fix for what fails. The check is about the connection:

| Item | What it checks |
|---|---|
| `net.dns`, `net.connect` | The `dsn`'s host resolves, and its port takes a connection. Not checked through a Unix socket. Of a `dsn` with several hosts, these and `db.tls` look at the first and only warn, since the connection goes to the first host that takes it: `db.connect` tries them in turn. |
| `db.tls` | The server takes TLS, with a certificate the config trusts. Only when the `sslmode` uses TLS. It warns when the server doesn't take TLS and `sslmode=prefer` (the default) lets the connection go on in plaintext, and when the certificate expires within 14 days. |
| `db.connect` | The user signs in. |
| `schema.usage` | The schema exists, and the user has `USAGE` on it. |
| `tables.found` | Every table `tables` names is a table or view in the schema. Only when `tables` is set. |
| `tables.read` | The user has `SELECT` on each table and view the source offers. |
| `cdc.wal-level`, `cdc.replication` | `wal_level` is `logical`, and the user may stream changes. These only warn: CDC is a pipeline's choice. |

A preflight is about the streams a pipeline reads:

| Item | What it checks |
|---|---|
| `stream.NAME.read` | The stream is a table or view in the schema, and the user has `SELECT` on it. |
| `stream.NAME.cursor` | An incremental stream's cursor is a column of a type the source reads by. |
| `stream.NAME.cdc` | A table read with CDC has a primary key, and its replica identity is `DEFAULT` or `FULL`. |
| `cdc.wal-level`, `cdc.replication` | As in the check, and they fail when a stream is read with CDC. |
| `cdc.wal-keep` | Warns when `max_slot_wal_keep_size` is `-1`, so a slot can keep the log without a limit. |
| `cdc.publication` | The publication has the tables, or the user can create it or add them. |
| `cdc.slot` | The pipeline's slot is a `pgoutput` slot of the database, or the server has a slot free to create it. It warns when the slot was invalidated, since a read with a position in it then fails with `POSITION_LOST` until its tables are read again from a new snapshot. |

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

### TLS

The `dsn`'s `sslmode` decides whether connections use TLS and how they verify the server, and `sslrootcert`, `sslcert` and `sslkey` name files on the machine that runs the connector. From 0.2.0, both roles also take them in place, as PEM, which suits a secret store better than a file:

- `tlsCa`: the authorities that sign the server's certificate. The certificate must chain to them under `require` and `prefer` as under `verify-ca`, and also name the host under `verify-full`.
- `tlsCert` and `tlsKey`: a client certificate and its unencrypted key, for a server that asks for one. `tlsKey` is a secret.

Any of them makes TLS required: `sslmode=disable` and `allow` are refused, and `prefer` doesn't fall back to a plaintext connection. They hold for every connection, the replication connection too.

```yaml
config:
  dsn: { secret: app-db-dsn }  # postgres://...?sslmode=verify-full
  tlsCa: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  tlsCert: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  tlsKey: { secret: app-db-key }
```

### Types

| Postgres | Arrives as |
|---|---|
| `boolean` | boolean |
| `smallint`, `integer`, `bigint` | 16-, 32- and 64-bit integers |
| `oid` | 64-bit integer |
| `real`, `double precision` | 32- and 64-bit floats |
| `numeric(p,s)` | a decimal of the same precision and scale, up to a precision of 38 with a scale from 0 to the precision; otherwise, and without a precision, `decimal(38, 9)`, since OpenETL's decimals hold up to 38 digits. A value it can't hold exactly is quarantined, as are `NaN` and the infinities. |
| `text`, `varchar`, `name` | string |
| `char(n)` | string, without the trailing spaces Postgres pads it with, as casting it to `text` gives it |
| `bytea` | binary |
| `date`, `time` | date, time of day in microseconds |
| `timestamp`, `timestamptz` | timestamp in microseconds, without and with a time zone (UTC) |
| `uuid` | UUID |
| `json`, `jsonb` | JSON |
| `interval` | From 0.2.0, interval of months, days and nanoseconds, the parts Postgres keeps |
| Arrays of `boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `text`, `varchar` and `char(n)`, in a table column declared with one dimension (`int[]`) | From 0.2.0, a list of the item's type, with nulls where the array has them |
| Enums | string: the label |
| Anything else, such as domains, even of a type above, arrays of other types, arrays declared with more than one dimension (`int[][]`) or none, as a view's are, network types, ranges and geometric types | string, in Postgres's text form |

`openetl discover` lists a column as nullable unless it's `NOT NULL`. In a table CDC can read, only the key's columns are listed as not nullable, since a deleted row carries only its key.

An array's bounds aren't kept: `'[0:1]={5,6}'` arrives as `[5, 6]`. Some values have no place in the type their column arrives as, and OpenETL can't quarantine a row of a list or interval column, so the read fails with `DATA_INVALID`, naming the column and the value:

- An array of more than one dimension in a column declared with one, such as `'{{1,2},{3,4}}'` in an `int[]`.
- An interval whose hours, minutes and seconds are past about 292 years, the most nanoseconds a 64-bit integer holds, such as `'3000000 hours'`, and the infinite intervals of Postgres 17 and later. Months and days have their own parts, so `'1000 years'` arrives.

To read such a column, read it through a view that casts it to `text`.

> [!NOTE]
> Before 0.2.0, intervals and arrays arrived as strings. A pipeline that read one then has a string column at its destination, and the new type is a change of it, which the destination refuses as it does any change of a column's type that isn't a widening. Read the column cast to `text` through a view to keep it a string.

### Large values in updates

Postgres keeps a large value out of the row, in the table's TOAST storage, as it does with a long `text` or `jsonb` that doesn't fit in about 2 kB even compressed. Under a table's default replica identity, the log leaves such a value out of an update that didn't change it, to save writing it again.

The source then sends that column as null, and names it as a column the update didn't send. Written with `merge`, the destination keeps the value it has for it; other write modes store the update as it came, with the column null and named in `_openetl_unchanged`.

With a SQL transform whose `WHERE` reads the table, such an update can bring a row into the `WHERE` that failed it before. The destination doesn't have that column's current value then, and the row lands without it, as [Statements that run](https://github.com/openetl-io/openetl/blob/main/docs/reference/specs/pipeline.md#statements-that-run) explains.

To have every update arrive whole, give such tables the `FULL` replica identity:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

The log then carries each updated or deleted row's old values in full, large values included, and the source fills each value an update left out from them. That costs more write-ahead log for each update and delete, and more for the slot to read. The other choices are to accept the behaviour above, or to read such tables with `full_refresh` or `incremental` in a scheduled pipeline, which read whole rows.

### Delivery

Full refresh and CDC streams replay exactly: a read resumed from a checkpoint yields the same rows in the same order. OpenETL counts incremental streams as ones that may repeat rows, because of the rows at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

From 0.2.0, a read that OpenETL gives an end stops there. OpenETL does this when it reads a writer lane's part of an epoch again on [several workers](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md), so the second read returns what the first did. The end is a resume point the first read sent, or, where the stream's read completed in that epoch, as a scheduled pipeline's does in its last one, where it completed.

- **CDC**: the end is a position in the write-ahead log. The read returns the changes of the transactions that committed before it and none after, though the log has moved on, then sends a resume point there if the end was one, and completes there. This holds once the stream's snapshot has been read.
- **Incremental**: the end of a read that completed is the highest cursor value it read up to. The read goes up to that value rather than the column's highest now, and completes there. It returns the rows that hold cursor values up to it now, so rows written or changed below it since make it differ from the first read.
- **Full refresh**, and incremental reads that ended at a resume point, ignore the end: the second read returns what the first did as long as the table hasn't changed.

> [!WARNING]
> A CDC stream whose snapshot was still being read in that part of the epoch ignores the end, whether it's a resume point or a completion. Its chunks would be read as the table is now, not as they were, so the read goes on to where the log is now. Unless nothing changed in the table since, it doesn't end where the first read did, and the attempt fails with `POSITION_LOST`. No version of the connector completes such a read where OpenETL asks: delete the pipeline and apply it again, which reads the table from a new snapshot.

From 0.2.0, a CDC read, a redo's included, that would start before the slot's confirmed position fails with `POSITION_LOST`: the slot doesn't stream the log from before that position. Before 0.2.0, the read started at the slot's position and skipped the changes in between. That happens when the slot moved on while a table wasn't read, such as a table paused while the pipeline read others through the slot, or when something else advanced the slot, such as another pipeline configured with the same `slot`. The table then needs a new snapshot: see [After a lost position](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/positions.md#after-a-lost-position).

## Destination

### What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, or else in the config's `schema` (`public` unless set). If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. The schema is created if it's missing.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true and the columns it had before the delete's epoch, so a change in that epoch before the delete doesn't reach them, and a row new in that epoch has only what the delete carried, which from CDC under the default replica identity is its key. A hard delete removes the row, and with it the version that kept older ones out: a version older than the delete that arrives in a later epoch inserts the row again.

Each epoch is staged into tables of its own, then applied behind a fence row in `_openetl_fence` with a marker per stream in `_openetl_loads`: in one transaction for the epoch, or one per stream under the pipeline's `delivery.commit.scope: stream`. Before it takes the fence, it creates any missing target and read tables, adds new columns and builds indexes (`CREATE INDEX CONCURRENTLY`), outside that transaction. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` and `_openetl_loads` in place.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Set it up

The user in `dsn` needs `USAGE` and `CREATE` on the schema, or, if the schema doesn't exist yet, `CREATE` on the database.

From 0.2.0, `tlsCa`, `tlsCert` and `tlsKey` give TLS's certificates in place, as the source's do: see [TLS](#tls).

From 0.2.0, its check has the source's `net.dns`, `net.connect`, `db.tls` and `db.connect`, then `schema.create`: the user has `USAGE` and `CREATE` on `schema`, to create tables in it and reach them, or can create it. A preflight checks the same of the pipeline's namespace, and for each stream, `stream.NAME.write`: a merged stream has a primary key among its schema's columns, and a table that exists takes `INSERT`, and also `DELETE` to overwrite it, `SELECT`, `UPDATE` and `DELETE` to merge into it, or `SELECT` for an append that skips the `_openetl_id`s the table has. A table that doesn't exist is created. One the user doesn't own fails the item if it lacks a column of the stream's, which only its owner can add, or if it's merged into without a unique index on the key or appended to with deduplication against the target without an index on `_openetl_id`, which only its owner can create; otherwise it warns, since a column the stream gains later can't be added. A hold's release updates the table in any write mode, which the preflight doesn't ask of append and overwrite.

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

Postgres's `text` and `jsonb` can't hold the NUL character, so a string or JSON value with one (`\u0000`) fails the epoch.

### Schema changes

New columns are added to the table, until it has the 1,600 columns Postgres allows, dropped ones included; past that, the destination refuses the column.

The destination doesn't change an existing column's type by itself, since `ALTER COLUMN … TYPE` can rewrite and lock the table. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change:

```sql
ALTER TABLE "raw"."orders" ALTER COLUMN "amount" TYPE numeric(20,2)
```

Run it, then release the hold. Or let the destination make such changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. It then widens `smallint` to `integer`, `bigint`, `real` or `double precision`, `integer` to `bigint`, any integer or `real` to `double precision`, integers and decimals to a `numeric` that holds them, and anything to `text`. A change that isn't a widening is refused either way.

A schema change waits up to 5 seconds for its table's lock, and tries up to 5 times, so queries on the table don't queue behind it.
