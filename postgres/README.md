---
title: PostgreSQL
description: Read Postgres tables in full, by a cursor column, or change by change through logical replication, and write into Postgres by appending, overwriting or merging.
---

The `postgres` package has a source and a destination. The source reads the tables, views and materialized views of one schema: whole, from where a cursor column last got to, or as a stream of every insert, update and delete through logical replication (CDC). The destination writes each stream into a table of its own, and commits each epoch exactly once.

## Source

### What it reads

Each table in the configured `schema` is a stream, or only the ones `tables` lists. A partitioned table is one stream, read through its root. Views and materialized views are streams too, read with `full_refresh` or `incremental`.

A stream can be read three ways, which the pipeline picks with each stream's `sync`:

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table and every view | The whole table each time. A table with a read order (below) is read in that order, `chunkRows` rows a query, and a stopped read resumes after its last chunk. One without is read in a single query. |
| `incremental` | Tables and views with a column of type `smallint`, `integer`, `bigint`, `numeric`, `date`, `timestamp`, `timestamptz`, `text` or `varchar` | The rows whose `cursor` column is at or above where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the read order. |
| `cdc` | Tables with a primary key, whose replica identity is the default or `FULL` | First a snapshot of the table, in chunks of `snapshotChunkRows`, then every change from the write-ahead log, through a replication slot with the `pgoutput` plugin. |

A table's read order is its primary key. A table without one is read in the order of a unique index instead, if it has one whose columns are all `NOT NULL`, each in its column's collation, with no `WHERE` and no expressions, and that has finished building; of several, the one with the fewest columns, then the first by name. A table that other tables inherit from has no such index, since a read of it reads their rows too, which its indexes don't cover; a partitioned table's unique index covers its partitions. Nor does such a table's primary key cover them, though it's still the read order and the stream's key: their rows can repeat a key, and then a chunked read misses a row whose key the chunk before it ended on, and a merge keeps one row of each key. A CDC read of such a table reads their rows in its snapshot, but none of their changes after it. No two rows share such a key, so a chunked read is exact as it is with a primary key. The index doesn't make the stream keyed: the catalog gives it no primary key, and it can't be read with `cdc`. A read resumed in another order than it started in, after the index is dropped or another takes its place, fails with `POSITION_LOST`: only a read from the start goes on from there. So does a read resumed after its table lost its primary key with no such index in its place. So does a split of the table that hasn't started yet, since its range is of the old order's first column. A table or view with neither is read in one query, and a stopped read reads it again from the start; that includes every view and materialized view, whose columns are never `NOT NULL`. Reading by physical position (`ctid`) instead isn't sound: an update can move a row to a page the read has passed, and it's then missed.

The catalog names no default cursor: nothing in Postgres's catalog shows that a column orders new and changed rows, so a pipeline names its `cursor`. An incremental read starts at the last read's highest cursor value, not after it, so the rows that had that value are read again. Rows whose cursor is `NULL` are never read, nor those whose cursor is `NaN` or an infinity, in a `numeric`, `date`, `timestamp` or `timestamptz`. Deleted rows aren't seen: CDC is the mode that sees deletes. A value from a sequence or the clock is taken before its transaction commits, so a row can commit with a value below one a read has already passed, and be missed.

A table with a read order, read in full or incrementally, is split into ranges of its order's first column from the server's histogram statistics (`pg_stats`), which OpenETL reads in parallel on up to `maxConnections` connections; a CDC table's initial snapshot is split in the same way when more than one connection may run. Small tables, and tables without statistics, are read whole. A range being read can be split again while it's read.

A column added to a table since the run discovered it fails the read with `TRANSIENT` before it reads, in any mode: the next attempt discovers the column and declares its type. A column added while a CDC read runs fails the read the same way, at the first change that has it. So does a column whose type changes so that it's declared as another type, as `ALTER COLUMN … TYPE` from `integer` to `bigint` does; one declared as the same, as `varchar` to `text`, reads on. A column whose type changed so since the run discovered it fails the read with `TRANSIENT` before it reads, in any mode, and the next attempt declares the new type. That needs an OpenETL that gives a read the types the run declared; with one that doesn't, the read sends the new type's values as the old type's.

`TRUNCATE` isn't carried as a CDC read's deletes: the source logs a warning, and the destination keeps the rows; a new snapshot of the table brings it back in step.

A read holds up to `maxConnections` connections, one for each lane it runs, and CDC holds one replication connection besides.

### Set it up

The user in `dsn` needs `USAGE` on the schema and `SELECT` on every table and view the source offers: the source's check fails otherwise, naming what it can't read. The check looks only at those `tables` lists, where it's set.

For CDC, the server must be Postgres 13 or later, and the server and the user need more:

- A `dsn` that reaches the primary: the read takes the log's position with `pg_current_wal_lsn()`, which a standby can't give.
- `wal_level = logical` in the server's configuration, which takes a restart. Without it, creating the slot fails with an error that says so.
- A role with the `REPLICATION` attribute, to create the slot and stream from it, or on Amazon RDS, the `rds_replication` role.
- A publication of the tables read with CDC, named by `publication` (`openetl` unless set). The source creates it, or adds the tables it lacks, if the user may. If not, create it yourself:

  ```sql
  CREATE PUBLICATION openetl FOR TABLE public.orders, public.customers WITH (publish_via_partition_root = true);
  ```

Each pipeline reads through a replication slot of its own: `slot`, or else `openetl_` and the pipeline's ID without its dashes, such as `openetl_00315f14c35a4f60875ffde7f5bd28d8` for the pipeline `00315f14-c35a-4f60-875f-fde7f5bd28d8`. The source creates it on the first read; on Postgres 17 and later it creates it as a failover slot, so a standby that synchronizes slots keeps it. If the server's `max_replication_slots` are all taken, the read fails with `RESOURCE_EXHAUSTED` until one is free. Deleting the pipeline drops the slot.

> [!WARNING]
> A replication slot keeps the server's write-ahead log until the pipeline has read it. A pipeline that's paused or failing for long makes the log grow on the source's disk. Set `max_slot_wal_keep_size` to cap it: OpenETL then warns as a slot nears the cap, and a slot the server invalidates has to be read again from a new snapshot.

### Check and preflight

The source's check and a pipeline's preflight report an item for each thing they look at, with a fix for what fails. The check is about the connection:

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

The `dsn`'s `sslmode` decides whether connections use TLS and how they verify the server, and `sslrootcert`, `sslcert` and `sslkey` name files on the machine that runs the connector. Both roles also take them in place, as PEM, which suits a secret store better than a file:

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
| `interval` | interval of months, days and nanoseconds, the parts Postgres keeps |
| Arrays of `boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `text`, `varchar` and `char(n)`, in a table column declared with one dimension (`int[]`) | a list of the item's type, with nulls where the array has them |
| Enums | string: the label |
| Anything else, such as domains, even of a type above, arrays of other types, arrays declared with more than one dimension (`int[][]`) or none, as a view's are, network types, ranges and geometric types | string, in Postgres's text form |

`openetl discover` lists a column as nullable unless it's `NOT NULL`. In a table CDC can read, only the key's columns are listed as not nullable, since a deleted row carries only its key.

A CDC read sends dates and timestamps as a full refresh does, BC ones too.

An array's bounds aren't kept: `'[0:1]={5,6}'` arrives as `[5, 6]`. Some values have no place in the type their column arrives as, and OpenETL can't quarantine a row of a list or interval column, or of a CDC read's date or timestamp column, so the read fails with `DATA_INVALID`, naming the column and the value:

- An array of more than one dimension in a column declared with one, such as `'{{1,2},{3,4}}'` in an `int[]`.
- An interval whose hours, minutes and seconds are past about 292 years, the most nanoseconds a 64-bit integer holds, such as `'3000000 hours'`, and the infinite intervals of Postgres 17 and later. Months and days have their own parts, so `'1000 years'` arrives.
- In a CDC read, a timestamp past 294247-01-10, the last that microseconds since 1970 hold. A full refresh or incremental read quarantines its row.

To read such a column, read it through a view that casts it to `text`.

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

Full refresh and CDC streams replay exactly: a read resumed from a checkpoint yields the same rows in the same order, apart from the CDC snapshots below. OpenETL counts incremental streams as ones that may repeat rows, because of the rows at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

A CDC snapshot reads each chunk of a table as the table is then, while the log brings every change after the stream's position in it. A row inserted after that position, or moved to a new key, comes from the log alone, even where a chunk reads it, if the log sent all of its columns. The log doesn't send generated columns, columns a publication's column list leaves out, or, under the default replica identity, a [large value](#large-values-in-updates) that a key change left as it was. A chunk that reads such a row sends it too, as an insert. Two kinds of row also reach an append differently, while a merge keeps the latest version of each:

- **A row that was in the table at the stream's position and changes before its chunk is read.** The chunk sends the row as it reads it, as an insert, after the log's change, so an append gets the change and then the chunk's insert of the same version. The source sends no row for the version from before the change, so that version never arrives. Such a row deleted before its chunk is read arrives as its delete alone.
- **A row inserted while the snapshot is read, with a key the snapshot hadn't reached, before the checkpoint a read resumes from part-way through the snapshot.** The resumed read reads it again in a chunk, so an append gets it twice.

A read that OpenETL gives an end stops there. OpenETL does this when it reads a writer lane's part of an epoch again on [several workers](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/workers.md), so the second read returns what the first did. The end is a resume point the first read sent, or, where the stream's read completed in that epoch, as a scheduled pipeline's does in its last one, where it completed.

- **CDC**: the end is a position in the write-ahead log. The read returns the changes of the transactions that committed before it and none after, though the log has moved on, then sends a resume point there if the end was one, and completes there. This holds once the stream's snapshot has been read. An end in another replication slot's log than the one the source reads fails with `POSITION_LOST`.
- **Incremental**: the end holds the highest cursor value the first read went up to, and the read goes up to that value rather than the column's highest now. Where the end is a resume point, the read returns no row past that resume point's, wherever its chunks end, then sends that resume point and completes there; where it's a completion, the read completes there. It returns the rows that hold cursor values up to the end now, so rows written or changed below it since make it differ from the first read. A resume point of a table read in another order than it is now fails with `POSITION_LOST`, as a read resumed from one does. A table read without a read order sends its resume point once it has read its rows, so a read up to that resume point returns them though the table has a primary key since.
- **Full refresh** ignores the end: the second read returns what the first did as long as the table hasn't changed.

> [!WARNING]
> A CDC stream whose snapshot was still being read in that part of the epoch ignores the end, whether it's a resume point or a completion. Its chunks would be read as the table is now, not as they were, so the read goes on to where the log is now. Unless nothing changed in the table since, it doesn't end where the first read did, and the attempt fails with `POSITION_LOST`. No version of the connector completes such a read where OpenETL asks: delete the pipeline and apply it again, which reads the table from a new snapshot.

A CDC read, a redo's included, that would start before the slot's confirmed position fails with `POSITION_LOST`: the slot doesn't stream the log from before that position. That happens when the slot moved on while a table wasn't read, such as a table paused while the pipeline read others through the slot, or when something else advanced the slot, such as another pipeline configured with the same `slot`. The table then needs a new snapshot: see [After a lost position](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/positions.md#after-a-lost-position).

## Destination

### What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, or else in the config's `schema` (`public` unless set). If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. The schema is created if it's missing.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then: [Overwrite](#overwrite) has how.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true and the columns it had before the delete's epoch, so a change in that epoch before the delete doesn't reach them, and a row new in that epoch has only what the delete carried, which from CDC under the default replica identity is its key. A hard delete removes the row, and with it the version that kept older ones out: a version older than the delete that arrives in a later epoch inserts the row again.

Each epoch is staged into tables of its own, then applied behind a fence row in `_openetl_fence` with a marker per stream in `_openetl_loads`: in one transaction for the epoch, or one per stream under the pipeline's `delivery.commit.scope: stream`. Before it takes the fence, it creates any missing target and read tables, adds new columns and builds indexes (`CREATE INDEX CONCURRENTLY`), outside that transaction; an overwrite that swaps builds the target's indexes on its read's table inside it, as nothing else uses that table. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` and `_openetl_loads` in place.

With the default commit scope, `pipeline`, the staging tables are `UNLOGGED`: their rows write no WAL and don't reach standbys. A crash of the server, or a failover to a standby, empties unlogged tables, so an epoch that's staged but not yet applied when it happens is lost as a whole: its commit applies nothing, and OpenETL reads the epoch again from the source. An overwrite's read table, which holds the read's applied epochs until it ends, is logged, and so is staging under `delivery.commit.scope: stream`, whose streams are applied one at a time. Autovacuum leaves staging tables alone, as they last only until their epoch is applied.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Overwrite

An overwrite applies each epoch of a read to a table of its own, `_openetl_read_…`, so the target keeps its old rows until the read ends. In the transaction that applies the read's last epoch, the read's rows replace the target's, one of two ways:

- **Swap.** The read's table takes the target's place: the destination drops the target and renames the read's table to the target's name, so the rows aren't written again. It does this when the table then differs from the target in nothing but its OID. First it gives the read's table what it can make again of the target: its columns' defaults and `NOT NULL`, its `CHECK`, primary key, unique and exclusion constraints and its other indexes, under their names, its owner, the privileges on it and on its columns, the comments on it and its columns, its storage parameters, and statistics, with `ANALYZE`. The read's table is created with the target's columns' order, collations and `fillfactor`, so its pages fill as the target's would. Checking the rows against `NOT NULL` and the constraints, and building the indexes, read the rows again; nothing writes them again.
- **Copy.** The target's rows are deleted, the read's inserted, and the read's table dropped. The target stays the same table: readers see the old rows until the transaction commits, and don't wait for the overwrite.

On Postgres 14 and later, after each epoch, the destination reads the pages at the end of the read's table that the epoch's rows went to, or, once it has swapped, of the target, so that their hint bits are set while the pages are recent: a hint bit set first after a checkpoint logs its page whole on a server with data checksums or `wal_log_hints`. It waits up to 5 seconds for a lock on the table to read them. The read's end then doesn't set them, nor, where the read's table has swapped in, do the target's first readers; rows copied in are new rows, whose hint bits the target's first readers set.

It copies when anything else hangs off the target, or a swap would change something about it: a view, materialized view, rule or function that depends on it, triggers, row security, a foreign key to it or from it, a publication or subscription, inheritance or partitions, extended statistics, an identity, generated or serial column or another sequence it owns, security labels, a replica identity other than the default, a clustered index, an index in a tablespace of its own, or with a statistics target, a comment on one of its indexes or constraints, a constraint that isn't validated, a column with options or a statistics target of its own, a column that the read's table doesn't have or has with another type, collation or storage, a privilege granted by a role that doesn't own the table, a comment or security label on its row type, a typed or unlogged target, one that belongs to an extension, a logical replication slot in its database with another plugin than `pgoutput`, which decodes every table's changes and would see none for the swap, or a target that the user doesn't own, directly or through a role. The read's table is checked in the same way. It logs why it copied.

The swap takes an `ACCESS EXCLUSIVE` lock on the target once the epoch's other work is done, rows copied into other tables included, and holds it while it drops the target and renames, until the transaction commits. It waits up to 5 seconds for the lock, as while a transaction that has read the table is still open, and copies instead if it doesn't get it; queries that start while it waits wait too. Where several of an epoch's streams swap, each waits for its lock in turn, holding those it has; if one doesn't come, or one stream's swap can't be finished, the destination undoes them all and lets go of every lock, copies that stream's rows, and the others try again, so rows are never copied into a table while a swap holds another's lock. Queries that start while it holds the lock wait for it, then read the new rows, as do statements prepared before it. It isn't MVCC-safe, as `TRUNCATE` isn't: a transaction whose snapshot is older than the swap, and that reads the table only after it, sees the rows of the read's epochs that were applied when its snapshot was taken, rather than the old rows. On a hot standby, replaying the lock waits for the queries on the table there, or cancels them, as replaying a `TRUNCATE` does. The table has a new OID, so what follows a table by OID, such as its counters in `pg_stat_user_tables`, starts again, and event triggers see the swap's `DROP TABLE` and `ALTER TABLE`.

Two settings change this; the [config schema](./destination.schema.json) has both:

- `overwriteSwap: false` copies always, so the table keeps its OID.
- `overwriteTruncate: true` empties the target with `TRUNCATE` rather than `DELETE` when the destination copies. That writes no WAL for each old row and leaves no dead rows, but `TRUNCATE` holds an `ACCESS EXCLUSIVE` lock on the table until the copy commits, which readers wait for, and isn't MVCC-safe: a transaction whose snapshot is older sees the table empty. It fires the table's `TRUNCATE` triggers rather than its `DELETE` ones, and row security doesn't limit it. It waits up to 5 seconds for the lock, and deletes the rows instead if it doesn't get it, if a foreign key refers to the table, if the user may not truncate it, or if a publication publishes the changes of the table, or of a table that inherits from it or is its partition, but not truncates.

### Set it up

The user in `dsn` needs `USAGE` and `CREATE` on the schema, or, if the schema doesn't exist yet, `CREATE` on the database. An overwrite [swaps](#overwrite) its read's table in only for a target that the user owns, or that a role owns whose privileges the user has, as a member that inherits them.

`tlsCa`, `tlsCert` and `tlsKey` give TLS's certificates in place, as the source's do: see [TLS](#tls).

Its check has the source's `net.dns`, `net.connect`, `db.tls` and `db.connect`, then `schema.create`: the user has `USAGE` and `CREATE` on `schema`, to create tables in it and reach them, or can create it. A schema that doesn't exist is created in a transaction the check rolls back, so a name PostgreSQL refuses, such as one that starts with `pg_`, fails the item, as it fails the first write with `CONFIG_INVALID`. A preflight checks the same of the pipeline's namespace, and for each stream, `stream.NAME.write`: a merged stream has a primary key among its schema's columns, and a table that exists takes `INSERT`, and also `DELETE` to overwrite it, `SELECT`, `UPDATE` and `DELETE` to merge into it, or `SELECT` for an append that skips the `_openetl_id`s the table has; with `overwriteTruncate`, it warns if the table doesn't take `TRUNCATE`, since an overwrite then deletes. A table that doesn't exist is created. One the user doesn't own fails the item if it lacks a column of the stream's, which only its owner can add, or if it's merged into without a unique index on the key or appended to with deduplication against the target without an index on `_openetl_id`, which only its owner can create; otherwise it warns, since a column the stream gains later can't be added, and an overwrite copies its rows in rather than swapping. A hold's release updates the table in any write mode, which the preflight doesn't ask of append and overwrite.

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

Postgres's `text` and `jsonb` can't hold the NUL character, or bytes that aren't UTF-8, so a string or JSON value with one (`\u0000` in JSON) fails the epoch's write. It fails with `DATA_INVALID`, naming the stream and the column.

### Schema changes

New columns are added to the table, until it has the 1,600 columns Postgres allows, dropped ones included; past that, the destination refuses the column.

The destination doesn't change an existing column's type by itself, since `ALTER COLUMN … TYPE` can rewrite and lock the table. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change:

```sql
ALTER TABLE "raw"."orders" ALTER COLUMN "amount" TYPE numeric(20,2)
```

Run it, then release the hold. Or let the destination make such changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. It then widens `smallint` to `integer`, `bigint`, `real` or `double precision`, `integer` to `bigint`, any integer or `real` to `double precision`, integers and decimals to a `numeric` that holds them, and anything to `text`. A change that isn't a widening is refused either way.

A schema change waits up to 5 seconds for its table's lock, and tries up to 5 times, so queries on the table don't queue behind it.
