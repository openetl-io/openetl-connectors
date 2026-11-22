---
title: SQL Server
description: Read SQL Server tables and views in full, by a cursor column, or change by change through change tracking or change data capture, and write into SQL Server by appending, overwriting or merging.
---

The `sqlserver` package has a source and a destination. The source reads the tables and views of one schema: whole, from where a cursor column last got to, or as a stream of every insert, update and delete through change tracking or change data capture (CDC). The destination writes each stream into a table of its own, and commits each epoch exactly once.

Both are tested against SQL Server 2022.

## Source

### What it reads

Each table and view in the configured `schema` (`dbo` unless set) is a stream, or only the ones `tables` lists. A view is read with `full_refresh` or `incremental`. A system-versioned (temporal) table is read with its period columns, and its history table is a stream of its own, like any other table. A computed column has its value, but under change data capture (below).

A stream can be read three ways, which the pipeline picks with each stream's `sync`:

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table and view | The whole table each time. A table with a primary key is read in key order, `chunkRows` rows a query, and a stopped read resumes after its last chunk. One without, and every view, is read in a single query, and a stopped read reads it again from the start. |
| `incremental` | Tables and views with a column of a number, `date`, `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset`, `char`, `varchar`, `nchar`, `nvarchar` or `rowversion` type | The rows whose `cursor` column is past where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the key. |
| `cdc` | Tables with a primary key that `cdcMethod` covers: tracked by change tracking, or captured by change data capture | First a snapshot of the table, in chunks of `snapshotChunkRows`, then its changes, polled every second. |

A table with a `rowversion` column offers it as the default cursor: SQL Server raises it on every insert and update, so it orders a row's versions. A read by a `rowversion` starts after the last read's highest value, and stops below the rowversion of the oldest transaction still open in the database, so a row that commits later isn't missed. A read by any other column starts at the last read's highest value, not after it, so the rows that had that value are read again; and a value from the clock or a sequence is taken before its transaction commits, so a row can commit with a value below one a read has already passed, and be missed. Rows whose cursor is `NULL` are never read, and deleted rows aren't seen: CDC is the mode that sees deletes.

A keyed table read in full or incrementally is split into ranges of its key's first column, from the statistics of its primary key's index, which OpenETL reads in parallel on up to `maxConnections` connections. Small tables, and tables without statistics, are read whole. A range being read can be split again while it's read.

A column added to a table since the run discovered it fails the read, in any mode, with `TRANSIENT`: the next attempt discovers the column and declares its type.

`cdcMethod` picks how `cdc` reads changes, for every stream of the source:

- **`change_tracking`** (the default) needs the least setup, and no SQL Server Agent. Each poll reads the keys changed since the last, with their rows as they are at the time of the read. A key changed several times between polls arrives once, with its latest row; a delete carries its key alone.
- **`change_data_capture`** reads every change with its whole row, from the change tables that SQL Server Agent's capture job fills from the transaction log. A change is read once the capture job has harvested it, a few seconds after it commits, and a read that isn't continuous ends at the changes harvested when it starts. Computed columns, and columns the table's capture instance doesn't capture, arrive null, named as columns the change didn't send, so a merge keeps the values the destination has for them. Choose it to keep every version of a row, or for a destination that reads a writer lane's part again (see [Delivery](#delivery)).

When a captured table's schema changes, make it a second capture instance with the new columns. The source reads the old instance up to where the new one starts, once the capture job has started it, then the new one, and the old one can be dropped once the source has read past it. Until then, a column added to the table arrives null in changes.

### Set it up

The login in `dsn` needs `SELECT` on the tables it reads:

```sql
CREATE LOGIN openetl WITH PASSWORD = '...';
USE shop;
CREATE USER openetl FOR LOGIN openetl;
GRANT SELECT ON SCHEMA::dbo TO openetl;
```

For CDC with change tracking, turn it on for the database and each table, and let the login see it:

```sql
ALTER DATABASE shop SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
ALTER TABLE dbo.orders ENABLE CHANGE_TRACKING;
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO openetl;
```

For CDC with change data capture, SQL Server Agent must run, which in a container takes `MSSQL_AGENT_ENABLED=true`. A member of `sysadmin` turns it on for the database, and a member of `db_owner` for each table; the login reads the change tables:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL;
GRANT SELECT ON SCHEMA::cdc TO openetl;
```

> [!WARNING]
> SQL Server cleans up changes after their retention: change tracking's `CHANGE_RETENTION`, 2 days by default, and change data capture's cleanup job, 3 days by default (`EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = ...`, in minutes). A pipeline that doesn't read a table for longer, or a table truncated under change tracking, fails with `POSITION_LOST`, which pauses it until it [reads from the start again](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/positions.md#after-a-lost-position). OpenETL reports how far behind each stream is, and how much retention it has left: with change data capture, only if the login has `SELECT` on `msdb.dbo.cdc_jobs`.

The `dsn` is a `sqlserver://` URL, such as `sqlserver://openetl:PASSWORD@db.example.com:1433?database=shop`, or an ADO or ODBC connection string. It's a secret: store it with `openetl secret set app-db-dsn`. Its `encrypt` decides TLS: `true` encrypts the whole connection, and verifies the server's certificate unless `trustServerCertificate=true`; `strict` (TDS 8.0, SQL Server 2022) always verifies it; `certificate` names a file of the authority to trust, and `hostNameInCertificate` the name the certificate holds. Without `encrypt`, only the sign-in is encrypted, unless the server forces encryption, and the server's certificate isn't verified, so the password can be read by whoever can stand between OpenETL and the server, unless the `dsn` sets `trustServerCertificate=false`. Set `encrypt=true` or `encrypt=strict`.

### Check and preflight

The source's check reports an item for each thing it looks at, with a fix for what fails:

| Item | What it checks |
|---|---|
| `net.dns`, `net.connect` | The `dsn`'s host resolves, and its port takes a connection. Not checked for a named instance without a port, which SQL Server Browser resolves. |
| `db.connect` | The login signs in and opens the database. A TLS handshake that fails, such as with a certificate the `dsn` doesn't trust, fails this item. |
| `db.tls` | The connection is encrypted: the `dsn`'s `encrypt` is `true` or `strict`, or the server encrypts it, and whether the `dsn` verifies the server's certificate. Otherwise it warns. |
| `server.version` | SQL Server 2016 or later. |
| `schema.exists` | The schema exists. |
| `schema.read` | The login has `SELECT` on every table and view in the schema. When `tables` is set, `table.NAME.read` instead, for each one it names. |
| `database.change-tracking` | With `change_tracking`, the database has change tracking on. It only warns: CDC is a pipeline's choice. |
| `database.change-data-capture`, `cdc.read` | With `change_data_capture`, the database has it on, and the login has `SELECT` on the `cdc` schema. |
| `cdc.agent` | With `change_data_capture`, SQL Server Agent is running. Skipped unless the login has `VIEW SERVER STATE`. |

A preflight is about the streams a pipeline reads:

| Item | What it checks |
|---|---|
| `stream.NAME.read` | The stream's table or view exists, and the login has `SELECT` on it. |
| `stream.NAME.cursor` | An incremental stream's cursor is a column of a type the source reads by. |
| `stream.NAME.change-tracking`, `stream.NAME.change-data-capture` | A table read with CDC has a primary key and is tracked, with `VIEW CHANGE TRACKING`, or captured, with `SELECT` on its capture instance. |
| `cdc.retention` | Warns when the database keeps changes no longer than the pipeline's schedule goes between syncs. With change data capture, skipped unless the login has `SELECT` on `msdb.dbo.cdc_jobs`. |

### Example

The [config schema](./source.schema.json) has every setting. This source offers the tables of `dbo`, and its pipeline reads `orders` by CDC and `customers` by its `updated_at` column:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-db
spec:
  connector: sqlserver
  config:
    dsn: { secret: app-db-dsn }
    cdcMethod: change_tracking
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

### Types

| SQL Server | Arrives as |
|---|---|
| `bit` | boolean |
| `tinyint` | 16-bit integer |
| `smallint`, `int`, `bigint` | 16-, 32- and 64-bit integers |
| `real`, `float` | 32- and 64-bit floats |
| `decimal(p,s)`, `numeric(p,s)` | a decimal of the same precision and scale |
| `money`, `smallmoney` | `decimal(19,4)`, `decimal(10,4)` |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `xml` | string |
| `binary`, `varbinary`, `image` | binary |
| `rowversion` | binary, 8 bytes |
| `uniqueidentifier` | UUID |
| `date` | date |
| `time(n)` | time of day, in microseconds |
| `datetime`, `smalldatetime`, `datetime2(n)` | timestamp without a time zone |
| `datetimeoffset(n)` | timestamp in UTC |
| `geography`, `geometry`, `hierarchyid` | string, as `ToString()` spells it |
| `sql_variant` | string |
| Alias types | as their base type |

Timestamps are declared in microseconds, as OpenETL declares every timestamp. A `datetime2(7)` or `datetimeoffset(7)` value arrives as text with its seventh digit, 100 nanoseconds, which OpenETL parses: it keeps the digit for a stream the pipeline reads with `timestamps: nanosecond`, into a destination that keeps nanoseconds, and drops it otherwise. Nanosecond timestamps end in 2262, and SQL Server's run to 9999: in such a stream, a later value doesn't fit, as the end of a system-versioned table's current rows' period does, `9999-12-31 23:59:59.9999999`. Times are in microseconds only, so `time(7)` drops its seventh digit. Keys and cursors keep every digit, so a read by such a column is exact.

`openetl discover` lists a column as nullable unless it's `NOT NULL` and not computed, and every column of a view as nullable, since what SQL Server notes of a view's columns goes stale as the tables under it change. In a table CDC can read, only the key's columns are listed as not nullable, since a deleted row may carry only its key.

### Delivery

A table read in full, a view, and a table CDC can't read replay exactly: a read resumed from a checkpoint reads on from it, with the rows as the table has them then. A table CDC can read, with either method, is declared to replay a superset, whichever mode reads it, since a stream has one declaration. Merged, the usual way to write CDC, it lands effectively once. Appended, it can carry a row twice: a snapshot's row and a change of it have different `_openetl_seq`, so different `_openetl_id`, and dedupe at apply keeps both. A Flink transform, which needs exact replay, can't read it. OpenETL counts incremental streams as ones that may repeat rows.

A CDC snapshot reads each chunk with the `_openetl_seq` of the position the snapshot started at, and leaves out the rows changed after that position, which arrive as their changes, whose `_openetl_seq` are higher. Under change tracking, which sees each change as it commits, a row so arrives once. Change data capture sees a change only once the capture job has harvested it: a row changed shortly before its chunk is read arrives in the snapshot with its new values, and again as its change. Merged by `_openetl_seq`, it lands as the table has it, and no change is lost.

A read that OpenETL gives an end, as it does to read a writer lane's part of an epoch again, stops there, after a resume point at that end. A snapshot's chunk read again has the rows the first read had, and those it left out, as the table has them then; a row deleted since is missing. Their changes arrive after the snapshot, as before. Under change data capture, the changes read again are the same, with the same `_openetl_seq`. Under change tracking, a key changed again past the end has only that later change, which is past the end, so it's left out, and a key the first read sent in two polls comes back once. OpenETL fails a redo with fewer rows than the first read had with `POSITION_LOST`. Only a destination that commits each writer lane on its own, such as Kafka, reads a part again: write to one from change data capture.

### Limits

- `cdc` reads only tables with a primary key, never views.
- Change tracking sends net changes, as its rows are at the time of the read: an update and the delete after it arrive as the delete alone, and an update's row can be newer than the version it's sent with.
- `sql_variant` values arrive as text, losing their base type.
- A primary key with a `sql_variant` column that holds anything but text can't order a read in chunks: SQL Server compares a variant's type family before its value, and the text a read goes on from has lost it. A read of `chunkRows` rows or more, a CDC snapshot of `snapshotChunkRows` or more, or a change tracking poll of `chunkRows` changes or more, reads its first chunk again and again past a date, time or number, and ends after the chunk at a binary or `uniqueidentifier` value, without the rows after it. Read such a table through a view of it, which is read in a single query.
- Under change data capture, a computed column's value arrives only in the snapshot; changes leave it out, so a merge keeps the snapshot's value at the destination, which goes stale as the row changes.

## Destination

### What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, or else in the config's `schema` (`dbo` unless set). If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. The schema is created if it's missing.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then.
- **Merge** updates and inserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. It's an `UPDATE` then an `INSERT`, not `MERGE`. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true. The table gets an index on the key: unique, unless the key has a text or binary column, which SQL Server's indexes compare with trailing spaces or zero bytes ignored; and none if a key column is `nvarchar(max)` or another type an index can't hold, in a table of yours.

Each epoch is bulk-copied into staging tables of its own, then applied in one transaction behind a fence row in `_openetl_fence`, with a marker per stream in `_openetl_loads`. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` and `_openetl_loads` in place.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Set it up

The destination needs SQL Server 2017 or later, and a database at compatibility level 130 or later. The login in `dsn` needs `CREATE TABLE` in the database, and `ALTER`, `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the schema, or `CREATE SCHEMA` if the schema doesn't exist yet:

```sql
USE warehouse;
CREATE USER openetl FOR LOGIN openetl;
GRANT CREATE TABLE, CREATE SCHEMA TO openetl;
GRANT ALTER, SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO openetl;
```

The `dsn` takes TLS settings as the source's does.

Its check has the source's `net.dns`, `net.connect`, `db.connect` and `db.tls`, then `server.version`, `database.compatibility`, `database.create-table`, and `schema.write`: the login can write tables in `schema`, or create it. A preflight checks the same of the pipeline's namespace as `namespace.write`, and for each stream, `stream.NAME.write`: the table would have no more than 1,024 columns, a table that exists takes `ALTER`, `SELECT`, `INSERT`, `UPDATE` and `DELETE`, and it warns of a column whose type the stream would change, with the statement that makes the change, or of one whose type an index of yours keeps, with typeChanges allowed or not.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: sqlserver
  config:
    dsn: { secret: warehouse-dsn }
    schema: raw
```

### Types

| Arrives as | SQL Server column |
|---|---|
| boolean | `bit` |
| 8- and 16-bit integers | `smallint` |
| unsigned 8-bit integers | `tinyint` |
| 32-bit integers, unsigned 16-bit | `int` |
| 64-bit integers, unsigned 32-bit | `bigint` |
| unsigned 64-bit integers | `decimal(20,0)` |
| 32- and 64-bit floats | `real`, `float`; NaN and infinities land as `NULL`, which neither holds |
| decimal(p, s) | `decimal(p,s)`, up to a precision of 38 |
| string, JSON | `nvarchar(max)`, or `nvarchar(450)` in a merge's key |
| binary | `varbinary(max)`, or `varbinary(900)` in a merge's key |
| UUID | `uniqueidentifier` |
| date | `date` |
| time of day | `time(6)`, or `time(7)` in nanoseconds |
| timestamp | `datetime2(6)`, or `datetimeoffset(6)` if it has a time zone; scale 7 in nanoseconds |
| interval | `nvarchar(max)`, in ISO 8601, such as `P1M2DT3.5S` |
| lists, structs and maps | `nvarchar(max)`, as JSON |

A date or timestamp outside the years 1 to 9999, which SQL Server's types hold, fails the commit, naming the column.

### Schema changes

New columns are added to the table, until it has the 1,024 columns SQL Server allows; past that, the destination refuses the column.

The destination doesn't change an existing column's type by itself, since `ALTER TABLE … ALTER COLUMN` can rewrite and lock the table. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change, dropping first the destination's own key index on the column if it has one:

```sql
DROP INDEX [_openetl_key_…] ON [raw].[orders]; ALTER TABLE [raw].[orders] ALTER COLUMN [id] bigint NOT NULL;
```

Run it, then release the hold. Or let the destination make such changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. It then widens `tinyint` to `smallint` to `int` to `bigint`, any of them or `real` to `float`, integers and decimals to a `decimal` that holds them, `time`, `datetime2` and `datetimeoffset` from scale 6 to 7, text to a longer `nvarchar` and binary to a longer `varbinary`, as a merge's key takes, and anything to `nvarchar(max)`. A change that isn't a widening is refused either way, as is one that a view of yours on the column blocks. Under an index of yours, a primary key's included, SQL Server changes a column only to a longer length of its type, such as `nvarchar(50)` to `nvarchar(450)`; any other change is refused without a statement, naming the index, which you drop, then change the column, then create again.

A schema change waits up to 5 seconds for its table's lock, and tries again up to 5 times, so queries on the table don't queue behind it.

### Limits

- A merge tells keys apart as the source's values differ: by case, accents and trailing spaces, whatever the key column's collation, where SQL Server's `=` and its default collation take `a`, `A` and `a ` as one. In a table of yours whose primary key or unique index takes two of the source's keys as one, by a case-insensitive collation or by trailing spaces, which every collation ignores, the commit fails with SQL Server's duplicate key error, naming the key, rather than merge them. A case-sensitive or binary collation (`_CS_`, `_BIN2`) tells case apart; only a key without such an index tells trailing spaces apart.
- A merge's text and binary key columns hold 450 characters, counted as UTF-16 code units, and 900 bytes, so an index can hold them; a longer value fails the write, naming the column. In a table of yours, a key column at least as long takes the keys as it is, and a shorter one is a widening, as above. The columns of a key together can pass the 1,700 bytes an index key holds, and a row whose key does fails the commit, with SQL Server's error.
