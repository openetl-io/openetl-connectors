---
title: Oracle Database
description: Read Oracle tables in full, by a cursor column, or change by change from the redo log through LogMiner, and write into Oracle by appending, overwriting or merging.
---

The `oracle` package has a source and a destination. The source reads the tables of one schema: whole, from where a cursor column last got to, or as a stream of every insert, update and delete from the redo log, which it reads through LogMiner (CDC). The destination writes each stream into a table of its own, and commits each epoch exactly once.

Both use [go-ora](https://github.com/sijms/go-ora), a pure Go driver, so nothing of Oracle's client has to be installed. Both are tested against Oracle Database 23ai Free (23.26), connected to its pluggable database `FREEPDB1`.

## Source

### What it reads

Each table in the configured `schema` is a stream, or only the ones `tables` lists; by default the schema is the user's own. Names are as Oracle stores them, which is usually in upper case: `ORDERS`, not `orders`. Views, temporary, nested and external tables aren't offered, and a table's hidden and virtual columns are left out. A column of a type the source can't read, such as `BFILE`, `VECTOR` or an object type, is left out too, and a table with no other column isn't offered: naming one in `tables` fails as misconfigured, naming its columns.

A stream can be read three ways, which the pipeline picks with each stream's `sync`:

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table | The whole table each time. A table whose primary key, or else a unique index on `NOT NULL` columns, has only `NUMBER`, character, `RAW`, `DATE` and `TIMESTAMP` columns is read in key order, `chunkRows` rows a query, and a stopped read resumes after its last chunk. Another table is read in a single query. |
| `incremental` | Tables with a `NUMBER`, character, `RAW`, `DATE` or `TIMESTAMP` column | The rows whose `cursor` column is at or above where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the key. |
| `cdc` | Tables read in chunks, as above, whose changes LogMiner can read: see [Limits](#limits) | First a snapshot of the table, as of one SCN, in chunks of `snapshotChunkRows`, then every committed change from the redo log. |

An incremental read starts at the last read's highest cursor value, not after it, so the rows that had that value are read again. Rows whose cursor is `NULL` are never read, and deleted rows aren't seen: CDC is the mode that sees deletes.

A table whose key starts with a `NUMBER` column is read in up to 32 key ranges at once, between the column's least and greatest values, as many as its statistics' row count (or else a count) fills at `chunkRows` a range. A range that falls behind is divided again at the midpoint of what it has left. A read holds up to `maxConnections` connections, one for each lane it runs; a CDC read's LogMiner session holds one of them.

A stopped full refresh or incremental read resumes in the order it started in. If the table's key was added, dropped or changed in between, a read that had got part of the way in key order fails with `POSITION_LOST`, and so does a key range that hadn't started yet, since its range is of the old key's first column: only a read from the start goes on from there. A table read in a single query that has been read is done, even if it has been given a key since.

Queries compare text in binary (`NLS_SORT` and `NLS_COMP` `BINARY`), so a character key is read in the order of its bytes, and an `NCHAR` or `NVARCHAR2` key in the order of its UTF-16 code units.

### How CDC reads the redo log

Every second, the source starts a LogMiner session over the redo written since the last one, with the online catalog as its dictionary, and reads its rows for the tables it reads. It keeps each transaction's changes in memory until the transaction commits, then sends them in commit order; a rolled-back transaction, a statement rolled back and the changes undone by `ROLLBACK TO SAVEPOINT` are dropped. A transaction that spans many sessions stays in memory until it ends, however long that takes.

Connected to a pluggable database, LogMiner finds the logs that hold the redo itself, and returns only that PDB's changes. Connected to a database that isn't a container database (or to `CDB$ROOT`), the source finds the archived and online logs in `V$ARCHIVED_LOG` and `V$LOG` and adds them to each session.

The source doesn't use `CONTINUOUS_MINE`, which Oracle removed in 19c, nor `COMMITTED_DATA_ONLY`. Changes that are still in the log buffer when a session reads its window aren't in it, and LGWR may write them seconds later, as it may a commit made with `COMMIT WRITE NOWAIT` or `BATCH`. The next window mines again from 5 seconds back from the end of the last, or from the last row the last window read of each redo thread if that's earlier, and a stream's position starts no later than that row, so such changes are read once they're written, and a change or a commit read twice is sent once.

Every change carries a sequence number: its commit's SCN times 10¹² plus its place in the transaction, so the destination applies a key's versions in order.

An update that changes a row's key arrives as two changes: a delete of the row under its old key, with the old values, then the update under the new key. Both have the update's sequence number, as they're of different keys.

### How CDC hands over from the snapshot

A stream with no state first takes an SCN and lists the transactions open at it (`V$TRANSACTION`). Each of its tables is read as of that SCN with a flashback query (`AS OF SCN`), in key order, `snapshotChunkRows` rows a query, with a resume point after each chunk. The redo is then read from before the first change of every transaction open at the SCN, and only changes whose transaction committed after the SCN are sent. A transaction that was open during the snapshot is in neither the snapshot nor out of the redo: it's sent once, whole, when it commits.

A flashback query reads the table as the database's undo kept it at the SCN. A snapshot that takes longer than `UNDO_RETENTION`, or a table whose definition changed after the SCN, fails with `ORA-01555` or `ORA-01466`; the read fails as lost, and starts again from a new snapshot.

### LOBs and what LogMiner doesn't give

The redo log doesn't hold a `CLOB`, `NCLOB`, `BLOB` or `XMLTYPE` value whole. For a change to a table that has one, the source reads those columns from the table as of the change's commit SCN, by the row's key, with a flashback query. If the row is gone by then, or the undo that query needs is gone (`ORA-01555`), it sends the columns as unchanged: merge keeps what the destination has for them. A LOB written in pieces with `DBMS_LOB`, or an `XMLTYPE` written alone, is read the same way, as an update of its row.

LogMiner can't read changes to `BOOLEAN`, `JSON` and `VECTOR` columns, nor to `UROWID`, `LONG` or `LONG RAW` ones, nor to a table or column whose name is longer than 30 bytes: a table with one isn't offered for CDC. A change LogMiner reports as unsupported fails the read, naming the table.

### Set it up

The database runs in `ARCHIVELOG` mode, with minimal supplemental logging. As `SYSDBA` in `CDB$ROOT` (or in the database, if it isn't a container database), with a fast recovery area to archive to:

```sql
ALTER SYSTEM SET DB_RECOVERY_FILE_DEST_SIZE = 10G SCOPE = BOTH;
ALTER SYSTEM SET DB_RECOVERY_FILE_DEST = '/opt/oracle/oradata/FREE/fra' SCOPE = BOTH;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

Each table read with CDC logs all its columns, so an update or delete carries the whole row. As its owner:

```sql
ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

The user in `dsn` is a user of the pluggable database. It needs these grants, which `SYSDBA` gives in that PDB; nothing more, and no `DBA` role:

```sql
GRANT CREATE SESSION TO openetl;
-- CDC
GRANT LOGMINING TO openetl;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO openetl;
GRANT SELECT ON SYS.V_$DATABASE TO openetl;
GRANT SELECT ON SYS.V_$LOG TO openetl;
GRANT SELECT ON SYS.V_$LOGFILE TO openetl;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO openetl;
GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO openetl;
GRANT SELECT ON SYS.V_$TRANSACTION TO openetl;
```

The user reads its own schema's tables without grants. To read another schema's, it needs `SELECT` on each table, and `FLASHBACK` on each table read with CDC, for the snapshot's and the LOBs' flashback queries:

```sql
GRANT SELECT, FLASHBACK ON app.orders TO openetl;
```

The source's check signs in and reads the schema's tables, and checks, item by item, what CDC needs: the log mode, supplemental logging, the `LOGMINING` privilege, `DBMS_LOGMNR`, the views, and then that a LogMiner session starts. A check doesn't know which sync modes a pipeline uses, so what only CDC needs warns rather than fails. A pipeline's preflight checks each stream as it's read: its table, its sync mode and cursor, and for CDC its supplemental logging and a flashback query of it, and whether the archived logs reach back twice the pipeline's interval.

> [!WARNING]
> Oracle keeps archived logs until RMAN deletes them, or the recovery area needs the space, whatever a pipeline has read. A pipeline that's paused or failing for longer finds the redo after its position gone: its read fails as lost, naming the log, and its tables have to be read again from a new snapshot. A log file deleted outside RMAN is still listed, so the read tries again while LogMiner misses it, and fails as lost once it has for 2 minutes. Delete archived logs on a schedule that keeps them longer than the pipeline's interval, such as with RMAN: `DELETE NOPROMPT ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE - 1';`. OpenETL reports how much archived redo is behind a pipeline's position, and that it's gone once it is.

A stream's state names the database it's of, by its DBID and the SCN of its last `RESETLOGS`. After the database is restored with `RESETLOGS`, or the `dsn` names another database, a read fails as lost.

### Example

The [config schema](./source.schema.json) has every setting. This source offers the tables of `APP`, and its pipeline reads `ORDERS` by CDC and `CUSTOMERS` by its `UPDATED_AT` column:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-db
spec:
  connector: oracle
  config:
    dsn: { secret: app-db-dsn }
    schema: APP
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
      - name: ORDERS
        sync: cdc
      - name: CUSTOMERS
        sync: incremental
        cursor: UPDATED_AT
  destination: { ref: warehouse }
```

`dsn` is a URL, `oracle://USER:PASSWORD@HOST:1521/SERVICE`, naming the pluggable database's service, such as `FREEPDB1`. It's a secret: store it with `openetl secret set app-db-dsn`. Options go in its query, as go-ora reads them: `SSL=true` connects with TLS, verifying the server's certificate unless `SSL VERIFY=false`, and `WALLET=/path/to/wallet` names a wallet; `CONNECTION TIMEOUT=SECONDS` bounds how long a connection takes to set up.

### Types

A snapshot and the redo log read each value the same way. Each session sets `TIME_ZONE` to UTC, and the date, timestamp and number formats.

| Oracle | Arrives as |
|---|---|
| `NUMBER(p)` with p up to 4, 9 and 18 | 16-, 32- and 64-bit integers |
| `NUMBER(p, s)` | decimal(p, s) up to a precision of 38; with s above p, decimal(s, s); with s below 0, decimal(p - s, 0) |
| `NUMBER` without a precision, or past 38 digits | decimal(38, 9) |
| `FLOAT`, `BINARY_DOUBLE` | 64-bit float; a `FLOAT` has up to 38 digits, which a 64-bit float rounds to about 16 |
| `BINARY_FLOAT` | 32-bit float |
| `BOOLEAN` | boolean |
| `VARCHAR2`, `NVARCHAR2`, `CHAR`, `NCHAR`, `CLOB`, `NCLOB`, `LONG` | string; a `CHAR` with its padding |
| `ROWID`, `UROWID` | string |
| `XMLTYPE` | string, as `XMLSERIALIZE` prints it |
| `JSON` | JSON, as `JSON_SERIALIZE` prints it |
| `RAW`, `BLOB`, `LONG RAW` | binary |
| `DATE`, `TIMESTAMP(0)` to `TIMESTAMP(6)` | timestamp in microseconds |
| `TIMESTAMP(7)` to `TIMESTAMP(9)` | timestamp in microseconds, sent as text with every digit (see below) |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE` | timestamp in UTC, in microseconds; from a precision of 7, sent as text with every digit |
| `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` | interval of months, or of days and nanoseconds |

Timestamps are declared in microseconds, as OpenETL declares every timestamp. A value of a `TIMESTAMP(7)` to `(9)` column arrives as text with all its digits, which OpenETL parses: it keeps the nanoseconds for a stream the pipeline reads with `timestamps: nanosecond`, into a destination that keeps them, and drops them otherwise. Nanosecond timestamps end in 2262, and Oracle's run to 9999: in such a stream, a later value doesn't fit.

A value that doesn't fit its declared type, such as a `NUMBER` with more digits than decimal(38, 9) holds, is sent as Oracle prints it, which OpenETL can't put in the declared type: by default its row is quarantined, as [Holds and quarantine](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/holds-and-quarantine.md) explains. An `INTERVAL YEAR TO MONTH` of more months than a 32-bit integer holds, past 178,956,970 years, fails the read.

Columns are listed as nullable unless they're `NOT NULL`.

### Schema changes

A change to a table's columns that CDC reads, a column added, renamed, modified or set unused, ends the read where it's in the redo log: the read sends the changes before it, then fails retryably, and the next attempt declares the table's columns as they are now. A column the run didn't declare, one added since its Discover, fails the read retryably before it sends a change, so the next attempt declares it too. In any mode, a column whose type changed since the run's Discover so that it's declared as another type, as a `NUMBER(10,2)` modified to `NUMBER(12,2)`, fails the read retryably before it reads, and the next attempt declares the new type. That needs an OpenETL that gives a read the types the run declared. LogMiner reads a change made before such a DDL with the table's definition as it is when it reads it, so it names that change's columns by their place in the row; the source reads them by their place, with the columns added after the change as unchanged.

A column dropped moves the columns after it, so a change made before the drop can't be read: the read fails as lost, and the stream has to be read again from a new snapshot. So do `TRUNCATE`, `ALTER TABLE` truncating, dropping or exchanging a partition or subpartition, and exchanging another table's partition or subpartition with the table, since the redo log doesn't say which rows they removed. `DROP TABLE` fails the read as misconfigured.

### Delivery

Full refresh and CDC streams replay exactly: a read resumed from a resume point sends nothing it sent before it. A CDC read sends the redo log's changes in the order the first read did. A full refresh isn't a snapshot of one moment: a read in key order reads each chunk as the table has it when it reads the chunk, so a row deleted since the read began, or since the read it resumes, isn't sent, and a row changed since is sent as it is then. A table read in a single query has no resume point inside its read, so a read of it that stops starts again from the table's start. OpenETL counts incremental streams as ones that may repeat rows, because of the rows at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

A read OpenETL asks to end at an earlier resume point (`until`), to read a part again, sends nothing past it, then that resume point, and completes there. Where the stream's read completed in that part, as a scheduled pipeline's does, OpenETL asks the read to complete where it did (`complete_at`) instead: an incremental read goes up to the highest cursor value the first read went up to, rather than the column's highest now, and a CDC read stops at the SCN the first read completed at, though the redo log has moved on; either then completes with the same state. A full refresh completes with no position, so a read again of a part it completed in goes to the table's end. A full refresh or incremental read sends the rows between the two ends again, as the table has them now: a row deleted since isn't sent, and one changed is sent with its values now, so that part doesn't land as it did. A CDC read sends the redo log's changes again as they were, with the same sequence numbers; the snapshot's rows of that part are read again as of the snapshot's SCN, so the stream lands as it did. A new stream's first part has no state to read again from: its read starts where the stream's first read started, which each of its positions names, with the snapshot as of the same SCN, though the table has changed since. A flashback query reads that SCN only while the undo keeps it, so once it doesn't (`ORA-01555`), or once the table's definition has changed since (`ORA-01466`), the read fails as lost rather than read a new snapshot. A CDC position moves with the stream's changes and the redo log's files, not with when a read ended, so a read again from a resume point ends where the first did.

### Limits

- Mining a pluggable database's own redo needs Oracle 19.10 or later; an earlier container database can't be read with CDC by a PDB's user. The source doesn't mine a PDB from `CDB$ROOT`.
- A table without a primary key or a unique index on `NOT NULL` columns, or whose key has a column the source can't order by (a float, an `INTERVAL`, a LOB, `ROWID`), can't be read with CDC, and is read whole in one query.
- A transaction's changes to the tables read are held in memory until it commits.
- While a pluggable database has no transactions, each window mines again from its last one, back at most to the start of the newest redo log that began 5 seconds ago, which other containers' redo fills.
- The source takes each redo thread's log files to hold its redo in SCN order. A commit LGWR writes after a later commit of the same thread that a read sent, if the read stops in between, would be missed: the next read starts after the later one.
- A LOB or `XMLTYPE` value is read from the table as of its change's commit, not from the redo log; it's sent as unchanged when the undo for that is gone.
- A table's rows written with direct-path loads (`INSERT /*+ APPEND */` into a table with `NOLOGGING`, or SQL\*Loader's direct path without logging) aren't in the redo log, and CDC doesn't see them.
- Dates before the Gregorian calendar's start, 15 October 1582, are read through go-ora, which counts them in the Gregorian calendar, while Oracle counts them in the Julian: they're days off, and a key or cursor that has such a date can resume a read at the wrong row. Years before 1 AD arrive as the proleptic Gregorian calendar counts them (Oracle's year -1 is year 0).
- A change made before a change of its table's columns, which LogMiner gives in Oracle's internal format, is read assuming the database's character set is `AL32UTF8` and its national one `AL16UTF16`, the defaults.
- An empty LOB, `EMPTY_CLOB()` or `EMPTY_BLOB()`, which Oracle doesn't count as `NULL`, arrives as null: go-ora reads it so.
- The source is tested against single-instance databases; with RAC, LogMiner reads every thread's redo, and the source checks every thread's logs are on disk, but it isn't tested. A change an instance writes late can be missed while the read has seen no row of that instance's redo.

## Destination

### What it writes

Each stream becomes a table in the schema the pipeline's `destination.namespace` names, exactly as written (`raw` is the schema `raw`, not `RAW`), or else in the schema of the user in `dsn`. If the pipeline's `destination.tables` gives the stream a name, the table has that name. Otherwise, it has the stream's name as the pipeline's `destination.naming` gives it, which by default is lowercase. The destination quotes every table and column name, so it keeps a name exactly as it's given: lowercase, mixed case, spaces and all.

> [!IMPORTANT]
> Oracle folds a name that isn't quoted to upper case. A name that isn't all upper case has to be quoted wherever it's read, so a table named `orders` with a column `id` is read with `SELECT "id" FROM "orders"`; `SELECT id FROM orders` looks for `ORDERS`. Set the pipeline's `destination.naming: source` to keep an Oracle source's upper-case names.

A schema that doesn't exist is created as a user that can't sign in (`CREATE USER … NO AUTHENTICATION`), with an unlimited quota on the database's default tablespace.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then. A read that ends without rows empties the table. The destination learns a stream's table from its schema or its rows, so a read that ends before the stream has had either leaves a table that was already there as it is.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true, and its other columns as the table has them, so an update to the row earlier in the same epoch isn't kept. A hard delete removes the row, and its `_openetl_seq` with it: a version older than the delete that arrives in a later epoch inserts the row again. A column an update didn't send, such as a LOB that CDC reads as unchanged, takes its value from the key's latest version in the same epoch that sent it, unless the table's row is newer than that version, and otherwise keeps the value the table has. Merge needs a unique index on the key, which the destination creates if the table has none; it fails, naming the table, if the table's rows repeat a key.

Each epoch is staged into tables of its own, then applied with `MERGE`, `INSERT … SELECT` and `UPDATE` statements in one transaction, behind a fence row in `_openetl_fence`, with a marker per stream in `_openetl_loads`. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less.

The staging tables, `_openetl_fence`, `_openetl_loads`, and the indexes the destination creates on `_openetl_id` and merge keys are in the `dsn` user's own schema, whichever schema the tables are in. Their names start with `_openetl_`. Leave `_openetl_fence` and `_openetl_loads` in place; the destination drops its staging tables when it's done with them, and those an attempt left behind when a later one starts.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`. Up to 16 writers stage a pipeline's rows at once.

### Set it up

The user in `dsn` is a user of the pluggable database, with a quota on its default tablespace. To write to its own schema, it needs these grants, which `SYSDBA` gives in that PDB:

```sql
CREATE USER openetl IDENTIFIED BY "…" QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE TO openetl;
```

To write to other schemas, creating them and their tables, it also needs:

```sql
GRANT CREATE USER, CREATE ANY TABLE, ALTER ANY TABLE, SELECT ANY TABLE, INSERT ANY TABLE,
  UPDATE ANY TABLE, DELETE ANY TABLE, CREATE ANY INDEX TO openetl;
```

Or, to write only to tables that exist in another schema, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER` and `INDEX` on each, from `SYSDBA` or the table's owner:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, INDEX ON app.orders TO openetl;
```

The database is Oracle Database 19c or later, with `COMPATIBLE` 12.2 or later for names of up to 128 bytes. With `COMPATIBLE` 23 or later, booleans are written as `BOOLEAN`; before, as `NUMBER(1)`, 1 for true.

The destination's check reaches the listener (`net.dns`, `net.connect`), signs in (`db.connect`), and checks, item by item, the server's version (`server.version`) and `COMPATIBLE` (`server.compatible`), that the user can create tables (`privilege.create-table`), and its quota on its default tablespace (`quota.tablespace`). Each failure says how to fix it: a missing grant or quota with the `GRANT` or `ALTER USER` that gives it. A pipeline's preflight checks the same of the user, then that it can write to the pipeline's schema, or create it (`schema.write`), and for each stream, `stream.NAME.names`, that Oracle can name the table and its columns; `stream.NAME.mode`, that a merged stream has a key; `stream.NAME.write`, that the user has the grants above on the table, or can create it; and `stream.NAME.types`, which warns if the table has a column whose type the stream would change, with the fix.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: oracle
  config:
    dsn: { secret: warehouse-dsn }
```

`dsn` is a URL, `oracle://USER:PASSWORD@HOST:1521/SERVICE`, with the options the source's takes. Its user's schema is where the tables go unless the pipeline names another.

### Types

| Arrives as | Oracle column |
|---|---|
| boolean | `BOOLEAN`, or `NUMBER(1)` before `COMPATIBLE` 23 |
| 8-, 16-, 32- and 64-bit integers | `NUMBER(3)`, `NUMBER(5)`, `NUMBER(10)`, `NUMBER(19)` |
| unsigned 8-, 16-, 32- and 64-bit integers | `NUMBER(3)`, `NUMBER(5)`, `NUMBER(10)`, `NUMBER(20)` |
| 32- and 64-bit floats | `BINARY_FLOAT`, `BINARY_DOUBLE`, NaN and infinities included |
| decimal(p, s), p up to 38, s not negative | `NUMBER(p,s)` |
| any other decimal | `VARCHAR2`, as text |
| string | `CLOB`; in a merge key, `VARCHAR2` |
| binary | `BLOB`; in a merge key, `RAW` |
| date | `DATE` |
| time of day | `INTERVAL DAY(0) TO SECOND(6)`, or `SECOND(9)` in nanoseconds: the time since midnight |
| timestamp | `TIMESTAMP(6)`, or `TIMESTAMP(9)` in nanoseconds; `WITH TIME ZONE`, in UTC, if it has one |
| interval | `VARCHAR2(100 BYTE)`, as JSON: `{"months": 1, "days": 2, "nanoseconds": 3000}` |
| UUID | `VARCHAR2(36 BYTE)` |
| JSON, lists, structs and maps | `CLOB`, as JSON |

> [!NOTE]
> Oracle stores an empty string as `NULL`, in a `CLOB` or a `VARCHAR2`, and an empty binary value as `NULL` too. A row that arrives with `""` reads back with `NULL`. A merge key that's an empty string is refused, as a merge can't match a null key.

### Schema changes

New columns are added to the table, until it has 999 columns, one fewer than the 1,000 Oracle allows, as each staging table has a column of its own besides; past that, the destination refuses the column.

A column whose type the table's column holds already, such as a 32-bit integer in a `NUMBER(19)`, is written to it as it is. The destination doesn't change an existing column's type by itself. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change:

```sql
ALTER TABLE "raw"."orders" MODIFY ("amount" NUMBER(19))
```

Oracle changes a type in place only within a kind: a `NUMBER`'s precision and scale, a `VARCHAR2`'s or `RAW`'s length, a `TIMESTAMP`'s or `INTERVAL`'s fractional digits, and a `DATE` to a `TIMESTAMP`. For any other change, such as a number to text or a `VARCHAR2` to a `CLOB`, the refusal gives statements that add a column of the new type, copy the values to it, drop the old column and rename the new one, which loses the old column's `NOT NULL`, default and place:

```sql
ALTER TABLE "raw"."orders" ADD ("_openetl_new" CLOB); UPDATE "raw"."orders" SET "_openetl_new" = TO_CLOB("note"); COMMIT;
ALTER TABLE "raw"."orders" DROP COLUMN "note"; ALTER TABLE "raw"."orders" RENAME COLUMN "_openetl_new" TO "note";
```

Run them, then release the hold. Or let the destination make the in-place changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one; the others are refused either way. A change the table's column can't hold, such as binary into a number, is refused without a fix.

A schema change waits up to 5 seconds for its table's lock, and tries again up to 5 times.

### Holds

When a stream holds a column, its rows arrive with the column null, and the destination keeps, in `_openetl_held`, which version each held cell is waiting for. A hold's release fills the cells by key in a merged table, or by `_openetl_id` in an appended one, and only where the row still has the version the release is for. A merged column the pipeline marks to keep its last value keeps what the table has until it's released.

### Delivery

Exactly once, or effectively once when a stream is merged from a source that may repeat rows, with the staging and fence above. Recovery after a crash applies an epoch that was staged but not applied, and doesn't apply again one that was.

### Limits

- Table and column names are up to 128 bytes, and can't be empty or hold a double quote (`"`) or NUL. A stream or column with such a name fails.
- A merge key's text columns share 4,000 bytes: one is a `VARCHAR2(4000 BYTE)`, two are `VARCHAR2(2000 BYTE)` each, and so on; a binary key column is at most `RAW(2000)`. A key value longer than its column fails the epoch.
- A table has at most 999 columns, the destination's own included, as its staging tables have one more.
- Dates and timestamps are written from year 1 to 9999; a value outside fails the epoch. Oracle counts days before 15 October 1582 in the Julian calendar, so such a date is stored with the year, month and day it arrives with, and 5 to 14 October 1582 become 15 October.
- A time of day is an `INTERVAL DAY(0) TO SECOND`, as Oracle has no time-of-day type; read it with `EXTRACT`, or add it to a date.
- go-ora reads an `INTERVAL` to the microsecond; `EXTRACT(SECOND FROM …)` gives the nanoseconds a `SECOND(9)` column keeps.
- The destination is tested against Oracle Database 23ai Free, single instance. It isn't tested against 19c or RAC.
