---
title: MySQL
description: Read MySQL and MariaDB tables in full, by a cursor column, or change by change from the binlog, and write into MySQL or MariaDB by appending, overwriting or merging.
---

The `mysql` package has a source and a destination, and both work against MariaDB too. The source reads the tables of one database: whole, from where a cursor column last got to, or as a stream of every insert, update and delete from the binary log (CDC). The destination writes each stream into a table of its own, and commits each epoch exactly once.

Both are tested against MySQL 8.4 and MariaDB 11.8.

## Source

### What it reads

Each base table in the configured `database` is a stream, or only the ones `tables` lists. Views aren't offered, and a table's virtual generated columns are left out, since the binlog doesn't carry them.

A stream can be read three ways, which the pipeline picks with each stream's `sync`:

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table | The whole table each time. A table whose primary key's columns are integers, decimals, strings, binary strings, dates, date-times, times, years or MariaDB UUIDs is read in key order, `chunkRows` rows a query, and a stopped read resumes after its last chunk. Another table is read in a single query. |
| `incremental` | Tables with an integer, `decimal`, `date`, `datetime`, `timestamp`, `year`, `char`, `varchar` or `text` column, or MariaDB's `inet4` or `inet6` | The rows whose `cursor` column is at or above where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the key. |
| `cdc` | Tables with a primary key read in chunks, as above | First a snapshot of the table, in chunks of `snapshotChunkRows`, then every change from the binlog, which the source reads as a replica does. |

An incremental read starts at the last read's highest cursor value, not after it, so the rows that had that value are read again. Rows whose cursor is `NULL` are never read, and deleted rows aren't seen: CDC is the mode that sees deletes.

A table read in key order whose key starts with an integer column other than `year` is read in up to 32 key ranges at once, between the column's least and greatest values, as many as the optimizer's estimate of its rows fills at `chunkRows` a range. A range that falls behind is divided again at the midpoint of what it has left. A read holds up to `maxConnections` connections, one for each lane it runs, and CDC holds one binlog connection besides.

### How CDC hands over from the snapshot

Each chunk of a table's snapshot is read with a locking read (`LOCK IN SHARE MODE`), which waits for the transactions writing its rows to commit and holds back new writes to them until it has noted where the binlog ends. The chunk then holds exactly the changes before that position. The source holds the chunk until it has read the binlog up to it, emits it there, and emits the changes after it, so nothing is lost or repeated. Writes to the chunk's rows wait while it's read, as they would for any locking read; `snapshotChunkRows` bounds how many rows that is.

Transactions MySQL compresses in the binlog (`binlog_transaction_compression`) are read as any other. An XA transaction's changes are in the binlog from its `XA PREPARE`, and are read from there, before it commits. Its `XA ROLLBACK` then fails the read, as the destination has changes that never committed, also where a later read gets to it: a stream's state keeps the XA transactions it read prepared until their `XA COMMIT`. A new snapshot of each table the transaction changed brings the destination back in step. MariaDB and MySQL log `XA COMMIT ... ONE PHASE` as one transaction, which is read as any other. Every change carries a sequence number made of the read's epoch, which goes up each time a read continues on another server, the binlog file's number, the position its transaction starts at and its place in the transaction, so the destination applies a key's versions in order.

### Set it up

The user in `dsn` needs `SELECT` on the tables it reads, and for CDC the privileges to read the binlog:

```sql
-- MySQL
GRANT SELECT ON app.* TO 'openetl'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'openetl'@'%';
-- MariaDB
GRANT SELECT ON app.* TO 'openetl'@'%';
GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO 'openetl'@'%';
```

`REPLICATION SLAVE` reads the binlog; `REPLICATION CLIENT` or `BINLOG MONITOR` reads where the binlog ends and which files it keeps. The snapshot's locking reads need only `SELECT`.

The server needs these settings, which a CDC read checks before it starts; it refuses to read otherwise, with an error naming what's set:

| Setting | Value | Note |
|---|---|---|
| `log_bin` | `ON` | MySQL's default; MariaDB needs `--log-bin` |
| `binlog_format` | `ROW` | |
| `binlog_row_image` | `FULL` | the default |
| `binlog_row_metadata` | `FULL` | the default is `MINIMAL`; `FULL` puts the columns' names in the binlog |
| `binlog_row_value_options` | not `PARTIAL_JSON` | MySQL only |

A session can set its own `binlog_format`, `binlog_row_image` or `binlog_row_value_options`. A change it logs as a statement, under `STATEMENT` or `MIXED`, has no rows to read; one that writes a table the read has fails the read rather than go unread, though a multi-table `UPDATE` or `DELETE` is taken to write only the first table it names. A change it logs without a column the stream reads, other than a delete's or an old row's columns outside the key, or with a partial update of a JSON column, fails the read rather than set the column null. Either way the stream has to be read again.

The binlog connection presents a server ID, which must differ from every replica's: `serverId`, or else one derived from the pipeline's name, at or above 2147483648. MariaDB ends a binlog connection when another connects with its server ID, as a replica or another pipeline of the same name can, and the read then fails as invalid config, naming `serverId`.

> [!WARNING]
> MySQL and MariaDB keep binlog files for `binlog_expire_logs_seconds`, whatever a pipeline has read. A pipeline that's paused or failing for longer finds its position purged: its read fails, and its tables have to be read again from a new snapshot. OpenETL reports how long the server keeps the file the pipeline's position is in, so it can warn before that happens, and a pipeline's preflight fails if the server keeps them for less than the time between its scheduled syncs.
>
> Amazon RDS and Aurora keep binlog files for their `binlog retention hours` instead, which is unset by default, so the files go as soon as they can, whatever `binlog_expire_logs_seconds` says, and what OpenETL reports and checks from it doesn't hold there. Set it: `CALL mysql.rds_set_configuration('binlog retention hours', 168);`.

With GTIDs (`gtid_mode = ON` on MySQL; MariaDB always has them), a stream's state keeps the GTIDs it has read too, and after a failover to another server of the same replication topology the read continues from them on the new server. Without them, a read of another server fails as lost, and the check and a CDC pipeline's preflight warn.

### Checks

A connection's check reports each of these items, with what it found, or why it failed and what to do about it:

| Item | Passes when |
|---|---|
| `net.dns`, `net.connect` | The DSN's host resolves and takes a connection. Not for a Unix socket. |
| `db.tls` | TLS is on, in `tls` or the DSN: the server takes it, with a certificate the config trusts. Under `preferred`, a server without TLS warns. |
| `db.connect` | The user signs in. |
| `database.found` | The database exists, and the user has a privilege in it. |
| `tables.found` | With `tables`: each table it names is in the database. |
| `tables.read` | The user can select from each table the source offers, and the source can read each of their columns. |
| `cdc.log-bin`, `cdc.binlog-format`, `cdc.binlog-row-image`, `cdc.binlog-row-metadata`, `cdc.binlog-row-value-options` | The server's settings are as above. |
| `cdc.gtid-mode` | `gtid_mode` is `ON`, or the server is MariaDB. |
| `cdc.replication` | The user has `REPLICATION SLAVE`, its own or through an active role, as `SHOW GRANTS` lists it. |
| `cdc.binlog-monitor` | The user can read where the binlog ends. |
| `cdc.retention` | Always, saying how long the server keeps binlog files. |

A check doesn't know which sync modes a pipeline uses, so the `cdc` items only warn there. A pipeline's preflight checks what it reads:

- `stream.NAME.read`: the table is one the source offers, and the user can select from it.
- `stream.NAME.cursor`, for an incremental stream: its cursor is a column of a type listed under [What it reads](#what-it-reads).
- `stream.NAME.cdc`, for a CDC stream: the table has a primary key the source reads in chunks.
- With a CDC stream, the `cdc` items, which fail it but for `cdc.gtid-mode`. There `cdc.retention` fails if the server purges binlog files sooner than the pipeline's schedule goes between syncs.

### Example

The [config schema](./source.schema.json) has every setting. This source offers the tables of `app`, and its pipeline reads `orders` by CDC and `customers` by its `updated_at` column:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-db
spec:
  connector: mysql
  config:
    dsn: { secret: app-db-dsn }
    database: app
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

`dsn` is a DSN as the Go MySQL driver spells it, `USER:PASSWORD@tcp(HOST:3306)/DATABASE`, or a URL, `mysql://USER:PASSWORD@HOST:3306/DATABASE` or `mariadb://…`. It's a secret: store it with `openetl secret set app-db-dsn`.

The connector's sessions are in `utf8mb4`, in UTC, in the SQL mode `STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION`, without a `sql_select_limit`, and with `autocommit` on, whatever the DSN or the server set: each connection sets them last. The DSN's `collation` is the sessions' if it's one of `utf8mb4`'s, any the server has, and is left out otherwise, as its `charset`, `time_zone`, `sql_mode` and character set variables are. Each other parameter the driver doesn't take must set a session variable, such as `wait_timeout=60` or `@@session.wait_timeout=60`; the connector refuses a DSN with one that doesn't, such as a global or a user variable.

### TLS

Set `tls` to connect over TLS, for the source's queries and its binlog connection, and for the destination; without it, the DSN's `tls` parameter decides.

```yaml
config:
  dsn: { secret: app-db-dsn }
  tls:
    mode: verify-identity
    ca: |
      -----BEGIN CERTIFICATE-----
      ...
```

| `mode` | Connects |
|---|---|
| `disabled` | without TLS |
| `preferred` | with TLS if the server has it, without verifying its certificate |
| `required` | with TLS, without verifying the certificate |
| `verify-ca` | with TLS, if the certificate is signed by `ca` |
| `verify-identity` | as `verify-ca`, and if the certificate names the DSN's host; the default |

`ca` holds the PEM certificates of the authorities that sign the server's certificate; without it, the system's are used. A user created `REQUIRE X509` presents a client certificate, `cert`, and its key, `key`, which is a secret. MySQL generates a CA and certificates that name no host, so its own take `verify-ca`; MariaDB makes a certificate signed by itself, which `verify-ca` takes with that certificate as `ca`. Each mode but `verify-identity` is tested connecting to both, and a client certificate to MySQL; `verify-identity` is tested refusing their certificates, which name no host, and isn't tested with one that names it.

### Types

A snapshot and the binlog read each value the same way.

| MySQL or MariaDB | Arrives as |
|---|---|
| `tinyint(1)`, `bool`, `bit(1)` | boolean; as MySQL reads it, any value but 0 is true |
| `tinyint`, `tinyint unsigned`, `smallint`, `year` | 16-bit integer |
| `smallint unsigned`, `mediumint`, `mediumint unsigned`, `int` | 32-bit integer |
| `int unsigned`, `bigint` | 64-bit integer |
| `bigint unsigned` | decimal(20, 0) |
| `float`, `double` | 32- and 64-bit floats |
| `decimal(p,s)` | a decimal of the same precision and scale up to a precision of 38; otherwise decimal(38, s) with s at most 9, and a value with more digits or places than that doesn't fit |
| `char`, `varchar`, the `text` types, `enum`, `set` | string, in UTF-8 from any character set; a `set` as its members joined by commas |
| `binary`, `varbinary`, the `blob` types, `bit(n)` | binary; `bit(n)` as its bits in big-endian bytes |
| spatial types | binary, as MySQL keeps them: a 4-byte SRID, then the geometry's WKB |
| `date` | date |
| `datetime` | timestamp in microseconds |
| `timestamp` | timestamp in microseconds, in UTC |
| `time` | time of day in microseconds; a value below 0 or past `24:00:00`, which `time` holds as a duration, doesn't fit |
| `json`, and MariaDB's `json`, which is `longtext` checked by `json_valid` | JSON, without the spaces MySQL prints |
| MariaDB's `uuid` | UUID |
| MariaDB's `inet4`, `inet6` | string, as MariaDB prints them |

A value that doesn't fit is sent as MySQL prints it, with every digit, which OpenETL can't put in the declared type: by default its row is quarantined, as [Holds and quarantine](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/holds-and-quarantine.md) explains.

A zero date, `0000-00-00`, or one with a zero month or day, which MySQL keeps where its SQL mode allows, arrives as null, so `date`, `datetime` and `timestamp` columns are listed as nullable even where `NOT NULL`. Other columns are listed as nullable unless they're `NOT NULL`.

### Schema changes

A column added to a table read with CDC fails the read once, retryably, and the next attempt declares it. A column that was added and dropped again before the read reached it is left out. A column whose type changed so that the binlog's values no longer fit the type the read declared fails the read the same way. The binlog names columns as they were: a change from before a column was renamed, or added, that the read reaches after it, doesn't have it, and sends it as unchanged, so a merge keeps the value the destination has rather than setting it null. No change carries what the rename or the add did to existing rows, such as an added column's default: the destination has it once the row changes again, or a new snapshot of the table brings it. `TRUNCATE` isn't in the binlog as rows: the source logs a warning, and the destination keeps the rows; a new snapshot of the table brings it back in step.

### Delivery

Full refresh and CDC streams replay exactly: a read resumed from a checkpoint yields the same rows in the same order. OpenETL counts incremental streams as ones that may repeat rows, because of the rows at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

A read OpenETL asks to end at an earlier resume point (`until`), to read a part again, sends nothing past it, then that resume point, and completes there. A full refresh or incremental read sends the rows between the two resume points again, as the table has them now. A CDC read from the same server sends the binlog's changes again as they were, with the same sequence numbers; the snapshot's rows of that part come as the table has them now, after those changes and before any later one, so a merged stream lands as it did, and an appended one has the rows as they are now. From another server, by GTIDs, it sends the same changes with the new server's sequence numbers, which are above every one the first read sent, and the snapshot's rows after them.

### Limits

- Nothing is held on the server for a pipeline, so there's nothing to acknowledge, set up or tear down: binlog retention is the server's (see above).
- A table without a primary key, or whose key has a boolean (`tinyint(1)` or `bit(1)`), `float`, `double`, `enum`, `set`, `bit`, `json` or spatial column, can't be read with CDC, and is read whole in one query.
- A `vector` column, or text in a character set without a decoder (such as `dec8` or `swe7`), makes the table unreadable; the error names the column.

## Destination

### What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, which is a database, or else in the config's `database`. If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. Names are up to 64 characters. The database is created if it's missing.

Tables are InnoDB, in `utf8mb4` with its binary collation (`utf8mb4_0900_bin` on MySQL, `utf8mb4_nopad_bin` on MariaDB), so keys that differ only in case, accents or trailing spaces stay apart.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. A column a version didn't send, which a source lists in `_openetl_unchanged`, takes its value from the key's latest earlier version in the epoch that sent it, unless the target's row is newer than that version, or else keeps the target's. Among an epoch's versions of a key, one without `_openetl_seq` ranks below one with it. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true, and the values it had before the epoch, so an update in the delete's own epoch doesn't land; a key that's new in the epoch takes the delete's values. A hard delete leaves nothing of the row to order by, so a version older than the delete that comes in a later epoch puts the row back. Merge adds a unique index on the key's columns if the table has none; it fails, saying so, if the table has two rows with one key.

Each epoch is staged into tables of its own, one per stream and writer lane. MySQL commits before each DDL statement, so the destination makes every table, column and index an epoch needs first, then applies its rows in one transaction that only changes rows: it checks the fence row in `_openetl_fence`, applies each stream, and writes a marker per stream in `_openetl_loads`, all or nothing. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` and `_openetl_loads` in place.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Set it up

The user in `dsn` needs `CREATE`, `ALTER`, `DROP`, `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the database, for its tables, their staging tables, `_openetl_fence` and `_openetl_loads`, and `CREATE` on it, globally or by a pattern, if it doesn't exist yet. The destination adds indexes with `ALTER TABLE`, so it needs no `INDEX`.

```sql
GRANT CREATE, ALTER, DROP, SELECT, INSERT, UPDATE, DELETE ON raw.* TO 'openetl'@'%';
```

The check asks for all of these on the whole database, whatever the write mode: each write creates, indexes and drops staging tables in it, updates `_openetl_fence`, and deletes old markers from `_openetl_loads`. Grants on single tables don't count.

A connection's check has the source's `net.dns`, `net.connect`, `db.tls` and `db.connect`, and `database.create`: the user has those privileges on `database`, or, if it doesn't exist, may create it. The user's privileges are read from `SHOW GRANTS`, which lists those of its active roles too. A pipeline's preflight checks `database.create` on the database it writes to, and `stream.NAME.write` for each stream, which fails for a merged stream without a primary key.

### Example

The [config schema](./destination.schema.json) has every setting; `tls` is the source's, as [TLS](#tls) explains.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: mysql
  config:
    dsn: { secret: warehouse-dsn }
    database: raw
```

### Types

| Arrives as | Column |
|---|---|
| boolean | `tinyint(1)` |
| 8-, 16-, 32- and 64-bit integers | `tinyint`, `smallint`, `int`, `bigint` |
| unsigned integers | the same, `unsigned` |
| 32- and 64-bit floats | `float`, `double`; NaN and infinities, which they don't hold, as null |
| decimal(p, s) | `decimal(p,s)` up to MySQL's 65 digits and scale of 30; `longtext` past them |
| string | `longtext` |
| binary | `longblob` |
| date | `date` |
| time of day | `time(6)`; nanoseconds are cut to microseconds |
| timestamp | `datetime(6)` in UTC, with or without a time zone, since `timestamp` ends in 2038; nanoseconds are cut to microseconds |
| UUID | `char(36)` |
| JSON, intervals, lists, structs and maps | `json`; on MariaDB `longtext` checked by `json_valid` |

A key's string and binary columns are `varchar` and `varbinary` instead, sized together to fit InnoDB's index; a longer key value fails its epoch. A date or timestamp outside the years 1 to 9999 fails its epoch too.

### Schema changes

New columns are added to the table, until it has the 1,017 columns InnoDB allows; past that, the destination refuses the column.

The destination doesn't change an existing column's type by itself, since `ALTER TABLE … MODIFY` can rebuild the table. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change, with the column's `NOT NULL` and default:

```sql
ALTER TABLE `raw`.`orders` MODIFY COLUMN `amount` decimal(20,2) NOT NULL DEFAULT '0.00'
```

Run it, then release the hold. Or let the destination make such changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. It then widens integers to wider integers, an unsigned one to a wider signed one, `float` to `double`, integers to `double` and the small ones to `float`, integers and decimals to a `decimal` that holds them, a `varchar` or `varbinary` to a longer one, binary to `longblob`, and anything else to `longtext`. A change that isn't a widening is refused either way.

A schema change waits up to 5 seconds for its table's metadata lock, and tries up to 5 times in all, so queries on the table don't queue behind it.
