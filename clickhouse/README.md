---
title: ClickHouse
description: Read ClickHouse tables in full or by a cursor column, in sorting-key order, and write into ClickHouse by appending, overwriting or merging, each epoch exactly once.
---

The `clickhouse` package has a source and a destination, both over ClickHouse's native protocol. The source reads the tables of one database, whole or from where a cursor column last got to. The destination writes each stream into a table of its own and applies each epoch exactly once, in steps that readers can see one at a time: see [How a commit works](#how-a-commit-works).

Both are tested against a single ClickHouse 26.8 server. Nothing was tested on a cluster: see [Clusters](#clusters).

## Source

### What it reads

Each table and view in the configured `database` is a stream, or only the ones `tables` lists. Materialized views aren't offered, as their rows are in the tables they write to. Columns defined as `ALIAS` or `EPHEMERAL` aren't read.

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table | The whole table each time. See below for the order and the chunks. |
| `incremental` | Tables with a column of an integer, float, `Decimal`, `Date`, `Date32`, `DateTime`, `DateTime64`, `String` or `FixedString` type | The rows whose `cursor` column is at or above where the last read got to, up to the column's highest value when this read starts, in chunks ordered by the cursor, then the sorting key. |

ClickHouse keeps no change log, so there is no `cdc`: a pipeline that asks for it fails with `CONFIG_INVALID`. Deleted and updated rows aren't seen as such; a full refresh reads the table as it is.

A table of the MergeTree family whose sorting key starts with plain columns that can't be null, of a type other than a float or `UUID`, is read in the order of those leading columns, `chunkRows` rows a query, and a stopped read resumes after its last chunk. A chunk takes every row that ties with its last one, so a sorting key that isn't unique still resumes exactly. A float isn't one of them, as a NaN compares false with every value, so a read can't resume after one. Other tables, such as one with `ORDER BY tuple()`, a `Nullable`, float, `UUID` or computed first key column, a view, or a `Log` or `Memory` table, are read in a single query.

A table whose engine merges rows by their sorting key, `ReplacingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CoalescingMergeTree` or `GraphiteMergeTree`, is read with `FINAL`, as merged. For `Replacing`, `Summing`, `Aggregating` and `Coalescing` tables whose sorting key is all such columns, `openetl discover` gives the sorting key as the stream's primary key. Other tables have none.

An incremental read starts at the last read's highest cursor value, not after it, so the rows that had that value are read again. Rows whose cursor is `NULL`, or a float's NaN, are never read.

A keyed table can be read in splits, ranges of its sorting key's first column cut at the values its primary index holds for its granules, up to 32 of about `chunkRows` rows each. A running split's remainder can be divided again, at the middle of the index's values in it. A table of a few granules isn't split. A read holds up to `maxConnections` connections, one for each lane it runs.

A read the platform redoes up to a resume point stops there.

### Set it up

The user needs `SELECT` on each table the source offers. ClickHouse lists only the tables a user has some privilege on, so a table it has none on isn't a stream.

```sql
GRANT SELECT ON analytics.* TO openetl;
```

The source changes no setting, so a user whose profile has `readonly = 1` can read: the platform's time limit on a check or a read cancels its query, rather than going to the server as `max_execution_time`, which such a user couldn't set. The user's profile's limits apply to each of the source's queries: a table read in one query, or a chunk, that reads past `max_rows_to_read` or `max_bytes_to_read`, or runs past `max_execution_time`, fails, and fails the same way when it's retried. Give the user a profile without them, or with room for the largest table read in one query.

The source's check is a list of items, each with what failed and how to fix it: the server's name resolves and its port answers (`net.dns`, `net.connect`), with TLS when `tls` is set (`net.tls`), the user signs in (`db.connect`), the server's version (`server.version`, which warns before 26.8, the version the connector is tested on), the database is there (`database.found`), each table in `tables` is (`tables.found`), and the user has `SELECT` on every table offered (`tables.read`).

Before a pipeline runs, its preflight looks at each stream as the pipeline reads it: the table or view is there and the user can select from it (`stream.NAME.read`); a full refresh says whether it reads in sorting-key order, resumably, or in one query (`stream.NAME.order`); an incremental read's cursor is a column of a type the source reads by (`stream.NAME.cursor`); and a stream read with `cdc` or `stream` fails (`stream.NAME.mode`), as ClickHouse keeps no change log.

### Example

The [config schema](./source.schema.json) has every setting. This source offers the tables of `analytics`, and its pipeline reads `events` by its `at` column:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: clickhouse-analytics
spec:
  connector: clickhouse
  config:
    addr: clickhouse.internal:9440
    tls: true
    user: openetl
    password: { secret: clickhouse-password }
    database: analytics
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: analytics-to-lake
spec:
  schedule: { every: 1h }
  source:
    ref: clickhouse-analytics
    streams:
      - name: events
        sync: incremental
        cursor: at
  destination: { ref: lake }
```

`addr` is the native protocol's `host:port`: 9000, or 9440 with `tls`.

### Types

| ClickHouse | Arrives as |
|---|---|
| `Bool` | boolean |
| `Int8`, `UInt8`, `Int16` | 16-bit integer |
| `UInt16`, `Int32` | 32-bit integer |
| `UInt32`, `Int64` | 64-bit integer |
| `UInt64` | `decimal(20, 0)` |
| `Int128`, `UInt128`, `Int256`, `UInt256` | string of the number's digits, as they can have more than a decimal's 38 |
| `Float32`, `BFloat16`, `Float64` | 32- and 64-bit floats |
| `Decimal(p, s)` | a decimal of the same precision and scale up to 38 digits; a wider one as a string of its digits |
| `String` | string; bytes that aren't UTF-8 become U+FFFD |
| `FixedString(n)` | binary |
| `UUID` | UUID |
| `Date`, `Date32` | date |
| `DateTime`, `DateTime64(p)` | timestamp in microseconds, in UTC: the same instant, whatever the column's time zone. A `DateTime64(7)` to `(9)` value arrives as text with all its digits, which the engine parses into microseconds, or nanoseconds for a stream that keeps them. |
| `Time`, `Time64(p)` | time of day in microseconds, `Time64`'s digits past the sixth cut; a value outside a day arrives as text, which the engine quarantines |
| `Enum8`, `Enum16`, `IPv4`, `IPv6` | string; an IPv4-mapped `IPv6` as `::ffff:1.2.3.4` |
| `LowCardinality(T)`, `SimpleAggregateFunction(f, T)` | as `T` |
| `Nullable(T)` | as `T`, nullable |
| `Array(T)` | list of `T` as above |
| `Map(K, V)`, `Tuple(...)`, an `Array` of `Time`, `Time64` or a `DateTime64(7)` to `(9)`, `JSON`, `Dynamic`, `Variant`, geo types such as `Point`, and anything else, anywhere in a column's type | JSON, as `toJSONString` gives it |

In that JSON, such as a `Map`'s, a `Tuple`'s or a `DateTime64(7)` to `(9)` array's, a date and time is wall-clock text in the column's time zone, or the server's if it has none, with no UTC offset: `"2026-11-03 10:00:00.123456789"`. A float's NaN or infinity is `null` there, unless the user's profile turns `output_format_json_quote_denormals` on.

As a `DateTime64(7)` to `(9)` column, and a `Time` or `Time64` one, can arrive as text, a read whose run didn't declare such a column, as one added to the table since, fails before it reads a row, with an error the platform retries, and the next attempt declares it. So does a read of a column whose type changed since the run declared it, as `ALTER TABLE … MODIFY COLUMN` from `Int32` to `Int64` does, whatever its type. That needs an OpenETL that gives a read the types the run declared.

### Delivery

Full refresh streams replay exactly: a read resumed from a checkpoint yields the same rows. OpenETL counts incremental streams as ones that may repeat rows, because of the rows at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

## Destination

### What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, a database, or else in the config's `database` (`default` unless set). The database is created if it's missing. The table's name is the one the pipeline gives the stream, exactly as written.

| Write mode | Table | What a commit does |
|---|---|---|
| `append` | `MergeTree`, ordered by the stream's primary key if it has one, else `tuple()` | Inserts the epoch's rows. |
| `overwrite` | as `append` | Writes a read's rows to a table of its own, whose partitions replace the target's when the read ends, so the target keeps its old rows until then. An empty read empties it. |
| `merge` | `ReplacingMergeTree(version, _openetl_is_deleted)`, ordered by the primary key | Inserts each key's latest version in the epoch, by `_openetl_seq`. |

> [!IMPORTANT]
> A merged table holds every version of a key until ClickHouse merges its parts, which it does in the background, when it chooses. Read it with `FINAL`, or with the setting `final = 1`, to see one row per key, its latest, and no row a hard delete removed:
>
> ```sql
> SELECT * FROM analytics.orders FINAL
> ```

The version is `_openetl_seq` where it's an integer, a `Date` or a `DateTime64`. Where it's a `Decimal` of scale 0, as Postgres's CDC gives it, the version is a column `_openetl_version` that ClickHouse computes from it. Either way an older version never wins over a newer one, even when it arrives later. A table whose `_openetl_seq` is of another type has no version, and the version inserted last wins; such a stream can't take hard deletes.

Deletes are soft unless the pipeline sets `destination.deletes: hard`. A soft delete keeps the row, with `_openetl_deleted` true and its other columns as the table had them. A hard delete sets `_openetl_is_deleted` to 1, and `FINAL` leaves the row out; the row stays in the table's parts, as the destination doesn't ask ClickHouse to remove it. A column an update didn't send, which `_openetl_unchanged` names, keeps the value the table has for it.

Append and overwrite skip rows a source sends again, by their `_openetl_id`, when the pipeline asks: a table that does has a `bloom_filter` index on `_openetl_id` for that lookup.

### How a commit works

ClickHouse has no transactions across statements, so a commit is built from steps that are each atomic, or that do nothing the second time:

1. Each writer lane inserts its rows into staging tables of its own, `_openetl_stage_…`, in column batches straight from Arrow. It keeps a stream's batches until it has 32 MiB of them, and inserts its largest stream's first when it keeps 256 MiB of all its streams'.
2. The commit builds the stream's part of the epoch, as the target will hold it, into a new table, and renames it `_openetl_ready_…` once it's whole.
3. It moves that table's partitions into the target with `ALTER TABLE … MOVE PARTITION`. A partition moves at once; one moved already isn't there to move again. A table the destination creates has one partition, so its part of the epoch moves at once; a table its user made with `PARTITION BY` takes it partition by partition.
4. It applies the epoch's patches, if the stream has any, in one `ALTER TABLE … UPDATE` ([Holds](#holds)).
5. It writes the stream's marker in `_openetl_loads`, and drops the staging and ready tables.

A fence in `_openetl_fence`, the pipeline's highest generation, stops a replaced attempt: a commit reads it before step 2 and again before step 3, and stops if a newer attempt has raised it. Nothing holds it between a read and the steps after, so an attempt replaced just after one can still run them. For `append` and `merge` streams they're the same epoch's steps, under the same names and marker, that the newer attempt redoes, and a partition moved or a patch applied once isn't again. An `overwrite` stream's commit also drops its stream's other read tables and, when its read has ended, replaces the target's partitions with its read's, both after the last read of the fence. It drops no read a newer attempt started, and a replace that finds the read's table gone after the newer attempt applied the epoch changes nothing. An attempt's recovery drops the read tables of the streams it doesn't overwrite, but no read a newer attempt started. It reads the fence again before each drop, which narrows the window but doesn't close it: a newer attempt that raises the fence between that read and the drop, and overwrites the stream with a read an earlier attempt started, still has that read's table dropped. Its next commit with rows of the stream makes the table again, empty, so when the read ends the target holds only the rows committed since the drop, or none if there were none. A commit that had built its epoch's table before the drop fails to move it, and so does each later attempt's recovery of the epoch. If a commit is cut off anywhere, the next attempt finishes it from the step it reached, so each epoch lands exactly once, except where a recovery's drop of a read table races a newer attempt as above. These steps aren't one transaction: readers can see a stream's rows before its patches, and each stream's part of an epoch becomes visible on its own, not together with the other streams'. Leave `_openetl_fence` and `_openetl_loads` in place; tables named `_openetl_stage_…`, `_openetl_ready_…` and `_openetl_read_…` are the destination's, and a later attempt drops the ones left behind.

A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Set it up

The user needs these on the database, and `CREATE DATABASE` if it doesn't exist yet, and `SELECT` on `system.parts` and `system.mutations`, which a commit reads for the parts it moves and the patch it stops when one fails:

```sql
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ADD COLUMN, ALTER MODIFY COLUMN,
  ALTER MOVE PARTITION, ALTER DELETE, ALTER UPDATE ON analytics.* TO openetl;
GRANT SELECT ON system.parts, SELECT ON system.mutations TO openetl;
```

`ALTER TABLE` on the database covers every `ALTER` there. The destination sets `async_insert = 0` and `mutations_sync = 2` on its queries, so a write is in a part, and a change done, before it goes on, and `join_use_nulls = 0` and `max_partitions_per_insert_block = 0` when it builds an epoch's table, so that its rows land in any number of a table's partitions. The user's settings profile must let it change them, with `readonly = 0`, which writes need anyway.

The destination's check is a list of items, each with what failed and the `GRANT` or setting that fixes it: the server's name resolves and its port answers (`net.dns`, `net.connect`), with TLS when `tls` is set (`net.tls`), the user signs in (`db.connect`), the server's version (`server.version`), the database is there or the user may create it (`database.create`), the user's grants, checked with `CHECK GRANT`, to create and drop tables (`grant.tables`), insert and select (`grant.rows`), add and widen columns (`grant.columns`), move, replace and drop partitions (`grant.partitions`), and fill held cells (`grant.update`), its `SELECT` on the system tables (`grant.system`), and that it may set what the destination's queries set (`settings.write`). A user whose profile has `readonly` set fails each grant, whatever its grants, as it can write nothing.

Before a pipeline runs, its preflight checks the same of the pipeline's namespace, and looks at each stream as it would be written (`stream.NAME.write`): a merged stream needs a primary key; a merged stream whose `_openetl_seq` can't version a `ReplacingMergeTree` warns, and fails if it hard-deletes; an existing table whose sorting, primary or partition key column the stream would change the type of warns, as the destination refuses that change; and a database whose name has a dot warns, as it can't take patches of held cells.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: clickhouse
  config:
    addr: clickhouse.internal:9440
    tls: true
    user: openetl
    password: { secret: clickhouse-password }
    database: raw
```

### Types

| Arrives as | ClickHouse column |
|---|---|
| boolean | `Bool` |
| integers | `Int8` to `Int64`, `UInt8` to `UInt64` |
| 16- and 32-bit floats, 64-bit floats | `Float32`, `Float64` |
| decimal(p, s), up to 76 digits | `Decimal(p, s)`; with a negative scale, whose values are multiples of a power of ten, `Decimal(p - s, 0)`, and with a scale past p, `Decimal(s, s)` |
| string, binary | `String` |
| fixed-size binary | `FixedString(n)` |
| date | `Date32`, clamped to its range, 1900-01-01 to 2299-12-31 |
| timestamp | `DateTime64(0, 3, 6 or 9, 'UTC')` by its unit, clamped to 1900-01-01 to 2262-04-11 23:47:16.854775807, as the driver counts in nanoseconds |
| time of day | `Time64(0, 3, 6 or 9)` |
| duration | `Int64`, a count of its unit |
| month interval | `Int32`, a count of months |
| day-time, month-day-nano interval | `Tuple(months Int32, days Int32, nanoseconds Int64)`; a null lands as zeroes |
| list | `Array(T)`; a null list lands as an empty one |
| map | `Map(K, Nullable(V))` |
| struct | `Tuple(name Nullable(T), ...)`; a null struct lands with every field null |
| dictionary | `LowCardinality(Nullable(T))`, or `LowCardinality(T)` where `T` can't be `Nullable` |
| UUID | `UUID` |
| JSON | `String` |

Each column is `Nullable` but for the metadata columns, a merge's key, and the types ClickHouse can't make `Nullable`: arrays, maps and tuples.

### Schema changes

New columns are added to the table, in one `ALTER TABLE` per table.

The destination doesn't change an existing column's type by itself, as `MODIFY COLUMN` rewrites the column's data. It refuses the change, the stream holds the column where it can, and the refusal gives the statement that makes the change:

```sql
ALTER TABLE `raw`.`orders` MODIFY COLUMN `amount` Nullable(Int64)
```

Or let it make such changes itself with the pipeline's `destination.typeChanges: allow`. It then widens integers to wider ones, an unsigned one to a wider signed one, integers of up to 16 and 32 bits to `Float32` and `Float64`, `Float32` to `Float64`, a `Decimal` to one that holds it, a `DateTime64` to a finer one in the same time zone, `Date32` to `DateTime64`, but neither to `DateTime64(9)`, whose range ends at 2262-04-11, `Time64` to a finer one, an array of a type to an array of a wider one, and anything but an array, map or tuple to `String`. Other changes, which can lose values, are refused either way, with the statement that makes them. A column in the table's sorting, primary or partition key is refused with no statement, as ClickHouse doesn't change those columns' types.

A column its user made non-`Nullable` stays so, and the destination tells the platform, which holds no cell there unless the pipeline gives a held value.

### Holds

The destination takes holds on `merge` and `append` streams, as [Holds and quarantine](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/holds-and-quarantine.md) explains: a held cell lands as null, and a patch fills it in later.

A table that takes held cells has a column `_openetl_held`, a `Map(String, String)` from each held column to the hex of the `_openetl_id` of the version whose value is held. A patch fills a cell only where the row still holds the version the patch comes from: by the key in a `merge` table, and by `_openetl_id` in an `append` one. So a patch from an older version, one repeated, or one for a key or ID the table doesn't have changes nothing.

- In a `merge` table, a held cell of a column marked to keep its last value keeps the table's value until its patch comes. A soft delete keeps the row's held cells, which the held version's patch still fills.
- A commit applies an epoch's patches after its rows, as one `ALTER TABLE … UPDATE` of the rows that hold the patched versions, which waits until it's done. Each patch looks its version up in a `Join` table of the epoch's patches. A patch also sets the row's `_openetl_synced_at` to its own. A patch column whose `openetl.fills` names another column, such as `col__v2` for `col`, fills its own cell for that column's hold. Run again after a commit is cut off, it fills nothing more. Readers can see the epoch's rows before its patches.
- A column in the table's sorting, primary or partition key can't be held, as `UPDATE` can't change it.

A patch needs `ALTER UPDATE`, which the grants above include.

### Clusters

Only a single server was tested. The destination creates plain `MergeTree` and `ReplacingMergeTree` tables, with no `ON CLUSTER`, and keeps its fence and markers in such tables, which are each server's own. On a cluster behind a load balancer, attempts reaching different servers would see different fences and markers, so neither its tables nor its exactly-once delivery are claimed there. The source reads through one server; reading `Replicated` or `Shared` tables, or `Distributed` ones, which are read in a single query, wasn't tested.

### Limits

- A patch rewrites the table's parts that hold a patched row, as any `ALTER TABLE … UPDATE` does.
- The database's name can't have a `.` in it on a stream that takes patches, as `joinGet` takes its table as `database.table`.
- An epoch isn't visible all at once: each stream's part becomes visible on its own, its rows before its patches, and in a table partitioned by its user, partition by partition, as nothing in ClickHouse moves several tables' or partitions' rows, or changes them, at once.
- A replaced attempt's recovery that drops a read table just as a newer attempt raises the fence and goes on with that read loses the read's rows, or fails every later commit and recovery of the stream, as the fence section above says. It takes a stream that one attempt overwrites and the attempt before it doesn't, and two attempts running at once.
- JSON lands as a `String`, not ClickHouse's `JSON` type.
- A timestamp outside 1900 to 2262-04-11 and a date outside 1900 to 2299 are clamped to the range; a null day-time or month-day-nano interval, list, map or struct isn't kept as null.
- Table and column names can be up to 160 bytes. ClickHouse takes a table name of up to 213 bytes less its database's name, so this leaves 53 for that.
