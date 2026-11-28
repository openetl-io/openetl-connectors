---
title: MongoDB
description: Read MongoDB collections in full, by a cursor field, or change by change through change streams, and write into MongoDB collections by appending, overwriting or merging.
---

The `mongodb` package has a source and a destination. The source reads the collections of one database: whole, from where a cursor field last got to, or as a stream of every insert, update, replace and delete through change streams (CDC). The destination writes each stream into a collection of its own.

## Source

### What it reads

Each collection in the configured `database` is a stream, or only the ones `collections` lists. Views, time series collections and `system.*` collections aren't offered. Every stream's primary key is `_id`.

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every collection | The whole collection each time, in `_id` order, `chunkDocuments` documents a query; a stopped read resumes after its last chunk. |
| `incremental` | Every collection, on a field the pipeline names as its `cursor` | The documents whose cursor field is at or past the last read's highest value, up to the field's highest value when this read starts, in `_id` order. |
| `cdc` | Every collection, when the deployment is a replica set or a sharded cluster | First a snapshot of the collection, then every change from the collection's change stream. |

A collection of more than `chunkDocuments` documents read in full or incrementally is offered in up to 32 `_id` ranges, which OpenETL can read in parallel. The ranges are cut at the `_id`s of a random sample, in the order of the `_id` index, which orders `_id`s of different types too: numbers, strings, documents, ObjectIds, dates and the rest are all read once. When OpenETL asks a range that's behind to split, it cuts what it has left at the median of a sample of its `_id`s, at its next resume point.

An incremental read's range ends at the cursor field's highest value in MongoDB's order, which puts dates above strings and strings above numbers, and compares the field as a query does: only values of that value's type match, any number for a number. Documents whose field has another type, or is missing or null, aren't read. A read starts at the last read's highest value, not after it, so the documents with that value are read again. Deleted documents aren't seen: CDC is the mode that sees deletes.

### CDC

A CDC read takes the deployment's cluster time when it starts, reads the collection in `_id` order, with majority reads that wait until the majority has every change up to that time, and then reads the change stream from just after it. A change made while the snapshot runs can be read in the snapshot and again from the change stream; the change's seq is higher, so a destination that merges keeps it.

| Change | Arrives as |
|---|---|
| insert | `insert`, with the document |
| update, replace | `update`, with the whole document: its post-image where the collection keeps pre- and post-images, or else the document as it is when the change is read, by a majority read, so never a write that may be rolled back. An update of a document since deleted is skipped then, and its delete follows. |
| delete | `delete`, with the `_id`, or with the deleted document's fields where the collection keeps pre-images |
| drop, rename, dropDatabase, invalidate | an error, `POSITION_LOST`, that names what happened; reading the collection again takes a new snapshot |

To have updates arrive as they were made, and deletes with their documents, turn on the collection's pre- and post-images, which needs MongoDB 6.0 or later:

```js
db.runCommand({collMod: "orders", changeStreamPreAndPostImages: {enabled: true}})
```

A record's seq, `openetl.seq`, is a `decimal(38, 0)`: the change's cluster time, its seconds and increment, times 2^32, plus its place among the changes of that cluster time, as a transaction's changes share one. The snapshot's documents take the read's starting time with place 0.

The state is the change stream's resume token. A read sends a resume point after every `chunkDocuments` changes, or sooner once their documents come to about 4 MB of text, and whenever it has read all the changes there are; a continuous read of a quiet collection moves its resume point to where the change stream has looked every 10 seconds, so its position doesn't fall out of the oplog.

The oplog keeps changes for as long as its size, or `oplogMinRetentionHours`, lets it. A pipeline stopped for longer, or a snapshot that takes longer, loses its position: the read fails with `POSITION_LOST`, which OpenETL doesn't retry, and reading the collection again takes a new snapshot. OpenETL asks the source how close each CDC stream is to that: it answers with how far the stream's position is behind the oplog's newest entry, and the time between its position and the oplog's oldest entry, which is about how long the oplog keeps it at the pace it's been written. It answers only for a user who can read `local.oplog.rs`.

### Fields and types

Documents have no schema, so discovery reads every document of each collection and declares each top-level field with the BSON types its values have. Every other field is nullable, and a field that's missing and one that's null are both null.

OpenETL runs discovery at the start of every sync, so its read is part of each sync's time. It's an aggregation on the server, which MongoDB runs on one thread. A collection of more than `chunkDocuments` documents is read in up to 32 `_id` ranges, as a read splits it, an aggregation each, where the server bounds each range's scan: on MongoDB 5.0 or later, by the `_id` index, and for a clustered collection, which has none, by its clustered scan, on 6.0.17 or a later 6.0, 7.0.13 or a later 7.0, and 7.3 or later. Elsewhere each range would scan the whole collection, so the collection is one aggregation. Up to `discoveryConnections` aggregations run at once, 4 by default, over every collection's ranges and the collections read whole. More finish sooner, and take more of the server's cores while they run; 1 reads each collection in one aggregation, one collection at a time. `sampleSize` reads that many random documents of each collection instead, which takes the server little time, and can miss rare fields and types, as the end of this section says. `packed` reads no documents: see [Packed documents](#packed-documents).

`_id` is the first column, a string that isn't nullable. Its text is different for every two `_id`s a collection can hold, whatever their types, and the same in every sync mode, so that a destination that merges by key keeps each document apart:

| `_id` | Sent as |
|---|---|
| ObjectId | its 24 hexadecimal digits, `65f0a1b2c3d4e5f601234567` |
| any other type | canonical Extended JSON, which tags each type: `5` is `{"$numberInt":"5"}`, `"5"` is `"5"` with its quotes, a long is `{"$numberLong":"…"}`, a null `_id` is `null` |

Canonical Extended JSON never spells 24 bare hexadecimal digits, so an ObjectId and the string of its digits are two keys too. MongoDB's `_id` index takes numbers of equal value for one key, `5` and `NumberLong(5)` among them, so a collection holds at most one of those. The [destination](#destination) writes these keys back as their BSON values with `extendedJSONKeys`.

| BSON type of a field's values | Declared as | Sent as |
|---|---|---|
| `bool` | boolean | `true`, `false` |
| `int`, `long` | 64-bit integer | the number |
| `double` | 64-bit float | the number, `NaN`, `inf` or `-inf` |
| `int` or `long`, and `double` | 64-bit float | the number |
| `decimal` | `decimal(38, 9)` | the number, or `NaN`, `Infinity` or `-Infinity` |
| `int` or `long`, and `decimal` | `decimal(38, 9)` | the number |
| `string`, `symbol` | string | the text |
| `objectId` | string | 24 hexadecimal digits |
| `date` | timestamp in microseconds, UTC | RFC 3339 with milliseconds |
| `binData` of subtype 4 and 16 bytes | UUID | its canonical text |
| other `binData` | binary | base64 |
| `object`, `array`, `regex`, `timestamp`, `javascript`, `javascriptWithScope`, `dbPointer`, `minKey`, `maxKey` | JSON | relaxed Extended JSON, such as `{"a":[1,"x"]}` or `{"$timestamp":{"t":1700000000,"i":3}}` |
| types that declare differently, such as `string` and `int`, or `double` and `decimal` | string | each value as above |
| only `null` or `undefined` | string | null |

Each value is sent as text, and OpenETL parses it into the column's declared type. Before it's sent, a read checks the BSON type of each top-level value against its column's declared type, which takes the types that discovery, having seen them as well, would still declare the column as:

| Declared as | Takes |
|---|---|
| 64-bit integer | `int`, `long` |
| 64-bit float | `int`, `long`, `double` |
| `decimal(38, 9)` | `int`, `long`, `decimal` |
| boolean | `bool` |
| timestamp | `date` |
| UUID | `binData` of subtype 4 and 16 bytes |
| binary | other `binData` |
| JSON | `object`, `array`, `regex`, `timestamp`, `javascript`, `javascriptWithScope`, `dbPointer`, `minKey`, `maxKey` |
| string | every type |

Every column takes `null` and `undefined`, and a missing field. A nested document or array is one JSON value, whatever it holds, so only top-level fields are checked. A value of another type, such as a `string` written into a field of integers after discovery ran, fails the read with a retryable error that names the collection, the field, its declared type and the value's BSON type, but not the value, and the next attempt's discovery declares the type that takes it, string in that case. A read whose request doesn't give the columns' types checks none.

A value of a type its column takes that still doesn't fit is quarantined: a date before year 1 or after 9999, and a decimal that's `NaN` or infinite, or that `decimal(38, 9)` doesn't hold exactly, with more than 9 digits after the point or 29 before it. A long in a field with doubles loses the digits a double doesn't keep. Nested documents and arrays are relaxed Extended JSON, which tags ObjectIds, decimals, binaries, dates and the other types JSON hasn't, but spells an `int` and a `long` alike, as a JSON integer, and a `double` as a number with a point, such as `1.0`. `packed` keeps every type, in canonical Extended JSON.

A document with a field the run didn't declare fails the read with a retryable error that names the field, and the next attempt's discovery declares it. A CDC read leaves out such a field when no document of the collection has it any more, as a change from before the field was removed can, since discovery wouldn't declare it. In the same way, it sends a value of a type its column doesn't take when no document has a value of that type in the field any more, and OpenETL quarantines it, rather than failing a read that the next discovery would fail again. A delete leaves out a field the run didn't declare and a value its column doesn't take, whatever the other documents have, since its values only say what it deleted. To ask whether a document has a field whose name has a `.` or starts with `$`, or a value's type in a field of such a name or an empty one, it uses `$getField`, which takes the name as it is, and which needs MongoDB 5.0 or later.

With `sampleSize`, the next discovery can miss the field again, and the read fails again, until a sample has a document with it. Nothing carries the field from the failed read to the next discovery: OpenETL gives discovery the config alone, the source keeps nothing between calls, and it only reads from MongoDB. The error says so: set `sampleSize` to 0, or read the collection `packed`. A value of a type its column doesn't take, which a sample can miss again just as well, doesn't fail a read with `sampleSize`: the read sends it, and OpenETL quarantines it.

### Packed documents

With `packed: true`, every stream has two columns:

| Column | Declared as | Holds |
|---|---|---|
| `_id` | string | the `_id`, as above |
| `data` | JSON | the whole document, `_id` included, in canonical Extended JSON, which keeps every type: `{"_id":{"$numberInt":"1"},"at":{"$date":{"$numberLong":"1767225600000"}}}` |

Discovery reads no documents, and a read never fails on a new field or a value's type, so collections whose fields vary without bound work. All three sync modes work: an incremental read's `cursor` is a top-level field of the documents, which needn't be a column. A CDC delete's `data` is the deleted document where the collection keeps pre-images, and null otherwise.

### Set it up

The user in `uri` needs the `read` role on the database. CDC needs a replica set or a sharded cluster; to report how close CDC streams are to losing their position, the user also needs `read` on the `local` database. Creating the user:

```js
db.getSiblingDB("admin").createUser({user: "openetl", pwd: "…", roles: [
  {role: "read", db: "app"},
  {role: "read", db: "local"},
]})
```

### Check and preflight

The source's check and a pipeline's preflight report an item for each thing they look at, with a fix for what fails. An item runs only if the items it needs passed or warned. The check is about the connection:

| Item | What it checks |
|---|---|
| `net.dns`, `net.connect` | Each host of the `uri`, or of its SRV record, resolves and takes a connection. With several hosts, each has items of its own, named after it, that only warn, since the driver needs one to find the rest. |
| `net.tls` | The TLS handshake succeeds with a certificate the `uri`'s TLS options trust. Only when the `uri` asks for TLS, as `mongodb+srv://` does unless it sets `tls=false`. It warns when the certificate expires within 14 days. |
| `db.connect` | The user signs in. A `uri` with no user connects without signing in, and leaves what it may do to the server. |
| `collections.found` | Every collection `collections` names is a collection in the database, not a view, a time series or a `system.*` collection. Only when `collections` is set. |
| `db.read` | The user has the `find` action on each collection the source offers, as the `read` role gives. |
| `cdc.replica-set`, `cdc.watch` | The deployment is a replica set or a sharded cluster, and the user has the `changeStream` action on the collections. These only warn: CDC is a pipeline's choice. |
| `cdc.images` | Warns of the collections that keep no pre- and post-images. |
| `cdc.oplog` | Reads the oplog's window, the time between its oldest and newest entries. It warns when the user can't read `local.oplog.rs`. |

A preflight is about the streams a pipeline reads:

| Item | What it checks |
|---|---|
| `stream.NAME.read` | The stream is a collection in the database, not a view, a time series or a `system.*` collection, and the user has `find` on it. |
| `stream.NAME.cursor` | An incremental stream names a cursor field. It warns when no index starts with the field, since each read then sorts and scans the collection. |
| `cdc.replica-set`, `cdc.watch` | As in the check, for the collections read with CDC, and they fail. |
| `cdc.images` | As in the check, for the collections read with CDC. It only warns: CDC works without them, as [CDC](#cdc) says. |
| `cdc.oplog` | As in the check, and it warns when the oplog's window is shorter than the longest a scheduled pipeline goes between syncs, since a stream's position may then be dropped before the next sync. It only warns: the window moves with the rate of writes. |

### Example

The [config schema](./source.schema.json) has every setting. This source offers the collections of `app`, and its pipeline reads `orders` by CDC and `customers` by its `updatedAt` field:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-db
spec:
  connector: mongodb
  config:
    uri: { secret: app-db-uri }
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
        cursor: updatedAt
  destination: { ref: warehouse }
```

`uri` is a connection string, such as `mongodb+srv://USER:PASSWORD@cluster0.example.net/` or `mongodb://USER:PASSWORD@HOST:27017/?replicaSet=rs0`. It's a secret: store it with `openetl secret set app-db-uri`.

### Delivery

Full refresh and CDC streams replay exactly: a read resumed from a checkpoint yields the same documents in the same order, as long as the collection is the same, and the oplog still has the CDC position. OpenETL counts incremental streams as ones that may repeat records, because of the documents at the cursor's last value, and the destination's capabilities decide what guarantee that leaves.

## Destination

### What it writes

Each stream becomes a collection in the database the pipeline's `destination.namespace` names, or else the config's `database`. If the pipeline's `destination.tables` gives the stream a name, the collection has that name. Otherwise, it has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`.

Each row is a document, and a null is a field the document doesn't have. A document's `_id` is:

- its key, where the stream is merged or overwritten with a primary key: the key's value for a key of one column, or else a document of the key's columns, in order;
- its `_openetl_id`, where the stream is appended and deduplicated against the collection, so a row whose `_openetl_id` the collection has is skipped;
- else an ObjectId the destination gives it. A stream deduplicated within each epoch only, as one with no primary key that the pipeline requires only `at_least_once` of, skips a row whose `_openetl_id` an earlier row of the epoch has, and a later epoch's row of that `_openetl_id` lands beside it.

A column named `_id` is written only as the key or part of it: in the last two cases, and where the key is other columns, it isn't written, so rename it in the pipeline to keep it. A key's values land as their types do in [Types](#types), so a timestamp keeps milliseconds: keys that differ only below a millisecond share a document.

From the MongoDB source, whose `_id` is a string that spells its BSON type, set `extendedJSONKeys: true`: a string key's 24 hexadecimal digits are then an ObjectId, and anything else is read as canonical Extended JSON, so each document gets back its `_id` as it was, `5` an `int` and `"5"` a string. A string key in neither form fails the write with `DATA_INVALID`. Without it, a string key is a string `_id`, one for each source `_id`.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a collection of its own, `_openetl_read_…`, with the target's validator and collation. When the read ends, its last epoch's apply gives it the target's indexes, and it then replaces the target, by `renameCollection`, so the target keeps its old documents until then.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`: an update guarded by it leaves a document with a higher `_openetl_seq` as it is. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the document keeps its fields, with `_openetl_deleted` true and the delete's metadata.

### Commits

Each epoch is staged into collections of its own, `_openetl_stage_…`, then applied by writes that apply alike however often they run: merge's guarded upserts and deletes, and inserts by an `_id` given when the row was staged, which skip a document with that `_id`. A row that a unique index of the user's refuses fails the commit with `DATA_INVALID`. A stream's apply ends with its marker, which one update of the pipeline's document in `_openetl_fence` writes, refused once a newer attempt has raised the fence. An attempt that recovers applies again the epochs whose markers aren't there, from their staging. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` in place.

The apply isn't one transaction, which MongoDB limits in size and time:

- Readers of a collection can see part of an epoch while it's applied, and after a crash, until the next attempt applies the rest.
- With the pipeline's commit scope, the default, the streams' markers are written together, but not their writes: a commit that fails on one stream leaves the streams it applied before it visible, and the next attempt applies the rest.
- A replaced attempt that's already applying finishes the writes it started. They're the same writes the next attempt makes, except for a stream appended without deduplication, whose rows the next attempt stages again, and which can then land twice.
- An overwrite's `renameCollection` can't be fenced, since MongoDB runs no rename in a transaction. A replaced attempt still applying an overwrite's last epoch after the next attempt has swapped its read in writes its last rows to a new `_openetl_read_…` collection, which stays until the stream's next overwrite read starts. If the next attempt crashed between the swap and its marker, the attempt after it swaps that collection in again, and the target then holds only those rows until the stream is read again.
- With `destination.deletes: hard`, nothing is kept of a deleted document, so a version of its key older than the delete that lands after it, as a source that repeats rows can send, brings the document back.

The destination reads from the primary, whatever read preference and read concern the `uri` gives, so that a commit finds every row it staged. Its writes take the `uri`'s write concern, or the server's default, and `majority` in place of an unacknowledged `w=0`. MongoDB's default is `majority`, but `w=1` where the replica set has an arbiter. A write concern of `w=1` acknowledges a write before the other members have it, so a failover can roll back an epoch that was committed.

Up to 16 writer lanes write at once. A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

### Set it up

The user in `uri` needs the `readWrite` role on each database the destination writes to. Taking a type change into a collection's validator, which the next section explains, needs `collMod` too, as the `dbAdmin` role has.

### Check and preflight

The destination's check has the source's `net.dns`, `net.connect`, `net.tls` and `db.connect`, then `db.write`: the user has, on `database`, the actions the destination takes, which the `readWrite` role gives: `find`, `insert`, `update`, `remove`, `createCollection`, `createIndex`, `listIndexes`, `renameCollectionSameDB` and `dropCollection`.

A preflight checks `db.write` on the pipeline's namespace, or on `database` if it names none, and has these items for each stream. A collection that doesn't exist passes them, and the first write creates it.

| Item | What it checks |
|---|---|
| `stream.NAME.write` | The collection isn't a view or a time series, and a merged stream has a primary key. Where its `$jsonSchema` validator is enforced, it fails when the validator requires a field the stream lacks, or takes no field its properties don't name and the stream has one, unless it has `patternProperties`. It warns of the columns whose types the validator doesn't take, which the stream holds, as [Schema changes](#schema-changes) explains. Where the pipeline lets the stream change types, it checks instead that the user has `collMod` on the collection. |
| `stream.NAME.indexes` | The collection's unique indexes, but `_id`'s and partial ones. It fails on an index on fields the stream doesn't have, unless it's sparse, since each document then indexes them as null. It warns of an index on the key of a stream that appends, since a row's second version repeats it, and of an index on other fields, since rows whose values repeat are refused, with `DATA_INVALID`. An index on the whole key of a stream that merges, or overwrites with a primary key, passes. |
| `stream.NAME.keys` | Only for a stream merged by one text key. It only warns, since the documents it finds may be another writer's: with `extendedJSONKeys` off, when the collection has an ObjectId `_id`, which the stream's text keys never find, and with it on, when the collection has a text `_id` spelled as the source spells one, which the option's `_id`s don't find. |

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: mongodb
  config:
    uri: { secret: warehouse-uri }
    database: raw
```

### Types

| Arrives as | BSON |
|---|---|
| boolean | `bool` |
| 8-, 16- and 32-bit integers, unsigned 8- and 16-bit | `int` |
| 64-bit integers, unsigned 32-bit | `long` |
| 32- and 64-bit floats | `double` |
| decimal | `decimal`, up to its 34 digits; a value with more is a string |
| string | `string` |
| binary | `binData` of subtype 0 |
| UUID | `binData` of subtype 4 |
| timestamp, date | `date`, which keeps milliseconds: finer digits are dropped |
| time of day | `string`, such as `"12:34:56.789012"` |
| interval | a document of `months`, `days` and `nanoseconds` |
| list | `array` |
| JSON | the value it spells, read as relaxed Extended JSON, so `{"$oid": "…"}`, `{"$date": "…"}` and the like land as their types, an integer as `int` or `long`, and any other number as `double`, an integer past 64 bits too; JSON that isn't valid Extended JSON is a string |

`_openetl_seq` is kept as a number that orders a key's versions: a timestamp as `long` in its unit since 1970, and a decimal as `decimal`. `_openetl_held` is a document from each held field to the `_openetl_id` of the version whose value it holds.

### Schema changes

A collection has no column types: a new column is a new field, and a widened column's new values land beside the old ones, whatever their types. Only a collection's `$jsonSchema` validator, which the user gives it, says what a field takes:

- A field its `required` lists takes no null, so OpenETL holds no cell there.
- A column whose BSON type the field's `bsonType` or `type` doesn't take is refused, and the stream holds it. A decimal of more than 34 digits, such as the MongoDB source's `decimal(38, 9)`, lands as `decimal`, or as `string` where a value has more digits than Decimal128 holds, so the field must take both: a validator that takes only `decimal` refuses it. The refusal gives the command that adds the types to the validator:

  ```js
  db.runCommand({"collMod": "orders", "validator": {"$jsonSchema": {"properties": {"amount": {"bsonType": ["int", "long"]}}}}})
  ```

  Run it, then release the hold. Since `collMod` replaces the whole validator, the refusals of a stream's columns all give one command, which adds every one's types. Or let the destination run it itself with the pipeline's `destination.typeChanges: allow`.

A validator whose `validationAction` is `warn`, or whose `validationLevel` is `off`, says nothing of what a field takes, nor does one that isn't a `$jsonSchema`. A document a validator rejects, such as one with a field that `additionalProperties: false` doesn't allow, fails the commit with `DATA_INVALID`, which says why.

### Limits

- Filling held cells uses `$getField` and `$setField`, which need MongoDB 5.0 or later.
- A document is at most 16 MB, as MongoDB allows.
