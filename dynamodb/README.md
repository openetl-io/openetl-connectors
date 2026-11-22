---
title: Amazon DynamoDB
description: Read DynamoDB tables in full, by a cursor attribute, or change by change through DynamoDB Streams, and write into DynamoDB tables by appending, overwriting or merging.
---

The `dynamodb` package has a source and a destination. The source reads a region's tables in one of three ways: whole by Scan, the items whose cursor attribute has reached a floor, or a snapshot followed by every insert, modify and remove from the table's DynamoDB stream (CDC). The destination writes each stream into a table of its own.

Both are tested against DynamoDB Local 3.3.2. Local doesn't show:

- **Credentials.** Local takes any access key made of letters and digits, so the tests can only show that a malformed key fails with `AUTH_INVALID`.
- **Throttling.** Local doesn't throttle, so the backoff and the retries of unprocessed items run only against fakes in unit tests.
- **Shard changes.** Local's streams keep a single shard and trim nothing. Shard lineage after splits and merges, trimmed positions and shards that age out are shown against a fake of the DynamoDB Streams API.
- **Item size.** The 400 KB check is the connector's own, so Local's limit is never reached.

## Connecting

The source and the destination share their connection settings:

- `region`: the tables' AWS region. Unset, it is `AWS_REGION` where the connector runs, or the AWS default configuration's.
- `accessKeyId` and `secretAccessKey` go together, with `sessionToken` for temporary credentials. Without them, the connector uses the AWS SDK's default credential chain, which finds only what the runner gives the connector: by default, nothing. Name an IAM role in the source's or destination's `aws.role`, which the runner assumes for the connector. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup. Keys in the config are used even if a role is named.
- `endpoint`: a DynamoDB-compatible endpoint, such as a VPC endpoint or DynamoDB Local. The source reads streams at the same endpoint, as Local serves both APIs. On AWS, the streams API has endpoints of its own, so leave `endpoint` unset for CDC.

Keep `accessKeyId`, `secretAccessKey` and `sessionToken` in secrets, as `{ secret: NAME }`.

A check reports these as separate items:

- the region;
- DNS, then a TCP connection, then TLS where the endpoint is https (on AWS, `dynamodb.REGION.amazonaws.com:443`);
- signing in, by listing the tables.

## Source

### What it reads

Each table the credentials can list is a stream, or only the ones `tables` lists. A stream's primary key is its table's key: the partition key, then the sort key if there is one.

| Sync mode | Offered for | How it reads |
|---|---|---|
| `full_refresh` | Every table | The whole table by Scan. A stopped read resumes after the last page it emitted. |
| `incremental` | Every table, on a top-level attribute the pipeline names as its `cursor` | A Scan with a filter: the items whose cursor attribute is at or past a floor, explained below. |
| `cdc` | Every table with a DynamoDB stream enabled | A snapshot of the table by Scan, then every change from its stream. |

A table is read in parallel scan segments, and OpenETL can read them at once as splits. By default there is one segment per GiB that `DescribeTable` reports, up to 64; `segments` sets the number. Each Scan request reads one page, at most 1 MB or `scanLimit` items, and ends at a resume point, its `LastEvaluatedKey`.

A Scan isn't a snapshot. It reads each item once, at whatever version the item has when the scan reaches it, with eventually consistent reads. An item written while the scan runs is read at its old or its new version. An item inserted or deleted while it runs may or may not be read. A read resumed from a resume point carries on after the last key it read, and never reads an item twice.

### Incremental

DynamoDB keeps no order of change, so an incremental read can't ask for "what changed since". It scans the whole table, with a filter on the pipeline's `cursor`: a top-level attribute, a number or a string, whose values only grow, such as an update time. **Every sync reads the whole table in read capacity, as a full refresh does**; it saves only what's emitted and written.

A read emits the items whose cursor is at or past its floor. The floor is the highest cursor value that the read before the last one saw, not the last one's. A write made while a scan runs, behind it, comes after every value the previous read saw, so the next read's floor is at or below it. This holds as long as cursor values only grow. The first two syncs emit every item. Items without the cursor attribute, or whose cursor has another type, are read only by those first syncs. Deleted items aren't seen; CDC sees deletes. When a cursor attribute holds numbers and strings, numbers count above strings, and strings above binaries.

A Query on a global secondary index doesn't replace the scan. A Query needs one partition key value, so it can't range over a cursor across the table without an index the user designs for it. It also reads an index that's only eventually consistent and lags the table, which would miss writes.

### CDC

A stream with CDC needs the table's DynamoDB stream enabled. The view type sets what each change carries:

| View type | Inserts and updates | Deletes |
|---|---|---|
| `NEW_AND_OLD_IMAGES` | The item as the change left it | The keys, and the item as it was |
| `NEW_IMAGE` | The item as the change left it | The keys |
| `KEYS_ONLY`, `OLD_IMAGE` | The item as it is when the read gets to the change, by a consistent `GetItem` per change | The keys, plus the old item with `OLD_IMAGE` |

With `KEYS_ONLY` or `OLD_IMAGE`, an update arrives as the item's current state, not as the change made it. A change to an item deleted since is skipped, and its delete follows. Prefer `NEW_AND_OLD_IMAGES`.

The first sync takes a snapshot. It scans the table in parallel segments with consistent reads, and emits each item as an insert. It then reads every shard of the stream from its oldest record, and skips records created more than 5 minutes before the snapshot began; 5 minutes covers the rounding of a record's creation time and clock skew. So an item changed in those 5 minutes, or while the snapshot runs, arrives as the snapshot's row and again as its change, with a higher `seq`. A merge ends at the latest version. An append keeps both rows.

Each row's `seq` is a 38-digit decimal: the snapshot's start, in seconds since 2020, times 10^29, plus the record's sequence number. It orders a key's changes, and puts the stream after its snapshot and a later snapshot after both. A sequence number of more than 29 digits fails the read with `CONTRACT_CHANGED`; DynamoDB's are up to 40 digits long, and Local's are 21.

A stream's shards are read in parallel. A child shard starts only once its parents are read to their ends, or are gone, so a key's changes stay in order across splits and merges. The state keeps each shard's last sequence number. A bounded sync reads each shard up to the records created after the sync started. An open shard counts as read once 3 `GetRecords` calls in a row return nothing. The state then records that the shard had nothing after its position up to that moment, unless a sync recorded so within the last minute, and the next sync goes on from its position. A continuous sync keeps polling every shard, and lists the shards again every 10 seconds while it's reading none.

A DynamoDB stream keeps records for 24 hours. The source reports each CDC stream's lag and how much of those 24 hours is left, from its state, without reading. A sync fails with `POSITION_LOST` when records it hasn't read may be gone:

- a shard says its position is trimmed, and the shard may have had records after it more than 24 hours ago, because the last sync to find nothing after the position was more than 24 hours ago;
- a shard the read hadn't finished has left the stream, and the last sync to find nothing after its position was more than 24 hours ago;
- the table's stream was turned off and on again, so it has another ARN;
- the snapshot, or a shard to be read from its start, began more than 24 hours ago.

Re-sync the stream to recover. Syncs less than 24 hours apart don't fail this way, however idle a shard is, with one exception. If two syncs run within a minute of each other, a sync within a minute of 24 hours after them can fail on a trimmed position though nothing was lost. A trimmed position that a sync within the last 24 hours found nothing after loses nothing, and the read goes on from the shard's oldest record. A shard that left the stream had nothing after its position in that case either, and the read goes on without it.

### Fields and types

Items are schemaless, so discovery reads the table to find its top-level attributes. The key attributes come first, typed by the table's definition. Every other attribute follows by name, nullable, with a type that covers every value discovery saw:

| Values seen | Column type |
|---|---|
| `N` that all fit a 64-bit integer | `int64` |
| `N` with at most 29 digits before the point and 9 after | `decimal(38, 9)` |
| `N` with more, within 38 digits | `decimal(38, S)`, its scale what the integer digits leave |
| `N` that no `decimal(38, S)` holds, such as `1E-130` | `string`, in plain decimal |
| `S` | `string` |
| `B` | `binary` |
| `BOOL` | `boolean` |
| `M`, `L`, `SS`, `NS`, `BS` alone | JSON |
| More than one of the types above, or `NULL` alone | `string` |

`NULL` is a null, and doesn't count as a type. A `string` column of mixed types holds each value's text: numbers in plain decimal, binaries in base64, and maps, lists and sets as JSON. In JSON, numbers keep every digit, binaries are base64, and sets are arrays.

By default, discovery reads every item, in parallel segments, so a read never meets an attribute discovery missed, at the cost of a scan of each table. `sampleSize` limits it to about that many items from random segments. A sample can miss a rare attribute. A read that meets an attribute its run doesn't declare fails with `TRANSIENT`, saying so, and the next attempt's discovery has another chance to find it. A CDC change with such an attribute fails the read only while the table's item still has it. Otherwise the change is read without it, since discovery can't find it any more.

### Packed items

With `packed: true`, each stream is the key attributes and `data`, a JSON column holding the item in DynamoDB JSON, such as `{"id":{"N":"5"},"tags":{"SS":["a"]}}`, which keeps every type. Discovery scans nothing of a table keyed on strings or binaries, and of one keyed on a number, only the key attributes, to find the number's type, from every item or `sampleSize` of them. No read fails on a new attribute. A delete's `data` is the deleted item where the stream's view has old images, and null otherwise.

### Set it up

The source needs these IAM actions on its tables:

- `dynamodb:ListTables`, which is account-wide;
- `dynamodb:DescribeTable` and `dynamodb:Scan`;
- for CDC, also `dynamodb:GetItem`, plus `dynamodb:DescribeStream`, `dynamodb:GetShardIterator` and `dynamodb:GetRecords` on `arn:aws:dynamodb:REGION:ACCOUNT:table/TABLE/stream/*`.

Enable a table's stream:

```sh
aws dynamodb update-table --table-name orders --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

A check also finds each table `tables` names and scans one item of it. A pipeline's preflight checks, item by item, for each stream:

- that the table can be scanned;
- that an incremental stream has a cursor;
- for CDC:
  - the stream is enabled, with a warning for a view type without new images;
  - its shards can be listed and an iterator taken;
  - its records can be read;
  - an item can be read;
  - a scheduled pipeline syncs within the stream's 24 hours: an interval over 24 hours fails, and one over 20 hours warns.

### Example

The [config schema](./source.schema.json) has every setting. This source offers two tables, and its pipeline reads `orders` by CDC and `customers` by its `updatedAt` attribute:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-tables
spec:
  connector: dynamodb
  aws: { role: arn:aws:iam::123456789012:role/openetl-dynamodb }
  config:
    region: eu-west-1
    tables: [orders, customers]
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: app-tables-to-warehouse
spec:
  schedule: { every: 1h }
  source:
    ref: app-tables
    streams:
      - name: orders
        sync: cdc
      - name: customers
        sync: incremental
        cursor: updatedAt
  destination: { ref: warehouse }
```

### Delivery

Every mode declares exact replay: a read resumed from a resume point reads on after it and repeats nothing. OpenETL counts incremental streams as ones that may repeat records, because of the items at the floor, and the destination's capabilities decide what guarantee that leaves.

A redo that OpenETL asks for, when a destination that commits lanes on their own lost one, stops where the recorded read sent its resume point. It ends with a resume point there, and completes there:

- A scan stops at the item whose key the resume point names. If that item has since been deleted, the scan stops at the page that ended there, and else reads on to its segment's end.
- A cdc read stops each snapshot segment at its key, and each shard at its sequence number. It doesn't read shards the resume point hadn't started. A redo from the beginning takes the snapshot's generation and start from the resume point, so its rows, and the 5 minutes of changes it reads again, are the recorded read's.

A cdc read that runs again from a resume point with no end point, as a redo of a part that ended at the completion, completes where the recorded read did, if it runs within a minute of it. The recorded read sends a resume point when it records that a shard had nothing new, and a read within the next minute doesn't record that again.

A redo reads items as the table holds them then: a scan's or a snapshot's items, and with a `KEYS_ONLY` or `OLD_IMAGE` stream, each change's item. Items written since can differ from the recorded read's.

## Destination

### What it writes

Each stream becomes a table named `NAMESPACE.TABLE` when the pipeline sets `destination.namespace`, or else `tablePrefix` and `TABLE`. `TABLE` is the name the pipeline's `destination.tables` gives the stream, or its name as `destination.naming` gives it, with `_` in place of anything DynamoDB doesn't take in a name. A name must come to 3–255 characters. `ApplySchema` creates each table with on-demand billing, keyed on:

- the stream's primary key, where it's merged or overwritten with one: its first column as the partition key and its second as the sort key, each `N` for numbers, `B` for binaries and `S` for the rest;
- `_openetl_key`, a string of the key's values in DynamoDB JSON, such as `{"L":[{"N":"1"},{"S":"x"}]}`, for a key of more than two columns;
- `_openetl_id`, with `_openetl_row` as the sort key, for a stream appended or overwritten without a key.

An existing table keeps its key, and must be keyed on the attributes the stream needs, or the sync fails with `CONFIG_INVALID`.

Each row is an item. A null is an attribute the item doesn't have. All three write modes work:

- **Append** puts each row. Its `_openetl_row` is the order it was staged in, so two rows alike, which share an `_openetl_id`, both land. Deduplicated within each epoch, as a stream with no primary key that the pipeline requires only `at_least_once` of is, a row whose `_openetl_id` an earlier row of the epoch has is skipped and counted as collapsed, and a later epoch's row of that `_openetl_id` lands beside it. Deduplicated against the target, its `_openetl_row` is `-`, so a row whose `_openetl_id` the table has is found, skipped, and counted as collapsed.
- **Overwrite** stages a read's rows. When the read ends, it writes them to the target, each with `_openetl_read` naming the read, then deletes the items the read didn't write. Until then, the target keeps its old items.
- **Merge** puts each key's version under a condition on `_openetl_seq`, so an item with a higher `_openetl_seq` stays as it is. Deletes are soft unless the pipeline sets `destination.deletes: hard`. A soft delete keeps the item's attributes, and sets `_openetl_deleted` true with the delete's metadata. A hard delete is conditioned the same way, so an older version that arrives after it puts the item back.

### Commits

The destination stages each epoch's rows as items of a control table, `_openetl_state`, which also holds each pipeline's fence. With a namespace, it's `NAMESPACE._openetl_state`, or else it's `tablePrefix` and `_openetl_state`. Commit then applies them with writes that land alike however often they run: merge's conditional puts and deletes, and append's puts by `_openetl_id` and `_openetl_row`. Rows are applied in parallel, in 16 groups by key, so a key's versions apply in order.

A stream's apply ends with its marker, which one conditional update of the pipeline's fence item writes. It's refused once a newer attempt has raised the fence. An attempt that recovers applies again, from their staging, the epochs whose markers aren't there, and finishes an overwrite's switch that a crash cut short. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows. Leave `_openetl_state` in place.

DynamoDB has no multi-table transaction for a whole epoch, and no table rename:

- Readers can see part of an epoch while it's applied, and after a crash, until the next attempt applies the rest.
- An overwrite's switch from old items to new isn't atomic. Readers see both while it runs.
- A replaced attempt that's still applying finishes the writes it started. They're the same writes the next attempt makes, except for a stream appended without deduplication against the target, whose rows the next attempt stages again, with other `_openetl_row`s, and which can then land twice.
- An overwrite's switch isn't fenced: its puts and its deletes of the items the read didn't write are plain writes, and only the marker after them is checked against the fence. A replaced attempt still switching a read in, after the next attempt has finished that switch and then switched a later read in, deletes the later read's items, so the target can be left with few of its items or none until the stream is read again.

Each row costs about three writes: staging it, applying it, and deleting it from staging. A merged row that keeps a target value, or a soft delete, also reads the item. Recovery and the start of an overwrite read scan the control table. Up to 16 writer lanes write at once. A continuous pipeline commits every 10 seconds unless it sets its own `delivery.commit.interval`.

Throttled requests are retried with backoff, up to 10 attempts at most 10 seconds apart, and unprocessed batch items are sent again after a pause that grows from 50 ms to 5 s.

### Set it up

The destination needs these IAM actions on its tables and its control table:

- `dynamodb:CreateTable`
- `dynamodb:DescribeTable`
- `dynamodb:PutItem`, `dynamodb:UpdateItem`, `dynamodb:DeleteItem` and `dynamodb:BatchWriteItem`
- `dynamodb:GetItem`, `dynamodb:Query` and `dynamodb:Scan`

Also `dynamodb:ListTables`, for the check's sign-in and for teardown to find the control tables. Where the control table exists, a check writes and deletes an item in it. A preflight checks that each stream's table exists keyed as the stream needs, or will be created.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: tables
spec:
  connector: dynamodb
  aws: { role: arn:aws:iam::123456789012:role/openetl-dynamodb-writer }
  config:
    region: eu-west-1
    tablePrefix: raw_
```

### Types

| Arrives as | DynamoDB |
|---|---|
| boolean | `BOOL` |
| integers, signed and unsigned | `N` |
| floats | `N`. NaN, infinities and values outside `N`'s range of 1E-130 to 9.99…E+125 are `S`, such as `"NaN"` and `"1.7976931348623157e+308"`, beside the `N` values of the same attribute |
| decimal | `N`, up to 38 significant digits; one with more is `S` |
| string | `S` |
| binary | `B` |
| UUID | `S`, as text |
| timestamp | `S`, RFC 3339 to the nanosecond, with `Z` for a time zone, or without a zone for a timestamp without one |
| date | `S`, such as `"2024-02-29"` |
| time of day | `S`, such as `"12:34:56.789012"` |
| interval | `M` of `months`, `days` and `nanoseconds` |
| list | `L` |
| JSON | the value it spells: objects as `M`, arrays as `L`, numbers as `N` with every digit, or `S` outside `N`'s range |

`_openetl_seq` is kept as an `N` that orders a key's versions: a timestamp in its unit since 1970. `_openetl_held` is an `M` from each held column to the `_openetl_id` of the version whose value it holds.

### Schema changes

An item has no column types. A new column is a new attribute, and a widened column's new values land beside the old ones, whatever their types. DynamoDB fixes the type of only the attributes that the table or an index is keyed on. A column among them whose type changes is refused, and the stream holds it. The refusal gives what makes the table take the change:

- For the table's key, delete the table, and the next sync creates it:

  ```sh
  aws dynamodb delete-table --table-name raw_orders
  ```

- For a global secondary index, delete it, then create it again with the new type:

  ```sh
  aws dynamodb update-table --table-name raw_orders --global-secondary-index-updates '[{"Delete":{"IndexName":"by_customer"}}]'
  ```

A local secondary index can't change without recreating the table.

The table's key attributes take no null, and OpenETL holds no cell there.

### Limits

- An item is at most 400 KB. A row past that, by the connector's own count with what staging adds, fails the write with `DATA_INVALID`. The count is an overestimate: numbers are counted by their digits.
- A value DynamoDB refuses fails the write with `DATA_INVALID`, such as an empty string in a key, or JSON nested deeper than 32 levels.
- The fence item holds every stream's marker, so a pipeline's markers must fit its 400 KB.
- When a pipeline is deleted, OpenETL's teardown removes its fence item and its staging from every table named `…_openetl_state` that the credentials can list. Leave out of `_openetl_state` anything that isn't the connector's. The table stays.
