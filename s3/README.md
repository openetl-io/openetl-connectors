---
title: Amazon S3
description: Read Parquet, Avro, CSV and JSON Lines files, and write Parquet, CSV and JSON Lines files, on Amazon S3, S3-compatible stores such as MinIO and Cloudflare R2, or a local disk.
---

The `s3` package has a source and a destination for files on Amazon S3, on another store with S3's API, or on a local filesystem, picked by the URL's scheme:

| Store | URL |
| --- | --- |
| Amazon S3, or an [S3-compatible store](#s3-compatible-stores) | `s3://BUCKET/PREFIX` |
| A local filesystem | `file:///ABSOLUTE/PATH` |

The source reads the files that a glob matches as a stream, in Parquet, Avro, CSV or JSON Lines. The destination writes each stream as a directory of Parquet, CSV or JSON Lines files, and appends each epoch's files exactly once, or replaces a table's files at once when a read ends.

> [!NOTE]
> 0.1.0-alpha has the destination's Parquet appends on S3 and local disks. The source, the destination's CSV and JSON Lines, and overwrite are from 0.2.0.

Both roles are tested on a local filesystem and against SeaweedFS's S3 API. The destination's Parquet appends have also run against Amazon S3. CI doesn't test Amazon S3 itself. See [Limits](#limits) for what SeaweedFS can't show.

## Connecting

The source and the destination share their connection settings. Keep keys in secrets, as `{ secret: NAME }`.

### Amazon S3

Set `region`. With `accessKeyId` and `secretAccessKey`, the connector uses those keys. Without them, it uses the AWS SDK's default credential chain, which finds only what the runner gives it. By default, the runner gives it no AWS credentials. Name an IAM role in the connector's `aws.role`, which the runner assumes for it, on EC2 and EKS alike. [Run on AWS](https://github.com/openetl-io/openetl/blob/main/docs/self-host/aws.md#credentials-for-connectors) explains the setup, and the runner's `inherit` setting. Keys in the config are used even if the connector names a role.

### S3-compatible stores

A store with S3's API, such as MinIO, Cloudflare R2 or Ceph, takes the same settings, with `endpoint` set to its S3 API's URL, `region` to one the store takes, and its keys in `accessKeyId` and `secretAccessKey`. With `endpoint`, the connector addresses buckets by path, `ENDPOINT/BUCKET/KEY`, rather than by host name. From 0.2.0, the destination's uploads to an `endpoint` carry a checksum only where the store asks for one, as some S3-compatible stores refuse the CRC32 that Amazon S3 takes with every upload.

Only SeaweedFS's S3 API is tested; the stores named here aren't. Appends need only S3's reads, writes, copies, deletes and listings. Overwrite also needs the store to honor S3's conditional writes, `If-None-Match` and `If-Match`: a pipeline that overwrites runs the destination's `store.conditional-write` preflight, which warns as `conditional_writes_ignored` on a store that ignores them, as [Checks and preflights](#checks-and-preflights) says.

### Local filesystem

A `file://` URL names a directory on the runner's disk, which the destination creates. It takes none of the store's settings. Overwrite's manifest is written under a file lock.

### Checks and preflights

From 0.2.0, a check reports these items, in order, for either role. Each one after the first runs only if the item before it passed or warned. On `file://` URLs, the check has only `store.list` and the destination's `store.write` and `store.delete`.

| Item | What it checks |
| --- | --- |
| `net.dns`, `net.connect`, `net.tls` | The store's API resolves, accepts a connection and, over HTTPS, completes a TLS handshake: `endpoint`, or `s3.REGION.amazonaws.com`. Without `endpoint`, a failure here only warns, since a runner can reach AWS through a proxy that the store's SDK uses and a direct connection doesn't. |
| `store.auth` | The store accepts the credentials. If the store knows the identity but refuses it access, this item passes, and the items after it show what access is missing. From 0.2.0, it passes too for a bucket in another region than `region`, whose answer doesn't show whether the store takes the credentials, and `store.bucket` fails. |
| `store.bucket` | The bucket exists. From 0.2.0, a bucket in another region than `region` fails this item, which names the bucket's region. |
| `store.list` | The identity lists the files under `url`: `s3:ListBucket`. |
| `store.write`, `store.delete` | The destination's only: it writes a file under `URL/_openetl/` (`s3:PutObject`), then deletes it (`s3:DeleteObject`). |

A permission the store refuses fails as `privilege_missing`, naming the privilege.

A source's preflight has two items for each stream the pipeline reads:

- `stream.NAME.read` reads the first byte of a file that the stream's glob matches. It warns as `no_files` if none does, since the stream's reads then find nothing.
- `stream.NAME.format` reads that file's schema, header or first records as the stream's `format`. It fails as `file_unreadable` if they aren't in that format, or if the file's compression doesn't match its name.

A destination's preflight has these items:

- `stream.NAME.mode` checks that the stream's write mode is append or overwrite. Merge fails as `setting_mismatch`.
- `stream.NAME.types` checks that the stream's columns have the types that the table's record of its columns has. A change fails as `type_change_refused`, as the write would refuse it, unless the pipeline lets the stream change types. See [Schema changes](#schema-changes).
- `store.conditional-write` runs only for a pipeline that overwrites. It writes a file under `URL/_openetl/`, then writes it three more times: only if it's absent, only if it's at the version it has, and only if it's at the version it had before that. Then it deletes the file. It warns as `conditional_writes_ignored` if the first or the last of the three succeeds, which happens on a store that ignores `If-None-Match` or `If-Match`. On such a store, a stale overwrite could put an older manifest back.

### Errors

Credentials that the store refuses fail the check and the sync with `AUTH_INVALID`, `AUTH_EXPIRED` or `PERMISSION_DENIED`, and OpenETL retries only `AUTH_EXPIRED`. A store that doesn't answer, rate limits the connector or returns a server error fails with `TRANSIENT` or `RATE_LIMITED`, which OpenETL retries. Without AWS credentials, the connector fails with `AUTH_INVALID`; when what gives it credentials, such as STS or the runner's endpoint of a role's, doesn't answer or returns a server error, with `TRANSIENT`. From 0.2.0, a bucket in another region than `region` fails the check and the sync with `CONFIG_INVALID`, and the error names the bucket's region to set; before, the check said the bucket didn't exist.

## Source

From 0.2.0.

### What it reads

Each of `streams` is the set of files under `url` that its `glob` matches, in one `format`:

- `*` and `?` match within one path segment. `**` matches any number of segments. `{a,b}` matches either alternative, and `[...]` matches one character of a class, with `[!...]` for its complement.
- The glob's part before the first of those characters is the prefix the source lists, so a glob that starts with a fixed directory lists only that directory.
- A file whose name ends in `.gz` or `.gzip`, `.zst` or `.zstd`, `.bz2`, or `.sz` or `.snappy` is decompressed with gzip, zstd, bzip2 or framed snappy first. One ending in `.xz`, `.lz4`, `.br`, `.zip`, `.7z` or `.lzo` fails the read with `DATA_INVALID`, naming the file and its compression, as the source can't decompress it. Parquet's and Avro's own compression needs nothing.

Each record gets three more columns:

| Column | What it holds |
| --- | --- |
| `_file` | The file's URL, such as `s3://my-bucket/raw/2026/01/orders.csv.gz`. |
| `_line` | The record's number in the file, from 1: its row in a Parquet file, its record in a CSV file (not counting the header), or its non-blank line in a JSON Lines file. |
| `_modified` | When the store last modified the file, in UTC. |

A file's own column of one of these names, as a file written from another files source has, gives way to the source's.

A stream's primary key is `_file` and `_line` unless `primaryKey` names other columns, and its cursor is `_modified`.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: landing-files
spec:
  connector: s3
  config:
    url: s3://my-bucket/landing
    region: eu-west-1
    streams:
      - name: orders
        glob: orders/**/*.parquet
        format: parquet
      - name: customers
        glob: exports/customers-*.csv.gz
        format: csv
        columns: { signup_date: date }
```

The [config schema](./source.schema.json) has every setting.

### Formats

- **Parquet** is read a row group at a time, with ranged reads of the column chunks, so a file is never held in memory whole. Batches carry the file's types, mapped as [Types](#types) says, or text for a column whose type differs between files, as [Schema changes](#schema-changes) says.
- **Avro** object container files are read a record at a time, and sent as JSON that OpenETL takes to the declared types. Each file's records must be Avro records, whose fields are the columns.
- **CSV** follows RFC 4180: quoted fields can hold the delimiter, line breaks and doubled quotes. `csv` sets `delimiter`, `quote`, `header`, `columnNames` for files without a header, `skipRows` before the header, `encoding` (a WHATWG label such as `windows-1252`), and `nullValues`. An unquoted value in `nullValues` (by default, only the empty one) is null; a quoted value never is, so `""` is an empty string. The source sends values as text, and OpenETL parses them to the stream's declared types, quarantining values that don't fit.
- **JSON Lines** holds one JSON object per line, and blank lines are skipped. The source sends each object as JSON, and OpenETL takes its values to the declared types.

### Sync modes and delivery

- **Full refresh** reads every matching file, in key order.
- **Incremental** reads files that weren't read before, in the order of their `_modified`. A file that's written again after it was read is read again, whole.

The state an incremental read leaves records each file it read since `lookback` (1 hour by default) before the newest file it read, with its modification time and, except on a `file://` URL, its ETag. The next read takes any file it doesn't list and that's no older than that, so files that share a modification time with ones already read, or that appear late, still get read. Files older than that, and not read, are skipped. S3 dates an object uploaded in parts from when the upload started, so an upload that takes longer than `lookback` can be missed. Raise `lookback` if your uploads run longer. The state lists at most 10,000 files; past that, the oldest ones drop out, and files no newer than them are treated as read. Files that share the oldest modification time the state keeps all stay in it, so when more than 10,000 files share one, the state lists every one of them.

The source's replay is **superset**: a retried read may send records again, such as those of a file it was partway through, or of a file written again. A pipeline that needs exactly once has to drop repeats by the primary key, `_file` and `_line` by default.

A read resumes within a file: after each row group of a Parquet file, after each batch of 10,000 records or 8 MiB of an Avro file, whose records before it are decoded again and skipped, and after each batch of 10,000 records or 8 MiB of a CSV or JSON Lines file. An uncompressed UTF-8 text file resumes with a ranged read at the batch's offset; a compressed or re-encoded one is decompressed from its start and the records before the offset skipped. A file that changes while it's read fails the read with `TRANSIENT`, and the retry reads it again.

Each stream is read in 8 splits, which OpenETL can read in parallel. A file goes to a split by its key's 32-bit hash, and each split keeps its own state. When OpenETL asks a split that's behind to split again, it gives about half of the files it hasn't started to a new split, by their hashes, at its next resume point; a split with fewer than two files left, or one read up to a point, isn't divided. A split read again up to a resume point past its halving halves where it did the first time. An incremental stream's next read goes on with the splits its last read ended with.

### Discovery

Discovery lists each stream's files, and reads:

- every Parquet file's footer and every Avro file's header, so columns from any file are declared, with a type every file's values fit, as [Schema changes](#schema-changes) says;
- every CSV file's header, and up to 1,000 records or 1 MiB of each file's values;
- up to 1,000 objects or 1 MiB of each JSON Lines file.

A text column's type is inferred from those values: `boolean` if every value is `true` or `false`, then integer, number, date (`YYYY-MM-DD`), and a timestamp with or without a UTC offset (`YYYY-MM-DD[T ]HH:MM:SS[.fraction]`, then `Z`, `±HH`, `±HHMM` or `±HH:MM` for one with an offset). A value is a number only if a 64-bit float holds it exactly, so a column with a value such as an integer past 64 bits, or a decimal of more digits than a float keeps, is text, which keeps every digit: `columns` can declare it `decimal(P,S)`. A value with a leading zero, such as `007`, makes its column text too, which keeps the zero; `0` and decimals such as `0.5` don't. Anything else, or a mix, is text. A JSON Lines field whose values are all objects or arrays is JSON. A column with no values in the sample is text.

`columns` overrides discovered types by name: `string`, `integer`, `number`, `boolean`, `date`, `time`, `timestamp` (without a time zone), `timestamp_tz`, `json`, `binary` (base64 text) or `decimal(P,S)`. Without a header, CSV columns are `f0`, `f1` and so on, unless `columnNames` names them. A Parquet or Avro column that `columns` names is sent as text, which OpenETL parses to the type, quarantining a value that doesn't fit. `columns` can't name `_file`, `_line` or `_modified`, which the source adds.

> [!NOTE]
> A file with a column that the stream's declared columns don't include fails the read with `TRANSIENT`, and the retry's discovery declares it. A JSON Lines field that first appears past discovery's sample is never discovered, so the read keeps failing until `columns` names it.

## Destination

### What it writes

Each stream's table is a directory under `url`, named after the table, below the pipeline's `destination.namespace` if it sets one: `URL/NAMESPACE/TABLE/`. Each epoch adds one or more files to it, named after the pipeline's ID, the epoch, its generation and the file's number, with the writer lane's before it for lanes after the first:

```text
s3://my-bucket/raw/orders/9af7d67a-fb93-45a8-9061-af62ce29978d-00000000000000000042-g00000000000000000001-00000.parquet
```

`format` picks the files' format:

- `parquet` (the default), compressed inside the file with `compression`: `zstd` (the default), `snappy`, `gzip` or `none`.
- `csv`, with a header line.
- `jsonl`, an object per line.

CSV and JSON Lines files are compressed whole with `compression`: `gzip` (adding `.gz` to the name), `zstd` (adding `.zst`), or `none` (the default).

A file closes at `fileSize` bytes, 256 MiB by default, and the next one starts. `fileSize` is at most 4 GiB (from 0.2.0), as Commit copies each file into place and S3 copies at most 5 GiB in one request.

The destination keeps what it needs besides the tables under `URL/_openetl/`: files being staged, a marker per stream and epoch, and a record of each table's columns. Leave it in place, and point readers at the table directories, or at an overwritten table's manifest, as [Write modes](#write-modes) says.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: lake-files
spec:
  connector: s3
  config:
    url: s3://lake/openetl
    region: eu-west-1
    accessKeyId: { secret: lake-key-id }
    secretAccessKey: { secret: lake-key }
```

The [config schema](./destination.schema.json) has every setting.

### Write modes

- **Append** adds each epoch's files to the table. Deletes from a CDC source arrive as rows whose `_openetl_op` is `delete`, and a reader that wants the latest version of each row picks it by `_openetl_seq`.
- **Overwrite**, from 0.2.0, replaces the table's contents with each full read. A read's files go to `TABLE/_reads/START/`, named for when the read started. When the read ends, the destination writes `TABLE/_symlink_format_manifest/manifest`, which lists the read's files, one URL per line, and then deletes the files of older reads under `_reads/`. That deletion is best effort: if listing `_reads/` or deleting a file fails, the write still succeeds, and the older files stay, listed by no manifest, until a later overwrite deletes them. The manifest is written with the store's conditional write, so an attempt that a newer one replaced can't put an older read back. A read that ends with no rows writes an empty manifest.

OpenETL picks each stream's mode. From 0.2.0, without the pipeline's `destination.write`, a stream read with `full_refresh` is overwritten, and a stream read with `incremental`, `cdc` or `stream` is appended to. `destination.write: append` appends to every stream, so each full read adds all its rows again. `destination.write: overwrite` overwrites every stream, and the plan refuses it if a stream is read with `incremental`, `cdc` or `stream`. [Pipeline settings](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/settings.md#destination) has the field. In a pipeline on more than one worker, each attempt reads an overwritten stream on one of them, and the plan warns so.

> [!IMPORTANT]
> An overwrite is atomic only for readers that read the files the manifest lists, as a table defined with Hive's `SymlinkTextInputFormat` does. A reader that lists the table's directory instead sees the files of every read not yet deleted, under `_reads/`. Readers that skip names starting with `_`, as Hadoop's do, see none of them. Between the manifest's write and the deletion of the older read's files, the older files are still there but listed by no manifest.
>
> The older read's files are deleted just after the manifest is replaced, so a reader that read the manifest before then can find its files gone and fail. Run such readers between overwrites, or have them try again.

### Upgrading from 0.1.0-alpha

The 0.1.0-alpha package appends to every stream, as it has no overwrite. From 0.2.0, a stream read with `full_refresh` is overwritten unless the pipeline sets `destination.write`, so a pipeline that appended its full reads overwrites them once the package is upgraded, with no change to its spec. To keep appending, set `destination.write: append` in the pipeline before its first sync with 0.2.0.

An overwrite deletes only older reads under `_reads/`, so the files appended before the upgrade stay at the top of the table's directory, listed by no manifest. A reader that reads the manifest sees only the latest read. A reader that lists the directory sees the appended files and the latest read's, both. A reader that skips names starting with `_` sees only the appended files, and never a read. Once the first overwrite has committed, point readers at the manifest, then, once no reader is still reading files it found by listing the directory, delete the table's files outside `_reads/` and `_symlink_format_manifest/`: the destination doesn't use them again.

The other way, a table overwritten and then appended to keeps its manifest and its last read under `_reads/`, and appended files go to the top of its directory, which the manifest doesn't list. A reader that reads the manifest sees only that last read, and never an appended row. A reader that lists the directory sees the last read and the appended files, both. A reader that skips names starting with `_` sees only the appended files. Once an appended read that holds every row has committed, point readers at the directory, then, once no reader is still reading files through the manifest, delete `_symlink_format_manifest/` and `_reads/` together, never one without the other: a manifest whose read is gone lists files that no longer exist. Only a full read appended with `destination.write: append` is known to hold every row. For a stream now read another way, keep the last read and its manifest until you have checked that the appended files hold every row you need. Kept, they cost storage, and a reader that lists the directory sees their rows with the appended ones.

### Exactly once

Writes go to staging first. To commit an epoch, the destination copies each staged file to its final name, then writes the stream's marker. An epoch is committed only once OpenETL's checkpoint has recorded it, under the generation in its file names, and a repeated commit copies the same files to the same names, so a file never appears twice. The marker shows that an epoch is done.

Streams whose source replays exactly land exactly once. A stream whose source may send rows again, such as a Postgres table read incrementally, or this package's own source, gets at least once: the destination can't drop the rows sent again, so a pipeline with such a stream has to accept that with `delivery: { require: at_least_once }`, and readers drop the repeats by `_openetl_id`.

In append mode each file appears when it's copied, so readers can see part of an epoch while it commits, and different streams' files land at different times.

A continuous pipeline commits every minute unless it sets its own `delivery.commit.interval`. Each commit adds at least one file per stream, so shorter intervals make more, smaller files.

### Permissions

The credentials need to read, write, copy, delete and list objects under the URL. On Amazon S3 that's `s3:ListBucket` on the bucket, and `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on its objects under the URL. Overwrite also needs the store to honor conditional writes, `If-None-Match` and `If-Match`, which the `store.conditional-write` preflight checks. The destination's check writes and deletes an object under `_openetl/`. The source needs to list and read objects under the URL.

A file over 16 MiB is uploaded in parts. From 0.2.0, a staged file whose write stops partway, as when a sync fails, has its parts dropped with `s3:AbortMultipartUpload`, which the credentials need for that; without it, the parts stay, as they do when the store doesn't answer the abort within 30 seconds of the writer's close. A bucket lifecycle rule that aborts incomplete multipart uploads after a day also clears those, and those of a connector that was killed.

## Types

### Source

Parquet columns keep their types where OpenETL has them, and become the nearest one where it doesn't:

| Parquet (as Arrow reads it) | Declared as |
| --- | --- |
| Boolean, int64, float64, string, binary, date32, decimal up to 38 digits, interval | The same |
| int8, int16, int32, uint8, uint16, uint32 | int64 |
| uint64 | decimal(20, 0) |
| float16, float32 | float64 |
| Large and view strings, and dictionaries of strings | string |
| Large, view and fixed-size binary | binary |
| Timestamps in any unit | Timestamp in microseconds, floored; in UTC if the file's has a time zone, and without one otherwise |
| date64 | date32 |
| time32, time64 | time64 in microseconds |
| Decimals over 38 digits, or with a scale below 0 or above their precision | Text |
| Lists of types kept as they are | The same |
| Structs, and other lists, such as lists of int8 | JSON text |
| Maps, and any other type | Plain text |
| UUID extension type | The same |
| Parquet's JSON type, or the JSON extension type | JSON |

A file that arrow-go can't read, such as one whose stored Arrow schema gives a column inside a list the JSON extension type over large strings, fails the read with an error that names the file.

Avro types map as:

| Avro | Declared as |
| --- | --- |
| boolean | boolean |
| int, long | int64 |
| float, double | float64 |
| string, enum | string |
| bytes, fixed | binary |
| decimal up to 38 digits | decimal of the same precision and scale |
| decimal over 38 digits | Text |
| date | date32 |
| time-millis, time-micros | time64 in microseconds |
| timestamp-millis, -micros, -nanos | Timestamp in microseconds, in UTC |
| local-timestamp-millis, -micros, -nanos | Timestamp in microseconds, without a time zone |
| uuid | UUID |
| A union of null and one type | That type |
| Other unions, records, arrays, maps, and duration | JSON |

In a JSON column, a decimal is a string with the digits its value needs, such as `"0.5"` for a `decimal(4, 2)` of 0.50.

CSV and JSON Lines values have the types [Discovery](#discovery) infers, or that `columns` declares.

### Destination

Parquet keeps the columns' Arrow types, timestamps to the nanosecond if the pipeline asks for nanoseconds, with one exception: intervals, which Parquet can't hold to the nanosecond, become a struct of `months` (32-bit integer), `days` (32-bit integer) and `nanoseconds` (64-bit integer), the parts JSON destinations spell them with.

JSON Lines writes each value as OpenETL's JSON destinations do: numbers and booleans as JSON, decimals at their scale, timestamps in RFC 3339 (with `Z` for those in UTC), dates as `YYYY-MM-DD`, binary as base64, JSON columns as their JSON, and intervals as an object of `months`, `days` and `nanoseconds`.

CSV writes each value as JSON Lines does, without quotes around text. A null is an empty unquoted field, and an empty string is `""`. Intervals and JSON columns are their JSON text.

## Schema changes

The source adds a column that a file brings once discovery has seen it, as [Discovery](#discovery) says. It doesn't change a column's type by itself.

A Parquet or Avro column whose type differs between a stream's files is declared with a type that every file's values fit: the wider of two decimals, at the larger scale and with the most integer digits; a decimal for an integer and a decimal, while it fits in 38 digits; a 64-bit float for a float and an integer or decimal; a timestamp for a date and a timestamp; and text for anything else. Each file's column is then sent as text, which OpenETL parses to the declared type, quarantining a value that doesn't fit. Integers of every width are already int64, and floats float64, so those agree without text.

OpenETL runs discovery at the start of each attempt, and the read reads every Parquet file's footer and Avro file's header again just after, to send each file's columns as discovery declared them. The two disagree only about files written or deleted in between. A file written in between with a column the read didn't see, or of another type, fails the read with `TRANSIENT`, and the next attempt reads it. The read knows the declared columns' names but not their types, so if the files whose types differed are deleted in between, it sends the column typed as the remaining files have it, which OpenETL refuses as a mismatch with the wider declared type, and the next attempt's discovery declares the narrower one.

The destination adds new columns: later files have them, and earlier files don't. Readers expect a column to have one type across a table's files, so the destination keeps a record of each table's columns in `URL/_openetl/schemas/TABLE.json`, and doesn't change a column's type by itself. When a stream's column changes type, the destination refuses the change, the stream stops with a schema drift error, and the refusal says to remove that record. Once it's removed and the pipeline resumed, the next attempt writes the new type, and the table's files then hold the column with two types: make sure its readers can take that first. The pipeline's `destination.typeChanges: allow` lets the destination change the record itself. Neither role holds rows.

## Limits

- An Avro read that resumes within a file decodes the file from its start again, as the Avro library doesn't give blocks' offsets.
- Each read, as well as discovery, reads every Parquet file's footer and Avro file's header, 16 at a time, so it takes longer to start with more files.
- A read checks that a file is the one it listed by its ETag, size and modification time, so a file replaced while it's read fails the read with `TRANSIENT`, and the next attempt reads it again. A `file://` URL has no ETags: there, a read checks the size and the modification time to the second, so a file replaced while it's read, by one of the same size within the same second, isn't noticed, and a Parquet file's row groups could then come from both.
- The source doesn't capture deleted files or deleted rows. A file written again is read again whole, as new records, if its modification time changed or its ETag did, so a file rewritten within the same second is read again too; on a `file://` URL, only if its modification time changed.
- Discovery reads every file's footer or header, so it takes longer with more files.
- The destination doesn't merge or deduplicate.
- Overwrite relies on the store's conditional writes. On an S3-compatible store that ignores `If-Match` and `If-None-Match`, a stale attempt could write an older read's manifest over a newer one's.
- SeaweedFS answers over plain HTTP, so the check's `net.tls` item is untested.
- SeaweedFS is the only S3-compatible store tested, and the test's checks no credentials, so refused credentials are tested only against stand-ins of S3's answers. Amazon S3's own consistency and permissions aren't shown by it.
