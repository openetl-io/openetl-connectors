---
title: Amazon Redshift
description: Write into Amazon Redshift by appending, overwriting or merging, loaded from Parquet files in S3 by COPY.
---

> [!WARNING]
> Unverified. The destination builds and its unit tests pass, but it hasn't run against a Redshift cluster or Serverless workgroup, and its conformance suite hasn't passed. Try it on data you can reload before you rely on it.

The `redshift` package has a destination. It writes each stream into a table of its own, staging each epoch as Parquet files in an S3 bucket that Redshift's `COPY` loads, and commits each epoch exactly once. It has no source: to read from a Postgres database into Redshift, use the [postgres](../postgres/README.md) source.

## Destination

### What it writes

Each stream becomes a table in the pipeline's `destination.namespace`, or else in the config's `schema` (`public` unless set). If the pipeline's `destination.tables` gives the stream a name, the table has that name, exactly as written. Otherwise, the table has the stream's name as the pipeline's `destination.naming` gives it: by default, lowercase, with `_` in place of anything outside `a-z`, `0-9` and `_`. The schema is created if it's missing.

Names are case-sensitive: each connection sets `enable_case_sensitive_identifier`, so a table or column keeps the case of its name. A query of a name with capitals must quote it, and must set `enable_case_sensitive_identifier` too.

All three write modes work:

- **Append** inserts each epoch's rows.
- **Overwrite** writes a read's rows to a table of its own, which replaces the target's rows when the read ends, so the target keeps its old rows until then.
- **Merge** upserts each key's latest version, ordered by `_openetl_seq`, so an older version never overwrites a newer one. Deletes are soft unless the pipeline sets `destination.deletes: hard`: the row stays, with `_openetl_deleted` true. Redshift enforces no keys, so the destination matches rows by the stream's key itself, and a row a person inserts with a key that's already there makes two.

Each epoch's rows are written to Parquet files, one for each stream and writer lane, uploaded to `bucket` under `prefix`, and loaded by `COPY` into staging tables of their own. Each file is deleted once it's loaded. The epoch is then applied in one transaction behind a fence row in `_openetl_fence`, with a marker per stream in `_openetl_loads`. A replaced attempt can't apply, and an epoch is never applied twice. Append and overwrite also skip rows a source sends again, by their `_openetl_id`, and merge keeps each key's latest version. So a stream lands exactly once, or effectively once when it's merged from a source that may repeat rows, unless the pipeline asks for less. Leave `_openetl_fence` and `_openetl_loads` in place.

Each `COPY` takes seconds, however few its rows, so a continuous pipeline commits every minute unless it sets its own `delivery.commit.interval`, and not more often than every 10 seconds.

### Set it up

The cluster or workgroup needs:

- A user for `dsn` with `CREATE` on the schema, or, if the schema doesn't exist yet, `CREATE` on the database, and `TEMP` on the database: each load and merge uses temporary tables.
- An S3 bucket in the cluster's region. A lifecycle rule that expires objects under `prefix` after a day cleans up a file whose delete failed.
- An IAM role associated with the cluster or workgroup, named by `iamRole` (`default` for its default role), with `s3:GetObject` and `s3:ListBucket` on the bucket. `COPY` reads Parquet files only as a role, and through S3's presigned URLs, so a bucket policy mustn't refuse those.
- An identity that uploads the files, with `s3:PutObject` and `s3:DeleteObject` on the bucket's keys under `prefix`: `accessKeyId` and `secretAccessKey`, or else the AWS default credentials where the connector runs.

```sql
CREATE USER openetl PASSWORD '...';
GRANT CREATE, TEMP ON DATABASE dev TO openetl;
```

### Check and preflight

The check is about the connection and the bucket:

| Item | What it checks |
|---|---|
| `net.dns`, `net.connect` | The `dsn`'s host resolves, and its port takes a connection. |
| `db.tls` | The cluster takes TLS, with a certificate the `dsn` trusts. Only when the `sslmode` uses TLS. It warns when the cluster doesn't take TLS and `sslmode=prefer` (the default) lets the connection go on in plaintext, and when the certificate expires within 14 days. |
| `db.connect` | The user signs in. |
| `schema.create` | The user can create tables in `schema`, or create it. |
| `db.temp` | The user has `TEMP` on the database. |
| `s3.write` | A small Parquet file uploads to the bucket under `prefix`. It's deleted afterwards. |
| `s3.copy` | `COPY` loads that file into a temporary table, as `iamRole`. |

A preflight checks `schema.create` of the pipeline's namespace, and for each stream, `stream.NAME.write`. A table that doesn't exist is created. One that exists takes `SELECT`, `INSERT` and `UPDATE`, and also `DELETE` to overwrite or merge into it: a hold's release updates the table in any write mode. It fails if the stream merges without a key, has a column the destination can't write, or would take the table past 1,600 columns. It fails if the user doesn't own the table, or isn't a superuser, and the table lacks a column of the stream's, which only its owner can add. It warns if a column's type is one the destination would refuse to change, below, and if the user doesn't own the table, since a column the stream gains later can't be added.

### Example

The [config schema](./destination.schema.json) has every setting.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: redshift
  config:
    dsn: { secret: redshift-dsn }
    schema: raw
    bucket: my-company-openetl-staging
    prefix: openetl
    region: us-east-1
    iamRole: arn:aws:iam::123456789012:role/RedshiftCopy
```

The `dsn` is a Postgres connection string to the cluster's or workgroup's endpoint, such as `postgres://openetl:PASSWORD@CLUSTER.ID.us-east-1.redshift.amazonaws.com:5439/dev?sslmode=verify-full`. IAM authentication isn't supported: the user signs in with a password.

### Types

| Arrives as | Redshift column |
|---|---|
| boolean | `boolean` |
| 8- and 16-bit integers, unsigned 8-bit | `smallint` |
| 32-bit integers, unsigned 16-bit | `integer` |
| 64-bit integers, unsigned 32-bit | `bigint` |
| unsigned 64-bit | `decimal(20,0)` |
| 32- and 64-bit floats | `real`, `double precision` |
| decimal(p, s) | `decimal(p,s)`, up to 38 digits; wider ones, or a negative scale, as `varchar(65535)` |
| string | `varchar(65535)` |
| binary | `varbyte(16777216)` |
| date, time of day | `date`, `time` |
| timestamp | `timestamp`, or `timestamptz` if it has a time zone, to the microsecond |
| interval | `varchar(64)`, as ISO 8601, such as `P1M2DT3.5S` |
| UUID | `varchar(36)` |
| JSON, lists, structs and maps | `super` |

A string, JSON value, list, struct or map over 65,535 bytes fails the epoch with `DATA_INVALID`: that's `VARCHAR`'s limit, and `JSON_PARSE` reads `SUPER`'s values from a `VARCHAR`. A timestamp's nanoseconds are cut to microseconds.

### Schema changes

New columns are added to the table, one statement each, until it has the 1,600 columns Redshift allows; past that, the destination refuses the column.

Redshift's `ALTER COLUMN … TYPE` changes only a `varchar`'s length, outside a transaction, so the destination doesn't change an existing column's type by itself. When a stream's column widens, it refuses the change, the stream holds the column where it can, and the refusal gives the statements that make the change: they add a column of the new type, copy the values into it, drop the old column and rename the new one, which moves the column to the table's end.

```sql
ALTER TABLE "raw"."orders" ADD COLUMN "_openetl_retype_…" bigint;
UPDATE "raw"."orders" SET "_openetl_retype_…" = CAST("amount" AS bigint);
ALTER TABLE "raw"."orders" DROP COLUMN "amount";
ALTER TABLE "raw"."orders" RENAME COLUMN "_openetl_retype_…" TO "amount";
```

Run them, then release the hold. Or let the destination make such changes itself with the pipeline's `destination.typeChanges: allow`, for every stream or one. It then widens `smallint` to `integer` to `bigint`, any of them or `real` to `double precision`, integers and decimals to a `decimal` that holds them, a `varchar` to a longer one, and anything but `varbyte` and `super` to `varchar(65535)`, in one transaction. A change that isn't a widening is refused either way, and so is one of the table's distribution key or a sort key, which Redshift can't drop, or of a `NOT NULL` column.

### Limits

- Redshift has no upsert, and the merge works out each key's row in temporary tables, so a merge's cost grows with the target's rows that the epoch touches as well as with the epoch's.
- Each epoch is one `COPY` per stream and lane. Many small streams commit slowly.
- `VARCHAR` compares without trailing blanks, so the destination matches text keys by their bytes too: `'a'` and `'a '` are different keys.
