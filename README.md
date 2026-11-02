# openetl-connectors

The official connectors for [openetl](https://github.com/unsuitable001/openetl). Each is an external connector: an executable that serves the connector protocol on a Unix socket, which the openetl runner starts as a subprocess.

| Connector | Executable | Notes |
|---|---|---|
| Postgres source | `source-postgres` | Full refresh in primary-key chunks, typed Arrow; CDC through a logical replication slot (pgoutput), with read-only snapshots in chunks |
| Postgres destination | `destination-postgres` | Stage and apply, with a fence row; append, overwrite and merge, dedupe at apply |
| Iceberg destination | `destination-iceberg` | Parquet data files committed through a REST or Glue catalog, one snapshot per table and epoch, with the catalog's compare-and-swap behind the fence; append, overwrite, and merge through equality deletes. Tables commit together on REST catalogs with multi-table commits. Glue is tested against moto, not real Glue. |
| Parquet on S3 destination | `destination-s3` | Parquet files staged under the pipeline, copied to names from the pipeline, epoch and generation, with a marker per stream and epoch; append. Also `file://`. |

## Build

The connectors build against the Go SDK in a checkout of openetl next to this one (`../openetl`):

```sh
go build -o bin/ ./cmd/...
```

From openetl, `make connectors` builds them into its `bin/connectors`, against that checkout's SDK.

## Test

```sh
go test ./...
# Against a Postgres (default postgres://openetl:openetl@localhost:5432/openetl):
OPENETL_TEST_POSTGRES_DSN=… go test -tags integration ./...
```

The CDC tests need the server to run with `wal_level = logical`, and a user that may create replication slots and publications.

The Iceberg tests need an Iceberg REST catalog at `OPENETL_TEST_ICEBERG_URI` (default `http://127.0.0.1:8181`) whose warehouse is the `iceberg` bucket of an S3 store at `OPENETL_TEST_S3_ENDPOINT` (default `http://127.0.0.1:8333`). openetl's compose file runs both (`make deps-up`); the tests create the bucket. With `OPENETL_TEST_GLUE_ENDPOINT` set to a Glue endpoint, such as moto's (`docker run -p 5000:5000 motoserver/moto:5.2.5`), they run against Glue too (on macOS, port 5000 is AirPlay's: map moto to another, such as `-p 5055:5000`).

The S3 destination's tests run against `file://` by default, and with `-tags integration` against the S3 store too, in a bucket they create.

## Layout

- `postgres/`: the Postgres source and destination, and their config schemas.
- `iceberg/`: the Iceberg destination, and its config schema.
- `s3/`: the Parquet on S3 destination, and its config schema.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.

## License

MIT
