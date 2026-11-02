# openetl-connectors

The official connectors for [openetl](https://github.com/unsuitable001/openetl). Each is an external connector: an executable that serves the connector protocol on a Unix socket, which the openetl runner starts as a subprocess.

| Connector | Executable | Notes |
|---|---|---|
| Postgres source | `source-postgres` | Full refresh in primary-key chunks, typed Arrow; CDC through a logical replication slot (pgoutput), with read-only snapshots in chunks |
| Postgres destination | `destination-postgres` | Stage and apply, with a fence row; append, overwrite and merge, dedupe at apply |
| Iceberg destination | `destination-iceberg` | Parquet data files committed through a REST or Glue catalog, one snapshot per table and epoch, with the catalog's compare-and-swap behind the fence; append and overwrite. Tables commit together on REST catalogs with multi-table commits. Not tried against Glue. |

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

The Iceberg tests need an Iceberg REST catalog at `OPENETL_TEST_ICEBERG_URI` (default `http://127.0.0.1:8181`) whose warehouse is the `iceberg` bucket of an S3 store at `OPENETL_TEST_S3_ENDPOINT` (default `http://127.0.0.1:8333`). openetl's compose file runs both (`make deps-up`); the tests create the bucket.

## Layout

- `postgres/`: the Postgres source and destination, and their config schemas.
- `iceberg/`: the Iceberg destination, and its config schema.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.

## License

MIT
