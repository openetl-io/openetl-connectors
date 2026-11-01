# openetl-connectors

The official connectors for [openetl](https://github.com/unsuitable001/openetl). Each is an external connector: an executable that serves the connector protocol on a Unix socket, which the openetl runner starts as a subprocess.

| Connector | Executable | Notes |
|---|---|---|
| Postgres source | `source-postgres` | Full-refresh snapshots in primary-key chunks, typed Arrow |
| Postgres destination | `destination-postgres` | Stage and apply, with a fence row; append, dedupe at apply |

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

## Layout

- `postgres/`: the Postgres source and destination, and their config schemas.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.

## License

MIT
