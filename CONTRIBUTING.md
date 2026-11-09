# Contributing to openetl-connectors

Thanks for helping. For a large change, open an issue first, so we can agree on the approach before you spend time on it. [AGENTS.md](./AGENTS.md) has the rules for changing the connectors, for people and coding agents alike.

## Build

The connectors build against the Go SDK in a checkout of openetl next to this one (`../openetl`):

```sh
go build -o bin/ ./cmd/...
```

From openetl, `make connectors` builds them into its `bin/connectors`, against that checkout's SDK.

`git config core.hooksPath .githooks` once: the pre-commit hook runs gofmt and `go vet -tags integration` on the packages a commit touches, as CI does.

## Test

```sh
go test ./...
# Against a Postgres (default postgres://openetl:openetl@localhost:5432/openetl):
OPENETL_TEST_POSTGRES_DSN=… go test -tags integration ./...
```

The CDC tests need the server to run with `wal_level = logical`, and a user that may create replication slots and publications.

The Iceberg tests need an Iceberg REST catalog at `OPENETL_TEST_ICEBERG_URI` (default `http://127.0.0.1:8181`) whose warehouse is the `iceberg` bucket of an S3 store at `OPENETL_TEST_S3_ENDPOINT` (default `http://127.0.0.1:8333`). openetl's compose file runs both (`make deps-up`); the tests create the bucket. With `OPENETL_TEST_GLUE_ENDPOINT` set to a Glue endpoint, such as moto's (`docker run -p 5000:5000 motoserver/moto:5.2.5`), they run against Glue too (on macOS, port 5000 is AirPlay's: map moto to another, such as `-p 5055:5000`).

The S3 destination's tests run against `file://` by default, and with `-tags integration` against the S3 store too, in a bucket they create.

The Kafka tests need a broker at `OPENETL_TEST_KAFKA_BROKERS` (default `127.0.0.1:9092`) and a Confluent-compatible schema registry at `OPENETL_TEST_SCHEMA_REGISTRY` (default `http://127.0.0.1:8081`). openetl's compose file runs Kafka and Karapace.

The Kinesis tests need a Kinesis endpoint at `OPENETL_TEST_KINESIS_ENDPOINT` (default `http://127.0.0.1:4568`), such as kinesis-mock's, which openetl's compose file runs, and the schema registry.

## Release

Each package directory has an `openetl-connector.yaml`: its name, metadata, and how each role is built. The `openetl` CLI does the rest, so nothing here is specific to this repository:

```sh
openetl connector validate --require author,logo,description,license postgres kafka kinesis iceberg s3
openetl connector package --tag postgres/v0.2.0
```

`package` picks the directory whose package is `postgres`, builds each role for every platform into `dist/` (`postgres-source-linux-amd64` and the like), and writes `dist/postgres.connector.yaml`, whose binaries point at the `postgres/v0.2.0` release's assets. The executables and the descriptor are that release's assets, and `openetl connector apply -f postgres.connector.yaml` pins the version. Tags are per package: `postgres/v0.2.0`, `kafka/v0.1.3`.

**A release candidate** is built once and tested as built. `gh workflow run release-candidate.yml -f version=0.1.0-alpha -f commit=SHA -f openetl=SHA` packages that commit, once `ci` has passed on it, for Linux on amd64 and arm64. The packages go to `ghcr.io/openetl-io/openetl-connectors`, private until the release is published, and each gets a draft release, which makes no tag until it's published. OpenETL's test suites install them from there, as a user does. `gh workflow run release-discard.yml -f version=VERSION` deletes a candidate's versions and drafts, and refuses a version that's been published.

## Layout

Each package directory holds its `openetl-connector.yaml`, its config schemas and its page, `README.md`.

- `postgres/`: the Postgres source and destination.
- `iceberg/`: the Iceberg destination.
- `s3/`: the Parquet on S3 destination.
- `kafka/`: the Kafka source and destination.
- `kinesis/`: the Kinesis source and destination.
- `records/`: the record layout and formats Kafka and Kinesis share: JSON, and Avro and Protobuf with a schema registry.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.
