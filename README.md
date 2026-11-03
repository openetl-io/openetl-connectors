# openetl-connectors

The official connectors for [openetl](https://github.com/unsuitable001/openetl). Each is an external connector: an executable that serves the connector protocol on a Unix socket, which the openetl runner starts as a subprocess.

| Connector | Executable | Notes |
|---|---|---|
| Postgres source | `source-postgres` | Full refresh in primary-key chunks, typed Arrow; CDC through a logical replication slot (pgoutput), with read-only snapshots in chunks |
| Postgres destination | `destination-postgres` | Stage and apply, with a fence row; append, overwrite and merge, dedupe at apply |
| Iceberg destination | `destination-iceberg` | Parquet data files committed through a REST or Glue catalog, one snapshot per table and epoch, with the catalog's compare-and-swap behind the fence; append, overwrite, and merge through equality deletes. Tables commit together on REST catalogs with multi-table commits. Glue is tested against moto, not real Glue. |
| Parquet on S3 destination | `destination-s3` | Parquet files staged under the pipeline, copied to names from the pipeline, epoch and generation, with a marker per stream and epoch; append. Also `file://`. |
| Kafka source | `source-kafka` | A stream per topic, read `read_committed` from each partition's offset (exact replay); bounded to the end it saw for scheduled pipelines, unbounded for continuous ones; JSON, Avro, Protobuf or text values, with a Confluent-compatible schema registry |
| Kafka destination | `destination-kafka` | A transaction per epoch across topics, on a pool of three transactional producers, with the epoch's marker in the same transaction (exactly-once); a record per row, its key the primary key; JSON, Avro or Protobuf |
| Kinesis source | `source-kinesis` | A stream per Kinesis stream, polled from each shard's sequence number (exact replay within retention), a parent shard read to its end before its children; bounded to where each shard caught up for scheduled pipelines, unbounded for continuous ones; Kafka's formats |
| Kinesis destination | `destination-kinesis` | `PutRecords`, delivered and acknowledged by the epoch's end (at-least-once); a record per row, its partition key the primary key or `_openetl_id`; JSON, Avro or Protobuf |

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

The Kafka tests need a broker at `OPENETL_TEST_KAFKA_BROKERS` (default `127.0.0.1:9092`) and a Confluent-compatible schema registry at `OPENETL_TEST_SCHEMA_REGISTRY` (default `http://127.0.0.1:8081`). openetl's compose file runs Kafka and Karapace.

The Kinesis tests need a Kinesis endpoint at `OPENETL_TEST_KINESIS_ENDPOINT` (default `http://127.0.0.1:4568`), such as kinesis-mock's, which openetl's compose file runs, and the schema registry.

## Release

```sh
go run ./tools/package --version 0.1.0
```

builds every connector in `connectors.yaml` for each platform into `dist/`, and writes each one's descriptor with `openetl connector package` (from `../openetl/bin/openetl`, or `--openetl`): its executables by download URL and digest, its author and logo, and its release. The descriptors and executables are the release's assets, and `openetl connector apply -f dist/source-postgres.yaml` pins a version. Name connectors to package only those, and give `--tag` a `{connector}` to version them separately (`--tag '{connector}/v{version}'`). A connector's entry in `connectors.yaml` is its executable's name, `{kind}-{name}`, which is how the platform names it; the tool checks that each executable reports that kind.

## Layout

- `postgres/`: the Postgres source and destination, and their config schemas.
- `iceberg/`: the Iceberg destination, and its config schema.
- `s3/`: the Parquet on S3 destination, and its config schema.
- `kafka/`: the Kafka source and destination, and their config schemas.
- `kinesis/`: the Kinesis source and destination, and their config schemas.
- `records/`: the record layout and formats Kafka and Kinesis share: JSON, and Avro and Protobuf with a schema registry.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.
- `connectors.yaml`: each connector's author and logo, for its descriptor. A test checks that every connector in `cmd/` has them.
- `tools/package/`: builds and packages connectors for a release (see Release).

## License

MIT
