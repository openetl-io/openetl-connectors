# Contributing to openetl-connectors

Thanks for helping. For a large change, open an issue first, so we can agree on the approach before you spend time on it. [AGENTS.md](./AGENTS.md) has the rules for changing the connectors, for people and coding agents alike.

## Build

```sh
go build -o bin/ ./cmd/...
```

The connectors build against the version of openetl's Go SDK, `github.com/openetl-io/openetl/sdk/go`, that `go.mod` requires: a release, `sdk/go/vX.Y.Z`, or between releases a pseudo-version of an openetl commit. To build against a checkout of openetl instead, while changing the SDK, say, use a workspace, which git ignores here:

```sh
go work init . ../openetl/sdk/go
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

The files tests, `objstore`'s, run against `file://` by default, and with `-tags integration` against the S3 store too, in a bucket they create, and against fake-gcs-server at `OPENETL_TEST_GCS_ENDPOINT` (default `http://127.0.0.1:4443`) and Azurite at `OPENETL_TEST_AZURE_ENDPOINT` (default `http://127.0.0.1:10000/devstoreaccount1`). Each files connector runs the conformance suite against its store.

The Kafka tests need a broker at `OPENETL_TEST_KAFKA_BROKERS` (default `127.0.0.1:9092`), a second one that takes SASL_SSL at `OPENETL_TEST_KAFKA_SASL_BROKERS` (default `127.0.0.1:9094`), and a Confluent-compatible schema registry at `OPENETL_TEST_SCHEMA_REGISTRY` (default `http://127.0.0.1:8081`). openetl's compose file runs Kafka and Karapace; the SASL_SSL broker, with its users and certificate, is the service `kafka-sasl` of `.github/actions/services`.

The Kinesis tests need a Kinesis endpoint at `OPENETL_TEST_KINESIS_ENDPOINT` (default `http://127.0.0.1:4568`), such as kinesis-mock's, which openetl's compose file runs, and the schema registry.

The other databases' tests each need their server, set up as `.github/actions/services` sets it up, which `.github/test-packages.sh` needs too:

- MySQL at `OPENETL_TEST_MYSQL_DSN` (default `openetl:openetl@tcp(127.0.0.1:3306)/openetl`), in the container `OPENETL_TEST_MYSQL_CONTAINER` (default `mysql`; `.github/test-packages.sh` always uses the containers `mysql` and `mariadb`), and MariaDB at `OPENETL_TEST_MARIADB_DSN` (default `openetl:openetl@tcp(127.0.0.1:3307)/openetl`), each with a row-based binlog and full row metadata;
- SQL Server at `OPENETL_TEST_SQLSERVER_DSN` (default `sqlserver://openetl:OpenETL-test-1@localhost:1433?database=openetl`), with SQL Server Agent, change tracking and change data capture on, and `sa` at `OPENETL_TEST_SQLSERVER_ADMIN_DSN`;
- Oracle Database at `OPENETL_TEST_ORACLE_DSN` (default `oracle://openetl:openetl@127.0.0.1:1521/FREEPDB1`), archiving its redo log with supplemental logging, and `SYS` at `OPENETL_TEST_ORACLE_ADMIN_DSN`;
- ClickHouse at `OPENETL_TEST_CLICKHOUSE_ADDR` (default `127.0.0.1:9000`), and for `.github/test-packages.sh` its HTTP interface at `OPENETL_TEST_CLICKHOUSE_HTTP` (default `http://127.0.0.1:8123`);
- MongoDB at `OPENETL_TEST_MONGODB_URI` (default `mongodb://openetl:openetl@127.0.0.1:27017/?directConnection=true`), a replica set;
- DynamoDB Local at `OPENETL_TEST_DYNAMODB_ENDPOINT` (default `http://127.0.0.1:8000`).

## CI

`ci` runs on every pull request and on pushes to `main` and release branches. Its job `ci` passes when every job it needs passed or had nothing to run, and is the one check a pull request needs.

- `check` runs on the whole repository, always: gofmt, `go vet -tags integration` and `go build`.
- `package NAME` checks one package: its `openetl-connector.yaml` with `openetl connector validate --require author,logo,description,license`; the unit tests of its directory and of `cmd/ROLE-NAME`; its integration tests, the SDK's conformance suite among them, against only the services it needs; and the package, built as a release builds it, through the conformance kit (`.github/test-packages.sh NAME`).
- `shared NAME` runs the tests of a shared Go package, such as `records`.

A pull request runs `package NAME` for each package whose directory or `cmd/*-NAME` it changes. A change to anything shared, another Go package, `go.mod`, `go.sum` or `.github/`, runs every package and shared package; a file at the root, such as this one, runs none. A push runs them all, and so does `nightly`. A release of a package needs its `package NAME` job to have passed on the tagged commit.

The `openetl` CLI is openetl's `main`, copied out of the image `ghcr.io/openetl-io/openetl:edge`, which openetl publishes from `main`, by `.github/actions/openetl`. There's no fallback: when the image can't be pulled, the job fails and says so, as `ci` is there to check the connectors against openetl's `main`. `.github/actions/services` starts the services a job needs, and caches are saved from pushes, and pull requests restore them.

`nightly` runs `ci` on `main`, every package, and checks what's released against what's coming in openetl: each package's newest version in the registry, through the same conformance kit, with the CLI of openetl's `main`. When a scheduled run fails, it opens an issue, or comments on the open one, and it closes the issue when a run passes again. `gh workflow run nightly.yml` runs it now, and touches no issue. `.github/test-packages.sh NAME=TARGET...` checks given packages, such as a released version (`postgres=oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha`), instead of packaging this checkout.

### A new connector

`ci` finds a package by its directory, so a new one is checked once it has:

- a top-level directory with its `openetl-connector.yaml`, with `author`, `logo`, `description` and `license`, its `README.md` and its config schemas;
- its executables in `cmd/source-NAME` and `cmd/destination-NAME`, which `openetl-connector.yaml` builds;
- a `conformance_test.go` that runs the SDK's conformance suite, and integration tests behind `//go:build integration` that read their service's address from an `OPENETL_TEST_…` variable, defaulting to the address `.github/actions/services` serves it at;
- its services in `SERVICES` in `.github/workflows/ci.yml`; a package not listed there gets every service, which works but is slower. A service that isn't there yet goes in `.github/actions/services`, and in `ALL_SERVICES`;
- a function `checks_NAME` in `.github/test-packages.sh`, which runs the conformance kit on each of its roles. Without one, its job fails.

## Release

Each package directory has an `openetl-connector.yaml`: its name, metadata, and how each role is built. The `openetl` CLI does the rest, so nothing here is specific to this repository:

```sh
openetl connector validate --require author,logo,description,license postgres mysql sqlserver oracle clickhouse mongodb dynamodb redshift kafka kinesis iceberg s3 gcs azureblob
openetl connector package --tag postgres/v0.2.0
```

`package` picks the directory whose package is `postgres`, builds each role for every platform into `dist/` (`postgres-source-linux-amd64` and the like), and writes `dist/postgres.connector.yaml`, whose binaries point at the `postgres/v0.2.0` release's assets. The executables and the descriptor are that release's assets, and `openetl connector apply -f postgres.connector.yaml` pins the version. Tags are per package: `postgres/v0.2.0`, `kafka/v0.1.3`.

**A release** is one package's tag, pushed: `PACKAGE/vX.Y.Z`, or `PACKAGE/vX.Y.Z-alpha.N`, `-beta.N` or `-rc.N` for a prerelease, on a commit in `main` that `ci` passed. Each package has versions of its own and is released when it has changed; nothing is edited before a release.

```sh
git tag postgres/v0.2.0-alpha.1 origin/main && git push origin postgres/v0.2.0-alpha.1
```

The push starts the `release` workflow, which releases that package alone. It refuses a tag on a commit that isn't in `main` or in a release branch (below), or on which `ci` hasn't passed with the package's job, `package PACKAGE`, and waits up to an hour for a run of `ci` that's still going; it refuses a version that's released already. Then it packages the package at the tag for Linux on amd64 and arm64, with the SDK that `go.mod` requires and the CLI of openetl's newest release, with checksums, SBOMs and third-party notices, pushes it to `ghcr.io/openetl-io/openetl-connectors/PACKAGE`, and publishes the release `PACKAGE/vX.Y.Z` with its files and notes generated from the pull requests since the package's previous tag, as a prerelease when the version has a suffix. A job in the `release` environment signs the package and its checksums with keyless cosign first. GHCR creates a new package private: after its first release, make it public in its settings, once; the run's summary says so. A run that failed can run again: it replaces its draft release. Once the release is published, the docs site builds again ([Docs](#docs)).

The CLI that packages a release is that of openetl's newest release, not `ci`'s `main`, so a package's descriptor is one a released `openetld` reads: the newest published release of openetl, drafts left out, whose tag is `vX.Y.Z` or a prerelease's (`sdk/go/v*` tags aren't), its `openetl-cli_VERSION_linux_ARCH.tar.gz` checked against the release's `checksums.txt`. The run's summary says which version it was.

`gh workflow run release.yml --ref BRANCH -f tag=postgres/v0.2.0-alpha.1` is a dry run: it packages that package at the branch's head and runs the checks, which only warn, and pushes, uploads and signs nothing. `-f openetl=v0.1.0-alpha` packages it with that openetl release's CLI instead of the newest.

A fix to a release that `main` has moved past goes on a branch `release-PACKAGE-X.Y`, made from the release's tag when it's first needed, and cherry-picked from `main`. The patch release is tagged there once `ci` passes on it.

## Docs

The [docs site](https://github.com/openetl-io/docs-web) shows each package from `main`: its page (`README.md` and any `docs/`), its schemas and its logo, with the package's releases and their notes. A fix to a page is on the site within minutes of reaching `main`; there's nothing to backport.

So a page can describe what no release has yet. Mark a setting or behaviour that isn't released with the version it arrives in, the package's next, such as "From 0.3.0", on the page and in the setting's `description` in the schema. A package with no release yet needs no marks, as none of it is released.

The `docs` workflow calls the site's deploy hook, the secret `CLOUDFLARE_DEPLOY_HOOK`, on a push to `main` that changes a package's page, `docs/`, `openetl-connector.yaml`, schemas or logo, and `release` calls it once a release is published, for the site's list of releases. Without the secret, it skips. `gh workflow run docs.yml` deploys now.

## Layout

Each package directory holds its `openetl-connector.yaml`, its config schemas and its page, `README.md`.

- `postgres/`: the Postgres source and destination.
- `mysql/`: the MySQL source and destination, which work against MariaDB too.
- `sqlserver/`: the SQL Server source and destination.
- `oracle/`: the Oracle Database source and destination.
- `clickhouse/`: the ClickHouse source and destination.
- `mongodb/`: the MongoDB source and destination.
- `dynamodb/`: the DynamoDB source and destination.
- `redshift/`: the Amazon Redshift destination.
- `iceberg/`: the Iceberg destination.
- `s3/`: files on Amazon S3, S3-compatible stores or a local disk: a source of Parquet, Avro, CSV and JSON Lines, and a destination that writes Parquet, CSV and JSON Lines.
- `gcs/`: the same files source and destination, on Google Cloud Storage.
- `azureblob/`: the same files source and destination, on Azure Blob Storage.
- `kafka/`: the Kafka source and destination.
- `kinesis/`: the Kinesis source and destination.
- `records/`: the record layout and formats Kafka and Kinesis share: JSON, and Avro and Protobuf with a schema registry.
- `keysplit/`: what the sources share of reading a table in splits of its key: the splits being read, which `Resplit` divides, an incremental read's floor, how a table's key is cut into ranges, what's left of an integer key's range, a `database/sql` read's connections, and how a read's splits run alongside its cdc.
- `objstore/`: what the files connectors share: their source and destination, and each store's part in `s3store/`, `gcsstore/` and `azurestore/`, so a connector's executable has only its own cloud's SDK. `Conformance`, with the `integration` tag, runs the conformance suite on a connector's store.
- `sqlstage/`: what the SQL destinations share: their `Write`, `Patch`, `PreCommit`, `Commit` and `Recover` over each one's SQL, their tables' and staging tables' names, the descriptor `PreCommit` returns, and a `database/sql` destination's fence and markers, and how it applies a stream's staging.
- `docstore/`: what the MongoDB and DynamoDB connectors share: a source's batch of text columns, and a destination's markers, how it settles an epoch's staged streams, and its lists of names.
- `connectortest/`: what the connectors' tests share: recorders of what a read emits, and a check's items' statuses.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.
