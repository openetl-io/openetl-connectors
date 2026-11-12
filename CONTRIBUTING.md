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

The S3 destination's tests run against `file://` by default, and with `-tags integration` against the S3 store too, in a bucket they create.

The Kafka tests need a broker at `OPENETL_TEST_KAFKA_BROKERS` (default `127.0.0.1:9092`) and a Confluent-compatible schema registry at `OPENETL_TEST_SCHEMA_REGISTRY` (default `http://127.0.0.1:8081`). openetl's compose file runs Kafka and Karapace.

The Kinesis tests need a Kinesis endpoint at `OPENETL_TEST_KINESIS_ENDPOINT` (default `http://127.0.0.1:4568`), such as kinesis-mock's, which openetl's compose file runs, and the schema registry.

## CI

`ci` runs on every pull request and on pushes to `main` and release branches: gofmt, `go vet`, `openetl connector validate`, the tests against Postgres, the Iceberg catalogs, Kafka and Kinesis, and each package, as a release packages it, through the conformance kit (`.github/test-packages.sh`). Its job `ci` passes when every other job did, and is the one check a pull request needs. The `openetl` CLI it uses is openetl's `main`, copied out of the image `ghcr.io/openetl-io/openetl:edge`, which openetl publishes from `main`; until that image exists, it's the CLI of openetl's newest release. Caches are saved from pushes, and pull requests restore them. `.github/actions/services` starts the services for `ci` and `nightly` alike.

`nightly` checks what's released against what's coming in openetl: each package's newest version in the registry, through the same conformance kit, with the CLI of openetl's `main`. When a scheduled run fails, it opens an issue, or comments on the open one, and it closes the issue when a run passes again. `gh workflow run nightly.yml` runs it now, and touches no issue. `.github/test-packages.sh NAME=TARGET...` checks given packages, such as a released version (`postgres=oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha`), instead of packaging this checkout.

## Release

Each package directory has an `openetl-connector.yaml`: its name, metadata, and how each role is built. The `openetl` CLI does the rest, so nothing here is specific to this repository:

```sh
openetl connector validate --require author,logo,description,license postgres kafka kinesis iceberg s3
openetl connector package --tag postgres/v0.2.0
```

`package` picks the directory whose package is `postgres`, builds each role for every platform into `dist/` (`postgres-source-linux-amd64` and the like), and writes `dist/postgres.connector.yaml`, whose binaries point at the `postgres/v0.2.0` release's assets. The executables and the descriptor are that release's assets, and `openetl connector apply -f postgres.connector.yaml` pins the version. Tags are per package: `postgres/v0.2.0`, `kafka/v0.1.3`.

**A release** is one package's tag, pushed: `PACKAGE/vX.Y.Z`, or `PACKAGE/vX.Y.Z-alpha.N`, `-beta.N` or `-rc.N` for a prerelease, on a commit in `main` that `ci` passed. Each package has versions of its own and is released when it has changed; nothing is edited before a release.

```sh
git tag postgres/v0.2.0-alpha.1 origin/main && git push origin postgres/v0.2.0-alpha.1
```

The push starts the `release` workflow, which releases that package alone. It refuses a tag on a commit that isn't in `main` or in a release branch (below), or that `ci` hasn't passed on, and waits up to an hour for a run of `ci` that's still going; it refuses a version that's released already. Then it packages the package at the tag for Linux on amd64 and arm64, with the SDK that `go.mod` requires and the CLI that `ci` uses, with checksums, SBOMs and third-party notices, pushes it to `ghcr.io/openetl-io/openetl-connectors/PACKAGE`, and publishes the release `PACKAGE/vX.Y.Z` with its files and notes generated from the pull requests since the package's previous tag, as a prerelease when the version has a suffix. A job in the `release` environment signs the package and its checksums with keyless cosign first. GHCR creates a new package private: after its first release, make it public in its settings, once; the run's summary says so. A run that failed can run again: it replaces its draft release. Once the release is published, the docs site builds again ([Docs](#docs)).

`gh workflow run release.yml --ref BRANCH -f tag=postgres/v0.2.0-alpha.1` is a dry run: it packages that package at the branch's head and runs the checks, which only warn, and pushes, uploads and signs nothing.

A fix to a release that `main` has moved past goes on a branch `release-PACKAGE-X.Y`, made from the release's tag when it's first needed, and cherry-picked from `main`. The patch release is tagged there once `ci` passes on it.

## Docs

The [docs site](https://github.com/openetl-io/docs-web) shows each package as released: its page (`README.md` and any `docs/`), its schemas and its logo come from the tag of its newest published release, and from `main` for a package that has no release yet. So a fix to a released package's page shows on the site with the package's next release.

The `docs` workflow calls the site's deploy hook, the secret `CLOUDFLARE_DEPLOY_HOOK`, on a push to `main` that changes a package's page, `docs/`, `openetl-connector.yaml`, schemas or logo, and `release` calls it once a release is published. Without the secret, it skips. `gh workflow run docs.yml` deploys now.

## Layout

Each package directory holds its `openetl-connector.yaml`, its config schemas and its page, `README.md`.

- `postgres/`: the Postgres source and destination.
- `iceberg/`: the Iceberg destination.
- `s3/`: the Parquet on S3 destination.
- `kafka/`: the Kafka source and destination.
- `kinesis/`: the Kinesis source and destination.
- `records/`: the record layout and formats Kafka and Kinesis share: JSON, and Avro and Protobuf with a schema registry.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.
