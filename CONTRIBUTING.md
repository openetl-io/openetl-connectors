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

**A release** is a tag, `vVERSION` such as `v0.1.0-alpha`, on a commit that `ci` passed, released with the `release` workflow. It's on a commit in `main`, or in its release line's branch for a patch to a line that `main` has moved past (see [Release branches](#release-branches)). In the alpha, openetl has the same tag, and its CLI and SDK at that tag build the packages, so tag openetl first.

```sh
git tag v0.1.0-alpha && git push origin v0.1.0-alpha
gh workflow run release.yml -f tag=v0.1.0-alpha -f publish=public
```

It packages every connector for Linux on amd64 and arm64, with checksums, SBOMs and third-party notices. `publish` says what happens to them:

- `public` pushes the packages to `ghcr.io/openetl-io/openetl-connectors` and publishes a release per package, `postgres/v0.1.0-alpha` and so on, which makes those tags. A job in the `release` environment signs each package and its checksums with keyless cosign first. Then it points the release line's branch at the release. GHCR creates a package private: after the first public release, make each package and `openetl-connectors/index` public in their settings, once.
- `private` pushes the packages to the private `ghcr.io/openetl-io/candidates/VERSION/openetl-connectors` and puts the files on draft releases. Nothing is signed. `gh workflow run release-discard.yml -f version=VERSION` removes both.
- `skip` packages and checks everything, and pushes nothing.

Running it again replaces the drafts. It refuses a version that's already released.

**A release candidate** is built once and tested as built. `gh workflow run release-candidate.yml -f version=0.1.0-alpha -f commit=SHA -f openetl=SHA` packages that commit, once `ci` has passed on it, for Linux on amd64 and arm64. The packages go to `ghcr.io/openetl-io/openetl-connectors`, private until the release is published, and each gets a draft release, which makes no tag until it's published. OpenETL's test suites install them from there, as a user does. `gh workflow run release-discard.yml -f version=VERSION` deletes a candidate's versions and drafts, and refuses a version that's been published.

### Release branches

Each release line, such as 0.1, has a branch, `release-0.1`, which holds the line's newest release and the fixes to it since. The [docs site](https://github.com/openetl-io/docs-web) builds each package's page from it, so a fix to a released package's page reaches readers without a release, while `main` runs ahead with what isn't released. Publishing a release points its line's branch at it: it makes the branch of a new line, with the label that backports to it, and moves the branch of a line released from `main` to the release. A branch that has commits the release doesn't, which were backported from `main`, starts again at the release, and the run's summary says where it was. A patch to a line that `main` has moved past is a pull request to the line's branch, tagged there once `ci` passes on it.

### Backports

A fix goes to `main` first. When it also fixes a released package, label its pull request `backport release-X.Y`, before it's merged or after. The `backport` workflow then opens a pull request of it against `release-X.Y`. Where it doesn't apply cleanly, the workflow comments with the commands to backport it by hand.

Backport a change to a page that corrects or completes what the released package does: a wrong setting, type mapping or permission, a missing step, a limit the release has. Don't backport what only `main` does, or a schema description of a setting the release doesn't have. A fix to text that `main` no longer has is a pull request to the release branch alone. When the next release is close, the fix can wait for it instead.

The workflow pushes a backport with the deploy key in the secret `BACKPORT_DEPLOY_KEY`, which has write access, so `ci` runs on it: GitHub starts no workflow for what a workflow's own token pushes. It opens the pull request with its own token, which **Allow GitHub Actions to create and approve pull requests**, in the repository's Actions settings, permits. Without the key, `ci` runs on a backport once it's merged; without the setting, the workflow comments with a link to open the pull request.

The docs site checks this repository every 15 minutes, and builds when a package's page, schemas or `openetl-connector.yaml` changed on its release branch, or a release was published.

## Layout

Each package directory holds its `openetl-connector.yaml`, its config schemas and its page, `README.md`.

- `postgres/`: the Postgres source and destination.
- `iceberg/`: the Iceberg destination.
- `s3/`: the Parquet on S3 destination.
- `kafka/`: the Kafka source and destination.
- `kinesis/`: the Kinesis source and destination.
- `records/`: the record layout and formats Kafka and Kinesis share: JSON, and Avro and Protobuf with a schema registry.
- `cmd/<executable>/`: one `main` per connector, which hands the connector to `sdk.Serve`.
