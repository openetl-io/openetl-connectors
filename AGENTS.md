# Working on openetl-connectors

For people and coding agents. [README.md](README.md) has the connectors, how to build and test them, and the layout; this file has the rules that aren't obvious from the code.

## Before a commit

- `git config core.hooksPath .githooks` once: the pre-commit hook runs gofmt and `go vet -tags integration` on the packages a commit touches, as CI does.
- Work on a branch from `main` and open a pull request. A change that needs openetl's SDK or protocol too goes on a branch of the same name in both repositories: CI checks a pull request here against openetl's branch of its name when there is one, and against `main` otherwise.

## Rules

- **This repository holds connector code and its tests only.** Packaging, validating `openetl-connector.yaml`, release names and any other tooling are `openetl connector …` commands in [openetl](https://github.com/openetl-io/openetl), so a third-party connector needs nothing from here. Don't add scripts.
- **A destination's commit is its own:** staging, applying, markers, fencing and catalogs live in its `Writer` (`PreCommit`, `Commit`, `Recover`), not in openetl's runner or engine.
- **No connector changes a column's type by itself,** on targets, staging or anything that becomes the user's table, topic schema or files, unless the pipeline's spec allows the change. A destination refuses it through `destination.RefusedColumn`, whose `Fix` says what a person can do to make the change (Postgres's `ALTER TABLE … ALTER COLUMN … TYPE`), and the stream holds the column where it can. New columns still land.
- Each connector runs the SDK's conformance suite (`sdk/go/conformance`) in its `conformance_test.go`. A new connector does too.
- **Comments** only where they're needed: contracts, behaviour that isn't obvious, units, and what nil or zero means. Don't restate names, and don't point to documents outside these repositories.
- **No `//nolint`.** Fix what the linter found; for integer conversions, bound the value first or make the types match.
