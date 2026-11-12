# Working on openetl-connectors

For people and coding agents. [CONTRIBUTING.md](CONTRIBUTING.md) has how to build and test the connectors, and the layout; each package's `README.md` is its page; this file has the rules that aren't obvious from the code.

## Before a commit

- `git config core.hooksPath .githooks` once: the pre-commit hook runs gofmt and `go vet -tags integration` on the packages a commit touches, as CI does.
- Work on a branch from `main` and open a pull request. A change that needs openetl's SDK or protocol too goes on a branch of the same name in both repositories: CI checks a pull request here against openetl's branch of its name when there is one, and against `main` otherwise.

## Rules

- **This repository holds connector code and its tests only.** Packaging, validating `openetl-connector.yaml`, release names and any other tooling are `openetl connector …` commands in [openetl](https://github.com/openetl-io/openetl), so a third-party connector needs nothing from here. Don't add scripts.
- **A destination's commit is its own:** staging, applying, markers, fencing and catalogs live in its `Writer` (`PreCommit`, `Commit`, `Recover`), not in openetl's runner or engine.
- **No connector changes a column's type by itself,** on targets, staging or anything that becomes the user's table, topic schema or files, unless the pipeline's spec allows the change. A destination refuses it through `destination.RefusedColumn`, whose `Fix` says what a person can do to make the change (Postgres's `ALTER TABLE … ALTER COLUMN … TYPE`), and the stream holds the column where it can. New columns still land.
- **Each package has versions of its own.** A release is its tag, `PACKAGE/vX.Y.Z` or a prerelease's `PACKAGE/vX.Y.Z-alpha.N`, `-beta.N` or `-rc.N`, pushed; nothing committed names a version. [CONTRIBUTING.md](CONTRIBUTING.md#release) has how.
- Each connector runs the SDK's conformance suite (`sdk/go/conformance`) in its `conformance_test.go`. A new connector does too.
- **Comments** only where they're needed: contracts, behaviour that isn't obvious, units, and what nil or zero means. Don't restate names, and don't point to documents outside these repositories.
- **No `//nolint`.** Fix what the linter found; for integer conversions, bound the value first or make the types match.

## Docs

- **Each package's page is its `README.md`,** which GitHub shows in the package's folder and OpenETL's docs site lists under Connectors. It follows the format and style rules in [openetl's docs/AGENTS.md](https://github.com/openetl-io/openetl/blob/main/docs/AGENTS.md): frontmatter with `title` and `description`, no `# H1`, relative links that start with `./` or `../`, GitHub's alert syntax, no raw HTML.
- A page covers, for each role: what it reads or writes and how, how to set it up (permissions, server settings, catalogs), a minimal config, the delivery guarantee it gives, how types map, what it refuses and holds, and its limits, stated plainly. Only what the code and its tests show.
- **The config schemas are the config's reference.** The site renders `source.schema.json` and `destination.schema.json`, so a page links them, shows a minimal config, and explains only the settings that need judgment. Every property has a `description`.
- **A change and its docs land together.** A change to what a connector reads, writes, needs or refuses updates its page and its schema's descriptions in the same pull request.
- **The site shows each package as released.** It builds a package's page, schemas and logo from its release line's branch, `release-X.Y`, which holds the newest release and fixes to its docs since; `main` runs ahead. A fix to a released package's page goes to `main` and is backported: [CONTRIBUTING.md](CONTRIBUTING.md#backports) has when and how.
- Pages here are about the connectors. Pipelines, the CLI and OpenETL itself are in [openetl's docs](https://github.com/openetl-io/openetl/tree/main/docs): link to them rather than explaining them again.
