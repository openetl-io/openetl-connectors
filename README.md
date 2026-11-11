# openetl-connectors

The official connectors for [OpenETL](https://github.com/openetl-io/openetl): the programs that read from and write to the systems OpenETL copies data between. Each one has a page with what it reads or writes, how to set it up, and what it does when a source's schema changes.

> [!NOTE]
> The connectors are in alpha, like OpenETL. In the alpha, they have the same version as OpenETL, such as `0.1.0-alpha`. Their config, and the protocol between OpenETL and its connectors, can change between releases.

## Connectors

| Connector | As a source | As a destination |
|---|---|---|
| [PostgreSQL](./postgres/README.md) | Tables, in full, by a cursor column, or change by change through logical replication | Tables, by appending, overwriting or merging; exactly once |
| [Kafka](./kafka/README.md) | Topics, in JSON, Avro or Protobuf | Topics, in one transaction per epoch; exactly once |
| [Amazon Kinesis](./kinesis/README.md) | Data streams, shard by shard | Data streams, a record per row; at least once |
| [Apache Iceberg](./iceberg/README.md) | | Tables, through a REST catalog or AWS Glue; exactly once |
| [Amazon S3](./s3/README.md) | | Parquet files, on S3 or a local disk; exactly once |

Exactly once is what a destination gives a stream whose source replays exactly, as every source here does except Postgres read by a cursor column; each page says what the rest get.

## Use one

Each connector is a package in the official registry, `oci://ghcr.io/openetl-io/openetl-connectors`, which an OpenETL installation reads by default. List the connectors with OpenETL's CLI:

```sh
openetl connector catalog
```

Install one with the command that the catalog shows for it:

```sh
openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha
```

A source or destination in OpenETL then names its connector, such as `connector: postgres`, with the config that the connector's page describes. The [quickstart](https://github.com/openetl-io/openetl/blob/main/docs/get-started/quickstart.md) installs the Postgres connector and runs a pipeline into Postgres. [Choose and manage connectors](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/connectors.md) covers versions and other registries, and [OpenETL's docs](https://github.com/openetl-io/openetl/blob/main/docs/index.md) cover pipelines.

Each package's executables, checksums and signatures are also on its GitHub release, such as [`postgres/v0.1.0-alpha`](https://github.com/openetl-io/openetl-connectors/releases/tag/postgres/v0.1.0-alpha).

## Contributing

[CONTRIBUTING.md](./CONTRIBUTING.md) has how to build and test the connectors, and [AGENTS.md](./AGENTS.md) the rules for changing them.

## License

[Apache 2.0](./LICENSE)
