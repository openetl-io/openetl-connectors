# openetl-connectors

The official connectors for [OpenETL](https://github.com/openetl-io/openetl): the programs that read from and write to the systems OpenETL copies data between. Each one has a page with what it reads or writes, how to set it up, and what it does when a source's schema changes.

> [!NOTE]
> The connectors are in alpha, like OpenETL. Each has versions of its own, such as `0.1.0-alpha`, released when it changes. Their config, and the protocol between OpenETL and its connectors, can change between releases.

## Connectors

| Connector | As a source | As a destination |
|---|---|---|
| [PostgreSQL](./postgres/README.md) | Tables, in full, by a cursor column, or change by change through logical replication | Tables, by appending, overwriting or merging; exactly once |
| [MySQL and MariaDB](./mysql/README.md) | Tables, in full, by a cursor column, or change by change from the binlog | Tables, by appending, overwriting or merging; exactly once |
| [SQL Server](./sqlserver/README.md) | Tables and views, in full, by a cursor column, change by change through change data capture, or through change tracking as the rows that changed, with their latest values | Tables, by appending, overwriting or merging; exactly once |
| [Oracle Database](./oracle/README.md) | Tables, in full, by a cursor column, or change by change from the redo log through LogMiner | Tables, by appending, overwriting or merging; exactly once |
| [ClickHouse](./clickhouse/README.md) | Tables, in full or by a cursor column | Tables, by appending, overwriting or merging; exactly once |
| [MongoDB](./mongodb/README.md) | Collections, in full, by a cursor field, or change by change through change streams | Collections, by appending, overwriting or merging; exactly once |
| [Amazon DynamoDB](./dynamodb/README.md) | Tables, in full, by a cursor attribute, or change by change through DynamoDB Streams | Tables, by appending, overwriting or merging; exactly once |
| [Amazon Redshift](./redshift/README.md) | | Tables, by appending, overwriting or merging, loaded from Parquet on S3 by `COPY`; exactly once, unverified |
| [Kafka](./kafka/README.md) | Topics, in JSON, Avro or Protobuf | Topics, in one transaction per epoch; exactly once |
| [Amazon Kinesis](./kinesis/README.md) | Data streams, shard by shard | Data streams, a record per row; at least once |
| [Apache Iceberg](./iceberg/README.md) | | Tables, through a REST catalog or AWS Glue; exactly once |
| [Amazon S3](./s3/README.md) | Parquet, Avro, CSV and JSON Lines files, on S3, S3-compatible stores such as MinIO and Cloudflare R2, or a local disk | Parquet, CSV and JSON Lines files, on the same stores; exactly once |
| [Google Cloud Storage](./gcs/README.md) | Parquet, Avro, CSV and JSON Lines files | Parquet, CSV and JSON Lines files; exactly once |
| [Azure Blob Storage](./azureblob/README.md) | Parquet, Avro, CSV and JSON Lines files | Parquet, CSV and JSON Lines files; exactly once |

Exactly once is what a destination gives a stream whose source replays exactly: a change-by-change read does here, as do Kafka's and Kinesis's sources, and a full refresh does while its table is unchanged. Other reads, such as one by a cursor column, may repeat rows; each page says what they get.

## Use one

Each connector is a package in the official registry, `oci://ghcr.io/openetl-io/openetl-connectors`, which an OpenETL installation reads by default. List the connectors with OpenETL's CLI:

```sh
openetl connector catalog
```

Install one with the command that the catalog shows for it, which names its newest version:

```sh
openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/postgres:VERSION
```

A source or destination in OpenETL then names its connector, such as `connector: postgres`, with the config that the connector's page describes. The [quickstart](https://github.com/openetl-io/openetl/blob/main/docs/get-started/quickstart.md) installs the Postgres connector and runs a pipeline into Postgres. [Choose and manage connectors](https://github.com/openetl-io/openetl/blob/main/docs/pipelines/connectors.md) covers versions and other registries, and [OpenETL's docs](https://github.com/openetl-io/openetl/blob/main/docs/index.md) cover pipelines.

Each version's executables, checksums and signatures are also on its [GitHub release](https://github.com/openetl-io/openetl-connectors/releases), such as `postgres/v0.1.0-alpha`, with what changed in it.

## Contributing

[CONTRIBUTING.md](./CONTRIBUTING.md) has how to build and test the connectors, and [AGENTS.md](./AGENTS.md) the rules for changing them.

## License

[Apache 2.0](./LICENSE)
