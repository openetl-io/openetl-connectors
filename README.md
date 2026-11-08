# openetl-connectors

The official connectors for [OpenETL](https://github.com/openetl-io/openetl): the programs that read from and write to the systems OpenETL copies data between. Each one has a page with what it reads or writes, how to set it up, and what it does when a source's schema changes.

> [!NOTE]
> Pre-alpha, like OpenETL: nothing has been released yet, and the connectors change with OpenETL's protocols and SDK.

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

A source or destination in OpenETL names its connector, such as `connector: postgres`, with the config its page describes. The [quickstart](https://github.com/openetl-io/openetl/blob/main/docs/get-started/quickstart.md) builds these connectors and runs a pipeline into Postgres, and [OpenETL's docs](https://github.com/openetl-io/openetl/blob/main/docs/index.md) cover pipelines.

## Contributing

[CONTRIBUTING.md](./CONTRIBUTING.md) has how to build and test the connectors, and [AGENTS.md](./AGENTS.md) the rules for changing them.

## License

[Apache 2.0](./LICENSE)
