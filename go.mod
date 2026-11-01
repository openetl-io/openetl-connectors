module github.com/unsuitable001/openetl-connectors

go 1.26.0

toolchain go1.26.10

require (
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/jackc/pgx/v5 v5.12.0
	github.com/unsuitable001/openetl/sdk/go v0.0.0
)

require (
	github.com/andybalholm/brotli v1.2.3 // indirect
	github.com/apache/thrift v0.24.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// Until the SDK is published, the connectors build against a checkout of
// openetl next to this one.
replace github.com/unsuitable001/openetl/sdk/go => ../openetl/sdk/go
