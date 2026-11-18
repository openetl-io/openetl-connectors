//go:build integration

package objstore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/openetl-io/openetl-connectors/objstore/azurestore"
	"github.com/openetl-io/openetl-connectors/objstore/gcsstore"
	"github.com/openetl-io/openetl-connectors/objstore/internal/testfiles"
	"github.com/openetl-io/openetl-connectors/objstore/s3store"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// TestStore is a store the tests run against.
type TestStore struct {
	Name string
	// A config of a prefix of its own.
	Config map[string]any
	// Changes to Config: an endpoint that doesn't answer, and a credential
	// the store refuses; nil for none.
	Unreachable, BadCredentials map[string]any
}

// Conformance runs the SDK's conformance suite on the store, with the
// connector's executables, cmd/ROLE-connector.
func Conformance(t *testing.T, connector string, st TestStore) {
	t.Helper()
	t.Run("destination", func(t *testing.T) {
		destinationConformance(t, conformance.Build(t, "../cmd/destination-"+connector), st)
	})
	t.Run("source", func(t *testing.T) { sourceConformance(t, conformance.Build(t, "../cmd/source-"+connector), st) })
	t.Run("splits", func(t *testing.T) { splitsOverTheProtocol(t, conformance.Build(t, "../cmd/source-"+connector), st) })
}

func destinationConformance(t *testing.T, exe string, st TestStore) {
	destinationSuite(t, exe, st, st.Config).Test(t)
	// The text formats, compressed.
	for _, format := range []string{formatCSV, formatJSONL} {
		t.Run(format, func(t *testing.T) {
			c := maps.Clone(st.Config)
			c["url"] = st.Config["url"].(string) + "/" + format
			c["format"], c["compression"] = format, "gzip"
			if format == formatJSONL {
				c["compression"] = "zstd"
			}
			s := destinationSuite(t, exe, st, c)
			if format == formatCSV {
				s.LossyTypes = csvLossyTypes
			}
			s.Test(t)
		})
	}
}

func sourceConformance(t *testing.T, exe string, st TestStore) {
	cfg := st.Config
	added := 0
	addFile := func(stream string, n int) {
		var b strings.Builder
		files := map[string][]byte{}
		switch stream {
		case "events", "all":
			b.WriteString("[")
			for i := range n {
				if i > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, `{"id":%d,"name":"e%d","at":"2026-01-02T03:04:05.%03dZ","price":"%d.25"}`, added+i, added+i, i, i)
			}
			b.WriteString("]")
			files[fmt.Sprintf("events/%04d.parquet", added)] = testfiles.ParquetOf(t, eventsSchema, b.String(), 4)
		case "people":
			b.WriteString("id,name,born\n")
			for i := range n {
				fmt.Fprintf(&b, "%d,\"p, %d\",2000-01-%02d\n", added+i, added+i, i%28+1)
			}
			files[fmt.Sprintf("people/%04d.csv.gz", added)] = testfiles.Gz(t, b.String())
		case "logs":
			for i := range n {
				fmt.Fprintf(&b, `{"id":%d,"level":"info","attrs":{"n":%d}}`+"\n", added+i, i)
			}
			files[fmt.Sprintf("logs/%04d.jsonl.zst", added)] = testfiles.Zst(t, b.String())
		case "avro":
			files[fmt.Sprintf("avro/%04d.avro", added)] = testfiles.AvroOf(t, added, n, 4)
		}
		added += n
		putFiles(t, cfg, files)
	}
	for _, stream := range []string{"events", "people", "logs", "avro"} {
		for range 3 {
			addFile(stream, 10)
		}
	}
	c := maps.Clone(cfg)
	c["streams"] = []map[string]any{
		{"name": "events", "glob": "events/*.parquet", "format": "parquet"},
		{"name": "all", "glob": "events/*.parquet", "format": "parquet"},
		{"name": "people", "glob": "people/*.csv.gz", "format": "csv", "columns": map[string]string{"born": "date"}},
		{"name": "logs", "glob": "logs/**", "format": "jsonl"},
		{"name": "avro", "glob": "avro/*.avro", "format": "avro"},
	}
	config, _ := json.Marshal(c)
	s := conformance.SourceSuite{
		Launch: conformance.Exec(exe),
		Config: config,
		Streams: []conformance.Stream{
			{Name: "events", Mode: source.Incremental},
			{Name: "all", Mode: source.FullRefresh},
			{Name: "people", Mode: source.Incremental},
			{Name: "logs", Mode: source.Incremental},
			{Name: "avro", Mode: source.Incremental},
		},
		AddRecords: func(_ context.Context, stream string) (int, error) {
			addFile(stream, 3)
			return 3, nil
		},
		StopsAtUntil: true,
	}
	if st.Unreachable != nil {
		s.Unreachable = withConfig(c, st.Unreachable)
	}
	if st.BadCredentials != nil {
		s.BadCredentials = withConfig(c, st.BadCredentials)
	}
	s.Test(t)
}

var eventsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: true},
	{Name: "price", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}, Nullable: true},
}, nil)

// What CSV doesn't keep, as the README's types table says.
var csvLossyTypes = map[string]string{
	"interval": "the JSON text of an object of months, days and nanoseconds",
}

func destinationSuite(t *testing.T, exe string, st TestStore, cfg map[string]any) conformance.DestinationSuite {
	t.Helper()
	b, _ := json.Marshal(cfg)
	settings, store := testStore(b)
	d, err := NewDestination(b, settings, store)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := d.bucket(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	s := conformance.DestinationSuite{
		Launch: conformance.Exec(exe),
		Config: b,
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readTable(ctx, d.store, bucket, namespace+"/"+stream)
		},
		Drop: func(ctx context.Context, namespace string) error {
			it := bucket.List(&blob.ListOptions{Prefix: namespace + "/"})
			for {
				obj, err := it.Next(ctx)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				if err := bucket.Delete(ctx, obj.Key); err != nil {
					return err
				}
			}
		},
	}
	if st.Unreachable != nil {
		s.Unreachable = withConfig(cfg, st.Unreachable)
	}
	if st.BadCredentials != nil {
		s.BadCredentials = withConfig(cfg, st.BadCredentials)
	}
	return s
}

func withConfig(cfg, changes map[string]any) []byte {
	c := maps.Clone(cfg)
	maps.Copy(c, changes)
	b, _ := json.Marshal(c)
	return b
}

// readTable reads a table's rows as its readers do: the files its manifest
// lists, if it has one, and otherwise the files in its directory, but
// those whose names start with _.
func readTable(ctx context.Context, st *store, b *blob.Bucket, table string) ([]map[string]any, error) {
	var keys []string
	listed, err := b.ReadAll(ctx, table+"/"+manifestPath)
	switch {
	case gcerrors.Code(err) == gcerrors.NotFound:
		it := b.List(&blob.ListOptions{Prefix: table + "/", Delimiter: "/"})
		for {
			obj, err := it.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			if !obj.IsDir && !strings.HasPrefix(strings.TrimPrefix(obj.Key, table+"/"), "_") {
				keys = append(keys, obj.Key)
			}
		}
	case err != nil:
		return nil, err
	default:
		for _, line := range strings.Fields(string(listed)) {
			k, ok := st.key(line)
			if !ok {
				return nil, errors.New("the manifest lists " + line + ", outside the URL")
			}
			keys = append(keys, k)
		}
	}
	var out []map[string]any
	for _, k := range keys {
		data, err := b.ReadAll(ctx, k)
		if err != nil {
			return nil, err
		}
		rows, err := readFile(ctx, k, data)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

// readFile reads a file's rows, by the format its name says: CSV cells as
// text, nil for an unquoted empty one, and JSON values as JSON has them.
func readFile(ctx context.Context, name string, data []byte) ([]map[string]any, error) {
	compression, base := compressionOf(name)
	r, err := decompress(compression, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	var out []map[string]any
	switch {
	case strings.HasSuffix(base, ".csv"):
		cr := newCSVReader(r, ',', '"')
		header, _, err := cr.read()
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		for {
			fields, quoted, err := cr.read()
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return nil, err
			}
			row := map[string]any{}
			for i, f := range fields {
				if f != "" || quoted[i] {
					row[header[i]] = f
				}
			}
			out = append(out, row)
		}
	case strings.HasSuffix(base, ".jsonl"):
		dec := json.NewDecoder(r)
		dec.UseNumber()
		for {
			var row map[string]any
			if err := dec.Decode(&row); errors.Is(err, io.EOF) {
				return out, nil
			} else if err != nil {
				return nil, err
			}
			out = append(out, row)
		}
	}
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1 << 16}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	tbl, err := fr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()
	tr := array.NewTableReader(tbl, -1)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := map[string]any{}
			for c, f := range rec.Schema().Fields() {
				row[f.Name] = rec.Column(c).GetOneForMarshal(i)
			}
			out = append(out, row)
		}
	}
	return out, nil
}

// splitsOverTheProtocol: Splits and Resplit over the protocol, through the built source: every
// record arrives once, across lanes and the splits Resplit adds, and the
// next read goes on from the splits the read ended with.
func splitsOverTheProtocol(t *testing.T, exe string, st TestStore) {
	launch := conformance.Exec(exe)
	cfg := st.Config
	files := map[string][]byte{}
	for i := range 240 {
		files[fmt.Sprintf("many/%03d.jsonl", i)] = fmt.Appendf(nil, "{\"id\":%d}\n{\"id\":%d}\n", i, i+1000)
	}
	putFiles(t, cfg, files)
	c := maps.Clone(cfg)
	c["streams"] = []map[string]any{{"name": "many", "glob": "many/*.jsonl", "format": "jsonl"}}
	config, _ := json.Marshal(c)
	p, err := launch(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	spec, err := protocolv1.NewConnectorServiceClient(p.Conn()).Spec(t.Context(), &protocolv1.SpecRequest{Host: conformance.Host})
	if err != nil {
		t.Fatal(err)
	}
	if !spec.GetSource().GetSplits() || !spec.GetSource().GetResplit() {
		t.Errorf("capabilities %v", spec.GetSource())
	}
	cl := protocolv1.NewSourceServiceClient(p.Conn())
	stream := &protocolv1.ConfiguredStream{Name: "many", SyncMode: protocolv1.SyncMode_SYNC_MODE_INCREMENTAL}
	var last []*protocolv1.Split
	halved := 0
	read := func() (map[string]int, map[string][]byte) {
		t.Helper()
		stream.Splits = nil
		res, err := cl.Splits(t.Context(), &protocolv1.SplitsRequest{Config: p.Config(), Stream: stream, Ended: last})
		if err != nil {
			t.Fatal(err)
		}
		stream.Splits = res.GetSplits()
		rs, err := cl.Read(t.Context(), &protocolv1.ReadRequest{Config: p.Config(), Pipeline: "p", Lanes: 3, Streams: []*protocolv1.ConfiguredStream{stream}})
		if err != nil {
			t.Fatal(err)
		}
		seen, ended, asked := map[string]int{}, map[string][]byte{}, map[string]bool{}
		for {
			m, err := rs.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			switch msg := m.GetMsg().(type) {
			case *protocolv1.ReadResponse_Batch:
				split := msg.Batch.GetSplit()
				if split == "" {
					t.Errorf("a batch of the whole stream")
				}
				for _, r := range msg.Batch.GetRows().GetRows() {
					var row map[string]any
					if err := json.Unmarshal(r, &row); err != nil {
						t.Fatal(err)
					}
					seen[fmt.Sprint(row[fileColumn], ":", row[lineColumn])]++
				}
				if !asked[split] {
					asked[split] = true
					// The read may have ended since the batch.
					res, err := cl.Resplit(t.Context(), &protocolv1.ResplitRequest{Config: p.Config(), Stream: "many", Split: split})
					if err != nil && !strings.Contains(err.Error(), "no read that splits again is running") {
						t.Fatal(err)
					}
					if res.GetAccepted() {
						halved++
					} else {
						// Asked again at its next batch, as a split
						// may run ahead of the batches received.
						asked[split] = false
					}
				}
			case *protocolv1.ReadResponse_Complete:
				ended[msg.Complete.GetSplit()] = msg.Complete.GetNext()
			}
		}
		last = nil
		for id, next := range ended {
			last = append(last, &protocolv1.Split{Id: id, State: next})
		}
		return seen, ended
	}
	seen, ended := read()
	if len(seen) != 480 {
		t.Errorf("%d distinct records of 480", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s arrived %d times", k, n)
		}
	}
	if halved == 0 || len(ended) != splitCount+halved {
		t.Errorf("%d splits completed, from %d and %d halved", len(ended), splitCount, halved)
	}
	putFiles(t, cfg, map[string][]byte{"many/new.jsonl": []byte("{\"id\":1}\n")})
	if seen, _ := read(); len(seen) != 1 {
		t.Errorf("the next read returned %v", seen)
	}
}

// The emulators' addresses, and Azurite's well-known account.
var (
	s3Endpoint    = cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	gcsEndpoint   = cmp.Or(os.Getenv("OPENETL_TEST_GCS_ENDPOINT"), "http://127.0.0.1:4443")
	azureEndpoint = cmp.Or(os.Getenv("OPENETL_TEST_AZURE_ENDPOINT"), "http://127.0.0.1:10000/devstoreaccount1")
)

const (
	azureAccount = "devstoreaccount1"
	azureKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	testBucket   = "openetl-test"
)

func prefix() string { return strings.ToLower(rand.Text())[:10] }

// FileStore is a local directory.
func FileStore(t *testing.T) TestStore {
	return TestStore{Name: "file", Config: map[string]any{"url": "file://" + t.TempDir()}}
}

// S3Store is the S3 store at OPENETL_TEST_S3_ENDPOINT, whose bucket it
// creates.
func S3Store(t *testing.T) TestStore {
	return TestStore{
		Name: "s3",
		Config: map[string]any{
			"url": "s3://" + s3Bucket(t) + "/" + prefix(), "endpoint": s3Endpoint, "region": "us-east-1",
			"accessKeyId": "openetl", "secretAccessKey": "openetl",
		},
		Unreachable: map[string]any{"endpoint": "http://127.0.0.1:1"},
	}
}

// GCSStore is fake-gcs-server at OPENETL_TEST_GCS_ENDPOINT, whose bucket it
// creates.
func GCSStore(t *testing.T) TestStore {
	return TestStore{
		Name:        "gcs",
		Config:      map[string]any{"url": "gs://" + gcsBucket(t) + "/" + prefix(), "endpoint": gcsEndpoint},
		Unreachable: map[string]any{"endpoint": "http://127.0.0.1:1"},
	}
}

// AzureStore is Azurite at OPENETL_TEST_AZURE_ENDPOINT, whose container it
// creates. Only Azurite of the emulators checks credentials.
func AzureStore(t *testing.T) TestStore {
	return TestStore{
		Name: "azure",
		Config: map[string]any{
			"url": "azblob://" + azureContainer(t) + "/" + prefix(), "endpoint": azureEndpoint, "accountName": azureAccount, "accountKey": azureKey,
		},
		Unreachable:    map[string]any{"endpoint": "http://127.0.0.1:1/" + azureAccount},
		BadCredentials: map[string]any{"accountKey": strings.Repeat("A", 86) + "=="},
	}
}

// testStore is the settings and the store of config's URL.
func testStore(config []byte) (any, NewStore) {
	var c struct {
		URL string `json:"url"`
	}
	_ = json.Unmarshal(config, &c)
	switch scheme, _, _ := strings.Cut(c.URL, ":"); scheme {
	case "s3":
		var s s3store.Config
		return &s, func(string) (Store, error) { return s3store.New(s) }
	case "gs":
		var s gcsstore.Config
		return &s, func(string) (Store, error) { return gcsstore.New(s) }
	case "azblob":
		var s azurestore.Config
		return &s, func(string) (Store, error) { return azurestore.New(s) }
	}
	return &struct{}{}, func(string) (Store, error) { return nil, nil }
}

// putFiles writes files to the store of cfg.
func putFiles(t *testing.T, cfg map[string]any, files map[string][]byte) {
	t.Helper()
	b, _ := json.Marshal(cfg)
	settings, store := testStore(b)
	d, err := NewDestination(b, settings, store)
	if err != nil {
		t.Fatal(err)
	}
	testfiles.Put(t, d.bucket, files)
}

func s3Bucket(t *testing.T) string {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, s3Endpoint+"/"+testBucket, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the S3 bucket (is the S3 store running at %s?): %v", s3Endpoint, err)
	}
	_ = res.Body.Close()
	return testBucket
}

func gcsBucket(t *testing.T) string {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, gcsEndpoint+"/storage/v1/b?project=openetl",
		bytes.NewReader([]byte(`{"name": "`+testBucket+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the GCS bucket (is fake-gcs-server running at %s?): %v", gcsEndpoint, err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		t.Fatalf("create the GCS bucket: %s", res.Status)
	}
	return testBucket
}

func azureContainer(t *testing.T) string {
	t.Helper()
	cred, err := container.NewSharedKeyCredential(azureAccount, azureKey)
	if err != nil {
		t.Fatal(err)
	}
	c, err := container.NewClientWithSharedKeyCredential(azureEndpoint+"/"+testBucket, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(t.Context(), nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		if errors.Is(err, t.Context().Err()) {
			t.Fatal(err)
		}
		t.Fatalf("create the Azure container (is Azurite running at %s?): %v", azureEndpoint, err)
	}
	return testBucket
}
