//go:build integration

package dynamodb

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func openWriter(t *testing.T, ns string, streams ...destination.Stream) *writer {
	return openGeneration(t, ns, 1, streams...)
}

func openGeneration(t *testing.T, ns string, gen uint64, streams ...destination.Stream) *writer {
	t.Helper()
	d, err := NewDestination([]byte("{" + connConfig(endpoint(), testAccessKeyID) + "}"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "p", Generation: gen, Namespace: ns, Streams: streams, Scope: destination.ScopePipeline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Recover(t.Context(), gen, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	return w.(*writer)
}

// batch builds rows of int64, string and microsecond timestamp columns.
func batch(t *testing.T, fields []arrow.Field, rows ...[]any) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer b.Release()
	for _, r := range rows {
		for i, v := range r {
			switch fb := b.Field(i).(type) {
			case *array.Int64Builder:
				fb.Append(int64(v.(int)))
			case *array.StringBuilder:
				fb.Append(v.(string))
			case *array.TimestampBuilder:
				fb.Append(arrow.Timestamp(v.(int)))
			}
		}
	}
	return b.NewRecordBatch()
}

var meta = []arrow.Field{
	{Name: idColumn, Type: arrow.BinaryTypes.String}, {Name: opColumn, Type: arrow.BinaryTypes.String},
	{Name: seqColumn, Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
}

// A column the table or an index is keyed on is refused a type change, with
// what makes it take it; a table keyed on other attributes than its stream
// fails the sync.
func TestDestinationRefusesKeyTypeChanges(t *testing.T) {
	cl := testClient(t)
	ns := prefix(t) + "ns"
	if _, err := cl.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(ns + ".t"), BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("g"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String("by_g"), KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("g"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	s := destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}}
	w := openWriter(t, ns, s)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "g", Type: arrow.PrimitiveTypes.Int64}}, nil)
	got, err := w.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "t", Schema: schema}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Refused) != 1 || got.Refused[0].Column != "g" || got.Refused[0].FromType != "S" || got.Refused[0].ToType != "N" || !strings.Contains(got.Refused[0].Fix, "by_g") {
		t.Errorf("refused %+v", got.Refused)
	}
	if l := got.Limits["t"]; !slicesEqual(l.NotNull, "id") || !slicesEqual(l.Unholdable, "id") || len(l.Fixed) != 2 {
		t.Errorf("limits %+v", l)
	}
	// The stream's key as text, where the table's is a number.
	schema = arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	got, err = w.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "t", Schema: schema}})
	if err != nil || len(got.Refused) != 1 || got.Refused[0].Column != "id" || !strings.Contains(got.Refused[0].Fix, "delete-table") {
		t.Errorf("a key of another type: %+v, %v", got.Refused, err)
	}
	// A stream keyed on another column than the table.
	w2 := openWriter(t, ns, destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"g"}})
	schema = arrow.NewSchema([]arrow.Field{{Name: "g", Type: arrow.BinaryTypes.String}}, nil)
	if _, err := w2.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "t", Schema: schema}}); connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("a table keyed on another attribute: %v", err)
	}
}

func slicesEqual(a []string, b ...string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

// An item past 400 KB is a data error.
func TestDestinationItemTooLarge(t *testing.T) {
	ns := prefix(t) + "ns"
	s := destination.Stream{Name: "t", Mode: destination.Append}
	w := openWriter(t, ns, s)
	fields := append([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, meta...)
	b := batch(t, fields, []any{strings.Repeat("x", 401*1024), "00000000-0000-0000-0000-000000000001", "insert", 1})
	defer b.Release()
	err := w.Write(t.Context(), destination.Epoch{Generation: 1, N: 1}, "t", b)
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID {
		t.Errorf("a 401 KB item: %v (%v), want DATA_INVALID", err, connectortest.CodeOf(err))
	}
}

// A key of three columns keys the table on _openetl_key, and an older
// version doesn't replace a newer one.
func TestDestinationCompositeKey(t *testing.T) {
	cl := testClient(t)
	ns := prefix(t) + "ns"
	s := destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"a", "b", "c"}}
	w := openWriter(t, ns, s)
	fields := append([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64}, {Name: "b", Type: arrow.BinaryTypes.String},
		{Name: "c", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String},
	}, meta...)
	if _, err := w.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "t", Schema: arrow.NewSchema(fields, nil)}}); err != nil {
		t.Fatal(err)
	}
	now := int(time.Now().UnixMicro())
	for n, row := range [][]any{
		{1, "x", 2, "new", "00000000-0000-0000-0000-000000000002", "update", now},
		{1, "x", 2, "old", "00000000-0000-0000-0000-000000000001", "insert", now - 1},
		{1, "x", 3, "other", "00000000-0000-0000-0000-000000000003", "insert", now},
	} {
		e := destination.Epoch{Generation: 1, N: uint64(n + 1)}
		b := batch(t, fields, row)
		if err := w.Write(t.Context(), e, "t", b); err != nil {
			t.Fatal(err)
		}
		b.Release()
		staged, err := w.PreCommit(t.Context(), e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Commit(t.Context(), e, []destination.Staged{staged}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := readTable(t.Context(), cl, ns+".t")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, it := range items {
		got[it[keyColumn].(string)] = it["v"].(string)
	}
	if len(got) != 2 || got[`{"L":[{"N":"1"},{"S":"x"},{"N":"2"}]}`] != "new" {
		t.Errorf("items %v", got)
	}
	d, err := cl.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: aws.String(ns + ".t")})
	if err != nil {
		t.Fatal(err)
	}
	if k := tableKey(d.Table); len(k) != 1 || k[0].name != keyColumn || d.Table.BillingModeSummary.BillingMode != types.BillingModePayPerRequest {
		t.Errorf("table key %v, billing %v", k, d.Table.BillingModeSummary)
	}
}

// commitRows writes rows to stream as epoch e and commits it.
func commitRows(t *testing.T, w *writer, e destination.Epoch, stream string, fields []arrow.Field, rows ...[]any) destination.Committed {
	t.Helper()
	if _, err := w.ApplySchema(t.Context(), []destination.StreamSchema{{Name: stream, Schema: arrow.NewSchema(fields, nil)}}); err != nil {
		t.Fatal(err)
	}
	b := batch(t, fields, rows...)
	defer b.Release()
	if err := w.Write(t.Context(), e, stream, b); err != nil {
		t.Fatal(err)
	}
	staged, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.Commit(t.Context(), e, []destination.Staged{staged})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Appended without deduplication, two rows alike, with one _openetl_id,
// both land; deduplicated on the target, a row whose _openetl_id the table
// has is skipped, and counted; deduplicated within each epoch, a repeat in
// the epoch is skipped, and counted, and a later epoch's row lands beside.
func TestDestinationAppendIdentity(t *testing.T) {
	cl := testClient(t)
	ns := prefix(t) + "ns"
	w := openWriter(t, ns, destination.Stream{Name: "all", Mode: destination.Append}, destination.Stream{Name: "once", Mode: destination.Append, Dedupe: destination.DedupeTarget},
		destination.Stream{Name: "epoch", Mode: destination.Append, Dedupe: destination.DedupeEpoch})
	fields := append([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, meta...)
	row := []any{"same", "00000000-0000-0000-0000-000000000001", "insert", 1}
	commitRows(t, w, destination.Epoch{Generation: 1, N: 1}, "all", fields, row, row)
	commitRows(t, w, destination.Epoch{Generation: 1, N: 2}, "once", fields, row)
	c := commitRows(t, w, destination.Epoch{Generation: 1, N: 3}, "once", fields, row)
	if c.Collapsed["once"] != 1 {
		t.Errorf("collapsed %v, want once: 1", c.Collapsed)
	}
	c = commitRows(t, w, destination.Epoch{Generation: 1, N: 4}, "epoch", fields, row, row)
	commitRows(t, w, destination.Epoch{Generation: 1, N: 5}, "epoch", fields, row)
	if c.Collapsed["epoch"] != 1 {
		t.Errorf("collapsed %v, want epoch: 1", c.Collapsed)
	}
	for stream, want := range map[string]int{"all": 2, "once": 1, "epoch": 2} {
		items, err := readTable(t.Context(), cl, ns+"."+stream)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != want {
			t.Errorf("%s has %d items, want %d: %v", stream, len(items), want, items)
		}
	}
}

// A hard delete removes the item, unless the item has a newer version.
func TestDestinationHardDelete(t *testing.T) {
	cl := testClient(t)
	ns := prefix(t) + "ns"
	w := openWriter(t, ns, destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete})
	fields := append([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String}}, meta...)
	commitRows(t, w, destination.Epoch{Generation: 1, N: 1}, "t", fields,
		[]any{1, "a", "00000000-0000-0000-0000-000000000001", "insert", 10},
		[]any{2, "b", "00000000-0000-0000-0000-000000000002", "insert", 30})
	commitRows(t, w, destination.Epoch{Generation: 1, N: 2}, "t", fields,
		[]any{1, "", "00000000-0000-0000-0000-000000000003", "delete", 20},
		[]any{2, "", "00000000-0000-0000-0000-000000000004", "delete", 20})
	items, err := readTable(t.Context(), cl, ns+".t")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0]["v"] != "b" {
		t.Errorf("items %v, want 2 alone, whose version is newer than its delete", items)
	}
}

// An overwrite read whose swap a crash cut short keeps its rows through
// the next attempt's StartReads, and Recover finishes the swap.
func TestDestinationPendingSwap(t *testing.T) {
	cl := testClient(t)
	ns := prefix(t) + "ns"
	s := destination.Stream{Name: "t", Mode: destination.Overwrite, PrimaryKey: []string{"id"}}
	w := openWriter(t, ns, s)
	fields := append([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String}}, meta...)
	first := time.Now().Add(-time.Hour)
	if err := w.StartReads(t.Context(), map[string]time.Time{"t": first}); err != nil {
		t.Fatal(err)
	}
	e := destination.Epoch{Generation: 1, N: 1}
	commitRows(t, w, e, "t", fields, []any{1, "new", "00000000-0000-0000-0000-000000000001", "insert", 1})
	// The read ended in e, and the crash came after its marker, before the
	// swap.
	if err := w.SetMarkers(t.Context(), []marker{{Stream: "t", Generation: 1, N: 1, Swap: readName("p", "t", first), Target: ns + ".t", Key: s.PrimaryKey}}); err != nil {
		t.Fatal(err)
	}
	if err := w.StartReads(t.Context(), map[string]time.Time{"t": time.Now()}); err != nil {
		t.Fatal(err)
	}
	openGeneration(t, ns, 2, s)
	items, err := readTable(t.Context(), cl, ns+".t")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0]["v"] != "new" {
		t.Errorf("items %v, want the read's row", items)
	}
}

// Teardown removes the pipeline's fence item and its staging, an epoch
// staged and not committed too, and no other pipeline's; again, it removes
// nothing more and doesn't fail.
func TestDestinationTeardown(t *testing.T) {
	cl := testClient(t)
	ns := prefix(t) + "ns"
	s := destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}}
	w := openWriter(t, ns, s)
	fields := append([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String}}, meta...)
	commitRows(t, w, destination.Epoch{Generation: 1, N: 1}, "t", fields, []any{1, "a", "00000000-0000-0000-0000-000000000001", "insert", 1})
	b := batch(t, fields, []any{2, "b", "00000000-0000-0000-0000-000000000002", "insert", 2})
	defer b.Release()
	if err := w.Write(t.Context(), destination.Epoch{Generation: 1, N: 2}, "t", b); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PreCommit(t.Context(), destination.Epoch{Generation: 1, N: 2}); err != nil {
		t.Fatal(err)
	}
	d, err := NewDestination([]byte("{" + connConfig(endpoint(), testAccessKeyID) + "}"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "q", Generation: 1, Namespace: ns, Streams: []destination.Stream{s}, Scope: destination.ScopePipeline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Recover(t.Context(), 1, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	control := ns + "." + controlTable
	pipelineOf := func() map[string]int {
		items, err := readTable(t.Context(), cl, control)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]int{}
		for _, it := range items {
			p, _ := it["p"].(string)
			switch {
			case p == "fence#p" || strings.HasPrefix(p, stagePrefix("p")) || strings.HasPrefix(p, readPrefix("p")):
				out["p"]++
			case p == "fence#q":
				out["q"]++
			default:
				out["other"]++
			}
		}
		return out
	}
	before := pipelineOf()
	if before["p"] < 2 || before["q"] != 1 {
		t.Fatalf("before teardown: %v, want p's fence and staging, and q's fence", before)
	}
	for range 2 {
		if err := d.Teardown(t.Context(), "p"); err != nil {
			t.Fatal(err)
		}
		if got := pipelineOf(); got["p"] != 0 || got["q"] != 1 || got["other"] != before["other"] {
			t.Errorf("after teardown: %v, want q's fence alone of %v", got, before)
		}
	}
	if items, err := readTable(t.Context(), cl, ns+".t"); err != nil || len(items) != 1 {
		t.Errorf("the table has %v, %v; want its row, which teardown keeps", items, err)
	}
}
