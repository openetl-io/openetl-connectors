//go:build integration

package dynamodb

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func testSource(t *testing.T, extra string) *Source {
	t.Helper()
	s, err := NewSource([]byte("{" + connConfig(endpoint(), "openetl") + extra + "}"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func discovered(t *testing.T, s *Source, name string) source.CatalogStream {
	t.Helper()
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cat.Streams {
		if cs.Name == name {
			return cs
		}
	}
	t.Fatalf("discovery has no %s", name)
	return source.CatalogStream{}
}

func read(t *testing.T, s *Source, cs source.ConfiguredStream) (*connectortest.Recorder, error) {
	t.Helper()
	rec := &connectortest.Recorder{}
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, rec)
	return rec, err
}

// Each attribute type is declared as attrTypes says, and read as its text,
// numbers with every digit.
func TestSourceTypes(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "kinds"
	createTable(t, cl, name, "", key{"id", types.ScalarAttributeTypeN})
	put(t, cl, name, kinds()...)
	s := testSource(t, fmt.Sprintf(`, "tables": [%q]`, name))
	cs := discovered(t, s, name)
	want := map[string]arrow.DataType{
		"id": arrow.PrimitiveTypes.Int64, "s": arrow.BinaryTypes.String, "n": &arrow.Decimal128Type{Precision: 38, Scale: 0},
		"f": decimalType, "b": arrow.BinaryTypes.Binary, "bool": arrow.FixedWidthTypes.Boolean, "null": arrow.BinaryTypes.String,
		"m": jsonType, "l": jsonType, "ss": jsonType, "ns": jsonType, "bs": jsonType,
		"i": arrow.PrimitiveTypes.Int64, "varies": arrow.BinaryTypes.String, "wide": arrow.BinaryTypes.String,
	}
	for _, f := range cs.Schema.Fields() {
		if w, ok := want[f.Name]; !ok || !arrow.TypeEqual(f.Type, w) {
			t.Errorf("%s is %v, want %v", f.Name, f.Type, w)
		}
		delete(want, f.Name)
	}
	if len(want) > 0 {
		t.Errorf("discovery has no %v", want)
	}
	if cs.Schema.Field(0).Name != "id" || cs.Schema.Field(0).Nullable || !slices.Equal(cs.PrimaryKey, []string{"id"}) {
		t.Errorf("the key isn't first, or is nullable: %v", cs.Schema)
	}
	rec, err := read(t, s, source.ConfiguredStream{Name: name, SyncMode: source.FullRefresh})
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]map[string]string{}
	for _, r := range rec.RowsOf(name) {
		rows[r["id"]] = r
	}
	for id, w := range map[string]map[string]string{
		"1": {
			"s": "héllo ✓", "n": "12345678901234567890123456789012345678", "f": "-1.5", "b": "AP8=", "bool": "true",
			"m": `{"a":[1,"x"]}`, "l": `[` + "1" + string(make0(125)) + `,null]`, "ss": `["a","b"]`, "ns": `[1,2.5]`, "bs": `["AQ==","Ag=="]`,
			"i": "9223372036854775807", "varies": "1", "wide": "0." + string(make0(129)) + "1",
		},
		"2": {"i": "-9223372036854775808", "f": "0.000000001", "varies": "text", "s": "", "wide": "99999999999999999999999999999999999999" + string(make0(88))},
		"3": {"varies": "{}", "l": "[]"},
	} {
		for k, v := range w {
			if got := rows[id][k]; got != v {
				t.Errorf("item %s's %s = %q, want %q", id, k, got, v)
			}
		}
	}
	if _, ok := rows["1"]["null"]; ok {
		t.Errorf("a NULL read as %q, want null", rows["1"]["null"])
	}
}

// A table read in segments reads every item once, in splits that resume
// from their states.
func TestSourceSplits(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "split"
	createTable(t, cl, name, "", key{"pk", types.ScalarAttributeTypeS}, key{"sk", types.ScalarAttributeTypeN})
	for i := range 200 {
		put(t, cl, name, map[string]types.AttributeValue{"pk": avS(strconv.Itoa(i % 13)), "sk": avI(i)})
	}
	s := testSource(t, fmt.Sprintf(`, "tables": [%q], "segments": 4, "scanLimit": 7`, name))
	cs := source.ConfiguredStream{Name: name, SyncMode: source.FullRefresh}
	splits, err := s.Splits(t.Context(), cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) != 4 {
		t.Fatalf("%d splits, want 4", len(splits))
	}
	seen := map[string]int{}
	var resumed []source.Split
	for _, sp := range splits {
		rec := &connectortest.Recorder{}
		cs.Splits = []source.Split{sp}
		if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, rec); err != nil {
			t.Fatal(err)
		}
		for _, r := range rec.RowsOf(name) {
			seen[r["sk"]]++
		}
		// The split's first resume point, as a crash after it leaves it.
		for _, e := range rec.Snapshot() {
			if e.Resume {
				resumed = append(resumed, source.Split{ID: sp.ID, State: e.State})
				break
			}
		}
	}
	if len(seen) != 200 {
		t.Errorf("read %d items, want 200", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("item %s read %d times", k, n)
		}
	}
	// Splits again, with resume points, are those splits, which read on
	// from them.
	again, err := s.Splits(t.Context(), cs, resumed)
	if err != nil || len(again) != len(resumed) {
		t.Fatalf("splits from states: %v, %v", again, err)
	}
	rec := &connectortest.Recorder{}
	cs.Splits = again
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, rec); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.RowsOf(name)); n >= 200 || n == 0 {
		t.Errorf("reading on from resume points read %d items, want fewer than 200", n)
	}
}

// An item with an attribute the run doesn't declare fails the read,
// retryably, saying to read every item where discovery samples.
func TestNewAttributeFailsRetryably(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "new"
	createTable(t, cl, name, "", key{"id", types.ScalarAttributeTypeN})
	put(t, cl, name, map[string]types.AttributeValue{"id": avI(1), "v": avS("a")}, map[string]types.AttributeValue{"id": avI(2), "rare": avS("b")})
	s := testSource(t, fmt.Sprintf(`, "tables": [%q], "sampleSize": 1`, name))
	_, err := read(t, s, source.ConfiguredStream{Name: name, SyncMode: source.FullRefresh, Columns: []string{"id", "v"}})
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Fatalf("a new attribute: %v (%v), want TRANSIENT", err, connectortest.CodeOf(err))
	}
	// Discovery of every item declares it.
	if cs := discovered(t, testSource(t, fmt.Sprintf(`, "tables": [%q]`, name)), name); index(cs.Schema, "rare") < 0 {
		t.Errorf("discovery of every item doesn't declare rare: %v", cs.Schema)
	}
}

// Discovery from a sample reads about sampleSize items, from segments of
// the table.
func TestSampledDiscovery(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "sample"
	createTable(t, cl, name, "", key{"id", types.ScalarAttributeTypeN})
	for i := range 300 {
		put(t, cl, name, map[string]types.AttributeValue{"id": avI(i), fmt.Sprint("a", i%3): avI(i)})
	}
	// DynamoDB Local's ItemCount is what DescribeTable says, which it keeps
	// as AWS does, updated now and then: the sample is from segments when
	// it's past sampleSize, else from the table's start.
	cs := discovered(t, testSource(t, fmt.Sprintf(`, "tables": [%q], "sampleSize": 30`, name)), name)
	for _, a := range []string{"id", "a0", "a1", "a2"} {
		if index(cs.Schema, a) < 0 {
			t.Errorf("a sample of 30 has no %s: %v", a, cs.Schema)
		}
	}
}

// An incremental read reads the items whose cursor is at or past the
// highest the read before the last one saw: the first two read every item,
// and each read after them from the highest cursor two reads before.
func TestIncrementalFloor(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "inc"
	createTable(t, cl, name, "", key{"id", types.ScalarAttributeTypeN})
	for i := range 10 {
		put(t, cl, name, map[string]types.AttributeValue{"id": avI(i), "at": avI(100 + i)})
	}
	s := testSource(t, fmt.Sprintf(`, "tables": [%q]`, name))
	cs := source.ConfiguredStream{Name: name, SyncMode: source.Incremental, Cursor: "at"}
	next := func() []byte {
		rec, err := read(t, s, cs)
		if err != nil {
			t.Fatal(err)
		}
		st, complete := rec.Last(name)
		if !complete {
			t.Fatal("the read didn't complete")
		}
		cs.State = st
		return st
	}
	next()
	// An item written after the first read, at 105, below the highest it
	// saw: the second read, which has no floor yet, reads it.
	put(t, cl, name, map[string]types.AttributeValue{"id": avI(0), "at": avI(105)})
	rec, err := read(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(rec.RowsOf(name), func(r map[string]string) bool { return r["id"] == "0" && r["at"] == "105" }) {
		t.Errorf("the second read missed an item written below the first read's highest cursor: %v", rec.RowsOf(name))
	}
	cs.State, _ = rec.Last(name)
	next()
	// Then, with nothing new, a read reads the items at its floor, 109.
	rec, err = read(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	if rows := rec.RowsOf(name); len(rows) != 1 || rows[0]["at"] != "109" {
		t.Errorf("reads with nothing new read %v, want the item at the floor, 109", rows)
	}
	var st segState
	if err := json.Unmarshal(cs.State, &st); err != nil || st.Floor == nil || stringOf(st.Floor.AttributeValue) != "" || st.Floor.AttributeValue.(*types.AttributeValueMemberN).Value != "109" {
		t.Errorf("state %s, want a floor of N 109", cs.State)
	}
}

// A table with no stream isn't offered for cdc; one with a stream is, with
// a seq.
func TestCDCOffered(t *testing.T) {
	cl := testClient(t)
	p := prefix(t)
	createTable(t, cl, p+"plain", "", key{"id", types.ScalarAttributeTypeN})
	createTable(t, cl, p+"streamed", types.StreamViewTypeNewImage, key{"id", types.ScalarAttributeTypeN})
	s := testSource(t, fmt.Sprintf(`, "tables": [%q, %q]`, p+"plain", p+"streamed"))
	if slices.Contains(discovered(t, s, p+"plain").SyncModes, source.CDC) {
		t.Error("a table without a stream is offered for cdc")
	}
	if cs := discovered(t, s, p+"streamed"); !slices.Contains(cs.SyncModes, source.CDC) || cs.Seq == nil {
		t.Errorf("a table with a stream: %v, seq %v", cs.SyncModes, cs.Seq)
	}
	_, err := read(t, s, source.ConfiguredStream{Name: p + "plain", SyncMode: source.CDC})
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("cdc of a table without a stream: %v", err)
	}
}

// Packed, a table keyed on a number, whose key attributes are reserved
// words, is discovered by its keys alone, and read whole.
func TestPackedDiscoveryKeys(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "ns"
	createTable(t, cl, name, "", key{"status", types.ScalarAttributeTypeN}, key{"order", types.ScalarAttributeTypeS})
	put(t, cl, name, map[string]types.AttributeValue{"status": avN("12.5"), "order": avS("first"), "payload": avS("full")})
	s := testSource(t, fmt.Sprintf(`, "tables": [%q], "packed": true`, name))
	f := discovered(t, s, name).Schema.Fields()
	if len(f) != 3 || f[0].Name != "status" || !arrow.TypeEqual(f[0].Type, decimalType) || f[1].Name != "order" || f[2].Name != dataColumn {
		t.Errorf("fields %v", f)
	}
	rec, err := read(t, s, source.ConfiguredStream{Name: name, SyncMode: source.FullRefresh})
	if err != nil {
		t.Fatal(err)
	}
	if rows := rec.RowsOf(name); len(rows) != 1 || rows[0]["status"] != "12.5" || rows[0][dataColumn] != `{"order":{"S":"first"},"payload":{"S":"full"},"status":{"N":"12.5"}}` {
		t.Errorf("rows %v", rows)
	}
}
