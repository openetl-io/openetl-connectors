//go:build integration

package oracle

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The privileges the README gives the destination's user: to write to its
// own schema, and to write to and create other schemas.
var (
	ownGrants = []string{"CREATE SESSION", "CREATE TABLE"}
	anyGrants = append(slices.Clone(ownGrants), "CREATE USER", "CREATE ANY TABLE", "ALTER ANY TABLE", "SELECT ANY TABLE",
		"INSERT ANY TABLE", "UPDATE ANY TABLE", "DELETE ANY TABLE", "CREATE ANY INDEX")
)

// dsnAs is testDSN signing in as user.
func dsnAs(user, password string) string {
	u, err := url.Parse(testDSN())
	if err != nil {
		panic(err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}

// destUser creates a user with an unlimited quota on USERS and grants,
// dropped with all it made when the test ends, and returns its DSN.
func destUser(t *testing.T, name string, grants ...string) string {
	t.Helper()
	admin := adminDB(t)
	dropUser(t, admin, name)
	exec(t, admin, "CREATE USER "+quoteIdent(name)+` IDENTIFIED BY "pw_`+strings.ToLower(name)+`" QUOTA UNLIMITED ON USERS`,
		"GRANT "+strings.Join(grants, ", ")+" TO "+quoteIdent(name))
	t.Cleanup(func() { dropUser(t, admin, name) })
	return dsnAs(name, "pw_"+strings.ToLower(name))
}

func dropUser(t *testing.T, admin *sql.DB, name string) {
	t.Helper()
	_, err := admin.ExecContext(context.Background(), "DROP USER "+quoteIdent(name)+" CASCADE")
	if err != nil && oraCode(err) != 1918 {
		t.Fatalf("drop user %s: %v", name, err)
	}
}

var dayToSecond = regexp.MustCompile(`^[+-]\d+ \d{2}:\d{2}:\d{2}(\.\d+)?$`)

// readTable reads a table as its readers see it, by column name: BOOLEAN
// as bool, an INTERVAL DAY TO SECOND as a time.Duration, and a JSON
// object of months, days and nanoseconds decoded. None if it doesn't exist.
func readTable(ctx context.Context, db *sql.DB, schema, table string) ([]map[string]any, error) {
	types := map[string]string{}
	cols, err := db.QueryContext(ctx, "SELECT column_name, data_type, data_precision, data_scale FROM all_tab_columns WHERE owner = :1 AND table_name = :2", schema, table)
	if err != nil {
		return nil, err
	}
	for cols.Next() {
		var c, typ string
		var prec, scale sql.NullInt64
		if err := cols.Scan(&c, &typ, &prec, &scale); err != nil {
			_ = cols.Close()
			return nil, err
		}
		if typ == "NUMBER" && prec.Valid && prec.Int64 == 1 && (!scale.Valid || scale.Int64 == 0) {
			typ = "NUMBER(1)"
		}
		types[c] = typ
	}
	_ = cols.Close()
	if len(types) == 0 {
		return nil, cols.Err()
	}
	rows, err := db.QueryContext(ctx, "SELECT * FROM "+quoteIdent(schema)+"."+quoteIdent(table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, n := range names {
			row[n] = readValue(types[n], vals[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func readValue(typ string, v any) any {
	if typ == "NUMBER(1)" {
		switch x := v.(type) {
		case int64:
			return x != 0
		case float64:
			return x != 0
		case string:
			return x == "1"
		case bool:
			return x
		}
	}
	s, ok := v.(string)
	switch {
	case !ok:
		return v
	case typ == "BOOLEAN":
		return s == "1"
	case strings.HasPrefix(typ, "INTERVAL DAY") && dayToSecond.MatchString(s):
		days, rest, _ := strings.Cut(strings.TrimLeft(s, "+"), " ")
		d, err := time.ParseDuration(strings.Replace(strings.Replace(rest, ":", "h", 1), ":", "m", 1) + "s")
		if err != nil || days != "00" && days != "0" {
			return s
		}
		return d
	case strings.HasPrefix(s, `{"months": `):
		var m map[string]any
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		if dec.Decode(&m) == nil {
			return m
		}
	}
	return s
}

type destEnv struct {
	t     *testing.T
	d     *Destination
	admin *sql.DB
	// The user, whose schema takes the tables of a pipeline without a
	// namespace.
	user string
}

func newDestEnv(t *testing.T, user string, grants ...string) *destEnv {
	t.Helper()
	d, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q}`, destUser(t, user, grants...)))
	if err != nil {
		t.Fatal(err)
	}
	return &destEnv{t: t, d: d, admin: adminDB(t), user: user}
}

// open opens a writer of generation gen, recovered from nothing.
func (e *destEnv) open(gen uint64, cfg destination.OpenConfig) *writer {
	e.t.Helper()
	cfg.Pipeline, cfg.Generation = cmp.Or(cfg.Pipeline, "p1"), gen
	w, err := e.d.Open(e.t.Context(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	if _, err := w.Recover(e.t.Context(), gen, destination.Epoch{}, nil); err != nil {
		e.t.Fatal(err)
	}
	return w.(*writer)
}

type streamBatch struct {
	stream string
	rec    arrow.RecordBatch
}

// stage writes epoch n's batches and pre-commits it.
func (e *destEnv) stage(w *writer, n uint64, batches ...streamBatch) destination.Staged {
	e.t.Helper()
	ep := destination.Epoch{Generation: w.cfg.Generation, N: n}
	for _, b := range batches {
		if err := w.Write(e.t.Context(), ep, b.stream, b.rec); err != nil {
			e.t.Fatalf("write %s: %v", b.stream, err)
		}
	}
	st, err := w.PreCommit(e.t.Context(), ep)
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *destEnv) commit(w *writer, n uint64, batches ...streamBatch) destination.Committed {
	e.t.Helper()
	st := e.stage(w, n, batches...)
	c, err := w.Commit(e.t.Context(), destination.Epoch{Generation: w.cfg.Generation, N: n}, []destination.Staged{st})
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// rows reads a table, as "col=value" cells of each row joined by ", ", in
// order.
func (e *destEnv) rows(schema, table string, cols ...string) []string {
	e.t.Helper()
	rows, err := readTable(e.t.Context(), e.admin, schema, table)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []string
	for _, r := range rows {
		cells := make([]string, len(cols))
		for i, c := range cols {
			v := r[c]
			if v == nil {
				v = "<null>"
			}
			cells[i] = fmt.Sprintf("%s=%v", c, v)
		}
		out = append(out, strings.Join(cells, ", "))
	}
	slices.Sort(out)
	return out
}

func batchOf(t *testing.T, fields []arrow.Field, rows string) arrow.RecordBatch {
	t.Helper()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields, nil), strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Release)
	return rec
}

// rowID is a row version's _openetl_id as JSON gives a FixedSizeBinary.
func rowID(n int) string {
	b := make([]byte, 16)
	b[15] = byte(n)
	return base64.StdEncoding.EncodeToString(b)
}

var (
	idField   = arrow.Field{Name: colID, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.NewMetadata([]string{metaExtension}, []string{"arrow.uuid"})}
	opField   = arrow.Field{Name: colOp, Type: arrow.BinaryTypes.String, Nullable: true}
	seqField  = arrow.Field{Name: colSeq, Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}, Nullable: true}
	delField  = arrow.Field{Name: colDeleted, Type: arrow.FixedWidthTypes.Boolean}
	unchField = arrow.Field{Name: colUnchanged, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
)

func equal(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s:\n got  %q\n want %q", what, got, want)
	}
}

// TestDestinationAppendDedupes appends three streams, deduped not at all,
// against the target, and within each epoch, and counts what each skipped.
func TestDestinationAppendDedupes(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_APPEND", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{
		{Name: "PLAIN", Mode: destination.Append}, {Name: "TARGET", Mode: destination.Append, Dedupe: destination.DedupeTarget}, {Name: "EPOCH", Mode: destination.Append, Dedupe: destination.DedupeEpoch},
	}})
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, idField}
	all := func(rows string) []streamBatch {
		b := batchOf(t, fields, rows)
		return []streamBatch{{"PLAIN", b}, {"TARGET", b}, {"EPOCH", b}}
	}
	c1 := e.commit(w, 1, all(fmt.Sprintf(`[{"id": 1, "_openetl_id": %q}, {"id": 2, "_openetl_id": %q}, {"id": 3, "_openetl_id": %q}]`, rowID(1), rowID(2), rowID(2)))...)
	c2 := e.commit(w, 2, all(fmt.Sprintf(`[{"id": 4, "_openetl_id": %q}, {"id": 5, "_openetl_id": %q}]`, rowID(2), rowID(3)))...)
	equal(t, "PLAIN", e.rows(e.user, "PLAIN", "id"), []string{"id=1", "id=2", "id=3", "id=4", "id=5"})
	equal(t, "TARGET", e.rows(e.user, "TARGET", "id"), []string{"id=1", "id=2", "id=5"})
	equal(t, "EPOCH", e.rows(e.user, "EPOCH", "id"), []string{"id=1", "id=2", "id=4", "id=5"})
	if c1.Collapsed["TARGET"] != 1 || c1.Collapsed["EPOCH"] != 1 || c2.Collapsed["TARGET"] != 1 || c2.Collapsed["EPOCH"] != 0 || c1.Collapsed["PLAIN"] != 0 {
		t.Errorf("collapsed %v, then %v", c1.Collapsed, c2.Collapsed)
	}
}

// TestDestinationMergeDeletes merges by _openetl_seq, which an older
// version doesn't overwrite, with soft and hard deletes.
func TestDestinationMergeDeletes(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_MERGE", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{
		{Name: "SOFT", Mode: destination.Merge, PrimaryKey: []string{"k"}},
		{Name: "HARD", Mode: destination.Merge, PrimaryKey: []string{"k"}, Deletes: destination.HardDelete},
	}})
	fields := []arrow.Field{{Name: "k", Type: arrow.BinaryTypes.String}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}, opField, seqField, delField}
	both := func(rows string) []streamBatch {
		b := batchOf(t, fields, rows)
		return []streamBatch{{"SOFT", b}, {"HARD", b}}
	}
	e.commit(w, 1, both(`[{"k": "a", "v": "a1", "_openetl_op": "insert", "_openetl_seq": "10", "_openetl_deleted": false},
		{"k": "b", "v": "b1", "_openetl_op": "insert", "_openetl_seq": "10", "_openetl_deleted": false},
		{"k": "a", "v": "a2", "_openetl_op": "update", "_openetl_seq": "12", "_openetl_deleted": false},
		{"k": "a", "v": "a0", "_openetl_op": "update", "_openetl_seq": "11", "_openetl_deleted": false},
		{"k": "c", "v": "c1", "_openetl_op": "insert", "_openetl_seq": "5", "_openetl_deleted": false}]`)...)
	e.commit(w, 2, both(`[{"k": "a", "v": "older", "_openetl_op": "update", "_openetl_seq": "11", "_openetl_deleted": false},
		{"k": "b", "v": null, "_openetl_op": "delete", "_openetl_seq": "20", "_openetl_deleted": true},
		{"k": "c", "v": "c2", "_openetl_op": "update", "_openetl_seq": null, "_openetl_deleted": false},
		{"k": "d", "v": null, "_openetl_op": "delete", "_openetl_seq": "30", "_openetl_deleted": true}]`)...)
	cols := []string{"k", "v", colOp, colDeleted}
	equal(t, "SOFT", e.rows(e.user, "SOFT", cols...), []string{
		"k=a, v=a2, _openetl_op=update, _openetl_deleted=false",
		"k=b, v=b1, _openetl_op=delete, _openetl_deleted=true",
		"k=c, v=c2, _openetl_op=update, _openetl_deleted=false",
		"k=d, v=<null>, _openetl_op=delete, _openetl_deleted=true",
	})
	equal(t, "HARD", e.rows(e.user, "HARD", cols...), []string{
		"k=a, v=a2, _openetl_op=update, _openetl_deleted=false",
		"k=c, v=c2, _openetl_op=update, _openetl_deleted=false",
	})
}

// TestDestinationMergeUnchanged merges updates that didn't send a column:
// it keeps the target's value, or takes the epoch's latest earlier version
// that sent it.
func TestDestinationMergeUnchanged(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_UNCH", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "U", Mode: destination.Merge, PrimaryKey: []string{"k"}}}})
	fields := []arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "big", Type: arrow.BinaryTypes.String, Nullable: true}, opField, seqField, unchField}
	commit := func(n uint64, rows string) { e.commit(w, n, streamBatch{"U", batchOf(t, fields, rows)}) }
	commit(1, `[{"k": 1, "v": "a1", "big": "B1", "_openetl_op": "insert", "_openetl_seq": "1"},
		{"k": 2, "v": "x1", "big": null, "_openetl_op": "update", "_openetl_seq": "1", "_openetl_unchanged": ["big"]}]`)
	commit(2, `[{"k": 1, "v": "a2", "big": null, "_openetl_op": "update", "_openetl_seq": "2", "_openetl_unchanged": ["big"]}]`)
	commit(3, `[{"k": 1, "v": "a3", "big": "B3", "_openetl_op": "update", "_openetl_seq": "3"},
		{"k": 1, "v": "a4", "big": null, "_openetl_op": "update", "_openetl_seq": "4", "_openetl_unchanged": ["big"]},
		{"k": 2, "v": "x2", "big": null, "_openetl_op": "update", "_openetl_seq": "2", "_openetl_unchanged": ["big"]}]`)
	cols := []string{"k", "v", "big", colUnchanged}
	equal(t, "U", e.rows(e.user, "U", cols...), []string{
		"k=1, v=a4, big=B3, _openetl_unchanged=<null>",
		`k=2, v=x2, big=<null>, _openetl_unchanged=["big"]`,
	})
}

// An epoch whose rows have _openetl_unchanged, none listing a column,
// applies.
func TestDestinationMergeNoneUnchanged(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_UNCHNONE", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "U", Mode: destination.Merge, PrimaryKey: []string{"k"}}}})
	fields := []arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}, opField, seqField, unchField}
	e.commit(w, 1, streamBatch{"U", batchOf(t, fields, `[{"k": 1, "v": "a", "_openetl_op": "insert", "_openetl_seq": "1"}]`)})
	e.commit(w, 2, streamBatch{"U", batchOf(t, fields, `[{"k": 1, "v": "b", "_openetl_op": "update", "_openetl_seq": "2"}]`)})
	equal(t, "U", e.rows(e.user, "U", "k", "v", colUnchanged), []string{"k=1, v=b, _openetl_unchanged=<null>"})
}

// A column the latest version didn't send keeps the target's value where
// the staged version that sent it is older than the target's row.
func TestDestinationMergeUnchangedOlderThanTarget(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_UNCHOLD", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "U", Mode: destination.Merge, PrimaryKey: []string{"k"}}}})
	fields := []arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, opField, seqField, unchField}
	commit := func(n uint64, rows string) { e.commit(w, n, streamBatch{"U", batchOf(t, fields, rows)}) }
	commit(1, `[{"k": 1, "v": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": "10"},
		{"k": 2, "v": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": "10"}]`)
	commit(2, `[{"k": 1, "v": "old", "n": 5, "_openetl_op": "update", "_openetl_seq": "5"},
		{"k": 1, "v": null, "n": 11, "_openetl_op": "update", "_openetl_seq": "11", "_openetl_unchanged": ["v"]},
		{"k": 2, "v": "newer", "n": 12, "_openetl_op": "update", "_openetl_seq": "12"},
		{"k": 2, "v": null, "n": 13, "_openetl_op": "update", "_openetl_seq": "13", "_openetl_unchanged": ["v"]}]`)
	equal(t, "U", e.rows(e.user, "U", "k", "v", "n", colUnchanged), []string{
		"k=1, v=current, n=11, _openetl_unchanged=<null>",
		"k=2, v=newer, n=13, _openetl_unchanged=<null>",
	})
}

// With held columns too, a held column comes with its hold from a newer
// version than the target's row, and not from an older one.
func TestDestinationMergeHeldUnchangedOlderThanTarget(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_HELDOLD", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "U", Mode: destination.Merge, PrimaryKey: []string{"k"}}}})
	heldField := arrow.Field{Name: destination.HeldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	fields := []arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, idField, opField, seqField, unchField, heldField}
	commit := func(n uint64, rows string) { e.commit(w, n, streamBatch{"U", batchOf(t, fields, rows)}) }
	commit(1, fmt.Sprintf(`[{"k": 1, "v": "current", "n": 10, "_openetl_id": %q, "_openetl_op": "insert", "_openetl_seq": "10"},
		{"k": 2, "v": "current", "n": 10, "_openetl_id": %q, "_openetl_op": "insert", "_openetl_seq": "10"}]`, rowID(1), rowID(2)))
	commit(2, fmt.Sprintf(`[{"k": 1, "v": null, "n": 5, "_openetl_id": %q, "_openetl_op": "update", "_openetl_seq": "5", "_openetl_held": ["v"]},
		{"k": 1, "v": null, "n": 11, "_openetl_id": %q, "_openetl_op": "update", "_openetl_seq": "11", "_openetl_unchanged": ["v"]},
		{"k": 2, "v": null, "n": 12, "_openetl_id": %q, "_openetl_op": "update", "_openetl_seq": "12", "_openetl_held": ["v"]},
		{"k": 2, "v": null, "n": 13, "_openetl_id": %q, "_openetl_op": "update", "_openetl_seq": "13", "_openetl_unchanged": ["v"]}]`, rowID(3), rowID(4), rowID(5), rowID(6)))
	got := e.rows(e.user, "U", "k", "v", "n", colUnchanged, destination.HeldColumn)
	if len(got) != 2 || got[0] != "k=1, v=current, n=11, _openetl_unchanged=<null>, _openetl_held=<null>" ||
		!strings.HasPrefix(got[1], "k=2, v=<null>, n=13, _openetl_unchanged=<null>, _openetl_held={") {
		t.Fatalf("got %q", got)
	}
}

// textOf is a value as readTable gives it, as text: times in UTC, and long
// text and binary values by their length.
func textOf(v any) string {
	switch x := v.(type) {
	case nil:
		return "<null>"
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05.999999999")
	case []byte:
		if len(x) > 32 {
			return fmt.Sprintf("%d bytes", len(x))
		}
		return fmt.Sprintf("%x", x)
	case string:
		if len(x) > 64 {
			return fmt.Sprintf("%d bytes", len(x))
		}
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case map[string]any:
		b, _ := json.Marshal(x)
		return string(b)
	}
	return fmt.Sprint(v)
}

// TestDestinationTypesRoundTrip writes every Arrow type the destination
// maps, at its edges and as nulls, and reads it back with its column types.
func TestDestinationTypesRoundTrip(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_TYPES", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "T", Mode: destination.Append}}})
	jsonExt := arrow.NewMetadata([]string{metaExtension}, []string{"arrow.json"})
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "dec", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
		{Name: "dec2", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "wide", Type: &arrow.Decimal256Type{Precision: 50, Scale: 10}, Nullable: true},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "tm", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
		{Name: "tmn", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "tsz", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "tsn", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
		{Name: "tszn", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "iv", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
		{Name: "lst", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "st", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
		{Name: "j", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: jsonExt},
		{Name: "u", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true, Metadata: arrow.NewMetadata([]string{metaExtension}, []string{"arrow.uuid"})},
	}
	long := strings.Repeat("long ", 8000)
	longBin := bytes.Repeat([]byte{0, 1, 2, 255}, 10000)
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	rows := fmt.Sprintf(`[
		{"id": 1, "i8": -128, "i16": -32768, "i32": -2147483648, "i64": -9223372036854775808, "u8": 0, "u16": 0, "u32": 0, "u64": 0,
		 "dec": "99999999999999999999999999999.999999999", "dec2": "-0.01", "wide": "1234567890123456789012345678901234567890.0123456789", "b": true,
		 "s": "héllo ✓ 𝄞 日本", "bin": %q, "d": "0001-01-01", "tm": "00:00:00", "tmn": "12:34:56.123456789",
		 "ts": "0001-01-01T00:00:00", "tsz": "9999-12-31T23:59:59.999999Z", "tsn": "2262-04-11T23:47:16.854775807", "tszn": "1677-09-21T00:12:43.145224192Z",
		 "iv": {"months": 1, "days": 2, "nanoseconds": 11045000006000}, "lst": [1, null, 3], "st": {"a": 1, "b": "x"}, "j": "{\"a\": [1, 2, {\"b\": null}], \"c\": \"✓\"}",
		 "u": %q},
		{"id": 2, "i8": 127, "i16": 32767, "i32": 2147483647, "i64": 9223372036854775807, "u8": 255, "u16": 65535, "u32": 4294967295, "u64": 18446744073709551615,
		 "dec": "-0.000000001", "dec2": "99999999.99", "wide": "-0.0000000001", "b": false,
		 "s": "a \"quote\", a \\ and a '\nline\ttab", "bin": %q, "d": "9999-12-31", "tm": "23:59:59.999999", "tmn": "23:59:59.999999999",
		 "ts": "9999-12-31T23:59:59.999999", "tsz": "1969-12-31T23:59:59.999999Z", "tsn": "1970-01-01T00:00:00.000000001", "tszn": "2026-11-08T12:34:56.123456789Z",
		 "iv": {"months": -14, "days": -1, "nanoseconds": -1000}, "lst": [], "st": {"a": null, "b": null}, "j": "null", "u": %q},
		{"id": 3, "s": %q, "bin": %q, "dec": "0", "j": "123"},
		{"id": 4, "s": %q},
		{"id": 5}
	]`, base64.StdEncoding.EncodeToString(allBytes), rowID(1), base64.StdEncoding.EncodeToString([]byte{0}),
		base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{255}, 16)), long, base64.StdEncoding.EncodeToString(longBin), strings.Repeat("é", 2000))
	floats := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true}, {Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}
	fb := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(floats, nil))
	defer fb.Release()
	f32s := []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), math.MaxFloat32, math.SmallestNonzeroFloat32, 0.1}
	f64s := []float64{math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64, 0.1}
	for i := range f32s {
		fb.Field(0).(*array.Int64Builder).Append(int64(11 + i))
		fb.Field(1).(*array.Float32Builder).Append(f32s[i])
		fb.Field(2).(*array.Float64Builder).Append(f64s[i])
	}
	fb.Field(0).(*array.Int64Builder).Append(17)
	fb.Field(1).AppendNull()
	fb.Field(2).AppendNull()
	frec := fb.NewRecordBatch()
	defer frec.Release()
	e.commit(w, 1, streamBatch{"T", batchOf(t, fields, rows)}, streamBatch{"T", frec})

	got, err := readTable(t.Context(), e.admin, e.user, "T")
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]map[string]any{}
	for _, r := range got {
		byID[fmt.Sprint(r["id"])] = r
	}
	want := map[string]map[string]string{
		"1": {"i8": "-128", "i16": "-32768", "i32": "-2147483648", "i64": "-9223372036854775808", "u8": "0", "u64": "0",
			"dec": "99999999999999999999999999999.999999999", "dec2": "-0.01", "wide": "1234567890123456789012345678901234567890.0123456789", "b": "true",
			"s": "héllo ✓ 𝄞 日本", "bin": "256 bytes", "d": "0001-01-01 00:00:00", "tm": "0s",
			"ts": "0001-01-01 00:00:00", "tsz": "9999-12-31 23:59:59.999999", "tsn": "2262-04-11 23:47:16.854775807", "tszn": "1677-09-21 00:12:43.145224192",
			"iv": `{"days":2,"months":1,"nanoseconds":11045000006000}`, "lst": "[1,null,3]", "st": `{"a":1,"b":"x"}`, "j": `{"a": [1, 2, {"b": null}], "c": "✓"}`,
			"u": "00000000-0000-0000-0000-000000000001"},
		"2": {"i8": "127", "i16": "32767", "i32": "2147483647", "i64": "9223372036854775807", "u8": "255", "u16": "65535", "u32": "4294967295", "u64": "18446744073709551615",
			"dec": "-0.000000001", "dec2": "99999999.99", "wide": "-0.0000000001", "b": "false",
			"s": "a \"quote\", a \\ and a '\nline\ttab", "bin": "00", "d": "9999-12-31 00:00:00", "tm": "23h59m59.999999s",
			"ts": "9999-12-31 23:59:59.999999", "tsz": "1969-12-31 23:59:59.999999", "tsn": "1970-01-01 00:00:00.000000001", "tszn": "2026-11-08 12:34:56.123456789",
			"iv": `{"days":-1,"months":-14,"nanoseconds":-1000}`, "lst": "[]", "st": `{"a":null,"b":null}`, "j": "null", "u": "ffffffff-ffff-ffff-ffff-ffffffffffff"},
		"3":  {"s": "40000 bytes", "bin": "40000 bytes", "dec": "0", "j": "123", "i64": "<null>", "b": "<null>", "u": "<null>"},
		"4":  {"s": "4000 bytes"},
		"5":  {"s": "<null>", "bin": "<null>", "d": "<null>", "tm": "<null>", "ts": "<null>", "tsz": "<null>", "iv": "<null>", "lst": "<null>", "st": "<null>", "j": "<null>", "wide": "<null>"},
		"11": {"f32": "NaN", "f64": "NaN"}, "12": {"f32": "+Inf", "f64": "+Inf"}, "13": {"f32": "-Inf", "f64": "-Inf"},
		"14": {"f32": "3.4028235e+38", "f64": "1.7976931348623157e+308"}, "15": {"f32": "1e-45", "f64": "5e-324"},
		"16": {"f32": "0.1", "f64": "0.1"}, "17": {"f32": "<null>", "f64": "<null>"},
	}
	for id, cols := range want {
		for c, v := range cols {
			if g := textOf(byID[id][c]); g != v {
				t.Errorf("id %s, %s: %q, want %q", id, c, g, v)
			}
		}
	}
	// go-ora reads an interval to the microsecond: its nanoseconds, as SQL
	// gives them.
	for id, want := range map[int]string{1: "56.123456789", 2: "59.999999999"} {
		var got string
		if err := e.admin.QueryRowContext(t.Context(), `SELECT TO_CHAR(EXTRACT(SECOND FROM "tmn")) FROM "OPENETL_D_TYPES"."T" WHERE "id" = :1`, id).Scan(&got); err != nil || got != want {
			t.Errorf("id %d, tmn's seconds: %q, want %q: %v", id, got, want, err)
		}
	}
	if byID["3"]["s"] != long || !bytes.Equal(byID["3"]["bin"].([]byte), longBin) || !bytes.Equal(byID["1"]["bin"].([]byte), allBytes) {
		t.Errorf("the long or binary values changed")
	}
	cat, err := w.catalog(t.Context(), e.user, []string{"T"})
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]string{}
	for _, c := range cat["T"].cols {
		types[c.name] = c.typ
	}
	_, _, compatible, err := server(t.Context(), e.admin)
	if err != nil {
		t.Fatal(err)
	}
	boolType := "NUMBER(1)"
	if major(compatible) >= 23 {
		boolType = "BOOLEAN"
	}
	if w.boolean != (major(compatible) >= 23) {
		t.Errorf("w.boolean = %v with COMPATIBLE %s", w.boolean, compatible)
	}
	for c, typ := range map[string]string{
		"id": "NUMBER(19)", "i8": "NUMBER(3)", "i16": "NUMBER(5)", "i32": "NUMBER(10)", "u8": "NUMBER(3)", "u16": "NUMBER(5)", "u32": "NUMBER(10)", "u64": "NUMBER(20)", "f32": "BINARY_FLOAT", "f64": "BINARY_DOUBLE",
		"dec": "NUMBER(38,9)", "dec2": "NUMBER(10,2)", "wide": "VARCHAR2(63 BYTE)", "b": boolType, "s": "CLOB", "bin": "BLOB", "d": "DATE",
		"tm": "INTERVAL DAY(0) TO SECOND(6)", "tmn": "INTERVAL DAY(0) TO SECOND(9)", "ts": "TIMESTAMP(6)", "tsz": "TIMESTAMP(6) WITH TIME ZONE",
		"tsn": "TIMESTAMP(9)", "tszn": "TIMESTAMP(9) WITH TIME ZONE", "iv": "VARCHAR2(100 BYTE)", "lst": "CLOB", "st": "CLOB", "j": "CLOB", "u": "VARCHAR2(36 BYTE)",
	} {
		if types[c] != typ {
			t.Errorf("column %s is %s, want %s", c, types[c], typ)
		}
	}
}

// TestDestinationMergeKeys refuses a merge key Oracle would store as null,
// or too long for its column, and keeps keys that differ in case or
// trailing spaces apart.
func TestDestinationMergeKeys(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_KEYS", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "K", Mode: destination.Merge, PrimaryKey: []string{"k"}}}})
	fields := []arrow.Field{{Name: "k", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "v", Type: arrow.PrimitiveTypes.Int64}}
	e.commit(w, 1, streamBatch{"K", batchOf(t, fields, `[{"k": "a", "v": 1}, {"k": "A", "v": 2}, {"k": "a ", "v": 3}, {"k": "ä", "v": 4}]`)})
	e.commit(w, 2, streamBatch{"K", batchOf(t, fields, `[{"k": "A", "v": 20}]`)})
	equal(t, "K", e.rows(e.user, "K", "k", "v"), []string{"k=A, v=20", "k=a , v=3", "k=a, v=1", "k=ä, v=4"})

	// A table whose rows repeat a key takes no merge.
	exec(t, e.admin, `CREATE TABLE "OPENETL_D_KEYS"."DUP" ("k" VARCHAR2(4000 BYTE), "v" NUMBER(19))`,
		`INSERT INTO "OPENETL_D_KEYS"."DUP" VALUES ('a', 1)`, `INSERT INTO "OPENETL_D_KEYS"."DUP" VALUES ('a', 2)`, "COMMIT")
	dup := e.open(2, destination.OpenConfig{Streams: []destination.Stream{{Name: "DUP", Mode: destination.Merge, PrimaryKey: []string{"k"}}}})
	st := e.stage(dup, 1, streamBatch{"DUP", batchOf(t, fields, `[{"k": "b", "v": 1}]`)})
	if _, err := dup.Commit(t.Context(), destination.Epoch{Generation: 2, N: 1}, []destination.Staged{st}); err == nil || !strings.Contains(err.Error(), "has repeats") {
		t.Errorf("a merge into a table with repeated keys: %v", err)
	}
	ep := destination.Epoch{Generation: 2, N: 2}
	for rows, want := range map[string]string{
		`[{"k": "", "v": 1}]`:   "Oracle stores as null",
		`[{"k": null, "v": 1}]`: "has no value",
		fmt.Sprintf(`[{"k": %q, "v": 1}]`, strings.Repeat("x", 4001)): "holds 4000",
	} {
		err := dup.Write(t.Context(), ep, "DUP", batchOf(t, fields, rows))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%.40s: %v, want an error that says %q", rows, err, want)
		}
	}
}

// TestDestinationNames writes tables and columns whose names are 128
// bytes, in mixed case, with spaces and quotes, merging on such a key, and
// refuses a name with a double quote.
func TestDestinationNames(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_NAMES", ownGrants...)
	table := "Mixed Case 'Tables' " + strings.Repeat("é", 54) // 128 bytes
	key := "Key Col " + strings.Repeat("K", 120)
	val := "vAl ñ"
	if len(table) != 128 || len(key) != 128 {
		t.Fatalf("names of %d and %d bytes", len(table), len(key))
	}
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: table, Mode: destination.Merge, PrimaryKey: []string{key}}}})
	fields := []arrow.Field{{Name: key, Type: arrow.PrimitiveTypes.Int64}, {Name: val, Type: arrow.BinaryTypes.String, Nullable: true}}
	e.commit(w, 1, streamBatch{table, batchOf(t, fields, fmt.Sprintf(`[{%q: 1, %q: "x"}, {%q: 2, %q: "y"}]`, key, val, key, val))})
	e.commit(w, 2, streamBatch{table, batchOf(t, fields, fmt.Sprintf(`[{%q: 1, %q: "z"}]`, key, val))})
	equal(t, "rows", e.rows(e.user, table, key, val), []string{key + "=1, " + val + "=z", key + "=2, " + val + "=y"})
	bad := []arrow.Field{fields[0], {Name: `a"b`, Type: arrow.PrimitiveTypes.Int64}}
	err := w.Write(t.Context(), destination.Epoch{Generation: 1, N: 3}, table, batchOf(t, bad, fmt.Sprintf(`[{%q: 3, "a\"b": 1}]`, key)))
	if err == nil || !strings.Contains(err.Error(), "double quote") {
		t.Errorf("a column named a\"b: %v", err)
	}
	md := arrow.NewMetadata([]string{metaTable}, []string{table})
	_, err = w.ApplySchema(t.Context(), []destination.StreamSchema{{Name: table, Schema: arrow.NewSchema(bad, &md)}})
	if err == nil || !strings.Contains(err.Error(), "double quote") {
		t.Errorf("applying a column named a\"b: %v", err)
	}
}

// TestDestinationTypeChangeFixes refuses changes of a column's type the
// stream doesn't allow, or that Oracle can't make in place, with a fix that
// makes them on the populated table; and makes the one it can when allowed.
func TestDestinationTypeChangeFixes(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_FIX", ownGrants...)
	md := arrow.NewMetadata([]string{metaTable}, []string{"N"})
	schema := func(fields ...arrow.Field) *arrow.Schema { return arrow.NewSchema(fields, &md) }
	id := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	apply := func(w *writer, s *arrow.Schema) []destination.RefusedColumn {
		t.Helper()
		a, err := w.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "N", Schema: s}})
		if err != nil {
			t.Fatal(err)
		}
		return a.Refused
	}
	fix := func(stmts string) {
		t.Helper()
		for s := range strings.SplitSeq(stmts, ";") {
			if s = strings.TrimSpace(s); s != "" {
				exec(t, e.admin, s)
			}
		}
	}
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "N", Mode: destination.Append}}})
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	e.commit(w, 1, streamBatch{"N", batchOf(t, []arrow.Field{id, n32}, `[{"id": 1, "n": 7}]`)})
	exec(t, e.admin, `ALTER TABLE "OPENETL_D_FIX"."N" MODIFY ("n" NOT NULL)`)

	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	r := apply(w, schema(id, n64))
	if len(r) != 1 || r[0].FromType != "NUMBER(10)" || r[0].ToType != "NUMBER(19)" || r[0].Fix != `ALTER TABLE "OPENETL_D_FIX"."N" MODIFY ("n" NUMBER(19))` {
		t.Fatalf("refused %+v", r)
	}
	exec(t, e.admin, r[0].Fix)
	if r := apply(w, schema(id, n64)); len(r) != 0 {
		t.Fatalf("after the fix, refused %+v", r)
	}
	var nullable string
	if err := e.admin.QueryRowContext(t.Context(), `SELECT nullable FROM all_tab_columns WHERE owner = 'OPENETL_D_FIX' AND table_name = 'N' AND column_name = 'n'`).Scan(&nullable); err != nil || nullable != "N" {
		t.Errorf("after the fix, n's nullable is %q: %v", nullable, err)
	}
	e.commit(w, 2, streamBatch{"N", batchOf(t, []arrow.Field{id, n64}, `[{"id": 2, "n": 8589934592}]`)})

	str := arrow.Field{Name: "n", Type: arrow.BinaryTypes.String, Nullable: true}
	allowed := e.open(2, destination.OpenConfig{Streams: []destination.Stream{{Name: "N", Mode: destination.Append, AllowTypeChanges: true}}})
	r = apply(allowed, schema(id, str))
	if len(r) != 1 || r[0].FromType != "NUMBER(19)" || r[0].ToType != "CLOB" || !strings.Contains(r[0].Reason, "in place") || !strings.Contains(r[0].Fix, `RENAME COLUMN "_openetl_new" TO "n"`) {
		t.Fatalf("refused %+v", r)
	}
	fix(r[0].Fix)
	if r := apply(allowed, schema(id, str)); len(r) != 0 {
		t.Fatalf("after the fix, refused %+v", r)
	}
	e.commit(allowed, 3, streamBatch{"N", batchOf(t, []arrow.Field{id, str}, `[{"id": 3, "n": "x"}]`)})
	equal(t, "N", e.rows(e.user, "N", "id", "n"), []string{"id=1, n=7", "id=2, n=8589934592", "id=3, n=x"})

	// Allowed, a change Oracle makes in place is made.
	d32 := arrow.Field{Name: "d", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true}
	d38 := arrow.Field{Name: "d", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}, Nullable: true}
	e.commit(allowed, 4, streamBatch{"N", batchOf(t, []arrow.Field{id, d32}, `[{"id": 4, "d": "12.34"}]`)})
	if r := apply(allowed, schema(id, str, d38)); len(r) != 0 {
		t.Fatalf("an allowed change, refused %+v", r)
	}
	e.commit(allowed, 5, streamBatch{"N", batchOf(t, []arrow.Field{id, d38}, `[{"id": 5, "d": "1234567890123456.1234"}]`)})
	equal(t, "N", e.rows(e.user, "N", "id", "d"), []string{"id=1, d=<null>", "id=2, d=<null>", "id=3, d=<null>", "id=4, d=12.34", "id=5, d=1234567890123456.1234"})
	// So are a timestamp's fractional digits, and a date to a timestamp.
	tsField := func(name string, unit arrow.TimeUnit, tz string) arrow.Field {
		return arrow.Field{Name: name, Type: &arrow.TimestampType{Unit: unit, TimeZone: tz}, Nullable: true}
	}
	dt := arrow.Field{Name: "dt", Type: arrow.FixedWidthTypes.Date32, Nullable: true}
	e.commit(allowed, 6, streamBatch{"N", batchOf(t, []arrow.Field{id, tsField("ts", arrow.Microsecond, ""), tsField("tz", arrow.Microsecond, "UTC"), dt},
		`[{"id": 6, "ts": "2026-01-02T03:04:05.123456", "tz": "2026-01-02T03:04:05.123456Z", "dt": "1999-12-31"}]`)})
	wider := []arrow.Field{id, str, d38, tsField("ts", arrow.Nanosecond, ""), tsField("tz", arrow.Nanosecond, "UTC"), tsField("dt", arrow.Microsecond, "")}
	if r := apply(allowed, schema(wider...)); len(r) != 0 {
		t.Fatalf("allowed temporal changes, refused %+v", r)
	}
	e.commit(allowed, 7, streamBatch{"N", batchOf(t, []arrow.Field{id, wider[3], wider[4], wider[5]},
		`[{"id": 7, "ts": "2026-01-02T03:04:05.123456789", "tz": "2026-01-02T03:04:05.123456789Z", "dt": "2026-01-02T03:04:05.5"}]`)})
	got, err := readTable(t.Context(), e.admin, e.user, "N")
	if err != nil {
		t.Fatal(err)
	}
	var temporal []string
	for _, row := range got {
		if id := fmt.Sprint(row["id"]); id == "6" || id == "7" {
			temporal = append(temporal, id+": "+textOf(row["ts"])+", "+textOf(row["tz"])+", "+textOf(row["dt"]))
		}
	}
	slices.Sort(temporal)
	equal(t, "after the temporal changes", temporal, []string{
		"6: 2026-01-02 03:04:05.123456, 2026-01-02 03:04:05.123456, 1999-12-31 00:00:00",
		"7: 2026-01-02 03:04:05.123456789, 2026-01-02 03:04:05.123456789, 2026-01-02 03:04:05.5",
	})
	cat, err := allowed.catalog(t.Context(), e.user, []string{"N"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cat["N"].cols {
		if want := map[string]string{"ts": "TIMESTAMP(9)", "tz": "TIMESTAMP(9) WITH TIME ZONE", "dt": "TIMESTAMP(6)"}[c.name]; want != "" && c.typ != want {
			t.Errorf("column %s is %s, want %s", c.name, c.typ, want)
		}
	}
	// A type the column can't hold is refused, without a fix.
	if r := apply(allowed, schema(id, str, arrow.Field{Name: "d", Type: arrow.BinaryTypes.Binary, Nullable: true})); len(r) != 1 || r[0].Fix != "" || r[0].FromType != "NUMBER(20,4)" || r[0].ToType != "BLOB" {
		t.Fatalf("binary over a decimal, refused %+v", r)
	}
}

// TestDestinationRecoverDropsStaleStaging recovers an epoch whose staging
// is gone as lost, and drops what an older generation staged.
func TestDestinationRecoverDropsStaleStaging(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_REC", ownGrants...)
	streams := []destination.Stream{{Name: "R", Mode: destination.Append}}
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	w1 := e.open(1, destination.OpenConfig{Streams: streams})
	e.commit(w1, 1, streamBatch{"R", batchOf(t, fields, `[{"id": 1}]`)})
	staged := e.stage(w1, 2, streamBatch{"R", batchOf(t, fields, `[{"id": 2}]`)})
	e.stage(w1, 3, streamBatch{"R", batchOf(t, fields, `[{"id": 3}]`)})
	staging := func() []string {
		t.Helper()
		tables, err := w1.ownTables(t.Context(), stagePrefix)
		if err != nil {
			t.Fatal(err)
		}
		return tables
	}
	if n := len(staging()); n != 2 {
		t.Fatalf("%d staging tables before the crash, want 2", n)
	}
	exec(t, e.admin, `DROP TABLE "OPENETL_D_REC".`+quoteIdent(staging()[0])+" PURGE")
	w2, err := e.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: 2, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close(context.Background()) }()
	if staging()[0] != stagingName("p1", "R", destination.Epoch{Generation: 1, N: 3}, 0) {
		// The one left is epoch 3's, which no record names.
		exec(t, e.admin, `DROP TABLE "OPENETL_D_REC".`+quoteIdent(stagingName("p1", "R", destination.Epoch{Generation: 1, N: 2}, 0))+" PURGE")
	}
	r, err := w2.Recover(t.Context(), 2, destination.Epoch{Generation: 1, N: 2}, []destination.Staged{staged})
	if err != nil || r != destination.Lost {
		t.Fatalf("recovered %v, %v; want lost", r, err)
	}
	if left := staging(); len(left) != 0 {
		t.Errorf("staging left after recovery: %q", left)
	}
	equal(t, "R", e.rows(e.user, "R", "id"), []string{"id=1"})
	if _, err := w1.Commit(t.Context(), destination.Epoch{Generation: 1, N: 2}, []destination.Staged{staged}); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("the fenced writer's commit: %v", err)
	}
}

// TestDestinationRecoverRedoesOnce recovers an epoch staged but not
// committed, which it applies, and one committed, which it doesn't apply
// again.
func TestDestinationRecoverRedoesOnce(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_REDO", ownGrants...)
	streams := []destination.Stream{{Name: "A", Mode: destination.Append}, {Name: "M", Mode: destination.Merge, PrimaryKey: []string{"id"}}}
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}}
	both := func(rows string) []streamBatch {
		b := batchOf(t, fields, rows)
		return []streamBatch{{"A", b}, {"M", b}}
	}
	recover := func(gen uint64, n uint64, st destination.Staged, want destination.Recovery) {
		t.Helper()
		w, err := e.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: gen, Streams: streams})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = w.Close(context.Background()) }()
		r, err := w.Recover(t.Context(), gen, destination.Epoch{Generation: gen - 1, N: n}, []destination.Staged{st})
		if err != nil || r != want {
			t.Fatalf("generation %d recovered %v, %v; want %v", gen, r, err, want)
		}
	}
	w1 := e.open(1, destination.OpenConfig{Streams: streams})
	e.commit(w1, 1, both(`[{"id": 1, "v": "a"}]`)...)
	// Staged, and the attempt ends before its commit.
	recover(2, 2, e.stage(w1, 2, both(`[{"id": 1, "v": "b"}, {"id": 2, "v": "c"}]`)...), destination.Redone)
	equal(t, "A", e.rows(e.user, "A", "id", "v"), []string{"id=1, v=a", "id=1, v=b", "id=2, v=c"})
	equal(t, "M", e.rows(e.user, "M", "id", "v"), []string{"id=1, v=b", "id=2, v=c"})

	// Committed, and the attempt ends before the platform records it.
	w3 := e.open(3, destination.OpenConfig{Streams: streams})
	st := e.stage(w3, 1, both(`[{"id": 3, "v": "d"}]`)...)
	if _, err := w3.Commit(t.Context(), destination.Epoch{Generation: 3, N: 1}, []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
	recover(4, 1, st, destination.Applied)
	equal(t, "A", e.rows(e.user, "A", "id", "v"), []string{"id=1, v=a", "id=1, v=b", "id=2, v=c", "id=3, v=d"})
	if left, err := w3.ownTables(t.Context(), stagePrefix); err != nil || len(left) != 0 {
		t.Errorf("staging left: %q, %v", left, err)
	}
}

// TestDestinationOverwriteReads overwrites a stream when its read ends:
// rows of an earlier read go, an attempt that resumes a read keeps what it
// staged, and a read that ends without rows empties the table.
func TestDestinationOverwriteReads(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_OVER", ownGrants...)
	streams := []destination.Stream{{Name: "O", Mode: destination.Overwrite}}
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	read := func(w *writer, at time.Time) {
		t.Helper()
		if err := w.StartReads(t.Context(), map[string]time.Time{"O": at}); err != nil {
			t.Fatal(err)
		}
	}
	end := func(w *writer, n uint64, rows string) {
		t.Helper()
		ep := destination.Epoch{Generation: w.cfg.Generation, N: n}
		if rows != "" {
			if err := w.Write(t.Context(), ep, "O", batchOf(t, fields, rows)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.EndRead(t.Context(), ep, "O"); err != nil {
			t.Fatal(err)
		}
		st, err := w.PreCommit(t.Context(), ep)
		if err == nil {
			_, err = w.Commit(t.Context(), ep, []destination.Staged{st})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	w := e.open(1, destination.OpenConfig{Streams: streams})
	read(w, start)
	e.commit(w, 1, streamBatch{"O", batchOf(t, fields, `[{"id": 1}, {"id": 2}]`)})
	end(w, 2, `[{"id": 3}]`)
	equal(t, "first read", e.rows(e.user, "O", "id"), []string{"id=1", "id=2", "id=3"})

	read(w, start.Add(time.Hour))
	e.commit(w, 3, streamBatch{"O", batchOf(t, fields, `[{"id": 4}]`)})
	equal(t, "during the second read", e.rows(e.user, "O", "id"), []string{"id=1", "id=2", "id=3"})
	w2 := e.open(2, destination.OpenConfig{Streams: streams})
	read(w2, start.Add(time.Hour))
	end(w2, 1, `[{"id": 5}]`)
	equal(t, "the second read, resumed", e.rows(e.user, "O", "id"), []string{"id=4", "id=5"})

	read(w2, start.Add(2*time.Hour))
	end(w2, 2, "")
	equal(t, "an empty read", e.rows(e.user, "O", "id"), nil)
	if left, err := w2.ownTables(t.Context(), readPrefix); err != nil || len(left) != 0 {
		t.Errorf("read tables left: %q, %v", left, err)
	}
}

// TestDestinationOverwriteReadTypes: an overwrite read's table doesn't
// change a column's type the stream doesn't allow it to, and takes the type
// a person widened the target to.
func TestDestinationOverwriteReadTypes(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_READTYPE", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "O", Mode: destination.Overwrite}}})
	if err := w.StartReads(t.Context(), map[string]time.Time{"O": time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatal(err)
	}
	id := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	e.commit(w, 1, streamBatch{"O", batchOf(t, []arrow.Field{id, n32}, `[{"id": 1, "n": 7}]`)})
	readType := func() string {
		t.Helper()
		reads, err := w.ownTables(t.Context(), readPrefix)
		if err != nil || len(reads) != 1 {
			t.Fatalf("read tables %q, %v", reads, err)
		}
		cols, err := w.columnTypes(t.Context(), e.user, reads[0])
		if err != nil {
			t.Fatal(err)
		}
		return typesOf(cols)["n"]
	}
	ep := destination.Epoch{Generation: 1, N: 2}
	st := e.stage(w, 2, streamBatch{"O", batchOf(t, []arrow.Field{id, n64}, `[{"id": 2, "n": 8589934592}]`)})
	if _, err := w.Commit(t.Context(), ep, []destination.Staged{st}); err == nil || !strings.Contains(err.Error(), "typeChanges") {
		t.Fatalf("a type change the stream doesn't allow: %v", err)
	}
	if got := readType(); got != "NUMBER(10)" {
		t.Errorf("after the refusal, the read's n is %s", got)
	}
	exec(t, e.admin, `CREATE TABLE "OPENETL_D_READTYPE"."O" ("id" NUMBER(19), "n" NUMBER(19))`)
	if _, err := w.Commit(t.Context(), ep, []destination.Staged{st}); err != nil {
		t.Fatalf("after the target was widened: %v", err)
	}
	if got := readType(); got != "NUMBER(19)" {
		t.Errorf("after the target was widened, the read's n is %s", got)
	}
}

// TestDestinationFencedKeepsNewerReads: an older generation, for which the
// stream appends, doesn't drop the read table of a newer one that overwrites
// it once the newer has raised the fence.
func TestDestinationFencedKeepsNewerReads(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_FENCEREAD", ownGrants...)
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	old := e.open(1, destination.OpenConfig{Streams: []destination.Stream{{Name: "O", Mode: destination.Append}}})
	newer := e.open(2, destination.OpenConfig{Streams: []destination.Stream{{Name: "O", Mode: destination.Overwrite}}})
	if err := newer.StartReads(t.Context(), map[string]time.Time{"O": time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatal(err)
	}
	e.commit(newer, 1, streamBatch{"O", batchOf(t, fields, `[{"id": 1}]`)})
	reads := func() []string {
		t.Helper()
		tables, err := newer.ownTables(t.Context(), readPrefix)
		if err != nil {
			t.Fatal(err)
		}
		return tables
	}
	if n := len(reads()); n != 1 {
		t.Fatalf("%d read tables, want 1", n)
	}
	old.DropRemovedReads(t.Context())
	if n := len(reads()); n != 1 {
		t.Errorf("the fenced generation dropped the newer one's read: %d read tables left", n)
	}
}

// TestDestinationPatchesHolds fills held cells of an append stream by
// _openetl_id and of a merge stream by key, where the target still holds
// the version a patch comes from.
func TestDestinationPatchesHolds(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_HOLD", ownGrants...)
	w := e.open(1, destination.OpenConfig{Streams: []destination.Stream{
		{Name: "A", Mode: destination.Append}, {Name: "M", Mode: destination.Merge, PrimaryKey: []string{"k"}},
	}})
	heldField := arrow.Field{Name: destination.HeldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	fields := []arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}, idField, seqField, heldField}
	rows := func(rows string) arrow.RecordBatch { return batchOf(t, fields, rows) }
	patch := func(n uint64, stream, rows string) {
		t.Helper()
		ep := destination.Epoch{Generation: 1, N: n}
		if err := w.Patch(t.Context(), ep, stream, batchOf(t, fields, rows)); err != nil {
			t.Fatal(err)
		}
		st, err := w.PreCommit(t.Context(), ep)
		if err == nil {
			_, err = w.Commit(t.Context(), ep, []destination.Staged{st})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	held := fmt.Sprintf(`[{"k": 1, "v": null, "_openetl_id": %q, "_openetl_seq": "1", "_openetl_held": ["v"]},
		{"k": 2, "v": null, "_openetl_id": %q, "_openetl_seq": "1", "_openetl_held": ["v"]},
		{"k": 3, "v": "c", "_openetl_id": %q, "_openetl_seq": "1"}]`, rowID(1), rowID(2), rowID(3))
	e.commit(w, 1, streamBatch{"A", rows(held)}, streamBatch{"M", rows(held)})
	// Key 2's newer version, held too.
	e.commit(w, 2, streamBatch{"M", rows(fmt.Sprintf(`[{"k": 2, "v": null, "_openetl_id": %q, "_openetl_seq": "2", "_openetl_held": ["v"]}]`, rowID(12)))})
	fill := fmt.Sprintf(`[{"k": 1, "v": "a", "_openetl_id": %q, "_openetl_held": ["v"]}, {"k": 2, "v": "stale", "_openetl_id": %q, "_openetl_held": ["v"]},
		{"k": 3, "v": "not held", "_openetl_id": %q, "_openetl_held": ["v"]}]`, rowID(1), rowID(2), rowID(3))
	patch(3, "A", fill)
	patch(4, "M", fill)
	cols := []string{"k", "v", destination.HeldColumn}
	equal(t, "A", e.rows(e.user, "A", cols...), []string{"k=1, v=a, _openetl_held=<null>", "k=2, v=stale, _openetl_held=<null>", "k=3, v=c, _openetl_held=<null>"})
	m := e.rows(e.user, "M", cols...)
	if len(m) != 3 || m[0] != "k=1, v=a, _openetl_held=<null>" || !strings.HasPrefix(m[1], "k=2, v=<null>, _openetl_held={") || m[2] != "k=3, v=c, _openetl_held=<null>" {
		t.Fatalf("M after the patches: %q", m)
	}
	patch(5, "M", fmt.Sprintf(`[{"k": 2, "v": "b", "_openetl_id": %q, "_openetl_held": ["v"]}]`, rowID(12)))
	equal(t, "M", e.rows(e.user, "M", cols...), []string{"k=1, v=a, _openetl_held=<null>", "k=2, v=b, _openetl_held=<null>", "k=3, v=c, _openetl_held=<null>"})
}

// TestDestinationLanes merges a key's versions from two lanes by
// _openetl_seq, and dedupes an epoch's _openetl_ids across them.
func TestDestinationLanes(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_LANES", ownGrants...)
	streams := []destination.Stream{{Name: "M", Mode: destination.Merge, PrimaryKey: []string{"k"}}, {Name: "A", Mode: destination.Append, Dedupe: destination.DedupeEpoch}}
	w0 := e.open(1, destination.OpenConfig{Streams: streams})
	l1, err := e.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: 1, Streams: streams, Lane: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l1.Close(context.Background()) }()
	w1 := l1.(*writer)
	fields := []arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}, idField, seqField}
	st0 := e.stage(w0, 1, streamBatch{"M", batchOf(t, fields, fmt.Sprintf(`[{"k": 1, "v": "newer", "_openetl_id": %q, "_openetl_seq": "2"}, {"k": 2, "v": "x", "_openetl_id": %q, "_openetl_seq": "1"}]`, rowID(1), rowID(2)))},
		streamBatch{"A", batchOf(t, fields, fmt.Sprintf(`[{"k": 1, "v": "a", "_openetl_id": %q}, {"k": 2, "v": "b", "_openetl_id": %q}]`, rowID(1), rowID(2)))})
	st1 := e.stage(w1, 1, streamBatch{"M", batchOf(t, fields, fmt.Sprintf(`[{"k": 1, "v": "older", "_openetl_id": %q, "_openetl_seq": "1"}]`, rowID(3)))},
		streamBatch{"A", batchOf(t, fields, fmt.Sprintf(`[{"k": 3, "v": "a again", "_openetl_id": %q}, {"k": 4, "v": "c", "_openetl_id": %q}]`, rowID(1), rowID(4)))})
	if _, err := w0.Commit(t.Context(), destination.Epoch{Generation: 1, N: 1}, []destination.Staged{st0, st1}); err != nil {
		t.Fatal(err)
	}
	equal(t, "M", e.rows(e.user, "M", "k", "v"), []string{"k=1, v=newer", "k=2, v=x"})
	if a := e.rows(e.user, "A", "v"); len(a) != 3 || a[2] != "v=c" {
		t.Errorf("A: %q, want one of a's two rows, b and c", a)
	}
	if left, err := w0.ownTables(t.Context(), stagePrefix); err != nil || len(left) != 0 {
		t.Errorf("staging left: %q, %v", left, err)
	}
}

// TestDestinationChecksItemByItem runs the checks for the least-privileged
// user, a user without CREATE TABLE or a quota, a user that doesn't exist,
// and an address nothing listens at.
func TestDestinationChecksItemByItem(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_CHK", ownGrants...)
	if err := check.Validate(e.d.Checks()); err != nil {
		t.Fatal(err)
	}
	passed := map[string]string{"net.dns": "passed", "net.connect": "passed", "db.connect": "passed", "server.version": "passed",
		"server.compatible": "passed", "privilege.create-table": "passed", "quota.tablespace": "passed"}
	chk := check.Run(t.Context(), e.d.Checks())
	expectItems(t, "the least-privileged user", chk, passed)
	_, _, compatible, err := server(t.Context(), e.admin)
	if err != nil {
		t.Fatal(err)
	}
	wantBool := "NUMBER(1), 1 for true"
	if major(compatible) >= 23 {
		wantBool = "BOOLEAN"
	}
	for _, it := range chk.Items {
		if it.Name == "server.compatible" && !strings.Contains(it.Rendered().Message, wantBool) {
			t.Errorf("server.compatible = %q, want %q", it.Rendered().Message, wantBool)
		}
	}
	if err := e.d.Check(t.Context()); err != nil {
		t.Fatal(err)
	}

	dropUser(t, e.admin, "OPENETL_D_BARE")
	exec(t, e.admin, `CREATE USER "OPENETL_D_BARE" IDENTIFIED BY "pw_openetl_d_bare"`, `GRANT CREATE SESSION TO "OPENETL_D_BARE"`)
	t.Cleanup(func() { dropUser(t, e.admin, "OPENETL_D_BARE") })
	bare, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q}`, dsnAs("OPENETL_D_BARE", "pw_openetl_d_bare")))
	if err != nil {
		t.Fatal(err)
	}
	r := check.Run(t.Context(), bare.Checks())
	expectItems(t, "a user that can only sign in", r, with(passed, map[string]string{
		"privilege.create-table": "failed privilege_missing", "quota.tablespace": "failed quota_missing",
	}))
	if fix := fixOf(r, "privilege.create-table"); !strings.Contains(fix, "GRANT CREATE TABLE TO OPENETL_D_BARE;") {
		t.Errorf("privilege.create-table's fix: %q", fix)
	}
	if fix := fixOf(r, "quota.tablespace"); !strings.Contains(fix, "ALTER USER OPENETL_D_BARE QUOTA UNLIMITED ON USERS;") {
		t.Errorf("quota.tablespace's fix: %q", fix)
	}

	for what, dsn := range map[string]string{
		"a bad password": dsnAs("OPENETL_D_CHK", "not-the-password"), "a user that doesn't exist": dsnAs("OPENETL_D_NOBODY", "not-the-password"), "a closed port": closedDSN,
	} {
		d, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q}`, dsn))
		if err != nil {
			t.Fatal(err)
		}
		r := check.Run(t.Context(), d.Checks())
		want := map[string]string{"net.dns": "passed", "net.connect": "passed", "db.connect": "failed credentials_rejected", "server.version": "skipped", "quota.tablespace": "skipped"}
		if dsn == closedDSN {
			want = map[string]string{"net.dns": "passed", "net.connect": "failed connection_refused", "db.connect": "skipped", "server.version": "skipped"}
		}
		expectItems(t, what, r, want)
		if err := d.Check(t.Context()); err == nil || strings.Contains(err.Error(), "not-the-password") {
			t.Errorf("%s: Check gave %v", what, err)
		}
	}
}

// TestDestinationPreflightItemByItem preflights streams that can be
// written, a merge without a key, a name Oracle can't take, a table whose
// column the stream would change, a schema the user can't create, and a
// table of another schema it has too few grants on.
func TestDestinationPreflightItemByItem(t *testing.T) {
	e := newDestEnv(t, "OPENETL_D_PRE", ownGrants...)
	exec(t, e.admin, `CREATE TABLE "OPENETL_D_PRE"."TYPED" ("n" NUMBER(10))`)
	md := func(table string) *arrow.Schema {
		m := arrow.NewMetadata([]string{metaTable}, []string{table})
		return arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, &m)
	}
	req := destination.PreflightRequest{Pipeline: "p1", Streams: []destination.Stream{
		{Name: "OK", Mode: destination.Append}, {Name: "NOKEY", Mode: destination.Merge}, {Name: `BAD"NAME`, Mode: destination.Append},
		{Name: "TYPED", Mode: destination.Append},
	}, Schemas: map[string]*arrow.Schema{"OK": md("OK"), "NOKEY": md("NOKEY"), `BAD"NAME`: md(`BAD"NAME`), "TYPED": md("TYPED")}}
	checks := e.d.Preflight(req)
	if err := check.Validate(checks); err != nil {
		t.Fatal(err)
	}
	r := check.Run(t.Context(), checks)
	expectItems(t, "the user's own schema", r, map[string]string{
		"db.connect": "passed", "privilege.create-table": "passed", "quota.tablespace": "passed", "schema.write": "passed",
		"stream.ok.names": "passed", "stream.ok.mode": "passed", "stream.ok.write": "passed", "stream.ok.types": "passed",
		"stream.nokey.mode":     "failed primary_key_missing",
		"stream.bad-name.names": "failed name_invalid", "stream.bad-name.types": "skipped",
		"stream.typed.write": "passed", "stream.typed.types": "warning type_change_refused",
	})
	if fix := fixOf(r, "stream.typed.types"); fix != `ALTER TABLE "OPENETL_D_PRE"."TYPED" MODIFY ("n" NUMBER(19))` {
		t.Errorf("stream.typed.types's fix: %q", fix)
	}

	req = destination.PreflightRequest{Pipeline: "p1", Namespace: "OPENETL_D_NONE", Streams: []destination.Stream{{Name: "OK", Mode: destination.Append}}, Schemas: map[string]*arrow.Schema{"OK": md("OK")}}
	expectItems(t, "a schema it can't create", check.Run(t.Context(), e.d.Preflight(req)), map[string]string{
		"schema.write": "failed privilege_missing", "stream.ok.write": "skipped",
	})

	dropUser(t, e.admin, "OPENETL_D_OWNER")
	exec(t, e.admin, `CREATE USER "OPENETL_D_OWNER" NO AUTHENTICATION QUOTA UNLIMITED ON USERS`, `CREATE TABLE "OPENETL_D_OWNER"."T" ("n" NUMBER(19))`,
		`GRANT SELECT, INSERT ON "OPENETL_D_OWNER"."T" TO "OPENETL_D_PRE"`)
	t.Cleanup(func() { dropUser(t, e.admin, "OPENETL_D_OWNER") })
	req = destination.PreflightRequest{Pipeline: "p1", Namespace: "OPENETL_D_OWNER", Streams: []destination.Stream{{Name: "T", Mode: destination.Append}, {Name: "NEW", Mode: destination.Append}},
		Schemas: map[string]*arrow.Schema{"T": md("T"), "NEW": md("NEW")}}
	r = check.Run(t.Context(), e.d.Preflight(req))
	expectItems(t, "another schema's table, with SELECT and INSERT", r, map[string]string{
		"schema.write": "passed", "stream.t.write": "failed privilege_missing", "stream.new.write": "failed privilege_missing",
	})
	if fix := fixOf(r, "stream.t.write"); !strings.Contains(fix, `GRANT UPDATE, DELETE, ALTER, INDEX ON "OPENETL_D_OWNER"."T" TO OPENETL_D_PRE;`) {
		t.Errorf("stream.t.write's fix: %q", fix)
	}
	if fix := fixOf(r, "stream.new.write"); !strings.Contains(fix, "GRANT CREATE ANY TABLE, SELECT ANY TABLE, INSERT ANY TABLE, UPDATE ANY TABLE, DELETE ANY TABLE, ALTER ANY TABLE, CREATE ANY INDEX TO OPENETL_D_PRE;") {
		t.Errorf("stream.new.write's fix: %q", fix)
	}

	// With the grants the fix names, the user merges into the table.
	exec(t, e.admin, `GRANT UPDATE, DELETE, ALTER, INDEX ON "OPENETL_D_OWNER"."T" TO "OPENETL_D_PRE"`)
	req.Streams = req.Streams[:1]
	expectItems(t, "another schema's table, with its grants", check.Run(t.Context(), e.d.Preflight(req)), map[string]string{"stream.t.write": "passed"})
	w := e.open(1, destination.OpenConfig{Namespace: "OPENETL_D_OWNER", Streams: []destination.Stream{{Name: "T", Mode: destination.Merge, PrimaryKey: []string{"n"}}}})
	fields := []arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}}
	e.commit(w, 1, streamBatch{"T", batchOf(t, fields, `[{"n": 1, "v": "a"}, {"n": 2, "v": "b"}]`)})
	e.commit(w, 2, streamBatch{"T", batchOf(t, fields, `[{"n": 1, "v": "c"}]`)})
	equal(t, "T", e.rows("OPENETL_D_OWNER", "T", "n", "v"), []string{"n=1, v=c", "n=2, v=b"})
}
