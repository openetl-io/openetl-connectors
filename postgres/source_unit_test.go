package postgres

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgtype"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func isTransient(err error) bool {
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	return errors.As(err, &coded) && coded.ErrorCode() == protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
}

// A full or incremental read of a table with a column the run didn't
// declare fails, retryably, before it reads: a numeric's text would land as
// text.
func TestReadSplitFailsOnAnUndeclaredColumn(t *testing.T) {
	s, err := NewSource([]byte(`{"dsn": "postgres://u:p@127.0.0.1:1/db?connect_timeout=1"}`))
	if err != nil {
		t.Fatal(err)
	}
	users := table{name: "users", cols: []sourceColumn{{name: "id", oid: pgtype.Int4OID}, {name: "price", oid: pgtype.NumericOID}}, key: []int{0}, order: []int{0}}
	for _, cs := range []source.ConfiguredStream{
		{Name: "users", SyncMode: source.FullRefresh, Columns: []string{"id"}},
		{Name: "users", SyncMode: source.Incremental, Cursor: "id", Columns: []string{"id"}},
	} {
		err := s.readSplit(t.Context(), newConnPool(s.conn, 1), []table{users}, cs, source.Split{}, nil)
		if !isTransient(err) || !strings.Contains(err.Error(), `"price"`) {
			t.Errorf("%v read with price undeclared: %v", cs.SyncMode, err)
		}
	}
}

// pgTime reads Postgres's ISO text as a full refresh reads the binary:
// days or microseconds since 1970, BC years, offsets to the second; a
// timestamp past what microseconds since 1970 hold is DATA_INVALID.
func TestPGTime(t *testing.T) {
	for s, want := range map[string]int64{
		"1970-01-02":    1,
		"0001-01-01 BC": -719528,
		"infinity":      math.MaxInt32,
		"-infinity":     math.MinInt32,
	} {
		if got, err := pgTime(s, false); err != nil || got != want {
			t.Errorf("date %s: %d, %v", s, got, err)
		}
	}
	for s, want := range map[string]int64{
		"1969-12-31 23:59:59.5":        -500_000,
		"1970-01-01 01:00:00+01":       0,
		"1970-01-01 00:53:28+00:53:28": 0,
		"1969-12-31 17:00:00.25-07":    250_000,
		"infinity":                     math.MaxInt64,
	} {
		if got, err := pgTime(s, true); err != nil || got != want {
			t.Errorf("timestamp %s: %d, %v", s, got, err)
		}
	}
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if _, err := pgTime("294270-01-01 00:00:00", true); !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID {
		t.Errorf("a timestamp past Arrow's: %v", err)
	}
	if _, err := pgTime("soon", false); err == nil {
		t.Error("a date that isn't one")
	}
}

// A CDC read of a table with a column the run didn't declare fails,
// retryably, before it reads the table's snapshot without the column.
func TestReadCDCFailsOnAnUndeclaredColumn(t *testing.T) {
	s, err := NewSource([]byte(`{"dsn": "postgres://u:p@127.0.0.1:1/db?connect_timeout=1"}`))
	if err != nil {
		t.Fatal(err)
	}
	users := table{name: "users", cols: []sourceColumn{{name: "id", oid: pgtype.Int4OID}, {name: "w", oid: pgtype.Int8OID}}, key: []int{0}, order: []int{0}, replident: 'd'}
	cs := source.ConfiguredStream{Name: "users", SyncMode: source.CDC, Columns: []string{"id"}}
	err = s.readCDC(t.Context(), newConnPool(s.conn, 1), source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{cs}}, []source.ConfiguredStream{cs}, []table{users}, nil)
	if !isTransient(err) || !strings.Contains(err.Error(), `"w"`) {
		t.Errorf("a cdc read with w undeclared: %v", err)
	}
}

// A read of a table with a column declared as another type than the run
// declared it fails, retryably, before it reads, in any mode: a bytea changed
// to text would land as other bytes. A change to a type declared the same,
// as varchar to text, reads on.
func TestReadFailsOnAChangedType(t *testing.T) {
	s, err := NewSource([]byte(`{"dsn": "postgres://u:p@127.0.0.1:1/db?connect_timeout=1"}`))
	if err != nil {
		t.Fatal(err)
	}
	declared := func(cols ...sourceColumn) *arrow.Schema {
		b, err := newBatcher(memory.DefaultAllocator, cols)
		if err != nil {
			t.Fatal(err)
		}
		b.release()
		return b.declared
	}
	id := sourceColumn{name: "id", oid: pgtype.Int4OID}
	was := declared(id, sourceColumn{name: "doc", oid: pgtype.ByteaOID}, sourceColumn{name: "note", oid: pgtype.VarcharOID})
	users := table{name: "users", cols: []sourceColumn{id, {name: "doc", oid: pgtype.TextOID}, {name: "note", oid: pgtype.TextOID}}, key: []int{0}, order: []int{0}, replident: 'd'}
	columns := []string{"id", "doc", "note"}
	for _, cs := range []source.ConfiguredStream{
		{Name: "users", SyncMode: source.FullRefresh, Columns: columns, Schema: was},
		{Name: "users", SyncMode: source.Incremental, Cursor: "id", Columns: columns, Schema: was},
	} {
		err := s.readSplit(t.Context(), newConnPool(s.conn, 1), []table{users}, cs, source.Split{}, nil)
		if !isTransient(err) || !strings.Contains(err.Error(), `"doc"`) {
			t.Errorf("%v read with doc changed: %v", cs.SyncMode, err)
		}
	}
	cs := source.ConfiguredStream{Name: "users", SyncMode: source.CDC, Columns: columns, Schema: was}
	err = s.readCDC(t.Context(), newConnPool(s.conn, 1), source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{cs}}, []source.ConfiguredStream{cs}, []table{users}, nil)
	if !isTransient(err) || !strings.Contains(err.Error(), `"doc"`) {
		t.Errorf("a cdc read with doc changed: %v", err)
	}
	if err := retyped(users, declared(id, sourceColumn{name: "doc", oid: pgtype.TextOID}, sourceColumn{name: "note", oid: pgtype.VarcharOID})); err != nil {
		t.Errorf("note's varchar changed to text: %v", err)
	}
	if err := retyped(users, nil); err != nil {
		t.Errorf("a request that doesn't say: %v", err)
	}
}
