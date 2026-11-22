package postgres

import (
	"errors"
	"strings"
	"testing"

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
