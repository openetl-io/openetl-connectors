//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type destEnv struct {
	t        *testing.T
	d        *Destination
	db       *sql.DB
	database string
}

func newDestEnv(t *testing.T, srv testServer) *destEnv {
	db, _ := testDB(t, srv.dsn)
	database := newDatabase(t, db)
	cfg, _ := json.Marshal(map[string]string{"dsn": srv.dsn, "database": database})
	d, err := NewDestination(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &destEnv{t: t, d: d, db: db, database: database}
}

func (e *destEnv) open(streams ...destination.Stream) destination.Writer {
	e.t.Helper()
	w, err := e.d.Open(e.t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: 1, Scope: destination.ScopePipeline, Streams: streams})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	if _, err := w.Recover(e.t.Context(), 1, destination.Epoch{}, nil); err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (e *destEnv) commit(w destination.Writer, n uint64, stream string, rec arrow.RecordBatch) {
	e.t.Helper()
	ep := destination.Epoch{Generation: 1, N: n}
	if err := w.Write(e.t.Context(), ep, stream, rec); err != nil {
		e.t.Fatal(err)
	}
	st, err := w.PreCommit(e.t.Context(), ep)
	if err == nil {
		_, err = w.Commit(e.t.Context(), ep, []destination.Staged{st})
	}
	if err != nil {
		e.t.Fatal(err)
	}
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

// TestMergeKeepsKeysThatDifferInCaseOrSpaces merges keys MySQL's default
// collations take as one: the destination's tables keep them apart.
func TestMergeKeepsKeysThatDifferInCaseOrSpaces(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newDestEnv(t, srv)
		w := e.open(destination.Stream{Name: "k", Mode: destination.Merge, PrimaryKey: []string{"id"}})
		fields := []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String}, {Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			source.OpField(), source.SeqField(seqType),
		}
		e.commit(w, 1, "k", batchOf(t, fields, `[{"id": "a", "v": 1, "openetl.seq": "1"}, {"id": "A", "v": 2, "openetl.seq": "2"},
			{"id": "a ", "v": 3, "openetl.seq": "3"}, {"id": "ä", "v": 4, "openetl.seq": "4"}]`))
		e.commit(w, 2, "k", batchOf(t, fields, `[{"id": "A", "v": 20, "openetl.op": "update", "openetl.seq": "5"}]`))
		rows, err := readTable(t.Context(), e.db, server{}, e.database, "k")
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range rows {
			got = append(got, fmt.Sprintf("%s=%v", r["id"], r["v"]))
		}
		slices.Sort(got)
		if want := []string{"A=20", "a =3", "a=1", "ä=4"}; !slices.Equal(got, want) {
			t.Fatalf("rows %q, want %q", got, want)
		}
	})
}

// TestTypeChangeRefusalRestatesTheColumn refuses a widening the pipeline
// doesn't allow, with a fix that keeps the column's NOT NULL and default.
func TestTypeChangeRefusalRestatesTheColumn(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newDestEnv(t, srv)
		w := e.open(destination.Stream{Name: "nums", Mode: destination.Append})
		e.commit(w, 1, "nums", batchOf(t, []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, `[{"x": 1}]`))
		mustExec(t, e.db, "ALTER TABLE "+quoteIdent(e.database)+".nums MODIFY COLUMN x int NOT NULL DEFAULT 7")
		md := arrow.NewMetadata([]string{sqlstage.TableKey}, []string{"nums"})
		wider := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, &md)
		applied, err := w.(destination.SchemaApplier).ApplySchema(t.Context(), []destination.StreamSchema{{Name: "nums", Schema: wider}})
		if err != nil {
			t.Fatal(err)
		}
		r := applied.Refused
		if len(r) != 1 || r[0].FromType != "int" || r[0].ToType != "bigint" || !strings.Contains(r[0].Fix, "MODIFY COLUMN `x` bigint NOT NULL DEFAULT") {
			t.Fatalf("refused %+v", r)
		}
		mustExec(t, e.db, r[0].Fix)
		var typ, null string
		var def sql.NullString
		err = e.db.QueryRowContext(t.Context(), "SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'nums' AND COLUMN_NAME = 'x'",
			e.database).Scan(&typ, &null, &def)
		if err != nil {
			t.Fatal(err)
		}
		if normType(typ) != "bigint" || null != "NO" || strings.Trim(def.String, "'") != "7" {
			t.Fatalf("after the fix: %s, nullable %s, default %v", typ, null, def)
		}
	})
}

// TestDestinationWithLeastPrivileges appends, overwrites and merges as a user
// with only the grants the README lists.
func TestDestinationWithLeastPrivileges(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		admin, _ := testDB(t, srv.dsn)
		database := newDatabase(t, admin)
		user := "w_" + database[2:12]
		mustExec(t, admin, "CREATE USER "+quoteLiteral(user)+" IDENTIFIED BY 'pw'")
		t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER "+quoteLiteral(user)) })
		mustExec(t, admin, "GRANT CREATE, ALTER, DROP, SELECT, INSERT, UPDATE, DELETE ON "+quoteIdent(database)+".* TO "+quoteLiteral(user))
		c, err := parseDSN(srv.dsn)
		if err != nil {
			t.Fatal(err)
		}
		c.User, c.Passwd, c.DBName = user, "pw", database
		d, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q, "database": %q}`, c.FormatDSN(), database))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Check(t.Context()); err != nil {
			t.Fatal(err)
		}
		e := &destEnv{t: t, d: d, db: admin, database: database}
		w := e.open(destination.Stream{Name: "a", Mode: destination.Append}, destination.Stream{Name: "o", Mode: destination.Overwrite},
			destination.Stream{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}})
		reads := w.(interface {
			StartReads(context.Context, map[string]time.Time) error
			EndRead(context.Context, destination.Epoch, string) error
		})
		if err := reads.StartReads(t.Context(), map[string]time.Time{"o": time.Now()}); err != nil {
			t.Fatal(err)
		}
		fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}}
		for n := uint64(1); n <= 2; n++ {
			ep := destination.Epoch{Generation: 1, N: n}
			rows := batchOf(t, fields, fmt.Sprintf(`[{"id": 1, "v": "x%d"}, {"id": %d, "v": "y"}]`, n, n+1))
			for _, s := range []string{"a", "o", "m"} {
				if err := w.Write(t.Context(), ep, s, rows); err != nil {
					t.Fatal(err)
				}
			}
			if n == 2 {
				if err := reads.EndRead(t.Context(), ep, "o"); err != nil {
					t.Fatal(err)
				}
			}
			st, err := w.PreCommit(t.Context(), ep)
			if err == nil {
				_, err = w.Commit(t.Context(), ep, []destination.Staged{st})
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		for table, want := range map[string]int{"a": 4, "o": 4, "m": 3} {
			rows, err := readTable(t.Context(), admin, server{}, database, table)
			if err != nil || len(rows) != want {
				t.Errorf("%s: %d rows, want %d: %v", table, len(rows), want, err)
			}
		}
	})
}

// TestPatchFillsAHeldColumnTwice holds a merge's price, then patches it from
// two columns, price__v2 that fills it and price itself: both land, and the
// cell is held no more.
func TestPatchFillsAHeldColumnTwice(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newDestEnv(t, srv)
		w := e.open(destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
		id := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
		rowID := arrow.Field{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{uuidExtension})}
		op := arrow.Field{Name: opColumn, Type: arrow.BinaryTypes.String}
		seq := arrow.Field{Name: seqColumn, Type: seqType, Nullable: true}
		held := arrow.Field{Name: heldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
		price := arrow.Field{Name: "price", Type: arrow.BinaryTypes.String, Nullable: true}
		v2 := arrow.Field{Name: "price__v2", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
		const uid = `"AQEBAQEBAQEBAQEBAQEBAQ=="`
		e.commit(w, 1, "users", batchOf(t, []arrow.Field{id, rowID, op, seq, price, v2, held},
			`[{"id": 1, "_openetl_id": `+uid+`, "_openetl_op": "update", "_openetl_seq": "1", "price": null, "_openetl_held": ["price"]}]`))
		fills := v2
		fills.Metadata = arrow.NewMetadata([]string{destination.FillsKey}, []string{"price"})
		ep := destination.Epoch{Generation: 1, N: 2}
		err := w.(destination.Patcher).Patch(t.Context(), ep, "users", batchOf(t, []arrow.Field{id, rowID, seq, held, fills, price},
			`[{"id": 1, "_openetl_id": `+uid+`, "_openetl_seq": "1", "_openetl_held": ["price"], "price__v2": 2.5, "price": "2.50"}]`))
		if err != nil {
			t.Fatal(err)
		}
		st, err := w.PreCommit(t.Context(), ep)
		if err == nil {
			_, err = w.Commit(t.Context(), ep, []destination.Staged{st})
		}
		if err != nil {
			t.Fatal(err)
		}
		var got string
		err = e.db.QueryRowContext(t.Context(), "SELECT CONCAT(coalesce(price, '-'), ':', price__v2, ':', coalesce(_openetl_held, '-')) FROM "+quoteIdent(e.database)+".users").Scan(&got)
		if want := "2.50:2.5:-"; err != nil || got != want {
			t.Fatalf("price:price__v2:held %s %v, want %s", got, err, want)
		}
	})
}

// TestSessionsCommitAndReadEveryRow reads and writes with a DSN whose
// sessions don't commit by themselves and return one row a SELECT: the
// source reads every row, and the destination's rows are there for another
// session to read.
func TestSessionsCommitAndReadEveryRow(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		if strings.Contains(srv.dsn, "?") {
			t.Skip("the DSN has parameters of its own")
		}
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, v varchar(20))")
		e.exec("INSERT INTO $db.items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
		e.exec("CREATE TABLE $db.keyless (v varchar(20))")
		e.exec("INSERT INTO $db.keyless VALUES ('a'), ('b'), ('c')")
		e.srv.dsn += "?autocommit=0&sql_select_limit=1"
		c := read(t, e.source(""), full("items", "keyless"))
		for _, s := range []string{"items", "keyless"} {
			if rows := c.rows(s); len(rows) != 3 || rows[0]["v"] != "a" {
				t.Errorf("the source read %s's rows %v, want 3 with v", s, rows)
			}
		}

		cfg, _ := json.Marshal(map[string]string{"dsn": e.srv.dsn, "database": e.database})
		d, err := NewDestination(cfg)
		if err != nil {
			t.Fatal(err)
		}
		de := &destEnv{t: t, d: d, db: e.db, database: e.database}
		w := de.open(destination.Stream{Name: "out", Mode: destination.Append})
		de.commit(w, 1, "out", batchOf(t, []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, `[{"x": 1}, {"x": 2}, {"x": 3}]`))
		var n int
		if err := e.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+quoteIdent(e.database)+".out").Scan(&n); err != nil || n != 3 {
			t.Errorf("another session sees %d rows of 3 the destination wrote: %v", n, err)
		}
	})
}
