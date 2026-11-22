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
		var n int
		if err := e.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+quoteIdent(e.database)+"._openetl_fence").Scan(&n); err != nil || n != 1 {
			t.Errorf("another session sees %d fence rows of 1 that Recover wrote: %v", n, err)
		}
		de.commit(w, 1, "out", batchOf(t, []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, `[{"x": 1}, {"x": 2}, {"x": 3}]`))
		if err := e.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+quoteIdent(e.database)+".out").Scan(&n); err != nil || n != 3 {
			t.Errorf("another session sees %d rows of 3 the destination wrote: %v", n, err)
		}
	})
}

// unchangedFields are a merge's columns for its unchanged tests; usersN
// lists its rows as id:name:n:unchanged.
var unchangedFields = []arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: opColumn, Type: arrow.BinaryTypes.String},
	{Name: seqColumn, Type: seqType, Nullable: true}, {Name: unchangedColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
}

// merge opens a writer that merges users, whose table ApplySchema makes
// with unchangedFields, as the engine does first.
func (e *destEnv) merge() destination.Writer {
	e.t.Helper()
	w := e.open(destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	if _, err := w.(destination.SchemaApplier).ApplySchema(e.t.Context(), []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema(unchangedFields, nil)}}); err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (e *destEnv) usersN() string {
	e.t.Helper()
	rows, err := e.db.QueryContext(e.t.Context(), "SELECT id, coalesce(name, '-'), n, coalesce(_openetl_unchanged, '-') FROM "+quoteIdent(e.database)+".users ORDER BY id")
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id, name, n, unchanged string
		if err := rows.Scan(&id, &name, &n, &unchanged); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, id+":"+name+":"+n+":"+unchanged)
	}
	return strings.Join(out, ",")
}

// TestMergeUnchangedWithoutSeq takes a column the latest version didn't
// send from the version staged before it that did, where the versions have
// no seq too: a null seq ranks below every other, as where the latest is
// picked.
func TestMergeUnchangedWithoutSeq(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newDestEnv(t, srv)
		w := e.merge()
		e.commit(w, 1, "users", batchOf(t, unchangedFields, `[
			{"id": 1, "name": "a", "n": 1, "_openetl_op": "insert"},
			{"id": 1, "name": null, "n": 2, "_openetl_op": "update", "_openetl_unchanged": ["name"]},
			{"id": 2, "name": "b", "n": 1, "_openetl_op": "insert"},
			{"id": 2, "name": null, "n": 2, "_openetl_op": "update", "_openetl_seq": 4, "_openetl_unchanged": ["name"]},
			{"id": 3, "name": "c", "n": 1, "_openetl_op": "insert", "_openetl_seq": 1},
			{"id": 3, "name": "old", "n": 0, "_openetl_op": "insert"},
			{"id": 3, "name": null, "n": 2, "_openetl_op": "update", "_openetl_seq": 2, "_openetl_unchanged": ["name"]}]`))
		if got := e.usersN(); got != "1:a:2:-,2:b:2:-,3:c:2:-" {
			t.Errorf("users = %s", got)
		}
	})
}

// TestMergeUnchangedOlderThanTarget keeps the target's value of a column
// the latest version didn't send where the staged version that sent it is
// older than the target's row, or where no staged version sent it.
func TestMergeUnchangedOlderThanTarget(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newDestEnv(t, srv)
		w := e.merge()
		e.commit(w, 1, "users", batchOf(t, unchangedFields, `[
			{"id": 1, "name": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": 10},
			{"id": 2, "name": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": 10},
			{"id": 3, "name": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": 10}]`))
		e.commit(w, 2, "users", batchOf(t, unchangedFields, `[
			{"id": 1, "name": "old", "n": 5, "_openetl_op": "update", "_openetl_seq": 5},
			{"id": 1, "name": null, "n": 11, "_openetl_op": "update", "_openetl_seq": 11, "_openetl_unchanged": ["name"]},
			{"id": 2, "name": "newer", "n": 12, "_openetl_op": "update", "_openetl_seq": 12},
			{"id": 2, "name": null, "n": 13, "_openetl_op": "update", "_openetl_seq": 13, "_openetl_unchanged": ["name"]},
			{"id": 3, "name": null, "n": 11, "_openetl_op": "update", "_openetl_seq": 11, "_openetl_unchanged": ["name"]}]`))
		if got := e.usersN(); got != "1:current:11:-,2:newer:13:-,3:current:11:-" {
			t.Errorf("users = %s", got)
		}
	})
}

// TestFencedKeepsNewerReads: an older generation, for which the stream
// appends, doesn't drop the read table of a newer one that overwrites it
// once the newer has raised the fence.
func TestFencedKeepsNewerReads(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newDestEnv(t, srv)
		open := func(gen uint64, mode destination.WriteMode) destination.Writer {
			w, err := e.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopePipeline,
				Streams: []destination.Stream{{Name: "users", Mode: mode}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Close(context.Background()) })
			if _, err := w.Recover(t.Context(), gen, destination.Epoch{}, nil); err != nil {
				t.Fatal(err)
			}
			return w
		}
		old := open(1, destination.Append)
		newer := open(2, destination.Overwrite)
		if err := newer.(interface {
			StartReads(context.Context, map[string]time.Time) error
		}).StartReads(t.Context(), map[string]time.Time{"users": time.UnixMicro(1000)}); err != nil {
			t.Fatal(err)
		}
		ep := destination.Epoch{Generation: 2, N: 1}
		if err := newer.Write(t.Context(), ep, "users", batchOf(t, []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, `[{"id": 1}]`)); err != nil {
			t.Fatal(err)
		}
		st, err := newer.PreCommit(t.Context(), ep)
		if err == nil {
			_, err = newer.Commit(t.Context(), ep, []destination.Staged{st})
		}
		if err != nil {
			t.Fatal(err)
		}
		exists := func() bool {
			var n int
			if err := e.db.QueryRowContext(t.Context(), "SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
				e.database, sqlstage.ReadName("p1", "users", time.UnixMicro(1000))).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n == 1
		}
		if !exists() {
			t.Fatal("the newer generation's read has no table")
		}
		old.(*writer).DropRemovedReads(t.Context())
		if !exists() {
			t.Error("the fenced generation dropped the newer one's read")
		}
	})
}
