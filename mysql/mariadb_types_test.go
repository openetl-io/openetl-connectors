//go:build integration

package mysql

import (
	"strings"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// TestCDCReadsMariaDBTypesAsTheSnapshotDoes covers the types only MariaDB has.
func TestCDCReadsMariaDBTypesAsTheSnapshotDoes(t *testing.T) {
	e := newSourceEnv(t, servers()[1])
	e.exec("CREATE TABLE $db.m (id int PRIMARY KEY, u uuid, i4 inet4, i6 inet6, j json)")
	values := `INSERT INTO $db.m VALUES (1, '123e4567-e89b-12d3-a456-426655440000', '192.168.0.1', '2001:db8::ff00:42:8329', '{"a": 1}'),
		(2, '00000000-0000-0000-0000-000000000001', '0.0.0.0', '::ffff:1.2.3.4', NULL)`
	e.exec(values)
	s := e.source("")
	first := read(t, s, cdc("mtypes", nil, "m"))
	snap := first.rows("m")
	e.exec("DELETE FROM $db.m")
	e.exec(values)
	next := read(t, s, cdc("mtypes", map[string]string{"m": first.state("m")}, "m"))
	var ins []map[string]string
	for _, r := range next.rows("m") {
		if r["openetl.op"] == source.OpInsert {
			ins = append(ins, r)
		}
	}
	if len(snap) != 2 || len(ins) != 2 {
		t.Fatalf("%d snapshot rows, %d inserts", len(snap), len(ins))
	}
	for i := range snap {
		for col, v := range snap[i] {
			if !strings.HasPrefix(col, "openetl.") && ins[i][col] != v {
				t.Errorf("row %d %s: the snapshot read %q, the binlog %q", i+1, col, v, ins[i][col])
			}
		}
	}
	if snap[0]["u"] != "123e4567-e89b-12d3-a456-426655440000" || snap[0]["i6"] != "2001:db8::ff00:42:8329" {
		t.Errorf("snapshot %v", snap[0])
	}
}

// TestMariaDBUUIDKeysReadInChunks reads a table keyed by a UUID in chunks,
// whole and by CDC, as the server orders UUIDs.
func TestMariaDBUUIDKeysReadInChunks(t *testing.T) {
	e := newSourceEnv(t, servers()[1])
	e.exec("CREATE TABLE $db.u (id uuid PRIMARY KEY, n int)")
	e.exec("INSERT INTO $db.u WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 50) SELECT UUID(), i FROM r")
	s := e.source(`, "chunkRows": 7, "snapshotChunkRows": 7`)
	if n := len(read(t, s, full("u")).rows("u")); n != 50 {
		t.Fatalf("full refresh: %d rows", n)
	}
	first := read(t, s, cdc("uuids", nil, "u"))
	if n := len(first.rows("u")); n != 50 {
		t.Fatalf("snapshot: %d rows", n)
	}
	e.exec("UPDATE $db.u SET n = -n WHERE n <= 10")
	next := read(t, s, cdc("uuids", map[string]string{"u": first.state("u")}, "u"))
	if n := len(next.rows("u")); n != 10 {
		t.Fatalf("%d changes", n)
	}
}
