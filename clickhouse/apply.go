package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// stagedTable is one lane's staging table of a stream.
type stagedTable struct {
	lane int
	name string
	cols []column
}

// stagingTables reads the columns of a stream's staging tables on every
// lane, and returns them with the stream's columns, each at the widest type
// a lane staged it in. Nil if one of them doesn't exist.
func (w *writer) stagingTables(ctx context.Context, lanes [][]string) ([]stagedTable, []column, error) {
	var all []stagedTable
	var cols []column
	for lane, tables := range lanes {
		for _, t := range tables {
			have, err := w.columnTypes(ctx, t)
			if err != nil {
				return nil, nil, err
			}
			if len(have) == 0 {
				return nil, nil, nil
			}
			have = slices.DeleteFunc(have, func(c column) bool { return c.name == rowColumn })
			all = append(all, stagedTable{lane: lane, name: t, cols: have})
			for _, c := range have {
				switch i := indexOfColumn(cols, c.name); {
				case i < 0:
					cols = append(cols, c)
				case widens(cols[i].typ, c.typ):
					cols[i] = c
				}
			}
		}
	}
	return all, cols, nil
}

// union is the relation that reads every lane's staging of a stream as one,
// with cols at the target's types, and each row's lane and place in it.
func (w *writer) union(tables []stagedTable, cols []string, target map[string]string) string {
	selects := make([]string, len(tables))
	for i, t := range tables {
		sel := make([]string, 0, len(cols)+2)
		for _, c := range cols {
			j := indexOfColumn(t.cols, c)
			switch {
			case j >= 0 && c == heldColumn && target[c] == heldType:
				id := "''"
				if indexOfColumn(t.cols, idColumn) >= 0 {
					id = "hex(" + ident(idColumn) + ")"
				}
				sel = append(sel, "CAST(arrayMap(x -> (assumeNotNull(x), "+id+"), arrayFilter(x -> x IS NOT NULL, "+ident(c)+")), "+literal(heldType)+") AS "+ident(c))
			case j >= 0 && canonical(t.cols[j].typ) == canonical(target[c]):
				sel = append(sel, ident(c))
			case j >= 0:
				sel = append(sel, "CAST("+ident(c)+" AS "+target[c]+") AS "+ident(c))
			default:
				sel = append(sel, "defaultValueOfTypeName("+literal(target[c])+") AS "+ident(c))
			}
		}
		sel = append(sel, strconv.Itoa(t.lane)+" AS "+ident(laneColumn), ident(rowColumn))
		selects[i] = "SELECT " + strings.Join(sel, ", ") + " FROM " + w.q(t.name)
	}
	return "(" + strings.Join(selects, " UNION ALL ") + ")"
}

// build fills a table with what the stream's part of the epoch adds to the
// target, as the target holds it, and names it ready once it's whole. It
// returns the rows it holds.
func (w *writer) build(ctx context.Context, desc staged, s stagedStream, ready string) (uint64, error) {
	tables, cols, err := w.stagingTables(ctx, s.stagings)
	if err != nil {
		return 0, err
	}
	if tables == nil {
		return 0, w.missing(ctx, desc, s)
	}
	if err := w.ensureTable(ctx, s.Name, s.Table, cols); err != nil {
		return 0, err
	}
	have, err := w.columnTypes(ctx, s.Table)
	if err != nil {
		return 0, err
	}
	target := map[string]string{}
	for _, c := range have {
		target[c.name] = c.typ
	}
	if s.Mode == modeOverwrite {
		if err := w.ensureRead(ctx, s); err != nil {
			return 0, err
		}
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	tmp := ready + "_" + hex.EncodeToString(b[:])
	l := label{Pipeline: w.cfg.Pipeline, Generation: desc.Generation, Epoch: desc.Epoch}
	if err := w.exec(ctx, "CREATE TABLE "+w.q(tmp)+" AS "+w.q(s.Table)+" COMMENT "+literal(l.comment())); err != nil {
		return 0, fmt.Errorf("create the epoch's table: %w", err)
	}
	built := false
	defer func() {
		if !built {
			w.drop(context.WithoutCancel(ctx), tmp)
		}
	}()
	rel := w.union(tables, names(cols), target)
	var q string
	if s.Mode == modeMerge {
		if q, err = w.mergeQuery(ctx, s, names(cols), rel, have, tmp); err != nil {
			return 0, err
		}
	} else {
		q = w.insertQuery(s, names(cols), rel, tmp)
	}
	// The epoch's table, partitioned as the target is, takes rows of any
	// number of its partitions, as they arrived.
	if err := w.exec(ch.Context(ctx, ch.WithSettings(ch.Settings{"join_use_nulls": 0, "max_partitions_per_insert_block": 0})), q); err != nil {
		return 0, err
	}
	var n uint64
	if err := w.conn.QueryRow(ctx, "SELECT count() FROM "+w.q(tmp)).Scan(&n); err != nil {
		return 0, classify(err)
	}
	err = w.exec(ctx, "RENAME TABLE "+w.q(tmp)+" TO "+w.q(ready))
	if isCode(err, codeTableAlreadyExists) {
		// Another attempt built it first.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	built = true
	return n, nil
}

// missing explains staging that isn't there: a newer generation's Recover
// drops what older ones staged.
func (w *writer) missing(ctx context.Context, desc staged, s stagedStream) error {
	if err := w.checkFence(ctx); errors.Is(err, destination.ErrFenced) {
		return err
	}
	return fmt.Errorf("clickhouse: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
}

// staged is the order rows were staged in, across lanes.
var stagedOrder = ident(laneColumn) + ", " + ident(rowColumn)

// insertQuery copies a stream's staging into tmp, in the order it was
// staged. A deduped stream inserts one row per _openetl_id, and with Dedupe
// only IDs the target, or the read's table, doesn't have.
func (w *writer) insertQuery(s stagedStream, cols []string, rel, tmp string) string {
	list := quoteAll(cols)
	q := "INSERT INTO " + w.q(tmp) + " (" + list + ") SELECT " + list + " FROM " + rel
	id := ident(idColumn)
	if s.Dedupe {
		q += " WHERE " + id + " NOT IN (SELECT " + id + " FROM " + w.q(s.into()) + " WHERE " + id + " IN (SELECT " + id + " FROM " + rel + "))"
	}
	q += " ORDER BY " + stagedOrder
	if s.deduped() {
		q += " LIMIT 1 BY " + id
	}
	return q
}

// mergeQuery inserts into tmp each key's latest staged version, by
// _openetl_seq and then staging order; the target's ReplacingMergeTree
// keeps it unless the target's version is newer. A soft delete keeps the
// row's other columns as the target has them, with its metadata columns
// the delete's; a hard delete sets _openetl_is_deleted, which FINAL leaves
// out. A column the latest version didn't send, as _openetl_unchanged says,
// takes its value from the key's latest staged version that did, or else
// keeps the target's.
func (w *writer) mergeQuery(ctx context.Context, s stagedStream, cols []string, rel string, target []column, tmp string) (string, error) {
	hasSeq, hasOp := slices.Contains(cols, seqColumn), slices.Contains(cols, opColumn)
	hasUnchanged := slices.Contains(cols, unchangedColumn)
	_, isDeleted := indexOf(target, isDeletedColumn)
	if s.HardDeletes && hasOp && !isDeleted {
		return "", fmt.Errorf("clickhouse: %s hard-deletes, and its table has no %s; it isn't one this destination made for merge", s.Table, isDeletedColumn)
	}
	key := quoteAll(s.Key)
	order := []string{}
	if hasSeq {
		order = append(order, ident(seqColumn))
	}
	order = append(order, ident(laneColumn), ident(rowColumn))
	desc := make([]string, len(order))
	for i, o := range order {
		desc[i] = o + " DESC"
	}
	latest := "(SELECT * FROM " + rel + " ORDER BY " + key + ", " + strings.Join(desc, ", ") + " LIMIT 1 BY " + key + ")"
	var unchanged []string
	if hasUnchanged {
		rows, err := w.conn.Query(ctx, "SELECT DISTINCT arrayJoin("+ident(unchangedColumn)+") AS c FROM "+rel+" ORDER BY c")
		if err != nil {
			return "", classify(err)
		}
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				_ = rows.Close()
				return "", err
			}
			if slices.Contains(cols, c) && !slices.Contains(s.Key, c) && !isMetadata(c) {
				unchanged = append(unchanged, c)
			}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return "", err
		}
	}
	soft := hasOp && !s.HardDeletes
	var last []string
	if slices.Contains(cols, heldColumn) {
		for _, c := range s.Last {
			if slices.Contains(cols, c) && !slices.Contains(s.Key, c) && !isMetadata(c) {
				last = append(last, c)
			}
		}
	}
	insertCols := slices.Clone(cols)
	if isDeleted && !slices.Contains(insertCols, isDeletedColumn) {
		insertCols = append(insertCols, isDeletedColumn)
	}
	join := soft || len(unchanged) > 0 || len(last) > 0
	sel := make([]string, len(insertCols))
	isDelete := "l." + ident(opColumn) + " = 'delete'"
	tcol := func(c string) string { return "t._t" + strconv.Itoa(slices.Index(cols, c)) }
	for i, c := range insertCols {
		v := "l." + ident(c)
		switch {
		case c == isDeletedColumn && hasOp && s.HardDeletes:
			v = "toUInt8(" + isDelete + ")"
		case c == isDeletedColumn:
			v = "toUInt8(0)"
		case slices.Contains(s.Key, c):
		case c == unchangedColumn && join:
			// The columns neither the latest version nor any earlier one
			// sent, and, where the target has the key, it never received.
			var resolved []string
			for j, u := range unchanged {
				resolved = append(resolved, "(x = "+literal(u)+" AND r._f"+strconv.Itoa(j)+")")
			}
			remaining := v
			if len(resolved) > 0 {
				remaining = "arrayFilter(x -> NOT (" + strings.Join(resolved, " OR ") + "), " + v + ")"
			}
			v = "if(t._found = 1, arrayIntersect(" + remaining + ", " + tcol(c) + "), " + remaining + ")"
		case slices.Contains(unchanged, c):
			j := strconv.Itoa(slices.Index(unchanged, c))
			v = "if(has(l." + ident(unchangedColumn) + ", " + literal(c) + "), if(r._f" + j + ", r._r" + j + ", if(t._found = 1, " + tcol(c) + ", " + v + ")), " + v + ")"
		}
		if slices.Contains(last, c) {
			// A held cell keeps the target's value, and stays held, by
			// this version, which a patch then fills.
			v = "if(mapContains(l." + ident(heldColumn) + ", " + literal(c) + ") AND t._found = 1, " + tcol(c) + ", " + v + ")"
		}
		if soft && join && (!isMetadata(c) || c == unchangedColumn || c == heldColumn) && !slices.Contains(s.Key, c) {
			v = "if(" + isDelete + " AND t._found = 1, " + tcol(c) + ", " + v + ")"
		}
		sel[i] = v + " AS " + ident(c)
	}
	q := "INSERT INTO " + w.q(tmp) + " (" + quoteAll(insertCols) + ") SELECT " + strings.Join(sel, ", ") + " FROM " + latest + " AS l"
	if !join {
		return q, nil
	}
	on := func(alias, prefix string) string {
		parts := make([]string, len(s.Key))
		for i, k := range s.Key {
			parts[i] = "l." + ident(k) + " = " + alias + "." + prefix + strconv.Itoa(i)
		}
		return strings.Join(parts, " AND ")
	}
	keyAs := func(prefix string) []string {
		out := make([]string, len(s.Key))
		for i, k := range s.Key {
			out[i] = ident(k) + " AS " + prefix + strconv.Itoa(i)
		}
		return out
	}
	tsel := keyAs("_k")
	for i, c := range cols {
		if !slices.Contains(s.Key, c) {
			tsel = append(tsel, ident(c)+" AS _t"+strconv.Itoa(i))
		}
	}
	tsel = append(tsel, "toUInt8(1) AS _found")
	q += " LEFT JOIN (SELECT " + strings.Join(tsel, ", ") + " FROM " + w.q(s.Table) + " FINAL WHERE (" + key + ") IN (SELECT " + key + " FROM " + rel + ")) AS t ON " + on("t", "_k")
	if len(unchanged) > 0 {
		ord := "tuple(" + strings.Join(order, ", ") + ")"
		rsel := keyAs("_k")
		for j, u := range unchanged {
			sent := "NOT has(" + ident(unchangedColumn) + ", " + literal(u) + ")"
			rsel = append(rsel, "argMaxIf(tuple("+ident(u)+"), "+ord+", "+sent+").1 AS _r"+strconv.Itoa(j), "countIf("+sent+") > 0 AS _f"+strconv.Itoa(j))
		}
		q += " LEFT JOIN (SELECT " + strings.Join(rsel, ", ") + " FROM " + rel + " GROUP BY " + key + ") AS r ON " + on("r", "_k")
	}
	return q, nil
}

// patch fills held cells from the epoch's patches, after its rows: each
// cell where the target still holds the version the patch comes from, by
// key for merge and by _openetl_id for append, and takes the column off the
// row's held ones. The patches, one row per key and version, go in a Join
// table that one mutation looks each row's held versions up in. Run again,
// it fills nothing more, as what it filled is no longer held.
func (w *writer) patch(ctx context.Context, desc staged, s stagedStream) error {
	tables, cols, err := w.stagingTables(ctx, s.patches)
	if err != nil {
		return err
	}
	if tables == nil {
		return w.missing(ctx, desc, s)
	}
	have, err := w.columnTypes(ctx, s.Table)
	if err != nil {
		return err
	}
	target := map[string]string{}
	for _, c := range have {
		target[c.name] = c.typ
	}
	if target[heldColumn] != heldType {
		return nil
	}
	keyed, err := w.keyColumnsOf(ctx, s.Table)
	if err != nil {
		return err
	}
	staged := map[string]string{}
	for _, c := range cols {
		staged[c.name] = c.typ
	}
	rel := w.union(tables, names(cols), staged)
	listed, err := w.strings(ctx, "SELECT DISTINCT assumeNotNull(c) AS h FROM (SELECT arrayJoin("+ident(heldColumn)+") AS c FROM "+rel+") WHERE c IS NOT NULL ORDER BY h")
	if err != nil {
		return err
	}
	// The columns the patches fill, each with the held column whose cell
	// it fills: its own, or the one its FillsKey names, as col__v2 fills
	// col's.
	var filled []string
	heldOf := map[string]string{}
	for _, c := range names(cols) {
		held := c
		if f, ok := s.Fills[c]; ok {
			held = f
		}
		if _, ok := target[c]; !ok || !slices.Contains(listed, held) || isMetadata(c) || isMetadata(held) || slices.Contains(keyed, c) || slices.Contains(s.Key, c) {
			continue
		}
		from, _ := unwrapNullable(staged[c])
		to, _ := unwrapNullable(target[c])
		if !widens(from, to) {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT, fmt.Errorf("clickhouse: patches of %s fill %q with %s, which its %s doesn't hold", s.Table, c, from, target[c]))
		}
		filled = append(filled, c)
		heldOf[c] = held
	}
	if len(filled) == 0 {
		return nil
	}
	join := joinName(w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch)
	var defs, keys, groups, sel []string
	for _, k := range s.Key {
		defs = append(defs, ident(k)+" "+target[k])
		keys = append(keys, ident(k))
		sel = append(sel, "CAST("+ident(k)+" AS "+target[k]+")")
	}
	groups = append(slices.Clone(keys), ident(idColumn))
	keys = append(keys, "_p_id")
	held := "arrayMap(x -> assumeNotNull(x), arrayFilter(x -> x IS NOT NULL, " + ident(heldColumn) + "))"
	defs = append(defs, "_p_id String", "_h Array(String)")
	sel = append(sel, "hex("+ident(idColumn)+")", "groupUniqArrayArray("+held+")")
	synced := target[syncedColumn] != "" && staged[syncedColumn] != ""
	if synced {
		defs = append(defs, "_s "+target[syncedColumn])
		sel = append(sel, "max(CAST("+ident(syncedColumn)+" AS "+target[syncedColumn]+"))")
	}
	for i, c := range filled {
		defs = append(defs, "_f"+strconv.Itoa(i)+" "+target[c])
		sel = append(sel, "anyIf(tuple(CAST("+ident(c)+" AS "+target[c]+")), has("+held+", "+literal(heldOf[c])+")).1")
	}
	l := label{Pipeline: w.cfg.Pipeline, Generation: desc.Generation, Epoch: desc.Epoch}
	for _, q := range []string{
		"DROP TABLE IF EXISTS " + w.q(join) + " SYNC",
		"CREATE TABLE " + w.q(join) + " (" + strings.Join(defs, ", ") + ") ENGINE = Join(ANY, LEFT, " + strings.Join(keys, ", ") + ") COMMENT " + literal(l.comment()),
		"INSERT INTO " + w.q(join) + " SELECT " + strings.Join(sel, ", ") + " FROM " + rel + " GROUP BY " + strings.Join(groups, ", "),
	} {
		if err := w.exec(ctx, q); err != nil {
			return err
		}
	}
	lookup := func(col, version string) string {
		// joinGet takes its table as database.table, unquoted.
		args := []string{literal(w.db + "." + join), literal(col)}
		for _, k := range s.Key {
			args = append(args, ident(k))
		}
		return "joinGet(" + strings.Join(append(args, version), ", ") + ")"
	}
	h := ident(heldColumn)
	// fills is whether the row holds the version of a patch that fills
	// held's cell.
	fills := func(held string) string {
		return "has(" + lookup("_h", h+"["+literal(held)+"]") + ", " + literal(held) + ")"
	}
	var set, anyFilled []string
	for i, c := range filled {
		cond := fills(heldOf[c])
		anyFilled = append(anyFilled, cond)
		set = append(set, ident(c)+" = if("+cond+", "+lookup("_f"+strconv.Itoa(i), h+"["+literal(heldOf[c])+"]")+", "+ident(c)+")")
	}
	if synced {
		// The patch's version is any of the held ones it filled.
		var versions []string
		for _, c := range filled {
			versions = append(versions, "if("+fills(heldOf[c])+", "+h+"["+literal(heldOf[c])+"], '')")
		}
		v := "coalesce(nullIf(" + strings.Join(versions, ", ''), nullIf(") + ", ''))"
		set = append(set, ident(syncedColumn)+" = if("+strings.Join(anyFilled, " OR ")+", "+lookup("_s", "assumeNotNull("+v+")")+", "+ident(syncedColumn)+")")
	}
	set = append(set, h+" = mapFilter((_hc, _hv) -> NOT has("+lookup("_h", "_hv")+", _hc), "+h+")")
	where := ident(idColumn) + " IN (SELECT " + ident(idColumn) + " FROM " + rel + ")"
	if len(s.Key) > 0 {
		k := "(" + quoteAll(s.Key) + ")"
		where = k + " IN (SELECT " + k + " FROM " + rel + ")"
	}
	mctx := ch.Context(ctx, ch.WithSettings(ch.Settings{"mutations_sync": 2}))
	err = w.exec(mctx, "ALTER TABLE "+w.q(s.Table)+" UPDATE "+strings.Join(set, ", ")+" WHERE notEmpty("+h+") AND "+where)
	if err != nil {
		// A failed mutation stays, and is retried, until it's killed. This
		// patch's is the one whose command names its Join table; the table's
		// other mutations aren't the destination's to kill.
		_ = w.conn.Exec(context.WithoutCancel(ctx), killPatch, w.db, s.Table, join)
	}
	return err
}

// killPatch kills a patch's mutation if it's unfinished. Its arguments are
// the database, the table, and the patch's Join table, which only that
// mutation's command names.
const killPatch = "KILL MUTATION WHERE database = ? AND table = ? AND NOT is_done AND position(command, ?) > 0"

// strings runs a query of one String column.
func (w *writer) strings(ctx context.Context, q string) ([]string, error) {
	rows, err := w.conn.Query(ctx, q)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func indexOf(cols []column, name string) (int, bool) {
	i := indexOfColumn(cols, name)
	return i, i >= 0
}

// partitions lists a table's partitions that have rows.
func (w *writer) partitions(ctx context.Context, table string) ([]string, error) {
	rows, err := w.conn.Query(ctx, "SELECT DISTINCT partition_id FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY partition_id", w.db, table)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// move moves every part of ready into the table into: each partition at
// once, and none twice. It first gives ready the columns into has gained.
func (w *writer) move(ctx context.Context, ready, into string) error {
	parts, err := w.partitions(ctx, ready)
	if err != nil || len(parts) == 0 {
		return err
	}
	if err := w.reconcile(ctx, ready, into); err != nil {
		return err
	}
	for _, p := range parts {
		if err := w.exec(ctx, "ALTER TABLE "+w.q(ready)+" MOVE PARTITION ID "+literal(p)+" TO TABLE "+w.q(into)); err != nil {
			return fmt.Errorf("move the epoch's rows into %s: %w", into, err)
		}
	}
	return nil
}

// reconcile gives table the columns and types of like, which has gained
// columns or widened them since table was made like it.
func (w *writer) reconcile(ctx context.Context, table, like string) error {
	have, err := w.columnTypes(ctx, table)
	if err != nil {
		return err
	}
	want, err := w.columnTypes(ctx, like)
	if err != nil {
		return err
	}
	var clauses []string
	for _, c := range want {
		switch i := indexOfColumn(have, c.name); {
		case i < 0:
			clauses = append(clauses, "ADD COLUMN IF NOT EXISTS "+ident(c.name)+" "+c.typ)
		case canonical(have[i].typ) != canonical(c.typ):
			clauses = append(clauses, "MODIFY COLUMN "+ident(c.name)+" "+c.typ)
		}
	}
	if len(clauses) == 0 {
		return nil
	}
	return w.exec(ctx, "ALTER TABLE "+w.q(table)+" "+strings.Join(clauses, ", "))
}

// replace makes the read's rows the target's: each of its partitions
// replaces the target's, and the target's others are dropped. Run again,
// it changes nothing more.
func (w *writer) replace(ctx context.Context, desc staged, s stagedStream) error {
	if s.Table == "" {
		return nil
	}
	ok, err := w.exists(ctx, s.Table)
	if err != nil || !ok {
		return err
	}
	var read []string
	if has, err := w.exists(ctx, s.Read); err != nil {
		return err
	} else if has {
		if err := w.reconcile(ctx, s.Read, s.Table); err != nil {
			return err
		}
		if read, err = w.partitions(ctx, s.Read); err != nil {
			return err
		}
	} else if done, err := w.marked(ctx, desc, s.Name); err != nil || done {
		// Another applier finished the epoch, and dropped the read's
		// table, since this one looked: the read wasn't empty.
		return err
	}
	for _, p := range read {
		if err := w.exec(ctx, "ALTER TABLE "+w.q(s.Table)+" REPLACE PARTITION ID "+literal(p)+" FROM "+w.q(s.Read)); err != nil {
			return err
		}
	}
	have, err := w.partitions(ctx, s.Table)
	if err != nil {
		return err
	}
	for _, p := range have {
		if !slices.Contains(read, p) {
			if err := w.exec(ctx, "ALTER TABLE "+w.q(s.Table)+" DROP PARTITION ID "+literal(p)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureRead creates the read's table like the target, labelled, or gives
// it the columns the target gained.
func (w *writer) ensureRead(ctx context.Context, s stagedStream) error {
	ok, err := w.exists(ctx, s.Read)
	if err != nil {
		return err
	}
	if ok {
		return w.reconcile(ctx, s.Read, s.Table)
	}
	l := label{Pipeline: w.cfg.Pipeline, Generation: w.cfg.Generation, Stream: s.Name}
	err = w.exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.q(s.Read)+" AS "+w.q(s.Table)+" COMMENT "+literal(l.comment()))
	if err != nil {
		return fmt.Errorf("clickhouse: create the read table for %s: %w", s.Name, err)
	}
	return nil
}

// columnTypes returns a table's columns in order, or none if it doesn't
// exist.
func (w *writer) columnTypes(ctx context.Context, table string) ([]column, error) {
	rows, err := w.conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position", w.db, table)
	if err != nil {
		return nil, classify(fmt.Errorf("clickhouse: read the columns of %s: %w", table, err))
	}
	defer func() { _ = rows.Close() }()
	var out []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.typ); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// notNull says which of a stream's columns its table holds no null in: the
// metadata columns, and a merge's key.
func (w *writer) notNull(stream string, key []string) func(string) bool {
	return func(c string) bool {
		return isMetadata(c) || w.streams[stream].Mode == destination.Merge && slices.Contains(key, c)
	}
}

// ApplySchema creates each stream's table and adds its new columns, in one
// statement per table. A column whose type changed is refused, unless the
// stream allows the change and it widens, and the rest applied. It reports
// the columns each table can't take a null in.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	w.ddl.Lock()
	defer w.ddl.Unlock()
	var out destination.SchemaApplied
	if err := w.ensureDatabase(ctx); err != nil {
		return out, err
	}
	out.Limits = make(map[string]destination.TableLimits, len(streams))
	for _, s := range streams {
		cfg := w.streams[s.Name]
		var key []string
		if cfg.Mode == destination.Merge {
			var err error
			if key, err = sqlstage.KeyColumns(s.Schema, cfg.PrimaryKey); err != nil {
				return out, fmt.Errorf("clickhouse: stream %q: %w", s.Name, err)
			}
		}
		cols, err := columnsOf(s.Schema, w.notNull(s.Name, key))
		if err != nil {
			return out, err
		}
		cols = targetColumns(cols)
		table := targetTable(s.Schema, s.Name)
		refused, err := w.alter(ctx, s.Name, table, cols, key)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		for _, r := range refused {
			r.Stream = s.Name
			out.Refused = append(out.Refused, r)
		}
		have, err := w.columnTypes(ctx, table)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		keyed, err := w.keyColumnsOf(ctx, table)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		var l destination.TableLimits
		for _, c := range have {
			if strings.HasPrefix(c.name, "_openetl_") {
				continue
			}
			if !canHoldNull(c.typ) {
				l.NotNull = append(l.NotNull, c.name)
			}
			// A patch can't change a key's column.
			if slices.Contains(keyed, c.name) {
				l.Unholdable = append(l.Unholdable, c.name)
			}
		}
		out.Limits[s.Name] = l
		w.setTable(s.Name, table)
	}
	return out, nil
}

// ensureTable creates stream's table with cols, or adds its columns to hold
// them, before an apply.
func (w *writer) ensureTable(ctx context.Context, stream, table string, cols []column) error {
	key := []string(nil)
	if w.streams[stream].Mode == destination.Merge {
		key = w.streams[stream].PrimaryKey
	}
	// Staging holds every column as it would a data column; the target's
	// own are what ApplySchema makes.
	want := make([]column, len(cols))
	for i, c := range cols {
		want[i] = c
		if inner, ok := unwrapNullable(c.typ); ok && (isMetadata(c.name) || slices.Contains(key, c.name)) {
			want[i].typ = inner
		}
	}
	refused, err := w.alter(ctx, stream, table, targetColumns(want), key)
	if err != nil {
		return err
	}
	if len(refused) > 0 {
		r := refused[0]
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT, fmt.Errorf("clickhouse: column %q of %s: %s%s", r.Column, table, r.Reason, fixOf(r)))
	}
	return nil
}

// targetColumns are a table's columns for staged ones: a held list is a
// map in the target.
func targetColumns(cols []column) []column {
	out := slices.Clone(cols)
	for i := range out {
		if out[i].name == heldColumn {
			out[i].typ = heldType
		}
	}
	return out
}

func fixOf(r destination.RefusedColumn) string {
	if r.Fix == "" {
		return ""
	}
	return "; to make the change, run " + r.Fix
}

// alter creates the table with cols if it doesn't exist, or adds its new
// columns in one ALTER TABLE, and returns the columns it refused, with why.
// It changes a column's type only if the stream allows type changes and the
// change widens it, as MODIFY COLUMN rewrites the column's data.
func (w *writer) alter(ctx context.Context, stream, table string, cols []column, key []string) ([]destination.RefusedColumn, error) {
	have, err := w.columnTypes(ctx, table)
	if err != nil {
		return nil, err
	}
	if len(have) == 0 {
		return nil, w.create(ctx, stream, table, cols, key)
	}
	allow := w.streams[stream].AllowTypeChanges
	keyed, err := w.keyColumnsOf(ctx, table)
	if err != nil {
		return nil, err
	}
	var refused []destination.RefusedColumn
	var clauses []string
	for _, c := range cols {
		i := indexOfColumn(have, c.name)
		if i < 0 {
			clauses = append(clauses, "ADD COLUMN IF NOT EXISTS "+ident(c.name)+" "+c.typ)
			continue
		}
		want, nullable := unwrapNullable(c.typ)
		was, wasNullable := unwrapNullable(have[i].typ)
		if nullable && !wasNullable {
			// The table's column holds no null, as its user made it: it
			// keeps that.
			c.typ = want
		}
		modify := "ALTER TABLE " + w.q(table) + " MODIFY COLUMN " + ident(c.name) + " " + c.typ
		switch {
		case widens(c.typ, have[i].typ):
		case canonical(want) == canonical(was):
		case slices.Contains(keyed, c.name):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: "it's in the table's sorting, primary or partition key, whose types ClickHouse doesn't change", FromType: have[i].typ, ToType: c.typ,
			})
		case !widens(have[i].typ, c.typ):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: fmt.Sprintf("it doesn't change a column from %s to %s, which can lose values", have[i].typ, c.typ), FromType: have[i].typ, ToType: c.typ,
				Fix: modify,
			})
		case allow:
			clauses = append(clauses, "MODIFY COLUMN "+ident(c.name)+" "+c.typ)
		default:
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: "it doesn't change a column's type unless the pipeline's typeChanges allows it, as that rewrites the column's data",
				FromType: have[i].typ, ToType: c.typ, Fix: modify,
			})
		}
	}
	if len(clauses) == 0 {
		return refused, nil
	}
	if err := w.exec(ctx, "ALTER TABLE "+w.q(table)+" "+strings.Join(clauses, ", ")); err != nil {
		return nil, fmt.Errorf("clickhouse: alter %s: %w", table, err)
	}
	return refused, nil
}

// keyColumnsOf lists the columns in a table's sorting, primary or partition
// key.
func (w *writer) keyColumnsOf(ctx context.Context, table string) ([]string, error) {
	rows, err := w.conn.Query(ctx, "SELECT name FROM system.columns WHERE database = ? AND table = ? AND (is_in_sorting_key OR is_in_primary_key OR is_in_partition_key)", w.db, table)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// create makes a stream's table: a ReplacingMergeTree ordered by the key,
// versioned by _openetl_seq, for merge, and a MergeTree otherwise, ordered
// by the key if the stream has one. A deduped stream's table has a bloom
// filter index on _openetl_id, which the apply's lookups skip granules by.
func (w *writer) create(ctx context.Context, stream, table string, cols []column, key []string) error {
	cfg := w.streams[stream]
	defs := columnDefs(cols)
	var engine, order string
	var settings []string
	switch {
	case cfg.Mode == destination.Merge:
		version := ""
		if i, ok := indexOf(cols, seqColumn); ok {
			seq, _ := unwrapNullable(cols[i].typ)
			switch m := decimalSpec.FindStringSubmatch(seq); {
			case versionType(seq):
				version = ident(seqColumn)
			case m != nil && m[2] == "0":
				// A version can't be a Decimal, as Postgres's CDC's seq is.
				defs = append(defs, ident(versionColumn)+" Int256 MATERIALIZED toInt256("+ident(seqColumn)+")")
				version = ident(versionColumn)
			}
		}
		if version != "" {
			version += ", " + ident(isDeletedColumn)
			defs = append(defs, ident(isDeletedColumn)+" UInt8")
		}
		engine, order = "ReplacingMergeTree("+version+")", "("+quoteAll(key)+")"
	default:
		engine, order = "MergeTree", "tuple()"
		if k := cfg.PrimaryKey; len(k) > 0 && !slices.ContainsFunc(k, func(c string) bool { _, ok := indexOf(cols, c); return !ok }) {
			order = "(" + quoteAll(k) + ")"
			settings = append(settings, "allow_nullable_key = 1")
		}
	}
	if cfg.Dedupe != destination.DedupeNone && slices.ContainsFunc(cols, func(c column) bool { return c.name == idColumn }) {
		defs = append(defs, "INDEX _openetl_id_bloom "+ident(idColumn)+" TYPE bloom_filter GRANULARITY 1")
	}
	q := "CREATE TABLE IF NOT EXISTS " + w.q(table) + " (" + strings.Join(defs, ", ") + ") ENGINE = " + engine + " ORDER BY " + order
	if len(settings) > 0 {
		q += " SETTINGS " + strings.Join(settings, ", ")
	}
	if err := w.exec(ctx, q); err != nil {
		return fmt.Errorf("clickhouse: create %s: %w", table, err)
	}
	return nil
}

// versionType reports whether a ReplacingMergeTree can be versioned by a
// column of type typ.
func versionType(typ string) bool {
	return intType.MatchString(typ) || typ == "Date" || typ == "Date32" || strings.HasPrefix(typ, "DateTime")
}

func (w *writer) ensureDatabase(ctx context.Context) error {
	ok, err := w.databaseExists(ctx)
	if err != nil || ok {
		return err
	}
	if err := w.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(w.db)); err != nil {
		return fmt.Errorf("clickhouse: create database %s: %w", w.db, err)
	}
	return nil
}

func (w *writer) databaseExists(ctx context.Context) (bool, error) {
	var n uint64
	if err := w.conn.QueryRow(ctx, "SELECT count() FROM system.databases WHERE name = ?", w.db).Scan(&n); err != nil {
		return false, classify(fmt.Errorf("clickhouse: database %s: %w", w.db, err))
	}
	return n > 0, nil
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	if err := w.ensureDatabase(ctx); err != nil {
		return 0, err
	}
	for _, q := range []string{
		"CREATE TABLE IF NOT EXISTS " + w.q(fenceTable) + " (pipeline String, generation UInt64, raised_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = MergeTree ORDER BY pipeline",
		"CREATE TABLE IF NOT EXISTS " + w.q(loadsTable) + ` (pipeline String, stream String, generation UInt64, epoch UInt64,
			loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = MergeTree ORDER BY (pipeline, stream, generation, epoch)`,
	} {
		if err := w.exec(ctx, q); err != nil {
			return 0, fmt.Errorf("clickhouse: prepare %s: %w", w.db, err)
		}
	}
	fence, raised, err := w.fence(ctx)
	if err != nil {
		return 0, err
	}
	if fence > gen {
		return 0, fmt.Errorf("%w: recovering as generation %d, and the fence is %d", destination.ErrFenced, gen, fence)
	}
	if !raised || fence < gen {
		if err := w.exec(ctx, "INSERT INTO "+w.q(fenceTable)+" (pipeline, generation) VALUES ("+literal(w.cfg.Pipeline)+", "+strconv.FormatUint(gen, 10)+")"); err != nil {
			return 0, fmt.Errorf("clickhouse: raise the fence: %w", err)
		}
	}
	if e.IsZero() {
		w.dropRemovedReads(ctx, gen)
		return destination.Applied, nil
	}
	desc, err := w.mergeLanes(committables)
	if err != nil {
		return 0, err
	}
	var missing []stagedStream
	needs, present := 0, 0
	for _, s := range desc.Streams {
		done, err := w.marked(ctx, desc, s.Name)
		if err != nil {
			return 0, err
		}
		if done {
			continue
		}
		missing = append(missing, s)
		ready, err := w.exists(ctx, readyName(w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch))
		if err != nil {
			return 0, err
		}
		for _, t := range s.staged() {
			needs++
			ok, err := w.exists(ctx, t)
			if err != nil {
				return 0, err
			}
			if ok || ready {
				present++
			}
		}
	}
	var r destination.Recovery
	switch {
	case len(missing) == 0:
		for _, s := range desc.Streams {
			w.drop(ctx, append(s.staged(), readyName(w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch))...)
		}
		r = destination.Applied
	case present == needs:
		for _, s := range missing {
			if _, err := w.apply(ctx, desc, s); err != nil {
				return 0, err
			}
		}
		r = destination.Redone
	case len(missing) == len(desc.Streams):
		// Nothing of it is visible, and some lane's staging is gone.
		r = destination.Lost
	default:
		return 0, fmt.Errorf("clickhouse: epoch %d-g%d is partly applied and the staging for the rest is gone", desc.Epoch, desc.Generation)
	}
	if r != destination.Lost {
		w.trimMarkers(ctx, desc.Epoch)
	}
	w.dropStale(ctx, gen)
	w.dropRemovedReads(ctx, gen)
	return r, nil
}

// trimMarkers deletes the pipeline's markers below an applied epoch, which
// no recovery checks again. Best effort, like drop.
func (w *writer) trimMarkers(ctx context.Context, applied uint64) {
	_ = w.conn.Exec(ctx, "DELETE FROM "+w.q(loadsTable)+" WHERE pipeline = ? AND epoch < ?", w.cfg.Pipeline, applied)
}

// labelled returns the database's tables whose names start with prefix,
// with their labels.
func (w *writer) labelled(ctx context.Context, prefix string) (map[string]label, error) {
	rows, err := w.conn.Query(ctx, "SELECT name, comment FROM system.tables WHERE database = ? AND startsWith(name, ?)", w.db, prefix)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]label{}
	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			return nil, err
		}
		var l label
		if json.Unmarshal([]byte(comment), &l) == nil && l.Pipeline == w.cfg.Pipeline {
			out[name] = l
		}
	}
	return out, rows.Err()
}

// dropStale drops what earlier generations of the pipeline staged and
// built. None of it can be applied once the epoch Recover found is: the
// fence stops their commits. Best effort, like drop.
func (w *writer) dropStale(ctx context.Context, gen uint64) {
	for _, prefix := range []string{"_openetl_stage_", "_openetl_ready_"} {
		tables, err := w.labelled(ctx, prefix)
		if err != nil {
			return
		}
		for name, l := range tables {
			if l.Generation < gen {
				w.drop(ctx, name)
			}
		}
	}
}

// dropAbandonedReads drops the pipeline's read tables that drop says were
// abandoned.
func (w *writer) dropAbandonedReads(ctx context.Context, drop func(l label, table string) bool) error {
	tables, err := w.labelled(ctx, "_openetl_read_")
	if err != nil {
		return fmt.Errorf("clickhouse: find abandoned reads: %w", err)
	}
	for name, l := range tables {
		if drop(l, name) {
			if err := w.exec(ctx, "DROP TABLE IF EXISTS "+w.q(name)+" SYNC"); err != nil {
				return fmt.Errorf("clickhouse: drop an abandoned read of %s: %w", l.Stream, err)
			}
		}
	}
	return nil
}

// dropRemovedReads drops the read tables of streams that no longer
// overwrite, once Recover has raised the fence to gen: none a newer
// generation made, and none once a newer generation has raised the fence,
// as it may overwrite such a stream and be writing its read. With no
// transaction to hold the fence in, it reads it again before each drop.
// Best effort.
func (w *writer) dropRemovedReads(ctx context.Context, gen uint64) {
	_ = w.dropAbandonedReads(ctx, func(l label, _ string) bool {
		return l.Generation <= gen && w.streams[l.Stream].Mode != destination.Overwrite && w.fenceAt(ctx, gen) == nil
	})
}

func (w *writer) Close(context.Context) error {
	for _, tables := range []map[uint64]map[string]*laneStaging{w.staging, w.patches} {
		for _, byStream := range tables {
			for _, ls := range byStream {
				ls.release()
			}
		}
	}
	return w.conn.Close()
}
