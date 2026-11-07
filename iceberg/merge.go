package iceberg

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ice "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

const (
	idColumn        = "_openetl_id"
	opColumn        = "_openetl_op"
	seqColumn       = "_openetl_seq"
	deletedColumn   = "_openetl_deleted"
	unchangedColumn = "_openetl_unchanged"
	syncedColumn    = "_openetl_synced_at"
	heldColumn      = "_openetl_held"
	// The engine names renamed columns' source names in each schema's
	// metadata: a JSON object from destination name to source name.
	sourceNamesKey = "openetl.source_names"
	// A filter on a key column lists at most this many values; beyond, it
	// bounds the column by their range.
	maxInValues = 1000
)

// A soft delete takes these from the delete, and the rest from the row.
var metadataColumns = []string{idColumn, opColumn, seqColumn, syncedColumn, deletedColumn}

// keyColumns maps a stream's primary key, under source names, to its
// destination names.
func keyColumns(sc *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := sc.Metadata().GetValue(sourceNamesKey); ok {
		var names map[string]string
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, fmt.Errorf("its %s metadata: %w", sourceNamesKey, err)
		}
		for dst, src := range names {
			renamed[src] = dst
		}
	}
	out := make([]string, len(key))
	for i, k := range key {
		out[i] = cmp.Or(renamed[k], k)
		idx := sc.FieldIndices(out[i])
		if len(idx) == 0 {
			return nil, fmt.Errorf("it merges on %q, which its batches don't have", k)
		}
		switch sc.Field(idx[0]).Type.ID() {
		case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
			return nil, fmt.Errorf("it merges on %q, a floating-point column, which Iceberg's equality deletes can't match on", k)
		}
	}
	return out, nil
}

// mergeInto stages the stream's epoch in tx as one row delta: equality
// deletes of the keys it changes, and their new rows. It returns the files
// it wrote, which no snapshot holds if tx doesn't commit.
func (w *writer) mergeInto(ctx context.Context, tx *table.Transaction, tbl *table.Table, s stagedStream, marker ice.Properties) ([]string, error) {
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		return nil, err
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	staged, err := readFiles(ctx, fs, s.Files, sc)
	if err != nil {
		return nil, err
	}
	m, err := newMerge(sc, staged, s.Key, s.HardDeletes, s.Last)
	staged.Release()
	if err != nil {
		return nil, err
	}
	defer func() { m.rec.Release() }()
	if err := m.readTarget(ctx, tbl); err != nil {
		return nil, fmt.Errorf("read the rows it merges into: %w", err)
	}
	rows, dels, err := m.result(ctx)
	if err != nil {
		return nil, err
	}
	if len(s.Patches) > 0 {
		if rows, dels, err = m.patch(ctx, tbl, fs, s, rows, dels); err != nil {
			return nil, fmt.Errorf("patch its rows: %w", err)
		}
	}
	defer rows.Release()
	defer dels.Release()

	var written []string
	var data, deletes []ice.DataFile
	if rows.NumRows() > 0 {
		if written, data, err = writeData(ctx, tbl, sc, rows); err != nil {
			return written, err
		}
	}
	if dels.NumRows() > 0 {
		ids := make([]int, len(s.Key))
		for i, k := range s.Key {
			f, ok := tbl.Schema().FindFieldByName(k)
			if !ok {
				return written, fmt.Errorf("its table has no key column %q", k)
			}
			ids[i] = f.ID
		}
		if deletes, err = tx.WriteEqualityDeletes(ctx, ids, one(dels)); err != nil {
			return written, err
		}
		for _, df := range deletes {
			written = append(written, df.FilePath())
		}
	}
	if len(data)+len(deletes) == 0 {
		return written, nil
	}
	return written, tx.NewRowDelta(marker).AddRows(data...).AddDeletes(deletes...).Commit(ctx)
}

// patch applies the epoch's patches after the merge: to the rows it adds,
// and to the table's rows of the other keys they name, which it rewrites.
// It takes rows and dels, and returns them with the patches applied.
func (m *merge) patch(ctx context.Context, tbl *table.Table, fs iceio.IO, s stagedStream, rows, dels arrow.RecordBatch) (arrow.RecordBatch, arrow.RecordBatch, error) {
	release := func() { rows.Release(); dels.Release() }
	ps, err := readPatches(ctx, fs, s.Patches, m.sc)
	if err != nil {
		release()
		return nil, nil, err
	}
	defer ps.Release()
	match := func(rec arrow.RecordBatch, r int) string { return keyOf(rec, m.key, r) }
	p := newPatcher(m.sc, ps, s.Fills, match)
	patched, _, err := p.apply(ctx, rows)
	if err != nil {
		release()
		return nil, nil, err
	}
	rows.Release()
	rows = patched
	have, skip := map[string]bool{}, map[string]bool{}
	for r := range int(rows.NumRows()) {
		have[match(rows, r)] = true
	}
	keyIdx := make([]int, len(m.key))
	for i := range keyIdx {
		keyIdx[i] = i
	}
	for r := range int(dels.NumRows()) {
		skip[keyOf(dels, keyIdx, r)] = true
	}
	filter := func(rs []int) ice.BooleanExpression { return keyFilter(m.sc, ps, m.key, rs) }
	target, err := patchTarget(ctx, tbl, m.sc, p, filter, have, skip)
	if err != nil || target == nil || target.NumRows() == 0 {
		if target != nil {
			target.Release()
		}
		if err != nil {
			release()
			return nil, nil, err
		}
		return rows, dels, nil
	}
	defer target.Release()
	all, err := concat(m.sc, []arrow.RecordBatch{rows, target})
	if err != nil {
		release()
		return nil, nil, err
	}
	keys := keysOf(target, m.key)
	defer keys.Release()
	allDels, err := concat(dels.Schema(), []arrow.RecordBatch{dels, keys})
	release()
	if err != nil {
		all.Release()
		return nil, nil, err
	}
	return all, allDels, nil
}

func one(b arrow.RecordBatch) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) { yield(b, nil) }
}

// merge is one stream's epoch merged into its table. Each key takes its
// latest staged version, by _openetl_seq and then staging order, unless the
// table's is newer. A delete keeps the row's other columns as they were,
// with its metadata columns updated, or with hard deletes removes the row. A
// column the latest version didn't send takes its value from the key's
// latest staged version that did, or else keeps the table's.
type merge struct {
	sc *arrow.Schema
	// The staged rows, in staging order, then the table's rows read.
	rec    arrow.RecordBatch
	staged int
	key    []int
	// -1 where the table has no such column.
	seq, op, unchanged, held int
	// Columns whose held cells keep the table's value.
	last  []int
	hard  bool
	keys  []string
	byKey map[string]*version
	// The table had no rows to delete.
	empty bool
}

type version struct {
	// Staged rows, in staging order.
	rows   []int
	latest int
	// Columns the latest version didn't send, and the earlier staged row
	// each takes its value from.
	from map[int]int
	// The unchanged columns none of the staged versions sent.
	remaining []string
	// Whether the merge needs the table's row: for a soft delete, or for
	// remaining columns. Otherwise only if it's newer (guard).
	full, guard bool
	// The table's row, or -1.
	target int
}

func columnIndex(sc *arrow.Schema, name string) int {
	if idx := sc.FieldIndices(name); len(idx) > 0 {
		return idx[0]
	}
	return -1
}

func newMerge(sc *arrow.Schema, staged arrow.RecordBatch, key []string, hard bool, last []string) (*merge, error) {
	m := &merge{
		sc: sc, rec: staged, staged: int(staged.NumRows()), hard: hard, byKey: map[string]*version{},
		seq: columnIndex(sc, seqColumn), op: columnIndex(sc, opColumn), unchanged: columnIndex(sc, unchangedColumn),
		held: columnIndex(sc, heldColumn),
	}
	for _, c := range last {
		if j := columnIndex(sc, c); j >= 0 && m.held >= 0 {
			m.last = append(m.last, j)
		}
	}
	if len(key) == 0 {
		return nil, errors.New("it merges, and has no key")
	}
	for _, k := range key {
		j := columnIndex(sc, k)
		if j < 0 {
			return nil, fmt.Errorf("it merges on %q, which its table doesn't have", k)
		}
		m.key = append(m.key, j)
	}
	staged.Retain()
	for r := range m.staged {
		k := m.keyOf(r)
		v := m.byKey[k]
		if v == nil {
			v = &version{target: -1}
			m.byKey[k] = v
			m.keys = append(m.keys, k)
		}
		v.rows = append(v.rows, r)
	}
	for _, k := range m.keys {
		v := m.byKey[k]
		m.resolve(v)
		switch {
		case m.isDelete(v.latest) && !hard, len(v.remaining) > 0, len(m.holdsLast(v.latest)) > 0:
			v.full = true
		case m.seq >= 0 && !m.rec.Column(m.seq).IsNull(v.latest):
			v.guard = true
		}
	}
	return m, nil
}

func (m *merge) keyOf(r int) string { return keyOf(m.rec, m.key, r) }

func keyOf(rec arrow.RecordBatch, key []int, r int) string {
	var b []byte
	for _, j := range key {
		c := rec.Column(j)
		if c.IsNull(r) {
			b = append(b, 0)
			continue
		}
		v := c.ValueStr(r)
		b = strconv.AppendInt(append(b, 1), int64(len(v)), 10)
		b = append(append(b, ':'), v...)
	}
	return string(b)
}

// holdsLast is the columns marked last that row r holds.
func (m *merge) holdsLast(r int) []int {
	if len(m.last) == 0 {
		return nil
	}
	h := heldAt(m.rec.Column(m.held), r)
	var out []int
	for _, j := range m.last {
		if _, ok := h[m.sc.Field(j).Name]; ok {
			out = append(out, j)
		}
	}
	return out
}

func (m *merge) isDelete(r int) bool {
	if m.op < 0 {
		return false
	}
	c := m.rec.Column(m.op)
	return !c.IsNull(r) && c.ValueStr(r) == "delete"
}

// newer reports whether row a is a later version than row b: a higher
// _openetl_seq, a null one being the lowest, and then a later row.
func (m *merge) newer(a, b int) bool {
	if m.seq >= 0 {
		c := m.rec.Column(m.seq)
		switch an, bn := c.IsNull(a), c.IsNull(b); {
		case an && !bn:
			return false
		case !an && bn:
			return true
		case !an && !bn:
			if d := compareAt(c, a, b); d != 0 {
				return d > 0
			}
		}
	}
	return a > b
}

func (m *merge) resolve(v *version) {
	v.latest = v.rows[0]
	for _, r := range v.rows[1:] {
		if m.newer(r, v.latest) {
			v.latest = r
		}
	}
	if m.unchanged < 0 {
		return
	}
	seen := map[string]bool{}
	for _, c := range listAt(m.rec.Column(m.unchanged), v.latest) {
		if seen[c] {
			continue
		}
		seen[c] = true
		j := columnIndex(m.sc, c)
		from := -1
		if j >= 0 && !slices.Contains(m.key, j) {
			for _, r := range v.rows {
				if r == v.latest || !m.newer(v.latest, r) || slices.Contains(listAt(m.rec.Column(m.unchanged), r), c) {
					continue
				}
				if from < 0 || m.newer(r, from) {
					from = r
				}
			}
		}
		if from < 0 {
			v.remaining = append(v.remaining, c)
			continue
		}
		if v.from == nil {
			v.from = map[int]int{}
		}
		v.from[j] = from
	}
}

// readTarget reads the table's rows for the keys whose merge needs them,
// and only those that could be newer for the keys that need them only if
// they are, which spares the files older than the epoch.
func (m *merge) readTarget(ctx context.Context, tbl *table.Table) error {
	if tbl.CurrentSnapshot() == nil {
		m.empty = true
		return nil
	}
	var full, guard []int
	floor := -1
	for _, k := range m.keys {
		switch v := m.byKey[k]; {
		case v.full:
			full = append(full, v.latest)
		case v.guard:
			guard = append(guard, v.latest)
			if floor < 0 || compareAt(m.rec.Column(m.seq), v.latest, floor) < 0 {
				floor = v.latest
			}
		}
	}
	var filters []ice.BooleanExpression
	if len(full) > 0 {
		filters = append(filters, m.keyFilter(full))
	}
	if len(guard) > 0 {
		f := m.keyFilter(guard)
		if lit, ok := literalAt(m.rec.Column(m.seq), floor); ok {
			f = ice.NewAnd(f, ice.LiteralPredicate(ice.OpGT, ice.Reference(seqColumn), lit))
		}
		filters = append(filters, f)
	}
	if len(filters) == 0 {
		return nil
	}
	filter := filters[0]
	if len(filters) > 1 {
		filter = ice.NewOr(filters[0], filters[1])
	}
	// A filter Iceberg can't bind would fail the scan; reading every row
	// only costs time.
	if _, err := ice.BindExpr(tbl.Schema(), filter, true); err != nil {
		filter = ice.AlwaysTrue{}
	}
	_, it, err := tbl.Scan(table.WithRowFilter(filter)).ToArrowRecords(ctx)
	if err != nil {
		return err
	}
	batches := []arrow.RecordBatch{m.rec}
	m.rec.Retain()
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	for b, err := range it {
		if err != nil {
			return err
		}
		c, err := conform(ctx, b, m.sc)
		b.Release()
		if err != nil {
			return err
		}
		batches = append(batches, c)
	}
	if len(batches) == 1 {
		return nil
	}
	rec, err := concat(m.sc, batches)
	if err != nil {
		return err
	}
	m.rec.Release()
	m.rec = rec
	for r := m.staged; r < int(rec.NumRows()); r++ {
		v := m.byKey[m.keyOf(r)]
		if v == nil || !v.full && !v.guard {
			continue
		}
		if v.target < 0 || m.newer(r, v.target) {
			v.target = r
		}
	}
	return nil
}

func (m *merge) keyFilter(rows []int) ice.BooleanExpression {
	return keyFilter(m.sc, m.rec, m.key, rows)
}

// keyFilter matches the keys of rec's rows, and possibly others.
func keyFilter(sc *arrow.Schema, rec arrow.RecordBatch, key []int, rows []int) ice.BooleanExpression {
	var preds []ice.BooleanExpression
columns:
	for _, j := range key {
		c := rec.Column(j)
		ref := ice.Reference(sc.Field(j).Name)
		seen := map[string]bool{}
		var lits []ice.Literal
		lo, hi := -1, -1
		for _, r := range rows {
			if c.IsNull(r) {
				continue columns
			}
			if lo < 0 || compareAt(c, r, lo) < 0 {
				lo = r
			}
			if hi < 0 || compareAt(c, r, hi) > 0 {
				hi = r
			}
			if len(lits) > maxInValues || seen[c.ValueStr(r)] {
				continue
			}
			seen[c.ValueStr(r)] = true
			lit, ok := literalAt(c, r)
			if !ok {
				continue columns
			}
			lits = append(lits, lit)
		}
		if len(lits) <= maxInValues {
			preds = append(preds, ice.SetPredicate(ice.OpIn, ref, lits))
			continue
		}
		l, _ := literalAt(c, lo)
		h, _ := literalAt(c, hi)
		preds = append(preds, ice.NewAnd(ice.LiteralPredicate(ice.OpGTEQ, ref, l), ice.LiteralPredicate(ice.OpLTEQ, ref, h)))
	}
	switch len(preds) {
	case 0:
		return ice.AlwaysTrue{}
	case 1:
		return preds[0]
	}
	return ice.NewAnd(preds[0], preds[1], preds[2:]...)
}

// result returns the rows the merge adds, and the keys it deletes first.
func (m *merge) result(ctx context.Context) (rows, dels arrow.RecordBatch, err error) {
	n := m.sc.NumFields()
	meta := make([]bool, n)
	for j, f := range m.sc.Fields() {
		meta[j] = slices.Contains(metadataColumns, f.Name)
	}
	idx := make([][]int, n)
	var unchanged [][]string
	var held []map[string][]byte
	var deleted []int
	for _, k := range m.keys {
		v := m.byKey[k]
		l, t := v.latest, v.target
		if t >= 0 && m.seq >= 0 {
			c := m.rec.Column(m.seq)
			if !c.IsNull(l) && !c.IsNull(t) && compareAt(c, t, l) > 0 {
				continue
			}
		}
		// The table may hold the key unless it was read in full for it.
		if !m.empty && (!v.full || t >= 0) {
			deleted = append(deleted, l)
		}
		if m.isDelete(l) && m.hard {
			continue
		}
		var u []string
		softDelete := t >= 0 && m.isDelete(l)
		switch {
		case softDelete:
			for j := range n {
				idx[j] = append(idx[j], cond(meta[j], l, t))
			}
			u = listAt(m.unchangedColumn(), t)
		case t >= 0 && len(v.remaining) > 0:
			for j, f := range m.sc.Fields() {
				src := l
				if r, ok := v.from[j]; ok {
					src = r
				}
				idx[j] = append(idx[j], cond(slices.Contains(v.remaining, f.Name), t, src))
			}
			had := listAt(m.unchangedColumn(), t)
			u = slices.DeleteFunc(slices.Clone(v.remaining), func(c string) bool { return !slices.Contains(had, c) })
		default:
			for j := range n {
				src := l
				if r, ok := v.from[j]; ok {
					src = r
				}
				idx[j] = append(idx[j], src)
			}
			u = v.remaining
		}
		unchanged = append(unchanged, u)
		if m.held >= 0 {
			held = append(held, m.heldOf(idx, l, t, softDelete))
		}
	}

	cols := make([]arrow.Array, n)
	defer releaseAll(cols)
	for j := range n {
		switch j {
		case m.unchanged:
			cols[j], err = listOf(m.sc.Field(j).Type, unchanged)
		case m.held:
			mt, ok := m.sc.Field(j).Type.(*arrow.MapType)
			if !ok {
				return nil, nil, fmt.Errorf("its table's %s is %s, not a map", heldColumn, m.sc.Field(j).Type)
			}
			cols[j], err = heldArray(mt, held)
		default:
			cols[j], err = take(ctx, m.rec.Column(j), idx[j])
		}
		if err != nil {
			return nil, nil, err
		}
	}
	rows = array.NewRecordBatch(m.sc, cols, int64(len(unchanged)))

	keyFields := make([]arrow.Field, len(m.key))
	keyCols := make([]arrow.Array, len(m.key))
	defer releaseAll(keyCols)
	for i, j := range m.key {
		keyFields[i] = m.sc.Field(j)
		if keyCols[i], err = take(ctx, m.rec.Column(j), deleted); err != nil {
			rows.Release()
			return nil, nil, err
		}
	}
	return rows, array.NewRecordBatch(arrow.NewSchema(keyFields, nil), keyCols, int64(len(deleted))), nil
}

// heldOf is the held columns of the row just added to idx, each held for
// the version the row takes its value from. A column marked last that the
// latest version holds keeps the table's value instead, held for the latest
// version.
func (m *merge) heldOf(idx [][]int, l, t int, softDelete bool) map[string][]byte {
	i := len(idx[0]) - 1
	seen := map[int]map[string][]byte{}
	at := func(r int) map[string][]byte {
		h, ok := seen[r]
		if !ok {
			h = heldAt(m.rec.Column(m.held), r)
			seen[r] = h
		}
		return h
	}
	out := map[string][]byte{}
	for j, f := range m.sc.Fields() {
		if j == m.held || j == m.unchanged || slices.Contains(metadataColumns, f.Name) {
			continue
		}
		if v, ok := at(idx[j][i])[f.Name]; ok {
			out[f.Name] = v
		}
	}
	if softDelete || t < 0 {
		return out
	}
	for _, j := range m.holdsLast(l) {
		name := m.sc.Field(j).Name
		idx[j][i] = t
		out[name] = at(l)[name]
	}
	return out
}

func (m *merge) unchangedColumn() arrow.Array {
	if m.unchanged < 0 {
		return nil
	}
	return m.rec.Column(m.unchanged)
}

func cond(c bool, a, b int) int {
	if c {
		return a
	}
	return b
}

func releaseAll(arrs []arrow.Array) {
	for _, a := range arrs {
		if a != nil {
			a.Release()
		}
	}
}

// take returns arr's values at rows.
func take(ctx context.Context, arr arrow.Array, rows []int) (arrow.Array, error) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for _, r := range rows {
		b.Append(int64(r))
	}
	indices := b.NewInt64Array()
	defer indices.Release()
	if e, ok := arr.(array.ExtensionArray); ok {
		storage, err := compute.TakeArray(ctx, e.Storage(), indices)
		if err != nil {
			return nil, err
		}
		defer storage.Release()
		return array.NewExtensionArrayWithStorage(e.ExtensionType(), storage), nil
	}
	return compute.TakeArray(ctx, arr, indices)
}

// listAt returns the strings of a list column's row; none if it's null.
func listAt(arr arrow.Array, r int) []string {
	l, ok := arr.(*array.List)
	if !ok || l.IsNull(r) {
		return nil
	}
	vals := l.ListValues()
	start, end := l.ValueOffsets(r)
	out := make([]string, 0, end-start)
	for i := int(start); i < int(end); i++ {
		if !vals.IsNull(i) {
			out = append(out, vals.ValueStr(i))
		}
	}
	return out
}

// listOf builds a list column of strings: null for an empty list.
func listOf(dt arrow.DataType, rows [][]string) (arrow.Array, error) {
	b, ok := array.NewBuilder(memory.DefaultAllocator, dt).(*array.ListBuilder)
	if !ok {
		return nil, fmt.Errorf("%s is %s, not a list", unchangedColumn, dt)
	}
	defer b.Release()
	vals, ok := b.ValueBuilder().(*array.StringBuilder)
	if !ok {
		return nil, fmt.Errorf("%s is %s, not a list of strings", unchangedColumn, dt)
	}
	for _, r := range rows {
		if len(r) == 0 {
			b.AppendNull()
			continue
		}
		b.Append(true)
		vals.AppendValues(r, nil)
	}
	return b.NewArray(), nil
}

// compareAt orders two non-null rows of arr.
func compareAt(arr arrow.Array, a, b int) int {
	if e, ok := arr.(array.ExtensionArray); ok {
		arr = e.Storage()
	}
	switch c := arr.(type) {
	case *array.Int8:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Int16:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Int32:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Int64:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Float32:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Float64:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Date32:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Time64:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Timestamp:
		return cmp.Compare(c.Value(a), c.Value(b))
	case *array.Decimal128:
		return c.Value(a).Cmp(c.Value(b))
	case *array.String:
		return strings.Compare(c.Value(a), c.Value(b))
	case *array.Binary:
		return bytes.Compare(c.Value(a), c.Value(b))
	case *array.FixedSizeBinary:
		return bytes.Compare(c.Value(a), c.Value(b))
	case *array.Boolean:
		return cmp.Compare(cond(c.Value(a), 1, 0), cond(c.Value(b), 1, 0))
	}
	return strings.Compare(arr.ValueStr(a), arr.ValueStr(b))
}

// literalAt is a non-null row of arr as an Iceberg literal of its column's
// type.
func literalAt(arr arrow.Array, r int) (ice.Literal, bool) {
	switch c := arr.(type) {
	case *extensions.UUIDArray:
		return ice.NewLiteral(c.Value(r)), true
	case *array.Int32:
		return ice.NewLiteral(c.Value(r)), true
	case *array.Int64:
		return ice.NewLiteral(c.Value(r)), true
	case *array.Float32:
		return ice.NewLiteral(c.Value(r)), true
	case *array.Float64:
		return ice.NewLiteral(c.Value(r)), true
	case *array.Boolean:
		return ice.NewLiteral(c.Value(r)), true
	case *array.String:
		return ice.NewLiteral(c.Value(r)), true
	case *array.Binary:
		return ice.NewLiteral(slices.Clone(c.Value(r))), true
	case *array.FixedSizeBinary:
		return ice.NewLiteral(slices.Clone(c.Value(r))), true
	case *array.Date32:
		return ice.NewLiteral(ice.Date(c.Value(r))), true
	case *array.Time64:
		return ice.NewLiteral(ice.Time(c.Value(r))), true
	case *array.Timestamp:
		return ice.NewLiteral(ice.Timestamp(c.Value(r))), true
	case *array.Decimal128:
		dt, _ := c.DataType().(*arrow.Decimal128Type)
		return ice.NewLiteral(ice.Decimal{Val: c.Value(r), Scale: int(dt.Scale)}), true
	}
	return nil, false
}

// readFiles reads data files no snapshot holds into one batch in sc.
func readFiles(ctx context.Context, fs iceio.IO, paths []string, sc *arrow.Schema) (arrow.RecordBatch, error) {
	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	for _, p := range paths {
		if err := readFile(ctx, fs, p, sc, &batches); err != nil {
			return nil, fmt.Errorf("staged file %s: %w", p, err)
		}
	}
	return concat(sc, batches)
}

func readFile(ctx context.Context, fs iceio.IO, path string, sc *arrow.Schema, out *[]arrow.RecordBatch) error {
	f, err := fs.Open(path)
	if err != nil {
		return err
	}
	pf, err := file.NewParquetReader(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 64 << 10}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()
	for rr.Next() {
		b, err := conform(ctx, rr.RecordBatch(), sc)
		if err != nil {
			return err
		}
		*out = append(*out, b)
	}
	return rr.Err()
}

// concat joins batches in sc into one.
func concat(sc *arrow.Schema, batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	var n int64
	for _, b := range batches {
		n += b.NumRows()
	}
	cols := make([]arrow.Array, sc.NumFields())
	defer releaseAll(cols)
	for j, f := range sc.Fields() {
		if len(batches) == 0 {
			cols[j] = array.MakeArrayOfNull(memory.DefaultAllocator, f.Type, 0)
			continue
		}
		parts := make([]arrow.Array, len(batches))
		for i, b := range batches {
			parts[i] = b.Column(j)
		}
		c, err := array.Concatenate(parts, memory.DefaultAllocator)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", f.Name, err)
		}
		cols[j] = c
	}
	return array.NewRecordBatch(sc, cols, n), nil
}
