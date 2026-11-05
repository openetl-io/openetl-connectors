package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ice "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// heldFromList makes a batch's _openetl_held, each row's list of held
// columns, into the table's map: each column to the row's _openetl_id.
func heldFromList(list, ids arrow.Array, dt arrow.DataType) (arrow.Array, error) {
	mt, ok := dt.(*arrow.MapType)
	if !ok {
		return nil, fmt.Errorf("%s is %s, not a map", heldColumn, dt)
	}
	rows := make([]map[string][]byte, list.Len())
	for r := range rows {
		cols := listAt(list, r)
		if len(cols) == 0 {
			continue
		}
		if ids == nil || ids.IsNull(r) {
			return nil, fmt.Errorf("a row holds %v, and has no %s", cols, idColumn)
		}
		rows[r] = map[string][]byte{}
		for _, c := range cols {
			rows[r][c] = bytesAt(ids, r)
		}
	}
	return heldArray(mt, rows)
}

// heldAt is a row's held columns, each to the _openetl_id of the version
// whose value is held; nil for none.
func heldAt(arr arrow.Array, r int) map[string][]byte {
	m, ok := arr.(*array.Map)
	if !ok || m.IsNull(r) {
		return nil
	}
	keys, items := m.Keys(), m.Items()
	start, end := m.ValueOffsets(r)
	out := make(map[string][]byte, end-start)
	for i := int(start); i < int(end); i++ {
		if !items.IsNull(i) {
			out[keys.ValueStr(i)] = bytesAt(items, i)
		}
	}
	return out
}

// heldArray builds a held column: null for a row that holds nothing.
func heldArray(dt *arrow.MapType, rows []map[string][]byte) (arrow.Array, error) {
	b := array.NewMapBuilderWithType(memory.DefaultAllocator, dt)
	defer b.Release()
	keys, ok := b.KeyBuilder().(*array.StringBuilder)
	if !ok {
		return nil, fmt.Errorf("%s's keys are %s, not strings", heldColumn, dt.KeyType())
	}
	items, ok := b.ItemBuilder().(*array.BinaryBuilder)
	if !ok {
		return nil, fmt.Errorf("%s's values are %s, not binary", heldColumn, dt.ItemType())
	}
	for _, m := range rows {
		if len(m) == 0 {
			b.AppendNull()
			continue
		}
		b.Append(true)
		for _, k := range slices.Sorted(maps.Keys(m)) {
			keys.Append(k)
			items.Append(m[k])
		}
	}
	return b.NewArray(), nil
}

func bytesAt(arr arrow.Array, r int) []byte {
	if e, ok := arr.(array.ExtensionArray); ok {
		arr = e.Storage()
	}
	switch c := arr.(type) {
	case *array.FixedSizeBinary:
		return slices.Clone(c.Value(r))
	case *array.Binary:
		return slices.Clone(c.Value(r))
	}
	return []byte(arr.ValueStr(r))
}

// patcher applies an epoch's patches to rows of the table, in the table's
// schema. A patch fills a held column of the row its key, or its
// _openetl_id, names, where the row still holds the version the patch's
// _openetl_id names, and the column stops being held.
type patcher struct {
	sc  *arrow.Schema
	rec arrow.RecordBatch
	// Patches by what they match on.
	by    map[string][]int
	match func(arrow.RecordBatch, int) string
	// The column filling each held column, by name.
	fills map[string]string
}

func newPatcher(sc *arrow.Schema, rec arrow.RecordBatch, fills map[string]string, match func(arrow.RecordBatch, int) string) *patcher {
	p := &patcher{sc: sc, rec: rec, by: map[string][]int{}, match: match, fills: map[string]string{}}
	for col, held := range fills {
		p.fills[held] = col
	}
	for r := range int(rec.NumRows()) {
		k := match(rec, r)
		p.by[k] = append(p.by[k], r)
	}
	return p
}

// matches lists what the patches match on, each once.
func (p *patcher) matches() []int {
	var rows []int
	for _, rs := range p.by {
		rows = append(rows, rs[0])
	}
	slices.Sort(rows)
	return rows
}

// apply returns base with its patches applied, and which rows they
// changed.
func (p *patcher) apply(ctx context.Context, base arrow.RecordBatch) (arrow.RecordBatch, []bool, error) {
	n := int(base.NumRows())
	held := columnIndex(p.sc, heldColumn)
	synced := columnIndex(p.sc, syncedColumn)
	changed := make([]bool, n)
	idx := make([][]int, p.sc.NumFields())
	kept := make([]map[string][]byte, n)
	for r := range n {
		for j := range idx {
			idx[j] = append(idx[j], r)
		}
		if held < 0 {
			continue
		}
		h := heldAt(base.Column(held), r)
		for _, q := range p.by[p.match(base, r)] {
			for name, version := range heldAt(p.rec.Column(held), q) {
				if v, ok := h[name]; !ok || !bytes.Equal(v, version) {
					continue
				}
				col := name
				if c, ok := p.fills[name]; ok {
					col = c
				}
				j := columnIndex(p.sc, col)
				if j < 0 {
					continue
				}
				idx[j][r] = n + q
				if synced >= 0 {
					idx[synced][r] = n + q
				}
				delete(h, name)
				changed[r] = true
			}
		}
		kept[r] = h
	}
	all, err := concat(p.sc, []arrow.RecordBatch{base, p.rec})
	if err != nil {
		return nil, nil, err
	}
	defer all.Release()
	cols := make([]arrow.Array, p.sc.NumFields())
	defer releaseAll(cols)
	for j := range cols {
		if j == held {
			cols[j], err = heldArray(p.sc.Field(j).Type.(*arrow.MapType), kept)
		} else {
			cols[j], err = take(ctx, all.Column(j), idx[j])
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return array.NewRecordBatch(p.sc, cols, int64(n)), changed, nil
}

// rewrite returns the rows of rec whose match is in changed.
func rewrite(ctx context.Context, rec arrow.RecordBatch, match func(arrow.RecordBatch, int) string, changed map[string]bool) (arrow.RecordBatch, error) {
	var rows []int
	for r := range int(rec.NumRows()) {
		if changed[match(rec, r)] {
			rows = append(rows, r)
		}
	}
	cols := make([]arrow.Array, rec.NumCols())
	defer releaseAll(cols)
	for j := range cols {
		var err error
		if cols[j], err = take(ctx, rec.Column(j), rows); err != nil {
			return nil, err
		}
	}
	return array.NewRecordBatch(rec.Schema(), cols, int64(len(rows))), nil
}

// patchTarget reads the table's rows the patches match that rows doesn't
// already have, patches them, and returns those the patches changed, all
// the table's rows of what they match. skip is what no row may be read
// for, such as keys the epoch deletes.
func patchTarget(ctx context.Context, tbl *table.Table, sc *arrow.Schema, p *patcher, filter func([]int) ice.BooleanExpression, have, skip map[string]bool) (arrow.RecordBatch, error) {
	var want []int
	for _, r := range p.matches() {
		if k := p.match(p.rec, r); !have[k] && !skip[k] {
			want = append(want, r)
		}
	}
	if len(want) == 0 || tbl.CurrentSnapshot() == nil {
		return nil, nil
	}
	f := filter(want)
	if _, err := ice.BindExpr(tbl.Schema(), f, true); err != nil {
		f = ice.AlwaysTrue{}
	}
	_, it, err := tbl.Scan(table.WithRowFilter(f)).ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}
	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	for b, err := range it {
		if err != nil {
			return nil, err
		}
		c, err := conform(ctx, b, sc)
		b.Release()
		if err != nil {
			return nil, err
		}
		batches = append(batches, c)
	}
	target, err := concat(sc, batches)
	if err != nil {
		return nil, err
	}
	defer target.Release()
	wanted := map[string]bool{}
	for _, r := range want {
		wanted[p.match(p.rec, r)] = true
	}
	patched, changed, err := p.apply(ctx, target)
	if err != nil {
		return nil, err
	}
	defer patched.Release()
	hit := map[string]bool{}
	for r, c := range changed {
		if k := p.match(patched, r); c && wanted[k] {
			hit[k] = true
		}
	}
	return rewrite(ctx, patched, p.match, hit)
}

// readPatches reads an epoch's patches back in sc.
func readPatches(ctx context.Context, fs iceio.IO, paths []string, sc *arrow.Schema) (arrow.RecordBatch, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	rec, err := readFiles(ctx, fs, paths, sc)
	if err != nil {
		return nil, fmt.Errorf("its patches: %w", err)
	}
	return rec, nil
}

// appendPatched stages an append epoch with patches in tx as one row
// delta: the staged files, and the rows the patches change rewritten, with
// equality deletes of their _openetl_id. It returns the files it wrote.
func (w *writer) appendPatched(ctx context.Context, tx *table.Transaction, tbl *table.Table, s stagedStream, files []ice.DataFile, marker ice.Properties) ([]string, error) {
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		return nil, err
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	ps, err := readPatches(ctx, fs, s.Patches, sc)
	if err != nil {
		return nil, err
	}
	defer ps.Release()
	id := columnIndex(sc, idColumn)
	if id < 0 {
		return nil, fmt.Errorf("its table has no %s", idColumn)
	}
	match := func(rec arrow.RecordBatch, r int) string { return string(bytesAt(rec.Column(id), r)) }
	p := newPatcher(sc, ps, s.Fills, match)
	filter := func(rows []int) ice.BooleanExpression { return keyFilter(sc, ps, []int{id}, rows) }
	rows, err := patchTarget(ctx, tbl, sc, p, filter, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("patch its rows: %w", err)
	}
	if rows == nil || rows.NumRows() == 0 {
		if rows != nil {
			rows.Release()
		}
		if len(files) == 0 {
			return nil, nil
		}
		return nil, tx.AddDataFiles(ctx, files, marker, table.WithoutAutoNameMapping())
	}
	dels := keysOf(rows, []int{id})
	defer dels.Release()
	// WriteRecords releases rows.
	written, data, err := writeData(ctx, tbl, sc, rows)
	if err != nil {
		return written, err
	}
	f, _ := tbl.Schema().FindFieldByName(idColumn)
	deletes, err := tx.WriteEqualityDeletes(ctx, []int{f.ID}, one(dels))
	if err != nil {
		return written, err
	}
	for _, df := range deletes {
		written = append(written, df.FilePath())
	}
	return written, tx.NewRowDelta(marker).AddRows(append(files, data...)...).AddDeletes(deletes...).Commit(ctx)
}

// writeData writes rows into new data files of tbl.
func writeData(ctx context.Context, tbl *table.Table, sc *arrow.Schema, rows arrow.RecordBatch) ([]string, []ice.DataFile, error) {
	var written []string
	var data []ice.DataFile
	for df, err := range table.WriteRecords(ctx, tbl, sc, one(rows), table.WithWriteUUID(uuid.New())) {
		if err != nil {
			return written, data, err
		}
		data = append(data, df)
		written = append(written, df.FilePath())
	}
	return written, data, nil
}

// keysOf is rec's key columns.
func keysOf(rec arrow.RecordBatch, key []int) arrow.RecordBatch {
	fields := make([]arrow.Field, len(key))
	cols := make([]arrow.Array, len(key))
	for i, j := range key {
		fields[i] = rec.Schema().Field(j)
		cols[i] = rec.Column(j)
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
}
