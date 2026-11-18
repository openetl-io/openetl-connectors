package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ice "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The staged descriptor's dedupe: which staged rows a commit skips by
// _openetl_id.
const (
	dedupeTarget = "target"
	dedupeEpoch  = "epoch"
)

func dedupeOf(d destination.Dedupe) string {
	switch d {
	case destination.DedupeTarget:
		return dedupeTarget
	case destination.DedupeEpoch:
		return dedupeEpoch
	}
	return ""
}

// deduped is what dedupe made of a stream's staged files.
type deduped struct {
	// The files to add: the staged files it kept, and those it wrote.
	paths []string
	// The files it wrote, which no snapshot holds unless the commit lands,
	// and the staged files they replace, which none does once it has.
	written, replaced []string
	// The rows it left out.
	dropped uint64
}

// dedupe leaves out of the staged files at paths each row whose _openetl_id
// an earlier row has, in staging order, and with target each one tbl has.
// A file that loses none stays as it is; the others are written again
// without those rows. It holds the files' IDs in memory, and with target
// reads the table's whole _openetl_id column: Iceberg has no index to look
// IDs up in.
func dedupe(ctx context.Context, tbl *table.Table, fs iceio.IO, paths []string, target bool) (deduped, error) {
	out := deduped{}
	ids := make([][]*string, len(paths))
	for i, p := range paths {
		col, err := readIDs(ctx, fs, p)
		if err != nil {
			return out, fmt.Errorf("staged file %s: %w", p, err)
		}
		ids[i] = col
	}
	seen := map[string]bool{}
	if target && tbl.CurrentSnapshot() != nil {
		staged := map[string]bool{}
		for _, col := range ids {
			for _, id := range col {
				if id != nil {
					staged[*id] = true
				}
			}
		}
		if err := targetIDs(ctx, tbl, staged, seen); err != nil {
			return out, fmt.Errorf("read the table's %s: %w", idColumn, err)
		}
	}
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		return out, err
	}
	for i, p := range paths {
		keep := make([]bool, len(ids[i]))
		lost := 0
		for r, id := range ids[i] {
			switch {
			case id == nil:
				keep[r] = true
			case seen[*id]:
				lost++
			default:
				seen[*id], keep[r] = true, true
			}
		}
		if lost == 0 {
			out.paths = append(out.paths, p)
			continue
		}
		out.dropped += uint64(lost)
		out.replaced = append(out.replaced, p)
		if lost == len(keep) {
			continue
		}
		written, err := rewriteKept(ctx, tbl, fs, p, sc, keep)
		out.written = append(out.written, written...)
		out.paths = append(out.paths, written...)
		if err != nil {
			return out, fmt.Errorf("staged file %s: %w", p, err)
		}
	}
	return out, nil
}

// readIDs is a staged file's _openetl_id column; nil for a null.
func readIDs(ctx context.Context, fs iceio.IO, path string) ([]*string, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	pf, err := file.NewParquetReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	defer func() { _ = pf.Close() }()
	col := pf.MetaData().Schema.ColumnIndexByName(idColumn)
	if col < 0 {
		return nil, fmt.Errorf("it has no %s, which dedupe needs", idColumn)
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 64 << 10}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	rr, err := fr.GetRecordReader(ctx, []int{col}, nil)
	if err != nil {
		return nil, err
	}
	defer rr.Release()
	var out []*string
	for rr.Next() {
		c := rr.RecordBatch().Column(0)
		for r := range c.Len() {
			if c.IsNull(r) {
				out = append(out, nil)
				continue
			}
			s := string(bytesAt(c, r))
			out = append(out, &s)
		}
	}
	return out, rr.Err()
}

// targetIDs adds to seen each of staged's IDs that tbl has.
func targetIDs(ctx context.Context, tbl *table.Table, staged, seen map[string]bool) error {
	if _, ok := tbl.Schema().FindFieldByName(idColumn); !ok {
		return nil
	}
	opts := []table.ScanOption{table.WithSelectedFields(idColumn)}
	if len(staged) <= maxInValues {
		f, _ := tbl.Schema().FindFieldByName(idColumn)
		lits := make([]ice.Literal, 0, len(staged))
		for id := range staged {
			switch f.Type.(type) {
			case ice.StringType:
				lits = append(lits, ice.NewLiteral(id))
			case ice.UUIDType:
				u, err := uuid.FromBytes([]byte(id))
				if err != nil {
					return err
				}
				lits = append(lits, ice.NewLiteral(u))
			default:
				lits = append(lits, ice.NewLiteral([]byte(id)))
			}
		}
		if len(lits) > 0 {
			filter := ice.SetPredicate(ice.OpIn, ice.Reference(idColumn), lits)
			if _, err := ice.BindExpr(tbl.Schema(), filter, true); err == nil {
				opts = append(opts, table.WithRowFilter(filter))
			}
		}
	}
	_, it, err := tbl.Scan(opts...).ToArrowRecords(ctx)
	if err != nil {
		return err
	}
	for b, err := range it {
		if err != nil {
			return err
		}
		c := b.Column(0)
		for r := range c.Len() {
			if id := string(bytesAt(c, r)); !c.IsNull(r) && staged[id] {
				seen[id] = true
			}
		}
		b.Release()
	}
	return nil
}

// rewriteKept writes the rows of the staged file at path that keep marks,
// or every row for nil, into new data files of tbl, and returns them.
func rewriteKept(ctx context.Context, tbl *table.Table, fs iceio.IO, path string, sc *arrow.Schema, keep []bool) ([]string, error) {
	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	if err := readFile(ctx, fs, path, sc, &batches); err != nil {
		return nil, err
	}
	rows, err := concat(sc, batches)
	if err != nil {
		return nil, err
	}
	defer rows.Release()
	if keep == nil {
		keep = make([]bool, rows.NumRows())
		for i := range keep {
			keep[i] = true
		}
	}
	if int(rows.NumRows()) != len(keep) {
		return nil, fmt.Errorf("it has %d rows, and %d IDs", rows.NumRows(), len(keep))
	}
	mb := array.NewBooleanBuilder(memory.DefaultAllocator)
	mb.AppendValues(keep, nil)
	mask := mb.NewArray()
	mb.Release()
	defer mask.Release()
	kept, err := compute.FilterRecordBatch(ctx, rows, mask, compute.DefaultFilterOptions())
	if err != nil {
		return nil, err
	}
	var written []string
	for df, err := range table.WriteRecords(ctx, tbl, sc, one(kept), table.WithWriteUUID(uuid.New())) {
		if err != nil {
			return written, err
		}
		written = append(written, df.FilePath())
	}
	return written, nil
}
