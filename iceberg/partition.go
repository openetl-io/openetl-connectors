package iceberg

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ice "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"
)

// partitionColumns are the names of the columns tbl's spec takes its
// values from.
func partitionColumns(tbl *table.Table) []string {
	var out []string
	spec := tbl.Spec()
	for _, f := range spec.Fields() {
		for _, id := range f.SourceIDs {
			if name, ok := tbl.Schema().FindColumnName(id); ok && !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out
}

// olderSpecs fails a partitioned table that holds data files of another
// partition spec: a delete written in the current one's partitions doesn't
// apply to them, so a merge or a patch would leave their rows in place.
func olderSpecs(ctx context.Context, tbl *table.Table) error {
	spec, snap := tbl.Spec(), tbl.CurrentSnapshot()
	if spec.IsUnpartitioned() || snap == nil {
		return nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return err
	}
	for _, m := range manifests {
		if m.ManifestContent() == ice.ManifestContentData && int(m.PartitionSpecID()) != spec.ID() {
			return configInvalid(fmt.Errorf("iceberg: %s holds files of an earlier partitioning, whose rows its deletes, partitioned by %s, can't reach: rewrite them into the current partitioning, as Spark's rewrite_data_files procedure does",
				strings.Join(tbl.Identifier(), "."), strings.Join(partitionColumns(tbl), ", ")))
		}
	}
	return nil
}

// fileDesc is a staged data file of a table, written with the table's
// partition spec: one partition's rows.
type fileDesc struct {
	// Its statistics, as an unpartitioned file's.
	stats ice.DataFile
	// Its partition, and the Avro logical types of those values that have
	// one.
	values  map[int]any
	logical map[int]string
}

// describeFile reads a staged file's footer, and its partition from its
// rows; errMixed if they aren't of one partition. iceberg-go infers a file's
// partition from its column statistics, which fails for a transform that
// doesn't keep order, such as bucket, and can give a truncated bound for a
// long string.
func describeFile(ctx context.Context, fs iceio.IO, path string, tbl *table.Table) (fileDesc, error) {
	// An unpartitioned table's own spec, whose ID needn't be 0.
	spec := tbl.Spec()
	if !spec.IsUnpartitioned() {
		spec = *ice.UnpartitionedSpec
	}
	stats, err := table.FileToDataFile(ctx, fs, path, tbl.Schema(), spec, 0, tbl.Properties())
	if err != nil {
		return fileDesc{}, err
	}
	d := fileDesc{stats: stats}
	if spec = tbl.Spec(); !spec.IsUnpartitioned() {
		if d.values, d.logical, err = partitionOf(ctx, fs, path, tbl); err != nil {
			return fileDesc{}, fmt.Errorf("the partition of %s: %w", path, err)
		}
	}
	return d, nil
}

// dataFile is the file as tbl's, its rows with IDs from firstRowID on; nil
// for none, as a format 3 table's commit gives those it adds without.
func (d fileDesc) dataFile(tbl *table.Table, firstRowID *int64) (ice.DataFile, error) {
	spec := tbl.Spec()
	if spec.IsUnpartitioned() && firstRowID == nil {
		return d.stats, nil
	}
	b, err := ice.NewDataFileBuilder(spec, ice.EntryContentData, d.stats.FilePath(), ice.ParquetFile, d.values, d.logical, nil, d.stats.Count(), d.stats.FileSizeBytes())
	if err != nil {
		return nil, err
	}
	b.ColumnSizes(d.stats.ColumnSizes()).ValueCounts(d.stats.ValueCounts()).NullValueCounts(d.stats.NullValueCounts()).
		NaNValueCounts(d.stats.NaNValueCounts()).LowerBoundValues(d.stats.LowerBoundValues()).UpperBoundValues(d.stats.UpperBoundValues()).
		SplitOffsets(d.stats.SplitOffsets())
	if firstRowID != nil {
		b.FirstRowID(*firstRowID)
	}
	return b.Build(), nil
}

// dataFiles describes staged files of tbl, parallelism at a time, as
// AddDataFiles and ReplaceDataFilesWithDataFiles take them: on a format 3
// table, with row IDs from the table's next one on. A file of more than one
// partition, written before the table's partitioning changed, is written
// again by partition into the files it adds to res.written, and to
// res.replaced itself.
func dataFiles(ctx context.Context, fs iceio.IO, tbl *table.Table, paths []string, rowIDs bool, res *staging) ([]ice.DataFile, error) {
	var mixed []string
	for {
		descs := make([]fileDesc, len(paths))
		var mu sync.Mutex
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(parallelism)
		for i, p := range paths {
			g.Go(func() error {
				d, err := describeFile(gctx, fs, p, tbl)
				if errors.Is(err, errMixed) {
					mu.Lock()
					mixed = append(mixed, p)
					mu.Unlock()
					return nil
				}
				if err != nil {
					return fmt.Errorf("staged file %s: %w", p, err)
				}
				descs[i] = d
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		if len(mixed) == 0 {
			return describedFiles(tbl, descs, rowIDs)
		}
		sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
		if err != nil {
			return nil, err
		}
		keep := slices.DeleteFunc(slices.Clone(paths), func(p string) bool { return slices.Contains(mixed, p) })
		for _, p := range mixed {
			written, err := rewriteKept(ctx, tbl, fs, p, sc, nil)
			res.written = append(res.written, written...)
			if err != nil {
				return nil, fmt.Errorf("staged file %s: %w", p, err)
			}
			res.replaced = append(res.replaced, p)
			keep = append(keep, written...)
		}
		paths, mixed = keep, nil
	}
}

func describedFiles(tbl *table.Table, descs []fileDesc, rowIDs bool) ([]ice.DataFile, error) {
	out := make([]ice.DataFile, len(descs))
	next := tbl.Metadata().NextRowID()
	for i, d := range descs {
		var first *int64
		if rowIDs && tbl.Metadata().Version() >= 3 {
			first = new(next)
			next += d.stats.Count()
		}
		df, err := d.dataFile(tbl, first)
		if err != nil {
			return nil, err
		}
		out[i] = df
	}
	return out, nil
}

// errMixed: a file's rows are of more than one partition of the table's
// spec, which changed after the file was written.
var errMixed = errors.New("rows of more than one partition")

// partitionOf is the partition of a file of tbl, from its rows, each
// partition field's value, and the Avro logical type of those that have
// one, as iceberg-go's writer gives them; errMixed if its rows aren't all of
// one partition.
func partitionOf(ctx context.Context, fs iceio.IO, path string, tbl *table.Table) (map[int]any, map[int]string, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = f.Close() }()
	pr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = pr.Close() }()
	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{BatchSize: 64 << 10}, memory.DefaultAllocator)
	if err != nil {
		return nil, nil, err
	}
	sc := tbl.Schema()
	values, logical := map[int]any{}, map[int]string{}
	spec := tbl.Spec()
	for _, pf := range spec.Fields() {
		src, ok := sc.FindFieldByID(pf.SourceID())
		if !ok {
			return nil, nil, fmt.Errorf("the table has no column %d, which partition field %s takes", pf.SourceID(), pf.Name)
		}
		name, _ := sc.FindColumnName(pf.SourceID())
		col := pr.MetaData().Schema.ColumnIndexByName(name)
		if col < 0 {
			return nil, nil, fmt.Errorf("the file has no column %q", name)
		}
		value := func(arr arrow.Array, r int) (any, error) {
			var v ice.Optional[ice.Literal]
			if !arr.IsNull(r) {
				lit, ok := literalAt(arr, r)
				if !ok {
					return nil, fmt.Errorf("can't partition by %q, a %s", name, src.Type)
				}
				v = ice.Optional[ice.Literal]{Valid: true, Val: lit}
			}
			if out := pf.Transform.Apply(v); out.Valid {
				return out.Val.Any(), nil
			}
			return nil, nil
		}
		rr, err := fr.GetRecordReader(ctx, []int{col}, nil)
		if err != nil {
			return nil, nil, err
		}
		first := true
		for rr.Next() {
			arr := rr.RecordBatch().Column(0)
			for r := range arr.Len() {
				v, err := value(arr, r)
				if err != nil {
					rr.Release()
					return nil, nil, err
				}
				if first {
					values[pf.FieldID], first = v, false
				} else if !reflect.DeepEqual(v, values[pf.FieldID]) {
					rr.Release()
					return nil, nil, errMixed
				}
			}
		}
		err = rr.Err()
		rr.Release()
		if err != nil {
			return nil, nil, err
		}
		switch pf.Transform.ResultType(src.Type).(type) {
		case ice.DateType:
			logical[pf.FieldID] = "date"
		case ice.TimeType:
			logical[pf.FieldID] = "time-micros"
		case ice.TimestampType, ice.TimestampTzType:
			logical[pf.FieldID] = "timestamp-micros"
		case ice.DecimalType:
			logical[pf.FieldID] = "decimal"
		case ice.UUIDType:
			logical[pf.FieldID] = "uuid"
		}
	}
	return values, logical, nil
}
