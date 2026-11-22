package objstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocloud.dev/blob"
	"golang.org/x/text/transform"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// A text file's batch ends at this many records, or bytes.
const (
	batchRecords = 10000
	batchBytes   = 8 << 20
	// A Parquet file's batches have at most this many rows; its resume
	// points are between row groups.
	parquetBatch = 64 << 10
)

// sink takes what a file's reader reads: batches of typed or text
// columns, or JSON objects, and positions to resume from after them, but
// for the file's end, which the caller marks.
type sink struct {
	records func(arrow.RecordBatch) error
	rows    func([]json.RawMessage) error
	resume  func(filePos) error
}

// changed is the error of a file that changed while it was read: the next
// attempt reads it again.
func changed(key string) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("objstore: %s changed while it was read", key))
}

// undeclared is the error of a column the read's request doesn't declare:
// the next attempt's discovery declares it.
func undeclared(key, column string) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		fmt.Errorf("objstore: %s has column %q, which the stream doesn't declare yet: the next attempt discovers it, unless discovery's sample of the files misses it, which the stream's columns then need to name", key, column))
}

// open opens the file from offset, and fails if it isn't the version
// listed.
func open(ctx context.Context, b *blob.Bucket, o object, offset int64) (*blob.Reader, error) {
	r, err := b.NewRangeReader(ctx, o.Key, offset, -1, nil)
	if err != nil {
		return nil, fmt.Errorf("objstore: read %s: %w", o.Key, err)
	}
	if !sameVersion(r.ModTime(), r.Size(), o) {
		_ = r.Close()
		return nil, changed(o.Key)
	}
	return r, nil
}

// sameVersion: listings give some stores' modification times finer than
// reads do.
func sameVersion(mod time.Time, size int64, o object) bool {
	return size == o.Size && mod.Truncate(time.Second).Equal(o.Modified.Truncate(time.Second))
}

// rangeReaderAt reads a file by ranges, as Parquet's reader does.
type rangeReaderAt struct {
	ctx context.Context
	b   *blob.Bucket
	o   object
	off int64
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.o.Size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.o.Size-off)
	rr, err := r.b.NewRangeReader(r.ctx, r.o.Key, off, n, nil)
	if err != nil {
		return 0, fmt.Errorf("objstore: read %s: %w", r.o.Key, err)
	}
	defer func() { _ = rr.Close() }()
	if !sameVersion(rr.ModTime(), rr.Size(), r.o) {
		return 0, changed(r.o.Key)
	}
	got, err := io.ReadFull(rr, p[:n])
	if err == nil && got < len(p) {
		err = io.EOF
	}
	return got, err
}

func (r *rangeReaderAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.off = offset
	case io.SeekCurrent:
		r.off += offset
	case io.SeekEnd:
		r.off = r.o.Size + offset
	}
	return r.off, nil
}

// control appends the columns the source adds to a batch of n records,
// the first of them the file's record first, from 1.
func control(cols []arrow.Array, location string, o object, first int64, n int) []arrow.Array {
	mem := memory.DefaultAllocator
	fb := array.NewStringBuilder(mem)
	lb := array.NewInt64Builder(mem)
	mb := array.NewTimestampBuilder(mem, utcMicros)
	defer fb.Release()
	defer lb.Release()
	defer mb.Release()
	mod := arrow.Timestamp(o.Modified.UnixMicro())
	for i := range n {
		fb.Append(location)
		lb.Append(first + int64(i))
		mb.Append(mod)
	}
	return append(cols, fb.NewArray(), lb.NewArray(), mb.NewArray())
}

// parquetSchema reads a Parquet file's schema from its footer.
func parquetSchema(ctx context.Context, b *blob.Bucket, o object) (*arrow.Schema, error) {
	pf, err := file.NewParquetReader(&rangeReaderAt{ctx: ctx, b: b, o: o})
	if err != nil {
		return nil, fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	defer func() { _ = pf.Close() }()
	_, sc, err := parquetReader(pf, pqarrow.ArrowReadProperties{})
	if err != nil {
		return nil, fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	return sc, nil
}

// parquetReader is a reader of a Parquet file and the file's schema, in
// which a column of Parquet's JSON type is binary with the JSON extension's
// metadata. The reader reads such a column's values as binary: arrow-go
// reads them as binary whatever the file's stored Arrow schema says, and
// panics where that schema makes them an extension type.
func parquetReader(pf *file.Reader, props pqarrow.ArrowReadProperties) (*pqarrow.FileReader, *arrow.Schema, error) {
	fr, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
	if err != nil {
		return nil, nil, err
	}
	fields := make([]arrow.Field, len(fr.Manifest.Fields))
	for i, sf := range fr.Manifest.Fields {
		fields[i] = *sf.Field
		if !sf.IsLeaf() {
			continue
		}
		if _, ok := pf.MetaData().Schema.Column(sf.ColIndex).LogicalType().(schema.JSONLogicalType); ok {
			sf.Field.Type = arrow.BinaryTypes.Binary
			fields[i].Type, fields[i].Metadata = arrow.BinaryTypes.Binary, jsonMeta
		}
	}
	return fr, arrow.NewSchema(fields, nil), nil
}

// nextBatch is rr.Next, with a panic in arrow-go's reading, such as one of
// a column whose stored Arrow type doesn't match its values, as an error.
func nextBatch(rr pqarrow.RecordReader, key string, rg int, sc *arrow.Schema) (ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ext []string
			for _, f := range sc.Fields() {
				if strings.Contains(f.Type.String(), "extension<") {
					ext = append(ext, strconv.Quote(f.Name))
				}
			}
			ok, err = false, fmt.Errorf("objstore: %s: row group %d: can't read the file's columns (of an extension type: %s): %v", key, rg, strings.Join(ext, ", "), r)
		}
	}()
	return rr.Next(), nil
}

// readParquet reads a Parquet file a row group at a time, by ranges, from
// the row group at.
func (s *Source) readParquet(ctx context.Context, b *blob.Bucket, o object, at filePos, declared map[string]bool, cols *columns, out sink) error {
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.BufferedStreamEnabled, props.BufferSize = true, 1<<20
	pf, err := file.NewParquetReader(&rangeReaderAt{ctx: ctx, b: b, o: o}, file.WithReadProps(props))
	if err != nil {
		return fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	defer func() { _ = pf.Close() }()
	fr, sc, err := parquetReader(pf, pqarrow.ArrowReadProperties{BatchSize: parquetBatch})
	if err != nil {
		return fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	stored, err := fr.Schema()
	if err != nil {
		return fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	// Each column's logical type in the file, and the stream's.
	fields := make([]arrow.Field, 0, sc.NumFields()+3)
	stream := make([]*column, sc.NumFields())
	convs := make([]conversion, sc.NumFields())
	for i, f := range sc.Fields() {
		if controlColumn(f.Name) {
			// The source's own takes its place.
			fields = append(fields, f)
			continue
		}
		if declared != nil && !declared[f.Name] {
			return undeclared(o.Key, f.Name)
		}
		var lf arrow.Field
		lf, convs[i] = logical(f)
		c := cols.by[lf.Name]
		if c == nil || !c.text && !sameType(c.field, lf) {
			return unplanned(o.Key, f.Name)
		}
		stream[i] = c
		fields = append(fields, lf)
	}
	location := s.store.location(o.Key)
	line := int64(1)
	for rg := range at.RowGroup {
		line += pf.RowGroup(rg).NumRows()
	}
	n := pf.NumRowGroups()
	for rg := at.RowGroup; rg < n; rg++ {
		rr, err := fr.GetRecordReader(ctx, nil, []int{rg})
		if err != nil {
			return fmt.Errorf("objstore: %s: row group %d: %w", o.Key, rg, err)
		}
		for {
			ok, err := nextBatch(rr, o.Key, rg, stored)
			if err != nil {
				rr.Release()
				return err
			}
			if !ok {
				break
			}
			rec := rr.RecordBatch()
			arrs := make([]arrow.Array, 0, len(fields)+3)
			batchFields := make([]arrow.Field, 0, len(fields)+3)
			for i, f := range fields {
				if stream[i] == nil {
					continue
				}
				c, err := convert(ctx, rec.Column(i), f, convs[i])
				if err == nil && stream[i].text {
					t, terr := textOf(ctx, c, f, stream[i].field)
					c.Release()
					c, err = t, terr
				}
				if err != nil {
					releaseAll(arrs)
					rr.Release()
					return fmt.Errorf("objstore: %s: column %q: %w", o.Key, f.Name, err)
				}
				arrs = append(arrs, c)
				bf := stream[i].field
				if stream[i].text {
					bf = arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String, Nullable: true}
				}
				batchFields = append(batchFields, bf)
			}
			arrs = control(arrs, location, o, line, int(rec.NumRows()))
			batch := array.NewRecordBatch(arrow.NewSchema(append(batchFields, controlFields()...), nil), arrs, rec.NumRows())
			releaseAll(arrs)
			line += rec.NumRows()
			if err := out.records(batch); err != nil {
				batch.Release()
				rr.Release()
				return err
			}
		}
		err = rr.Err()
		rr.Release()
		if err != nil {
			return fmt.Errorf("objstore: %s: row group %d: %w", o.Key, rg, err)
		}
		if rg+1 < n {
			if err := out.resume(filePos{RowGroup: rg + 1, Records: line - 1}); err != nil {
				return err
			}
		}
	}
	return nil
}

func releaseAll(as []arrow.Array) {
	for _, a := range as {
		a.Release()
	}
}

// textStream is a text file read from a position: decompressed, decoded to
// UTF-8, with the bytes it has read counted.
type textStream struct {
	r      *bufio.Reader
	closer func()
	// The bytes before r's first.
	base int64
}

// openText opens a text file at offset: by a range read for an uncompressed
// UTF-8 file, and otherwise by reading from its start and skipping.
func (s *streamSpec) openText(ctx context.Context, b *blob.Bucket, o object, offset int64) (*textStream, error) {
	compression, _ := compressionOf(o.Key)
	seek := compression == "none" && s.encoding == nil && offset > 0
	from := int64(0)
	if seek {
		from = offset
	}
	raw, err := open(ctx, b, o, from)
	if err != nil {
		return nil, err
	}
	dec, err := decompress(compression, raw)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	var r io.Reader = dec
	if s.encoding != nil {
		r = transform.NewReader(r, s.encoding.NewDecoder())
	}
	ts := &textStream{r: bufio.NewReaderSize(r, 1<<16), closer: func() { _ = dec.Close(); _ = raw.Close() }, base: from}
	if from == 0 {
		if bom, err := ts.r.Peek(3); err == nil && bytes.Equal(bom, []byte{0xef, 0xbb, 0xbf}) {
			_, _ = ts.r.Discard(3)
			ts.base = 3
		}
	}
	return ts, nil
}

// header reads a CSV file's rows to skip and its header, and returns its
// columns' names. The stream is then at its first record.
func (s *streamSpec) header(cr *csvReader) ([]string, error) {
	for range s.cfg.CSV.SkipRows {
		if _, _, err := cr.read(); err != nil {
			return nil, err
		}
	}
	if !s.csvHeader {
		return s.cfg.CSV.ColumnNames, nil
	}
	names, _, err := cr.read()
	if err != nil {
		return nil, err
	}
	for i, n := range names {
		if slices.Contains(names[:i], n) {
			return nil, fmt.Errorf("the header names %q twice", n)
		}
	}
	return names, nil
}

// readCSV reads a CSV file's records from at, as batches of text columns,
// which the platform parses to their declared types.
func (s *Source) readCSV(ctx context.Context, b *blob.Bucket, spec *streamSpec, o object, at filePos, declared map[string]bool, out sink) error {
	ts, err := spec.openText(ctx, b, o, 0)
	if err != nil {
		return err
	}
	cr := newCSVReader(ts.r, spec.delim, spec.quote)
	names, err := spec.header(cr)
	if errors.Is(err, io.EOF) {
		ts.closer()
		return nil
	}
	if err != nil {
		ts.closer()
		return fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	if at.Offset > 0 {
		if seekable, _ := compressionOf(o.Key); seekable == "none" && spec.encoding == nil {
			ts.closer()
			if ts, err = spec.openText(ctx, b, o, at.Offset); err != nil {
				return err
			}
			cr = newCSVReader(ts.r, spec.delim, spec.quote)
		} else {
			for ts.base+cr.offset < at.Offset {
				if _, _, err := cr.read(); err != nil {
					ts.closer()
					return fmt.Errorf("objstore: %s: skip to %d: %w", o.Key, at.Offset, err)
				}
			}
		}
	}
	defer ts.closer()
	location := s.store.location(o.Key)
	// The next record's number, and where it starts.
	line, pending := at.Records+1, int64(0)
	next := func() ([]string, []bool, error) {
		pending = ts.base + cr.offset
		fields, quoted, err := cr.read()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("objstore: %s: record %d: %w", o.Key, line, err)
		}
		return fields, quoted, err
	}
	fields, quoted, err := next()
	for !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		mem := memory.DefaultAllocator
		builders := map[string]*array.StringBuilder{}
		var order []string
		builder := func(name string) (*array.StringBuilder, error) {
			if sb, ok := builders[name]; ok {
				return sb, nil
			}
			if declared != nil && !declared[name] {
				return nil, undeclared(o.Key, name)
			}
			sb := array.NewStringBuilder(mem)
			builders[name] = sb
			order = append(order, name)
			return sb, nil
		}
		for _, n := range names {
			if controlColumn(n) {
				continue
			}
			if _, err := builder(n); err != nil {
				return err
			}
		}
		first, rows, size := line, 0, 0
		for rows < batchRecords && size < batchBytes && !errors.Is(err, io.EOF) {
			if err != nil {
				return err
			}
			for i, f := range fields {
				name := "f" + strconv.Itoa(i)
				switch {
				case i < len(names):
					name = names[i]
				case spec.csvHeader || len(spec.cfg.CSV.ColumnNames) > 0:
					return fmt.Errorf("objstore: %s: record %d has %d fields, and the header %d", o.Key, line, len(fields), len(names))
				}
				if controlColumn(name) {
					// The source's own takes its place.
					continue
				}
				sb, err := builder(name)
				if err != nil {
					return err
				}
				for sb.Len() < rows {
					sb.AppendNull()
				}
				if !quoted[i] && spec.nulls[f] {
					sb.AppendNull()
				} else {
					sb.Append(f)
				}
				size += len(f)
			}
			rows++
			line++
			fields, quoted, err = next()
		}
		cols := make([]arrow.Array, 0, len(order)+3)
		schemaFields := make([]arrow.Field, 0, len(order)+3)
		for _, n := range order {
			sb := builders[n]
			for sb.Len() < rows {
				sb.AppendNull()
			}
			cols = append(cols, sb.NewArray())
			sb.Release()
			schemaFields = append(schemaFields, arrow.Field{Name: n, Type: arrow.BinaryTypes.String, Nullable: true})
		}
		cols = control(cols, location, o, first, rows)
		batch := array.NewRecordBatch(arrow.NewSchema(append(schemaFields, controlFields()...), nil), cols, int64(rows))
		releaseAll(cols)
		if err := out.records(batch); err != nil {
			batch.Release()
			return err
		}
		if !errors.Is(err, io.EOF) {
			if err := out.resume(filePos{Offset: pending, Records: line - 1}); err != nil {
				return err
			}
		}
	}
	return nil
}

// lineReader reads a text stream's non-blank lines, and counts the bytes
// before each.
type lineReader struct {
	ts *textStream
	// The bytes read.
	offset int64
}

// next returns the next non-blank line and where it starts, or io.EOF.
func (l *lineReader) next() ([]byte, int64, error) {
	for {
		start := l.ts.base + l.offset
		line, err := l.ts.r.ReadBytes('\n')
		l.offset += int64(len(line))
		if t := bytes.TrimSpace(line); len(t) > 0 {
			return t, start, nil
		}
		if err != nil {
			return nil, start, err
		}
	}
}

// readJSONL reads a JSON Lines file's objects from at, as JSON rows, which
// the platform takes to their declared types. Each gains the source's
// columns.
func (s *Source) readJSONL(ctx context.Context, b *blob.Bucket, spec *streamSpec, o object, at filePos, declared map[string]bool, out sink) error {
	compression, _ := compressionOf(o.Key)
	seekable := compression == "none" && spec.encoding == nil
	from := int64(0)
	if seekable {
		from = at.Offset
	}
	ts, err := spec.openText(ctx, b, o, from)
	if err != nil {
		return err
	}
	defer ts.closer()
	lr := &lineReader{ts: ts}
	location, _ := json.Marshal(s.store.location(o.Key))
	modified, _ := json.Marshal(o.Modified.UTC().Format(time.RFC3339Nano))
	line := at.Records + 1
	text, pending, err := lr.next()
	for err == nil && pending < at.Offset {
		text, pending, err = lr.next()
	}
	for !errors.Is(err, io.EOF) {
		var rows []json.RawMessage
		size := 0
		for len(rows) < batchRecords && size < batchBytes && !errors.Is(err, io.EOF) {
			if err != nil {
				return fmt.Errorf("objstore: %s: %w", o.Key, err)
			}
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(text, &obj); err != nil || obj == nil {
				return fmt.Errorf("objstore: %s: record %d isn't a JSON object: %.80s", o.Key, line, text)
			}
			for k := range obj {
				if declared != nil && !declared[k] {
					return undeclared(o.Key, k)
				}
			}
			obj[fileColumn], obj[lineColumn], obj[modifiedColumn] = location, json.RawMessage(strconv.FormatInt(line, 10)), modified
			row, merr := json.Marshal(obj)
			if merr != nil {
				return merr
			}
			rows = append(rows, row)
			size += len(row)
			line++
			text, pending, err = lr.next()
		}
		if err := out.rows(rows); err != nil {
			return err
		}
		if !errors.Is(err, io.EOF) {
			if err := out.resume(filePos{Offset: pending, Records: line - 1}); err != nil {
				return err
			}
		}
	}
	return nil
}
