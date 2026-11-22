package objstore

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"

	"github.com/openetl-io/openetl-connectors/records"
)

// The formats the destination writes, and the source reads besides Avro.
const (
	formatParquet = "parquet"
	formatCSV     = "csv"
	formatJSONL   = "jsonl"
	formatAvro    = "avro"
)

var parquetCodecs = map[string]compress.Compression{
	"zstd": compress.Codecs.Zstd, "snappy": compress.Codecs.Snappy, "gzip": compress.Codecs.Gzip, "none": compress.Codecs.Uncompressed,
}

// extension is a file's, by format and, for text formats, compression.
func extension(format, compression string) string {
	ext := "." + format
	switch {
	case format == formatParquet:
	case compression == "gzip":
		ext += ".gz"
	case compression == "zstd":
		ext += ".zst"
	}
	return ext
}

// fileWriter writes one file's batches, all of one schema.
type fileWriter interface {
	write(batch arrow.RecordBatch) error
	// size is about how many bytes the file has, compressed.
	size() int64
	rows() int64
	// close finishes the file, which makes it durable.
	close() error
}

// newFileWriter writes a file of format to w, which it closes.
func newFileWriter(format, compression string, sc *arrow.Schema, w io.WriteCloser) (fileWriter, error) {
	switch format {
	case formatParquet:
		props := parquet.NewWriterProperties(parquet.WithCompression(parquetCodecs[compression]), parquet.WithVersion(parquet.V2_LATEST))
		fw, err := pqarrow.NewFileWriter(sc, w, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
		if err != nil {
			return nil, err
		}
		return &parquetWriter{fw}, nil
	}
	cw := &countingWriter{w: w}
	var out io.Writer = cw
	var flush io.Closer
	switch compression {
	case "gzip":
		z := gzip.NewWriter(cw)
		out, flush = z, z
	case "zstd":
		z, err := zstd.NewWriter(cw)
		if err != nil {
			return nil, err
		}
		out, flush = z, z
	}
	tw := &textWriter{out: out, flush: flush, file: w, counted: cw}
	cols, err := records.Cols(sc)
	if err != nil {
		return nil, err
	}
	tw.cols = cols
	if format == formatJSONL {
		tw.row, err = records.NewEncoder(context.Background(), records.JSON, nil, "", "", cols, false)
		return tw, err
	}
	for i, f := range sc.Fields() {
		enc, err := records.NewEncoder(context.Background(), records.JSON, nil, "", "", cols[i:i+1], false)
		if err != nil {
			return nil, err
		}
		v, _ := f.Metadata.GetValue(records.ExtensionNameKey)
		if et, ok := f.Type.(arrow.ExtensionType); ok {
			v = et.ExtensionName()
		}
		tw.cells = append(tw.cells, cellEncoder{enc: enc, skip: len(records.AppendJSONString([]byte("{"), f.Name)) + 1, json: v == records.JSONExtension})
		if i > 0 {
			tw.header = append(tw.header, ',')
		}
		tw.header = appendCSVField(tw.header, f.Name, false)
	}
	tw.header = append(tw.header, '\n')
	return tw, nil
}

type parquetWriter struct{ fw *pqarrow.FileWriter }

// A row group closes at this many bytes.
const rowGroupSize = 64 << 20

func (p *parquetWriter) write(batch arrow.RecordBatch) error {
	if err := p.fw.WriteBuffered(batch); err != nil {
		return err
	}
	if p.fw.RowGroupTotalBytesWritten() >= rowGroupSize {
		p.fw.NewBufferedRowGroup()
	}
	return nil
}

func (p *parquetWriter) size() int64 {
	return p.fw.TotalBytesWritten() + p.fw.RowGroupTotalBytesWritten()
}
func (p *parquetWriter) rows() int64  { return int64(p.fw.NumRows()) }
func (p *parquetWriter) close() error { return p.fw.Close() }

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// textWriter writes CSV, a header line then a line a row, or JSON Lines, an
// object a line.
type textWriter struct {
	out     io.Writer
	flush   io.Closer
	file    io.Closer
	counted *countingWriter
	cols    []records.Col
	// JSON Lines.
	row records.Encoder
	// CSV.
	header []byte
	cells  []cellEncoder
	n      int64
	buf    []byte
	vals   []any
}

// cellEncoder encodes a CSV cell as JSON, whose object it is the one field
// of: skip is how long its name's prefix is.
type cellEncoder struct {
	enc  records.Encoder
	skip int
	// The cell holds JSON, kept as its text.
	json bool
}

func (t *textWriter) write(batch arrow.RecordBatch) error {
	if t.header != nil {
		if _, err := t.out.Write(t.header); err != nil {
			return err
		}
		t.header = nil
	}
	t.vals = append(t.vals[:0], make([]any, len(t.cols))...)
	for i := range int(batch.NumRows()) {
		for j := range t.cols {
			v, err := t.cols[j].At(batch.Column(j), i)
			if err != nil {
				return err
			}
			t.vals[j] = v
		}
		var err error
		if t.row != nil {
			b, err := t.row.Encode(t.vals)
			if err != nil {
				return err
			}
			t.buf = append(append(t.buf[:0], b...), '\n')
		} else if t.buf, err = t.csvLine(t.buf[:0]); err != nil {
			return err
		}
		if _, err := t.out.Write(t.buf); err != nil {
			return err
		}
		t.n++
	}
	return nil
}

// csvLine is the row as a CSV line: a null is an empty field, and an empty
// text a quoted one. Other values are their JSON, but text, dates and the
// like without its quotes.
func (t *textWriter) csvLine(b []byte) ([]byte, error) {
	for j, c := range t.cells {
		if j > 0 {
			b = append(b, ',')
		}
		v := t.vals[j]
		if v == nil {
			continue
		}
		if raw, ok := v.(json.RawMessage); ok && c.json {
			b = appendCSVField(b, string(raw), true)
			continue
		}
		enc, err := c.enc.Encode([]any{v})
		if err != nil {
			return nil, err
		}
		text := string(enc[c.skip : len(enc)-1])
		if strings.HasPrefix(text, `"`) {
			if err := json.Unmarshal([]byte(text), &text); err != nil {
				return nil, err
			}
		}
		b = appendCSVField(b, text, true)
	}
	return append(b, '\n'), nil
}

// appendCSVField quotes s if it needs to be, or is empty and not null.
func appendCSVField(b []byte, s string, value bool) []byte {
	if !(value && s == "") && !strings.ContainsAny(s, ",\"\r\n") {
		return append(b, s...)
	}
	b = append(b, '"')
	b = append(b, strings.ReplaceAll(s, `"`, `""`)...)
	return append(b, '"')
}

func (t *textWriter) size() int64 { return t.counted.n }
func (t *textWriter) rows() int64 { return t.n }

func (t *textWriter) close() error {
	if t.header != nil {
		if _, err := t.out.Write(t.header); err != nil {
			_ = t.file.Close()
			return err
		}
	}
	if t.flush != nil {
		if err := t.flush.Close(); err != nil {
			_ = t.file.Close()
			return fmt.Errorf("compress: %w", err)
		}
	}
	return t.file.Close()
}

// compressionOf is the compression name's extension says, and the name
// without it.
func compressionOf(name string) (compression, base string) {
	for ext, c := range map[string]string{".gz": "gzip", ".gzip": "gzip", ".zst": "zstd", ".zstd": "zstd", ".sz": "snappy", ".snappy": "snappy", ".bz2": "bzip2"} {
		if strings.HasSuffix(name, ext) {
			return c, strings.TrimSuffix(name, ext)
		}
	}
	return "none", name
}

// decompress reads r decompressed with compression: gzip, zstd, bzip2, snappy's
// framing format, or none.
func decompress(compression string, r io.Reader) (io.ReadCloser, error) {
	switch compression {
	case "gzip":
		return gzip.NewReader(r)
	case "zstd":
		z, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		return z.IOReadCloser(), nil
	case "bzip2":
		return io.NopCloser(bzip2.NewReader(r)), nil
	case "snappy":
		return io.NopCloser(snappy.NewReader(r)), nil
	}
	return io.NopCloser(r), nil
}
