package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"gocloud.dev/blob"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// SourceConfig is where the source's files are, and its streams: the
// settings of every connector's source, besides its store's.
type SourceConfig struct {
	// SCHEME://BUCKET/PREFIX, or file:// and an absolute path.
	URL     string         `json:"url"`
	Streams []StreamConfig `json:"streams"`
	// How long before the newest file an incremental read has read it still
	// looks for files it hasn't: a Go duration. Default 1h.
	Lookback string `json:"lookback"`
}

// StreamConfig is a stream: the files a glob matches, of one format.
type StreamConfig struct {
	Name string `json:"name"`
	// Relative to the URL.
	Glob string `json:"glob"`
	// parquet, avro, csv or jsonl.
	Format string    `json:"format"`
	CSV    CSVConfig `json:"csv"`
	// Columns' types, by name, over what discovery infers.
	Columns map[string]string `json:"columns"`
	// Empty: _file and _line.
	PrimaryKey []string `json:"primaryKey"`
}

type CSVConfig struct {
	// One character. Default a comma.
	Delimiter string `json:"delimiter"`
	// One character. Default a double quote.
	Quote string `json:"quote"`
	// Nil: true.
	Header *bool `json:"header"`
	// The columns of files without a header. Empty: f0, f1 and so on.
	ColumnNames []string `json:"columnNames"`
	// Lines before the header.
	SkipRows int `json:"skipRows"`
	// A WHATWG encoding label. Default utf-8.
	Encoding string `json:"encoding"`
	// Unquoted values that are null. Nil: the empty one.
	NullValues []string `json:"nullValues"`
}

type Source struct {
	cfg      SourceConfig
	store    *store
	lookback time.Duration
	streams  map[string]*streamSpec
	// The splits being read, by runningKey.
	running sync.Map
}

// streamSpec is a parsed StreamConfig.
type streamSpec struct {
	cfg          StreamConfig
	glob         *regexp.Regexp
	prefix       string
	delim, quote byte
	csvHeader    bool
	// Nil for UTF-8.
	encoding encoding.Encoding
	nulls    map[string]bool
	declared []arrow.Field
}

var (
	_ source.Source      = (*Source)(nil)
	_ source.CheckLister = (*Source)(nil)
	_ source.Preflighter = (*Source)(nil)
	_ source.Resplitter  = (*Source)(nil)
	_ source.Splitter    = (*Source)(nil)
)

// NewSource is the source of config, whose store's settings decode into
// settings, which store makes the store of.
func NewSource(config []byte, settings any, store NewStore) (*Source, error) {
	var cfg SourceConfig
	if err := decode(config, &cfg, settings); err != nil {
		return nil, err
	}
	st, err := newStore(cfg.URL, store)
	if err != nil {
		return nil, err
	}
	s := &Source{cfg: cfg, store: st, lookback: time.Hour, streams: map[string]*streamSpec{}}
	if cfg.Lookback != "" {
		if s.lookback, err = time.ParseDuration(cfg.Lookback); err != nil || s.lookback < 0 {
			return nil, fmt.Errorf("objstore: lookback %q isn't a duration of 0 or more", cfg.Lookback)
		}
	}
	if len(cfg.Streams) == 0 {
		return nil, errors.New("objstore: the source has no streams")
	}
	for _, sc := range cfg.Streams {
		spec, err := parseStream(sc)
		if err != nil {
			return nil, err
		}
		if _, dup := s.streams[sc.Name]; dup {
			return nil, fmt.Errorf("objstore: two streams are named %q", sc.Name)
		}
		s.streams[sc.Name] = spec
	}
	return s, nil
}

func parseStream(sc StreamConfig) (*streamSpec, error) {
	if sc.Name == "" || sc.Glob == "" {
		return nil, errors.New("objstore: a stream needs a name and a glob")
	}
	spec := &streamSpec{cfg: sc, delim: ',', quote: '"', csvHeader: sc.CSV.Header == nil || *sc.CSV.Header}
	var err error
	if spec.glob, spec.prefix, err = globRegexp(strings.TrimPrefix(sc.Glob, "/")); err != nil {
		return nil, fmt.Errorf("objstore: stream %q: %w", sc.Name, err)
	}
	switch sc.Format {
	case formatParquet, formatJSONL, formatAvro:
	case formatCSV:
		for _, c := range []struct {
			name, v string
			to      *byte
		}{{"delimiter", sc.CSV.Delimiter, &spec.delim}, {"quote", sc.CSV.Quote, &spec.quote}} {
			switch {
			case c.v == "":
			case len(c.v) == 1 && c.v[0] != '\n' && c.v[0] != '\r':
				*c.to = c.v[0]
			default:
				return nil, fmt.Errorf("objstore: stream %q: the CSV %s %q isn't one ASCII character", sc.Name, c.name, c.v)
			}
		}
		if spec.delim == spec.quote {
			return nil, fmt.Errorf("objstore: stream %q: the CSV delimiter and quote are the same", sc.Name)
		}
		if spec.csvHeader && len(sc.CSV.ColumnNames) > 0 {
			return nil, fmt.Errorf("objstore: stream %q: columnNames are for CSV files without a header", sc.Name)
		}
		spec.nulls = map[string]bool{}
		for _, n := range cmpOr(sc.CSV.NullValues, []string{""}) {
			spec.nulls[n] = true
		}
	default:
		return nil, fmt.Errorf("objstore: stream %q: format %q is none of parquet, avro, csv and jsonl", sc.Name, sc.Format)
	}
	if sc.CSV.Encoding != "" && !strings.EqualFold(sc.CSV.Encoding, "utf-8") && !strings.EqualFold(sc.CSV.Encoding, "utf8") {
		if spec.encoding, err = htmlindex.Get(sc.CSV.Encoding); err != nil {
			return nil, fmt.Errorf("objstore: stream %q: encoding %q: %w", sc.Name, sc.CSV.Encoding, err)
		}
	}
	for _, name := range sortedKeys(sc.Columns) {
		f, err := declaredField(name, sc.Columns[name])
		if err != nil {
			return nil, fmt.Errorf("objstore: stream %q: %w", sc.Name, err)
		}
		spec.declared = append(spec.declared, f)
	}
	return spec, nil
}

func cmpOr[T any](a, b []T) []T {
	if a != nil {
		return a
	}
	return b
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (s *Source) Discover(ctx context.Context) (_ source.Catalog, err error) {
	defer func() { err = s.store.classify(err) }()
	b, err := s.store.open(ctx)
	if err != nil {
		return source.Catalog{}, err
	}
	defer func() { _ = b.Close() }()
	var cat source.Catalog
	for _, sc := range s.cfg.Streams {
		spec := s.streams[sc.Name]
		fields, err := s.discover(ctx, b, spec)
		if err != nil {
			return cat, err
		}
		key := sc.PrimaryKey
		if len(key) == 0 {
			key = []string{fileColumn, lineColumn}
		}
		cat.Streams = append(cat.Streams, source.CatalogStream{
			Name: sc.Name, PrimaryKey: key, CursorField: modifiedColumn, Replay: source.Superset,
			Schema:    arrow.NewSchema(append(fields, controlFields()...), nil),
			SyncModes: []source.SyncMode{source.FullRefresh, source.Incremental},
		})
	}
	return cat, nil
}

// How much of each text file discovery samples.
const (
	sampleRecords = 1000
	sampleBytes   = 1 << 20
)

// discover is a stream's columns: those its files have, every one's, of a
// type every file's values fit, or the type the config declares.
func (s *Source) discover(ctx context.Context, b *blob.Bucket, spec *streamSpec) ([]arrow.Field, error) {
	files, err := spec.list(ctx, b, wholeRange, true)
	if err != nil {
		return nil, err
	}
	var fields []arrow.Field
	at := map[string]int{}
	add := func(f arrow.Field) {
		if _, ok := at[f.Name]; ok || f.Name == fileColumn || f.Name == lineColumn || f.Name == modifiedColumn {
			return
		}
		at[f.Name] = len(fields)
		fields = append(fields, f)
	}
	samples := map[string]*inferred{}
	var order []string
	seen := func(name string) *inferred {
		in, ok := samples[name]
		if !ok {
			in = &inferred{}
			samples[name] = in
			order = append(order, name)
		}
		return in
	}
	if spec.cfg.Format == formatParquet || spec.cfg.Format == formatAvro {
		c, err := spec.schemaOf(ctx, b, files)
		if err != nil {
			return nil, err
		}
		for _, f := range c.fields() {
			add(f)
		}
		files = nil
	}
	for _, o := range files {
		switch spec.cfg.Format {
		case formatCSV:
			if err := spec.sampleCSV(ctx, b, o, seen); err != nil {
				return nil, err
			}
		case formatJSONL:
			if err := spec.sampleJSONL(ctx, b, o, seen); err != nil {
				return nil, err
			}
		}
	}
	for _, name := range order {
		add(samples[name].field(name))
	}
	for _, d := range spec.declared {
		if i, ok := at[d.Name]; ok {
			fields[i] = d
		} else {
			add(d)
		}
	}
	return fields, nil
}

// sampleCSV reads a CSV file's header, and the values of its first
// records.
func (s *streamSpec) sampleCSV(ctx context.Context, b *blob.Bucket, o object, seen func(string) *inferred) error {
	ts, err := s.openText(ctx, b, o, 0)
	if err != nil {
		return err
	}
	defer ts.closer()
	cr := newCSVReader(ts.r, s.delim, s.quote)
	names, err := s.header(cr)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	for _, n := range names {
		seen(n)
	}
	for range sampleRecords {
		fields, quoted, err := cr.read()
		if errors.Is(err, io.EOF) || cr.offset > sampleBytes {
			break
		}
		if err != nil {
			return fmt.Errorf("objstore: %s: %w", o.Key, err)
		}
		for i, f := range fields {
			name := "f" + fmt.Sprint(i)
			if i < len(names) {
				name = names[i]
			}
			in := seen(name)
			if quoted[i] || !s.nulls[f] {
				in.text(f)
			}
		}
	}
	return nil
}

// sampleJSONL reads the keys and values of a JSON Lines file's first
// objects.
func (s *streamSpec) sampleJSONL(ctx context.Context, b *blob.Bucket, o object, seen func(string) *inferred) error {
	ts, err := s.openText(ctx, b, o, 0)
	if err != nil {
		return err
	}
	defer ts.closer()
	lr := &lineReader{ts: ts}
	for range sampleRecords {
		text, _, err := lr.next()
		if errors.Is(err, io.EOF) || lr.offset > sampleBytes {
			break
		}
		if err != nil {
			return fmt.Errorf("objstore: %s: %w", o.Key, err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(text, &obj); err != nil {
			return fmt.Errorf("objstore: %s: a line isn't a JSON object: %.80s", o.Key, text)
		}
		for _, k := range sortedKeys(obj) {
			seen(k).json(obj[k])
		}
	}
	return nil
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	b, err := s.store.open(ctx)
	if err != nil {
		return s.store.classify(err)
	}
	defer func() { _ = b.Close() }()
	typed := &typedColumns{}
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, se source.SplitEmitter) error {
		return s.store.classify(s.readSplit(ctx, b, typed, cs, sp, se))
	})
}

// readSplit reads a split's files in order, from its state, with a resume
// point after each batch, and one after each file.
func (s *Source) readSplit(ctx context.Context, b *blob.Bucket, typed *typedColumns, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
	spec, ok := s.streams[cs.Name]
	if !ok {
		return fmt.Errorf("objstore: unknown stream %q", cs.Name)
	}
	incremental := cs.SyncMode == source.Incremental
	var st readState
	if len(sp.State) > 0 {
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return fmt.Errorf("objstore: stream %q's state: %w", cs.Name, err)
		}
	}
	r, err := rangeOf(sp.ID, &st)
	if err != nil {
		return err
	}
	var declared map[string]bool
	if cs.Columns != nil {
		declared = map[string]bool{}
		for _, c := range cs.Columns {
			declared[c] = true
		}
	}
	var cols *columns
	if spec.cfg.Format == formatParquet || spec.cfg.Format == formatAvro {
		if cols, err = typed.of(ctx, b, spec); err != nil {
			return err
		}
	}
	files, err := spec.list(ctx, b, r, incremental)
	if err != nil {
		return err
	}
	until := sp.Until
	if sp.ID == source.WholeStream && len(cs.Splits) == 0 {
		until = cs.Until
	}
	run := &running{until: len(until) > 0}
	key := runningKey(cs.Name, sp.ID)
	s.running.Store(key, run)
	defer s.running.Delete(key)
	// A read up to a resume point after a Resplit's halving that its
	// state is before halves where the first read did.
	var cuts []cut
	if len(until) > 0 {
		var u readState
		if err := json.Unmarshal(until, &u); err == nil && len(u.Gave) > len(st.Gave) {
			cuts = u.Gave[len(st.Gave):]
		}
	}
	cutTo := func(o object, inclusive bool) {
		for len(cuts) > 0 {
			c := cmpPos(cuts[0], o, incremental)
			if c > 0 || c == 0 && !inclusive {
				return
			}
			r, st.Range, st.Gave = cuts[0].Keep, &cuts[0].Keep, append(st.Gave, cuts[0])
			cuts = cuts[1:]
		}
	}
	// resume sends a resume point at st, in or after file o, and reports
	// whether it's the read's until, where the split completes. A Resplit
	// asked for gives half the files yet to be read to a new split.
	resume := func(o object, in bool) (bool, error) {
		cutTo(o, true)
		var added []source.Split
		run.mu.Lock()
		if run.pending {
			run.pending = false
			current := ""
			if in {
				current = o.Key
			}
			if keep, give, ok := halve(r, run.left, current); ok {
				r = keep
				st.Range = &keep
				st.Gave = append(slices.Clip(st.Gave), cut{Keep: keep, Give: give, Key: o.Key, Modified: o.Modified.UnixNano()})
				child := st
				child.At, child.Range, child.Gave = nil, &give, nil
				state, err := json.Marshal(child)
				if err != nil {
					run.mu.Unlock()
					return false, err
				}
				added = append(added, source.Split{ID: give.id(), State: state})
				run.left = slices.DeleteFunc(run.left, func(k string) bool { return !keep.has(k) })
			}
		}
		run.mu.Unlock()
		state, err := json.Marshal(st)
		if err != nil {
			return false, err
		}
		if len(until) > 0 && bytes.Equal(state, until) {
			if err := out.ResumePoint(until); err != nil {
				return false, err
			}
			return true, out.Complete(until)
		}
		return false, out.ResumePoint(state, added...)
	}
	setLeft := func(from int) {
		run.mu.Lock()
		defer run.mu.Unlock()
		run.left = run.left[:0]
		for _, o := range files[from:] {
			if r.has(o.Key) && !st.done(o, incremental) {
				run.left = append(run.left, o.Key)
			}
		}
	}
	unmarked := 0
	for i, o := range files {
		cutTo(o, false)
		if !r.has(o.Key) || st.done(o, incremental) {
			continue
		}
		setLeft(i + 1)
		at := filePos{Key: o.Key, Modified: o.Modified.UnixNano(), Size: o.Size}
		if st.At != nil && st.At.Key == o.Key && st.At.Modified == at.Modified && st.At.Size == o.Size {
			at = *st.At
		}
		stop := false
		sk := sink{
			records: out.Records,
			rows:    func(rows []json.RawMessage) error { return out.Rows(rows) },
			resume: func(p filePos) error {
				p.Key, p.Modified, p.Size = at.Key, at.Modified, at.Size
				st.At = &p
				var err error
				stop, err = resume(o, true)
				if err == nil && stop {
					err = errStop
				}
				return err
			},
		}
		switch spec.cfg.Format {
		case formatParquet:
			err = s.readParquet(ctx, b, o, at, declared, cols, sk)
		case formatAvro:
			err = s.readAvro(ctx, b, o, at, declared, cols, sk)
		case formatCSV:
			err = s.readCSV(ctx, b, spec, o, at, declared, sk)
		case formatJSONL:
			err = s.readJSONL(ctx, b, spec, o, at, declared, sk)
		}
		if stop {
			return nil
		}
		if err != nil {
			return err
		}
		st.finish(o, incremental)
		// A large state's resume points come every so many files, but for
		// a Resplit's.
		run.mu.Lock()
		pending := run.pending
		run.mu.Unlock()
		if unmarked++; len(st.Read) > 1000 && unmarked < 100 && !pending {
			continue
		}
		unmarked = 0
		if stop, err := resume(o, false); err != nil || stop {
			return err
		}
	}
	if !incremental {
		return out.Complete(nil)
	}
	next, err := json.Marshal(st.next(s.lookback))
	if err != nil {
		return err
	}
	return out.Complete(next)
}

// errStop ends a file's read at the read's until.
var errStop = errors.New("objstore: the read is at its until")
