package mongodb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl-connectors/docstore"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	// A MongoDB connection string.
	URI sdk.Secret `json:"uri"`
	// The database collections are written to when the pipeline names no
	// namespace.
	Database string `json:"database"`
	// String key columns are _ids as the MongoDB source spells them
	// (keyText), written back as their BSON values.
	ExtendedJSONKeys bool `json:"extendedJSONKeys"`
}

// Destination writes each stream to a collection, staging an epoch's rows
// in collections of their own and applying them in Commit, with writes that
// apply alike however often they run: merge upserts and deletes keyed on
// the primary key and guarded by _openetl_seq, append inserts keyed on
// _openetl_id where they're deduped against the target, or on an ObjectId
// staging gave the row. A stream's apply ends
// in its marker, which a single-document update of the pipeline's fence
// writes, refused once a newer generation has raised the fence. A crash
// before the marker leaves the staging, which Recover applies again.
type Destination struct {
	cfg DestinationConfig
	// From uri, as parsed when the destination was made.
	opts *options.ClientOptions
}

var (
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	var cfg DestinationConfig
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("mongodb: config: %w", err)
	}
	switch {
	case cfg.URI.IsZero():
		return nil, errors.New("mongodb: config needs a uri")
	case cfg.Database == "":
		return nil, errors.New("mongodb: config needs a database")
	}
	opts, err := clientOptions(cfg.URI.Reveal())
	if err != nil {
		return nil, err
	}
	return &Destination{cfg: cfg, opts: opts}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	c, err := connect(d.cfg.URI.Reveal())
	if err != nil {
		return nil, err
	}
	db := cmpOr(cfg.Namespace, d.cfg.Database)
	w := &writer{
		client: c, db: c.Database(db), cfg: cfg, bsonKeys: d.cfg.ExtendedJSONKeys,
		streams: map[string]destination.Stream{}, targets: map[string]string{},
		epochs: map[destination.Epoch]map[string]*laneStream{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

const (
	idColumn        = "_openetl_id"
	opColumn        = "_openetl_op"
	seqColumn       = "_openetl_seq"
	syncedColumn    = "_openetl_synced_at"
	deletedColumn   = "_openetl_deleted"
	unchangedColumn = "_openetl_unchanged"
	// A row's held columns, as a list; in the target, a document from each
	// to the _openetl_id of the version whose value is held.
	heldColumn = destination.HeldColumn
	// The pipeline's fence: its generation, and each stream's marker.
	fenceCollection = "_openetl_fence"
	// Rows a write sends at a time.
	writeBatch = 1000
)

type writer struct {
	client *mongo.Client
	db     *mongo.Database
	cfg    destination.OpenConfig
	// String keys are read with bsonKey.
	bsonKeys bool

	streams map[string]destination.Stream
	starts  map[string]time.Time
	// What the lane has staged of each epoch, by stream.
	epochs map[destination.Epoch]map[string]*laneStream
	n      int64

	mu sync.Mutex
	// Each stream's collection, as its schemas name it.
	targets map[string]string
}

// laneStream is what a lane staged of a stream in an epoch.
type laneStream struct {
	staging, target, read string
	rows                  int64
	ended                 bool
}

func hash(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(h[:6])
}

// stagingPrefix starts the names of the pipeline's staging collections.
func stagingPrefix(pipeline string) string { return "_openetl_stage_" + hash(pipeline) + "_" }

func stagingName(pipeline, stream string, e destination.Epoch, lane int) string {
	return fmt.Sprintf("%s%d_%d_%d_%s", stagingPrefix(pipeline), e.Generation, e.N, lane, hash(stream))
}

// readName is the collection an overwrite read's rows go into, until it
// ends and takes the target's place.
func readName(pipeline, stream string, start time.Time) string {
	return "_openetl_read_" + hash(pipeline, stream) + "_" + hash(strconv.FormatInt(start.UnixMicro(), 10))
}

func (w *writer) setTarget(stream, target string) {
	w.mu.Lock()
	w.targets[stream] = target
	w.mu.Unlock()
}

func (w *writer) target(stream string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.targets[stream]
}

// StartReads drops the collections of reads that the attempt starts over.
func (w *writer) StartReads(ctx context.Context, starts map[string]time.Time) error {
	w.starts = starts
	names, err := w.db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return classify(fmt.Errorf("mongodb: list collections of %s: %w", w.db.Name(), err))
	}
	for stream, start := range starts {
		prefix, current := "_openetl_read_"+hash(w.cfg.Pipeline, stream)+"_", readName(w.cfg.Pipeline, stream, start)
		for _, n := range names {
			if strings.HasPrefix(n, prefix) && n != current {
				if err := w.db.Collection(n).Drop(ctx); err != nil {
					return classify(fmt.Errorf("mongodb: drop %s: %w", n, err))
				}
			}
		}
	}
	return nil
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.lane(e, stream).ended = true
	return nil
}

func (w *writer) lane(e destination.Epoch, stream string) *laneStream {
	if w.epochs[e] == nil {
		w.epochs[e] = map[string]*laneStream{}
	}
	ls := w.epochs[e][stream]
	if ls == nil {
		ls = &laneStream{staging: stagingName(w.cfg.Pipeline, stream, e, w.cfg.Lane)}
		if start, ok := w.starts[stream]; ok && w.streams[stream].Mode == destination.Overwrite {
			ls.read = readName(w.cfg.Pipeline, stream, start)
		}
		w.epochs[e][stream] = ls
	}
	return ls
}

// keyed reports whether a stream's documents are keyed on its primary key:
// merged, or overwritten with a key.
func keyed(s destination.Stream) bool {
	return len(s.PrimaryKey) > 0 && (s.Mode == destination.Merge || s.Mode == destination.Overwrite)
}

// stagedRow is a staging collection's document: a row, or a patch.
type stagedRow struct {
	N   int64    `bson:"_id"`
	Doc bson.Raw `bson:"d"`
	// The columns whose target values a merged row keeps: those held with
	// "last", and those an update didn't send.
	Keep []string `bson:"k,omitempty"`
	// A patch: Doc has the key, k, or the version, v, it fills, and the
	// values, f, by column.
	Patch bool `bson:"p,omitempty"`
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	return w.stage(ctx, e, stream, batch, false)
}

// Patch stages patches with the epoch's rows; Commit applies them after
// every lane's rows.
func (w *writer) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	return w.stage(ctx, e, stream, batch, true)
}

func (w *writer) stage(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch, patch bool) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("mongodb: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Mode == destination.Overwrite {
		if _, ok := w.starts[stream]; !ok {
			return fmt.Errorf("mongodb: stream %q overwrites, but its read has no start", stream)
		}
	}
	ls := w.lane(e, stream)
	ls.target = targetCollection(batch.Schema(), stream)
	w.setTarget(stream, ls.target)
	var rows []any
	var err error
	if patch {
		rows, err = w.patchRows(cfg, batch)
	} else {
		rows, err = w.rows(cfg, batch)
	}
	if err != nil {
		return fmt.Errorf("mongodb: stream %q: %w", stream, err)
	}
	if _, err := w.db.Collection(ls.staging).InsertMany(ctx, rows, options.InsertMany().SetOrdered(false)); err != nil {
		return classify(fmt.Errorf("mongodb: stage %s: %w", stream, err))
	}
	ls.rows += int64(len(rows))
	return nil
}

// columns are a batch's converters by column, nil for the metadata columns
// that don't go into documents as they are.
func columns(schema *arrow.Schema) ([]convert, error) {
	out := make([]convert, schema.NumFields())
	for i, f := range schema.Fields() {
		switch f.Name {
		case seqColumn:
			out[i] = seqConverter(f.Type)
		case heldColumn, unchangedColumn:
		default:
			c, _, err := converter(f)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
	}
	return out, nil
}

// seqConverter keeps _openetl_seq as a number, which orders a key's versions
// exactly: integers as they are, timestamps in their unit since 1970, and
// decimals as Decimal128.
func seqConverter(t arrow.DataType) convert {
	switch t := t.(type) {
	case *arrow.TimestampType:
		return func(a arrow.Array, i int) (any, error) { return int64(storage(a).(*array.Timestamp).Value(i)), nil }
	case *arrow.Decimal128Type:
		return func(a arrow.Array, i int) (any, error) {
			return decimalValue(storage(a).(*array.Decimal128).Value(i).BigInt(), t.Scale)
		}
	}
	c, _, err := converter(arrow.Field{Name: seqColumn, Type: t})
	if err != nil {
		return func(arrow.Array, int) (any, error) { return nil, err }
	}
	return c
}

// rows are a batch's rows as staged documents. A document's _id is its key
// where its stream is keyed, its _openetl_id where its stream is deduped
// against the target, and an ObjectId otherwise, given here so that
// applying it again finds it: deduped within each epoch only, a later
// epoch's row of the same _openetl_id lands beside it.
// A null is no field; a held column's field is null, unless its target value
// is kept.
func (w *writer) rows(cfg destination.Stream, batch arrow.RecordBatch) ([]any, error) {
	schema := batch.Schema()
	convs, err := columns(schema)
	if err != nil {
		return nil, err
	}
	keyCols, err := keyColumns(schema, cfg)
	if err != nil {
		return nil, err
	}
	idCol, heldCol, unchangedCol := index(schema, idColumn), index(schema, heldColumn), index(schema, unchangedColumn)
	if cfg.Dedupe != destination.DedupeNone && idCol < 0 {
		return nil, fmt.Errorf("deduped at apply, but its batches have no %s", idColumn)
	}
	last := map[string]bool{}
	for _, f := range schema.Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" {
			last[f.Name] = true
		}
	}
	out := make([]any, 0, batch.NumRows())
	for i := range int(batch.NumRows()) {
		doc := bson.D{{Key: "_id", Value: nil}}
		for c, conv := range convs {
			name := schema.Field(c).Name
			if conv == nil || batch.Column(c).IsNull(i) || name == "_id" {
				continue
			}
			v, err := conv(batch.Column(c), i)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", name, err)
			}
			doc = append(doc, bson.E{Key: name, Value: v})
		}
		switch {
		case keyed(cfg):
			doc[0].Value, err = w.keyValue(batch, keyCols, i)
			if err != nil {
				return nil, err
			}
		case cfg.Dedupe == destination.DedupeTarget:
			doc[0].Value, _ = convs[idCol](batch.Column(idCol), i)
		default:
			doc[0].Value = bson.NewObjectID()
		}
		var keep []string
		if heldCol >= 0 {
			if held := docstore.ListStrings(batch.Column(heldCol), i); len(held) > 0 {
				version, _ := convs[idCol](batch.Column(idCol), i)
				m := bson.D{}
				for _, c := range held {
					m = append(m, bson.E{Key: c, Value: version})
					if last[c] && cfg.Mode == destination.Merge {
						keep = append(keep, c)
					}
				}
				doc = append(doc, bson.E{Key: heldColumn, Value: m})
			}
		}
		if unchangedCol >= 0 && cfg.Mode == destination.Merge {
			keep = append(keep, docstore.ListStrings(batch.Column(unchangedCol), i)...)
		}
		raw, err := bson.Marshal(doc)
		if err != nil {
			return nil, err
		}
		w.n++
		out = append(out, stagedRow{N: w.n, Doc: raw, Keep: keep})
	}
	return out, nil
}

// patchRows are a batch of patches as staged documents: each names the
// version whose held cells it fills, by _openetl_id, and for merge the key,
// and the value of each column its _openetl_held lists, from the column of
// that name or the one whose metadata says it fills it.
func (w *writer) patchRows(cfg destination.Stream, batch arrow.RecordBatch) ([]any, error) {
	schema := batch.Schema()
	convs, err := columns(schema)
	if err != nil {
		return nil, err
	}
	keyCols, err := keyColumns(schema, cfg)
	if err != nil {
		return nil, err
	}
	idCol, heldCol := index(schema, idColumn), index(schema, heldColumn)
	if idCol < 0 || heldCol < 0 {
		return nil, fmt.Errorf("patches need %s and %s", idColumn, heldColumn)
	}
	fills := map[string]int{}
	for c, f := range schema.Fields() {
		name := f.Name
		if v, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			name = v
		}
		if _, ok := fills[name]; !ok || name != f.Name {
			fills[name] = c
		}
	}
	out := make([]any, 0, batch.NumRows())
	for i := range int(batch.NumRows()) {
		version, _ := convs[idCol](batch.Column(idCol), i)
		p := bson.D{{Key: "v", Value: version}}
		if cfg.Mode == destination.Merge {
			k, err := w.keyValue(batch, keyCols, i)
			if err != nil {
				return nil, err
			}
			p = append(p, bson.E{Key: "k", Value: k})
		}
		f := bson.D{}
		for _, col := range docstore.ListStrings(batch.Column(heldCol), i) {
			c, ok := fills[col]
			if !ok {
				return nil, fmt.Errorf("a patch fills %q, which it has no column for", col)
			}
			var v any
			if !batch.Column(c).IsNull(i) {
				if v, err = convs[c](batch.Column(c), i); err != nil {
					return nil, err
				}
			}
			f = append(f, bson.E{Key: col, Value: v})
		}
		raw, err := bson.Marshal(append(p, bson.E{Key: "f", Value: f}))
		if err != nil {
			return nil, err
		}
		w.n++
		out = append(out, stagedRow{N: w.n, Doc: raw, Patch: true})
	}
	return out, nil
}

func index(schema *arrow.Schema, name string) int {
	if i := schema.FieldIndices(name); len(i) > 0 {
		return i[0]
	}
	return -1
}

func keyColumns(schema *arrow.Schema, cfg destination.Stream) ([]int, error) {
	if !keyed(cfg) {
		return nil, nil
	}
	out := make([]int, len(cfg.PrimaryKey))
	for k, c := range cfg.PrimaryKey {
		if out[k] = index(schema, c); out[k] < 0 {
			return nil, fmt.Errorf("its batches have no key column %q", c)
		}
	}
	return out, nil
}

// keyValue is a keyed document's _id: its key's one column's value, or a
// document of its columns' in the key's order.
func (w *writer) keyValue(batch arrow.RecordBatch, cols []int, i int) (any, error) {
	vals := bson.D{}
	for _, c := range cols {
		f := batch.Schema().Field(c)
		if batch.Column(c).IsNull(i) {
			return nil, fmt.Errorf("key column %q is null", f.Name)
		}
		conv, kind, err := converter(f)
		if err != nil {
			return nil, err
		}
		if w.bsonKeys && kind == "string" && isString(f.Type) {
			conv = func(a arrow.Array, i int) (any, error) {
				v, err := bsonKey(str(a, i))
				return v, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, err)
			}
		}
		v, err := conv(batch.Column(c), i)
		if err != nil {
			return nil, err
		}
		vals = append(vals, bson.E{Key: f.Name, Value: v})
	}
	if len(vals) == 1 {
		return vals[0].Value, nil
	}
	return vals, nil
}

// staged is a lane's committable: what it staged of an epoch.
type staged struct {
	Generation uint64         `json:"generation"`
	N          uint64         `json:"n"`
	Lane       int            `json:"lane"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Name string `json:"name"`
	// Its collection; empty where the lane had no rows of it.
	Target string                `json:"target,omitempty"`
	Mode   destination.WriteMode `json:"mode"`
	Key    []string              `json:"key,omitempty"`
	Dedupe destination.Dedupe    `json:"dedupe,omitempty"`
	Hard   bool                  `json:"hard,omitempty"`
	Rows   int64                 `json:"rows,omitempty"`
	Read   string                `json:"read,omitempty"`
	Ended  bool                  `json:"ended,omitempty"`
	// Every lane's staging collections that have rows, once mergeLanes has
	// merged them.
	stages []string
}

func (s stagedStream) config() destination.Stream {
	d := destination.HardDelete
	if !s.Hard {
		d = destination.SoftDelete
	}
	return destination.Stream{Name: s.Name, Mode: s.Mode, PrimaryKey: s.Key, Dedupe: s.Dedupe, Deletes: d}
}

// into is where an epoch's rows go: an overwrite read's collection.
func (s stagedStream) into() string {
	if s.Mode == destination.Overwrite {
		return s.Read
	}
	return s.Target
}

func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	st := staged{Generation: e.Generation, N: e.N, Lane: w.cfg.Lane}
	lanes := w.epochs[e]
	delete(w.epochs, e)
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range lanes {
			if !yield(k) {
				return
			}
		}
	}) {
		ls, cfg := lanes[name], w.streams[name]
		ss := stagedStream{
			Name: name, Target: ls.target, Mode: cfg.Mode, Key: cfg.PrimaryKey, Dedupe: cfg.Dedupe,
			Hard: cfg.Deletes == destination.HardDelete, Rows: ls.rows, Read: ls.read, Ended: ls.ended,
		}
		st.Streams = append(st.Streams, ss)
	}
	return json.Marshal(st)
}

// mergeLanes parses every lane's committable, and merges what they say of
// each stream: its staging collections, in lane order.
func (w *writer) mergeLanes(e destination.Epoch, committables []destination.Staged) ([]stagedStream, error) {
	byName := map[string]*stagedStream{}
	var order []string
	for _, b := range committables {
		if len(b) == 0 {
			continue
		}
		var st staged
		if err := json.Unmarshal(b, &st); err != nil {
			return nil, fmt.Errorf("mongodb: a committable: %w", err)
		}
		if st.Generation != e.Generation || st.N != e.N {
			return nil, fmt.Errorf("mongodb: a committable of epoch %d.%d, not %d.%d", st.Generation, st.N, e.Generation, e.N)
		}
		for _, s := range st.Streams {
			m := byName[s.Name]
			if m == nil {
				m = &stagedStream{Name: s.Name, Mode: s.Mode, Key: s.Key, Dedupe: s.Dedupe, Hard: s.Hard, Read: s.Read}
				byName[s.Name] = m
				order = append(order, s.Name)
			}
			m.Target = cmpOr(m.Target, s.Target)
			m.Read = cmpOr(m.Read, s.Read)
			m.Ended = m.Ended || s.Ended
			m.Rows += s.Rows
			if s.Rows > 0 {
				m.stages = append(m.stages, stagingName(w.cfg.Pipeline, s.Name, e, st.Lane))
			}
		}
	}
	out := make([]stagedStream, 0, len(order))
	for _, name := range order {
		s := byName[name]
		if s.Target == "" {
			s.Target = cmpOr(w.target(name), targetCollection(arrow.NewSchema(nil, nil), name))
		}
		out = append(out, *s)
	}
	return out, nil
}

type marker = docstore.Marker

type fenceDoc struct {
	Generation uint64   `bson:"generation"`
	Applied    []marker `bson:"applied"`
}

func (w *writer) fence() *mongo.Collection { return w.db.Collection(fenceCollection) }

// Markers is the pipeline's fence: ErrFenced if a newer generation than
// the writer's has raised it.
func (w *writer) Markers(ctx context.Context) (map[string]marker, error) {
	var f fenceDoc
	err := w.fence().FindOne(ctx, bson.D{{Key: "_id", Value: w.cfg.Pipeline}}).Decode(&f)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, classify(fmt.Errorf("mongodb: read the fence: %w", err))
	}
	if f.Generation > w.cfg.Generation {
		return nil, destination.ErrFenced
	}
	out := map[string]marker{}
	for _, m := range f.Applied {
		out[m.Stream] = m
	}
	return out, nil
}

// SetMarkers replaces the streams' markers, unless a newer generation has
// raised the fence: one document's update, so it's either all or none.
func (w *writer) SetMarkers(ctx context.Context, ms []marker) error {
	names := make(bson.A, len(ms))
	for i, m := range ms {
		names[i] = m.Stream
	}
	update := mongo.Pipeline{{{Key: "$set", Value: bson.D{{Key: "applied", Value: bson.D{{Key: "$concatArrays", Value: bson.A{
		bson.D{{Key: "$filter", Value: bson.D{
			{Key: "input", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$applied", bson.A{}}}}},
			{Key: "cond", Value: bson.D{{Key: "$not", Value: bson.A{bson.D{{Key: "$in", Value: bson.A{"$$this.s", bson.D{{Key: "$literal", Value: names}}}}}}}}},
		}}},
		bson.D{{Key: "$literal", Value: ms}},
	}}}}}}}}
	res, err := w.fence().UpdateOne(ctx, bson.D{{Key: "_id", Value: w.cfg.Pipeline}, {Key: "generation", Value: bson.D{{Key: "$lte", Value: w.cfg.Generation}}}}, update)
	if err != nil {
		return classify(fmt.Errorf("mongodb: mark %s applied: %w", w.cfg.Pipeline, err))
	}
	if res.MatchedCount == 0 {
		return destination.ErrFenced
	}
	return nil
}

// Commit applies each stream whose marker isn't the epoch's yet: with
// ScopePipeline, every stream's rows and then their markers together, with
// ScopeStream, stream by stream.
func (w *writer) Commit(ctx context.Context, e destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	streams, err := w.mergeLanes(e, committables)
	if err != nil {
		return destination.Committed{}, err
	}
	return docstore.Settle(ctx, w, w.cfg.Scope, e, streams)
}

// Swap puts an ended overwrite read's collection, which apply created and
// gave the target's indexes, in its target's place, and then clears the
// marker's Swap. A read's collection that isn't there has taken its place
// already. Swap only renames, since any other command on the read would
// create it again, empty, once another swap had renamed it.
func (w *writer) Swap(ctx context.Context, m marker) error {
	if m.Swap == "" {
		return nil
	}
	err := w.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: w.db.Name() + "." + m.Swap},
		{Key: "to", Value: w.db.Name() + "." + m.Target},
		{Key: "dropTarget", Value: true},
	}).Err()
	if se, ok := errors.AsType[mongo.ServerError](err); err != nil && (!ok || !se.HasErrorCode(codeNamespaceNotFound)) {
		return classify(fmt.Errorf("mongodb: put %s in %s's place: %w", m.Swap, m.Target, err))
	}
	m.Swap = ""
	return w.SetMarkers(ctx, []marker{m})
}

// ensureRead creates an overwrite read's collection, with its target's
// validator and collation, unless it's there.
func (w *writer) ensureRead(ctx context.Context, read, target string) error {
	specs, err := w.db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$in", Value: bson.A{read, target}}}}})
	if err != nil {
		return classify(fmt.Errorf("mongodb: list collections of %s: %w", w.db.Name(), err))
	}
	cmd := bson.D{{Key: "create", Value: read}}
	for _, s := range specs {
		if s.Name == read {
			return nil
		}
		for _, k := range []string{"validator", "validationLevel", "validationAction", "collation"} {
			if v, err := s.Options.LookupErr(k); err == nil {
				cmd = append(cmd, bson.E{Key: k, Value: v})
			}
		}
	}
	err = w.db.RunCommand(ctx, cmd).Err()
	if se, ok := errors.AsType[mongo.ServerError](err); ok && se.HasErrorCode(codeNamespaceExists) {
		return nil
	}
	return classify(err)
}

// copyIndexes gives read the target's indexes but _id's.
func (w *writer) copyIndexes(ctx context.Context, target, read string) error {
	cur, err := w.db.Collection(target).Indexes().List(ctx)
	if se, ok := errors.AsType[mongo.ServerError](err); ok && se.HasErrorCode(codeNamespaceNotFound) {
		return nil
	}
	if err != nil {
		return classify(fmt.Errorf("mongodb: list %s's indexes: %w", target, err))
	}
	var specs []bson.Raw
	if err := cur.All(ctx, &specs); err != nil {
		return classify(fmt.Errorf("mongodb: list %s's indexes: %w", target, err))
	}
	var keep bson.A
	for _, s := range specs {
		if name, _ := s.Lookup("name").StringValueOK(); name != "_id_" {
			keep = append(keep, s)
		}
	}
	if len(keep) == 0 {
		return nil
	}
	return classify(w.db.RunCommand(ctx, bson.D{{Key: "createIndexes", Value: read}, {Key: "indexes", Value: keep}}).Err())
}

// Apply writes a stream's staged rows of every lane, and then its patches;
// an overwrite read that ended gets its target's indexes too, before its
// marker names it for the swap. collapsed: the rows dedupe skipped.
func (w *writer) Apply(ctx context.Context, s stagedStream) (collapsed uint64, err error) {
	cfg := s.config()
	coll := w.db.Collection(s.into())
	if s.Mode == destination.Overwrite {
		if err := w.ensureRead(ctx, s.Read, s.Target); err != nil {
			return 0, err
		}
		if s.Ended {
			if err := w.copyIndexes(ctx, s.Target, s.Read); err != nil {
				return 0, err
			}
		}
	}
	var patches []stagedRow
	var ops []mongo.WriteModel
	seen := map[string]bool{}
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		n, err := w.bulk(ctx, coll, ops, !keyed(cfg))
		ops = ops[:0]
		// Other inserts' _ids the collection has are an earlier apply's.
		if cfg.Dedupe == destination.DedupeTarget {
			collapsed += n
		}
		return err
	}
	for _, st := range s.stages {
		cur, err := w.db.Collection(st).Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			return collapsed, classify(fmt.Errorf("mongodb: read %s: %w", st, err))
		}
		for cur.Next(ctx) {
			var r stagedRow
			if err := cur.Decode(&r); err != nil {
				_ = cur.Close(ctx)
				return collapsed, err
			}
			if r.Patch {
				patches = append(patches, r)
				continue
			}
			if cfg.Dedupe == destination.DedupeEpoch {
				id := string(r.Doc.Lookup(idColumn).Value)
				if seen[id] {
					collapsed++
					continue
				}
				seen[id] = true
			}
			ops = append(ops, rowModel(cfg, r))
			if len(ops) >= writeBatch {
				if err := flush(); err != nil {
					_ = cur.Close(ctx)
					return collapsed, err
				}
			}
		}
		err = cur.Err()
		_ = cur.Close(ctx)
		if err != nil {
			return collapsed, classify(fmt.Errorf("mongodb: read %s: %w", st, err))
		}
	}
	if err := flush(); err != nil {
		return collapsed, err
	}
	if len(patches) == 0 {
		return collapsed, nil
	}
	if !keyed(cfg) {
		if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: idColumn, Value: 1}}}); err != nil {
			return collapsed, classify(fmt.Errorf("mongodb: index %s's %s: %w", coll.Name(), idColumn, err))
		}
	}
	for _, p := range patches {
		ops = append(ops, patchModel(cfg, p))
	}
	_, err = w.bulk(ctx, coll, ops, false)
	return collapsed, err
}

// bulk writes ops: inserts in any order, skipping, and counting, those of
// _ids the collection has, which an earlier apply wrote; other writes in
// order, so that a key's versions apply in order.
func (w *writer) bulk(ctx context.Context, coll *mongo.Collection, ops []mongo.WriteModel, inserts bool) (skipped uint64, err error) {
	_, err = coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(!inserts))
	be, ok := errors.AsType[mongo.BulkWriteException](err)
	if !ok || !inserts || be.WriteConcernError != nil {
		return 0, classify(err)
	}
	for _, we := range be.WriteErrors {
		if we.Code != codeDuplicateKey || !onID(we.WriteError) {
			return skipped, classify(err)
		}
		skipped++
	}
	return skipped, nil
}

// onID reports whether a duplicate key is the _id index's, not one a user's
// unique index found.
func onID(we mongo.WriteError) bool {
	if kp, ok := we.Raw.Lookup("keyPattern").DocumentOK(); ok {
		elems, _ := kp.Elements()
		return len(elems) == 1 && elems[0].Key() == "_id"
	}
	return strings.Contains(we.Message, " index: _id_ ")
}

// rowModel is the write that applies a staged row: an insert, or for a keyed
// stream, an upsert or delete that leaves a document with a higher
// _openetl_seq as it is. A soft delete keeps the document's other fields,
// held ones too, and updates its metadata. A merged row's kept columns keep
// the document's values and holds.
func rowModel(cfg destination.Stream, r stagedRow) mongo.WriteModel {
	if !keyed(cfg) {
		return mongo.NewInsertOneModel().SetDocument(r.Doc)
	}
	key := r.Doc.Lookup("_id")
	seq := r.Doc.Lookup(seqColumn)
	op, _ := r.Doc.Lookup(opColumn).StringValueOK()
	guard := func(expr any) any {
		if seq.Type == 0 {
			return expr
		}
		return bson.D{{Key: "$cond", Value: bson.A{bson.D{{Key: "$gt", Value: bson.A{"$" + seqColumn, seq}}}, "$$ROOT", expr}}}
	}
	upsert := func(expr any) mongo.WriteModel {
		return mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "_id", Value: key}}).SetUpsert(true).
			SetUpdate(mongo.Pipeline{{{Key: "$replaceWith", Value: guard(expr)}}})
	}
	if op == "delete" {
		if cfg.Deletes == destination.HardDelete {
			filter := bson.D{{Key: "_id", Value: key}}
			if seq.Type != 0 {
				filter = append(filter, bson.E{Key: seqColumn, Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: seq}}}}})
			}
			return mongo.NewDeleteOneModel().SetFilter(filter)
		}
		deleted, meta := bson.D{}, bson.D{}
		elems, _ := r.Doc.Elements()
		for _, e := range elems {
			if e.Key() == deletedColumn {
				continue
			}
			deleted = append(deleted, bson.E{Key: e.Key(), Value: e.Value()})
			switch e.Key() {
			case idColumn, opColumn, seqColumn, syncedColumn:
				meta = append(meta, bson.E{Key: e.Key(), Value: e.Value()})
			}
		}
		deleted = append(deleted, bson.E{Key: deletedColumn, Value: true})
		meta = append(meta, bson.E{Key: deletedColumn, Value: true})
		return upsert(bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$size", Value: bson.D{{Key: "$objectToArray", Value: "$$ROOT"}}}}, 1}}},
			bson.D{{Key: "$literal", Value: deleted}},
			bson.D{{Key: "$mergeObjects", Value: bson.A{"$$ROOT", bson.D{{Key: "$literal", Value: meta}}}}},
		}}})
	}
	if len(r.Keep) == 0 {
		return upsert(bson.D{{Key: "$literal", Value: r.Doc}})
	}
	rest := bson.D{}
	var held any = bson.D{}
	elems, _ := r.Doc.Elements()
	for _, e := range elems {
		switch {
		case e.Key() == heldColumn:
			held = e.Value()
		case !slices.Contains(r.Keep, e.Key()):
			rest = append(rest, bson.E{Key: e.Key(), Value: e.Value()})
		}
	}
	// The target's fields of the kept columns, and its holds on them, which
	// the row's own holds override, so that a held version's patch still
	// finds its hold on a column the row didn't send.
	keptOf := func(doc any) bson.D {
		return bson.D{{Key: "$arrayToObject", Value: bson.D{{Key: "$filter", Value: bson.D{
			{Key: "input", Value: bson.D{{Key: "$objectToArray", Value: doc}}},
			{Key: "cond", Value: bson.D{{Key: "$in", Value: bson.A{"$$this.k", bson.D{{Key: "$literal", Value: r.Keep}}}}}},
		}}}}}
	}
	holds := bson.D{{Key: "$mergeObjects", Value: bson.A{keptOf(bson.D{{Key: "$ifNull", Value: bson.A{"$" + heldColumn, bson.D{}}}}), bson.D{{Key: "$literal", Value: held}}}}}
	heldField := bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "h", Value: holds}}},
		{Key: "in", Value: bson.D{{Key: "$cond", Value: bson.A{bson.D{{Key: "$eq", Value: bson.A{"$$h", bson.D{{Key: "$literal", Value: bson.D{}}}}}}, "$$REMOVE", "$$h"}}}},
	}}}
	return upsert(bson.D{{Key: "$mergeObjects", Value: bson.A{
		bson.D{{Key: "$literal", Value: rest}}, keptOf("$$ROOT"), bson.D{{Key: heldColumn, Value: heldField}},
	}}})
}

// patchModel fills each column a patch has, in the document its key, or its
// version for append, names, where that column is still held by the
// patch's version, and clears the hold.
func patchModel(cfg destination.Stream, r stagedRow) mongo.WriteModel {
	version := r.Doc.Lookup("v")
	var stages mongo.Pipeline
	elems, _ := r.Doc.Lookup("f").Document().Elements()
	for _, e := range elems {
		col := bson.D{{Key: "$literal", Value: e.Key()}}
		val := any(bson.D{{Key: "$literal", Value: e.Value()}})
		if e.Value().Type == bson.TypeNull {
			val = "$$REMOVE"
		}
		heldBy := bson.D{{Key: "$getField", Value: bson.D{{Key: "field", Value: col}, {Key: "input", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$" + heldColumn, bson.D{}}}}}}}}
		filled := bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: heldColumn},
			{Key: "input", Value: bson.D{{Key: "$setField", Value: bson.D{{Key: "field", Value: col}, {Key: "input", Value: "$$ROOT"}, {Key: "value", Value: val}}}}},
			{Key: "value", Value: bson.D{{Key: "$setField", Value: bson.D{{Key: "field", Value: col}, {Key: "input", Value: "$" + heldColumn}, {Key: "value", Value: "$$REMOVE"}}}}},
		}}}
		stages = append(stages, bson.D{{Key: "$replaceWith", Value: bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{heldBy, version}}}, filled, "$$ROOT",
		}}}}})
	}
	if keyed(cfg) {
		return mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "_id", Value: r.Doc.Lookup("k")}}).SetUpdate(stages)
	}
	return mongo.NewUpdateManyModel().SetFilter(bson.D{{Key: idColumn, Value: version}}).SetUpdate(stages)
}

// Recover raises the fence to gen, then settles e: Applied where every
// stream's marker is e's, Redone after applying the streams whose marker
// isn't from their staging, Lost where their staging is gone. It then drops
// what earlier generations staged.
func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	_, err := w.fence().UpdateOne(ctx,
		bson.D{{Key: "_id", Value: w.cfg.Pipeline}, {Key: "generation", Value: bson.D{{Key: "$lte", Value: gen}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "generation", Value: gen}}}}, options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		return 0, destination.ErrFenced
	}
	if err != nil {
		return 0, classify(fmt.Errorf("mongodb: raise the fence: %w", err))
	}
	rec := destination.Applied
	if !e.IsZero() {
		if rec, err = w.settleRecorded(ctx, e, committables); err != nil {
			return 0, err
		}
	}
	return rec, w.dropStale(ctx, gen)
}

func (w *writer) settleRecorded(ctx context.Context, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	streams, err := w.mergeLanes(e, committables)
	if err != nil {
		return 0, err
	}
	return docstore.SettleRecorded(ctx, "mongodb", w, w.cfg.Scope, e, streams)
}

func (w *writer) Mark(s stagedStream, e destination.Epoch) marker {
	return docstore.Mark(e, s.Name, s.Mode == destination.Overwrite && s.Ended, s.Read, s.Target)
}

func (w *writer) DropStaging(ctx context.Context, s stagedStream) error {
	for _, st := range s.stages {
		if err := w.db.Collection(st).Drop(ctx); err != nil {
			return classify(fmt.Errorf("mongodb: drop %s: %w", st, err))
		}
	}
	return nil
}

func (w *writer) Staging(ctx context.Context, s stagedStream) (present, missing int, err error) {
	if len(s.stages) == 0 {
		return 0, 0, nil
	}
	names, err := w.db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$regex", Value: "^" + stagingPrefix(w.cfg.Pipeline)}}}})
	if err != nil {
		return 0, 0, classify(fmt.Errorf("mongodb: list collections of %s: %w", w.db.Name(), err))
	}
	for _, st := range s.stages {
		if slices.Contains(names, st) {
			present++
		} else {
			missing++
		}
	}
	return present, missing, nil
}

// dropStale drops what generations before gen staged.
func (w *writer) dropStale(ctx context.Context, gen uint64) error {
	prefix := stagingPrefix(w.cfg.Pipeline)
	names, err := w.db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$regex", Value: "^" + prefix}}}})
	if err != nil {
		return classify(fmt.Errorf("mongodb: list collections of %s: %w", w.db.Name(), err))
	}
	for _, n := range names {
		g, _, _ := strings.Cut(strings.TrimPrefix(n, prefix), "_")
		if v, err := strconv.ParseUint(g, 10, 64); err == nil && v < gen {
			if err := w.db.Collection(n).Drop(ctx); err != nil {
				return classify(fmt.Errorf("mongodb: drop %s: %w", n, err))
			}
		}
	}
	return nil
}

func (w *writer) Close(ctx context.Context) error {
	return w.client.Disconnect(ctx)
}
