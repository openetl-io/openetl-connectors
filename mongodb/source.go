package mongodb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl-connectors/docstore"
	"github.com/openetl-io/openetl-connectors/keysplit"
	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	// A MongoDB connection string.
	URI sdk.Secret `json:"uri"`
	// The database whose collections are offered as streams.
	Database string `json:"database"`
	// The collections to offer. Empty: every collection in the database.
	Collections []string `json:"collections"`
	// How many documents of each collection discovery reads, at random, to
	// find its fields and their types. 0: every document.
	SampleSize int `json:"sampleSize"`
	// Documents per query; each chunk ends at a resume point. Default 10000.
	ChunkDocuments int `json:"chunkDocuments"`
	// Each stream is _id and data, the whole document in canonical Extended
	// JSON, instead of a column per top-level field; discovery reads no
	// documents.
	Packed bool `json:"packed"`
}

// dataColumn is a packed stream's document.
const dataColumn = "data"

// Source reads a database's collections, each a stream keyed by _id: a full
// refresh reads its documents in _id order, a chunk per query, an
// incremental read those whose cursor field is at or past the last read's
// highest, and cdc its snapshot and then its change stream (cdc.go). A
// collection read in splits is read in _id ranges (source_splits.go).
type Source struct {
	cfg SourceConfig
	// From uri, as parsed when the source was made.
	opts *options.ClientOptions

	keysplit.Reads
}

var (
	_ source.Source            = (*Source)(nil)
	_ source.Splitter          = (*Source)(nil)
	_ source.Resplitter        = (*Source)(nil)
	_ source.RetentionReporter = (*Source)(nil)
)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{ChunkDocuments: 10_000}
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
	case cfg.SampleSize < 0 || cfg.ChunkDocuments <= 0:
		return nil, errors.New("mongodb: sampleSize can't be negative, and chunkDocuments must be positive")
	}
	opts, err := clientOptions(cfg.URI.Reveal())
	if err != nil {
		return nil, err
	}
	return &Source{cfg: cfg, opts: opts}, nil
}

// client connects for one call; done disconnects it.
func (s *Source) client(ctx context.Context) (c *mongo.Client, done func(), err error) {
	c, err = connect(s.cfg.URI.Reveal())
	if err != nil {
		return nil, nil, err
	}
	return c, func() { _ = c.Disconnect(context.WithoutCancel(ctx)) }, nil
}

func (s *Source) db(c *mongo.Client) *mongo.Database { return c.Database(s.cfg.Database) }

// listCollections are the database's collections offered as streams: not
// views, time series or system collections, which have no _id order to read
// in.
func (s *Source) listCollections(ctx context.Context, db *mongo.Database) ([]string, error) {
	names, err := db.ListCollectionNames(ctx, bson.D{{Key: "type", Value: "collection"}})
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: list collections of %s: %w", db.Name(), err))
	}
	names = slices.DeleteFunc(names, func(n string) bool {
		return strings.HasPrefix(n, "system.") || len(s.cfg.Collections) > 0 && !slices.Contains(s.cfg.Collections, n)
	})
	slices.Sort(names)
	return names, nil
}

// The type of a cdc stream's seq: a change's cluster time, its seconds and
// increment, times 2^32, plus its place among the changes of that time.
var seqType = docstore.SeqType

// Discover offers each collection with _id as keyText spells it, and the
// other top-level fields its documents have, each declared as fieldType says
// from the types its values have, in every document unless sampleSize says
// how many to read; packed, with _id and data.
func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	c, done, err := s.client(ctx)
	if err != nil {
		return source.Catalog{}, err
	}
	defer done()
	db := s.db(c)
	names, err := s.listCollections(ctx, db)
	if err != nil {
		return source.Catalog{}, err
	}
	changes, err := changeStreams(ctx, c)
	if err != nil {
		return source.Catalog{}, err
	}
	var cat source.Catalog
	for _, name := range names {
		fields := []arrow.Field{{Name: "_id", Type: arrow.BinaryTypes.String}}
		if s.cfg.Packed {
			fields = append(fields, arrow.Field{Name: dataColumn, Type: jsonType, Nullable: true})
		} else {
			types, err := s.fieldTypes(ctx, db.Collection(name))
			if err != nil {
				return source.Catalog{}, err
			}
			for _, f := range slices.Sorted(func(yield func(string) bool) {
				for k := range types {
					if k != "_id" && !yield(k) {
						return
					}
				}
			}) {
				fields = append(fields, arrow.Field{Name: f, Type: fieldType(types[f]), Nullable: true})
			}
		}
		cs := source.CatalogStream{
			Name: name, PrimaryKey: []string{"_id"}, SplitsByKey: true, Replay: source.Exact,
			Schema:    arrow.NewSchema(fields, nil),
			SyncModes: []source.SyncMode{source.FullRefresh, source.Incremental},
		}
		if changes {
			cs.SyncModes, cs.Seq = append(cs.SyncModes, source.CDC), seqType
		}
		cat.Streams = append(cat.Streams, cs)
	}
	return cat, nil
}

// fieldTypes are the BSON types of each top-level field's values, as $type
// names them, with "uuid" for 16-byte binaries of subtype 4: a binary's
// length, then its subtype, orders it.
func (s *Source) fieldTypes(ctx context.Context, coll *mongo.Collection) (map[string][]string, error) {
	var lo, hi [16]byte
	for i := range hi {
		hi[i] = 0xff
	}
	typ := bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "t", Value: bson.D{{Key: "$type", Value: "$f.v"}}}}},
		{Key: "in", Value: bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$and", Value: bson.A{
				bson.D{{Key: "$eq", Value: bson.A{"$$t", "binData"}}},
				bson.D{{Key: "$gte", Value: bson.A{"$f.v", bson.Binary{Subtype: bson.TypeBinaryUUID, Data: lo[:]}}}},
				bson.D{{Key: "$lte", Value: bson.A{"$f.v", bson.Binary{Subtype: bson.TypeBinaryUUID, Data: hi[:]}}}},
			}}},
			"uuid", "$$t",
		}}}},
	}}}
	var pipeline mongo.Pipeline
	if s.cfg.SampleSize > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: s.cfg.SampleSize}}}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "f", Value: bson.D{{Key: "$objectToArray", Value: "$$ROOT"}}}}}},
		bson.D{{Key: "$unwind", Value: "$f"}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$f.k"}, {Key: "t", Value: bson.D{{Key: "$addToSet", Value: typ}}}}}},
	)
	cur, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: discover %s: %w", coll.Name(), err))
	}
	var rows []struct {
		Field string   `bson:"_id"`
		Types []string `bson:"t"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, classify(fmt.Errorf("mongodb: discover %s: %w", coll.Name(), err))
	}
	out := map[string][]string{}
	for _, r := range rows {
		slices.Sort(r.Types)
		out[r.Field] = r.Types
	}
	return out, nil
}

// changeStreams reports whether the deployment has change streams: a replica
// set's members and a sharded cluster's routers do.
func changeStreams(ctx context.Context, c *mongo.Client) (bool, error) {
	var hello struct {
		SetName string `bson:"setName"`
		Msg     string `bson:"msg"`
	}
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return false, classify(fmt.Errorf("mongodb: hello: %w", err))
	}
	return hello.SetName != "" || hello.Msg == "isdbgrid", nil
}

// Read reads full refresh and incremental streams through ReadSplits, and
// each cdc stream on a goroutine of its own, since a continuous one never
// ends.
func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	c, done, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer done()
	db := s.db(c)
	var follow []source.ConfiguredStream
	rest := req
	rest.Streams = nil
	for _, cs := range req.Streams {
		if cs.SyncMode == source.CDC {
			follow = append(follow, cs)
		} else {
			rest.Streams = append(rest.Streams, cs)
		}
	}
	// Where a bounded read's change streams end.
	var end bson.Timestamp
	if len(follow) > 0 && !req.Continuous {
		if end, err = clusterTime(ctx, c); err != nil {
			return err
		}
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, cs := range follow {
		g.Go(func() error {
			r := &cdcReader{s: s, db: db, cs: cs, out: out.Split(cs.Name, source.WholeStream), continuous: req.Continuous, end: end}
			if p, ok := out.(source.Progresser); ok {
				r.progress = p
			}
			return r.read(ctx)
		})
	}
	if len(rest.Streams) > 0 {
		g.Go(func() error {
			return source.ReadSplits(ctx, rest, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
				return s.readSplit(ctx, db, cs, sp, out)
			})
		})
	}
	return g.Wait()
}

// readState is where a full refresh or incremental read of a split is.
type readState struct {
	// The split's _id range: from Lo, below Hi, in the _id index's order,
	// which orders values of different types too; nil: no bound.
	Lo *value `json:"lo,omitempty"`
	Hi *value `json:"hi,omitempty"`
	// The last _id read; nil before the first.
	After *value `json:"after,omitempty"`
	// An incremental read's cursor values: from Since, up to and including
	// Max, the highest when the read started; nil Since: from the lowest,
	// nil Max: not found yet.
	Since *value `json:"since,omitempty"`
	Max   *value `json:"max,omitempty"`
}

func parseState[T any](stream string, b []byte) (T, error) {
	var st T
	if len(b) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("mongodb: %s state: %w", stream, err))
	}
	return st, nil
}

// readSplit reads a split's documents a chunk at a time, with a resume
// point after each but the last. An incremental read takes those with a
// cursor value in its range, which compares as MongoDB compares a field to a
// value: values of other types than Max's don't match. With an until, it
// stops at that resume point's document.
func (s *Source) readSplit(ctx context.Context, db *mongo.Database, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
	coll := db.Collection(cs.Name)
	st, err := parseState[readState](cs.Name, sp.State)
	if err != nil {
		return err
	}
	if sp.ID == source.WholeStream && cs.Splits == nil {
		// ReadSplits doesn't carry the whole stream's until.
		sp.Until = cs.Until
	}
	var until *readState
	if sp.Until != nil {
		u, err := parseState[readState](cs.Name, sp.Until)
		if err != nil {
			return err
		}
		until = &u
	}
	filter := bson.D{}
	if cs.SyncMode == source.Incremental {
		if cs.Cursor == "" {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("mongodb: %s is read incrementally, and needs a cursor field", cs.Name))
		}
		if st.Max == nil {
			if st.Max, err = maxCursor(ctx, coll, cs.Cursor); err != nil {
				return err
			}
			if st.Max == nil {
				next, err := json.Marshal(readState{Since: st.Since})
				if err != nil {
					return err
				}
				return out.Complete(next)
			}
		}
		cond := bson.D{{Key: "$lte", Value: st.Max.RawValue}}
		if st.Since != nil {
			// At Since, not after it: documents with the last read's highest
			// value may have come since, and are read again.
			cond = append(bson.D{{Key: "$gte", Value: st.Since.RawValue}}, cond...)
		}
		filter = bson.D{{Key: cs.Cursor, Value: cond}}
	}
	cols, err := s.knownFields(ctx, coll, cs)
	if err != nil {
		return err
	}
	b := s.newBatcher(cs, cols, false)
	defer b.Release()
	var r *keysplit.Reading
	if until == nil {
		r = s.Start(cs.Name, sp.ID, true)
		defer s.End(cs.Name, sp.ID)
	}
	for {
		hi := st.Hi
		if until != nil {
			hi = until.After
		}
		got, last, err := s.readChunk(ctx, coll, filter, st.Lo, st.After, hi, func(doc bson.Raw) error {
			return b.addFlushing(doc, "", seq{}, out)
		})
		if err == nil {
			err = b.Flush(out)
		}
		if err != nil {
			return err
		}
		if last != nil {
			st.After = last
		}
		if got < s.cfg.ChunkDocuments {
			break
		}
		var added []source.Split
		if r != nil {
			if added, err = s.resplit(ctx, coll, r, &st); err != nil {
				return err
			}
		}
		state, err := json.Marshal(st)
		if err != nil {
			return err
		}
		if err := out.ResumePoint(state, added...); err != nil {
			return err
		}
	}
	if r != nil {
		r.Over.Store(true)
	}
	if until != nil {
		return s.endAt(ctx, coll, filter, until.After, sp.Until, b, "", seq{}, out)
	}
	var next []byte
	if cs.SyncMode == source.Incremental {
		if next, err = json.Marshal(readState{Since: st.Max}); err != nil {
			return err
		}
	}
	return out.Complete(next)
}

// endAt finishes a read that stops at an until: the document with its last
// _id, if it's still there, then a resume point and the completion with
// until's state.
func (s *Source) endAt(ctx context.Context, coll *mongo.Collection, filter bson.D, after *value, state []byte, b *batcher, op string, q seq, out source.SplitEmitter) error {
	if after != nil {
		doc, err := coll.FindOne(ctx, append(bson.D{{Key: "_id", Value: after.RawValue}}, filter...)).Raw()
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
		case err != nil:
			return classify(fmt.Errorf("mongodb: read %s: %w", coll.Name(), err))
		default:
			if err := b.add(doc, op, q); err != nil {
				return err
			}
			if err := b.Flush(out); err != nil {
				return err
			}
		}
	}
	if err := out.ResumePoint(state); err != nil {
		return err
	}
	return out.Complete(state)
}

// readChunk reads up to chunkDocuments documents matching filter in _id
// order: from lo, or after after, below hi. got counts them, and last is the
// last one's _id.
func (s *Source) readChunk(ctx context.Context, coll *mongo.Collection, filter bson.D, lo, after, hi *value, each func(bson.Raw) error) (got int, last *value, err error) {
	if after != nil {
		lo = after
	}
	// Nothing is past MaxKey or below MinKey, and min and max can't bound
	// at them.
	if lo != nil && lo.Type == bson.TypeMaxKey || hi != nil && hi.Type == bson.TypeMinKey {
		return 0, nil, nil
	}
	if filter == nil {
		filter = bson.D{}
	}
	limit := s.cfg.ChunkDocuments
	if after != nil {
		limit++
	}
	opts := options.Find().SetHint(bson.D{{Key: "_id", Value: 1}}).SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit))
	// min and max bound the _id index's keys, across types, as $gt and $lt
	// don't: a comparison matches values of one type alone. min is
	// inclusive, so after's document comes first, if it's still there.
	if lo != nil {
		opts.SetMin(bson.D{{Key: "_id", Value: lo.RawValue}})
	}
	if hi != nil {
		opts.SetMax(bson.D{{Key: "_id", Value: hi.RawValue}})
	}
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return 0, nil, classify(fmt.Errorf("mongodb: read %s: %w", coll.Name(), err))
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	first := true
	for got < s.cfg.ChunkDocuments && cur.Next(ctx) {
		id := cur.Current.Lookup("_id")
		if first && after != nil && id.Equal(after.RawValue) {
			first = false
			continue
		}
		first = false
		if err := each(cur.Current); err != nil {
			return got, last, err
		}
		got++
		last = &value{bson.RawValue{Type: id.Type, Value: slices.Clone(id.Value)}}
	}
	if err := cur.Err(); err != nil {
		return got, last, classify(fmt.Errorf("mongodb: read %s: %w", coll.Name(), err))
	}
	// A min without a max ends below MaxKey: a document whose _id is
	// MaxKey ends the last range.
	if hi == nil && lo != nil && got < s.cfg.ChunkDocuments {
		doc, err := coll.FindOne(ctx, append(bson.D{{Key: "_id", Value: bson.MaxKey{}}}, filter...)).Raw()
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
		case err != nil:
			return got, last, classify(fmt.Errorf("mongodb: read %s: %w", coll.Name(), err))
		default:
			if err := each(doc); err != nil {
				return got, last, err
			}
			got++
			last = &value{bson.RawValue{Type: bson.TypeMaxKey}}
		}
	}
	return got, last, nil
}

// maxCursor is the highest value of a collection's field, nil if no document
// has one; an index on the field makes it one lookup.
func maxCursor(ctx context.Context, coll *mongo.Collection, field string) (*value, error) {
	doc, err := coll.FindOne(ctx, bson.D{{Key: field, Value: bson.D{{Key: "$exists", Value: true}, {Key: "$ne", Value: nil}}}},
		options.FindOne().SetSort(bson.D{{Key: field, Value: -1}}).SetProjection(bson.D{{Key: field, Value: 1}})).Raw()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: read %s's highest %s: %w", coll.Name(), field, err))
	}
	v, err := doc.LookupErr(field)
	if err != nil {
		return nil, fmt.Errorf("mongodb: %s's highest %s: %w", coll.Name(), field, err)
	}
	return &value{bson.RawValue{Type: v.Type, Value: slices.Clone(v.Value)}}, nil
}

// knownFields gives a read whose request doesn't say its columns, as a
// tool's doesn't, the fields the collection's documents have now, so that
// its batches have the same columns whichever documents they hold.
func (s *Source) knownFields(ctx context.Context, coll *mongo.Collection, cs source.ConfiguredStream) ([]string, error) {
	if s.cfg.Packed {
		return []string{dataColumn}, nil
	}
	if cs.Columns != nil {
		return cs.Columns, nil
	}
	types, err := s.fieldTypes(ctx, coll)
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(types)), nil
}

// batchBytes is about how many bytes of text a batch holds.
const batchBytes = 4 << 20

// seq is a cdc record's position: its cluster time, and its place among the
// records of that time.
type seq struct {
	t     bson.Timestamp
	place uint32
}

// decimal is the seq as seqType holds it.
func (q seq) decimal() decimal128.Num {
	x := uint64(q.t.T)<<32 | uint64(q.t.I)
	return decimal128.New(int64(x>>32), x<<32|uint64(q.place))
}

// batcher builds a stream's batches: its fields as text, and for cdc its op
// and seq. With the run's columns, every batch has each of them, and a
// document with another field fails the read, retryably, so the next
// attempt's discovery declares it; without, every batch has the columns it
// starts with, and those documents bring since. Packed, a batch is _id and
// data.
type batcher struct {
	docstore.Batch
	stream   string
	declared bool
	changes  bool
	packed   bool
	// Discovery's sample size; 0: it reads every document.
	sampled int
	// For a field the run doesn't declare: whether the collection's
	// documents no longer have it, so that the next discovery won't declare
	// it either, and the row leaves it out. Nil: none are.
	gone func(field string) (bool, error)
}

// newBatcher makes a batcher of columns, which are the run's where cs has
// them.
func (s *Source) newBatcher(cs source.ConfiguredStream, columns []string, changes bool) *batcher {
	b := &batcher{stream: cs.Name, changes: changes, declared: cs.Columns != nil, packed: s.cfg.Packed, sampled: s.cfg.SampleSize}
	b.Batch = docstore.NewBatch(changes)
	b.Column("_id")
	for _, c := range columns {
		if !strings.HasPrefix(c, "openetl.") {
			b.Column(c)
		}
	}
	return b
}

// add appends a document's row; only declared fields of a delete's, whose
// values only say what it deleted.
func (b *batcher) add(doc bson.Raw, op string, q seq) error {
	if b.packed {
		return b.addPacked(doc, true, op, q)
	}
	elems, err := doc.Elements()
	if err != nil {
		return fmt.Errorf("mongodb: a document of %s: %w", b.stream, err)
	}
	// Each element's column, or -1 to leave it out, settled before any is
	// added, so that a failure leaves the batch as it was.
	at := make([]int, len(elems))
	for j, e := range elems {
		k := e.Key()
		i, ok := b.Index[k]
		switch {
		case ok:
		case op == source.OpDelete:
			i = -1
		case b.declared:
			gone := false
			if b.gone != nil {
				if gone, err = b.gone(k); err != nil {
					return err
				}
			}
			if !gone {
				return b.newField(k)
			}
			i = -1
		default:
			i = b.Column(k)
		}
		at[j] = i
	}
	set := make([]bool, len(b.Cols))
	for j, e := range elems {
		i := at[j]
		if i < 0 || set[i] {
			continue
		}
		set[i] = true
		t, ok := text(e.Value())
		if e.Key() == "_id" {
			t, ok = keyText(e.Value()), true
		}
		if ok {
			b.Cols[i].Append(t)
			b.Size += len(t)
		} else {
			b.Cols[i].AppendNull()
		}
	}
	for i, done := range set {
		if !done {
			b.Cols[i].AppendNull()
		}
	}
	b.end(op, q)
	return nil
}

// newField is the error of a field the run doesn't declare. Discovery is
// given the config alone, and the source keeps nothing between calls and
// writes nothing upstream, so nothing tells the next discovery about the
// field: one that reads every document finds it, and a sample can miss it
// again.
func (b *batcher) newField(k string) error {
	err := fmt.Errorf("mongodb: %s has a new field %q; the read starts again so that discovery declares it", b.stream, k)
	if b.sampled > 0 {
		err = fmt.Errorf("%w, but discovery reads a sample of %d documents, which can miss it again: set sampleSize to 0 to read every document, or packed to read documents whole", err, b.sampled)
	}
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
}

// addPacked appends a packed row: data is the document in canonical
// Extended JSON, or null where whole is false.
func (b *batcher) addPacked(doc bson.Raw, whole bool, op string, q seq) error {
	id, err := doc.LookupErr("_id")
	if err != nil {
		return fmt.Errorf("mongodb: a document of %s has no _id: %w", b.stream, err)
	}
	var data []byte
	if whole {
		if data, err = bson.MarshalExtJSON(doc, true, false); err != nil {
			return fmt.Errorf("mongodb: a document of %s: %w", b.stream, err)
		}
	}
	k := keyText(id)
	b.Cols[0].Append(k)
	if data == nil {
		b.Cols[1].AppendNull()
	} else {
		b.Cols[1].Append(string(data))
	}
	b.Size += len(k) + len(data)
	b.end(op, q)
	return nil
}

// addKey appends a delete's row from its key alone.
func (b *batcher) addKey(key bson.RawValue, q seq) error {
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: key}})
	if err != nil {
		return err
	}
	if b.packed {
		return b.addPacked(doc, false, source.OpDelete, q)
	}
	return b.add(doc, source.OpDelete, q)
}

func (b *batcher) end(op string, q seq) {
	if b.changes {
		b.Op.Append(op)
		b.Seq.Append(q.decimal())
	}
	b.Rows++
}

// addFlushing adds a row, and flushes once the batch is full.
func (b *batcher) addFlushing(doc bson.Raw, op string, q seq, out source.SplitEmitter) error {
	if err := b.add(doc, op, q); err != nil {
		return err
	}
	if b.Size < batchBytes {
		return nil
	}
	return b.Flush(out)
}
