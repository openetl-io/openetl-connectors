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
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

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
	// The most aggregations discovery runs at once, each over a collection
	// or a range of its _ids. Default 4.
	DiscoveryConnections int `json:"discoveryConnections"`
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
	// Holds a unit for each discovery aggregation running, over every call,
	// as splits each find their fields where a read doesn't name them.
	discovering *semaphore.Weighted
	// The server's version, read by the first discovery that needs it; nil
	// before.
	versionMu sync.Mutex
	version   *serverVersion

	keysplit.Reads
}

var (
	_ source.Source            = (*Source)(nil)
	_ source.Splitter          = (*Source)(nil)
	_ source.Resplitter        = (*Source)(nil)
	_ source.RetentionReporter = (*Source)(nil)
)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{ChunkDocuments: 10_000, DiscoveryConnections: 4}
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
	case cfg.SampleSize < 0 || cfg.ChunkDocuments <= 0 || cfg.DiscoveryConnections <= 0:
		return nil, errors.New("mongodb: sampleSize can't be negative, and chunkDocuments and discoveryConnections must be positive")
	}
	opts, err := clientOptions(cfg.URI.Reveal())
	if err != nil {
		return nil, err
	}
	return &Source{cfg: cfg, opts: opts, discovering: semaphore.NewWeighted(int64(cfg.DiscoveryConnections))}, nil
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
	var types []map[string][]string
	if !s.cfg.Packed {
		if types, err = s.fieldTypes(ctx, db, names); err != nil {
			return source.Catalog{}, err
		}
	}
	var cat source.Catalog
	for i, name := range names {
		fields := []arrow.Field{{Name: "_id", Type: arrow.BinaryTypes.String}}
		if s.cfg.Packed {
			fields = append(fields, arrow.Field{Name: dataColumn, Type: jsonType, Nullable: true})
		} else {
			for _, f := range slices.Sorted(maps.Keys(types[i])) {
				if f != "_id" {
					fields = append(fields, arrow.Field{Name: f, Type: fieldType(types[i][f]), Nullable: true})
				}
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

// fieldTypes are, for each collection named, the BSON types of each
// top-level field's values, as $type names them, with "uuid" for 16-byte
// binaries of subtype 4: from every document, or from sampleSize of them.
// Up to discoveryConnections aggregations run at once, over every
// collection's parts and every call's: where more than one may, a
// collection that isn't sampled, and whose ranges the server bounds, is read
// in the _id ranges cuts gives it, and else whole.
func (s *Source) fieldTypes(ctx context.Context, db *mongo.Database, names []string) ([]map[string][]string, error) {
	type part struct {
		coll *mongo.Collection
		// The collection's index in names.
		of     int
		lo, hi *value
	}
	var ranged map[string]bool
	if s.cfg.SampleSize == 0 && s.cfg.DiscoveryConnections > 1 {
		var err error
		if ranged, err = s.ranged(ctx, db, names); err != nil {
			return nil, err
		}
	}
	var parts []part
	for i, name := range names {
		coll := db.Collection(name)
		var cuts []*value
		if ranged[name] {
			var err error
			if cuts, err = s.cuts(ctx, coll); err != nil {
				return nil, err
			}
		}
		var lo *value
		for _, hi := range append(cuts, nil) {
			parts = append(parts, part{coll: coll, of: i, lo: lo, hi: hi})
			lo = hi
		}
	}
	found := make([]map[string][]string, len(parts))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.cfg.DiscoveryConnections)
	for j, p := range parts {
		g.Go(func() (err error) {
			if err := s.discovering.Acquire(gctx, 1); err != nil {
				return err
			}
			defer s.discovering.Release(1)
			found[j], err = s.rangeTypes(gctx, p.coll, p.lo, p.hi)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := make([]map[string][]string, len(names))
	for i := range out {
		out[i] = map[string][]string{}
	}
	for j, p := range parts {
		for f, ts := range found[j] {
			out[p.of][f] = append(out[p.of][f], ts...)
		}
	}
	for _, types := range out {
		for f, ts := range types {
			slices.Sort(ts)
			types[f] = slices.Compact(ts)
		}
	}
	return out, nil
}

// ranged are the collections named whose _id ranges the server bounds, as
// rangesBounded says, which discovery reads in ranges.
func (s *Source) ranged(ctx context.Context, db *mongo.Database, names []string) (map[string]bool, error) {
	v, err := s.buildVersion(ctx, db.Client())
	if err != nil || !rangesBounded(v, false) {
		return nil, err
	}
	out := map[string]bool{}
	for _, n := range names {
		out[n] = true
	}
	if rangesBounded(v, true) {
		return out, nil
	}
	specs, err := db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$in", Value: names}}}})
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: list collections of %s: %w", db.Name(), err))
	}
	for _, sp := range specs {
		if _, err := sp.Options.LookupErr("clusteredIndex"); err == nil {
			out[sp.Name] = false
		}
	}
	return out, nil
}

// serverVersion is a server's major, minor and patch version.
type serverVersion [3]int

func (v serverVersion) atLeast(major, minor, patch int) bool {
	return slices.Compare(v[:], []int{major, minor, patch}) >= 0
}

// buildVersion is the version of the server c connects to, as buildInfo
// gives it, read once: a sharded cluster's router's, which is upgraded
// after its shards.
func (s *Source) buildVersion(ctx context.Context, c *mongo.Client) (serverVersion, error) {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	if s.version != nil {
		return *s.version, nil
	}
	var info struct {
		VersionArray []int `bson:"versionArray"`
	}
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&info); err != nil {
		return serverVersion{}, classify(fmt.Errorf("mongodb: buildInfo: %w", err))
	}
	var v serverVersion
	copy(v[:], info.VersionArray)
	s.version = &v
	return v, nil
}

// rangesBounded reports whether a server of version v bounds a range of
// _ids, as idRange matches it, by the scan it plans, so that a range reads
// only its own documents. The _id index bounds it from 5.0, which first
// gives $expr's $gt, $gte, $lt and $lte index bounds. A clustered
// collection, which has no _id index, bounds its clustered scan by them
// from 6.0.17, 7.0.13 and 7.3; the releases before, and 6.1 to 6.3, 7.1 and
// 7.2, scan it all.
func rangesBounded(v serverVersion, clustered bool) bool {
	switch {
	case !clustered:
		return v.atLeast(5, 0, 0)
	case v.atLeast(7, 3, 0):
		return true
	case v.atLeast(7, 0, 13):
		return !v.atLeast(7, 1, 0)
	case v.atLeast(6, 0, 17):
		return !v.atLeast(6, 1, 0)
	}
	return false
}

// typeExpr is the BSON type of expression v's value as $type names it, with
// "uuid" for a 16-byte binary of subtype 4: a binary's length, then its
// subtype, orders it, which finds the UUIDs.
func typeExpr(v any) bson.D {
	var zeros, ones [16]byte
	for i := range ones {
		ones[i] = 0xff
	}
	return bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "t", Value: bson.D{{Key: "$type", Value: v}}}}},
		{Key: "in", Value: bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$and", Value: bson.A{
				bson.D{{Key: "$eq", Value: bson.A{"$$t", "binData"}}},
				bson.D{{Key: "$gte", Value: bson.A{v, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: zeros[:]}}}},
				bson.D{{Key: "$lte", Value: bson.A{v, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: ones[:]}}}},
			}}},
			"uuid", "$$t",
		}}}},
	}}}
}

// rangeTypes are the BSON types of each top-level field's values, as
// typeExpr names them, in the documents of a collection whose _ids are from
// lo and below hi, nil for no bound, or in sampleSize of them.
func (s *Source) rangeTypes(ctx context.Context, coll *mongo.Collection, lo, hi *value) (map[string][]string, error) {
	var pipeline mongo.Pipeline
	switch {
	case s.cfg.SampleSize > 0:
		pipeline = append(pipeline, bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: s.cfg.SampleSize}}}})
	case lo != nil || hi != nil:
		pipeline = append(pipeline, idRange(lo, false, hi))
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "f", Value: bson.D{{Key: "$objectToArray", Value: "$$ROOT"}}}}}},
		bson.D{{Key: "$unwind", Value: "$f"}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$f.k"}, {Key: "t", Value: bson.D{{Key: "$addToSet", Value: typeExpr("$f.v")}}}}}},
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

// endAt finishes a read that stops at an until: the rows b holds and the
// document with its last _id, if it's still there, then a resume point and
// the completion with until's state.
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
		}
	}
	if err := b.Flush(out); err != nil {
		return err
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
	types, err := s.fieldTypes(ctx, coll.Database(), []string{coll.Name()})
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(types[0])), nil
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
// starts with, and those documents bring since. With the run's types, a
// value of a type its column doesn't take fails the read too (unfit).
// Packed, a batch is _id and data.
type batcher struct {
	docstore.Batch
	stream   string
	declared bool
	changes  bool
	packed   bool
	// Discovery's sample size; 0: it reads every document.
	sampled int
	// The run's type of each column, by its index, and the kinds of values
	// it takes; a column past them, or without a type, takes every kind.
	types []arrow.DataType
	takes []kindSet
	// For a field the run doesn't declare, typ "", or a value of type typ,
	// as typeExpr names it, that the field's declared type doesn't take:
	// whether the collection's documents no longer have the field, or no
	// longer have such a value in it, so that the next discovery won't
	// declare it either. Nil: none are.
	gone func(field, typ string) (bool, error)
	// add's, kept for the next document's: each element's column, and its
	// value.
	at   []int
	vals []bson.RawValue
}

// controlNames are the names of a cdc batch's control columns, which the
// run's columns can have too.
var controlNames = []string{source.OpField().Name, source.SeqField(seqType).Name}

// newBatcher makes a batcher of columns, which are the run's where cs has
// them.
func (s *Source) newBatcher(cs source.ConfiguredStream, columns []string, changes bool) *batcher {
	b := &batcher{stream: cs.Name, changes: changes, declared: cs.Columns != nil, packed: s.cfg.Packed, sampled: s.cfg.SampleSize}
	b.Batch = docstore.NewBatch(changes)
	b.Column("_id")
	for _, c := range columns {
		if !changes || !slices.Contains(controlNames, c) {
			b.Column(c)
		}
	}
	if cs.Schema != nil && !b.packed {
		b.types, b.takes = make([]arrow.DataType, len(b.Names)), make([]kindSet, len(b.Names))
		for i, n := range b.Names {
			b.takes[i] = allKinds
			if f := cs.Schema.FieldIndices(n); len(f) == 1 {
				b.types[i] = cs.Schema.Field(f[0]).Type
				b.takes[i] = takes(b.types[i])
			}
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
	at, vals := b.at[:0], b.vals[:0]
	for _, e := range elems {
		k, v := e.Key(), e.Value()
		i, ok := b.Index[k]
		switch {
		case ok:
			if i < len(b.takes) && b.takes[i]&(1<<kindOf(v)) == 0 {
				if i, err = b.unfit(k, i, v, op); err != nil {
					return err
				}
			}
		case op == source.OpDelete:
			i = -1
		case b.declared:
			gone := false
			if b.gone != nil {
				if gone, err = b.gone(k, ""); err != nil {
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
		at, vals = append(at, i), append(vals, v)
	}
	b.at, b.vals = at, vals
	set := make([]bool, len(b.Cols))
	for j, v := range vals {
		i := at[j]
		if i < 0 || set[i] {
			continue
		}
		set[i] = true
		t, ok := text(v)
		if b.Names[i] == "_id" {
			t, ok = keyText(v), true
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
	// So as not to keep the document.
	clear(vals)
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

// unfit is where a value of field k goes whose type its column, i, doesn't
// take, as discovery would have declared the field as another type had it
// seen the value: it fails the read, retryably, so that the next attempt's
// discovery declares the type that takes it, as for a new field. Where that
// discovery mightn't see the value either, as a sample can miss it, or
// won't, as no document has such a value any more, which a cdc change from
// before can find, it goes to i as text, which the engine quarantines or
// holds. A delete's is left out, -1, as its values only say what it
// deleted.
func (b *batcher) unfit(k string, i int, v bson.RawValue, op string) (int, error) {
	switch {
	case op == source.OpDelete:
		return -1, nil
	case b.sampled > 0:
		return i, nil
	}
	typ := typeNames[kindOf(v)].name
	if b.gone != nil {
		if gone, err := b.gone(k, typ); err != nil || gone {
			return i, err
		}
	}
	declared := b.types[i].String()
	if e, ok := b.types[i].(arrow.ExtensionType); ok {
		declared = e.ExtensionName()
	}
	return i, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("mongodb: %s's field %q is declared %s, and a document has a %s value there, which it doesn't take; the read starts again so that discovery declares a type that does", b.stream, k, declared, typ))
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
