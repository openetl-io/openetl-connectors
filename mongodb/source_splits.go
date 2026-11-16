package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// The most _id ranges a collection's read in.
const maxRanges = 32

// Splits divides a collection read in full or incrementally into _id ranges
// of about chunkDocuments documents each, cut at a random sample's _ids in
// the order of the _id index. Small collections are read whole, and cdc
// streams always. An incremental read's ranges share the highest cursor value
// it reads and its floor, where the last read's splits all ended.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	whole := func(st readState) ([]source.Split, error) {
		if w, err := source.Whole(ended); err == nil {
			return w, nil
		}
		state, err := json.Marshal(st)
		if err != nil {
			return nil, err
		}
		return []source.Split{{ID: source.WholeStream, State: state}}, nil
	}
	if cs.SyncMode == source.CDC {
		// A state from a read in splits starts the collection again.
		return whole(readState{})
	}
	c, done, err := s.client(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	coll := s.db(c).Collection(cs.Name)
	var st readState
	if cs.SyncMode == source.Incremental {
		for _, sp := range ended {
			if prev, err := parseState[readState](cs.Name, sp.State); err != nil {
				return nil, err
			} else if prev.Since != nil {
				st.Since = prev.Since
			}
		}
		if cs.Cursor != "" {
			if st.Max, err = maxCursor(ctx, coll, cs.Cursor); err != nil {
				return nil, err
			}
		}
		if st.Max == nil {
			return whole(st)
		}
	}
	n, err := coll.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: count %s: %w", cs.Name, err))
	}
	ranges := min(maxRanges, (n+int64(s.cfg.ChunkDocuments)-1)/int64(s.cfg.ChunkDocuments))
	if ranges < 2 {
		return whole(st)
	}
	bounds, err := sampleIDs(ctx, coll, int(ranges)*16, nil, nil)
	if err != nil {
		return nil, err
	}
	var cuts []*value
	for i := 1; i < int(ranges) && len(bounds) > 0; i++ {
		b := bounds[i*len(bounds)/int(ranges)]
		if bound(b) && (len(cuts) == 0 || !cuts[len(cuts)-1].Equal(b.RawValue)) {
			cuts = append(cuts, b)
		}
	}
	if len(cuts) == 0 {
		return whole(st)
	}
	splits := make([]source.Split, 0, len(cuts)+1)
	for i := range len(cuts) + 1 {
		r := st
		if i > 0 {
			r.Lo = cuts[i-1]
		}
		if i < len(cuts) {
			r.Hi = cuts[i]
		}
		state, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		splits = append(splits, source.Split{ID: strconv.Itoa(i), State: state})
	}
	return splits, nil
}

// sampleIDs are the _ids of n random documents, those strictly between lo
// and hi where they're set, in the _id index's order: aggregation compares
// values of different types in the order the index keeps them. A range is
// sampled from the collection's sample, which is cheap, or where that has
// fewer than two of its _ids, from all of them.
func sampleIDs(ctx context.Context, coll *mongo.Collection, n int, lo, hi *value) ([]*value, error) {
	var in bson.A
	if lo != nil {
		in = append(in, bson.D{{Key: "$gt", Value: bson.A{"$_id", lo.RawValue}}})
	}
	if hi != nil {
		in = append(in, bson.D{{Key: "$lt", Value: bson.A{"$_id", hi.RawValue}}})
	}
	sample := bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: n}}}}
	if len(in) == 0 {
		return aggregateIDs(ctx, coll, mongo.Pipeline{sample})
	}
	match := bson.D{{Key: "$match", Value: bson.D{{Key: "$expr", Value: bson.D{{Key: "$and", Value: in}}}}}}
	ids, err := aggregateIDs(ctx, coll, mongo.Pipeline{sample, match})
	if err != nil || len(ids) >= 2 {
		return ids, err
	}
	return aggregateIDs(ctx, coll, mongo.Pipeline{match, sample})
}

// aggregateIDs are the _ids a pipeline's documents have, in order.
func aggregateIDs(ctx context.Context, coll *mongo.Collection, pipeline mongo.Pipeline) ([]*value, error) {
	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	)
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: sample %s: %w", coll.Name(), err))
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	var out []*value
	for cur.Next(ctx) {
		id := cur.Current.Lookup("_id")
		out = append(out, &value{bson.RawValue{Type: id.Type, Value: slices.Clone(id.Value)}})
	}
	return out, classify(cur.Err())
}

// bound reports whether a range can start or end at v: min and max can't
// at MinKey or MaxKey.
func bound(v *value) bool { return v.Type != bson.TypeMinKey && v.Type != bson.TypeMaxKey }

type readingKey struct{ stream, split string }

// readingSplit is a split being read.
type readingSplit struct {
	// Resplit asked, and the split's next resume point answers.
	asked atomic.Bool
	// It can't be split: Resplit refuses.
	over atomic.Bool
}

func (s *Source) startSplit(stream, id string) *readingSplit {
	r := &readingSplit{}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reading == nil {
		s.reading = map[readingKey]*readingSplit{}
	}
	s.reading[readingKey{stream, id}] = r
	return r
}

func (s *Source) endSplit(stream, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reading, readingKey{stream, id})
}

// Resplit asks a split being read to split its remainder at its next resume
// point, which works out where. It refuses a split it knows it can't split,
// or that's already been asked.
func (s *Source) Resplit(_ context.Context, stream, split string) (bool, error) {
	s.mu.Lock()
	r := s.reading[readingKey{stream, split}]
	s.mu.Unlock()
	if r == nil || r.over.Load() {
		return false, nil
	}
	return r.asked.CompareAndSwap(false, true), nil
}

// resplit runs at each resume point of a split. If it was asked, it divides
// what's left of the range at the median of a sample of the _ids in it:
// st keeps the lower part, and the split to add takes the upper.
func (s *Source) resplit(ctx context.Context, coll *mongo.Collection, r *readingSplit, st *readState) ([]source.Split, error) {
	if !r.asked.Swap(false) {
		return nil, nil
	}
	ids, err := sampleIDs(ctx, coll, 1000, st.After, st.Hi)
	if err != nil {
		return nil, err
	}
	ids = slices.DeleteFunc(ids, func(v *value) bool { return !bound(v) })
	if len(ids) == 0 {
		r.over.Store(true)
		return nil, nil
	}
	mid := ids[len(ids)/2]
	upper := *st
	upper.Lo, upper.After = mid, nil
	state, err := json.Marshal(upper)
	if err != nil {
		return nil, err
	}
	st.Hi = mid
	id := sha256.Sum256(state)
	return []source.Split{{ID: hex.EncodeToString(id[:6]), State: state}}, nil
}
