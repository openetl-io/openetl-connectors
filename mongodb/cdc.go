package mongodb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// cdcState is where a cdc read of a collection is.
type cdcState struct {
	// The cluster time the read started at: its snapshot's seq, and where
	// its change stream starts while Token is nil.
	Time bson.Timestamp `json:"time"`
	// The change stream's resume token, after the last change read.
	Token *value `json:"token,omitempty"`
	// The place of that change among those of its cluster time, from 1.
	Place uint32 `json:"place,omitempty"`
	// Set while the snapshot is read.
	Snapshot *snapshotState `json:"snapshot,omitempty"`
}

type snapshotState struct {
	// The last _id the snapshot read; nil before the first.
	After *value `json:"after,omitempty"`
}

// How often a continuous read that reads no changes moves its resume point
// to where its change stream has looked, so a quiet collection's position
// doesn't fall out of the oplog. A bounded read moves it only once the
// stream has moved on this far, so that reading from a position it
// completed at, with no changes since, completes there again.
const (
	quietResume = 10 * time.Second
	staleResume = time.Minute
)

// cdcReader reads a collection with cdc: its documents in _id order as
// inserts at the cluster time the read started, then its change stream from
// that time, so a change made while the snapshot ran is read again after it.
// A record's seq is its change's cluster time, which orders its versions,
// with the snapshot's below every change after it.
type cdcReader struct {
	s          *Source
	db         *mongo.Database
	cs         source.ConfiguredStream
	out        source.SplitEmitter
	progress   source.Progresser
	continuous bool
	// Where a bounded read ends: the cluster time when it started.
	end bson.Timestamp

	coll   *mongo.Collection
	st     cdcState
	until  *cdcState
	untilB []byte
	b      *batcher
	// The cluster time of the change being read.
	at bson.Timestamp
	// Fields the run doesn't declare that no document had at a cluster
	// time, by field.
	gone map[string]bson.Timestamp
}

func (r *cdcReader) read(ctx context.Context) error {
	r.coll = r.db.Collection(r.cs.Name)
	state, until := r.cs.State, r.cs.Until
	if len(r.cs.Splits) == 1 && r.cs.Splits[0].ID == source.WholeStream {
		state, until = r.cs.Splits[0].State, r.cs.Splits[0].Until
	}
	var err error
	if r.st, err = parseState[cdcState](r.cs.Name, state); err != nil {
		return err
	}
	if until != nil {
		u, err := parseState[cdcState](r.cs.Name, until)
		if err != nil {
			return err
		}
		r.until, r.untilB = &u, until
	}
	if state == nil {
		// A read again from the start to an until starts where it did.
		if r.until != nil {
			r.st.Time = r.until.Time
		} else if r.st.Time, err = clusterTime(ctx, r.db.Client()); err != nil {
			return err
		}
		r.st.Snapshot = &snapshotState{}
	}
	cols, err := r.s.knownFields(ctx, r.coll, r.cs)
	if err != nil {
		return err
	}
	r.b = r.s.newBatcher(r.cs, cols, true)
	defer r.b.Release()
	if r.st.Snapshot != nil {
		if stopped, err := r.snapshot(ctx); stopped || err != nil {
			return err
		}
	}
	if r.until != nil && r.until.Snapshot == nil && r.until.Token == nil {
		return r.stopAtUntil()
	}
	return r.follow(ctx)
}

// snapshot reads the collection a chunk at a time; stopped: at the until.
// It reads every change up to the read's cluster time, which the change
// stream then starts after.
func (r *cdcReader) snapshot(ctx context.Context) (stopped bool, err error) {
	q := seq{t: r.st.Time}
	var hi *value
	if r.until != nil && r.until.Snapshot != nil {
		hi = r.until.Snapshot.After
	}
	err = atLeast(ctx, r.coll, r.st.Time, func(ctx context.Context, coll *mongo.Collection) error {
		for {
			got, last, err := r.s.readChunk(ctx, coll, nil, nil, r.st.Snapshot.After, hi, func(doc bson.Raw) error {
				return r.b.addFlushing(doc, source.OpInsert, q, r.out)
			})
			if err != nil {
				return err
			}
			if last != nil {
				r.st.Snapshot.After = last
			}
			if got < r.s.cfg.ChunkDocuments {
				break
			}
			if err := r.resume(); err != nil {
				return err
			}
		}
		if hi != nil {
			stopped = true
			return r.s.endAt(ctx, coll, nil, hi, r.untilB, r.b, source.OpInsert, q, r.out)
		}
		return nil
	})
	if err != nil || stopped {
		return stopped, err
	}
	r.st.Snapshot = nil
	return false, r.resume()
}

// atLeast runs fn with coll's reads seeing every change up to cluster time
// t: majority reads, in a causally consistent session that has seen t, wait
// until the majority has every change up to t.
func atLeast(ctx context.Context, coll *mongo.Collection, t bson.Timestamp, fn func(context.Context, *mongo.Collection) error) error {
	sess, err := coll.Database().Client().StartSession(options.Session().SetCausalConsistency(true))
	if err != nil {
		return classify(fmt.Errorf("mongodb: start a session: %w", err))
	}
	defer sess.EndSession(context.WithoutCancel(ctx))
	if err := sess.AdvanceOperationTime(&t); err != nil {
		return err
	}
	c := coll.Database().Collection(coll.Name(), options.Collection().SetReadConcern(readconcern.Majority()))
	return fn(mongo.NewSessionContext(ctx, sess), c)
}

// resume flushes, and emits a resume point at the state.
func (r *cdcReader) resume() error {
	if err := r.b.Flush(r.out); err != nil {
		return err
	}
	state, err := json.Marshal(r.st)
	if err != nil {
		return err
	}
	return r.out.ResumePoint(state)
}

// stopAtUntil ends a read at its until, past which it emitted nothing.
func (r *cdcReader) stopAtUntil() error {
	if err := r.b.Flush(r.out); err != nil {
		return err
	}
	if err := r.out.ResumePoint(r.untilB); err != nil {
		return err
	}
	return r.out.Complete(r.untilB)
}

// complete ends a bounded read where it is.
func (r *cdcReader) complete() error {
	if err := r.b.Flush(r.out); err != nil {
		return err
	}
	state, err := json.Marshal(r.st)
	if err != nil {
		return err
	}
	return r.out.Complete(state)
}

// follow reads the change stream: inserts, updates and replaces as their
// documents after the change, deletes as their keys, or the documents before
// them where the collection keeps pre-images. Updates take the post-image
// where the collection keeps them, or else the document as it is when read,
// skipped if it's since been deleted. A drop or rename ends the stream: the
// position is lost, and only a new snapshot reads the collection again.
func (r *cdcReader) follow(ctx context.Context) error {
	images, err := r.images(ctx)
	if err != nil {
		return err
	}
	opts := options.ChangeStream().SetMaxAwaitTime(time.Second)
	if images {
		opts.SetFullDocument(options.WhenAvailable).SetFullDocumentBeforeChange(options.WhenAvailable)
	} else {
		opts.SetFullDocument(options.UpdateLookup)
	}
	if r.st.Token != nil {
		opts.SetStartAfter(r.st.Token.Document())
	} else {
		// After the snapshot's time, whose changes the snapshot has.
		start := bson.Timestamp{T: r.st.Time.T, I: r.st.Time.I + 1}
		if start.I == 0 {
			start = bson.Timestamp{T: r.st.Time.T + 1}
		}
		opts.SetStartAtOperationTime(&start)
	}
	stream, err := r.coll.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return r.failed(err)
	}
	defer func() { _ = stream.Close(context.WithoutCancel(ctx)) }()
	r.b.gone = r.fieldGone(ctx)
	var (
		last, _  = tokenTime(r.st.Token)
		place    = r.st.Place
		read     bool
		resumed  = time.Now()
		reported time.Time
	)
	for {
		if stream.TryNext(ctx) {
			ev := stream.Current
			t, i := ev.Lookup("clusterTime").Timestamp()
			at := bson.Timestamp{T: t, I: i}
			if !r.continuous && after(at, r.end) {
				return r.complete()
			}
			tok := clone(ev.Lookup("_id"))
			if r.until != nil && tokenCompare(tok, r.until.Token) > 0 {
				return r.stopAtUntil()
			}
			if at == last {
				place++
			} else {
				last, place = at, 1
			}
			r.at = at
			if err := r.change(ctx, ev, seq{t: at, place: place}); err != nil {
				return err
			}
			r.st.Token, r.st.Place, read = tok, place, true
			if r.until != nil && tokenCompare(tok, r.until.Token) == 0 {
				return r.stopAtUntil()
			}
			if r.b.Rows >= r.s.cfg.ChunkDocuments {
				if err := r.resume(); err != nil {
					return err
				}
				resumed = time.Now()
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := stream.Err(); err != nil {
			return r.failed(err)
		}
		// The batch is drained: the stream has looked as far as pbrt.
		pbrt := clone(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: stream.ResumeToken()})
		switch {
		case r.b.Rows > 0:
			if err := r.resume(); err != nil {
				return err
			}
			resumed = time.Now()
		case r.continuous && time.Since(resumed) >= quietResume && tokenCompare(pbrt, r.st.Token) > 0:
			r.st.Token, r.st.Place = pbrt, 0
			if err := r.resume(); err != nil {
				return err
			}
			resumed = time.Now()
		}
		if r.progress != nil && time.Since(reported) >= 10*time.Second {
			reported = time.Now()
			if pt, ok := tokenTime(pbrt); ok {
				if err := r.progress.Progress(source.Progress{Stream: r.cs.Name, Lag: new(max(0, time.Since(time.Unix(int64(pt.T), 0))))}); err != nil {
					return err
				}
			}
		}
		if r.until != nil && tokenCompare(pbrt, r.until.Token) >= 0 {
			return r.stopAtUntil()
		}
		if !r.continuous {
			if pt, ok := tokenTime(pbrt); !ok || !after(r.end, pt) {
				if from, ok := r.position(); !read && (!ok || pt.T >= from.T+uint32(staleResume/time.Second)) {
					r.st.Token, r.st.Place = pbrt, 0
				}
				return r.complete()
			}
		}
	}
}

// fieldGone reports whether no document of the collection has a field
// anymore, as a change from before it was removed can: none has it at a
// cluster time at or after the change's. A change after that time is
// checked again, since a document can have brought the field back.
func (r *cdcReader) fieldGone(ctx context.Context) func(string) (bool, error) {
	return func(field string) (bool, error) {
		if t, ok := r.gone[field]; ok && !after(r.at, t) {
			return true, nil
		}
		t, err := clusterTime(ctx, r.db.Client())
		if err != nil {
			return false, err
		}
		err = atLeast(ctx, r.coll, t, func(ctx context.Context, coll *mongo.Collection) error {
			return coll.FindOne(ctx, bson.D{{Key: field, Value: bson.D{{Key: "$exists", Value: true}}}}, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
		})
		switch {
		case err == nil:
			return false, nil
		case !errors.Is(err, mongo.ErrNoDocuments):
			return false, classify(fmt.Errorf("mongodb: read %s: %w", r.cs.Name, err))
		}
		if r.gone == nil {
			r.gone = map[string]bson.Timestamp{}
		}
		r.gone[field] = t
		return !after(r.at, t), nil
	}
}

// position is the cluster time the state's token is at.
func (r *cdcReader) position() (bson.Timestamp, bool) {
	if r.st.Token == nil {
		return r.st.Time, true
	}
	return tokenTime(r.st.Token)
}

// change adds an event's record, or fails for one that ends the stream.
func (r *cdcReader) change(ctx context.Context, ev bson.Raw, q seq) error {
	op, _ := ev.Lookup("operationType").StringValueOK()
	switch op {
	case "insert", "update", "replace":
		doc, ok := ev.Lookup("fullDocument").DocumentOK()
		if !ok {
			key := ev.Lookup("documentKey", "_id")
			var err error
			doc, err = r.coll.FindOne(ctx, bson.D{{Key: "_id", Value: key}}).Raw()
			if errors.Is(err, mongo.ErrNoDocuments) {
				// Deleted since: its delete follows.
				return nil
			}
			if err != nil {
				return classify(fmt.Errorf("mongodb: read %s: %w", r.cs.Name, err))
			}
		}
		kind := source.OpUpdate
		if op == "insert" {
			kind = source.OpInsert
		}
		return r.b.add(doc, kind, q)
	case "delete":
		if doc, ok := ev.Lookup("fullDocumentBeforeChange").DocumentOK(); ok {
			return r.b.add(doc, source.OpDelete, q)
		}
		return r.b.addKey(ev.Lookup("documentKey", "_id"), q)
	case "drop", "rename", "dropDatabase", "invalidate":
		if err := r.resume(); err != nil {
			return err
		}
		what := "was dropped"
		if to, ok := ev.Lookup("to", "coll").StringValueOK(); ok {
			what = "was renamed to " + to
		}
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf("mongodb: %s %s (%s); reading it again takes a new snapshot", r.cs.Name, what, op))
	}
	// Events of other types come only with showExpandedEvents.
	return nil
}

// failed classifies a change stream's error: a position the oplog no longer
// has is lost.
func (r *cdcReader) failed(err error) error {
	if se, ok := errors.AsType[mongo.ServerError](err); ok && (se.HasErrorCode(codeChangeStreamLost) || se.HasErrorCode(codeChangeStreamFatal) || se.HasErrorCode(codeCappedPositionLost)) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf("mongodb: %s's change stream position is no longer in the oplog, so reading it again takes a new snapshot: %w", r.cs.Name, err))
	}
	return classify(fmt.Errorf("mongodb: change stream of %s: %w", r.cs.Name, err))
}

// images reports whether the collection keeps pre- and post-images.
func (r *cdcReader) images(ctx context.Context) (bool, error) {
	specs, err := r.db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: r.cs.Name}})
	if err != nil {
		return false, classify(fmt.Errorf("mongodb: list collections of %s: %w", r.db.Name(), err))
	}
	if len(specs) == 0 {
		return false, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf("mongodb: %s doesn't exist; reading it again takes a new snapshot", r.cs.Name))
	}
	on, _ := specs[0].Options.Lookup("changeStreamPreAndPostImages", "enabled").BooleanOK()
	return on, nil
}

// clusterTime is the deployment's cluster time now, which is at or past
// every change made before; a ping's operationTime can be the majority's,
// before the last changes.
func clusterTime(ctx context.Context, c *mongo.Client) (bson.Timestamp, error) {
	res, err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if err != nil {
		return bson.Timestamp{}, classify(fmt.Errorf("mongodb: ping: %w", err))
	}
	t, i, ok := res.Lookup("$clusterTime", "clusterTime").TimestampOK()
	if !ok {
		return bson.Timestamp{}, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, errors.New("mongodb: the server has no cluster time, so no change streams: cdc needs a replica set or a sharded cluster"))
	}
	return bson.Timestamp{T: t, I: i}, nil
}

func after(a, b bson.Timestamp) bool { return a.T > b.T || a.T == b.T && a.I > b.I }

func clone(v bson.RawValue) *value {
	return &value{bson.RawValue{Type: v.Type, Value: bytes.Clone(v.Value)}}
}

// tokenData is a resume token's _data: hex of a key that orders tokens as
// the oplog does.
func tokenData(tok *value) string {
	if tok == nil {
		return ""
	}
	doc, ok := tok.DocumentOK()
	if !ok {
		return ""
	}
	s, _ := doc.Lookup("_data").StringValueOK()
	return s
}

// tokenCompare orders two tokens; nil is before every other.
func tokenCompare(a, b *value) int {
	return bytes.Compare([]byte(tokenData(a)), []byte(tokenData(b)))
}

// tokenTime is the cluster time a token's key starts with, after the tag
// 0x82 of a timestamp.
func tokenTime(tok *value) (bson.Timestamp, bool) {
	b, err := hex.DecodeString(tokenData(tok))
	if err != nil || len(b) < 9 || b[0] != 0x82 {
		return bson.Timestamp{}, false
	}
	return bson.Timestamp{T: binary.BigEndian.Uint32(b[1:5]), I: binary.BigEndian.Uint32(b[5:9])}, true
}
