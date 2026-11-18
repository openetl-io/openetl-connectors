package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// Retention says, for each cdc stream, how far its position is behind the
// oplog's newest entry, and how long until the oplog drops it: the oplog
// drops its oldest entries as new ones come, so at the pace it has kept, in
// as long as the position is past its oldest. Positions the oplog has
// dropped already, and a user who can't read local.oplog.rs, say nothing.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, _ map[string][]source.Split) ([]source.Progress, error) {
	at := map[string]bson.Timestamp{}
	for name, b := range states {
		var st cdcState
		if json.Unmarshal(b, &st) != nil || st.Time.T == 0 {
			continue
		}
		r := cdcReader{st: st}
		if t, ok := r.position(); ok {
			at[name] = t
		}
	}
	if len(at) == 0 {
		return nil, nil
	}
	c, done, err := s.client(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	first, last, err := oplogEnds(ctx, c)
	if se, ok := errors.AsType[mongo.ServerError](err); ok && se.HasErrorCode(codeUnauthorized) || errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(fmt.Errorf("mongodb: read the oplog's ends: %w", err))
	}
	var out []source.Progress
	for name, t := range at {
		if after(first, t) {
			continue
		}
		p := source.Progress{Stream: name, Lag: new(seconds(last.T, t.T)), RetentionLeft: new(seconds(t.T, first.T))}
		out = append(out, p)
	}
	return out, nil
}

// oplogEnds are the times of the oplog's oldest and newest entries;
// mongo.ErrNoDocuments without an oplog, as on a standalone server.
func oplogEnds(ctx context.Context, c *mongo.Client) (first, last bson.Timestamp, err error) {
	oplog := c.Database("local").Collection("oplog.rs")
	end := func(order int) (bson.Timestamp, error) {
		doc, err := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: order}}).SetProjection(bson.D{{Key: "ts", Value: 1}})).Raw()
		if err != nil {
			return bson.Timestamp{}, err
		}
		t, i := doc.Lookup("ts").Timestamp()
		return bson.Timestamp{T: t, I: i}, nil
	}
	if first, err = end(1); err == nil {
		last, err = end(-1)
	}
	return first, last, err
}

func seconds(a, b uint32) time.Duration {
	if a < b {
		return 0
	}
	return time.Duration(a-b) * time.Second
}
