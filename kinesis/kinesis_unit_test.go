package kinesis

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestCmpSeq(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{{"9", "10", -1}, {"10", "9", 1}, {"123", "123", 0}, {"124", "123", 1}, {"49590338271490256608559692538361571095921575989136588898", "49590338271490256608559692538361571095921575989136588899", -1}} {
		if got := cmpSeq(c.a, c.b); (got > 0) != (c.want > 0) || (got < 0) != (c.want < 0) {
			t.Errorf("cmpSeq(%s, %s) = %d, want the sign of %d", c.a, c.b, got, c.want)
		}
	}
}

func TestPartitionKey(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: idColumn, Type: arrow.BinaryTypes.String},
	}, nil)
	cols, err := records.Cols(sc)
	if err != nil {
		t.Fatal(err)
	}
	vals := []any{int64(7), "a\"b", "0b6c0b1e-5f2d-4c55-9d8e-1f3a2b4c5d6e"}
	long := strings.Repeat("x", 300)
	sum := sha256.Sum256([]byte(long))
	for _, c := range []struct {
		key  []int
		vals []any
		want string
	}{
		{[]int{0}, vals, "7"},
		{[]int{1}, vals, `a"b`},
		{[]int{0, 1}, vals, `[7,"a\"b"]`},
		{nil, vals, "0b6c0b1e-5f2d-4c55-9d8e-1f3a2b4c5d6e"},
		{[]int{1}, []any{int64(7), long, ""}, hex.EncodeToString(sum[:])},
	} {
		e := &streamEncoder{cols: cols, id: 2}
		for _, i := range c.key {
			e.key = append(e.key, i)
			e.keyCols = append(e.keyCols, cols[i])
		}
		got, err := e.partitionKey(c.vals)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("key %v: %q, want %q", c.key, got, c.want)
		}
	}
}

// Retention left counts from the state's time only for shards still
// behind.
func TestProgress(t *testing.T) {
	now := time.Now()
	r := &streamRead{
		name: "s", retention: 24 * time.Hour,
		shards: map[string]shardState{"a": {Since: now.Add(-2 * time.Hour)}, "b": {Since: now.Add(-20 * time.Hour)}},
		behind: map[string]time.Duration{"a": 3 * time.Second, "b": 0},
	}
	p := r.progress(now)
	if *p.Lag != 3*time.Second || *p.RetentionLeft != 22*time.Hour {
		t.Errorf("lag %v, retention left %v", *p.Lag, *p.RetentionLeft)
	}
	r.behind["a"] = 0
	if p := r.progress(now); *p.Lag != 0 || *p.RetentionLeft != 24*time.Hour {
		t.Errorf("caught up: lag %v, retention left %v", *p.Lag, *p.RetentionLeft)
	}
}

func shardOf(id string, closed bool, parents ...string) types.Shard {
	sh := types.Shard{ShardId: aws.String(id), SequenceNumberRange: &types.SequenceNumberRange{StartingSequenceNumber: aws.String("1")}}
	if closed {
		sh.SequenceNumberRange.EndingSequenceNumber = aws.String("2")
	}
	if len(parents) > 0 {
		sh.ParentShardId = aws.String(parents[0])
	}
	if len(parents) > 1 {
		sh.AdjacentParentShardId = aws.String(parents[1])
	}
	return sh
}

func listing(shards ...types.Shard) map[string]types.Shard {
	out := map[string]types.Shard{}
	for _, sh := range shards {
		out[aws.ToString(sh.ShardId)] = sh
	}
	return out
}

func splitIDs(splits []source.Split) []string {
	out := []string{}
	for _, sp := range splits {
		out = append(out, sp.ID)
	}
	return out
}

// A shard whose parents aren't done isn't a split. A done shard is one only
// while the read needs to know it's done: until its children appear, and
// are runnable or done themselves.
func TestSplitSet(t *testing.T) {
	split := listing(shardOf("p", true), shardOf("c1", false, "p"), shardOf("c2", false, "p"))
	merge := listing(shardOf("a", true), shardOf("b", true), shardOf("m", false, "a", "b"))
	done, at, fresh := shardState{Done: true}, shardState{After: "5"}, shardState{}
	for _, c := range []struct {
		name   string
		listed map[string]types.Shard
		shards map[string]shardState
		want   []string
	}{
		{"never read", split, map[string]shardState{"p": fresh, "c1": fresh, "c2": fresh}, []string{"p"}},
		{"parent midway", split, map[string]shardState{"p": at, "c1": fresh, "c2": fresh}, []string{"p"}},
		{"parent done", split, map[string]shardState{"p": done, "c1": at, "c2": at}, []string{"c1", "c2"}},
		{"a child done too", listing(shardOf("p", true), shardOf("c1", true, "p"), shardOf("c2", false, "p"), shardOf("g", false, "c1")),
			map[string]shardState{"p": done, "c1": done, "c2": at, "g": fresh}, []string{"c2", "g"}},
		{"one parent done", merge, map[string]shardState{"a": done, "b": at, "m": fresh}, []string{"a", "b"}},
		{"both parents done", merge, map[string]shardState{"a": done, "b": done, "m": fresh}, []string{"m"}},
		{"parent expired", listing(shardOf("c", false, "gone")), map[string]shardState{"c": fresh}, []string{"c"}},
		{"one parent expired", listing(shardOf("b", true), shardOf("m", false, "gone", "b")), map[string]shardState{"b": at, "m": fresh}, []string{"b"}},
		{"no children yet", listing(shardOf("p", true)), map[string]shardState{"p": done}, []string{"p"}},
	} {
		r := &streamRead{name: "s", listed: c.listed, shards: c.shards}
		if got := splitIDs(r.splitSet()); !slices.Equal(got, c.want) {
			t.Errorf("%s: splits %v, want %v", c.name, got, c.want)
		}
	}
}

// What's above a shard the read has reached is done, however far up, since
// done shards stop being splits. What's beside it isn't.
func TestDoneAbove(t *testing.T) {
	r := &streamRead{
		listed: listing(shardOf("g", true), shardOf("p", true, "g"), shardOf("c1", false, "p"), shardOf("c2", false, "p"), shardOf("other", false)),
		shards: map[string]shardState{"c1": {After: "5"}, "other": {After: "6"}},
	}
	r.doneAbove("c1")
	if !r.shards["p"].Done || !r.shards["g"].Done {
		t.Errorf("shards %v", r.shards)
	}
	if _, ok := r.shards["c2"]; ok {
		t.Errorf("shards %v", r.shards)
	}
	if r.shards["c1"].Done || r.shards["other"].Done {
		t.Errorf("shards %v", r.shards)
	}
}

func newSplitRead(listed map[string]types.Shard, bounded bool, shards map[string]shardState) *streamRead {
	r := &streamRead{name: "s", listed: listed, shards: shards, splits: map[string]bool{}, running: map[string]bool{}, stopped: map[string]bool{}, behind: map[string]time.Duration{}}
	for id := range shards {
		r.splits[id] = true
	}
	r.settleSplits(bounded)
	return r
}

// A retry has only the splits that haven't completed. A listed shard outside
// them is below one of them and waits, or else completed earlier and is
// neither read nor added again.
func TestSettleSplits(t *testing.T) {
	t.Run("what completed isn't read again", func(t *testing.T) {
		r := newSplitRead(listing(shardOf("p", true), shardOf("c1", true, "p"), shardOf("c2", false, "p"), shardOf("g1", false, "c1"), shardOf("g2", false, "c1")), false,
			map[string]shardState{"c2": {After: "5"}, "g1": {After: "6"}, "g2": {}})
		start, added := r.take()
		if !slices.Equal(start, []string{"c2", "g1", "g2"}) || len(added) != 0 {
			t.Errorf("start %v, added %v", start, added)
		}
	})
	t.Run("a resharded split's children wait for it", func(t *testing.T) {
		r := newSplitRead(listing(shardOf("p", true), shardOf("c1", false, "p"), shardOf("c2", false, "p")), false, map[string]shardState{"p": {After: "5"}})
		if start, added := r.take(); !slices.Equal(start, []string{"p"}) || len(added) != 0 {
			t.Errorf("start %v, added %v", start, added)
		}
		r.shards["p"] = shardState{After: "9", Done: true}
		start, added := r.take()
		if !slices.Equal(start, []string{"c1", "c2"}) || !slices.Equal(splitIDs(added), []string{"c1", "c2"}) {
			t.Errorf("after the parent: start %v, added %v", start, added)
		}
	})
	merge := func() map[string]types.Shard {
		return listing(shardOf("a", true), shardOf("b", true), shardOf("m", false, "a", "b"))
	}
	t.Run("a merge after one parent completed", func(t *testing.T) {
		r := newSplitRead(merge(), false, map[string]shardState{"b": {After: "5"}})
		if start, added := r.take(); !slices.Equal(start, []string{"b"}) || len(added) != 0 {
			t.Errorf("start %v, added %v", start, added)
		}
		r.shards["b"] = shardState{After: "9", Done: true}
		if start, added := r.take(); !slices.Equal(start, []string{"m"}) || !slices.Equal(splitIDs(added), []string{"m"}) {
			t.Errorf("after the last parent: start %v, added %v", start, added)
		}
	})
	t.Run("a bounded read doesn't know how far the parent got", func(t *testing.T) {
		r := newSplitRead(merge(), true, map[string]shardState{"b": {After: "5"}})
		if start, added := r.take(); !slices.Equal(start, []string{"b"}) || len(added) != 0 {
			t.Errorf("start %v, added %v", start, added)
		}
		r.shards["b"] = shardState{After: "9", Done: true}
		if start, added := r.take(); len(start) != 0 || len(added) != 0 {
			t.Errorf("after the last parent: start %v, added %v", start, added)
		}
	})
	t.Run("a merged child that joined before its parent completed", func(t *testing.T) {
		r := newSplitRead(merge(), false, map[string]shardState{"a": {After: "5"}, "m": {}})
		if start, added := r.take(); !slices.Equal(start, []string{"a"}) || len(added) != 0 {
			t.Errorf("start %v, added %v", start, added)
		}
		r.shards["a"] = shardState{After: "9", Done: true}
		if start, added := r.take(); !slices.Equal(start, []string{"m"}) || len(added) != 0 {
			t.Errorf("after the parent: start %v, added %v", start, added)
		}
	})
	t.Run("a done split's children join when it completes", func(t *testing.T) {
		r := newSplitRead(merge(), false, map[string]shardState{"a": {Done: true}, "b": {Done: true}})
		if !r.below("m", r.splits) {
			t.Error("m is below its parents")
		}
		if start, added := r.take(); !slices.Equal(start, []string{"m"}) || !slices.Equal(splitIDs(added), []string{"m"}) {
			t.Errorf("start %v, added %v", start, added)
		}
	})
	t.Run("a split whose parent expired", func(t *testing.T) {
		r := newSplitRead(listing(shardOf("c", false, "gone")), false, map[string]shardState{"gone": {After: "5", Done: true}})
		if start, added := r.take(); !slices.Equal(start, []string{"c"}) || !slices.Equal(splitIDs(added), []string{"c"}) {
			t.Errorf("start %v, added %v", start, added)
		}
	})
}

func TestSplitShards(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	got, err := splitShards([]source.Split{{ID: "a", State: encodeShard(shardState{After: "5", Done: true})}, {ID: "b"}}, start)
	if err != nil || len(got) != 2 || got["a"].After != "5" || !got["a"].Done || !got["b"].Since.Equal(start) {
		t.Errorf("splits: %v, %v", got, err)
	}
	got, err = splitShards([]source.Split{{ID: source.WholeStream, State: encodeState(map[string]shardState{"c": {After: "7"}})}}, start)
	if err != nil || len(got) != 1 || got["c"].After != "7" {
		t.Errorf("whole: %v, %v", got, err)
	}
	for _, none := range [][]source.Split{nil, {{ID: source.WholeStream}}} {
		if got, err := splitShards(none, start); err != nil || got != nil {
			t.Errorf("never read: %v, %v", got, err)
		}
	}
	if _, err := splitShards([]source.Split{{ID: "a", State: []byte("{")}}, start); err == nil {
		t.Error("a broken state")
	}
}

func TestChildless(t *testing.T) {
	r := &streamRead{listed: listing(shardOf("p", true), shardOf("c", false, "p"), shardOf("alone", true))}
	if r.childless("p") || !r.childless("alone") || r.childless("expired") {
		t.Error("childless")
	}
}
