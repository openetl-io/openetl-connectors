package kafka

import (
	"maps"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestProgress(t *testing.T) {
	now := time.Now()
	r := &topicRead{name: "t", next: map[int32]int64{0: 5, 1: 9}, hwm: map[int32]int64{0: 8, 1: 9}, last: now.Add(-time.Hour), retention: 7 * 24 * time.Hour}
	p := r.progress(now)
	if *p.LagRecords != 3 || *p.Lag != time.Hour || p.RetentionLeft == nil || *p.RetentionLeft != 7*24*time.Hour-time.Hour {
		t.Errorf("behind: %+v", p)
	}
	r.next[0] = 8
	if p := r.progress(now); *p.LagRecords != 0 || *p.RetentionLeft != 7*24*time.Hour {
		t.Errorf("caught up: %+v", p)
	}
	r.next[0], r.last = 5, time.Time{}
	if p := r.progress(now); p.RetentionLeft != nil {
		t.Errorf("behind, with no record read: %+v", p)
	}
	r.retention = 0
	if p := r.progress(now); p.RetentionLeft != nil {
		t.Errorf("unknown retention: %+v", p)
	}
}

func TestSplitOffsets(t *testing.T) {
	got, err := splitOffsets([]source.Split{{ID: "p0", State: encodeOffset(5)}, {ID: "p12", State: encodeOffset(0)}, {ID: "p3"}})
	if err != nil || !maps.Equal(got, map[int32]int64{0: 5, 12: 0}) {
		t.Errorf("splits: %v, %v", got, err)
	}
	got, err = splitOffsets([]source.Split{{ID: source.WholeStream, State: encodeState(map[int32]int64{1: 7, 2: 9})}})
	if err != nil || !maps.Equal(got, map[int32]int64{1: 7, 2: 9}) {
		t.Errorf("whole: %v, %v", got, err)
	}
	got, err = splitOffsets(nil)
	if err != nil || len(got) != 0 {
		t.Errorf("never read: %v, %v", got, err)
	}
	for _, id := range []string{"0", "px", "shardId-1"} {
		if _, err := splitOffsets([]source.Split{{ID: id}}); err == nil {
			t.Errorf("split %q isn't a partition", id)
		}
	}
	if _, err := splitOffsets([]source.Split{{ID: "p1", State: []byte("{")}}); err == nil {
		t.Error("a broken state")
	}
}
