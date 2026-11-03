package kafka

import (
	"testing"
	"time"
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
