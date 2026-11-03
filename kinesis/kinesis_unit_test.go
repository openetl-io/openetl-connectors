package kinesis

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl-connectors/records"
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
