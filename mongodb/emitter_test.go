package mongodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func codeOf(err error) protocolv1.ErrorCode {
	var c interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &c) {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

// intID is an int32 _id's text.
func intID(i int) string { return fmt.Sprintf(`{"$numberInt":"%d"}`, i) }

// recorder is an emitter that keeps what a read emits, in order.
type recorder struct {
	mu     sync.Mutex
	events []emitted
}

type emitted struct {
	stream, split string
	// A batch's rows, each field's value as text; a null isn't there.
	rows             []map[string]string
	resume, complete bool
	state            []byte
	added            []source.Split
}

func (r *recorder) add(e emitted) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

// snapshot is what's emitted so far.
func (r *recorder) snapshot() []emitted {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]emitted(nil), r.events...)
}

// rows are every row emitted for stream.
func (r *recorder) rows(stream string) []map[string]string {
	var out []map[string]string
	for _, e := range r.snapshot() {
		if e.stream == stream {
			out = append(out, e.rows...)
		}
	}
	return out
}

// last is the last resume point's or completion's state for stream.
func (r *recorder) last(stream string) (state []byte, complete bool) {
	for _, e := range r.snapshot() {
		if e.stream == stream && (e.resume || e.complete) {
			state, complete = e.state, e.complete
		}
	}
	return state, complete
}

func (r *recorder) Rows(string, []json.RawMessage) error { return errors.New("rows aren't expected") }
func (r *recorder) Records(stream string, b arrow.RecordBatch) error {
	return r.Split(stream, source.WholeStream).Records(b)
}
func (r *recorder) ResumePoint(stream string, state []byte) error {
	return r.add(emitted{stream: stream, resume: true, state: state})
}
func (r *recorder) Complete(stream string, next []byte) error {
	return r.add(emitted{stream: stream, complete: true, state: next})
}
func (r *recorder) Split(stream, id string) source.SplitEmitter {
	return &splitRecorder{r: r, stream: stream, id: id}
}

type splitRecorder struct {
	r          *recorder
	stream, id string
}

func (s *splitRecorder) Rows([]json.RawMessage) error { return errors.New("rows aren't expected") }
func (s *splitRecorder) Records(b arrow.RecordBatch) error {
	defer b.Release()
	rows := make([]map[string]string, b.NumRows())
	for i := range rows {
		rows[i] = map[string]string{}
		for c, f := range b.Schema().Fields() {
			if col := b.Column(c); !col.IsNull(i) {
				rows[i][f.Name] = col.ValueStr(i)
			}
		}
	}
	return s.r.add(emitted{stream: s.stream, split: s.id, rows: rows})
}
func (s *splitRecorder) ResumePoint(state []byte, added ...source.Split) error {
	return s.r.add(emitted{stream: s.stream, split: s.id, resume: true, state: state, added: added})
}
func (s *splitRecorder) Complete(next []byte, added ...source.Split) error {
	return s.r.add(emitted{stream: s.stream, split: s.id, complete: true, state: next, added: added})
}
