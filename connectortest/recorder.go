// Package connectortest is what the connectors' tests share: recorders of
// what a read emits, and a check's items' statuses.
package connectortest

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// CodeOf is err's protocol code, unspecified without one.
func CodeOf(err error) protocolv1.ErrorCode {
	var c interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &c) {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

// Recorder is an emitter that keeps what a read emits, in order.
type Recorder struct {
	mu     sync.Mutex
	events []Emitted
}

type Emitted struct {
	Stream, Split string
	// A batch's rows, each field's value as text; a null isn't there.
	Rows             []map[string]string
	Resume, Complete bool
	State            []byte
	Added            []source.Split
}

func (r *Recorder) add(e Emitted) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

// Snapshot is what's emitted so far.
func (r *Recorder) Snapshot() []Emitted {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Emitted(nil), r.events...)
}

// RowsOf are every row emitted for stream.
func (r *Recorder) RowsOf(stream string) []map[string]string {
	var out []map[string]string
	for _, e := range r.Snapshot() {
		if e.Stream == stream {
			out = append(out, e.Rows...)
		}
	}
	return out
}

// Last is the Last resume point's or completion's state for stream.
func (r *Recorder) Last(stream string) (state []byte, complete bool) {
	for _, e := range r.Snapshot() {
		if e.Stream == stream && (e.Resume || e.Complete) {
			state, complete = e.State, e.Complete
		}
	}
	return state, complete
}

func (r *Recorder) Rows(string, []json.RawMessage) error { return errors.New("rows aren't expected") }
func (r *Recorder) Records(stream string, b arrow.RecordBatch) error {
	return r.Split(stream, source.WholeStream).Records(b)
}
func (r *Recorder) ResumePoint(stream string, state []byte) error {
	return r.add(Emitted{Stream: stream, Resume: true, State: state})
}
func (r *Recorder) Complete(stream string, next []byte) error {
	return r.add(Emitted{Stream: stream, Complete: true, State: next})
}
func (r *Recorder) Split(stream, id string) source.SplitEmitter {
	return &splitRecorder{r: r, stream: stream, id: id}
}

type splitRecorder struct {
	r          *Recorder
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
	return s.r.add(Emitted{Stream: s.stream, Split: s.id, Rows: rows})
}
func (s *splitRecorder) ResumePoint(state []byte, added ...source.Split) error {
	return s.r.add(Emitted{Stream: s.stream, Split: s.id, Resume: true, State: state, Added: added})
}
func (s *splitRecorder) Complete(next []byte, added ...source.Split) error {
	return s.r.add(Emitted{Stream: s.stream, Split: s.id, Complete: true, State: next, Added: added})
}
