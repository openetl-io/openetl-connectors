package connectortest

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// SplitEvent is what a split emitted: rows' values in one column, or a resume
// point or completion.
type SplitEvent struct {
	Stream, Split string
	Rows          []string
	Resume        bool
	Complete      bool
	State         string
	Added         []source.Split
}

// SplitCapture records what a source emits on each split, in order, keeping
// one column of its rows.
type SplitCapture struct {
	Col string
	// Runs on the split's reader for each batch, before it's recorded.
	OnBatch func(stream, split string)

	mu     sync.Mutex
	events []SplitEvent
}

func (c *SplitCapture) Rows(stream string, _ []json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", stream)
}

func (c *SplitCapture) Records(stream string, b arrow.RecordBatch) error {
	return c.Split(stream, source.WholeStream).Records(b)
}

func (c *SplitCapture) ResumePoint(stream string, state []byte) error {
	return c.Split(stream, source.WholeStream).ResumePoint(state)
}

func (c *SplitCapture) Complete(stream string, next []byte) error {
	return c.Split(stream, source.WholeStream).Complete(next)
}

func (c *SplitCapture) Split(stream, id string) source.SplitEmitter {
	return splitSink{c, stream, id}
}

// add keeps e with a copy of its added splits, which an emitter's caller may
// reuse once the call returns.
func (c *SplitCapture) add(e SplitEvent) {
	e.Added = cloneSplits(e.Added)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

type splitSink struct {
	c          *SplitCapture
	stream, id string
}

func (s splitSink) Rows([]json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", s.stream)
}

func (s splitSink) Records(b arrow.RecordBatch) error {
	defer b.Release()
	if s.c.OnBatch != nil {
		s.c.OnBatch(s.stream, s.id)
	}
	col := b.Column(b.Schema().FieldIndices(s.c.Col)[0])
	// A column may hold more values than the batch has rows.
	rows := make([]string, b.NumRows())
	for i := range rows {
		rows[i] = col.ValueStr(i)
	}
	s.c.add(SplitEvent{Stream: s.stream, Split: s.id, Rows: rows})
	return nil
}

func (s splitSink) ResumePoint(state []byte, added ...source.Split) error {
	s.c.add(SplitEvent{Stream: s.stream, Split: s.id, Resume: true, State: string(state), Added: added})
	return nil
}

func (s splitSink) Complete(next []byte, added ...source.Split) error {
	s.c.add(SplitEvent{Stream: s.stream, Split: s.id, Complete: true, State: string(next), Added: added})
	return nil
}

// Events are what the read emitted, in order.
func (c *SplitCapture) Events() []SplitEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}

// Values are the column's Values read, in the order they were emitted.
func (c *SplitCapture) Values() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.events {
		out = append(out, e.Rows...)
	}
	return out
}

// Of are the events of split, in order.
func (c *SplitCapture) Of(split string) []SplitEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []SplitEvent
	for _, e := range c.events {
		if e.Split == split {
			out = append(out, e)
		}
	}
	return out
}

// Added are the splits the read added, in order.
func (c *SplitCapture) Added() []source.Split {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []source.Split
	for _, e := range c.events {
		out = append(out, e.Added...)
	}
	return out
}

// ValuesOf are the column's values in events, in order.
func ValuesOf(events []SplitEvent) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.Rows...)
	}
	return out
}
