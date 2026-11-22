package sqlserver

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// A record version's _openetl_seq. With change tracking, its change's
// version, and a snapshot row's, the version its snapshot started at. With
// change data capture, its transaction's commit LSN × 10^13 plus its place
// in the transaction, from 1; a snapshot row's is the LSN its snapshot
// started at, place 0.
var seqType = &arrow.Decimal128Type{Precision: 38, Scale: 0}

const (
	// How often a read that has caught up asks for new changes.
	pollEvery = time.Second
	// How often a stream whose position moved without changes gets a resume
	// point.
	quietEvery = 10 * time.Second
	// How often the read reports its progress.
	progressEvery = 10 * time.Second
)

// cdcState is a cdc stream's state.
type cdcState struct {
	Method string `json:"method"`
	// Every change at or before it has been read; while the snapshot is read,
	// the position its changes are read from after it. A change tracking
	// version, or a change data capture LSN in hex.
	Position string `json:"position"`
	// When Position was the database's latest, by the server's UTC clock:
	// change tracking only.
	At *time.Time `json:"at,omitempty"`
	// While the table's snapshot is read: where it continues.
	Snapshot *snapshotCursor `json:"snapshot,omitempty"`
	// Change tracking: a read of the changes up to a version, partly done.
	Poll *trackingPoll `json:"poll,omitempty"`
	// The position the table's snapshot started at, where a read again
	// from no state reads it from. Empty in a state from before the source
	// kept it.
	Origin string `json:"origin,omitempty"`
}

type snapshotCursor struct {
	// The primary key of the last row read, spelled as text; none before the
	// first chunk.
	After []string `json:"after,omitempty"`
}

type trackingPoll struct {
	To string     `json:"to"`
	At *time.Time `json:"at,omitempty"`
	// The key of the last change read, spelled as text.
	After []string `json:"after"`
}

// cdcMethod is how cdc reads a table's changes: change tracking or change
// data capture.
type cdcMethod interface {
	name() string
	// current is the database's latest position, and when, by the server's
	// UTC clock, if the method keeps it.
	current(ctx context.Context) (string, *time.Time, error)
	// start is the position t's changes are read from after its snapshot.
	start(ctx context.Context, t table) (string, *time.Time, error)
	// unchanged is what a snapshot's query adds to its FROM clause, and a
	// condition: t's row, aliased t, has no change after pos that the read
	// can see. Rows that have are left to the changes read after the
	// snapshot.
	unchanged(ctx context.Context, t table, pos string, a *args) (from, where string, err error)
	// poll reads ct's changes after its position up to to, and moves it
	// there, or to its stop. It returns how many it emitted.
	poll(ctx context.Context, r *cdcReader, ct *cdcTable, to string, at *time.Time) (int, error)
	compare(a, b string) int
	seq(pos string) (decimal128.Num, error)
}

// cdcTable is a table read with cdc.
type cdcTable struct {
	t   table
	b   *batcher
	out source.SplitEmitter
	st  cdcState
	// Where the read ends, and as the request spelled it: its until, or the
	// completion it completes at again, which a completion's position says
	// as an until's does; nil to read on.
	stop  *cdcState
	until []byte
	// The state of its last resume point, and when.
	resumed   []byte
	resumedAt time.Time
	done      bool
}

type cdcReader struct {
	s   *Source
	m   cdcMethod
	out source.Emitter
}

// readCDC reads each table's snapshot, in key order and a chunk at a time,
// with the seq of the position the snapshot started at, leaving out the rows
// changed after it, then its changes after that position, polling for more.
// A read given an end, which reads a part again, leaves out none: the
// changes past its end that would bring them aren't read, so a chunk read
// again has the rows the first read had, and any it left out, as the table
// has them then. The tables take turns on one connection at a time. Nothing
// is written to the source.
func (s *Source) readCDC(ctx context.Context, req source.ReadRequest, cfgs []source.ConfiguredStream, tables []table, out source.Emitter) error {
	var m cdcMethod = &tracking{s: s}
	if s.cfg.CDCMethod == methodCapture {
		m = &capture{s: s}
	}
	r := &cdcReader{s: s, m: m, out: out}
	var reading []*cdcTable
	defer func() {
		for _, ct := range reading {
			ct.b.release()
		}
	}()
	for _, cs := range cfgs {
		ct, err := r.open(ctx, cs, tables)
		if err != nil {
			return err
		}
		reading = append(reading, ct)
	}
	var target string
	if !req.Continuous {
		var err error
		if target, _, err = m.current(ctx); err != nil {
			return err
		}
	}
	nextProgress := time.Now()
	for {
		busy, left := false, 0
		for _, ct := range reading {
			if ct.done {
				continue
			}
			worked, err := r.step(ctx, ct)
			if err != nil {
				return err
			}
			busy = busy || worked
			if !ct.done && !req.Continuous && ct.stop == nil && ct.st.Snapshot == nil && ct.st.Poll == nil && m.compare(ct.st.Position, target) >= 0 {
				if err := r.complete(ct); err != nil {
					return err
				}
			}
			if !ct.done {
				left++
			}
		}
		if left == 0 {
			return nil
		}
		if now := time.Now(); !nextProgress.After(now) {
			r.progress(ctx, reading)
			nextProgress = now.Add(progressEvery)
		}
		if !busy {
			select {
			case <-time.After(pollEvery):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// open sets up a stream's read: from its state, or where its table starts,
// to its end, if it has one.
func (r *cdcReader) open(ctx context.Context, cs source.ConfiguredStream, tables []table) (*cdcTable, error) {
	m := r.m
	cs = wholeStream(cs)
	t := tables[slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })]
	if len(t.key) == 0 {
		return nil, configInvalid("sqlserver: %s has no primary key, so it can't be read with cdc", t.name)
	}
	t, err := declaredOnly(t, cs.Columns)
	if err != nil {
		return nil, err
	}
	end := cs.Until
	if end == nil {
		end = cs.CompleteAt
	}
	ct := &cdcTable{t: t, out: streamEmitter(r.out, t.name), until: end}
	if err := unmarshalState(t.name, cs.State, &ct.st); err != nil {
		return nil, err
	}
	if end != nil {
		ct.stop = &cdcState{}
		if err := unmarshalState(t.name, end, ct.stop); err != nil {
			return nil, err
		}
	}
	switch {
	case ct.st.Method != "" && ct.st.Method != m.name():
		return nil, positionLost("sqlserver: %s was read with %s, and the source now reads with %s", t.name, ct.st.Method, m.name())
	case ct.st.Method != "":
	case ct.stop != nil:
		if ct.st, err = redoFrom(t.name, m.name(), ct.stop); err != nil {
			return nil, err
		}
	default:
		// A new table, or a state from a full refresh, which has no
		// position: the table starts again.
		pos, at, err := m.start(ctx, t)
		if err != nil {
			return nil, err
		}
		ct.st = cdcState{Method: m.name(), Position: pos, At: at, Snapshot: &snapshotCursor{}, Origin: pos}
	}
	if ct.b, err = newBatcher(memory.DefaultAllocator, t.cols, true); err != nil {
		return nil, fmt.Errorf("sqlserver: %s: %w", t.name, err)
	}
	ct.resumed, ct.resumedAt = cs.State, time.Now()
	return ct, nil
}

// redoFrom is where a read again from no state to stop starts: the
// snapshot, at the position the first read's started at, which a stop in
// the snapshot has as its position.
func redoFrom(name, method string, stop *cdcState) (cdcState, error) {
	switch {
	case stop.Snapshot != nil:
		return cdcState{Method: method, Position: stop.Position, At: stop.At, Snapshot: &snapshotCursor{}, Origin: stop.Position}, nil
	case stop.Origin == "":
		return cdcState{}, positionLost("sqlserver: %s: its read again from the start ends past its snapshot, at a state that doesn't say where the snapshot started, "+
			"so the changes after it can't be read again; the stream needs a new snapshot", name)
	}
	return cdcState{Method: method, Position: stop.Origin, Snapshot: &snapshotCursor{}, Origin: stop.Origin}, nil
}

// step reads a chunk of the table's snapshot, or its changes up to the
// database's latest position. It reports whether it emitted anything.
func (r *cdcReader) step(ctx context.Context, ct *cdcTable) (bool, error) {
	if r.reached(ct) {
		return false, r.complete(ct)
	}
	if ct.st.Snapshot != nil {
		return true, r.chunk(ctx, ct)
	}
	var to string
	var at *time.Time
	stop := ct.stop
	switch {
	case ct.st.Poll != nil:
		to, at = ct.st.Poll.To, ct.st.Poll.At
	case stop != nil && r.m.compare(stop.Position, ct.st.Position) > 0:
		// Polls the first read made one after another, as one.
		to, at = stop.Position, stop.At
	case stop != nil && stop.Poll != nil:
		to, at = stop.Poll.To, stop.Poll.At
	case stop != nil:
		return false, fmt.Errorf("sqlserver: %s: the read passed its until", ct.t.name)
	default:
		cur, now, err := r.m.current(ctx)
		if err != nil {
			return false, err
		}
		if r.m.compare(cur, ct.st.Position) <= 0 {
			return false, nil
		}
		to, at = cur, now
	}
	n, err := r.m.poll(ctx, r, ct, to, at)
	if err != nil {
		return false, err
	}
	switch {
	case r.reached(ct):
		return n > 0, r.complete(ct)
	case n > 0 || time.Since(ct.resumedAt) >= quietEvery:
		return n > 0, r.resume(ct)
	}
	return false, nil
}

// chunk reads the next chunk of a table's snapshot.
func (r *cdcReader) chunk(ctx context.Context, ct *cdcTable) error {
	t := ct.t
	keys := t.keyCols()
	krefs := refs(keys, "t")
	var a args
	var from string
	var where []string
	if ct.stop == nil {
		var cond string
		var err error
		if from, cond, err = r.m.unchanged(ctx, t, ct.st.Position, &a); err != nil {
			return err
		}
		where = append(where, cond)
	}
	if after := ct.st.Snapshot.After; after != nil {
		if len(after) != len(keys) {
			return fmt.Errorf("sqlserver: %s state has %d key columns, the table has %d", t.name, len(after), len(keys))
		}
		where = append(where, afterKey(keys, krefs, after, &a))
	}
	bounded := ct.stop != nil && ct.stop.Snapshot != nil
	if bounded {
		where = append(where, upToKey(keys, krefs, ct.stop.Snapshot.After, &a))
	}
	text := make([]string, len(keys))
	for i, k := range keys {
		text[i] = k.textOf(krefs[i])
	}
	cond := ""
	if len(where) > 0 {
		cond = " WHERE " + strings.Join(where, " AND ")
	}
	q := "SELECT TOP (" + strconv.Itoa(r.s.cfg.SnapshotChunkRows) + ") " + selectList(t.cols, "t") + ", " + strings.Join(text, ", ") +
		" FROM " + qualified(r.s.cfg.Schema, t.name) + " AS t" + from + cond + " ORDER BY " + strings.Join(krefs, ", ")
	var err error
	if ct.b.rowSeq, err = r.m.seq(ct.st.Position); err != nil {
		return err
	}
	ct.b.rowOp = source.OpInsert
	n, last, err := r.s.query(ctx, r.s.db, ct.b, t.name, q, a, len(keys), ct.out)
	if err != nil {
		return err
	}
	switch {
	case n == r.s.cfg.SnapshotChunkRows:
		ct.st.Snapshot = &snapshotCursor{After: last}
	case bounded:
		ct.st = *ct.stop
	default:
		ct.st.Snapshot = nil
	}
	return r.resume(ct)
}

// reached reports whether the table's read is at its stop.
func (r *cdcReader) reached(ct *cdcTable) bool {
	return ct.stop != nil && r.samePlace(ct.st, *ct.stop)
}

// samePlace reports whether two states are at the same place in a read: the
// same position, and the same key of the same snapshot or poll. When a
// position was the database's latest doesn't count.
func (r *cdcReader) samePlace(a, b cdcState) bool {
	if a.Method != b.Method || r.m.compare(a.Position, b.Position) != 0 || (a.Snapshot == nil) != (b.Snapshot == nil) || (a.Poll == nil) != (b.Poll == nil) {
		return false
	}
	if a.Snapshot != nil && !slices.Equal(a.Snapshot.After, b.Snapshot.After) {
		return false
	}
	return a.Poll == nil || r.m.compare(a.Poll.To, b.Poll.To) == 0 && slices.Equal(a.Poll.After, b.Poll.After)
}

// state is the table's state, spelled as the request spelled its until if
// it's there.
func (r *cdcReader) state(ct *cdcTable) ([]byte, error) {
	if r.reached(ct) {
		return ct.until, nil
	}
	return json.Marshal(ct.st)
}

// resume flushes the table's batch and emits a resume point, unless its
// last was at the same state.
func (r *cdcReader) resume(ct *cdcTable) error {
	if err := ct.b.flush(ct.out); err != nil {
		return err
	}
	b, err := r.state(ct)
	if err != nil {
		return err
	}
	if bytes.Equal(b, ct.resumed) {
		return nil
	}
	if err := ct.out.ResumePoint(b); err != nil {
		return err
	}
	ct.resumed, ct.resumedAt = b, time.Now()
	return nil
}

// complete ends the table's bounded read where it is, after a resume point
// there, so a read from that resume point completes at the same state.
func (r *cdcReader) complete(ct *cdcTable) error {
	if err := r.resume(ct); err != nil {
		return err
	}
	if err := ct.out.Complete(ct.resumed); err != nil {
		return err
	}
	ct.done = true
	return nil
}

// progress reports how far each table is behind, and how long the source
// keeps its changes.
func (r *cdcReader) progress(ctx context.Context, tables []*cdcTable) {
	pr, ok := r.out.(source.Progresser)
	if !ok {
		return
	}
	for _, ct := range tables {
		if ct.done || ct.st.Snapshot != nil {
			continue
		}
		p, err := r.s.progressOf(ctx, ct.t.name, ct.st)
		if err != nil || p == nil {
			continue
		}
		if err := pr.Progress(*p); err != nil {
			return
		}
	}
}

// wholeStream is cs with its one split, the whole stream, as its State,
// Until and CompleteAt: a request may give a stream read whole either way.
// Other splits, a read's in another mode, are left out, so the table starts
// again, as Splits says.
func wholeStream(cs source.ConfiguredStream) source.ConfiguredStream {
	if len(cs.Splits) == 1 && cs.Splits[0].ID == source.WholeStream {
		sp := cs.Splits[0]
		cs.State, cs.Until, cs.CompleteAt = sp.State, sp.Until, sp.CompleteAt
	}
	cs.Splits = nil
	return cs
}

// declaredOnly narrows t to the columns the run declared. A column added
// since the run's Discover has no declared type: the read fails, retryably,
// and the next attempt declares it. No columns: the request doesn't say.
func declaredOnly(t table, columns []string) (table, error) {
	if len(columns) == 0 {
		return t, nil
	}
	for _, c := range t.cols {
		if !slices.Contains(columns, c.name) {
			return t, transient("sqlserver: %s has a new column %q; the read starts again to declare it", t.name, c.name)
		}
	}
	return t, nil
}

// tracking reads changes through change tracking: each poll reads, in key
// order, the keys changed since the position, with their rows as they are
// now; a delete has its key alone. A key changed more than once between
// polls arrives once, with its latest version.
type tracking struct{ s *Source }

func (m *tracking) name() string { return methodTracking }

func (m *tracking) current(ctx context.Context) (string, *time.Time, error) {
	var v *int64
	var at time.Time
	if err := m.s.db.QueryRowContext(ctx, "SELECT CHANGE_TRACKING_CURRENT_VERSION(), SYSUTCDATETIME()").Scan(&v, &at); err != nil {
		return "", nil, classify(fmt.Errorf("sqlserver: change tracking: %w", err))
	}
	if v == nil {
		return "", nil, configInvalid("sqlserver: change tracking is off for the database: ALTER DATABASE ... SET CHANGE_TRACKING = ON")
	}
	at = at.UTC()
	return strconv.FormatInt(*v, 10), &at, nil
}

func (m *tracking) start(ctx context.Context, t table) (string, *time.Time, error) {
	pos, at, err := m.current(ctx)
	if err != nil {
		return "", nil, err
	}
	min, err := m.minValid(ctx, t)
	if err != nil {
		return "", nil, err
	}
	if min == nil {
		return "", nil, configInvalid("sqlserver: %s: change tracking is off for the table, or the user lacks VIEW CHANGE TRACKING on it: ALTER TABLE %s ENABLE CHANGE_TRACKING",
			t.name, qualified(m.s.cfg.Schema, t.name))
	}
	return pos, at, nil
}

func (m *tracking) minValid(ctx context.Context, t table) (*int64, error) {
	var min *int64
	if err := m.s.db.QueryRowContext(ctx, "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@p1))", qualified(m.s.cfg.Schema, t.name)).Scan(&min); err != nil {
		return nil, classify(fmt.Errorf("sqlserver: change tracking: %w", err))
	}
	return min, nil
}

// valid fails with POSITION_LOST once the table's changes after v aren't
// all kept.
func (m *tracking) valid(ctx context.Context, t table, v int64) error {
	min, err := m.minValid(ctx, t)
	switch {
	case err != nil:
		return err
	case min == nil:
		return positionLost("sqlserver: %s: change tracking is off for the table now, or the user lacks VIEW CHANGE TRACKING on it", t.name)
	case *min > v:
		return positionLost("sqlserver: %s: the changes after version %d have been cleaned up, or the table was truncated; the oldest version kept is %d", t.name, v, *min)
	}
	return nil
}

func (m *tracking) unchanged(_ context.Context, t table, pos string, a *args) (string, string, error) {
	v, err := strconv.ParseInt(pos, 10, 64)
	if err != nil {
		return "", "", fmt.Errorf("sqlserver: %s: a change tracking version %q", t.name, pos)
	}
	keys := t.keyCols()
	return " CROSS APPLY CHANGETABLE(VERSION " + qualified(m.s.cfg.Schema, t.name) + ", (" + strings.Join(refs(keys, ""), ", ") + "), (" + strings.Join(refs(keys, "t"), ", ") + ")) AS v",
		"(v.SYS_CHANGE_VERSION IS NULL OR v.SYS_CHANGE_VERSION <= " + a.add(v) + ")", nil
}

func (m *tracking) poll(ctx context.Context, r *cdcReader, ct *cdcTable, to string, at *time.Time) (int, error) {
	t := ct.t
	v, err1 := strconv.ParseInt(ct.st.Position, 10, 64)
	w, err2 := strconv.ParseInt(to, 10, 64)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("sqlserver: %s: change tracking versions %q and %q", t.name, ct.st.Position, to)
	}
	if err := m.valid(ctx, t, v); err != nil {
		return 0, err
	}
	p := ct.st.Poll
	if p == nil {
		p = &trackingPoll{To: to, At: at}
	}
	keys := t.keyCols()
	crefs, trefs := refs(keys, "c"), refs(keys, "t")
	sel := make([]string, len(t.cols))
	for i, c := range t.cols {
		alias := "t"
		if slices.Contains(t.key, i) {
			alias = "c"
		}
		sel[i] = c.selectExpr(alias)
	}
	on := make([]string, len(keys))
	text := make([]string, len(keys))
	for i, k := range keys {
		on[i] = trefs[i] + " = " + crefs[i]
		text[i] = k.textOf(crefs[i])
	}
	stop := ct.stop
	bounded := stop != nil && stop.Poll != nil && stop.Position == ct.st.Position && stop.Poll.To == to
	ident := qualified(m.s.cfg.Schema, t.name)
	limit := r.s.cfg.ChunkRows
	total := 0
	for {
		var a args
		vp, wp := a.add(v), a.add(w)
		where := []string{"c.SYS_CHANGE_VERSION <= " + wp}
		if p.After != nil {
			if len(p.After) != len(keys) {
				return total, fmt.Errorf("sqlserver: %s state has %d key columns, the table has %d", t.name, len(p.After), len(keys))
			}
			where = append(where, afterKey(keys, crefs, p.After, &a))
		}
		if bounded {
			where = append(where, upToKey(keys, crefs, stop.Poll.After, &a))
		}
		q := "SELECT TOP (" + strconv.Itoa(limit) + ") " + strings.Join(sel, ", ") + ", c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, CASE WHEN " + trefs[0] + " IS NULL THEN 0 ELSE 1 END, " +
			strings.Join(text, ", ") + " FROM CHANGETABLE(CHANGES " + ident + ", " + vp + ") AS c LEFT JOIN " + ident + " AS t ON " + strings.Join(on, " AND ") +
			" WHERE " + strings.Join(where, " AND ") + " ORDER BY " + strings.Join(crefs, ", ")
		n, last, err := m.changes(ctx, r, ct, q, a)
		total += n
		if err != nil {
			return total, err
		}
		// The changes read are complete only if none were cleaned up while
		// they were read.
		if err := m.valid(ctx, t, v); err != nil {
			return total, err
		}
		switch {
		case n == limit:
			p.After = last
			ct.st.Poll = p
			if err := r.resume(ct); err != nil {
				return total, err
			}
			if r.reached(ct) {
				return total, nil
			}
		case bounded:
			ct.st = *stop
			return total, nil
		default:
			ct.st.Position, ct.st.At, ct.st.Poll = to, at, nil
			return total, nil
		}
	}
}

// changes runs a change tracking poll's query and emits its changes,
// returning the last one's key.
func (m *tracking) changes(ctx context.Context, r *cdcReader, ct *cdcTable, q string, a args) (int, []string, error) {
	rows, err := r.s.db.QueryContext(ctx, q, a...)
	if err != nil {
		return 0, nil, classify(fmt.Errorf("sqlserver: read %s's changes: %w", ct.t.name, err))
	}
	defer func() { _ = rows.Close() }()
	nc := len(ct.t.cols)
	vals := make([]any, nc+3+len(ct.t.key))
	ptrs := make([]any, len(vals))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	n := 0
	var last []string
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return n, nil, classify(fmt.Errorf("sqlserver: read %s's changes: %w", ct.t.name, err))
		}
		version, _ := vals[nc].(int64)
		kind, _ := text(vals[nc+1])
		exists, _ := vals[nc+2].(int64)
		op := source.OpUpdate
		switch {
		case exists == 0:
			op = source.OpDelete
		case strings.TrimSpace(kind) == "I":
			op = source.OpInsert
		}
		if err := ct.b.addChange(vals[:nc], op, decimal128.FromI64(version), nil); err != nil {
			return n, nil, err
		}
		if ct.b.bytes >= r.s.cfg.BatchBytes {
			if err := ct.b.flush(ct.out); err != nil {
				return n, nil, err
			}
		}
		last = last[:0]
		for _, v := range vals[nc+3:] {
			s, _ := text(v)
			last = append(last, s)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return n, nil, classify(fmt.Errorf("sqlserver: read %s's changes: %w", ct.t.name, err))
	}
	return n, slices.Clone(last), nil
}

func (m *tracking) compare(a, b string) int {
	x, _ := strconv.ParseInt(a, 10, 64)
	y, _ := strconv.ParseInt(b, 10, 64)
	return cmpInt(x, y)
}

func cmpInt(x, y int64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

func (m *tracking) seq(pos string) (decimal128.Num, error) {
	v, err := strconv.ParseInt(pos, 10, 64)
	if err != nil {
		return decimal128.Num{}, fmt.Errorf("sqlserver: a change tracking version %q", pos)
	}
	return decimal128.FromI64(v), nil
}

// capture reads changes through change data capture: each poll reads the
// transactions the capture job has harvested since the position, in commit
// order, every change with its whole row as the capture instance has it.
// Where a table has two capture instances, as while its schema changes, the
// newer one is read from its start.
type capture struct{ s *Source }

func (m *capture) name() string { return methodCapture }

func (m *capture) current(ctx context.Context) (string, *time.Time, error) {
	var l []byte
	if err := m.s.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&l); err != nil {
		return "", nil, classify(fmt.Errorf("sqlserver: change data capture: %w", err))
	}
	if l == nil {
		return "", nil, configInvalid("sqlserver: change data capture is off for the database: EXEC sys.sp_cdc_enable_db")
	}
	return lsnText(l), nil, nil
}

type captureInstance struct {
	name string
	// Nil until the capture job has started it.
	start *big.Int
	// The columns it captures.
	cols []string
}

// instances are t's capture instances, oldest first.
func (m *capture) instances(ctx context.Context, t table) ([]captureInstance, error) {
	rows, err := m.s.db.QueryContext(ctx, `SELECT ct.capture_instance, ct.start_lsn, cc.column_name
		FROM cdc.change_tables ct JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id
		WHERE ct.source_object_id = OBJECT_ID(@p1) ORDER BY ct.create_date, ct.capture_instance, cc.column_ordinal`, qualified(m.s.cfg.Schema, t.name))
	if err != nil {
		return nil, classify(fmt.Errorf("sqlserver: %s's capture instances: %w", t.name, err))
	}
	defer func() { _ = rows.Close() }()
	var out []captureInstance
	for rows.Next() {
		var name, col string
		var start []byte
		if err := rows.Scan(&name, &start, &col); err != nil {
			return nil, classify(err)
		}
		if len(out) == 0 || out[len(out)-1].name != name {
			in := captureInstance{name: name}
			if start != nil {
				in.start = new(big.Int).SetBytes(start)
			}
			out = append(out, in)
		}
		out[len(out)-1].cols = append(out[len(out)-1].cols, col)
	}
	return out, classify(rows.Err())
}

func (m *capture) start(ctx context.Context, t table) (string, *time.Time, error) {
	ins, err := m.instances(ctx, t)
	if err != nil {
		return "", nil, err
	}
	if len(ins) == 0 {
		return "", nil, configInvalid("sqlserver: %s isn't captured: EXEC sys.sp_cdc_enable_table @source_schema = %s, @source_name = %s, @role_name = NULL",
			t.name, literal(m.s.cfg.Schema), literal(t.name))
	}
	cur, _, err := m.current(ctx)
	if err != nil {
		return "", nil, err
	}
	low, err := m.minLSN(ctx, ins[0].name)
	if err != nil {
		return "", nil, err
	}
	// A table captured since the capture job last harvested starts before
	// its instance's first change.
	pos := lsnInt(cur)
	if before := new(big.Int).Sub(low, big.NewInt(1)); before.Cmp(pos) > 0 {
		pos = before
	}
	return lsnText(lsnBytes(pos)), nil, nil
}

func (m *capture) minLSN(ctx context.Context, instance string) (*big.Int, error) {
	var l []byte
	if err := m.s.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(@p1)", instance).Scan(&l); err != nil {
		return nil, classify(fmt.Errorf("sqlserver: change data capture: %w", err))
	}
	return new(big.Int).SetBytes(l), nil
}

// unchanged sees only the changes the capture job has harvested.
func (m *capture) unchanged(ctx context.Context, t table, pos string, a *args) (string, string, error) {
	ins, err := m.instances(ctx, t)
	if err != nil {
		return "", "", err
	}
	if len(ins) == 0 {
		return "", "", positionLost("sqlserver: %s isn't captured any more", t.name)
	}
	at := a.add(lsnBytes(lsnInt(pos)))
	keys := refs(t.keyCols(), "")
	var conds []string
	for _, in := range ins {
		on := make([]string, len(keys))
		for i, k := range keys {
			on[i] = "x." + k + " = t." + k
		}
		conds = append(conds, "NOT EXISTS (SELECT 1 FROM cdc."+quote(in.name+"_CT")+" AS x WHERE x.__$start_lsn > "+at+" AND "+strings.Join(on, " AND ")+")")
	}
	return "", strings.Join(conds, " AND "), nil
}

func (m *capture) poll(ctx context.Context, r *cdcReader, ct *cdcTable, to string, _ *time.Time) (int, error) {
	t := ct.t
	from := new(big.Int).Add(lsnInt(ct.st.Position), big.NewInt(1))
	end := lsnInt(to)
	ins, err := m.instances(ctx, t)
	if err != nil {
		return 0, err
	}
	if len(ins) == 0 {
		return 0, positionLost("sqlserver: %s isn't captured any more", t.name)
	}
	total := 0
	for _, rg := range instanceRanges(ins, from, end) {
		in, lo, hi := ins[rg.i], rg.lo, rg.hi
		low, err := m.minLSN(ctx, in.name)
		if err != nil {
			return total, err
		}
		if low.Cmp(lo) > 0 {
			return total, positionLost("sqlserver: %s: the changes from %s have been cleaned up, or capture instance %s started after them; the oldest it keeps is %s",
				t.name, lsnText(lsnBytes(lo)), in.name, lsnText(lsnBytes(low)))
		}
		n, err := m.read(ctx, r, ct, in, lo, hi)
		total += n
		if err != nil {
			return total, err
		}
	}
	ct.st.Position = to
	return total, nil
}

// instanceRange is the changes from lo to hi, inclusive, that a table's
// capture instance ins[i] gives a poll.
type instanceRange struct {
	i      int
	lo, hi *big.Int
}

// instanceRanges divides the changes from lo to hi between a table's capture
// instances, oldest first: each up to where the next starts, and a newer one
// from its start. A newer one the capture job hasn't started has none: until
// it starts, the older one has them all.
func instanceRanges(ins []captureInstance, lo, hi *big.Int) []instanceRange {
	var out []instanceRange
	for i, in := range ins {
		if i > 0 && in.start == nil {
			continue
		}
		from, to := lo, hi
		if i > 0 && in.start.Cmp(from) > 0 {
			from = in.start
		}
		if i+1 < len(ins) && ins[i+1].start != nil {
			if before := new(big.Int).Sub(ins[i+1].start, big.NewInt(1)); before.Cmp(to) < 0 {
				to = before
			}
		}
		if from.Cmp(to) <= 0 {
			out = append(out, instanceRange{i, from, to})
		}
	}
	return out
}

// The ops of __$operation: a delete's row, an insert's, and an update's row
// after it; 3, its row before, is skipped.
var captureOps = map[int64]string{1: source.OpDelete, 2: source.OpInsert, 4: source.OpUpdate}

// read emits the changes one capture instance has from lo to hi, with a
// resume point between transactions every chunkRows changes.
func (m *capture) read(ctx context.Context, r *cdcReader, ct *cdcTable, in captureInstance, lo, hi *big.Int) (int, error) {
	t := ct.t
	sel := make([]string, len(t.cols))
	var unchanged []string
	for i, c := range t.cols {
		// The change tables hold no values of computed columns.
		if slices.Contains(in.cols, c.name) && !c.computed {
			sel[i] = c.selectExpr("c")
		} else {
			sel[i] = "NULL"
			unchanged = append(unchanged, c.name)
		}
	}
	// In a transaction's order, __$command_id's, as fn_cdc_get_all_changes
	// can't give it: a statement's deletes come before its inserts, and an
	// insert shares its __$seqval with the delete of the row it was, which
	// in a heap has another key. An update's row before it, 3, isn't read.
	q := "SELECT " + strings.Join(sel, ", ") + ", c.__$start_lsn, CONVERT(bigint, c.__$operation) FROM cdc." + quote(in.name+"_CT") +
		" AS c WHERE c.__$start_lsn BETWEEN @p1 AND @p2 AND c.__$operation <> 3 ORDER BY c.__$start_lsn, c.__$command_id, c.__$seqval, c.__$operation"
	rows, err := r.s.db.QueryContext(ctx, q, lsnBytes(lo), lsnBytes(hi))
	if err != nil {
		return 0, classify(fmt.Errorf("sqlserver: read %s's changes: %w", t.name, err))
	}
	defer func() { _ = rows.Close() }()
	nc := len(t.cols)
	vals := make([]any, nc+2)
	ptrs := make([]any, len(vals))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	// Cleanup can remove changes the query hasn't reached yet, so at each
	// resume point, and at the end, the oldest change kept is still at most
	// the first read since the last.
	start := lo
	kept := func() error {
		low, err := m.minLSN(ctx, in.name)
		if err != nil {
			return err
		}
		if low.Cmp(start) > 0 {
			return positionLost("sqlserver: %s: the changes from %s were cleaned up while they were read; the oldest capture instance %s keeps is %s",
				t.name, lsnText(lsnBytes(start)), in.name, lsnText(lsnBytes(low)))
		}
		return nil
	}
	var txn []byte
	var place int64
	n, since := 0, 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return n, classify(fmt.Errorf("sqlserver: read %s's changes: %w", t.name, err))
		}
		lsn, _ := vals[nc].([]byte)
		op, ok := captureOps[vals[nc+1].(int64)]
		if !ok {
			continue
		}
		if !bytes.Equal(lsn, txn) {
			if txn != nil && since >= r.s.cfg.ChunkRows {
				if err := kept(); err != nil {
					return n, err
				}
				ct.st.Position = lsnText(txn)
				if err := r.resume(ct); err != nil {
					return n, err
				}
				start = new(big.Int).Add(new(big.Int).SetBytes(txn), big.NewInt(1))
				since = 0
			}
			txn, place = slices.Clone(lsn), 0
		}
		place++
		seq := decimal128.FromBigInt(new(big.Int).Add(new(big.Int).Mul(new(big.Int).SetBytes(lsn), lsnShift), big.NewInt(place)))
		if err := ct.b.addChange(vals[:nc], op, seq, unchanged); err != nil {
			return n, err
		}
		if ct.b.bytes >= r.s.cfg.BatchBytes {
			if err := ct.b.flush(ct.out); err != nil {
				return n, err
			}
		}
		n++
		since++
	}
	if err := rows.Err(); err != nil {
		return n, classify(err)
	}
	return n, kept()
}

var lsnShift = big.NewInt(10_000_000_000_000)

func (m *capture) compare(a, b string) int { return lsnInt(a).Cmp(lsnInt(b)) }

func (m *capture) seq(pos string) (decimal128.Num, error) {
	return decimal128.FromBigInt(new(big.Int).Mul(lsnInt(pos), lsnShift)), nil
}

// An LSN is spelled 0x and 20 hex digits, as SQL Server spells binary(10).
func lsnText(l []byte) string { return "0x" + strings.ToUpper(hex.EncodeToString(l)) }

func lsnInt(s string) *big.Int {
	b, _ := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	return new(big.Int).SetBytes(b)
}

func lsnBytes(n *big.Int) []byte { return n.FillBytes(make([]byte, 10)) }
