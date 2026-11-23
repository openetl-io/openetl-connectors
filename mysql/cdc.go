package mysql

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

const (
	// How often streams that emitted since their last resume point get one,
	// and how often the rest do, once the binlog has moved on.
	resumeEvery = time.Second
	quietEvery  = 10 * time.Second
	// How often the server sends a heartbeat on an idle binlog, which says
	// where it ends.
	heartbeatEvery = time.Second
	// How often the read reports its progress, and logs it.
	progressEvery = 10 * time.Second
	logLagEvery   = time.Minute
)

// A record version's _openetl_seq: the epoch, which grows when the read moves
// to another server; the binlog file's number and the position its
// transaction starts at; then the row's place in the transaction, which is 0
// for a snapshot's rows.
var seqType = &arrow.Decimal128Type{Precision: 38, Scale: 0}

func seqOf(epoch uint32, p binlogPos, n uint64) decimal128.Num {
	return decimal128.New(int64(epoch)<<32|int64(p.number()), uint64(p.Pos)<<32|n)
}

// afterChunkSnapshot, if set, runs between a chunk's rows and its binlog
// position: for tests.
var afterChunkSnapshot func()

// binlogPos is a place in a server's binlog.
type binlogPos struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

// number is the file's sequence number, its name's numeric extension.
func (p binlogPos) number() uint32 {
	_, ext, _ := strings.Cut(p.File[strings.LastIndexByte(p.File, '/')+1:], ".")
	n, _ := strconv.ParseUint(ext, 10, 31)
	return uint32(n)
}

func (p binlogPos) less(q binlogPos) bool {
	if p.number() != q.number() {
		return p.number() < q.number()
	}
	return p.Pos < q.Pos
}

// cdcState is a CDC stream's state.
type cdcState struct {
	// The server read: MySQL's server_uuid, or MariaDB's server_id.
	Server string `json:"server"`
	// Every transaction that ended before it has been read.
	binlogPos
	// The GTIDs of every transaction read, where the server keeps GTIDs; a
	// read of another server, after a failover, continues from them.
	GTID string `json:"gtid,omitempty"`
	// Grows with each server the stream is read from, so its seqs do.
	Epoch uint32 `json:"epoch,omitempty"`
	// When the last transaction read committed.
	At *time.Time `json:"at,omitempty"`
	// Until the table's snapshot has been read: where it continues.
	Snapshot *snapshotCursor `json:"snapshot,omitempty"`
	// Where the stream's first read started the binlog, and the GTIDs before
	// it: a read again from no state starts there.
	Start     *binlogPos `json:"start,omitempty"`
	StartGTID string     `json:"startGtid,omitempty"`
	// The XA transactions prepared whose rows were read, by their xids as
	// the binlog writes them, until their XA COMMIT.
	XA []string `json:"xa,omitempty"`
}

type snapshotCursor struct {
	// The primary key of the last row read, spelled as keyText spells it;
	// none before the first chunk.
	After []string `json:"after,omitempty"`
	// Where the table is read in key ranges: those not read yet, each with
	// its own position, in place of After.
	Ranges []*snapshotRange `json:"ranges,omitempty"`
}

// snapshotRange is a key range of a table whose snapshot isn't read yet.
type snapshotRange struct {
	keyRange
	After []string `json:"after,omitempty"`
}

// cdcStream is one table read from the binlog.
type cdcStream struct {
	t table
	b *batcher
	// Its state's position; nil for none. Transactions before it were read
	// before.
	from *cdcState
	// Its state's GTIDs, where the read skips by them.
	fromGTID gomysql.GTIDSet
	// What's left of the table's snapshot; empty once it has been read.
	ranges []*snapshotRange
	// Emitted since its last resume point.
	dirty bool
	// Its last resume point's position, and when.
	resumed   binlogPos
	resumedAt time.Time
	// Where its first read started the binlog, and the GTIDs before it.
	start     *binlogPos
	startGTID string
	// A read that stops at a resume point or completion it sent before:
	// that state, as it came and parsed, with its GTIDs; nil for a read to
	// the binlog's end. Such a read sends its snapshot's rows once it gets there:
	// those its state, fromState, hadn't read and until's had.
	until     *cdcState
	untilRaw  []byte
	untilGTID gomysql.GTIDSet
	fromState cdcState
	// It got to until, and completed.
	finished bool
	// As its state's XA.
	xa []string
}

// relation is a table as a binlog table map describes it.
type relation struct {
	stream *cdcStream // nil if it isn't read
	// The stream's column of each of the binlog's, or -1.
	at   []int
	cols []binlogColumn
	// The stream's columns the binlog's lack, as one added or renamed after
	// it: a change sends them as unchanged.
	absent []string
}

type txn struct {
	start binlogPos
	gtid  gomysql.GTIDSet
	at    time.Time
	// A BEGIN was read: statements before the commit are its.
	begun bool
	skip  map[*cdcStream]bool
	// Rows emitted so far, and the streams they're of, each with the number
	// of its last.
	n    uint64
	sent map[*cdcStream]uint64
	// How many rows were emitted before each of its savepoints, by name.
	savepoints map[string]uint64
	// Its xid, from its XA END, if it's an XA transaction.
	xa string
}

// chunk is a snapshot's chunk, held until the binlog passes its position.
type chunk struct {
	stream *cdcStream
	rng    *snapshotRange
	// Transactions that changed its rows ended before it; none after.
	at   binlogPos
	rows [][]any
	last []string
	// It had snapshotChunkRows rows, so more may follow.
	full bool
}

type cdcReader struct {
	ctx        context.Context
	s          *Source
	out        source.Emitter
	conns      *connPool
	conn       *sql.Conn
	srv        server
	continuous bool
	streams    []*cdcStream
	rels       map[uint64]*relation
	server     string
	epoch      uint32
	// The GTIDs of the transactions read; nil where the server keeps none.
	gset gomysql.GTIDSet
	// Transactions that ended before it have been read.
	pos binlogPos
	// A bounded read ends once pos reaches it.
	target binlogPos
	// Where the server last said its binlog ends, and when the transaction
	// read last committed.
	serverEnd  binlogPos
	lastCommit time.Time
	txn        *txn
	chunks     []*chunk
}

// readCDC reads the streams from the binlog: each table's changes, and
// until it has been read, its snapshot, in chunks between transactions, a
// chunk of a key range per connection the read can hold. A chunk is read
// with a locking read, which waits for the transactions writing its rows and
// keeps others from writing them until it has its binlog position: the
// chunk holds exactly the changes before that position. It's held until the
// binlog reaches it, and emitted before the changes after. Nothing is written
// to the source.
func (s *Source) readCDC(ctx context.Context, conns *connPool, srv server, req source.ReadRequest, cfgs []source.ConfiguredStream, tables []table, out source.Emitter) error {
	conn, err := conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conns.Put(conn)
	r := &cdcReader{ctx: ctx, s: s, out: out, conns: conns, conn: conn, srv: srv, continuous: req.Continuous, rels: map[uint64]*relation{}}
	if err := r.checkServer(ctx); err != nil {
		return err
	}
	for _, cs := range cfgs {
		t, _ := findTable(tables, cs.Name)
		if !chunked(t) {
			return configInvalid("mysql: %s has no primary key whose columns order as they compare, so it can't be read with cdc", t.name)
		}
		if t, err = declaredOnly(t, cs.Columns); err != nil {
			return err
		}
		st := &cdcStream{t: t}
		var state cdcState
		if cs.State != nil {
			if err := json.Unmarshal(cs.State, &state); err != nil {
				return fmt.Errorf("mysql: %s state: %w", cs.Name, err)
			}
		}
		// A state from a full refresh has no position, and starts the table
		// again.
		if state.File != "" {
			st.from = &state
			st.start, st.startGTID, st.xa = state.Start, state.StartGTID, state.XA
		}
		stop := cs.Until
		if stop == nil {
			stop = cs.CompleteAt
		}
		if stop != nil && !req.Continuous {
			if err := r.stopAt(st, state, stop); err != nil {
				return err
			}
		} else if st.ranges, err = r.snapshotRanges(ctx, t, state); err != nil {
			return err
		}
		for _, rng := range st.ranges {
			if rng.After != nil && len(rng.After) != len(t.key) {
				return fmt.Errorf("mysql: %s state has %d key columns, the table has %d", t.name, len(rng.After), len(t.key))
			}
		}
		if st.b, err = newBatcher(memory.DefaultAllocator, t.cols, true); err != nil {
			return fmt.Errorf("mysql: %s: %w", t.name, err)
		}
		r.streams = append(r.streams, st)
	}
	defer func() {
		for _, st := range r.streams {
			st.b.release()
		}
	}()
	syncer := replication.NewBinlogSyncer(r.syncerConfig(req.Pipeline))
	defer syncer.Close()
	streamer, err := r.start(ctx, syncer)
	if err != nil {
		return err
	}
	// A new stream's first resume point comes before its records, so a read
	// again from no state up to it has none to send.
	for _, st := range r.streams {
		if st.from == nil && st.until == nil {
			st.start = &binlogPos{File: r.pos.File, Pos: r.pos.Pos}
			if r.gset != nil {
				st.startGTID = r.gset.String()
			}
			if err := r.resumeStream(st); err != nil {
				return err
			}
		}
	}
	if err := r.reachUntil(); err != nil {
		return err
	}
	return r.run(ctx, streamer)
}

// stopAt sets a stream up to stop at the resume point, or the completion,
// whose state is until: either is the read's position then. From no state,
// its read starts the binlog where the first read did.
func (r *cdcReader) stopAt(st *cdcStream, from cdcState, until []byte) error {
	var u cdcState
	if err := json.Unmarshal(until, &u); err != nil {
		return fmt.Errorf("mysql: %s until: %w", st.t.name, err)
	}
	if u.File == "" {
		return fmt.Errorf("mysql: %s until %s has no binlog position", st.t.name, until)
	}
	g, err := r.parseGTID(u.GTID)
	if err != nil {
		return err
	}
	st.until, st.untilRaw, st.untilGTID, st.fromState = &u, until, g, from
	if st.from == nil && u.Start != nil {
		st.from = &cdcState{Server: u.Server, binlogPos: *u.Start, GTID: u.StartGTID, Epoch: u.Epoch}
	}
	return nil
}

// reachUntil completes each stream whose read stops at a position on this
// server that the read has reached.
func (r *cdcReader) reachUntil() error {
	for _, st := range r.streams {
		if st.until != nil && !st.finished && st.until.Server == r.server && !r.pos.less(st.until.binlogPos) {
			if err := r.finish(st); err != nil {
				return err
			}
		}
	}
	return nil
}

// finish completes a stream at its until: it sends the snapshot's rows its
// first read sent between its state and until, as the table has them now,
// with the seq of until's position, or on another server of where the read
// stopped, so that they come after the changes before it and before those
// after; then a resume point with until, and completes with it. On another
// server, the read has to have every transaction until's GTIDs have.
func (r *cdcReader) finish(st *cdcStream) error {
	if st.until.Server != r.server && (r.gset == nil || st.untilGTID == nil || !r.gset.Contain(st.untilGTID)) {
		return positionLost("mysql: %s is read again up to where it got to on server %s, and server %s, which the read is on, hasn't every transaction before there; read it again", st.t.name, st.until.Server, r.server)
	}
	from, fromArgs, err := remaining(st.t, st.fromState)
	if err != nil {
		return err
	}
	to, toArgs, err := remaining(st.t, *st.until)
	if err != nil {
		return err
	}
	if from != "FALSE" && to != "TRUE" {
		tr := &tableReader{s: r.s, t: st.t}
		q := "SELECT " + tr.list() + " FROM " + tr.ident() + " WHERE " + from + " AND NOT " + to + " ORDER BY " + tr.names(st.t.key)
		seq := seqOf(st.until.Epoch, st.until.binlogPos, 0)
		if st.until.Server != r.server {
			seq = seqOf(r.epoch, r.pos, 0)
		}
		err := scan(r.ctx, r.conn, q, append(fromArgs, toArgs...), len(st.t.cols), func(vals []any) error {
			return r.add(st, source.OpInsert, seq, vals, nil)
		})
		if err != nil {
			return fmt.Errorf("mysql: snapshot of %s: %w", st.t.name, err)
		}
	}
	if err := st.flush(r.out); err != nil {
		return err
	}
	if err := r.out.ResumePoint(st.t.name, st.untilRaw); err != nil {
		return err
	}
	st.finished = true
	return r.out.Complete(st.t.name, st.untilRaw)
}

// remaining is a condition on a table's rows: those its snapshot hadn't
// read at state s.
func remaining(t table, s cdcState) (string, []any, error) {
	c := s.Snapshot
	switch {
	case s.File == "" || c != nil && c.After == nil && len(c.Ranges) == 0:
		return "TRUE", nil, nil
	case c == nil:
		return "FALSE", nil, nil
	}
	ranges := c.Ranges
	if len(ranges) == 0 {
		ranges = []*snapshotRange{{After: c.After}}
	}
	keyCols := (&tableReader{t: t}).cols(t.key)
	var ors []string
	var args []any
	for _, rng := range ranges {
		var w where
		if err := rng.where(keyCols[0], &w); err != nil {
			return "", nil, err
		}
		if rng.After != nil {
			if err := w.after(keyCols, rng.After); err != nil {
				return "", nil, err
			}
		}
		if len(w.conds) == 0 {
			return "TRUE", nil, nil
		}
		ors = append(ors, "("+strings.Join(w.conds, " AND ")+")")
		args = append(args, w.args...)
	}
	return "(" + strings.Join(ors, " OR ") + ")", args, nil
}

func (r *cdcReader) finished() bool {
	return !slices.ContainsFunc(r.streams, func(st *cdcStream) bool { return !st.finished })
}

// checkServer refuses a server whose binlog doesn't log whole rows with
// their columns' names, and learns which server it is.
func (r *cdcReader) checkServer(ctx context.Context) error {
	problems, err := binlogProblems(ctx, r.conn)
	if err != nil {
		return err
	}
	if len(problems) > 0 {
		return configInvalid("mysql: cdc can't read the binlog: %s", strings.Join(problems, "; "))
	}
	if r.srv.mariadb {
		var id string
		err = r.conn.QueryRowContext(ctx, "SELECT @@server_id").Scan(&id)
		r.server = "server_id:" + id
		return classify(err)
	}
	return classify(r.conn.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&r.server))
}

// binlogSetting is a server setting a cdc read needs, to read whole rows
// with their columns' names.
type binlogSetting struct {
	// The check's name after "cdc.".
	name, variable, title, required, fix string
	ok                                   func(value string) bool
	// Only MySQL has it: a server without it passes.
	optional bool
}

var binlogSettings = []binlogSetting{
	{name: "log-bin", variable: "log_bin", title: "Log changes in the binlog", required: "ON",
		ok:  func(v string) bool { return strings.EqualFold(v, "ON") || v == "1" },
		fix: "Start the server with the binlog on: MySQL has it unless started with --skip-log-bin or --disable-log-bin, and MariaDB needs --log-bin."},
	{name: "binlog-format", variable: "binlog_format", title: "Log changes as rows", required: "ROW",
		ok:  func(v string) bool { return strings.EqualFold(v, "ROW") },
		fix: "Set binlog_format = ROW in the server's option file, and run SET GLOBAL binlog_format = 'ROW';"},
	{name: "binlog-row-image", variable: "binlog_row_image", title: "Log whole rows", required: "FULL",
		ok:  func(v string) bool { return strings.EqualFold(v, "FULL") },
		fix: "Set binlog_row_image = FULL in the server's option file, and run SET GLOBAL binlog_row_image = 'FULL';"},
	{name: "binlog-row-metadata", variable: "binlog_row_metadata", title: "Log the columns' names", required: "FULL",
		ok:  func(v string) bool { return strings.EqualFold(v, "FULL") },
		fix: "Set binlog_row_metadata = FULL in the server's option file, and run SET GLOBAL binlog_row_metadata = 'FULL';"},
	{name: "binlog-row-value-options", variable: "binlog_row_value_options", title: "Log whole JSON values", required: "empty, not PARTIAL_JSON",
		ok:       func(v string) bool { return !strings.Contains(strings.ToUpper(v), "PARTIAL_JSON") },
		fix:      "Set binlog_row_value_options to the empty string in the server's option file, and run SET GLOBAL binlog_row_value_options = '';",
		optional: true},
}

// globalVariable is a global setting's value; false for one the server
// doesn't have.
func globalVariable(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, name string) (string, bool, error) {
	rows, err := q.QueryContext(ctx, "SHOW GLOBAL VARIABLES WHERE Variable_name = ?", name)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return "", false, rows.Err()
	}
	var n, v sql.NullString
	if err := rows.Scan(&n, &v); err != nil {
		return "", false, err
	}
	return v.String, true, rows.Close()
}

// binlogProblems are the server's settings that keep cdc from reading its
// binlog.
func binlogProblems(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]string, error) {
	var out []string
	for _, b := range binlogSettings {
		v, found, err := globalVariable(ctx, q, b.variable)
		if err != nil {
			return nil, classify(fmt.Errorf("mysql: binlog settings: %w", err))
		}
		if found && !b.ok(v) || !found && !b.optional {
			out = append(out, fmt.Sprintf("%s is %s, and cdc needs %s", b.variable, cmp.Or(v, "not set"), b.required))
		}
	}
	return out, nil
}

// snapshotRanges is what's left of a table's snapshot, from its state. One
// not started is read in key ranges if several connections can read it.
func (r *cdcReader) snapshotRanges(ctx context.Context, t table, state cdcState) ([]*snapshotRange, error) {
	cursor := state.Snapshot
	switch {
	case state.File == "" || cursor != nil && cursor.After == nil && len(cursor.Ranges) == 0:
		if cap(r.conns.Held) == 1 {
			return []*snapshotRange{{}}, nil
		}
		ranges, err := r.s.keyRanges(ctx, r.conn, t, r.s.cfg.SnapshotChunkRows)
		if err != nil {
			return nil, err
		}
		if len(ranges) == 0 {
			return []*snapshotRange{{}}, nil
		}
		out := make([]*snapshotRange, len(ranges))
		for i, k := range ranges {
			out[i] = &snapshotRange{keyRange: k}
		}
		return out, nil
	case cursor == nil:
		return nil, nil
	case len(cursor.Ranges) > 0:
		return cursor.Ranges, nil
	}
	return []*snapshotRange{{After: cursor.After}}, nil
}

func (r *cdcReader) syncerConfig(pipeline string) replication.BinlogSyncerConfig {
	c := r.s.conn
	// A socket's path, with no port, which the syncer dials as a socket.
	host, port := c.Addr, ""
	if c.Net != "unix" {
		host, port, _ = net.SplitHostPort(c.Addr)
	}
	p, _ := strconv.ParseUint(port, 10, 16)
	id := r.s.cfg.ServerID
	if id == 0 {
		// One the server's own replicas, numbered from 1, are unlikely to
		// have; a pipeline's next read takes over its last one's.
		h := fnv.New32a()
		_, _ = h.Write([]byte(pipeline))
		id = h.Sum32() | 1<<31
	}
	flavor := gomysql.MySQLFlavor
	if r.srv.mariadb {
		flavor = gomysql.MariaDBFlavor
	}
	return replication.BinlogSyncerConfig{
		ServerID: id, Flavor: flavor, Host: host, Port: uint16(p), User: c.User, Password: c.Passwd, TLSConfig: c.TLS,
		TimestampStringLocation: time.UTC, RenderJSONAsMySQLText: true, HeartbeatPeriod: heartbeatEvery,
		ReadTimeout: 30 * time.Second, DisableRetrySync: true, FillZeroLogPos: r.srv.mariadb,
		Logger: slog.New(slog.DiscardHandler),
	}
}

// start starts the binlog where the streams' earliest position is: by file
// and position on the server they were read from, or by GTIDs on another; at
// the binlog's end if no stream has one.
func (r *cdcReader) start(ctx context.Context, syncer *replication.BinlogSyncer) (*replication.BinlogStreamer, error) {
	end, endGTID, err := r.binlogEnd(ctx)
	if err != nil {
		return nil, err
	}
	if !r.continuous {
		r.target = end
	}
	var from []*cdcStream
	for _, st := range r.streams {
		if st.from != nil {
			from = append(from, st)
			r.epoch = max(r.epoch, st.from.Epoch)
		}
	}
	if len(from) == 0 {
		r.pos = end
		if r.gset, err = r.parseGTID(endGTID); err != nil {
			return nil, err
		}
		streamer, err := syncer.StartSync(gomysql.Position{Name: end.File, Pos: end.Pos})
		return streamer, classify(err)
	}
	if slices.ContainsFunc(from, func(st *cdcStream) bool { return st.from.Server == r.server }) {
		for _, st := range from {
			if st.from.Server != r.server {
				return nil, positionLost("mysql: %s was read from server %s, and the others from %s; read it again", st.t.name, st.from.Server, r.server)
			}
		}
		first := slices.MinFunc(from, func(a, b *cdcStream) int {
			switch {
			case a.from.binlogPos.less(b.from.binlogPos):
				return -1
			case b.from.binlogPos.less(a.from.binlogPos):
				return 1
			}
			return 0
		})
		r.pos, r.lastCommit = first.from.binlogPos, deref(first.from.At)
		if r.gset, err = r.parseGTID(first.from.GTID); err != nil {
			return nil, err
		}
		streamer, err := syncer.StartSync(gomysql.Position{Name: r.pos.File, Pos: r.pos.Pos})
		return streamer, classify(err)
	}
	// Another server, as after a failover: the binlog is another's, so the
	// read goes by GTIDs, from the set every stream's holds, and its seqs
	// start above the last server's.
	r.epoch++
	for _, st := range from {
		if st.fromGTID, err = r.parseGTID(st.from.GTID); err != nil {
			return nil, err
		}
		if st.fromGTID == nil {
			return nil, positionLost("mysql: %s was read from server %s without GTIDs, and the source is now server %s; read it again", st.t.name, st.from.Server, r.server)
		}
	}
	i := slices.IndexFunc(from, func(st *cdcStream) bool {
		return !slices.ContainsFunc(from, func(o *cdcStream) bool { return !o.fromGTID.Contain(st.fromGTID) })
	})
	if i < 0 {
		return nil, positionLost("mysql: the streams' GTIDs don't share a start on server %s; read them again", r.server)
	}
	r.gset = from[i].fromGTID.Clone()
	slog.Info("reading the binlog of another server by GTIDs", "server", r.server, "gtid", r.gset.String())
	streamer, err := syncer.StartSyncGTID(r.gset.Clone())
	return streamer, classify(err)
}

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// parseGTID reads a GTID set, nil if the server keeps no GTIDs.
func (r *cdcReader) parseGTID(s string) (gomysql.GTIDSet, error) {
	if s == "" {
		return nil, nil
	}
	flavor := gomysql.MySQLFlavor
	if r.srv.mariadb {
		flavor = gomysql.MariaDBFlavor
	}
	g, err := gomysql.ParseGTIDSet(flavor, s)
	if err != nil {
		return nil, fmt.Errorf("mysql: GTIDs %q: %w", s, err)
	}
	return g, nil
}

// binlogEnd is where the binlog ends, and the GTIDs of the transactions
// before it, empty where the server keeps none.
func (r *cdcReader) binlogEnd(ctx context.Context) (binlogPos, string, error) {
	p, gtid, err := binlogStatus(ctx, r.conn, r.srv)
	if err == nil && r.srv.mariadb {
		err = queryRowText(ctx, r.conn, "SELECT coalesce(BINLOG_GTID_POS(?, ?), '')", &gtid, p.File, p.Pos)
	}
	return p, gtid, err
}

// binlogStatus is where the binlog ends, and on MySQL the GTIDs before it.
func binlogStatus(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, srv server) (binlogPos, string, error) {
	stmt := "SHOW BINARY LOG STATUS"
	if srv.mariadb {
		stmt = "SHOW MASTER STATUS"
	}
	rows, err := q.QueryContext(ctx, stmt)
	if n, _ := serverError(err); n == 1064 {
		// MySQL before 8.2.
		rows, err = q.QueryContext(ctx, "SHOW MASTER STATUS")
	}
	if err != nil {
		return binlogPos{}, "", classify(fmt.Errorf("mysql: %s: %w", stmt, err))
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return binlogPos{}, "", classify(err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return binlogPos{}, "", classify(err)
		}
		return binlogPos{}, "", sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, errNoBinlog)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return binlogPos{}, "", classify(err)
	}
	pos, err := strconv.ParseUint(vals[1].String, 10, 32)
	if err != nil {
		return binlogPos{}, "", fmt.Errorf("mysql: binlog position %q: %w", vals[1].String, err)
	}
	p := binlogPos{File: vals[0].String, Pos: uint32(pos)}
	if len(vals) > 4 {
		return p, strings.ReplaceAll(vals[4].String, "\n", ""), nil
	}
	return p, "", nil
}

func queryRowText(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, stmt string, dest *string, args ...any) error {
	rows, err := q.QueryContext(ctx, stmt, args...)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(dest); err != nil {
			return classify(err)
		}
	}
	return classify(rows.Err())
}

func (r *cdcReader) run(ctx context.Context, streamer *replication.BinlogStreamer) error {
	nextProgress, nextLog := time.Now(), time.Now().Add(logLagEvery)
	for {
		if r.finished() {
			return nil
		}
		if r.txn == nil {
			if len(r.chunks) == 0 {
				if err := r.readChunks(ctx); err != nil {
					return err
				}
			}
			if err := r.resume(); err != nil {
				return err
			}
			if !r.continuous && len(r.chunks) == 0 && !r.pos.less(r.target) && r.snapshotsRead() {
				return r.complete()
			}
		}
		now := time.Now()
		if !nextProgress.After(now) {
			logLag := !nextLog.After(now)
			if err := r.progress(ctx, logLag); err != nil {
				return err
			}
			nextProgress = now.Add(progressEvery)
			if logLag {
				nextLog = now.Add(logLagEvery)
			}
		}
		// Chunks the binlog had passed leave it nothing to wait for.
		if r.txn == nil && len(r.chunks) == 0 && !r.snapshotsRead() {
			continue
		}
		ectx, cancel := context.WithTimeout(ctx, heartbeatEvery)
		ev, err := streamer.GetEvent(ectx)
		cancel()
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, context.DeadlineExceeded):
			continue
		case err != nil:
			return classify(fmt.Errorf("mysql: binlog: %w", err))
		}
		if err := r.event(ctx, ev); err != nil {
			return err
		}
	}
}

func (r *cdcReader) event(ctx context.Context, ev *replication.BinlogEvent) error {
	h := ev.Header
	event := ev.Event
	if tagged, ok := event.(*replication.GtidTaggedLogEvent); ok {
		event = &tagged.GTIDEvent
	}
	switch e := event.(type) {
	case *replication.FormatDescriptionEvent:
		// Sent again where a read starts in a file, with MariaDB's position
		// after it filled in, which isn't one.
		return nil
	case *replication.RotateEvent:
		// The one a binlog starts with names the file it starts in.
		fake := h.LogPos == 0 || h.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0
		if r.txn == nil && !(fake && r.pos.File == string(e.NextLogName)) {
			r.pos = binlogPos{File: string(e.NextLogName), Pos: uint32(e.Position)}
			r.serverEnd = maxPos(r.serverEnd, r.pos)
			return r.passed()
		}
		return nil
	case *replication.GTIDEvent:
		var g gomysql.GTIDSet
		if h.EventType != replication.ANONYMOUS_GTID_EVENT {
			var err error
			if g, err = e.GTIDNext(); err != nil {
				return err
			}
		}
		at := e.ImmediateCommitTime()
		if at.IsZero() {
			at = time.Unix(int64(h.Timestamp), 0)
		}
		return r.begin(g, at)
	case *replication.MariadbGTIDEvent:
		g, err := e.GTIDNext()
		if err != nil {
			return err
		}
		if err := r.begin(g, time.Unix(int64(h.Timestamp), 0)); err != nil {
			return err
		}
		// MariaDB writes no BEGIN: a group that isn't a statement on its own
		// ends at its COMMIT or XID, whatever statements, such as SAVEPOINT,
		// come before.
		r.txn.begun = !e.IsStandalone()
		return nil
	case *replication.QueryEvent:
		q := strings.ToUpper(strings.TrimSpace(string(e.Query)))
		switch {
		case q == "BEGIN" || strings.HasPrefix(q, "XA START"):
			if r.txn == nil {
				if err := r.begin(nil, time.Unix(int64(h.Timestamp), 0)); err != nil {
					return err
				}
			}
			r.txn.begun = true
			return nil
		case q == "COMMIT":
			return r.commit(h)
		case q == "ROLLBACK":
			return r.rollback(h)
		}
		if err := r.savepoint(q); err != nil {
			return err
		}
		if x, ok := strings.CutPrefix(q, "XA END "); ok && r.txn != nil {
			r.txn.xa = x
		}
		// An XA transaction prepared before: its rows were read then.
		xaCommit, committed := strings.CutPrefix(q, "XA COMMIT ")
		xaRollback, rolledBack := strings.CutPrefix(q, "XA ROLLBACK ")
		for _, st := range r.streams {
			switch {
			case st.finished || r.txn != nil && r.txn.skip[st]:
			case rolledBack && slices.Contains(st.xa, xaRollback):
				return positionLost("mysql: binlog: XA transaction %s changed %s and was prepared, when its changes were read, and then rolled back: "+
					"the destination has changes that never committed; read %[2]s again", xaRollback, st.t.name)
			case committed && slices.Contains(st.xa, xaCommit):
				st.xa = slices.DeleteFunc(st.xa, func(x string) bool { return x == xaCommit })
				st.dirty = true
			}
		}
		if written, cols, refs, ok := statementWrites(q); ok {
			is := func(st *cdcStream) func(tableName) bool {
				return func(t tableName) bool {
					return cmp.Or(t.schema, strings.ToUpper(string(e.Schema))) == strings.ToUpper(r.s.cfg.Database) && t.name == strings.ToUpper(st.t.name)
				}
			}
			for _, st := range r.streams {
				hasCol := slices.ContainsFunc(st.t.cols, func(c sourceColumn) bool { return slices.Contains(cols, strings.ToUpper(c.name)) })
				if !st.finished && (r.txn == nil || !r.txn.skip[st]) && (slices.ContainsFunc(written, is(st)) || hasCol && slices.ContainsFunc(refs, is(st))) {
					return configInvalid("mysql: binlog: a change to %s logged as a statement, which has no rows to read: a session set binlog_format to STATEMENT or MIXED; "+
						"log changes with binlog_format = ROW, and read %[1]s again", st.t.name)
				}
			}
		}
		if strings.HasPrefix(q, "TRUNCATE") && string(e.Schema) == r.s.cfg.Database {
			for _, st := range r.streams {
				if strings.Contains(q, strings.ToUpper(st.t.name)) {
					slog.Warn("a table read with cdc may have been truncated; its rows stay in the destination, so a new snapshot of it brings the destination back in step", "table", st.t.name, "statement", string(e.Query))
				}
			}
		}
		if r.txn != nil && !r.txn.begun {
			// A statement on its own, such as DDL, is its own transaction.
			return r.commit(h)
		}
		return nil
	case *replication.XIDEvent:
		return r.commit(h)
	case *replication.GenericEvent:
		// An XA transaction's changes end at its XA PREPARE; its XA COMMIT
		// or XA ROLLBACK comes later, as a statement on its own, unless its
		// body's first byte says it's an XA COMMIT ONE PHASE.
		if h.EventType == replication.XA_PREPARE_LOG_EVENT {
			if r.txn != nil && r.txn.xa != "" && len(e.Data) > 0 && e.Data[0] == 0 {
				for st := range r.txn.sent {
					if !slices.Contains(st.xa, r.txn.xa) {
						st.xa = append(st.xa, r.txn.xa)
					}
				}
			}
			return r.commit(h)
		}
	case *replication.TableMapEvent:
		return r.relation(ctx, e)
	case *replication.RowsEvent:
		return r.rows(h.EventType, e)
	case *replication.TransactionPayloadEvent:
		for _, inner := range e.Events {
			if err := r.event(ctx, inner); err != nil {
				return err
			}
		}
		return nil
	case *replication.HeartbeatEvent:
		if r.txn == nil && e.Filename != "" {
			r.serverEnd = maxPos(r.serverEnd, binlogPos{File: e.Filename, Pos: h.LogPos})
		}
	}
	// Events between transactions: the binlog's own.
	if r.txn == nil && h.LogPos > 0 && h.Flags&replication.LOG_EVENT_ARTIFICIAL_F == 0 && r.pos.File != "" && h.LogPos > r.pos.Pos {
		r.pos.Pos = h.LogPos
		r.serverEnd = maxPos(r.serverEnd, r.pos)
		return r.passed()
	}
	return nil
}

// tableName is a table a statement names, with its schema if it names one.
type tableName struct{ schema, name string }

// statementWrites is what q, a statement in upper case that changes rows,
// writes, which in the binlog means a session logged it as a statement: the
// tables it writes, and the columns an UPDATE of more than one table sets
// without naming their table, which may be of any table in refs. ok is
// false for another statement.
func statementWrites(q string) (written []tableName, cols []string, refs []tableName, ok bool) {
	ts := sqlTokens(q)
	if len(ts) == 0 {
		return nil, nil, nil, false
	}
	if ts[0] == "WITH" {
		// The statement after the common table expressions, outside their
		// parentheses.
		depth := 0
	cte:
		for j, t := range ts {
			switch {
			case t == "(":
				depth++
			case t == ")":
				depth--
			case depth == 0 && (t == "UPDATE" || t == "DELETE"):
				ts = ts[j:]
				break cte
			}
		}
	}
	i := 1
	skip := func(words ...string) {
		for i < len(ts) && slices.Contains(words, ts[i]) {
			i++
		}
	}
	switch ts[0] {
	case "INSERT", "REPLACE":
		skip("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE", "INTO")
		list, _ := tableList(ts, i)
		return list[:min(len(list), 1)], nil, nil, len(list) > 0
	case "UPDATE":
		skip("LOW_PRIORITY", "IGNORE")
		var aliases map[string]tableName
		refs, aliases, i = references(ts, i, "SET")
		if len(refs) == 1 {
			return refs, nil, nil, true
		}
		// Each assignment's [[schema.]table.]column, to WHERE, ORDER or LIMIT.
		depth, start := 0, true
		for i++; i < len(ts); i++ {
			switch t := ts[i]; {
			case t == "(":
				depth++
			case t == ")":
				depth--
			case depth > 0:
			case t == ",":
				start = true
				continue
			case t == "WHERE" || t == "ORDER" || t == "LIMIT":
				return written, cols, refs, true
			case start:
				n, j := dotted(ts, i)
				switch i = j - 1; len(n) {
				case 1:
					cols = append(cols, n[0])
				case 2:
					written = append(written, cmp.Or(aliases[n[0]], tableName{name: n[0]}))
				case 3:
					written = append(written, tableName{n[0], n[1]})
				}
			}
			start = false
		}
		return written, cols, refs, true
	case "DELETE":
		skip("LOW_PRIORITY", "QUICK", "IGNORE")
		from := i < len(ts) && ts[i] == "FROM"
		if from {
			i++
		}
		var list []tableName
		var aliases map[string]tableName
		list, i = tableList(ts, i)
		switch {
		case from && i < len(ts) && ts[i] == "USING":
			_, aliases, _ = references(ts, i+1, "WHERE")
		case from:
			return list[:min(len(list), 1)], nil, nil, len(list) > 0
		case i < len(ts) && ts[i] == "FROM":
			_, aliases, _ = references(ts, i+1, "WHERE")
		}
		for j, t := range list {
			if a, ok := aliases[t.name]; ok && t.schema == "" {
				list[j] = a
			}
		}
		return list, nil, nil, true
	}
	return nil, nil, nil, false
}

// tableList reads a list of [schema.]table[.*] from ts[i:], and returns where
// it ends.
func tableList(ts []string, i int) ([]tableName, int) {
	var list []tableName
	for i < len(ts) && isName(ts[i]) {
		n, j := dotted(ts, i)
		list, i = append(list, tableOf(n)), j
		if i >= len(ts) || ts[i] != "," {
			break
		}
		i++
	}
	return list, i
}

// references reads table references from ts[i:] to the first of stop: each
// table, and by each one's alias and name, the table. It returns where they
// end.
func references(ts []string, i int, stop ...string) ([]tableName, map[string]tableName, int) {
	var refs []tableName
	aliases := map[string]tableName{}
	expect := true
	for ; i < len(ts) && !slices.Contains(stop, ts[i]); i++ {
		switch t := ts[i]; {
		case t == "," || t == "JOIN" || t == "STRAIGHT_JOIN":
			expect = true
		case expect && isName(t):
			n, j := dotted(ts, i)
			ref := tableOf(n)
			refs, aliases[ref.name], i, expect = append(refs, ref), ref, j, false
			if i < len(ts) && ts[i] == "AS" {
				i++
			}
			if i < len(ts) && isName(ts[i]) && !slices.Contains(afterTable, ts[i]) {
				aliases[strings.TrimPrefix(ts[i], "`")] = ref
			} else {
				i--
			}
		}
	}
	return refs, aliases, i
}

// afterTable are the words that may follow a table reference that aren't
// its alias.
var afterTable = []string{"ON", "USING", "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "NATURAL", "OUTER", "STRAIGHT_JOIN", "SET", "WHERE",
	"USE", "FORCE", "IGNORE", "PARTITION", "ORDER", "LIMIT"}

// dotted reads a name and the names it's qualified by, a.b.c, from ts[i:],
// with a trailing .* left out, and returns where it ends.
func dotted(ts []string, i int) ([]string, int) {
	n := []string{strings.TrimPrefix(ts[i], "`")}
	for i++; i+1 < len(ts) && ts[i] == "."; i += 2 {
		if ts[i+1] == "*" {
			return n, i + 2
		}
		n = append(n, strings.TrimPrefix(ts[i+1], "`"))
	}
	return n, i
}

// tableOf is the table that a dotted name names.
func tableOf(n []string) tableName {
	t := tableName{name: n[len(n)-1]}
	if len(n) > 1 {
		t.schema = n[len(n)-2]
	}
	return t
}

func isName(t string) bool { return t != "" && (t[0] == '`' || nameChar(t[0])) }

// sqlTokens splits q into words, names quoted with backticks, as a backtick
// and the name, and other characters one by one, leaving out strings,
// whose place a ' takes, and comments.
func sqlTokens(q string) []string {
	var ts []string
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == '`':
			n, rest := nextName(q[i:])
			ts, i = append(ts, "`"+n), len(q)-len(rest)
		case c == '\'' || c == '"':
			j := i + 1
			for ; j < len(q) && (q[j] != c || j+1 < len(q) && q[j+1] == c); j++ {
				if q[j] == '\\' || q[j] == c {
					j++
				}
			}
			ts, i = append(ts, "'"), j+1
		case strings.HasPrefix(q[i:], "/*"):
			_, rest, _ := strings.Cut(q[i+2:], "*/")
			i = len(q) - len(rest)
		case c == '#' || strings.HasPrefix(q[i:], "-- "):
			_, rest, _ := strings.Cut(q[i:], "\n")
			i = len(q) - len(rest)
		case nameChar(c):
			j := i
			for j < len(q) && nameChar(q[j]) {
				j++
			}
			ts, i = append(ts, q[i:j]), j
		case c > ' ':
			ts, i = append(ts, string(c)), i+1
		default:
			i++
		}
	}
	return ts
}

// nextName is the name s starts with, after spaces, bare or quoted with
// backticks, and what follows it.
func nextName(s string) (name, rest string) {
	s = strings.TrimLeft(s, " \t\r\n")
	if !strings.HasPrefix(s, "`") {
		i := 0
		for i < len(s) && nameChar(s[i]) {
			i++
		}
		return s[:i], s[i:]
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		switch {
		case s[i] != '`':
			b.WriteByte(s[i])
		case i+1 < len(s) && s[i+1] == '`':
			b.WriteByte('`')
			i++
		default:
			return b.String(), s[i+1:]
		}
	}
	return "", ""
}

func nameChar(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= 0x80
}

func maxPos(a, b binlogPos) binlogPos {
	if a.less(b) {
		return b
	}
	return a
}

func (r *cdcReader) begin(g gomysql.GTIDSet, at time.Time) error {
	if r.txn != nil {
		return errors.New("mysql: binlog: a transaction began inside another")
	}
	r.txn = &txn{start: r.pos, gtid: g, at: at, skip: map[*cdcStream]bool{}, sent: map[*cdcStream]uint64{}, savepoints: map[string]uint64{}}
	for _, st := range r.streams {
		switch {
		case st.from == nil:
		case st.fromGTID != nil:
			r.txn.skip[st] = g != nil && st.fromGTID.Contain(g)
		default:
			r.txn.skip[st] = r.pos.less(st.from.binlogPos)
		}
		// On another server, a read that stops at until stops at the first
		// transaction it hadn't read.
		if st.until != nil && !st.finished && st.until.Server != r.server && g != nil && st.untilGTID != nil && !st.untilGTID.Contain(g) {
			if err := r.finish(st); err != nil {
				return err
			}
		}
	}
	// Chunks the binlog has reached come before the transaction's changes.
	return r.passed()
}

// commit ends the transaction at the event whose header h is.
func (r *cdcReader) commit(h *replication.EventHeader) error {
	if r.txn == nil {
		return nil
	}
	if r.gset != nil && r.txn.gtid != nil {
		if err := r.gset.Update(r.txn.gtid.String()); err != nil {
			return err
		}
	}
	r.lastCommit = r.txn.at
	r.txn = nil
	if h.LogPos > 0 {
		r.pos.Pos = h.LogPos
	}
	r.serverEnd = maxPos(r.serverEnd, r.pos)
	return r.passed()
}

// rollback ends a transaction that rolled back, which the binlog has where
// it changed a table that can't roll back, such as a MyISAM one, whose
// changes stay.
func (r *cdcReader) rollback(h *replication.EventHeader) error {
	if r.txn != nil {
		if err := r.rolledBack(0); err != nil {
			return err
		}
	}
	return r.commit(h)
}

// savepoint takes a transaction's SAVEPOINT or ROLLBACK TO, q. The binlog
// has the changes after a savepoint that a ROLLBACK TO it undid where the
// transaction changed a table that can't roll back.
func (r *cdcReader) savepoint(q string) error {
	if r.txn == nil {
		return nil
	}
	if name, ok := strings.CutPrefix(q, "SAVEPOINT "); ok {
		r.txn.savepoints[name] = r.txn.n
	} else if name, ok := strings.CutPrefix(q, "ROLLBACK TO "); ok {
		return r.rolledBack(r.txn.savepoints[name])
	}
	return nil
}

// rolledBack fails the read if the transaction emitted rows after its
// first n, which it rolled back: they never committed.
func (r *cdcReader) rolledBack(n uint64) error {
	var names []string
	for _, st := range r.streams {
		if r.txn.sent[st] > n {
			names = append(names, st.t.name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return positionLost("mysql: binlog: the transaction at %s:%d changed %s and rolled the changes back, which the binlog has as it changed a table that can't roll back, such as a MyISAM one; "+
		"they never committed, and were read as if they had: read %[3]s again", r.txn.start.File, r.txn.start.Pos, strings.Join(names, ", "))
}

// relation maps a table the binlog describes to its stream. A column the
// stream doesn't have fails the read if the table still has it, so the next
// attempt declares it; one since dropped is left out.
func (r *cdcReader) relation(ctx context.Context, m *replication.TableMapEvent) error {
	rel := &relation{}
	r.rels[m.TableID] = rel
	if string(m.Schema) != r.s.cfg.Database {
		return nil
	}
	st := r.stream(string(m.Table))
	if st == nil {
		return nil
	}
	rel.stream = st
	names := m.ColumnNameString()
	if len(names) != int(m.ColumnCount) {
		return configInvalid("mysql: the binlog doesn't name %s's columns; cdc needs binlog_row_metadata = FULL", st.t.name)
	}
	enums, sets := m.EnumStrValueMap(), m.SetStrValueMap()
	seen := 0
	for i, name := range names {
		j := slices.IndexFunc(st.t.cols, func(c sourceColumn) bool { return c.name == name })
		if j < 0 {
			var n int
			err := r.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
				r.s.cfg.Database, st.t.name, name).Scan(&n)
			if err != nil {
				return classify(err)
			}
			if n > 0 {
				return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("mysql: %s has a new column %q; the read starts again to take it", st.t.name, name))
			}
		} else {
			seen++
		}
		rel.at = append(rel.at, j)
		rel.cols = append(rel.cols, binlogColumn{enum: enums[i], set: sets[i]})
	}
	for _, k := range st.t.key {
		if !slices.Contains(rel.at, k) {
			return fmt.Errorf("mysql: %s's binlog lacks its primary key column %q", st.t.name, st.t.cols[k].name)
		}
	}
	for j, c := range st.t.cols {
		if !slices.Contains(rel.at, j) {
			rel.absent = append(rel.absent, c.name)
		}
	}
	return nil
}

func (r *cdcReader) stream(name string) *cdcStream {
	for _, st := range r.streams {
		if st.t.name == name {
			return st
		}
	}
	return nil
}

// rows emits a rows event's changes: an update of a key as a delete of the
// old key and the new row.
func (r *cdcReader) rows(t replication.EventType, e *replication.RowsEvent) error {
	rel := r.rels[e.TableID]
	if rel == nil {
		return fmt.Errorf("mysql: binlog: rows of table %d, which the binlog hasn't described", e.TableID)
	}
	st := rel.stream
	switch {
	case st == nil || st.finished:
		return nil
	case r.txn == nil:
		return fmt.Errorf("mysql: binlog: changes to %s outside a transaction, at %s:%d", st.t.name, r.pos.File, r.pos.Pos)
	case r.txn.skip[st]:
		return nil
	}
	var op string
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		op = source.OpInsert
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		op = source.OpDelete
	default:
		op = source.OpUpdate
	}
	skipped := func(i int) []int {
		if i < len(e.SkippedColumns) {
			return e.SkippedColumns[i]
		}
		return nil
	}
	for i := 0; i < len(e.Rows); i++ {
		vals, err := rel.values(e.Rows[i], skipped(i), op != source.OpInsert)
		if err != nil {
			return err
		}
		if op == source.OpUpdate {
			i++
			if i == len(e.Rows) {
				return fmt.Errorf("mysql: binlog: an update of %s without its new row", st.t.name)
			}
			after, err := rel.values(e.Rows[i], skipped(i), false)
			if err != nil {
				return err
			}
			if keyOf(st.t, vals) != keyOf(st.t, after) {
				if err := r.emit(st, source.OpDelete, vals, rel.absent); err != nil {
					return err
				}
			}
			vals = after
		}
		if err := r.emit(st, op, vals, rel.absent); err != nil {
			return err
		}
	}
	return nil
}

func keyOf(t table, vals []any) string {
	var b strings.Builder
	for _, k := range t.key {
		b.WriteString(keyText(t.cols[k], vals[k]))
		b.WriteByte(0)
	}
	return b.String()
}

// values are a binlog row's values by stream column, as the snapshot reads
// them. skipped are the binlog's columns the row doesn't have, which only
// the columns not in the key may be if keyOnly.
func (rel *relation) values(row []any, skipped []int, keyOnly bool) ([]any, error) {
	for _, i := range skipped {
		if j := rel.at[i]; j >= 0 && (!keyOnly || slices.Contains(rel.stream.t.key, j)) {
			return nil, configInvalid("mysql: binlog: a change to %s without %s's value, which a session logged with binlog_row_image MINIMAL or NOBLOB; "+
				"log changes with binlog_row_image = FULL, and read %[1]s again", rel.stream.t.name, rel.stream.t.cols[j].name)
		}
	}
	vals := make([]any, len(rel.stream.t.cols))
	for i, v := range row {
		j := rel.at[i]
		if j < 0 {
			continue
		}
		c := rel.stream.t.cols[j]
		if _, ok := v.(*replication.JsonDiff); ok {
			return nil, configInvalid("mysql: binlog: a partial update of %s.%s, which a session logged with binlog_row_value_options = PARTIAL_JSON; "+
				"log changes without it, and read %[1]s again", rel.stream.t.name, c.name)
		}
		x, err := binlogValue(c, rel.cols[i], v)
		if err != nil {
			return nil, fmt.Errorf("mysql: %s.%s: %w", rel.stream.t.name, c.name, err)
		}
		vals[j] = x
	}
	return vals, nil
}

func (r *cdcReader) emit(st *cdcStream, op string, vals []any, unchanged []string) error {
	r.txn.n++
	r.txn.sent[st] = r.txn.n
	return r.add(st, op, seqOf(r.epoch, r.txn.start, r.txn.n), vals, unchanged)
}

// add appends a change, with the columns it didn't send as unchanged.
func (r *cdcReader) add(st *cdcStream, op string, seq decimal128.Num, vals []any, unchanged []string) error {
	if err := st.b.add(vals); err != nil {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("mysql: %s: %w", st.t.name, err))
	}
	st.b.rb.Field(len(vals)).(interface{ Append(string) }).Append(op)
	st.b.rb.Field(len(vals) + 1).(interface{ Append(decimal128.Num) }).Append(seq)
	lb := st.b.rb.Field(len(vals) + 2).(*array.ListBuilder)
	if len(unchanged) == 0 {
		lb.AppendNull()
	} else {
		lb.Append(true)
		for _, c := range unchanged {
			lb.ValueBuilder().(*array.StringBuilder).Append(c)
		}
	}
	st.dirty = true
	if st.b.bytes >= r.s.cfg.BatchBytes {
		return st.flush(r.out)
	}
	return nil
}

func (st *cdcStream) flush(out source.Emitter) error {
	return st.b.flush(func(rec arrow.RecordBatch) error { return out.Records(st.t.name, rec) })
}

// passed runs once the read has reached pos: each chunk the binlog has
// reached is emitted.
func (r *cdcReader) passed() error {
	var held []*chunk
	var moved []*cdcStream
	for _, c := range r.chunks {
		if r.pos.less(c.at) {
			held = append(held, c)
			continue
		}
		st := c.stream
		seq := seqOf(r.epoch, c.at, 0)
		for _, row := range c.rows {
			if err := r.add(st, source.OpInsert, seq, row, nil); err != nil {
				return err
			}
		}
		if c.full {
			c.rng.After = c.last
		} else {
			st.ranges = slices.DeleteFunc(st.ranges, func(rng *snapshotRange) bool { return rng == c.rng })
			if len(st.ranges) == 0 {
				slog.Info("read a table's snapshot", "table", st.t.name)
			}
		}
		if !slices.Contains(moved, st) {
			moved = append(moved, st)
		}
	}
	r.chunks = held
	for _, st := range moved {
		if err := r.resumeStream(st); err != nil {
			return err
		}
	}
	return r.reachUntil()
}

func (r *cdcReader) snapshotsRead() bool {
	for _, st := range r.streams {
		if len(st.ranges) > 0 {
			return false
		}
	}
	return true
}

// readChunks reads the next chunk of as many key ranges not read yet as the
// read can hold connections for, in the tables' order.
func (r *cdcReader) readChunks(ctx context.Context) error {
	type read struct {
		st  *cdcStream
		rng *snapshotRange
	}
	var todo []read
	for _, st := range r.streams {
		for _, rng := range st.ranges {
			todo = append(todo, read{st, rng})
		}
	}
	if len(todo) == 0 {
		return nil
	}
	conns := []*sql.Conn{r.conn}
	defer func() {
		for _, c := range conns[1:] {
			r.conns.Put(c)
		}
	}()
	for len(conns) < len(todo) {
		c := r.conns.TryGet(ctx)
		if c == nil {
			break
		}
		conns = append(conns, c)
	}
	rctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	chunks := make([]*chunk, len(conns))
	var wg sync.WaitGroup
	for i, conn := range conns {
		wg.Go(func() {
			c, err := r.readChunk(rctx, conn, todo[i].st, todo[i].rng)
			if err != nil {
				cancel(err)
				return
			}
			chunks[i] = c
		})
	}
	wg.Wait()
	if err := context.Cause(rctx); err != nil {
		return err
	}
	r.chunks = chunks
	return r.passed()
}

// readChunk reads a key range's next chunk, on a connection only it is
// using: its rows with a locking read, then where the binlog ends.
func (r *cdcReader) readChunk(ctx context.Context, conn *sql.Conn, st *cdcStream, rng *snapshotRange) (*chunk, error) {
	t := st.t
	tr := &tableReader{s: r.s, t: t}
	keyCols := tr.cols(t.key)
	var w where
	if err := rng.where(keyCols[0], &w); err != nil {
		return nil, err
	}
	if rng.After != nil {
		if err := w.after(keyCols, rng.After); err != nil {
			return nil, err
		}
	}
	q := "SELECT " + tr.list() + " FROM " + tr.ident() + w.sql() + " ORDER BY " + tr.names(t.key) +
		" LIMIT " + strconv.Itoa(r.s.cfg.SnapshotChunkRows) + " LOCK IN SHARE MODE"
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	c := &chunk{stream: st, rng: rng}
	err = scan(ctx, tx, q, w.args, len(t.cols), func(vals []any) error {
		c.rows = append(c.rows, vals)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("mysql: snapshot of %s: %w", t.name, err)
	}
	if afterChunkSnapshot != nil {
		afterChunkSnapshot()
	}
	if c.at, _, err = binlogStatus(ctx, tx, r.srv); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, classify(err)
	}
	if n := len(c.rows); n > 0 {
		for _, k := range t.key {
			c.last = append(c.last, keyText(t.cols[k], c.rows[n-1][k]))
		}
	}
	c.full = len(c.rows) == r.s.cfg.SnapshotChunkRows
	return c, nil
}

// resume flushes and emits resume points: for streams that emitted since
// their last after resumeEvery, and for the rest after quietEvery if the
// binlog has moved on.
func (r *cdcReader) resume() error {
	now := time.Now()
	for _, st := range r.streams {
		if st.until != nil {
			continue
		}
		if st.dirty && now.Sub(st.resumedAt) >= resumeEvery || st.resumed.less(r.pos) && now.Sub(st.resumedAt) >= quietEvery {
			if err := r.resumeStream(st); err != nil {
				return err
			}
		}
	}
	return nil
}

// state is the stream's state at the read's position, or at its own if
// that's ahead.
func (r *cdcReader) state(st *cdcStream) ([]byte, error) {
	s := cdcState{Server: r.server, binlogPos: r.pos, Epoch: r.epoch, Snapshot: st.cursor(), Start: st.start, StartGTID: st.startGTID, XA: st.xa}
	if r.gset != nil {
		s.GTID = r.gset.String()
	}
	if !r.lastCommit.IsZero() {
		s.At = &r.lastCommit
	}
	if st.from != nil && st.fromGTID == nil && r.pos.less(st.from.binlogPos) {
		s.binlogPos, s.GTID, s.At = st.from.binlogPos, st.from.GTID, st.from.At
	}
	if s.At != nil {
		at := s.At.UTC()
		s.At = &at
	}
	return json.Marshal(s)
}

// cursor is the snapshot's position, as a state spells it.
func (st *cdcStream) cursor() *snapshotCursor {
	switch {
	case len(st.ranges) == 0:
		return nil
	case len(st.ranges) == 1 && st.ranges[0].keyRange == keyRange{}:
		return &snapshotCursor{After: st.ranges[0].After}
	}
	return &snapshotCursor{Ranges: st.ranges}
}

func (r *cdcReader) resumeStream(st *cdcStream) error {
	if err := st.flush(r.out); err != nil {
		return err
	}
	b, err := r.state(st)
	if err != nil {
		return err
	}
	if err := r.out.ResumePoint(st.t.name, b); err != nil {
		return err
	}
	st.dirty, st.resumed, st.resumedAt = false, r.pos, time.Now()
	return nil
}

// complete ends a bounded read: each stream's next sync starts where this
// one ended.
func (r *cdcReader) complete() error {
	for _, st := range r.streams {
		if st.until != nil {
			if !st.finished {
				if err := r.finish(st); err != nil {
					return err
				}
			}
			continue
		}
		if err := st.flush(r.out); err != nil {
			return err
		}
		b, err := r.state(st)
		if err != nil {
			return err
		}
		if err := r.out.Complete(st.t.name, b); err != nil {
			return err
		}
	}
	return nil
}

// progress reports how far the read is behind the binlog's end, in bytes
// and in time, and how long the server keeps the binlog it hasn't read.
func (r *cdcReader) progress(ctx context.Context, log bool) error {
	p, err := binlogRetention(ctx, r.conn, r.pos, r.lastCommit)
	if err != nil {
		slog.Debug("reading the binlog's retention failed", "err", err)
		return nil
	}
	p.Lag = r.lag(p.LagBytes)
	if log && p.LagBytes != nil {
		slog.Info("binlog lag", "bytes", *p.LagBytes)
	}
	if pr, ok := r.out.(source.Progresser); ok {
		return pr.Progress(p)
	}
	return nil
}

// lag is how old the change being read is: zero once the read has reached
// the binlog's end, unknown before the first transaction otherwise.
func (r *cdcReader) lag(behind *uint64) *time.Duration {
	switch {
	case r.txn != nil:
		return new(max(time.Since(r.txn.at), 0))
	case behind != nil && *behind == 0 || !r.pos.less(r.serverEnd) && behind == nil:
		return new(time.Duration(0))
	case !r.lastCommit.IsZero():
		return new(max(time.Since(r.lastCommit), 0))
	}
	return nil
}

// declaredOnly narrows t to the columns the run declared. A column added
// since the run's Discover has no declared type; left out here, it fails the
// read in relation, and the next attempt declares it. No columns: the
// request doesn't say.
func declaredOnly(t table, columns []string) (table, error) {
	if len(columns) == 0 {
		return t, nil
	}
	cols := make([]sourceColumn, 0, len(t.cols))
	at := make(map[int]int, len(t.cols))
	for i, c := range t.cols {
		if slices.Contains(columns, c.name) {
			at[i] = len(cols)
			cols = append(cols, c)
		}
	}
	key := make([]int, len(t.key))
	for i, k := range t.key {
		j, ok := at[k]
		if !ok {
			return t, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("mysql: %s has a new key column %q; the read starts again to take it", t.name, t.cols[k].name))
		}
		key[i] = j
	}
	t.cols, t.key = cols, key
	return t, nil
}

var errNoBinlog = errors.New("mysql: the server has no binlog; cdc needs log_bin = ON")

func configInvalid(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf(format, args...))
}

func positionLost(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf(format, args...))
}
