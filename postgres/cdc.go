package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.Acknowledger = (*Source)(nil)
	_ source.Teardowner   = (*Source)(nil)
)

const (
	// How often streams that emitted since their last resume point get one,
	// and how often the rest do, once the log has moved on.
	resumeEvery = time.Second
	quietEvery  = 10 * time.Second
	// How often the slot hears where the pipeline has committed to.
	statusEvery = 10 * time.Second
	// A log quiet for this long is asked where it is, and sooner while a
	// chunk waits for it.
	idleAfter = time.Second
	heldAfter = 10 * time.Millisecond
	// How often the read reports its progress, and logs it.
	progressEvery = 10 * time.Second
	logLagEvery   = time.Minute
)

// A record version's _openetl_seq: its transaction's commit LSN, then its
// position in the transaction, which is 0 for a snapshot's rows.
var (
	seqType  = &arrow.Decimal128Type{Precision: 38, Scale: 0}
	seqShift = decimal128.FromU64(1_000_000_000_000_000_000)
)

func seqOf(commit pglogrepl.LSN, n uint64) decimal128.Num {
	return decimal128.FromU64(uint64(commit)).Mul(seqShift).Add(decimal128.FromU64(n))
}

// afterChunkSnapshot, if set, runs between a chunk's snapshot and its WAL
// position: for tests.
var afterChunkSnapshot func()

// cdcState is a CDC stream's state.
type cdcState struct {
	Slot string `json:"slot"`
	// Every transaction that committed before it has been read.
	LSN lsn `json:"lsn"`
	// Until the table's snapshot has been read: where it continues.
	Snapshot *snapshotCursor `json:"snapshot,omitempty"`
}

type snapshotCursor struct {
	// The primary key of the last row read, spelled as text; none before
	// the first chunk.
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

// lsn is spelled as Postgres spells it.
type lsn pglogrepl.LSN

func (l lsn) MarshalText() ([]byte, error) { return []byte(pglogrepl.LSN(l).String()), nil }

func (l *lsn) UnmarshalText(b []byte) error {
	v, err := pglogrepl.ParseLSN(string(b))
	*l = lsn(v)
	return err
}

// slotName is the pipeline's replication slot: the config's, or one named
// after the pipeline.
func (s *Source) slotName(pipeline string) string {
	if s.cfg.Slot != "" {
		return s.cfg.Slot
	}
	var b strings.Builder
	for _, r := range strings.ToLower(pipeline) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	name := "openetl_" + b.String()
	if b.Len() == 0 || len(name) > 63 {
		h := sha256.Sum256([]byte(pipeline))
		name = "openetl_" + hex.EncodeToString(h[:16])
	}
	return name
}

// cdcStream is one table read through the slot.
type cdcStream struct {
	t table
	b *changeBatcher
	// Changes in transactions that committed before it were read before.
	from pglogrepl.LSN
	// What's left of the table's snapshot; empty once it has been read.
	ranges []*snapshotRange
	// Converts each column's text as the engine reads it.
	conv []func([]byte) []byte
	// Emitted since its last resume point.
	dirty bool
	// Its last resume point's position, and when.
	resumed   pglogrepl.LSN
	resumedAt time.Time
}

// relation is a table as the log describes it.
type relation struct {
	stream *cdcStream // nil if it isn't read
	// The stream's column of each of the relation's, or -1.
	at []int
	// The relation's column of each of the stream's key columns, or -1.
	key  []int
	conv []func([]byte) []byte
	// Each column's name.
	names []string
}

type txn struct {
	xid    uint32
	commit pglogrepl.LSN
	at     time.Time
	// Rows emitted so far.
	n uint64
	// Keys it changed in tables whose snapshot isn't read yet.
	keys map[*cdcStream][]string
}

// chunk is a snapshot's chunk, held until the log has passed every
// transaction its snapshot saw.
type chunk struct {
	stream *cdcStream
	rng    *snapshotRange
	snap   pgSnapshot
	// Transactions that committed at or after it weren't seen.
	lsn  pglogrepl.LSN
	rows map[string][][]byte
	// Keys in key order.
	keys []string
	last []string
	// It had chunkRows rows, so more may follow.
	full bool
}

type cdcReader struct {
	s          *Source
	out        source.Emitter
	conns      *connPool
	conn       *pgx.Conn
	repl       *pgconn.PgConn
	slot       string
	pub        string
	version    int
	continuous bool
	streams    []*cdcStream
	rels       map[uint32]*relation
	// Transactions that committed before it have been read.
	pos pglogrepl.LSN
	// A bounded read ends once pos reaches it.
	target pglogrepl.LSN
	// The slot's confirmed position, and what checkpoints have committed.
	confirmed pglogrepl.LSN
	acked     atomic.Uint64
	// Where the server last said its log ends, and when the transaction read
	// last committed.
	serverEnd  pglogrepl.LSN
	lastCommit time.Time
	txn        *txn
	// Chunks held, of different key ranges.
	chunks []*chunk
	// Transactions read lately that changed keys of tables whose snapshot
	// isn't read yet; a later chunk's snapshot may not have seen them.
	recent []*txn
}

// readCDC reads the streams through the pipeline's replication slot: each
// table's changes, and until it has been read, its snapshot, in chunks
// between them, a chunk of a key range per connection the read can hold. A
// chunk is read in a transaction of its own; it's held until the log has
// passed every transaction its snapshot saw, and a key a later transaction
// changes gets its chunk row first. Nothing is written to the source but the
// slot and, if missing, the publication.
func (s *Source) readCDC(ctx context.Context, conns *connPool, req source.ReadRequest, cfgs []source.ConfiguredStream, tables []table, out source.Emitter) error {
	conn, err := conns.get(ctx)
	if err != nil {
		return err
	}
	defer conns.put(ctx, conn)
	r := &cdcReader{s: s, out: out, conns: conns, conn: conn, slot: s.slotName(req.Pipeline), pub: s.cfg.Publication, continuous: req.Continuous, rels: map[uint32]*relation{}}
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&r.version); err != nil {
		return classify(err)
	}
	if r.version < 130000 {
		return configInvalid("postgres: CDC needs Postgres 13 or later")
	}
	for _, cs := range cfgs {
		i := 0
		for tables[i].name != cs.Name {
			i++
		}
		t := tables[i]
		if len(t.key) == 0 {
			return configInvalid("postgres: %s has no primary key, so it can't be read with cdc", t.name)
		}
		if t, err = declaredOnly(t, cs.Columns); err != nil {
			return err
		}
		st := &cdcStream{t: t}
		var state cdcState
		if cs.State != nil {
			if err := json.Unmarshal(cs.State, &state); err != nil {
				return fmt.Errorf("postgres: %s state: %w", cs.Name, err)
			}
		}
		if state.Slot != "" && state.Slot != r.slot {
			return positionLost("postgres: %s was read through replication slot %q, and the source now reads %q", t.name, state.Slot, r.slot)
		}
		// A state from a full refresh has no position, and starts the table
		// again.
		st.from = pglogrepl.LSN(state.LSN)
		if st.ranges, err = r.snapshotRanges(ctx, t, state); err != nil {
			return err
		}
		for _, rng := range st.ranges {
			if rng.After != nil && len(rng.After) != len(t.key) {
				return fmt.Errorf("postgres: %s state has %d key columns, the table has %d", t.name, len(rng.After), len(t.key))
			}
		}
		st.b = newChangeBatcher(memory.DefaultAllocator, t.cols)
		for _, c := range t.cols {
			st.conv = append(st.conv, textConv(c.oid))
		}
		r.streams = append(r.streams, st)
	}
	defer func() {
		for _, st := range r.streams {
			st.b.release()
		}
	}()
	if err := r.ensurePublication(ctx); err != nil {
		return err
	}
	confirmed, err := r.ensureSlot(ctx)
	if err != nil {
		return err
	}
	r.confirmed = confirmed
	if !r.continuous {
		var at string
		if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&at); err != nil {
			return classify(err)
		}
		if r.target, err = pglogrepl.ParseLSN(at); err != nil {
			return err
		}
	}
	// From the earliest stream's position, or the slot's if a stream has
	// none.
	start := r.streams[0].from
	for _, st := range r.streams {
		start = min(start, st.from)
	}
	start = max(start, confirmed)
	r.pos = start
	for _, st := range r.streams {
		st.resumed = max(st.from, start)
	}

	rc := s.conn.Config.Copy()
	rc.RuntimeParams["replication"] = "database"
	if r.repl, err = pgconn.ConnectConfig(ctx, rc); err != nil {
		return classify(fmt.Errorf("postgres: replication connection: %w", err))
	}
	defer func() { _ = r.repl.Close(context.WithoutCancel(ctx)) }()
	err = pglogrepl.StartReplication(ctx, r.repl, r.slot, start, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names " + quoteLiteral(pgx.Identifier{r.pub}.Sanitize())},
	})
	if err != nil {
		if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "55006" {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: replication slot %q: %w", r.slot, err))
		}
		return classify(fmt.Errorf("postgres: start replication: %w", err))
	}
	s.setReader(r)
	defer s.setReader(nil)
	return r.run(ctx)
}

// snapshotRanges is what's left of a table's snapshot, from its state. One
// not started is read in key ranges if several connections can read it.
func (r *cdcReader) snapshotRanges(ctx context.Context, t table, state cdcState) ([]*snapshotRange, error) {
	cursor := state.Snapshot
	switch {
	case state.Slot == "" || cursor != nil && cursor.After == nil && len(cursor.Ranges) == 0:
		if r.conns.limit == 1 {
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

func configInvalid(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf(format, args...))
}

func positionLost(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf(format, args...))
}

// ensurePublication creates the publication, or adds the tables it lacks.
func (r *cdcReader) ensurePublication(ctx context.Context) error {
	names := make([]string, len(r.streams))
	for i, st := range r.streams {
		names[i] = pgx.Identifier{r.s.cfg.Schema, st.t.name}.Sanitize()
	}
	pub := pgx.Identifier{r.pub}.Sanitize()
	var all bool
	err := r.conn.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname = $1", r.pub).Scan(&all)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = r.conn.Exec(ctx, "CREATE PUBLICATION "+pub+" FOR TABLE "+strings.Join(names, ", ")+" WITH (publish_via_partition_root = true)")
		if err != nil {
			return classify(fmt.Errorf("postgres: create publication %s (or create it yourself, for the tables read with cdc): %w", r.pub, err))
		}
		return nil
	}
	if err != nil || all {
		return classify(err)
	}
	var missing []string
	for i, st := range r.streams {
		var in bool
		err := r.conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)", r.pub, r.s.cfg.Schema, st.t.name).Scan(&in)
		if err != nil {
			return classify(err)
		}
		if !in {
			missing = append(missing, names[i])
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if _, err := r.conn.Exec(ctx, "ALTER PUBLICATION "+pub+" ADD TABLE "+strings.Join(missing, ", ")); err != nil {
		return classify(fmt.Errorf("postgres: publication %s lacks %s, and adding them failed: %w", r.pub, strings.Join(missing, ", "), err))
	}
	return nil
}

// ensureSlot creates the slot, or takes it over from a connection that
// still holds it, and returns its confirmed position. A slot that's gone or
// invalidated while a stream has a position in it is lost; one invalidated
// while none has is made again.
func (r *cdcReader) ensureSlot(ctx context.Context) (pglogrepl.LSN, error) {
	invalidated := "coalesce(wal_status = 'lost', false)"
	if r.version >= 170000 {
		invalidated += " OR invalidation_reason IS NOT NULL"
	}
	for attempt := 0; ; attempt++ {
		var active, invalid bool
		var pid *int32
		var confirmed, plugin string
		var db *string
		err := r.conn.QueryRow(ctx, `SELECT active, active_pid, coalesce(confirmed_flush_lsn::text, ''), coalesce(plugin, ''), database, `+invalidated+`
			FROM pg_replication_slots WHERE slot_name = $1`, r.slot).Scan(&active, &pid, &confirmed, &plugin, &db, &invalid)
		if errors.Is(err, pgx.ErrNoRows) {
			for _, st := range r.streams {
				if st.from != 0 {
					return 0, positionLost("postgres: replication slot %q is gone, as after a failover without failover slots; its tables need a snapshot again", r.slot)
				}
			}
			return r.createSlot(ctx)
		}
		if err == nil && invalid {
			for _, st := range r.streams {
				if st.from != 0 {
					return 0, positionLost("postgres: replication slot %q was invalidated, as max_slot_wal_keep_size does to a slot too far behind; its tables need a snapshot again", r.slot)
				}
			}
			if _, err := r.conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", r.slot); err != nil {
				return 0, classify(fmt.Errorf("postgres: drop invalidated replication slot %q: %w", r.slot, err))
			}
			slog.Info("dropped an invalidated replication slot, to read from the start", "slot", r.slot)
			return r.createSlot(ctx)
		}
		if err != nil {
			return 0, classify(err)
		}
		var current string
		if err := r.conn.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
			return 0, classify(err)
		}
		if plugin != "pgoutput" || db == nil || *db != current {
			return 0, configInvalid("postgres: replication slot %q isn't a pgoutput slot of database %s", r.slot, current)
		}
		if !active {
			return pglogrepl.ParseLSN(confirmed)
		}
		if attempt == 0 && pid != nil {
			// An earlier job's connection; the job is fenced already.
			if _, err := r.conn.Exec(ctx, "SELECT pg_terminate_backend($1)", *pid); err != nil {
				slog.Warn("can't end the connection holding the replication slot; waiting for it to time out", "slot", r.slot, "pid", *pid, "err", err)
			}
		}
		if attempt >= 20 {
			return 0, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: replication slot %q is held by PID %v", r.slot, deref(pid)))
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func deref(p *int32) any {
	if p == nil {
		return "?"
	}
	return *p
}

func (r *cdcReader) createSlot(ctx context.Context) (pglogrepl.LSN, error) {
	q := "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')"
	if r.version >= 170000 {
		// Kept on a standby that synchronizes slots, so it survives failover.
		q = "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', failover => true)"
	}
	var at string
	if err := r.conn.QueryRow(ctx, q, r.slot).Scan(&at); err != nil {
		if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "55000" {
			return 0, configInvalid("postgres: %s (CDC needs wal_level = logical)", pe.Message)
		}
		return 0, classify(fmt.Errorf("postgres: create replication slot %q: %w", r.slot, err))
	}
	slog.Info("created a replication slot", "slot", r.slot, "lsn", at)
	return pglogrepl.ParseLSN(at)
}

func (r *cdcReader) run(ctx context.Context) error {
	nextStatus := time.Now()
	nextProgress, nextLog := time.Now(), time.Now().Add(logLagEvery)
	quietSince := time.Now()
	// The next status asks the server where it is.
	ask := false
	for {
		if r.txn == nil {
			if len(r.chunks) == 0 {
				if err := r.readChunks(ctx); err != nil {
					return err
				}
				if len(r.chunks) > 0 {
					// Learn at once whether the log has passed them.
					nextStatus, ask = time.Time{}, true
				}
			}
			if err := r.resume(); err != nil {
				return err
			}
			if !r.continuous && len(r.chunks) == 0 && r.pos >= r.target && r.snapshotsRead() {
				return r.complete()
			}
		}
		now := time.Now()
		if acked := pglogrepl.LSN(r.acked.Load()); acked > r.confirmed {
			r.confirmed, nextStatus = acked, time.Time{}
		}
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
		if !nextStatus.After(now) {
			if err := r.status(ctx, ask); err != nil {
				return err
			}
			nextStatus, ask = now.Add(statusEvery), false
		}
		// Chunks that passed at once leave the log nothing to wait for.
		if r.txn == nil && len(r.chunks) == 0 && !r.snapshotsRead() {
			continue
		}
		quiet := idleAfter
		if r.txn == nil && len(r.chunks) > 0 {
			quiet = heldAfter
		}
		deadline := now.Add(quiet)
		if nextStatus.Before(deadline) {
			deadline = nextStatus
		}
		dctx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := r.repl.ReceiveMessage(dctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				if r.txn == nil && time.Since(quietSince) >= quiet {
					nextStatus, ask = time.Time{}, true
					quietSince = time.Now()
				}
				continue
			}
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: replication: %w", err))
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			switch m.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				k, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					return err
				}
				quietSince = time.Now()
				r.serverEnd = max(r.serverEnd, k.ServerWALEnd)
				if r.txn == nil && k.ServerWALEnd > r.pos {
					r.pos = k.ServerWALEnd
					if err := r.passed(); err != nil {
						return err
					}
				}
				if k.ReplyRequested {
					nextStatus = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				x, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					return err
				}
				quietSince = time.Now()
				r.serverEnd = max(r.serverEnd, x.ServerWALEnd)
				if err := r.message(ctx, x.WALData); err != nil {
					return err
				}
			}
		case *pgproto3.ErrorResponse:
			return classify(pgconn.ErrorResponseToPgError(m))
		// The server may send these at any time, on any connection.
		case *pgproto3.NoticeResponse:
			slog.Info("the server sent a notice on the replication connection", "slot", r.slot, "severity", m.Severity, "message", m.Message)
		case *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
		default:
			return fmt.Errorf("postgres: replication: unexpected %T", msg)
		}
	}
}

// status tells the slot what checkpoints have committed, and with ask,
// asks the server where it is.
func (r *cdcReader) status(ctx context.Context, ask bool) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.repl, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.confirmed, WALFlushPosition: r.confirmed, WALApplyPosition: r.confirmed, ReplyRequested: ask,
	})
	if err != nil {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: replication status: %w", err))
	}
	return nil
}

// progress reports how far the slot is behind the log, how much more WAL it
// can hold before max_slot_wal_keep_size invalidates it, and how old the
// transaction being read is.
func (r *cdcReader) progress(ctx context.Context, log bool) error {
	var behind, safe *int64
	if err := r.conn.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, safe_wal_size FROM pg_replication_slots WHERE slot_name = $1", r.slot).Scan(&behind, &safe); err != nil {
		slog.Debug("reading the replication slot's lag failed", "slot", r.slot, "err", err)
		return nil
	}
	p := source.Progress{Lag: r.lag()}
	if behind != nil && *behind >= 0 {
		p.LagBytes = new(uint64(*behind))
	}
	if safe != nil && *safe >= 0 {
		p.RetentionLeftBytes = new(uint64(*safe))
	}
	if log && behind != nil {
		slog.Info("replication slot lag", "slot", r.slot, "bytes", *behind)
	}
	if pr, ok := r.out.(source.Progresser); ok {
		return pr.Progress(p)
	}
	return nil
}

// lag is how old the change being read is: zero once the read has reached
// where the server last said its log ends, and unknown before the first
// transaction otherwise.
func (r *cdcReader) lag() *time.Duration {
	switch {
	case r.txn != nil:
		return new(max(time.Since(r.txn.at), 0))
	case r.pos >= r.serverEnd:
		return new(time.Duration(0))
	case !r.lastCommit.IsZero():
		return new(max(time.Since(r.lastCommit), 0))
	}
	return nil
}

func (r *cdcReader) message(ctx context.Context, data []byte) error {
	m, err := pglogrepl.Parse(data)
	if err != nil {
		return fmt.Errorf("postgres: replication: %w", err)
	}
	switch m := m.(type) {
	case *pglogrepl.RelationMessage:
		return r.relation(ctx, m)
	case *pglogrepl.BeginMessage:
		r.txn = &txn{xid: m.Xid, commit: m.FinalLSN, at: m.CommitTime}
	case *pglogrepl.CommitMessage:
		if r.txn == nil {
			return errors.New("postgres: replication: a commit without a begin")
		}
		if len(r.txn.keys) > 0 {
			r.recent = append(r.recent, r.txn)
		}
		r.lastCommit = r.txn.at
		r.txn = nil
		r.pos = m.TransactionEndLSN
		return r.passed()
	case *pglogrepl.InsertMessage:
		return r.change(m.RelationID, source.OpInsert, 0, nil, m.Tuple)
	case *pglogrepl.UpdateMessage:
		return r.change(m.RelationID, source.OpUpdate, m.OldTupleType, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessage:
		return r.change(m.RelationID, source.OpDelete, m.OldTupleType, m.OldTuple, nil)
	case *pglogrepl.TruncateMessage:
		for _, id := range m.RelationIDs {
			if rel := r.rels[id]; rel != nil && rel.stream != nil {
				slog.Warn("a table read with cdc was truncated; its rows stay in the destination, so a new snapshot of it brings the destination back in step", "table", rel.stream.t.name)
			}
		}
	}
	return nil
}

// relation maps a table the log describes to its stream. A column the
// stream doesn't have fails the read, so the next attempt declares it.
func (r *cdcReader) relation(ctx context.Context, m *pglogrepl.RelationMessage) error {
	rel := &relation{}
	r.rels[m.RelationID] = rel
	name := m.RelationName
	if m.Namespace != r.s.cfg.Schema || r.stream(name) == nil {
		// A partition, published under its own name.
		var root *string
		err := r.conn.QueryRow(ctx, `SELECT p.relname FROM pg_class p JOIN pg_namespace n ON n.oid = p.relnamespace
			WHERE p.oid = pg_partition_root($1::oid::regclass) AND p.oid <> $1::oid AND n.nspname = $2`, m.RelationID, r.s.cfg.Schema).Scan(&root)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return classify(err)
		}
		if root == nil {
			return nil
		}
		name = *root
	}
	st := r.stream(name)
	if st == nil {
		return nil
	}
	rel.stream = st
	rel.key = make([]int, len(st.t.key))
	for i := range rel.key {
		rel.key[i] = -1
	}
	for _, c := range m.Columns {
		j := -1
		for k, sc := range st.t.cols {
			if sc.name == c.Name {
				j = k
			}
		}
		if j < 0 {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: %s has a new column %q; the read starts again to take it", st.t.name, c.Name))
		}
		for k, kc := range st.t.key {
			if kc == j {
				rel.key[k] = len(rel.at)
			}
		}
		rel.at = append(rel.at, j)
		rel.conv = append(rel.conv, textConv(c.DataType))
		rel.names = append(rel.names, c.Name)
	}
	for _, k := range rel.key {
		if k < 0 {
			return fmt.Errorf("postgres: %s's log lacks a primary key column", st.t.name)
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

// change emits one change: a key change as a delete of the old key and the
// new row.
func (r *cdcReader) change(id uint32, op string, oldType uint8, old, new *pglogrepl.TupleData) error {
	rel := r.rels[id]
	if rel == nil {
		return fmt.Errorf("postgres: replication: a change to relation %d, which the log hasn't described", id)
	}
	st := rel.stream
	if st == nil || r.txn == nil || r.txn.commit < st.from {
		return nil
	}
	var oldKey string
	if old != nil {
		k, err := rel.keyOf(old)
		if err != nil {
			return err
		}
		oldKey = k
	}
	if op == source.OpDelete {
		vals, _ := rel.values(old, nil)
		return r.emit(st, oldKey, op, vals, nil)
	}
	key, err := rel.keyOf(new)
	if err != nil {
		if old == nil || !errors.Is(err, errKeyUnchanged) {
			return err
		}
		key = oldKey
	}
	var full *pglogrepl.TupleData
	if oldType == 'O' {
		full = old
	}
	if old != nil && oldKey != key {
		vals, _ := rel.values(old, nil)
		if err := r.emit(st, oldKey, source.OpDelete, vals, nil); err != nil {
			return err
		}
	}
	vals, unchanged := rel.values(new, full)
	return r.emit(st, key, op, vals, unchanged)
}

var errKeyUnchanged = errors.New("postgres: a primary key value wasn't sent")

// keyOf spells a tuple's key as text, as the snapshot's queries do.
func (rel *relation) keyOf(t *pglogrepl.TupleData) (string, error) {
	var b strings.Builder
	for i, at := range rel.key {
		c := t.Columns[at]
		switch c.DataType {
		case pglogrepl.TupleDataTypeText:
		case pglogrepl.TupleDataTypeToast:
			return "", fmt.Errorf("%w for %s, as an unchanged TOAST value; set REPLICA IDENTITY FULL on it", errKeyUnchanged, rel.stream.t.name)
		default:
			return "", fmt.Errorf("postgres: %s's primary key column %q is null in the log", rel.stream.t.name, rel.names[at])
		}
		if i > 0 {
			b.WriteByte(0)
		}
		b.Write(c.Data)
	}
	return b.String(), nil
}

// values are a tuple's values by stream column, as text the engine reads,
// with the columns an update didn't send, which take full's values if it
// has them.
func (rel *relation) values(t, full *pglogrepl.TupleData) ([][]byte, []string) {
	vals := make([][]byte, len(rel.stream.t.cols))
	var unchanged []string
	for i, c := range t.Columns {
		j := rel.at[i]
		switch c.DataType {
		case pglogrepl.TupleDataTypeText:
			vals[j] = convert(rel.conv[i], c.Data)
		case pglogrepl.TupleDataTypeToast:
			if full != nil && full.Columns[i].DataType == pglogrepl.TupleDataTypeText {
				vals[j] = convert(rel.conv[i], full.Columns[i].Data)
			} else {
				unchanged = append(unchanged, rel.names[i])
			}
		}
	}
	return vals, unchanged
}

func convert(f func([]byte) []byte, v []byte) []byte {
	if f == nil {
		return v
	}
	return f(v)
}

// emit adds a change to its stream's batch, after the chunk row of its key
// if the chunk's snapshot didn't see the change.
func (r *cdcReader) emit(st *cdcStream, key, op string, vals [][]byte, unchanged []string) error {
	for _, c := range r.chunks {
		if c.stream != st {
			continue
		}
		if row, ok := c.rows[key]; ok && !c.snap.sees(r.txn.xid) {
			delete(c.rows, key)
			if err := r.add(st, source.OpInsert, seqOf(r.txn.commit, 0), row, nil); err != nil {
				return err
			}
		}
	}
	if len(st.ranges) > 0 {
		if r.txn.keys == nil {
			r.txn.keys = map[*cdcStream][]string{}
		}
		r.txn.keys[st] = append(r.txn.keys[st], key)
	}
	r.txn.n++
	return r.add(st, op, seqOf(r.txn.commit, r.txn.n), vals, unchanged)
}

func (r *cdcReader) add(st *cdcStream, op string, seq decimal128.Num, vals [][]byte, unchanged []string) error {
	st.b.add(op, seq, vals, unchanged)
	st.dirty = true
	if st.b.bytes >= r.s.cfg.BatchBytes {
		return st.b.flush(st.t.name, r.out)
	}
	return nil
}

// passed runs once the read has reached pos: each chunk is emitted if the
// log has passed every transaction its snapshot saw.
func (r *cdcReader) passed() error {
	var held []*chunk
	var moved []*cdcStream
	for _, c := range r.chunks {
		if r.pos < c.lsn {
			held = append(held, c)
			continue
		}
		st := c.stream
		seq := seqOf(c.lsn, 0)
		for _, k := range c.keys {
			if row, ok := c.rows[k]; ok {
				if err := r.add(st, source.OpInsert, seq, row, nil); err != nil {
					return err
				}
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
	return nil
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
// read can hold connections for, in the tables' order. Every chunk's snapshot
// is taken with the log stopped between transactions, as one alone would be,
// and is held until the log passes its own WAL position.
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
		r.recent = nil
		// The log can go on for as long as it likes, on one connection.
		r.conns.closeIdle(ctx)
		return nil
	}
	conns := []*pgx.Conn{r.conn}
	defer func() {
		for _, c := range conns[1:] {
			r.conns.put(ctx, c)
		}
	}()
	for len(conns) < len(todo) {
		c := r.conns.tryGet(ctx)
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
	r.reconcile(chunks)
	r.chunks = chunks
	return r.passed()
}

// reconcile drops from new chunks the keys of transactions read before them
// that their snapshots didn't see yet, which changed those keys later than
// the chunks show. It forgets transactions every later snapshot will see.
func (r *cdcReader) reconcile(chunks []*chunk) {
	kept := r.recent[:0]
	for _, x := range r.recent {
		seen := true
		for _, c := range chunks {
			if c.snap.full(x.xid) < c.snap.xmin {
				continue
			}
			seen = false
			if !c.snap.sees(x.xid) {
				for _, k := range x.keys[c.stream] {
					delete(c.rows, k)
				}
			}
		}
		if !seen {
			kept = append(kept, x)
		}
	}
	r.recent = kept
}

// readChunk reads a key range's next chunk, on a connection only it is using.
func (r *cdcReader) readChunk(ctx context.Context, conn *pgx.Conn, st *cdcStream, rng *snapshotRange) (*chunk, error) {
	t := st.t
	cols := make([]string, len(t.cols))
	for i, c := range t.cols {
		cols[i] = pgx.Identifier{c.name}.Sanitize()
	}
	key := make([]string, len(t.key))
	keyText := make([]string, len(t.key))
	for i, k := range t.key {
		key[i] = cols[k]
		keyText[i] = cols[k] + "::text AS _openetl_key_" + strconv.Itoa(i)
	}
	q := "SELECT " + strings.Join(cols, ", ") + ", " + strings.Join(keyText, ", ") + " FROM " + pgx.Identifier{r.s.cfg.Schema, t.name}.Sanitize()
	var args [][]byte
	arg := func(v string) string {
		args = append(args, []byte(v))
		return "$" + strconv.Itoa(len(args))
	}
	where := rng.where(key[0], arg)
	if rng.After != nil {
		where = append(where, afterKey(key, rng.After, arg))
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY " + strings.Join(key, ", ") + " LIMIT " + strconv.Itoa(r.s.cfg.SnapshotChunkRows)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// The transaction's snapshot is taken by its first statement, and the
	// WAL position after it.
	var snapText, at string
	if err := tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&snapText); err != nil {
		return nil, classify(err)
	}
	if afterChunkSnapshot != nil {
		afterChunkSnapshot()
	}
	if err := tx.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&at); err != nil {
		return nil, classify(err)
	}
	c := &chunk{stream: st, rng: rng, rows: map[string][][]byte{}}
	if c.snap, err = parseSnapshot(snapText); err != nil {
		return nil, err
	}
	if c.lsn, err = pglogrepl.ParseLSN(at); err != nil {
		return nil, err
	}
	n := len(t.cols)
	rr := tx.Conn().PgConn().ExecParams(ctx, q, args, nil, nil, nil)
	for rr.NextRow() {
		vals := rr.Values()
		row := make([][]byte, n)
		for i, v := range vals[:n] {
			if v != nil {
				row[i] = convert(st.conv[i], append([]byte{}, v...))
			}
		}
		c.last = c.last[:0]
		var b strings.Builder
		for i, v := range vals[n:] {
			if i > 0 {
				b.WriteByte(0)
			}
			b.Write(v)
			c.last = append(c.last, string(v))
		}
		k := b.String()
		c.rows[k] = row
		c.keys = append(c.keys, k)
	}
	if _, err := rr.Close(); err != nil {
		return nil, classify(fmt.Errorf("postgres: snapshot of %s: %w", t.name, err))
	}
	c.last = append([]string(nil), c.last...)
	c.full = len(c.keys) == r.s.cfg.SnapshotChunkRows
	if err := tx.Commit(ctx); err != nil {
		return nil, classify(err)
	}
	return c, nil
}

// resume flushes and emits resume points: for streams that emitted since
// their last after resumeEvery, and for the rest after quietEvery if the
// log has moved on.
func (r *cdcReader) resume() error {
	now := time.Now()
	for _, st := range r.streams {
		if st.dirty && now.Sub(st.resumedAt) >= resumeEvery || r.pos > st.resumed && now.Sub(st.resumedAt) >= quietEvery {
			if err := r.resumeStream(st); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *cdcReader) state(st *cdcStream) ([]byte, error) {
	return json.Marshal(cdcState{Slot: r.slot, LSN: lsn(max(st.from, r.pos)), Snapshot: st.cursor()})
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
	if err := st.b.flush(st.t.name, r.out); err != nil {
		return err
	}
	b, err := r.state(st)
	if err != nil {
		return err
	}
	if err := r.out.ResumePoint(st.t.name, b); err != nil {
		return err
	}
	st.dirty, st.resumed, st.resumedAt = false, max(st.from, r.pos), time.Now()
	return nil
}

// complete ends a bounded read: each stream's next sync starts where this
// one ended.
func (r *cdcReader) complete() error {
	for _, st := range r.streams {
		if err := st.b.flush(st.t.name, r.out); err != nil {
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

// ack records that checkpoints up to at have committed.
func (r *cdcReader) ack(at pglogrepl.LSN) {
	for {
		cur := r.acked.Load()
		if uint64(at) <= cur || r.acked.CompareAndSwap(cur, uint64(at)) {
			return
		}
	}
}

func (s *Source) setReader(r *cdcReader) {
	s.mu.Lock()
	s.reader = r
	s.mu.Unlock()
}

// Committed lets the slot discard the log before the streams' positions.
// Without a read running, it advances the slot itself.
func (s *Source) Committed(ctx context.Context, states map[string][]byte, _ map[string][]source.Split) error {
	var low pglogrepl.LSN
	slot := ""
	for _, b := range states {
		var st cdcState
		if json.Unmarshal(b, &st) != nil || st.Slot == "" || st.LSN == 0 {
			continue
		}
		if low == 0 || pglogrepl.LSN(st.LSN) < low {
			low, slot = pglogrepl.LSN(st.LSN), st.Slot
		}
	}
	if low == 0 {
		return nil
	}
	s.mu.Lock()
	r := s.reader
	s.mu.Unlock()
	if r != nil && r.slot == slot {
		r.ack(low)
		return nil
	}
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	_, err = conn.Exec(ctx, `SELECT pg_replication_slot_advance(slot_name, $2::pg_lsn) FROM pg_replication_slots
		WHERE slot_name = $1 AND NOT active AND confirmed_flush_lsn < $2::pg_lsn`, slot, low.String())
	return classify(err)
}

// Teardown drops the pipeline's replication slot, ending a connection that
// still holds it. The publication stays: other pipelines may use it.
func (s *Source) Teardown(ctx context.Context, pipeline string) error {
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	slot := s.slotName(pipeline)
	if _, err := conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active", slot); err != nil {
		return classify(err)
	}
	for attempt := 0; ; attempt++ {
		_, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slot)
		pe, ok := errors.AsType[*pgconn.PgError](err)
		if !ok || pe.Code != "55006" || attempt >= 20 {
			if err == nil {
				slog.Info("dropped the replication slot", "slot", slot)
			}
			return classify(err)
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pgSnapshot is pg_current_snapshot()'s: which transactions it sees.
type pgSnapshot struct {
	xmin, xmax uint64
	xip        map[uint64]bool
}

func parseSnapshot(s string) (pgSnapshot, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return pgSnapshot{}, fmt.Errorf("postgres: a snapshot %q", s)
	}
	var snap pgSnapshot
	var err error
	if snap.xmin, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
		return snap, err
	}
	if snap.xmax, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
		return snap, err
	}
	snap.xip = map[uint64]bool{}
	for x := range strings.SplitSeq(parts[2], ",") {
		if x == "" {
			continue
		}
		v, err := strconv.ParseUint(x, 10, 64)
		if err != nil {
			return snap, err
		}
		snap.xip[v] = true
	}
	return snap, nil
}

// full is a transaction ID with the epoch, near the snapshot's: the ID
// within 2^31 of xmax whose low 32 bits are xid.
func (s pgSnapshot) full(xid uint32) uint64 {
	ahead := (uint64(xid) - s.xmax) & math.MaxUint32
	if ahead < 1<<31 {
		return s.xmax + ahead
	}
	return s.xmax - (1<<32 - ahead)
}

// sees reports whether the snapshot sees a committed transaction's changes.
func (s pgSnapshot) sees(xid uint32) bool {
	x := s.full(xid)
	return x < s.xmin || x < s.xmax && !s.xip[x]
}

// textConv converts a type's text output where the engine reads another
// spelling: booleans, and bytea's hex as base64.
func textConv(oid uint32) func([]byte) []byte {
	switch oid {
	case pgtype.BoolOID:
		return func(v []byte) []byte {
			switch string(v) {
			case "t":
				return []byte("true")
			case "f":
				return []byte("false")
			}
			return v
		}
	case pgtype.ByteaOID:
		return func(v []byte) []byte {
			h, ok := strings.CutPrefix(string(v), `\x`)
			if !ok {
				return v
			}
			b, err := hex.DecodeString(h)
			if err != nil {
				return v
			}
			return []byte(base64.StdEncoding.EncodeToString(b))
		}
	}
	return nil
}

// declaredOnly narrows t to the columns the run declared. A column added
// since the run's Discover has no declared type, so its text would land as
// text. Left out here, it fails the read in relation, and the next attempt
// declares it. No columns: the request doesn't say.
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
			return t, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: %s has a new key column %q; the read starts again to take it", t.name, t.cols[k].name))
		}
		key[i] = j
	}
	t.cols, t.key = cols, key
	return t, nil
}

// changeBatcher builds a CDC stream's batches: each column as text, which
// the engine parses as the declared type, then the control columns.
type changeBatcher struct {
	b     *array.RecordBuilder
	n     int
	rows  int
	bytes int
}

func newChangeBatcher(mem memory.Allocator, cols []sourceColumn) *changeBatcher {
	fields := make([]arrow.Field, 0, len(cols)+3)
	for _, c := range cols {
		fields = append(fields, arrow.Field{Name: c.name, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	fields = append(fields, source.OpField(), source.SeqField(seqType), source.UnchangedField())
	return &changeBatcher{b: array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil)), n: len(cols)}
}

// add appends a row; a nil value is null.
func (c *changeBatcher) add(op string, seq decimal128.Num, vals [][]byte, unchanged []string) {
	for i := range c.n {
		sb := c.b.Field(i).(*array.StringBuilder)
		if vals[i] == nil {
			sb.AppendNull()
			continue
		}
		sb.BinaryBuilder.Append(vals[i])
		c.bytes += len(vals[i])
	}
	c.b.Field(c.n).(*array.StringBuilder).Append(op)
	c.b.Field(c.n + 1).(*array.Decimal128Builder).Append(seq)
	lb := c.b.Field(c.n + 2).(*array.ListBuilder)
	if len(unchanged) == 0 {
		lb.AppendNull()
	} else {
		lb.Append(true)
		for _, u := range unchanged {
			lb.ValueBuilder().(*array.StringBuilder).Append(u)
		}
	}
	c.rows++
}

func (c *changeBatcher) flush(stream string, out source.Emitter) error {
	if c.rows == 0 {
		return nil
	}
	rec := c.b.NewRecordBatch()
	c.rows, c.bytes = 0, 0
	if err := out.Records(stream, rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (c *changeBatcher) release() { c.b.Release() }
