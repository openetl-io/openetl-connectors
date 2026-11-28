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
	// How long a read waits for the connection holding its slot to let it
	// go: polling first after slotPollFirst, then twice as long each time,
	// up to slotPollMax apart.
	slotHeldFor   = 10 * time.Second
	slotPollFirst = 10 * time.Millisecond
	slotPollMax   = 500 * time.Millisecond
	// The most changes of a stream whose keys need the server to place them
	// (place) that wait to be placed together.
	placeEvery = 1000
)

// How long a read waits for a transaction the log has read to be visible to
// new snapshots, as one is once its backend has left the proc array; a var
// for tests.
var visibleWithin = 10 * time.Second

// A record version's _openetl_seq: its transaction's commit LSN, then its
// position in the transaction, which is 0 for a snapshot's rows.
var (
	seqType  = &arrow.Decimal128Type{Precision: 38, Scale: 0}
	seqShift = decimal128.FromU64(1_000_000_000_000_000_000)
)

func seqOf(commit pglogrepl.LSN, n uint64) decimal128.Num {
	return decimal128.FromU64(uint64(commit)).Mul(seqShift).Add(decimal128.FromU64(n))
}

// beforeChunkSnapshot, if set, runs in a chunk's transaction before its
// snapshot: for tests.
var beforeChunkSnapshot func()

// afterChunkSnapshot, if set, runs between a chunk's snapshot and its WAL
// position: for tests.
var afterChunkSnapshot func()

// beforeStartReplication, if set, runs once a read has checked its slot's
// position, before it takes the slot: for tests.
var beforeStartReplication func()

// snapshotFrom, if set, gives before each snapshot the read takes, a chunk's
// or one that asks what's visible, the ID of an exported snapshot to take in
// its place, or "" for none: for tests.
var snapshotFrom func() string

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

// slotName is the pipeline's replication slot: the config's, or openetl_ and
// the lowercase letters and digits of pipeline, the pipeline's ID.
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
	// While ranges are left: the changes read and not placed yet (settle),
	// in the log's order, and their values' bytes.
	pending      []*logChange
	pendingBytes int
	// Its key columns are all integers, which place compares in Go.
	ints bool
	// Otherwise, their types and collations as SQL names them, the latter
	// empty for a type without one; nil until the first place.
	types, collations []string
	// The snapshot as place compares keys with it; nil once a chunk is read
	// or passed, until the next place.
	plan *plan
	// Converts each column's text as the engine reads it.
	conv []func([]byte) []byte
	// Emitted since its last resume point.
	dirty bool
	// Its last resume point's position, and when.
	resumed   pglogrepl.LSN
	resumedAt time.Time
	// Where a bounded read stops, its until's or its complete_at's
	// position: it emits no change of a transaction that committed there
	// or later, and completes there, with stopState, after a resume point
	// with it if it's an until. Zero: where the read would end.
	stop      pglogrepl.LSN
	stopState []byte
	until     bool
	// It stopped, and completed.
	done bool
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
	// Changes read so far.
	n uint64
	// What a chunk read later must see it for, here or after a restart: the
	// streams it changed a key of that no chunk had read, until each one's
	// snapshot is read, and the held chunks whose snapshots didn't see it
	// change their keys, until each one passes.
	ahead []*cdcStream
	held  []*chunk
	// It's in the reader's unseen; a snapshot saw it, and so every later one
	// does.
	listed, seen bool
}

// version is a version of a row as a stream's batch takes it.
type version struct {
	op        string
	seq       decimal128.Num
	vals      [][]byte
	unchanged []string
}

// logChange is a change the log read to a stream with a snapshot left to
// read, with its key spelled as keyOf spells it.
type logChange struct {
	version
	x   *txn
	key string
}

// chunk is a snapshot's chunk, held until the log has passed every
// transaction its snapshot saw.
type chunk struct {
	stream *cdcStream
	rng    *snapshotRange
	snap   pgSnapshot
	// Transactions that committed at or after it weren't seen. It's held
	// until the log reaches it, and its rows' sequence is its.
	lsn  pglogrepl.LSN
	rows map[string][][]byte
	// Keys in key order.
	keys []string
	last []string
	// Its snapshot saw rows of its range past it, so the range goes on.
	full bool
	// What it emits after its rows: for each of its keys that a change its
	// snapshot didn't see came to, the row it read of the key, if any, then
	// each change to the key, in the log's order.
	after []version
	// Its rows' bytes, and the changes' in after.
	bytes, afterBytes int
	// It passed, or was dropped (drop).
	done, dropped bool
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
	// Transactions read that a chunk read later must see (txn.ahead,
	// txn.held), and no snapshot is known to see yet: the log can read a
	// transaction's commit before its backend leaves the proc array, and a
	// snapshot taken between doesn't see it.
	unseen []*txn
}

// readCDC reads the streams through the pipeline's replication slot: each
// table's changes, and until it has been read, its snapshot, in chunks
// between them, a chunk of a key range per connection the read can hold. A
// chunk is read in a transaction of its own; it's held until the log has
// passed every transaction its snapshot saw.
//
// A row's history starts at the chunk that reads it. Until a chunk has read a
// key, the log's changes to it aren't emitted: the chunk carries the row as
// it then is. A change to a held chunk's key that its snapshot saw is in the
// chunk's row; one it didn't see is emitted after that row. Every change
// after that comes from the log. Nothing is written to the source but the
// slot and, if missing, the publication.
func (s *Source) readCDC(ctx context.Context, conns *connPool, req source.ReadRequest, cfgs []source.ConfiguredStream, tables []table, out source.Emitter) error {
	for _, cs := range cfgs {
		t := tables[slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })]
		if err := undeclared(t, cs.Columns); err != nil {
			return err
		}
		if err := retyped(t, cs.Schema); err != nil {
			return err
		}
	}
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
		if !req.Continuous {
			if err := st.stopAt(r.slot, cs.Until, cs.CompleteAt); err != nil {
				return err
			}
		}
		st.b = newChangeBatcher(memory.DefaultAllocator, t.cols)
		for _, c := range t.cols {
			st.conv = append(st.conv, textConv(c.oid))
		}
		st.ints = !slices.ContainsFunc(t.key, func(k int) bool { return !intKey[t.cols[k].oid] })
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
	if err := r.startAt(confirmed); err != nil {
		return err
	}
	if !r.continuous {
		if r.target, err = r.end(ctx); err != nil {
			return err
		}
	}

	rc := s.conn.Config.Copy()
	rc.RuntimeParams["replication"] = "database"
	if r.repl, err = pgconn.ConnectConfig(ctx, rc); err != nil {
		return classify(fmt.Errorf("postgres: replication connection: %w", err))
	}
	defer func() { _ = r.repl.Close(context.WithoutCancel(ctx)) }()
	if beforeStartReplication != nil {
		beforeStartReplication()
	}
	err = pglogrepl.StartReplication(ctx, r.repl, r.slot, r.pos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names " + quoteLiteral(pgx.Identifier{r.pub}.Sanitize())},
	})
	if err != nil {
		if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "55006" {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: replication slot %q: %w", r.slot, err))
		}
		return classify(fmt.Errorf("postgres: start replication: %w", err))
	}
	unset := s.setReader(r)
	defer unset()
	// The slot may have been advanced since ensureSlot, by Committed without
	// a read, or by another process; it streams from where it was advanced
	// to. Now that the read holds it, only the read's own status moves it.
	if confirmed, err = r.slotConfirmed(ctx); err != nil {
		return err
	}
	if err := r.startAt(confirmed); err != nil {
		return err
	}
	return r.run(ctx)
}

// startAt checks that the slot, confirmed up to confirmed, can stream every
// stream's changes, and sets where the read starts: the earliest stream's
// position, or the slot's if it's later or a stream has none.
func (r *cdcReader) startAt(confirmed pglogrepl.LSN) error {
	// The slot can't stream the log before its confirmed position, so a read
	// from before it would skip changes: a stream acknowledged past while it
	// wasn't read, or a redo from a position acknowledged since.
	for _, st := range r.streams {
		if st.from != 0 && st.from < confirmed {
			return positionLost("postgres: %s resumes from %s, and replication slot %q was acknowledged up to %s and streams nothing before it, so the changes between can't be read again; its table needs a snapshot again", st.t.name, st.from, r.slot, confirmed)
		}
	}
	start := r.streams[0].from
	for _, st := range r.streams {
		start = min(start, st.from)
	}
	start = max(start, confirmed)
	r.confirmed, r.pos = confirmed, start
	for _, st := range r.streams {
		st.resumed = max(st.from, start)
	}
	return nil
}

// slotConfirmed is the slot's confirmed position.
func (r *cdcReader) slotConfirmed(ctx context.Context) (pglogrepl.LSN, error) {
	var at string
	if err := r.conn.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", r.slot).Scan(&at); err != nil {
		return 0, classify(fmt.Errorf("postgres: replication slot %q: %w", r.slot, err))
	}
	return pglogrepl.ParseLSN(at)
}

// stopAt sets where a bounded read of the stream stops: its until, or its
// complete_at, which is never set with it. Either is a state this source
// sent, whose position says which transactions the read that sent it had
// read: those that committed before it, in slot's log; one in another's fails
// with POSITION_LOST. A stream with a snapshot left to read ignores it, as a
// chunk read again isn't the chunk read before.
func (st *cdcStream) stopAt(slot string, until, completeAt []byte) error {
	state := until
	if state == nil {
		state = completeAt
	}
	if state == nil || len(st.ranges) > 0 {
		return nil
	}
	var at cdcState
	if err := json.Unmarshal(state, &at); err != nil {
		return fmt.Errorf("postgres: where %s stops: %w", st.t.name, err)
	}
	if at.Snapshot != nil || at.LSN == 0 {
		return nil
	}
	if at.Slot != slot {
		return positionLost("postgres: %s is to stop at %s in replication slot %q, and the source reads %q", st.t.name, pglogrepl.LSN(at.LSN), at.Slot, slot)
	}
	st.stop, st.stopState, st.until = pglogrepl.LSN(at.LSN), state, until != nil
	return nil
}

// end is where a bounded read ends: where the log is now, or where its
// streams stop if all of them do.
func (r *cdcReader) end(ctx context.Context) (pglogrepl.LSN, error) {
	var end pglogrepl.LSN
	stops := true
	for _, st := range r.streams {
		end = max(end, st.stop)
		stops = stops && st.stop != 0
	}
	if stops {
		return end, nil
	}
	var at string
	if err := r.conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&at); err != nil {
		return 0, classify(err)
	}
	now, err := pglogrepl.ParseLSN(at)
	return max(now, end), err
}

// fresh says a state's snapshot hasn't started.
func fresh(state cdcState) bool {
	c := state.Snapshot
	return state.Slot == "" || c != nil && c.After == nil && len(c.Ranges) == 0
}

// snapshotRanges is what's left of a table's snapshot, from its state. One
// not started is read in key ranges if several connections can read it.
func (r *cdcReader) snapshotRanges(ctx context.Context, t table, state cdcState) ([]*snapshotRange, error) {
	cursor := state.Snapshot
	switch {
	case fresh(state):
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
	giveUp, wait := time.Now().Add(slotHeldFor), slotPollFirst
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
		if time.Now().After(giveUp) {
			return 0, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: replication slot %q is held by PID %v", r.slot, deref(pid)))
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		wait = min(2*wait, slotPollMax)
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
		pe, ok := errors.AsType[*pgconn.PgError](err)
		if ok && pe.Code == "55000" {
			return 0, configInvalid("postgres: %s (CDC needs wal_level = logical)", pe.Message)
		}
		if ok && pe.Code == "53400" {
			return 0, classify(fmt.Errorf("postgres: create replication slot %q (drop the slots no pipeline uses, or raise max_replication_slots): %w", r.slot, err))
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
			if err := r.stopped(); err != nil {
				return err
			}
			if err := r.resume(ctx); err != nil {
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
					if err := r.passed(ctx, r.pos); err != nil {
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
		// A transaction whose commit is at or after a chunk's position isn't
		// one its snapshot saw, and the log has read every one before it, so
		// the chunk passes before its changes, which it doesn't hold then.
		if err := r.passed(ctx, m.FinalLSN); err != nil {
			return err
		}
		r.txn = &txn{xid: m.Xid, commit: m.FinalLSN, at: m.CommitTime}
	case *pglogrepl.CommitMessage:
		if r.txn == nil {
			return errors.New("postgres: replication: a commit without a begin")
		}
		r.lastCommit = r.txn.at
		r.txn = nil
		r.pos = m.TransactionEndLSN
		return r.passed(ctx, r.pos)
	case *pglogrepl.InsertMessage:
		return r.change(ctx, m.RelationID, source.OpInsert, 0, nil, m.Tuple)
	case *pglogrepl.UpdateMessage:
		return r.change(ctx, m.RelationID, source.OpUpdate, m.OldTupleType, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessage:
		return r.change(ctx, m.RelationID, source.OpDelete, m.OldTupleType, m.OldTuple, nil)
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
		if err := r.sameType(ctx, st.t, st.t.cols[j], m.RelationID, c); err != nil {
			return err
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

// sameType fails, retryably, where the log describes t's column sc with a
// type declared otherwise than sc's, as an ALTER COLUMN ... TYPE since the
// read started leaves it: its values would land as sc's type. The next
// attempt declares the new type. The log describes the changes before such
// an ALTER with the old type, which reads on once the table's column has
// the declared type: the declared type takes the old one's values.
func (r *cdcReader) sameType(ctx context.Context, t table, sc sourceColumn, relid uint32, c *pglogrepl.RelationMessageColumn) error {
	if c.DataType == sc.oid && c.TypeModifier == sc.typmod {
		return nil
	}
	declared := declaredType(sc)
	if arrow.TypeEqual(declaredType(sourceColumn{oid: c.DataType, typmod: c.TypeModifier, dims: sc.dims}), declared) {
		return nil
	}
	now := sourceColumn{dims: sc.dims}
	err := r.conn.QueryRow(ctx, "SELECT atttypid, atttypmod FROM pg_attribute WHERE attrelid = $1 AND attname = $2 AND NOT attisdropped", relid, sc.name).Scan(&now.oid, &now.typmod)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return classify(err)
	}
	if err == nil && arrow.TypeEqual(declaredType(now), declared) {
		return nil
	}
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: %s's column %q changed type; the read starts again to declare it", t.name, sc.name))
}

func (r *cdcReader) stream(name string) *cdcStream {
	for _, st := range r.streams {
		if st.t.name == name {
			return st
		}
	}
	return nil
}

// change reads one change: a key change as a delete of the old key and the
// new row, each placed by its own key.
func (r *cdcReader) change(ctx context.Context, id uint32, op string, oldType uint8, old, new *pglogrepl.TupleData) error {
	rel := r.rels[id]
	if rel == nil {
		return fmt.Errorf("postgres: replication: a change to relation %d, which the log hasn't described", id)
	}
	st := rel.stream
	if st == nil || r.txn == nil || r.txn.commit < st.from || st.done || st.stop != 0 && r.txn.commit >= st.stop {
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
		return r.read(ctx, st, oldKey, op, vals, nil)
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
		if err := r.read(ctx, st, oldKey, source.OpDelete, vals, nil); err != nil {
			return err
		}
	}
	vals, unchanged := rel.values(new, full)
	return r.read(ctx, st, key, op, vals, unchanged)
}

var errKeyUnchanged = errors.New("postgres: a primary key value wasn't sent")

// keyOf spells a tuple's key as text, as readChunk's queries do.
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

// read takes a change of the transaction being read. Once the stream's
// snapshot is read, it's emitted. Until then, it waits to be placed against
// the snapshot (settle): at once where Go compares the stream's keys, and
// otherwise with the stream's next changes.
func (r *cdcReader) read(ctx context.Context, st *cdcStream, key, op string, vals [][]byte, unchanged []string) error {
	r.txn.n++
	ch := version{op: op, seq: seqOf(r.txn.commit, r.txn.n), vals: vals, unchanged: unchanged}
	if len(st.ranges) == 0 {
		return r.add(st, ch)
	}
	for _, v := range vals {
		st.pendingBytes += len(v)
	}
	st.pending = append(st.pending, &logChange{version: ch, x: r.txn, key: key})
	if st.ints || len(st.pending) >= placeEvery || st.pendingBytes >= r.s.cfg.BatchBytes {
		return r.settle(ctx, st)
	}
	return nil
}

// settle places the stream's pending changes against its snapshot, and
// applies each in the log's order. It runs before anything moves the
// snapshot on, a chunk read or passed, and before a resume point.
func (r *cdcReader) settle(ctx context.Context, st *cdcStream) error {
	if len(st.pending) == 0 {
		return nil
	}
	keys := make([]string, len(st.pending))
	for i, ch := range st.pending {
		keys[i] = ch.key
	}
	at, err := r.place(ctx, st, keys)
	if err != nil {
		return err
	}
	for i, ch := range st.pending {
		if err := r.apply(st, ch, at[i]); err != nil {
			return err
		}
	}
	clear(st.pending)
	st.pending, st.pendingBytes = st.pending[:0], 0
	return nil
}

// apply emits a change to a key the snapshot has read, and leaves one to a
// key it hasn't read to the chunk that will. A change to a held chunk's key
// is in the chunk's row if the chunk's snapshot saw it; if not, the chunk
// emits its row of the key, if it has one, then the change.
func (r *cdcReader) apply(st *cdcStream, ch *logChange, at placed) error {
	c := at.chunk
	if c != nil && c.dropped {
		// Placed before the chunk was dropped: no chunk has read it now.
		c = nil
	}
	switch {
	case at.behind:
		return r.add(st, ch.version)
	case c == nil:
		r.owe(ch.x, st, nil)
	case c.snap.sees(ch.x.xid):
		ch.x.seen = true
	default:
		if vals, ok := c.rows[ch.key]; ok {
			delete(c.rows, ch.key)
			c.after = append(c.after, version{op: source.OpInsert, seq: seqOf(ch.x.commit, 0), vals: vals})
		}
		c.after = append(c.after, ch.version)
		for _, v := range ch.vals {
			c.afterBytes += len(v)
		}
		r.owe(ch.x, nil, c)
		if c.afterBytes > max(r.s.cfg.BatchBytes, c.bytes) {
			r.drop(c)
		}
	}
	return nil
}

// owe has a chunk read later see x: one of st, or, after a restart while c
// is held, the one that reads c's keys again.
func (r *cdcReader) owe(x *txn, st *cdcStream, c *chunk) {
	if x.seen {
		return
	}
	if st != nil && !slices.Contains(x.ahead, st) {
		x.ahead = append(x.ahead, st)
	}
	if c != nil && !slices.Contains(x.held, c) {
		x.held = append(x.held, c)
	}
	if !x.listed {
		x.listed = true
		r.unseen = append(r.unseen, x)
	}
}

// owed says a chunk read later must see x: of st, or of any stream if st is
// nil. A held chunk that passed leaves its keys read, so a read resumed after
// it reads them from the log; one that was dropped left them to its stream.
func owed(x *txn, st *cdcStream) bool {
	return slices.ContainsFunc(x.ahead, func(a *cdcStream) bool { return len(a.ranges) > 0 && (st == nil || a == st) }) ||
		slices.ContainsFunc(x.held, func(c *chunk) bool { return !c.done && (st == nil || c.stream == st) })
}

// drop gives up a held chunk whose changes after its snapshot outgrew what
// it may hold, its rows' bytes or batchBytes: it emits nothing, the next
// round reads its keys again, and that round's chunk must see the
// transactions whose changes it held.
func (r *cdcReader) drop(c *chunk) {
	c.done, c.dropped = true, true
	r.chunks = slices.DeleteFunc(r.chunks, func(h *chunk) bool { return h == c })
	for _, x := range r.unseen {
		if slices.Contains(x.held, c) {
			r.owe(x, c.stream, nil)
		}
	}
	c.rows, c.after = nil, nil
	r.replan()
	slog.Debug("a held chunk of a table's snapshot outgrew what it may hold, and is read again", "table", c.stream.t.name)
}

func (r *cdcReader) add(st *cdcStream, v version) error {
	if err := st.b.add(v.op, v.seq, v.vals, v.unchanged); err != nil {
		return fmt.Errorf("postgres: %s: %w", st.t.name, err)
	}
	st.dirty = true
	if st.b.bytes >= r.s.cfg.BatchBytes {
		return st.b.flush(st.t.name, r.out)
	}
	return nil
}

// passed runs once the log has read every transaction that committed
// before at: each chunk whose position is at or before it is emitted, its
// rows and then what it holds after them.
func (r *cdcReader) passed(ctx context.Context, at pglogrepl.LSN) error {
	var moving []*cdcStream
	for _, c := range r.chunks {
		if c.lsn <= at && !slices.Contains(moving, c.stream) {
			moving = append(moving, c.stream)
		}
	}
	if len(moving) == 0 {
		return nil
	}
	// Changes are placed against the chunks held while they were read, which
	// can drop a chunk.
	for _, st := range moving {
		if err := r.settle(ctx, st); err != nil {
			return err
		}
	}
	var held []*chunk
	var moved []*cdcStream
	for _, c := range r.chunks {
		if c.lsn > at {
			held = append(held, c)
			continue
		}
		st := c.stream
		if !slices.Contains(moved, st) {
			moved = append(moved, st)
		}
		seq := seqOf(c.lsn, 0)
		for _, k := range c.keys {
			if vals, ok := c.rows[k]; ok {
				if err := r.add(st, version{op: source.OpInsert, seq: seq, vals: vals}); err != nil {
					return err
				}
			}
		}
		for _, v := range c.after {
			if err := r.add(st, v); err != nil {
				return err
			}
		}
		c.done = true
		switch {
		case c.full:
			c.rng.After = c.last
		default:
			st.ranges = slices.DeleteFunc(st.ranges, func(rng *snapshotRange) bool { return rng == c.rng })
			if len(st.ranges) == 0 {
				slog.Info("read a table's snapshot", "table", st.t.name)
			}
		}
	}
	r.chunks = held
	r.replan()
	for _, st := range moved {
		if err := r.resumeStream(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

// replan drops the streams' plans, once chunks are read, passed or dropped.
func (r *cdcReader) replan() {
	for _, st := range r.streams {
		st.plan = nil
	}
}

// visible waits until new snapshots see every transaction that a chunk read
// later must see (owed): for st, or any stream if st is nil. Before a round
// of chunks, that's what they read; before st's resume point, what a read
// resumed there reads, which doesn't read these transactions from the log.
func (r *cdcReader) visible(ctx context.Context, st *cdcStream) error {
	waits := func() *txn {
		// A transaction owed nothing now can be owed again, as its changes
		// to another stream are placed: owe lists it again.
		r.unseen = slices.DeleteFunc(r.unseen, func(x *txn) bool {
			if x.seen || !owed(x, nil) {
				x.listed = false
				return true
			}
			return false
		})
		for _, x := range r.unseen {
			if owed(x, st) {
				return x
			}
		}
		return nil
	}
	giveUp, wait := time.Now().Add(visibleWithin), time.Millisecond
	for waits() != nil {
		snap, err := r.snapshot(ctx)
		if err != nil {
			return err
		}
		for _, x := range r.unseen {
			x.seen = x.seen || snap.sees(x.xid)
		}
		x := waits()
		if x == nil {
			return nil
		}
		if time.Now().After(giveUp) {
			return r.unseenFor(ctx, x)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		wait = min(2*wait, 100*time.Millisecond)
	}
	return nil
}

// unseenFor is the error for a transaction the log read that new snapshots
// still don't see after visibleWithin. Its backend waits for synchronous
// standbys to confirm it; if the read's own replication connection is one
// of them, it confirms only what the pipeline has checkpointed, which can't
// pass the transaction while the read waits for it, so retrying is no use.
func (r *cdcReader) unseenFor(ctx context.Context, x *txn) error {
	var state, app string
	if r.repl != nil {
		err := r.conn.QueryRow(ctx, "SELECT sync_state, application_name FROM pg_stat_replication WHERE pid = $1", r.repl.PID()).Scan(&state, &app)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return classify(err)
		}
	}
	return unseenError(x.xid, state, app)
}

// unseenError is unseenFor's error for transaction xid, given the read's
// walsender's pg_stat_replication.sync_state and application_name, empty if
// it has none.
func unseenError(xid uint32, state, app string) error {
	switch state {
	case "sync":
		return configInvalid("postgres: transaction %d, which the log has read, isn't visible to new snapshots after %v: it waits for synchronous standbys, and synchronous_standby_names makes one of them this read's replication connection (application_name %q), which confirms only what the pipeline has checkpointed, and the pipeline can't checkpoint past the transaction until new snapshots see it; leave the connection out of synchronous_standby_names, or give it an application_name of its own in the source's dsn", xid, visibleWithin, app)
	case "quorum":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: transaction %d, which the log has read, isn't visible to new snapshots after %v, as while synchronous standbys haven't confirmed it; synchronous_standby_names counts this read's replication connection (application_name %q) among them, which confirms only what the pipeline has checkpointed; the read starts again", xid, visibleWithin, app))
	}
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: transaction %d, which the log has read, isn't visible to new snapshots after %v, as while a synchronous standby hasn't confirmed it; the read starts again", xid, visibleWithin))
}

// snapshot is a new snapshot of which transactions are visible.
func (r *cdcReader) snapshot(ctx context.Context) (pgSnapshot, error) {
	var text string
	if id := imported(); id != "" {
		tx, err := r.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return pgSnapshot{}, classify(err)
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(id)); err != nil {
			return pgSnapshot{}, classify(err)
		}
		if err := tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
			return pgSnapshot{}, classify(err)
		}
	} else if err := r.conn.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&text); err != nil {
		return pgSnapshot{}, classify(err)
	}
	return parseSnapshot(text)
}

// imported is the snapshot snapshotFrom gives, if any.
func imported() string {
	if snapshotFrom == nil {
		return ""
	}
	return snapshotFrom()
}

// placed is where a key lies against its stream's snapshot.
type placed struct {
	// The snapshot has read it: its range is read, or it's at or before its
	// range's position.
	behind bool
	// Otherwise, the held chunk of its range whose keys it's among: after
	// the range's position, up to the chunk's last key, or anywhere in the
	// range if the chunk reads the rest of it. Nil: no chunk has read it.
	chunk *chunk
}

// plan is a stream's snapshot as place compares keys with it.
type plan struct{ ranges []planRange }

type planRange struct {
	// Its bounds, of the key's first column, and its position; nil: none.
	lo, hi, after *bound
	// Its held chunk, and that chunk's last key if more of the range follows
	// it; nil: none.
	chunk *chunk
	last  *bound
}

// bound is a key, or a value of its first column alone, spelled as text,
// with its integers if the key's columns are integers.
type bound struct {
	text []string
	ints []int64
}

// place says where each of st's keys, spelled as keyOf spells them, lies
// against its snapshot, in the order readChunk's queries read the key in:
// its columns' types and collations. Go compares keys whose columns are all
// integers; the server orders the others, in one query.
func (r *cdcReader) place(ctx context.Context, st *cdcStream, keys []string) ([]placed, error) {
	p, err := r.plan(st)
	if err != nil {
		return nil, err
	}
	var le func(i int, b *bound) bool
	if st.ints {
		le, err = intOrder(st, keys)
	} else {
		le, err = r.serverOrder(ctx, st, p, keys)
	}
	if err != nil {
		return nil, err
	}
	out := make([]placed, len(keys))
	for i := range keys {
		out[i] = p.place(i, le)
	}
	return out, nil
}

// place says where key i lies, le saying whether it's at or before a bound:
// a bound of the first column alone is after every key with that value.
func (p *plan) place(i int, le func(int, *bound) bool) placed {
	for _, pr := range p.ranges {
		if pr.lo != nil && le(i, pr.lo) || pr.hi != nil && !le(i, pr.hi) {
			continue
		}
		switch {
		case pr.after != nil && le(i, pr.after):
			return placed{behind: true}
		case pr.chunk == nil || pr.last != nil && !le(i, pr.last):
			return placed{}
		}
		return placed{chunk: pr.chunk}
	}
	// The ranges together held every key, and those read are gone.
	return placed{behind: true}
}

func (r *cdcReader) plan(st *cdcStream) (*plan, error) {
	if st.plan != nil {
		return st.plan, nil
	}
	var bad error
	bnd := func(text ...string) *bound {
		b := &bound{text: text}
		for _, v := range text {
			if !st.ints {
				break
			}
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				bad = fmt.Errorf("postgres: %s's snapshot has the key %q: %w", st.t.name, text, err)
			}
			b.ints = append(b.ints, n)
		}
		return b
	}
	p := &plan{}
	for _, rng := range st.ranges {
		var pr planRange
		if rng.Lo != nil {
			pr.lo = bnd(*rng.Lo)
		}
		if rng.Hi != nil {
			pr.hi = bnd(*rng.Hi)
		}
		if rng.After != nil {
			pr.after = bnd(rng.After...)
		}
		for _, c := range r.chunks {
			if c.rng == rng {
				pr.chunk = c
			}
		}
		if pr.chunk != nil && pr.chunk.full {
			pr.last = bnd(pr.chunk.last...)
		}
		p.ranges = append(p.ranges, pr)
	}
	if bad != nil {
		return nil, bad
	}
	st.plan = p
	return p, nil
}

// intOrder compares keys of integers with bounds as numbers, column by
// column.
func intOrder(st *cdcStream, keys []string) (func(int, *bound) bool, error) {
	ks := make([][]int64, len(keys))
	for i, k := range keys {
		for v := range strings.SplitSeq(k, "\x00") {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("postgres: %s's log has the key %q: %w", st.t.name, strings.Split(k, "\x00"), err)
			}
			ks[i] = append(ks[i], n)
		}
	}
	return func(i int, b *bound) bool {
		for j, v := range b.ints {
			if ks[i][j] != v {
				return ks[i][j] < v
			}
		}
		return true
	}, nil
}

// serverOrder has the server sort the keys with the plan's bounds, each
// column's text cast to its type with its collation: a bound sorts after a
// key equal to it, and a bound of the first column alone after every key
// with that value.
func (r *cdcReader) serverOrder(ctx context.Context, st *cdcStream, p *plan, keys []string) (func(int, *bound) bool, error) {
	if err := r.keyTypes(ctx, st); err != nil {
		return nil, err
	}
	n := len(st.t.key)
	cols := make([][]*string, n)
	add := func(vals []string) int {
		for j := range cols {
			var v *string
			if j < len(vals) {
				v = &vals[j]
			}
			cols[j] = append(cols[j], v)
		}
		return len(cols[0]) - 1
	}
	// Keys first, so a bound equal to one sorts after it.
	of := make([]int, len(keys))
	added := map[string]int{}
	for i, k := range keys {
		j, ok := added[k]
		if !ok {
			j = add(strings.Split(k, "\x00"))
			added[k] = j
		}
		of[i] = j
	}
	at := map[*bound]int{}
	for _, pr := range p.ranges {
		for _, b := range []*bound{pr.lo, pr.hi, pr.after, pr.last} {
			if b != nil {
				at[b] = add(b.text)
			}
		}
	}
	args := make([]any, n)
	in := make([]string, n)
	names := make([]string, n)
	by := make([]string, n)
	for j := range n {
		args[j] = cols[j]
		in[j] = "$" + strconv.Itoa(j+1) + "::text[]"
		names[j] = "c" + strconv.Itoa(j)
		by[j] = "(u.c" + strconv.Itoa(j) + "::" + st.types[j] + ")"
		if st.collations[j] != "" {
			by[j] += " COLLATE " + st.collations[j]
		}
		by[j] += " NULLS LAST"
	}
	q := "SELECT u.o FROM unnest(" + strings.Join(in, ", ") + ") WITH ORDINALITY AS u(" + strings.Join(names, ", ") + ", o) ORDER BY " + strings.Join(by, ", ") + ", u.o"
	rows, err := r.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, classify(fmt.Errorf("postgres: place %s's changes against its snapshot: %w", st.t.name, err))
	}
	order, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, classify(fmt.Errorf("postgres: place %s's changes against its snapshot: %w", st.t.name, err))
	}
	rank := make([]int, len(cols[0]))
	for i, o := range order {
		rank[o-1] = i
	}
	return func(i int, b *bound) bool { return rank[of[i]] < rank[at[b]] }, nil
}

// keyTypes sets st's key columns' types and collations, as SQL names them,
// the types without a modifier, as a parameter compared with the column
// takes it.
func (r *cdcReader) keyTypes(ctx context.Context, st *cdcStream) error {
	if st.types != nil {
		return nil
	}
	names := make([]string, len(st.t.key))
	for i, k := range st.t.key {
		names[i] = st.t.cols[k].name
	}
	rows, err := r.conn.Query(ctx, `SELECT quote_ident(tn.nspname) || '.' || quote_ident(ty.typname), coalesce(quote_ident(cn.nspname) || '.' || quote_ident(co.collname), '')
		FROM unnest($3::text[]) WITH ORDINALITY AS k(name, ord)
		JOIN pg_namespace n ON n.nspname = $1
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = $2
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = k.name AND NOT a.attisdropped
		JOIN pg_type ty ON ty.oid = a.atttypid
		JOIN pg_namespace tn ON tn.oid = ty.typnamespace
		LEFT JOIN pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
		ORDER BY k.ord`, r.s.cfg.Schema, st.t.name, names)
	if err != nil {
		return classify(err)
	}
	var types, collations []string
	for rows.Next() {
		var typ, coll string
		if err := rows.Scan(&typ, &coll); err != nil {
			rows.Close()
			return classify(err)
		}
		types, collations = append(types, typ), append(collations, coll)
	}
	if err := rows.Err(); err != nil {
		return classify(err)
	}
	if len(types) != len(names) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: %s's key columns %q aren't all there; the read starts again", st.t.name, names))
	}
	st.types, st.collations = types, collations
	return nil
}

// snapshotsRead says no stream has a range of its snapshot left to read.
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
		r.unseen = nil
		// The log can go on for as long as it likes, on one connection.
		r.conns.closeIdle(ctx)
		return nil
	}
	for _, st := range r.streams {
		if err := r.settle(ctx, st); err != nil {
			return err
		}
	}
	// So each chunk's snapshot sees every change the log has read to a key no
	// chunk had read.
	if err := r.visible(ctx, nil); err != nil {
		return err
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
	r.chunks = chunks
	r.replan()
	return r.passed(ctx, r.pos)
}

// readChunk reads a key range's next chunk, on a connection only it is using.
func (r *cdcReader) readChunk(ctx context.Context, conn *pgx.Conn, st *cdcStream, rng *snapshotRange) (*chunk, error) {
	t := st.t
	cols := make([]string, len(t.cols))
	for i, c := range t.cols {
		cols[i] = pgx.Identifier{c.name}.Sanitize()
	}
	key := make([]string, len(t.key))
	// The key again, unconverted and in text, which is how the log spells it
	// for keyOf. ::text isn't, for char(n), boolean and inet.
	keyText := make([]string, len(t.key))
	for i, k := range t.key {
		key[i] = cols[k]
		keyText[i] = cols[k] + " AS _openetl_key_" + strconv.Itoa(i)
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
	// A row past the chunk says the range goes on; without it, the range is
	// read once the chunk is.
	q += " ORDER BY " + strings.Join(key, ", ") + " LIMIT " + strconv.Itoa(min(r.s.cfg.SnapshotChunkRows, math.MaxInt-1)+1)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if beforeChunkSnapshot != nil {
		beforeChunkSnapshot()
	}
	if id := imported(); id != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(id)); err != nil {
			return nil, classify(err)
		}
	}
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
		if len(c.keys) == r.s.cfg.SnapshotChunkRows {
			c.full = true
			continue
		}
		vals := rr.Values()
		row := make([][]byte, n)
		for i, v := range vals[:n] {
			if v != nil {
				row[i] = convert(st.conv[i], append([]byte{}, v...))
				c.bytes += len(v)
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
	if err := tx.Commit(ctx); err != nil {
		return nil, classify(err)
	}
	return c, nil
}

// resume flushes and emits resume points: for streams that emitted or read
// changes since their last after resumeEvery, and for the rest after
// quietEvery if the log has moved on.
func (r *cdcReader) resume(ctx context.Context) error {
	now := time.Now()
	for _, st := range r.streams {
		if st.done {
			continue
		}
		if (st.dirty || len(st.pending) > 0) && now.Sub(st.resumedAt) >= resumeEvery || r.pos > st.resumed && now.Sub(st.resumedAt) >= quietEvery {
			if err := r.resumeStream(ctx, st); err != nil {
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

// resumeStream emits the stream's resume point where the read is, once its
// changes are placed and new snapshots see what a read resumed there must.
func (r *cdcReader) resumeStream(ctx context.Context, st *cdcStream) error {
	if err := r.settle(ctx, st); err != nil {
		return err
	}
	if err := r.visible(ctx, st); err != nil {
		return err
	}
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

// stopped completes, between transactions, the streams whose read has
// reached where it stops, with the state it was given.
func (r *cdcReader) stopped() error {
	for _, st := range r.streams {
		if st.stop == 0 || st.done || r.pos < st.stop {
			continue
		}
		if err := st.b.flush(st.t.name, r.out); err != nil {
			return err
		}
		if st.until {
			if err := r.out.ResumePoint(st.t.name, st.stopState); err != nil {
				return err
			}
		}
		if err := r.out.Complete(st.t.name, st.stopState); err != nil {
			return err
		}
		st.done = true
	}
	return nil
}

// complete ends a bounded read: each stream's next sync starts where this
// one ended.
func (r *cdcReader) complete() error {
	for _, st := range r.streams {
		if st.done {
			continue
		}
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

// setReader makes r the read Committed acknowledges on, with what the read
// it replaces was acknowledged if that read the same slot, and returns what
// unsets it. That unsets r alone, so a read that ends after the next one
// started leaves the next one's in place.
func (s *Source) setReader(r *cdcReader) (unset func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.reader; old != nil && old.slot == r.slot {
		r.ack(pglogrepl.LSN(old.acked.Load()))
	}
	s.reader = r
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.reader == r {
			s.reader = nil
		}
	}
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
	// Under the lock, so a read that replaces this one takes the
	// acknowledgement with it.
	s.mu.Lock()
	r := s.reader
	if r != nil && r.slot == slot {
		r.ack(low)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
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
		tag, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slot)
		pe, ok := errors.AsType[*pgconn.PgError](err)
		if !ok || pe.Code != "55006" || attempt >= 20 {
			// No row: there was no slot to drop.
			if err == nil && tag.RowsAffected() > 0 {
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
// spelling: booleans, bytea's hex as base64, and char(n) without the spaces
// that pad it.
func textConv(oid uint32) func([]byte) []byte {
	switch oid {
	case pgtype.BPCharOID:
		return unpad
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

// changeBatcher builds a CDC stream's batches: each column as text, which
// the engine parses as the declared type, or built from its text where the
// engine can't (changeTyped), then the control columns.
type changeBatcher struct {
	b     *array.RecordBuilder
	typed []*textTyped
	n     int
	rows  int
	bytes int
}

func newChangeBatcher(mem memory.Allocator, cols []sourceColumn) *changeBatcher {
	fields := make([]arrow.Field, 0, len(cols)+3)
	typed := make([]*textTyped, len(cols))
	for i, c := range cols {
		f := arrow.Field{Name: c.name, Type: arrow.BinaryTypes.String, Nullable: true}
		if typed[i] = changeTyped(c); typed[i] != nil {
			f.Type = typed[i].dt
		}
		fields = append(fields, f)
	}
	fields = append(fields, source.OpField(), source.SeqField(seqType), source.UnchangedField())
	return &changeBatcher{b: array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil)), typed: typed, n: len(cols)}
}

// add appends a row; a nil value is null.
func (c *changeBatcher) add(op string, seq decimal128.Num, vals [][]byte, unchanged []string) error {
	for i := range c.n {
		fb := c.b.Field(i)
		if vals[i] == nil {
			fb.AppendNull()
			continue
		}
		if t := c.typed[i]; t != nil {
			if err := t.add(fb, vals[i]); err != nil {
				return fmt.Errorf("column %q: %w", c.b.Schema().Field(i).Name, err)
			}
		} else {
			fb.(*array.StringBuilder).BinaryBuilder.Append(vals[i])
		}
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
	return nil
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
