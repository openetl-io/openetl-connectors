package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/source"
)

const (
	pollEvery     = time.Second
	resumeEvery   = time.Second
	quietEvery    = 10 * time.Second
	progressEvery = 10 * time.Second
	// How long redo can stay in the log buffer before LGWR writes it, at
	// most: a window mines again what was this new when the last one
	// started, so a change written late isn't passed.
	flushLag = 5 * time.Second
	// Of a fresh snapshot's SCN, while the table's definition is newer.
	settleTries = 30
)

type cdcState struct {
	// Every transaction that committed at or before SCN is sent; a
	// snapshot's rows are as of SCN.
	SCN uint64 `json:"scn"`
	// Mining starts again here: at or before the first change to the
	// stream's table of every transaction open at SCN.
	From uint64 `json:"from"`
	// Until the table's snapshot has been read: where it continues.
	Snapshot *snapshotCursor `json:"snapshot,omitempty"`
	// The database's DBID and RESETLOGS SCN: an SCN means nothing in another
	// incarnation.
	DB string `json:"db"`
	// When SCN's transaction committed, or its redo log began: how far
	// behind the read is, in time.
	At *time.Time `json:"at,omitempty"`
}

type snapshotCursor struct {
	// The key of the last row read, as keyText spells it; empty before the
	// first.
	After []string `json:"after,omitempty"`
}

type cdcStream struct {
	t     table
	state cdcState
	b     *batcher
	// All the columns the source reads, declared or not, and their indexes
	// by SEGMENT_COLUMN_ID.
	all   []column
	bySeg map[int]int
	// Every SEGMENT_COLUMN_ID the table has, hidden and unread columns'
	// too.
	segs map[int]bool
	lobs bool
	// A read that stops at a resume point.
	until    *cdcState
	untilRaw []byte
	finished bool

	dirty bool
	// The SCN of its last resume point's state, and when it was sent.
	resumed   uint64
	resumedAt time.Time
	// The commit of the last change sent, and when it was.
	last       uint64
	lastAt     time.Time
	unreadable map[string]bool
}

// txn is an open transaction's changes to the tables read, in redo order.
type txn struct {
	first  uint64
	events []redoEvent
	seen   map[string]bool
}

type redoEvent struct {
	scn      uint64
	table    string
	rowID    string
	redo     string
	mismatch bool
	// The table's object version the change was made in.
	objv uint64
}

// logRow is a row of V$LOGMNR_CONTENTS.
type logRow struct {
	scn                uint64
	usn, slt, sqn      uint64
	op                 int
	rollback           bool
	rsID, rowID, table string
	ssn                uint64
	info, redo         string
	at                 time.Time
	objv               uint64
}

// lookahead asks for the window's later changes of a table's definition,
// before a change made in an earlier one can be read.
type lookahead struct {
	table string
	scn   uint64
}

func (l *lookahead) Error() string { return "oracle: look ahead at " + l.table + "'s definition" }

func (x logRow) xid() string { return fmt.Sprintf("%d.%d.%d", x.usn, x.slt, x.sqn) }

type cdcReader struct {
	ctx        context.Context
	s          *Source
	out        source.Emitter
	conn       *sql.Conn
	schema, db string
	streams    map[string]*cdcStream
	order      []*cdcStream
	continuous bool

	txns map[string]*txn
	// Every commit at or before done is handled.
	done uint64
	// Where the next window starts.
	lo      uint64
	samples []scnSample
	// The first SCN of the redo log that changes still in the log buffer
	// come after, and when it began; 0 if not known.
	mark   uint64
	markAt time.Time
	lag    time.Duration
	// A column change of a table read: the read sends what's before it and
	// starts again.
	restartAt uint64
	restart   string
	// The window's changes of tables' definitions, by table, once looked
	// ahead at: whether each drops a column, by the object version it makes.
	ddls map[string]map[uint64]bool
}

type scnSample struct {
	at  time.Time
	scn uint64
}

// readCDC reads the cdc streams: each table's snapshot, as of one SCN by a
// flashback query, then the changes of transactions that committed after
// it, from the redo log through LogMiner.
func (s *Source) readCDC(ctx context.Context, conns *connPool, req source.ReadRequest, streams []source.ConfiguredStream, tables []table, out source.Emitter) error {
	conn, err := conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conns.Put(conn)
	defer endLogMiner(context.WithoutCancel(ctx), conn)
	// The read holds what may be the pool's only connection.
	schema := s.cfg.Schema
	if schema == "" {
		if schema, err = currentSchema(ctx, conn); err != nil {
			return err
		}
	}
	r := &cdcReader{ctx: ctx, s: s, out: out, conn: conn, schema: schema, streams: map[string]*cdcStream{}, continuous: req.Continuous, txns: map[string]*txn{}, done: ^uint64(0)}
	if err := conn.QueryRowContext(ctx, `SELECT dbid || ':' || resetlogs_change# FROM v$database`).Scan(&r.db); err != nil {
		return classify(fmt.Errorf("oracle: read the database's identity: %w", err))
	}
	var fresh []*cdcStream
	for _, cs := range streams {
		i := slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })
		if i < 0 {
			return configInvalid("oracle: there's no table %s.%s", schema, cs.Name)
		}
		t := tables[i]
		if t.noCDC != "" {
			return configInvalid("oracle: %s can't be read with cdc: %s", t.name, t.noCDC)
		}
		st := &cdcStream{t: t, all: t.cols, bySeg: map[int]int{}}
		for j, c := range t.cols {
			st.bySeg[c.seg] = j
		}
		if st.t, err = declaredOnly(t, cs.Columns); err != nil {
			return err
		}
		// Of the columns sent: a LOB added since Discover isn't.
		st.lobs = slices.ContainsFunc(st.t.cols, column.lob)
		if err := r.supplemental(st.t.name); err != nil {
			return err
		}
		if st.segs, err = r.segments(st.t.name); err != nil {
			return err
		}
		switch {
		case cs.State == nil:
			fresh = append(fresh, st)
		default:
			if err := json.Unmarshal(cs.State, &st.state); err != nil {
				return fmt.Errorf("oracle: %s state: %w", t.name, err)
			}
			if st.state.DB != r.db {
				return positionLost("oracle: %s's state is of database %s, and this is %s: read the stream again from a snapshot", t.name, st.state.DB, r.db)
			}
		}
		if cs.Until != nil && !req.Continuous {
			var u cdcState
			if err := json.Unmarshal(cs.Until, &u); err != nil {
				return fmt.Errorf("oracle: %s until: %w", t.name, err)
			}
			st.until, st.untilRaw = &u, cs.Until
		}
		st.b = newBatcher(memory.DefaultAllocator, st.t.cols, true)
		defer st.b.release()
		r.streams[t.name] = st
		r.order = append(r.order, st)
	}
	if err := r.settle(fresh); err != nil {
		return err
	}
	for _, st := range r.order {
		if err := r.snapshot(st); err != nil {
			return err
		}
	}
	return r.mine()
}

// supplemental checks the redo log holds a change's every column, so an
// update or a delete carries the row: supplemental logging of all columns,
// for the table or the database.
func (r *cdcReader) supplemental(name string) error {
	var all string
	err := r.conn.QueryRowContext(r.ctx, `SELECT CASE WHEN d.supplemental_log_data_all = 'YES' OR EXISTS (
	  SELECT 1 FROM all_log_groups g WHERE g.owner = :1 AND g.table_name = :2 AND g.log_group_type = 'ALL COLUMN LOGGING')
	  THEN 'YES' ELSE 'NO' END FROM v$database d`, r.schema, name).Scan(&all)
	switch {
	case err != nil:
		return classify(fmt.Errorf("oracle: read %s's supplemental logging: %w", name, err))
	case all != "YES":
		return configInvalid("oracle: the redo log doesn't hold all of %s's columns, which cdc needs: run ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", name, quoteIdent(r.schema), quoteIdent(name))
	}
	return nil
}

// segments are the SEGMENT_COLUMN_IDs of a table's columns.
func (r *cdcReader) segments(name string) (map[int]bool, error) {
	rows, err := r.conn.QueryContext(r.ctx, `SELECT segment_column_id FROM all_tab_cols
	WHERE owner = :1 AND table_name = :2 AND segment_column_id IS NOT NULL`, r.schema, name)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = rows.Close() }()
	segs := map[int]bool{}
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			return nil, classify(err)
		}
		segs[n] = true
	}
	return segs, classify(rows.Err())
}

// settle starts the fresh streams' snapshots: an SCN they're read as of,
// and where the redo after it starts. From is before the first change of
// every transaction open at the SCN: the lower of the SCN before the open
// transactions are listed, and their first changes.
func (r *cdcReader) settle(fresh []*cdcStream) error {
	if len(fresh) == 0 {
		return nil
	}
	for try := 0; ; try++ {
		before, err := currentSCN(r.ctx, r.conn)
		if err != nil {
			return err
		}
		var open sql.NullInt64
		if err := r.conn.QueryRowContext(r.ctx, `SELECT min(start_scn) FROM v$transaction`).Scan(&open); err != nil {
			return classify(fmt.Errorf("oracle: list the open transactions: %w", err))
		}
		at, err := currentSCN(r.ctx, r.conn)
		if err != nil {
			return err
		}
		from := before
		if open.Valid && uint64(max(open.Int64, 0)) < from {
			from = uint64(open.Int64)
		}
		changed := ""
		for _, st := range fresh {
			_, err := r.conn.ExecContext(r.ctx, `SELECT 1 FROM `+quoteIdent(r.schema)+`.`+quoteIdent(st.t.name)+` AS OF SCN :1 WHERE ROWNUM < 1`, at)
			switch {
			case oraCode(err) == 1466:
				// The table's definition is newer than the SCN.
				changed = st.t.name
			case err != nil:
				return classify(fmt.Errorf("oracle: read %s as of SCN %d: %w", st.t.name, at, err))
			}
		}
		if changed == "" {
			for _, st := range fresh {
				st.state = cdcState{SCN: at, From: from, Snapshot: &snapshotCursor{}, DB: r.db}
			}
			return nil
		}
		if try == settleTries {
			return transient("oracle: %s's definition keeps changing, so it can't be read as of an SCN", changed)
		}
		if err := sleep(r.ctx, time.Second); err != nil {
			return err
		}
	}
}

// snapshot reads the rest of a stream's snapshot, in key order, as of its
// SCN, a chunk at a time, each ending at a resume point; or, for a read
// that stops at a resume point during the snapshot, up to its key.
func (r *cdcReader) snapshot(st *cdcStream) error {
	if st.state.Snapshot == nil {
		return nil
	}
	seq := seqOf(st.state.SCN, 0)
	stop := st.until != nil && st.until.Snapshot != nil
	var upTo func(*where) error
	if stop {
		upTo = func(w *where) error {
			cols := make([]column, len(st.t.key))
			for i, k := range st.t.key {
				cols[i] = st.t.cols[k]
			}
			return w.compare(cols, "<=", st.until.Snapshot.After)
		}
	}
	for {
		last, err := readOrdered(r.ctx, r.conn, r.schema, st.t, st.t.key, st.state.SCN, upTo, st.state.Snapshot.After, r.s.cfg.SnapshotChunkRows, func(vals []any) error {
			if err := st.b.change(vals, source.OpInsert, seq, nil); err != nil {
				return fmt.Errorf("oracle: %s: %w", st.t.name, err)
			}
			if st.b.bytes >= r.s.cfg.BatchBytes {
				return st.flush(r.out)
			}
			return nil
		})
		switch {
		case oraCode(err) == 1555 || oraCode(err) == 8181:
			return positionLost("oracle: %s's snapshot as of SCN %d is older than the undo the database keeps: raise UNDO_RETENTION, or read with more connections, and read the stream again: %w", st.t.name, st.state.SCN, err)
		case oraCode(err) == 1466:
			return positionLost("oracle: %s's definition changed during its snapshot as of SCN %d: read the stream again: %w", st.t.name, st.state.SCN, err)
		case err != nil:
			return classify(err)
		}
		if stop && last == nil {
			return r.finish(st)
		}
		if last == nil {
			st.state.Snapshot = nil
		} else {
			st.state.Snapshot.After = last
		}
		if err := r.resumeStream(st); err != nil {
			return err
		}
		if last == nil {
			return nil
		}
	}
}

func (st *cdcStream) flush(out source.Emitter) error {
	return st.b.flush(func(rec arrow.RecordBatch) error { return out.Records(st.t.name, rec) })
}

// mine reads the redo from the streams' positions, a window at a time up to
// the current SCN, and sends each transaction that changed a table read
// when it commits. A read that isn't continuous ends at the SCN it started
// at.
func (r *cdcReader) mine() error {
	var end uint64
	r.lo = ^uint64(0)
	r.done = ^uint64(0)
	for _, st := range r.order {
		if st.finished {
			continue
		}
		r.lo = min(r.lo, st.state.From)
		r.done = min(r.done, st.state.SCN)
	}
	if r.lo == ^uint64(0) {
		return nil
	}
	if r.lo > r.done+1 {
		r.lo = r.done + 1
	}
	logs, err := redoLogs(r.ctx, r.conn, r.lo)
	if err != nil {
		return err
	}
	if err := covered(logs, r.lo); err != nil {
		return err
	}
	if !r.continuous {
		if end, err = currentSCN(r.ctx, r.conn); err != nil {
			return err
		}
	}
	var progressed time.Time
	for {
		started := time.Now()
		e, err := currentSCN(r.ctx, r.conn)
		if err != nil {
			return err
		}
		r.samples = append(r.samples, scnSample{started, e})
		if e >= r.lo {
			if err := r.window(r.lo, e); err != nil {
				return err
			}
			r.lag = time.Since(started)
		}
		if r.mark, r.markAt, err = logMark(r.ctx, r.conn); err != nil {
			return err
		}
		// The next window starts where this one would have missed changes
		// still in the log buffer.
		for len(r.samples) > 1 && started.Sub(r.samples[1].at) >= flushLag {
			r.samples = r.samples[1:]
		}
		if started.Sub(r.samples[0].at) >= flushLag {
			r.lo = max(r.lo, r.samples[0].scn)
		}
		if r.restart != "" {
			for _, st := range r.order {
				if st.until == nil && !st.finished {
					if err := r.resumeStream(st); err != nil {
						return err
					}
				}
			}
			return transient("oracle: %s; the read starts again to read it with its new columns", r.restart)
		}
		if r.allFinished() {
			return nil
		}
		if !r.continuous && r.done >= end {
			return r.complete()
		}
		if err := r.resume(); err != nil {
			return err
		}
		if time.Since(progressed) >= progressEvery {
			r.progress()
			progressed = time.Now()
		}
		if err := sleep(r.ctx, pollEvery-time.Since(started)); err != nil {
			return err
		}
	}
}

// window mines the redo from SCN lo to SCN e.
func (r *cdcReader) window(lo, e uint64) error {
	r.ddls = map[string]map[uint64]bool{}
	for {
		err := r.mineRange(lo, e)
		la, ok := errors.AsType[*lookahead](err)
		if !ok {
			return err
		}
		if err := r.lookAhead(la, e); err != nil {
			return err
		}
	}
}

// lookAhead reads the changes of a table's definition from SCN la.scn to
// SCN e.
func (r *cdcReader) lookAhead(la *lookahead, e uint64) error {
	if err := startLogMiner(r.ctx, r.conn, la.scn, e); err != nil {
		return err
	}
	defer endLogMiner(context.WithoutCancel(r.ctx), r.conn)
	rows, err := r.conn.QueryContext(r.ctx, `SELECT nvl(data_objv#, 0), nvl(sql_redo, ' ') FROM v$logmnr_contents
	WHERE `+thisContainer+` AND operation_code = 5 AND seg_owner = :1 AND table_name = :2`, r.schema, la.table)
	if err != nil {
		return mining(err)
	}
	defer func() { _ = rows.Close() }()
	ddls := map[uint64]bool{}
	for rows.Next() {
		var objv uint64
		var stmt string
		if err := rows.Scan(&objv, &stmt); err != nil {
			return mining(err)
		}
		stmt = strings.ToLower(stmt)
		ddls[objv] = ddls[objv] || strings.HasPrefix(stmt, "alter table") && strings.Contains(stmt, " drop")
	}
	r.ddls[la.table] = ddls
	return mining(rows.Err())
}

// mineRange mines the redo from SCN lo to SCN e.
func (r *cdcReader) mineRange(lo, e uint64) error {
	if err := startLogMiner(r.ctx, r.conn, lo, e); err != nil {
		return err
	}
	defer endLogMiner(context.WithoutCancel(r.ctx), r.conn)
	names := make([]string, 0, len(r.order))
	for _, st := range r.order {
		names = append(names, quoteLiteral(st.t.name))
	}
	rows, err := r.conn.QueryContext(r.ctx, `SELECT scn, xidusn, xidslt, xidsqn, operation_code, rollback, csf,
	  nvl(rs_id, ' '), ssn, nvl(row_id, ' '), nvl(table_name, ' '), nvl(info, ' '), nvl(sql_redo, ' '), timestamp,
	  nvl(data_objv#, 0)
	FROM v$logmnr_contents
	WHERE `+thisContainer+` AND (operation_code IN (7, 36)
	  OR (seg_owner = :1 AND table_name IN (`+strings.Join(names, ", ")+`) AND operation_code IN (1, 2, 3, 5, 9, 68, 255)))`, r.schema)
	if err != nil {
		return mining(fmt.Errorf("oracle: read LogMiner's changes from SCN %d: %w", lo, err))
	}
	defer func() { _ = rows.Close() }()
	var x logRow
	var more bool
	var redo strings.Builder
	for rows.Next() {
		var y logRow
		var rollback, csf int
		var part string
		if err := rows.Scan(&y.scn, &y.usn, &y.slt, &y.sqn, &y.op, &rollback, &csf, &y.rsID, &y.ssn, &y.rowID, &y.table, &y.info, &part, &y.at, &y.objv); err != nil {
			return mining(err)
		}
		y.rollback = rollback == 1
		if more {
			// The rest of the statement before, past SQL_REDO's 4000 bytes.
			redo.WriteString(part)
		} else {
			x = y
			redo.Reset()
			redo.WriteString(part)
		}
		if more = csf == 1; more {
			continue
		}
		x.redo = strings.TrimSpace(redo.String())
		if r.restart != "" && x.scn > r.restartAt {
			// The read starts again after the change of a table's columns.
			return nil
		}
		if err := r.row(x); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return mining(err)
	}
	return r.reached(e)
}

// row handles a row of the redo, in SCN order.
func (r *cdcReader) row(x logRow) error {
	if err := r.reached(x.scn - 1); err != nil {
		return err
	}
	switch x.op {
	case 1, 2, 3, 9, 68:
		tx := r.txns[x.xid()]
		id := strconv.FormatUint(x.scn, 10) + "/" + x.rsID + "/" + strconv.FormatUint(x.ssn, 10)
		if tx == nil {
			tx = &txn{first: x.scn, seen: map[string]bool{}}
			r.txns[x.xid()] = tx
		}
		if tx.seen[id] {
			return nil
		}
		tx.seen[id] = true
		if x.rollback {
			// A rolled back statement or savepoint's undo of a change before
			// it to the same row.
			for i := len(tx.events) - 1; i >= 0; i-- {
				if ev := tx.events[i]; ev.rowID == x.rowID && ev.table == x.table {
					tx.events = slices.Delete(tx.events, i, i+1)
					break
				}
			}
			return nil
		}
		tx.events = append(tx.events, redoEvent{scn: x.scn, table: x.table, rowID: x.rowID, redo: x.redo, mismatch: x.info == "Dictionary Mismatch", objv: x.objv})
	case 7:
		tx := r.txns[x.xid()]
		delete(r.txns, x.xid())
		if tx == nil || x.scn <= r.done {
			return nil
		}
		return r.commit(tx, x.scn, x.at)
	case 36:
		delete(r.txns, x.xid())
		if x.sqn == 0xFFFFFFFF {
			prefix := fmt.Sprintf("%d.%d.", x.usn, x.slt)
			for id := range r.txns {
				if strings.HasPrefix(id, prefix) {
					delete(r.txns, id)
				}
			}
		}
	case 5:
		return r.ddl(x)
	case 255:
		if st := r.streams[x.table]; st != nil && !st.finished && x.scn > st.state.SCN {
			return configInvalid("oracle: LogMiner can't read a change to %s at SCN %d (%s), so cdc can't read the table", x.table, x.scn, x.info)
		}
	}
	return nil
}

// reached notes every row at or before scn is handled.
func (r *cdcReader) reached(scn uint64) error {
	if r.restart != "" && scn > r.restartAt {
		scn = r.restartAt
	}
	if scn > r.done || r.done == ^uint64(0) {
		r.done = scn
	}
	return r.passedUntil()
}

// commit sends a transaction's changes to the tables read, each with the
// commit's SCN and its place in the transaction.
func (r *cdcReader) commit(tx *txn, scn uint64, at time.Time) error {
	for _, ev := range tx.events {
		if _, ok := r.ddls[ev.table]; ev.mismatch && !ok {
			return &lookahead{table: ev.table, scn: ev.scn}
		}
	}
	for i, ev := range tx.events {
		st := r.streams[ev.table]
		if st == nil || st.finished || scn <= st.state.SCN || st.until != nil && scn > st.until.SCN {
			continue
		}
		row, err := parseRedo(ev.redo)
		if err != nil {
			return fmt.Errorf("oracle: %s at SCN %d: %w", ev.table, ev.scn, err)
		}
		vals, unchanged, err := r.values(st, row, ev)
		if err != nil {
			return fmt.Errorf("oracle: %s at SCN %d: %w", ev.table, ev.scn, err)
		}
		seq := seqOf(scn, i+1)
		if row.old != nil {
			old, oldUnchanged, err := r.values(st, redoRow{op: source.OpDelete, vals: row.old}, ev)
			if err != nil {
				return fmt.Errorf("oracle: %s at SCN %d: %w", ev.table, ev.scn, err)
			}
			if keyMoved(st.t, old, vals) {
				// The row under its old key is deleted first. Their keys differ,
				// so both take the update's sequence number.
				if err := st.b.change(old, source.OpDelete, seq, oldUnchanged); err != nil {
					return fmt.Errorf("oracle: %s: %w", st.t.name, err)
				}
			}
		}
		if st.lobs && row.op != source.OpDelete {
			if unchanged, err = r.lobs(st, vals, unchanged, scn); err != nil {
				return err
			}
		}
		if err := st.b.change(vals, row.op, seq, unchanged); err != nil {
			return fmt.Errorf("oracle: %s: %w", st.t.name, err)
		}
		st.dirty, st.last, st.lastAt = true, scn, at
		if st.b.bytes >= r.s.cfg.BatchBytes {
			if err := st.flush(r.out); err != nil {
				return err
			}
		}
	}
	return nil
}

// values are a change's row as the stream's columns, and the columns it
// doesn't carry: LOBs, which the redo log doesn't hold in full.
//
// LogMiner reads a change with the table's definition as it is when the
// window is mined. A change made before its columns changed it can't
// decode: it names their columns by their place in the row, COL n, and
// gives their values in their internal format. Adding, renaming, setting
// unused and modifying columns keeps each column's place, so such a change
// is read with the table's columns as the read found them, those added
// after it unchanged; dropping one moves the columns after it, so a change
// made before a drop can't be read.
func (r *cdcReader) values(st *cdcStream, row redoRow, ev redoEvent) ([]any, []string, error) {
	vals := make([]any, len(st.t.cols))
	byName := row.vals
	if ev.mismatch {
		for objv, drop := range r.ddls[ev.table] {
			if drop && objv > ev.objv {
				return nil, nil, positionLost("the change was made before a column of the table was dropped, and LogMiner can't read it: read the stream again from a snapshot")
			}
		}
		byName = map[string]expr{}
		for name, e := range row.vals {
			n, err := strconv.Atoi(strings.TrimPrefix(name, "COL "))
			if j, ok := st.bySeg[n]; ok && err == nil {
				byName[st.all[j].name] = e
			} else if !st.segs[n] {
				return nil, nil, positionLost("the change was made before a change of the table's columns, and has a column %s the table doesn't have now: read the stream again from a snapshot", name)
			}
		}
	}
	var unchanged []string
	for i, c := range st.t.cols {
		e, ok := byName[c.name]
		switch {
		case c.lob() || !ok:
			unchanged = append(unchanged, c.name)
		case ev.mismatch && e.fn == "HEXTORAW":
			b, err := redoValue(column{kind: kBinary}, e)
			if err != nil {
				return nil, nil, err
			}
			if vals[i], err = rawValue(c, b.([]byte)); err != nil {
				return nil, nil, fmt.Errorf("column %s: %w", c.name, err)
			}
		default:
			v, err := redoValue(c, e)
			if err != nil {
				return nil, nil, fmt.Errorf("column %s: %w", c.name, err)
			}
			vals[i] = v
		}
	}
	return vals, unchanged, nil
}

// keyMoved is whether an update's row, new, has another key than its old
// values, old; false where either doesn't say.
func keyMoved(t table, old, new []any) bool {
	for _, k := range t.key {
		c := t.cols[k]
		if old[k] == nil || new[k] == nil {
			return false
		}
		if keyText(c, old[k]) != keyText(c, new[k]) {
			return true
		}
	}
	return false
}

// lobs reads a change's LOB columns from the table as of its commit, by
// key. A row gone by then, or past the undo the database keeps, leaves them
// unchanged.
func (r *cdcReader) lobs(st *cdcStream, vals []any, unchanged []string, scn uint64) ([]string, error) {
	var sel []string
	var lobIdx []int
	for i, c := range st.t.cols {
		if c.lob() {
			sel = append(sel, c.selectExpr())
			lobIdx = append(lobIdx, i)
		}
	}
	w := where{args: []any{scn}}
	for _, k := range st.t.key {
		c := st.t.cols[k]
		if vals[k] == nil {
			return unchanged, nil
		}
		if err := w.add(c, "=", keyText(c, vals[k])); err != nil {
			return nil, err
		}
	}
	dest := make([]any, len(sel))
	ptrs := make([]any, len(sel))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	err := r.conn.QueryRowContext(r.ctx, `SELECT `+strings.Join(sel, ", ")+` FROM `+quoteIdent(r.schema)+`.`+quoteIdent(st.t.name)+` AS OF SCN :1 t`+w.sql(), w.args...).Scan(ptrs...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return unchanged, nil
	case slices.Contains([]int{1555, 8181, 1466}, oraCode(err)):
		if st.unreadable == nil || !st.unreadable["undo"] {
			slog.Warn("a change's LOB columns are past the undo the database keeps: they're sent unchanged", "table", st.t.name, "scn", scn, "err", err)
			st.unreadable = map[string]bool{"undo": true}
		}
		return unchanged, nil
	case err != nil:
		return nil, classify(fmt.Errorf("oracle: read %s's LOBs as of SCN %d: %w", st.t.name, scn, err))
	}
	for i, j := range lobIdx {
		vals[j] = dest[i]
		unchanged = slices.DeleteFunc(unchanged, func(n string) bool { return n == st.t.cols[j].name })
	}
	if len(unchanged) == 0 {
		unchanged = nil
	}
	return unchanged, nil
}

// ddl handles a change of a table read's definition: one that changes its
// columns stops the read there, and the next starts again with them.
func (r *cdcReader) ddl(x logRow) error {
	st := r.streams[x.table]
	if st == nil || st.finished || x.scn <= st.state.SCN {
		return nil
	}
	stmt := strings.ToLower(x.redo)
	switch {
	case strings.HasPrefix(stmt, "truncate"):
		return positionLost("oracle: %s was truncated at SCN %d, and the redo log doesn't say which rows that deleted: read the stream again from a snapshot", x.table, x.scn)
	case strings.HasPrefix(stmt, "drop table"):
		return configInvalid("oracle: %s was dropped at SCN %d", x.table, x.scn)
	}
	now, err := describe(r.ctx, r.conn, r.schema, []string{x.table})
	if err != nil {
		return err
	}
	if len(now) == 1 && slices.EqualFunc(now[0].cols, st.all, sameColumn) {
		return nil
	}
	if r.restart == "" {
		r.restart, r.restartAt = fmt.Sprintf("%s's columns changed at SCN %d", x.table, x.scn), x.scn
	}
	return nil
}

func sameColumn(a, b column) bool {
	return a.name == b.name && a.dataType == b.dataType && a.precision == b.precision && a.scale == b.scale && a.seg == b.seg
}

// passedUntil completes each stream whose read stops at a resume point the
// read has passed.
func (r *cdcReader) passedUntil() error {
	for _, st := range r.order {
		if st.until != nil && !st.finished && st.until.Snapshot == nil && r.done >= st.until.SCN {
			if err := r.finish(st); err != nil {
				return err
			}
		}
	}
	return nil
}

// finish completes a stream at its until: a resume point with until, and
// complete with it.
func (r *cdcReader) finish(st *cdcStream) error {
	if err := st.flush(r.out); err != nil {
		return err
	}
	if err := r.out.ResumePoint(st.t.name, st.untilRaw); err != nil {
		return err
	}
	st.finished = true
	return r.out.Complete(st.t.name, st.untilRaw)
}

func (r *cdcReader) allFinished() bool {
	return !slices.ContainsFunc(r.order, func(st *cdcStream) bool { return !st.finished })
}

// position is where a stream's next read starts, which depends on the
// stream's changes and the redo logs, not on when the read ended, so a read
// again from a resume point ends where the first did. Its SCN is past the
// stream's last change and, up to what's mined, the start of the redo log
// the log buffer's changes come after; its From is before the first change
// of every transaction open at it.
func (r *cdcReader) position(st *cdcStream) cdcState {
	s := st.state
	if s.Snapshot != nil || r.done == ^uint64(0) {
		return s
	}
	if st.last > s.SCN {
		s.SCN, s.At = st.last, new(st.lastAt.UTC())
	}
	if m := min(r.mark-1, r.done); r.mark > 0 && m > s.SCN {
		s.SCN = m
		if m == r.mark-1 {
			s.At = new(r.markAt.UTC())
		}
	}
	// A transaction whose first change wasn't mined began after the mark,
	// or after the read's start.
	s.From = min(max(st.state.From, r.mark), r.done+1)
	for _, tx := range r.txns {
		if len(tx.events) > 0 {
			s.From = min(s.From, tx.first)
		}
	}
	return s
}

func (r *cdcReader) state(st *cdcStream) ([]byte, error) { return json.Marshal(r.position(st)) }

func (r *cdcReader) resume() error {
	now := time.Now()
	for _, st := range r.order {
		if st.until != nil || st.finished {
			continue
		}
		if st.dirty && now.Sub(st.resumedAt) >= resumeEvery || st.resumed < r.position(st).SCN && now.Sub(st.resumedAt) >= quietEvery {
			if err := r.resumeStream(st); err != nil {
				return err
			}
		}
	}
	return nil
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
	st.dirty, st.resumed, st.resumedAt = false, r.position(st).SCN, time.Now()
	return nil
}

// complete ends a read that isn't continuous: each stream's next read
// starts where this one ended.
func (r *cdcReader) complete() error {
	for _, st := range r.order {
		if st.finished {
			continue
		}
		if st.until != nil {
			if err := r.finish(st); err != nil {
				return err
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

// progress reports how far the read is behind: how long its last window
// took to mine.
func (r *cdcReader) progress() {
	pr, ok := r.out.(source.Progresser)
	if !ok {
		return
	}
	lag := r.lag
	if err := pr.Progress(source.Progress{Lag: &lag}); err != nil {
		slog.Debug("reporting progress failed", "err", err)
	}
}

// declaredOnly narrows t to the columns the run declared. A column added
// since the run's Discover has no declared type. No columns: the request
// doesn't say.
func declaredOnly(t table, columns []string) (table, error) {
	if len(columns) == 0 {
		return t, nil
	}
	cols := make([]column, 0, len(t.cols))
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
			return t, transient("oracle: %s has a new key column %q; the read starts again to take it", t.name, t.cols[k].name)
		}
		key[i] = j
	}
	t.cols, t.key = cols, key
	return t, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
