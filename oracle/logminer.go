package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"
)

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func currentSCN(ctx context.Context, q queryer) (uint64, error) {
	var scn uint64
	if err := q.QueryRowContext(ctx, `SELECT current_scn FROM v$database`).Scan(&scn); err != nil {
		return 0, classify(fmt.Errorf("oracle: read the current SCN: %w", err))
	}
	return scn, nil
}

// redoLog is an archived or online redo log file.
type redoLog struct {
	name                string
	first, next         uint64
	thread, sequence    int64
	online, unavailable bool
}

// redoLogs lists the redo log files with redo after SCN from, by thread
// and sequence: archived logs still on disk, preferred over their online
// copy, and the online logs not yet archived.
func redoLogs(ctx context.Context, q queryer, from uint64) ([]redoLog, error) {
	rows, err := q.QueryContext(ctx, `SELECT name, first_change#, next_change#, thread#, sequence#, is_online, is_gone FROM (
	  SELECT a.name, a.first_change#, a.next_change#, a.thread#, a.sequence#, 0 is_online,
	    CASE WHEN a.name IS NULL OR a.status <> 'A' OR a.deleted = 'YES' THEN 1 ELSE 0 END is_gone
	  FROM v$archived_log a, v$database d
	  WHERE a.next_change# > :1 AND a.standby_dest = 'NO'
	    AND a.resetlogs_change# = d.resetlogs_change# AND a.resetlogs_time = d.resetlogs_time
	  UNION ALL
	  SELECT min(f.member), l.first_change#, l.next_change#, l.thread#, l.sequence#, 1, 0
	  FROM v$log l JOIN v$logfile f ON f.group# = l.group#
	  WHERE l.next_change# > :2 AND l.status IN ('CURRENT', 'ACTIVE', 'INACTIVE')
	  GROUP BY l.first_change#, l.next_change#, l.thread#, l.sequence#)
	ORDER BY thread#, sequence#, is_online, is_gone`, from, from)
	if err != nil {
		return nil, classify(fmt.Errorf("oracle: list the redo logs: %w", err))
	}
	defer func() { _ = rows.Close() }()
	var logs []redoLog
	for rows.Next() {
		var l redoLog
		var name sql.NullString
		var online, unavailable int
		if err := rows.Scan(&name, &l.first, &l.next, &l.thread, &l.sequence, &online, &unavailable); err != nil {
			return nil, classify(err)
		}
		l.name, l.online, l.unavailable = name.String, online == 1, unavailable == 1
		if n := len(logs); n > 0 && logs[n-1].thread == l.thread && logs[n-1].sequence == l.sequence {
			// The same log archived to another destination, or online too.
			if logs[n-1].unavailable && !l.unavailable {
				logs[n-1] = l
			}
			continue
		}
		logs = append(logs, l)
	}
	return logs, classify(rows.Err())
}

// logMark is the first SCN of the newest redo log that began flushLag ago
// or earlier, of the thread whose is oldest, and when it began: a change
// still in the log buffer comes after it. 0 if there's none.
func logMark(ctx context.Context, q queryer) (uint64, time.Time, error) {
	var scn sql.NullInt64
	var at sql.NullTime
	err := q.QueryRowContext(ctx, `SELECT min(b), min(t) FROM (
	  SELECT max(first_change#) b, SYS_EXTRACT_UTC(FROM_TZ(CAST(max(first_time) AS TIMESTAMP), TO_CHAR(SYSTIMESTAMP, 'TZR'))) t
	  FROM v$log WHERE first_time <= SYSDATE - :1 / 86400 GROUP BY thread#)`, int(flushLag/time.Second)).Scan(&scn, &at)
	if err != nil {
		return 0, time.Time{}, classify(fmt.Errorf("oracle: read the redo logs' first SCNs: %w", err))
	}
	return uint64(max(scn.Int64, 0)), at.Time, nil
}

// covered checks that the redo from SCN from on is all on disk, in each
// redo thread: a log that holds from, and every log after it.
func covered(logs []redoLog, from uint64) error {
	if len(logs) == 0 {
		return positionLost("oracle: no redo log holds SCN %d: the read starts after the redo the database keeps", from)
	}
	for i, l := range logs {
		first := i == 0 || logs[i-1].thread != l.thread
		switch {
		case l.unavailable:
			return positionLost("oracle: the archived redo log of thread %d, sequence %d, with SCNs %d to %d, is gone, and the read needs it from SCN %d: read the stream again from a snapshot, and keep archived logs longer", l.thread, l.sequence, l.first, l.next, from)
		case first && l.first > from:
			return positionLost("oracle: the redo log that holds SCN %d is gone: thread %d's oldest log on disk starts at SCN %d. Read the stream again from a snapshot, and keep archived logs longer", from, l.thread, l.first)
		case !first && logs[i-1].sequence+1 != l.sequence:
			return positionLost("oracle: thread %d's redo log sequence %d is gone, between the logs the read needs from SCN %d", l.thread, logs[i-1].sequence+1, from)
		}
	}
	return nil
}

// startLogMiner starts a LogMiner session on conn over the redo from SCN
// from to SCN to, inclusive; to 0 is the current SCN. It reads with the
// online catalog as its dictionary. Connected to a PDB, LogMiner finds the
// redo logs itself and returns only that PDB's changes; in a non-CDB, or
// in CDB$ROOT, the session is given the logs that hold the redo.
func startLogMiner(ctx context.Context, conn *sql.Conn, from, to uint64) error {
	var cdb, con string
	if err := conn.QueryRowContext(ctx, `SELECT d.cdb, sys_context('USERENV', 'CON_NAME') FROM v$database d`).Scan(&cdb, &con); err != nil {
		return classify(fmt.Errorf("oracle: read the database's container: %w", err))
	}
	if cdb != "YES" || con == "CDB$ROOT" {
		logs, err := redoLogs(ctx, conn, from)
		if err != nil {
			return err
		}
		if err := covered(logs, from); err != nil {
			return err
		}
		for i, l := range upTo(logs, to) {
			opt := "SYS.DBMS_LOGMNR.ADDFILE"
			if i == 0 {
				opt = "SYS.DBMS_LOGMNR.NEW"
			}
			if _, err := conn.ExecContext(ctx, `BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => `+opt+`); END;`, l.name); err != nil {
				return mining(fmt.Errorf("oracle: add the redo log %s to LogMiner: %w", l.name, err))
			}
		}
	}
	end := ""
	args := []any{from}
	if to != 0 {
		end, args = "ENDSCN => :2, ", append(args, to)
	}
	_, err := conn.ExecContext(ctx, `BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, `+end+`OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT); END;`, args...)
	if err != nil {
		return mining(fmt.Errorf("oracle: start LogMiner from SCN %d: %w", from, err))
	}
	return nil
}

// upTo are the logs, of every thread, that begin at or before SCN to; to 0
// is all of them.
func upTo(logs []redoLog, to uint64) []redoLog {
	if to == 0 {
		return logs
	}
	return slices.DeleteFunc(slices.Clone(logs), func(l redoLog) bool { return l.first > to })
}

// thisContainer keeps the rows of the container the session is in: mined
// in CDB$ROOT, the redo holds every container's, whose transaction IDs can
// be the same.
const thisContainer = `nvl(src_con_id, 0) = to_number(sys_context('USERENV', 'CON_ID'))`

// mining classifies a LogMiner session's failure. Once the read has checked
// the logs are on disk, a log LogMiner can't find or open is one being
// switched or archived.
func mining(err error) error {
	if slices.Contains([]int{1291, 1292, 310, 308, 1289}, oraCode(err)) {
		return transient("%w", err)
	}
	return classify(err)
}

// endLogMiner ends conn's LogMiner session, if it has one.
func endLogMiner(ctx context.Context, conn *sql.Conn) {
	_, _ = conn.ExecContext(ctx, `BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; END;`)
}
