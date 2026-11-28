package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl-connectors/sqlstage"
)

// swapStep runs after each step of a swap that tests stop at: "prepared",
// once the read's table matches the target, "locked", once the target is
// locked, and "swapped", once the read's table has its place. An error ends
// the apply.
var swapStep = func(context.Context, pgx.Tx, string) error { return nil }

// replace makes each ended read's rows its target's, in tx. Where the swap
// is on and exact, the read's table takes the target's place, by name, and
// the target is dropped; otherwise the target's rows are deleted, or
// truncated, and the read's inserted, and the read's table dropped. Copies
// go first, and then the swaps take every lock they need before any of
// them drops a target, so that a swap's lock on its target is held for
// least time, and not while another stream copies. swapped are the streams
// that swapped.
func (w *writer) replace(ctx context.Context, tx pgx.Tx, ended []stagedStream, prep map[string]prepared) (swapped []string, _ error) {
	var todo []ending
	for _, s := range ended {
		p := prep[s.Name]
		switch {
		case s.Table == "" && p.hasRead:
			return nil, fmt.Errorf("postgres: apply %s: its read has rows in %s, and no target table", s.Name, s.Read)
		case s.Table == "":
			// The stream never had a schema, so it has no table to empty.
			continue
		}
		var exists bool
		if err := tx.QueryRow(ctx, relationExists, w.ident(s.Table)).Scan(&exists); err != nil {
			return nil, fmt.Errorf("postgres: apply %s: %w", s.Name, err)
		}
		if !exists {
			continue
		}
		e := ending{s: s, p: p}
		if w.swap {
			var why string
			var err error
			if e.plan, why, err = w.prepareSwap(ctx, tx, s, p); err != nil {
				return nil, fmt.Errorf("postgres: apply %s: swap its read's table in: %w", s.Name, err)
			}
			if e.plan == nil {
				w.copiesIn(s, why)
			}
		}
		todo = append(todo, e)
	}
	var swaps []ending
	for _, e := range todo {
		if e.plan != nil {
			swaps = append(swaps, e)
		} else if err := w.copyIn(ctx, tx, e.s, e.p, false); err != nil {
			return nil, fmt.Errorf("postgres: apply %s: %w", e.s.Name, err)
		}
	}
	// A swap whose lock, check or switch fails undoes every swap's, letting
	// go of their locks, then copies, and the rest try again: no copy runs
	// while a swap holds its target.
	for len(swaps) > 0 {
		i, why, err := w.switchAll(ctx, tx, swaps)
		if err != nil {
			return nil, fmt.Errorf("postgres: apply %s: swap its read's table in: %w", swaps[max(i, 0)].s.Name, err)
		}
		if i < 0 {
			for _, e := range swaps {
				swapped = append(swapped, e.s.Name)
			}
			break
		}
		e := swaps[i]
		w.copiesIn(e.s, why)
		if err := w.copyIn(ctx, tx, e.s, e.p, e.plan.created); err != nil {
			return nil, fmt.Errorf("postgres: apply %s: %w", e.s.Name, err)
		}
		swaps = slices.Delete(swaps, i, i+1)
	}
	return swapped, nil
}

// ending is an overwrite stream whose read ends: a swap if plan isn't nil.
type ending struct {
	s    stagedStream
	p    prepared
	plan *swapPlan
}

// switchAll puts each swap's read's table in its target's place in tx, in
// one savepoint: it locks every target, waiting up to lockTimeout for each,
// and checks it and the read's table again; then, for each, it gives the
// read's table the target's privileges, drops the target, gives the read's
// table its names, and checks that the table then is as the target was,
// but for its OID. At the first that fails, it rolls the savepoint back,
// undoing them all and letting go of every lock: i is that swap's, and why
// says why; -1 if none failed.
func (w *writer) switchAll(ctx context.Context, tx pgx.Tx, swaps []ending) (i int, why string, _ error) {
	i = -1
	why, err := inSavepoint(ctx, tx, func(tx pgx.Tx) (string, error) {
		for j, e := range swaps {
			i = j
			target, read := w.ident(e.s.Table), w.ident(e.s.Read)
			if err := lockBriefly(ctx, tx, "LOCK TABLE "+target+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				return "", err
			}
			if err := swapStep(ctx, tx, "locked"); err != nil {
				return "", err
			}
			// Another session may have changed the target, or the read's
			// table, since they were read.
			if why, err := unswappable(ctx, tx, target, read); err != nil || why != "" {
				return why, err
			}
			if why, err := unswappable(ctx, tx, read, read); err != nil || why != "" {
				return "the read's table: " + why, err
			}
			now, err := signature(ctx, tx, target)
			if err != nil {
				return "", err
			}
			if now != e.plan.before {
				return "it changed while the read's table was made to match it: its " + differs(e.plan.before, now), nil
			}
		}
		for j, e := range swaps {
			i = j
			if why, err := w.switchIn(ctx, tx, e.s, e.plan); err != nil || why != "" {
				return why, err
			}
		}
		i = -1
		return "", nil
	})
	if err != nil || why == "" {
		return i, "", err
	}
	return i, why, nil
}

func (w *writer) copiesIn(s stagedStream, why string) {
	slog.Info("an overwrite copies its read's rows into its table, rather than putting the read's table in its place",
		"stream", s.Name, "table", w.ident(s.Table), "reason", why)
}

// copyIn replaces the target's rows with the read's, which readers see once
// tx commits, and drops the read's table; created says a swap created it.
// With OverwriteTruncate, it truncates the target if it can within
// lockTimeout, and deletes its rows otherwise, or where a publication of
// the target or a table it truncates would send the inserts and not the
// truncate.
func (w *writer) copyIn(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared, created bool) error {
	target := w.ident(s.Table)
	emptied := false
	if w.truncate {
		why, err := inSavepoint(ctx, tx, func(tx pgx.Tx) (string, error) {
			// The lock, on the table and those it truncates, holds off a
			// change to their publications.
			if err := lockBriefly(ctx, tx, "LOCK TABLE "+target+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				return "", err
			}
			var dropped bool
			if err := tx.QueryRow(ctx, `WITH RECURSIVE t(oid) AS (SELECT to_regclass($1)::oid UNION SELECT i.inhrelid FROM pg_inherits i JOIN t ON i.inhparent = t.oid)
				SELECT EXISTS (SELECT FROM t JOIN pg_class c ON c.oid = t.oid JOIN pg_namespace n ON n.oid = c.relnamespace
					JOIN pg_publication_tables pt ON pt.schemaname = n.nspname AND pt.tablename = c.relname
					JOIN pg_publication p ON p.pubname = pt.pubname WHERE NOT p.pubtruncate)`, target).Scan(&dropped); err != nil || dropped {
				return "a publication publishes its rows' changes, but not a truncate", err
			}
			_, err := tx.Exec(ctx, "TRUNCATE "+target)
			return "", err
		})
		if err != nil {
			return err
		}
		if why != "" {
			slog.Info("an overwrite deletes its table's rows, rather than truncating it", "stream", s.Name, "table", target, "reason", why)
		}
		emptied = why == ""
	}
	if !emptied {
		if _, err := tx.Exec(ctx, "DELETE FROM "+target); err != nil {
			return err
		}
	}
	if p.hasRead {
		if _, err := tx.Exec(ctx, "INSERT INTO "+target+columnList(p.read)+"SELECT "+quoteAll(p.read)+" FROM "+w.ident(s.Read)); err != nil {
			return err
		}
	}
	if p.hasRead || created {
		if _, err := tx.Exec(ctx, "DROP TABLE "+w.ident(s.Read)); err != nil {
			return err
		}
	}
	return nil
}

// inSavepoint runs f in a savepoint of tx, which it rolls back if f fails or
// gives a reason not to go on. why is that reason, or the server's error;
// any other error, such as a lost connection, is err.
func inSavepoint(ctx context.Context, tx pgx.Tx, f func(tx pgx.Tx) (string, error)) (why string, err error) {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return "", err
	}
	why, err = f(sp)
	if err == nil && why == "" {
		return "", sp.Commit(ctx)
	}
	if rerr := sp.Rollback(ctx); rerr != nil {
		return "", errors.Join(err, rerr)
	}
	pe, ok := errors.AsType[*pgconn.PgError](err)
	switch {
	case err == nil:
		return why, nil
	case !ok:
		return "", err
	case pe.Code == "55P03":
		return fmt.Sprintf("other sessions held a lock on it for more than %s", lockTimeout), nil
	}
	return fmt.Sprintf("%s (SQLSTATE %s)", pe.Message, pe.Code), nil
}

// lockBriefly runs stmt, waiting up to lockTimeout for its locks.
func lockBriefly(ctx context.Context, tx pgx.Tx, stmt string) error {
	var was string
	if err := tx.QueryRow(ctx, "SELECT current_setting('lock_timeout')").Scan(&was); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true)", fmt.Sprintf("%dms", lockTimeout.Milliseconds())); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, stmt); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true)", was)
	return err
}

// swapPlan is a swap whose read's table matches its target, but for the
// names that the target holds until it's dropped.
type swapPlan struct {
	// The target's signature, which the read's table must have once it has
	// the target's place.
	before string
	// Statements that give the read's table's constraints and indexes the
	// target's names.
	renames []string
	// The read had no table, so the swap created one, empty.
	created bool
}

// prepareSwap makes the read's table match the target in tx, if the swap
// would be exact: why says why not. It builds the target's indexes and
// constraints on it under names of its own, and changes nothing that
// readers of the target see.
func (w *writer) prepareSwap(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared) (*swapPlan, string, error) {
	plan := &swapPlan{created: !p.hasRead}
	target, read := w.ident(s.Table), w.ident(s.Read)
	why, err := inSavepoint(ctx, tx, func(tx pgx.Tx) (string, error) {
		if why, err := unswappable(ctx, tx, target, read); err != nil || why != "" {
			return why, err
		}
		if p.hasRead {
			if why, err := unswappable(ctx, tx, read, read); err != nil || why != "" {
				return "the read's table: " + why, err
			}
		}
		var err error
		if plan.before, err = signature(ctx, tx, target); err != nil {
			return "", err
		}
		if plan.created {
			// The target's columns, with their types, collations, NOT NULL,
			// storage and compression.
			if _, err := tx.Exec(ctx, "CREATE TABLE "+read+" (LIKE "+target+" INCLUDING STORAGE INCLUDING COMPRESSION)"); err != nil {
				return "", err
			}
		}
		var why string
		if plan.renames, why, err = w.match(ctx, tx, s, target, read); err != nil || why != "" {
			return why, err
		}
		return "", swapStep(ctx, tx, "prepared")
	})
	if err != nil || why != "" {
		return nil, why, err
	}
	return plan, "", nil
}

// switchIn puts the read's table in the target's place in tx, once
// switchAll has the target's lock. why says why it didn't; switchAll rolls
// back what it did. The read's table takes the target's privileges only
// here, as one whose owner gave up its own SELECT couldn't be copied from.
func (w *writer) switchIn(ctx context.Context, tx pgx.Tx, s stagedStream, plan *swapPlan) (string, error) {
	target, read := w.ident(s.Table), w.ident(s.Read)
	rows, err := tx.Query(ctx, grantsQuery, target, read)
	if err != nil {
		return "", err
	}
	stmts, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	stmts = append(stmts, "DROP TABLE "+target)
	stmts = append(stmts, plan.renames...)
	stmts = append(stmts, "ALTER TABLE "+read+" RENAME TO "+pgx.Identifier{s.Table}.Sanitize())
	for _, q := range stmts {
		if _, err := tx.Exec(ctx, q); err != nil {
			return "", err
		}
	}
	after, err := signature(ctx, tx, target)
	if err != nil {
		return "", err
	}
	if after != plan.before {
		return "the read's table would differ from it in its " + differs(plan.before, after), nil
	}
	return "", swapStep(ctx, tx, "swapped")
}

// differs names the parts of two signatures that differ.
func differs(a, b string) string {
	var x, y map[string]json.RawMessage
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return "signature"
	}
	var out []string
	for _, k := range slices.Sorted(maps.Keys(x)) {
		if string(x[k]) != string(y[k]) {
			out = append(out, k)
		}
	}
	return strings.Join(out, ", ")
}

// unswappable says why the read's table can't take the target's place
// exactly, if it can't; read need not exist yet. A swap keeps only what it
// can make again on the read's table: its columns' defaults and NOT NULL,
// the target's constraints and indexes, its owner, privileges, comments and
// storage parameters. What else hangs off the target, or would see the
// change, makes it not exact.
func unswappable(ctx context.Context, q pgx.Tx, target, read string) (string, error) {
	var why string
	err := q.QueryRow(ctx, unswappableQuery, target, read).Scan(&why)
	if errors.Is(err, pgx.ErrNoRows) {
		return "it's gone", nil
	}
	return why, err
}

// The columns of the relation c, as a swap must keep them.
const swapColumns = `(SELECT array_agg(row(a.attname, a.atttypid, a.atttypmod, a.attndims, a.attcollation, a.attstorage, to_jsonb(a) ->> 'attcompression')::text ORDER BY a.attnum)
	FROM pg_attribute a WHERE a.attrelid = %s.oid AND a.attnum > 0 AND NOT a.attisdropped)`

var unswappableQuery = `SELECT CASE
	WHEN c.relkind <> 'r' THEN 'it isn''t an ordinary table'
	WHEN c.relpersistence <> 'p' THEN 'it''s unlogged'
	WHEN c.relispartition OR c.relhassubclass OR EXISTS (SELECT FROM pg_inherits WHERE inhrelid = c.oid OR inhparent = c.oid) THEN 'it''s a partition, or inherits from a table or is inherited from'
	WHEN c.reloftype <> 0 THEN 'it''s a typed table'
	WHEN NOT pg_has_role(c.relowner, 'USAGE') THEN 'the user doesn''t own it'
	WHEN c.relrowsecurity OR c.relforcerowsecurity OR EXISTS (SELECT FROM pg_policy WHERE polrelid = c.oid) THEN 'it has row security'
	WHEN c.relreplident <> 'd' THEN 'its replica identity isn''t the default'
	WHEN EXISTS (SELECT FROM pg_trigger WHERE tgrelid = c.oid AND NOT tgisinternal) THEN 'it has triggers'
	WHEN EXISTS (SELECT FROM pg_rewrite WHERE ev_class = c.oid) THEN 'it has rules'
	WHEN EXISTS (SELECT FROM pg_constraint WHERE contype = 'f' AND (conrelid = c.oid OR confrelid = c.oid)) THEN 'a foreign key refers to it or from it'
	WHEN EXISTS (SELECT FROM pg_constraint WHERE conrelid = c.oid AND NOT convalidated) THEN 'it has a constraint that isn''t validated'
	WHEN EXISTS (SELECT FROM pg_constraint WHERE conrelid = c.oid AND contype NOT IN ('p', 'u', 'x', 'c', 'n')) THEN 'it has a constraint trigger'
	WHEN EXISTS (SELECT FROM pg_publication WHERE puballtables) OR EXISTS (SELECT FROM pg_publication_rel WHERE prrelid = c.oid) THEN 'a publication publishes it'
	WHEN EXISTS (SELECT FROM pg_publication_tables WHERE pubname IN (SELECT pubname FROM pg_publication WHERE NOT puballtables)
		AND schemaname = n.nspname AND tablename = c.relname) THEN 'a publication publishes it'
	WHEN EXISTS (SELECT FROM pg_replication_slots WHERE slot_type = 'logical' AND database = current_database() AND plugin <> 'pgoutput')
		THEN 'a logical replication slot decodes its database''s changes, which a swap has none of'
	WHEN EXISTS (SELECT FROM pg_subscription_rel WHERE srrelid = c.oid) THEN 'a subscription writes to it'
	WHEN EXISTS (SELECT FROM pg_statistic_ext WHERE stxrelid = c.oid) THEN 'it has extended statistics'
	WHEN EXISTS (SELECT FROM pg_seclabel WHERE classoid = 'pg_class'::regclass AND objoid = c.oid) THEN 'it has security labels'
	WHEN EXISTS (SELECT FROM pg_description WHERE classoid = 'pg_type'::regclass AND objoid = c.reltype)
		OR EXISTS (SELECT FROM pg_seclabel WHERE classoid = 'pg_type'::regclass AND objoid = c.reltype) THEN 'its row type has a comment or a security label'
	WHEN EXISTS (SELECT FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = c.oid AND deptype = 'e') THEN 'it belongs to an extension'
	WHEN EXISTS (SELECT FROM pg_attribute WHERE attrelid = c.oid AND attnum > 0 AND NOT attisdropped AND (attidentity <> '' OR attgenerated <> '')) THEN 'it has identity or generated columns'
	WHEN EXISTS (SELECT FROM pg_attribute a WHERE attrelid = c.oid AND attnum > 0 AND NOT attisdropped
		AND (coalesce(attstattarget::int, -1) >= 0 OR EXISTS (SELECT FROM unnest(a.attoptions) o WHERE o NOT LIKE 'pg\_%'))) THEN 'a column has options or a statistics target'
	WHEN EXISTS (SELECT FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid WHERE i.indrelid = c.oid
		AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive OR i.indisclustered OR ic.reltablespace <> 0)) THEN 'an index of it is clustered, in a tablespace of its own, being built or invalid'
	WHEN EXISTS (SELECT FROM pg_index i JOIN pg_attribute ia ON ia.attrelid = i.indexrelid WHERE i.indrelid = c.oid AND coalesce(ia.attstattarget::int, -1) >= 0)
		THEN 'an index of it has a statistics target'
	WHEN EXISTS (SELECT FROM pg_description d WHERE (d.classoid = 'pg_class'::regclass AND d.objoid IN (SELECT indexrelid FROM pg_index WHERE indrelid = c.oid))
		OR (d.classoid = 'pg_constraint'::regclass AND d.objoid IN (SELECT oid FROM pg_constraint WHERE conrelid = c.oid))) THEN 'an index or a constraint of it has a comment'
	WHEN EXISTS (SELECT FROM aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x WHERE x.grantor <> c.relowner)
		OR EXISTS (SELECT FROM pg_attribute a, aclexplode(a.attacl) x WHERE a.attrelid = c.oid AND x.grantor <> c.relowner) THEN 'a role that doesn''t own it granted a privilege on it'
	WHEN EXISTS (SELECT FROM pg_depend d WHERE
		((d.refclassid = 'pg_class'::regclass AND d.refobjid = c.oid) OR (d.refclassid = 'pg_type'::regclass AND d.refobjid IN (c.reltype, (SELECT typarray FROM pg_type WHERE oid = c.reltype))))
		AND NOT ((d.classid = 'pg_class'::regclass AND (d.objid = c.oid OR d.objid = c.reltoastrelid OR d.objid IN (SELECT indexrelid FROM pg_index WHERE indrelid = c.oid)))
			OR (d.classid = 'pg_constraint'::regclass AND d.objid IN (SELECT oid FROM pg_constraint WHERE conrelid = c.oid AND contype IN ('p', 'u', 'x', 'c', 'n')))
			OR (d.classid = 'pg_attrdef'::regclass AND d.objid IN (SELECT oid FROM pg_attrdef WHERE adrelid = c.oid))
			OR (d.classid = 'pg_type'::regclass AND d.objid IN (c.reltype, (SELECT typarray FROM pg_type WHERE oid = c.reltype))))) THEN 'another object, such as a view, depends on it'
	WHEN r.oid IS NOT NULL AND (r.relam <> c.relam OR r.reltablespace <> c.reltablespace) THEN 'its access method or tablespace isn''t the read''s table''s'
	WHEN r.oid IS NOT NULL AND ` + fmt.Sprintf(swapColumns, "c") + ` IS DISTINCT FROM ` + fmt.Sprintf(swapColumns, "r") + `
		THEN 'its columns aren''t the read''s table''s, in their names, order, types, collations or storage'
	ELSE '' END
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_class r ON r.oid = to_regclass($2)
	WHERE c.oid = to_regclass($1)`

// signature is what a swap keeps of a table, as JSON: everything the
// catalog has of it, its columns, constraints and indexes, but their OIDs,
// what depends on its OID, its physical state and the flags that say only
// that it once had indexes, rules, triggers or children. Names are part of
// it, so the read's table has its target's only once it has its names.
func signature(ctx context.Context, q pgx.Tx, table string) (string, error) {
	var sig string
	err := q.QueryRow(ctx, signatureQuery, table).Scan(&sig)
	return sig, err
}

const signatureQuery = `SELECT jsonb_build_object(
	'table', to_jsonb(c) - '{oid,reltype,relfilenode,reltoastrelid,relpages,reltuples,relallvisible,relallfrozen,relfrozenxid,relminmxid,relnatts,relacl,relrewrite,relhasindex,relhasrules,relhastriggers,relhassubclass}'::text[],
	'privileges', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.grantee, x.privilege_type) FROM aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x),
	'comment', obj_description(c.oid, 'pg_class'),
	'toast', (SELECT t.reloptions FROM pg_class t WHERE t.oid = c.reltoastrelid),
	'columns', (SELECT jsonb_agg(to_jsonb(a) - '{attrelid,attnum,atthasmissing,attmissingval,attoptions,attacl}'::text[] || jsonb_build_object(
			'default', pg_get_expr(d.adbin, d.adrelid),
			'comment', col_description(c.oid, a.attnum),
			'options', (SELECT jsonb_agg(o ORDER BY o) FROM unnest(a.attoptions) o WHERE o NOT LIKE 'pg\_%'),
			'privileges', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.grantee, x.privilege_type) FROM aclexplode(a.attacl) x))
		ORDER BY a.attnum)
		FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
	'constraints', (SELECT jsonb_agg(to_jsonb(co) - '{oid,conrelid,conindid,conkey,confkey,conbin,conparentid}'::text[] || jsonb_build_object(
			'definition', pg_get_constraintdef(co.oid),
			'index', (SELECT relname FROM pg_class WHERE oid = co.conindid),
			'comment', obj_description(co.oid, 'pg_constraint'))
		ORDER BY co.conname, co.contype)
		FROM pg_constraint co WHERE co.conrelid = c.oid),
	'indexes', (SELECT jsonb_agg(to_jsonb(i) - '{indexrelid,indrelid,indkey,indexprs,indpred,indcheckxmin}'::text[] || jsonb_build_object(
			'name', ic.relname,
			'definition', pg_get_indexdef(i.indexrelid),
			'tablespace', ic.reltablespace,
			'options', ic.reloptions,
			'comment', obj_description(ic.oid, 'pg_class'),
			'statistics', (SELECT jsonb_agg(to_jsonb(ia) -> 'attstattarget' ORDER BY ia.attnum) FROM pg_attribute ia WHERE ia.attrelid = ic.oid))
		ORDER BY ic.relname)
		FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid WHERE i.indrelid = c.oid)
	)::text FROM pg_class c WHERE c.oid = to_regclass($1)`

// swapTable is what a swap makes again of a table on the read's.
type swapTable struct {
	// Quoted.
	owner                 string
	comment               *string
	options, toastOptions []string
	columns               []relColumn
	constraints           []relConstraint
	// Those that back no constraint.
	indexes []relIndex
}

type relColumn struct {
	name string
	// Its default's expression; nil for none.
	def     *string
	notNull bool
	comment *string
}

type relConstraint struct {
	name string
	// contype.
	typ string
	def string
	// A NOT NULL constraint's column.
	column string
	// The index it's backed by: its name and storage parameters.
	index        string
	indexOptions []string
}

type relIndex struct {
	name   string
	unique bool
	// Its definition after "CREATE [UNIQUE] INDEX name ON table ".
	using string
}

// describe reads what a swap makes again of table.
func describe(ctx context.Context, q pgx.Tx, table string) (swapTable, error) {
	var r swapTable
	err := q.QueryRow(ctx, `SELECT quote_ident(pg_get_userbyid(c.relowner)), obj_description(c.oid, 'pg_class'), coalesce(c.reloptions, '{}'), coalesce(t.reloptions, '{}')
		FROM pg_class c LEFT JOIN pg_class t ON t.oid = c.reltoastrelid WHERE c.oid = to_regclass($1)`, table).Scan(&r.owner, &r.comment, &r.options, &r.toastOptions)
	if err != nil {
		return r, err
	}
	rows, err := q.Query(ctx, `SELECT a.attname, pg_get_expr(d.adbin, d.adrelid), a.attnotnull, col_description(a.attrelid, a.attnum)
		FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`, table)
	if err != nil {
		return r, err
	}
	if r.columns, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (relColumn, error) {
		var c relColumn
		return c, row.Scan(&c.name, &c.def, &c.notNull, &c.comment)
	}); err != nil {
		return r, err
	}
	rows, err = q.Query(ctx, `SELECT co.conname, co.contype::text, pg_get_constraintdef(co.oid),
			coalesce((SELECT attname FROM pg_attribute WHERE co.contype = 'n' AND attrelid = co.conrelid AND attnum = co.conkey[1]), ''),
			coalesce(ic.relname, ''), coalesce(ic.reloptions, '{}')
		FROM pg_constraint co LEFT JOIN pg_class ic ON ic.oid = co.conindid
		WHERE co.conrelid = to_regclass($1) ORDER BY co.conname`, table)
	if err != nil {
		return r, err
	}
	if r.constraints, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (relConstraint, error) {
		var c relConstraint
		return c, row.Scan(&c.name, &c.typ, &c.def, &c.column, &c.index, &c.indexOptions)
	}); err != nil {
		return r, err
	}
	// pg_get_indexdef names the table as quote_ident does, qualified.
	rows, err = q.Query(ctx, `SELECT ic.relname, i.indisunique, CASE WHEN starts_with(def, prefix) THEN substr(def, length(prefix) + 1) ELSE '' END
		FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace,
			LATERAL (SELECT pg_get_indexdef(i.indexrelid) AS def,
				'CREATE ' || CASE WHEN i.indisunique THEN 'UNIQUE ' ELSE '' END || 'INDEX ' || quote_ident(ic.relname) || ' ON ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ' ' AS prefix) x
		WHERE i.indrelid = to_regclass($1)
			AND NOT EXISTS (SELECT FROM pg_constraint co WHERE co.conrelid = i.indrelid AND co.conindid = i.indexrelid)
		ORDER BY ic.relname`, table)
	if err != nil {
		return r, err
	}
	r.indexes, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (relIndex, error) {
		var x relIndex
		return x, row.Scan(&x.name, &x.unique, &x.using)
	})
	return r, err
}

func (r swapTable) notNull(column string) *relConstraint {
	i := slices.IndexFunc(r.constraints, func(c relConstraint) bool { return c.typ == "n" && c.column == column })
	if i < 0 {
		return nil
	}
	return &r.constraints[i]
}

// match makes the read's table, which has the target's columns, match the
// target in tx: its columns' defaults, NOT NULL and comments, its
// constraints and indexes, its statistics, owner, comment and storage
// parameters; switchIn gives it the target's privileges. Constraints and indexes whose names the target holds
// get names of their own; renames gives them the target's once it's gone.
// Checking the rows against the target's constraints and building its
// indexes are the only steps that read them.
func (w *writer) match(ctx context.Context, tx pgx.Tx, s stagedStream, target, read string) (renames []string, why string, _ error) {
	t, err := describe(ctx, tx, target)
	if err != nil {
		return nil, "", err
	}
	r, err := describe(ctx, tx, read)
	if err != nil {
		return nil, "", err
	}
	if len(t.columns) != len(r.columns) {
		return nil, "its columns aren't the read's table's", nil
	}
	ident := func(name string) string { return pgx.Identifier{name}.Sanitize() }
	qualified := func(name string) string { return w.ident(name) }
	var alter, after []string
	for i, tc := range t.columns {
		rc := r.columns[i]
		if rc.name != tc.name {
			return nil, "its columns aren't the read's table's", nil
		}
		col := ident(tc.name)
		switch {
		case tc.def == nil && rc.def != nil:
			alter = append(alter, "ALTER COLUMN "+col+" DROP DEFAULT")
		case tc.def != nil && (rc.def == nil || *rc.def != *tc.def):
			alter = append(alter, "ALTER COLUMN "+col+" SET DEFAULT "+*tc.def)
		}
		tn, rn := t.notNull(tc.name), r.notNull(tc.name)
		switch {
		case !tc.notNull && rc.notNull:
			alter = append(alter, "ALTER COLUMN "+col+" DROP NOT NULL")
		case tc.notNull && !rc.notNull && tn != nil:
			alter = append(alter, "ADD CONSTRAINT "+ident(tn.name)+" "+tn.def)
		case tc.notNull && !rc.notNull:
			alter = append(alter, "ALTER COLUMN "+col+" SET NOT NULL")
		case tn != nil && rn != nil && tn.name != rn.name:
			after = append(after, "ALTER TABLE "+read+" RENAME CONSTRAINT "+ident(rn.name)+" TO "+ident(tn.name))
		}
		if !equal(tc.comment, rc.comment) {
			after = append(after, "COMMENT ON COLUMN "+read+"."+col+" IS "+literal(tc.comment))
		}
	}
	for _, rc := range r.constraints {
		switch {
		case rc.typ == "c" && !slices.ContainsFunc(t.constraints, func(tc relConstraint) bool { return tc.typ == "c" && tc.name == rc.name && tc.def == rc.def }):
			alter = append(alter, "DROP CONSTRAINT "+ident(rc.name))
		case rc.typ != "c" && rc.typ != "n":
			return nil, "the read's table has constraints of its own", nil
		}
	}
	var backed []relConstraint
	for _, tc := range t.constraints {
		switch tc.typ {
		case "c":
			if !slices.ContainsFunc(r.constraints, func(rc relConstraint) bool { return rc.typ == "c" && rc.name == tc.name && rc.def == tc.def }) {
				alter = append(alter, "ADD CONSTRAINT "+ident(tc.name)+" "+tc.def)
			}
		case "p", "u", "x":
			backed = append(backed, tc)
		}
	}
	// One statement, so that the rows are read once for NOT NULL and the
	// checks.
	if len(alter) > 0 {
		if _, err := tx.Exec(ctx, "ALTER TABLE "+read+" "+strings.Join(alter, ", ")); err != nil {
			return nil, "", err
		}
	}
	for _, tc := range backed {
		tmp := sqlstage.HashName("_openetl_swap_", s.Read, tc.name)
		if _, err := tx.Exec(ctx, "ALTER TABLE "+read+" ADD CONSTRAINT "+ident(tmp)+" "+tc.def); err != nil {
			return nil, "", err
		}
		if len(tc.indexOptions) > 0 {
			if _, err := tx.Exec(ctx, "ALTER INDEX "+qualified(tmp)+" SET ("+options(tc.indexOptions, "")+")"); err != nil {
				return nil, "", err
			}
		}
		renames = append(renames, "ALTER TABLE "+read+" RENAME CONSTRAINT "+ident(tmp)+" TO "+ident(tc.name))
	}
	// The read's indexes, such as the _openetl_id one its dedupe needs,
	// become the target's like ones, or go.
	unused := slices.Clone(r.indexes)
	for _, ti := range t.indexes {
		if ti.using == "" {
			return nil, "it has an index whose definition can't be read back", nil
		}
		if i := slices.IndexFunc(unused, func(x relIndex) bool { return x.unique == ti.unique && x.using == ti.using }); i >= 0 {
			renames = append(renames, "ALTER INDEX "+qualified(unused[i].name)+" RENAME TO "+ident(ti.name))
			unused = slices.Delete(unused, i, i+1)
			continue
		}
		tmp := sqlstage.HashName("_openetl_swap_", s.Read, ti.name)
		unique := ""
		if ti.unique {
			unique = "UNIQUE "
		}
		if _, err := tx.Exec(ctx, "CREATE "+unique+"INDEX "+ident(tmp)+" ON "+read+" "+ti.using); err != nil {
			return nil, "", err
		}
		renames = append(renames, "ALTER INDEX "+qualified(tmp)+" RENAME TO "+ident(ti.name))
	}
	for _, x := range unused {
		if _, err := tx.Exec(ctx, "DROP INDEX "+qualified(x.name)); err != nil {
			return nil, "", err
		}
	}
	// So that the planner knows the rows as soon as they're the target's.
	after = append(after, "ANALYZE "+read)
	if r.owner != t.owner {
		after = append(after, "ALTER TABLE "+read+" OWNER TO "+t.owner)
	}
	if !equal(t.comment, r.comment) {
		after = append(after, "COMMENT ON TABLE "+read+" IS "+literal(t.comment))
	}
	if !slices.Equal(t.options, r.options) || !slices.Equal(t.toastOptions, r.toastOptions) {
		var change []string
		if len(r.options) > 0 || len(r.toastOptions) > 0 {
			change = append(change, "RESET ("+strings.Join(slices.Concat(optionNames(r.options, ""), optionNames(r.toastOptions, "toast.")), ", ")+")")
		}
		if len(t.options) > 0 || len(t.toastOptions) > 0 {
			set := slices.DeleteFunc([]string{options(t.options, ""), options(t.toastOptions, "toast.")}, func(s string) bool { return s == "" })
			change = append(change, "SET ("+strings.Join(set, ", ")+")")
		}
		after = append(after, "ALTER TABLE "+read+" "+strings.Join(change, ", "))
	}
	for _, q := range after {
		if _, err := tx.Exec(ctx, q); err != nil {
			return nil, "", err
		}
	}
	return renames, "", nil
}

// grantsQuery gives the GRANTs and REVOKEs that make the privileges on $2,
// table and column, those on $1, which its owner granted.
const grantsQuery = `WITH want AS (
		SELECT '' AS col, x.grantee, x.privilege_type AS priv, x.is_grantable AS g
		FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x WHERE c.oid = to_regclass($1)
		UNION ALL
		SELECT quote_ident(a.attname), x.grantee, x.privilege_type, x.is_grantable
		FROM pg_attribute a, aclexplode(a.attacl) x WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped),
	have AS (
		SELECT '' AS col, x.grantee, x.privilege_type AS priv, x.is_grantable AS g
		FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x WHERE c.oid = to_regclass($2)
		UNION ALL
		SELECT quote_ident(a.attname), x.grantee, x.privilege_type, x.is_grantable
		FROM pg_attribute a, aclexplode(a.attacl) x WHERE a.attrelid = to_regclass($2) AND a.attnum > 0 AND NOT a.attisdropped)
	SELECT format('GRANT %s%s ON %s TO %s%s', w.priv, CASE WHEN w.col <> '' THEN ' (' || w.col || ')' ELSE '' END, $2::text,
			CASE w.grantee WHEN 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(w.grantee)) END, CASE WHEN w.g THEN ' WITH GRANT OPTION' ELSE '' END)
		FROM want w WHERE NOT EXISTS (SELECT FROM have h WHERE h.col = w.col AND h.grantee = w.grantee AND h.priv = w.priv AND h.g >= w.g)
	UNION ALL
	SELECT format('REVOKE %s%s%s ON %s FROM %s',
			CASE WHEN EXISTS (SELECT FROM want w WHERE w.col = h.col AND w.grantee = h.grantee AND w.priv = h.priv) THEN 'GRANT OPTION FOR ' ELSE '' END,
			h.priv, CASE WHEN h.col <> '' THEN ' (' || h.col || ')' ELSE '' END, $2::text,
			CASE h.grantee WHEN 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(h.grantee)) END)
		FROM have h WHERE NOT EXISTS (SELECT FROM want w WHERE w.col = h.col AND w.grantee = h.grantee AND w.priv = h.priv AND w.g >= h.g)`

func equal(a, b *string) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }

func literal(s *string) string {
	if s == nil {
		return "NULL"
	}
	return quoteLiteral(*s)
}

// options is reloptions, each name=value, as SET takes them, with prefix
// before each name.
func options(opts []string, prefix string) string {
	out := make([]string, len(opts))
	for i, o := range opts {
		name, value, _ := strings.Cut(o, "=")
		out[i] = prefix + name + " = " + quoteLiteral(value)
	}
	return strings.Join(out, ", ")
}

func optionNames(opts []string, prefix string) []string {
	out := make([]string, len(opts))
	for i, o := range opts {
		name, _, _ := strings.Cut(o, "=")
		out[i] = prefix + name
	}
	return out
}
