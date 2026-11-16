package oracle

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	go_ora "github.com/sijms/go-ora/v2"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/sqlcheck"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

var (
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
	_ destination.Checker     = (*Destination)(nil)
)

var (
	noQuota = check.Reason{
		Name: "quota_missing", Code: deniedCode,
		Message: "{user} has no quota on {tablespace}, its default tablespace, which holds the destination's staging and the tables of its own schema.",
		Fix:     "As SYSDBA in the container the dsn connects to, run ALTER USER {user} QUOTA UNLIMITED ON {tablespace};",
	}
	noSchema = check.Reason{
		Name: "privilege_missing", Code: deniedCode,
		Message: "The schema {schema} doesn't exist, and {user} has no CREATE USER to create it.",
		Fix:     "As SYSDBA in the container the dsn connects to, run GRANT CREATE USER TO {user}; or create the schema with CREATE USER \"{schema}\" NO AUTHENTICATION QUOTA UNLIMITED ON USERS;",
	}
	cantWrite = check.Reason{
		Name: "privilege_missing", Code: deniedCode,
		Message: "{user} can't write {stream} to {table}: it has no {missing}.",
		Fix:     "As SYSDBA in the container the dsn connects to, or as the table's owner, run {grant}",
	}
	badName = check.Reason{
		Name: "name_invalid", Code: configInvalidCode,
		Message: "{stream}'s {what} {name} {why}.",
		Fix:     "Rename it in the pipeline, which names the destination's tables and columns.",
	}
	noKey = check.Reason{
		Name: "primary_key_missing", Code: configInvalidCode,
		Message: "{stream} merges, and {why}.",
		Fix:     "Give {stream} a primary key in the pipeline, or write it with append or overwrite.",
	}
	typeRefused = check.Reason{
		Name: "type_change_refused", Code: configInvalidCode,
		Message: "{table} can't take {stream}'s column {column} as {to}: {reason}. The pipeline holds the column's values until it can.",
	}
)

// The privileges a user needs on another schema's table, and those that
// give each on every table.
var (
	tablePrivileges = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "ALTER", "INDEX"}
	anyPrivileges   = map[string]string{
		"SELECT": "SELECT ANY TABLE", "INSERT": "INSERT ANY TABLE", "UPDATE": "UPDATE ANY TABLE",
		"DELETE": "DELETE ANY TABLE", "ALTER": "ALTER ANY TABLE", "INDEX": "CREATE ANY INDEX",
	}
)

// kit makes the checks' database handle, with the source's reasons for the
// errors of signing in, which both roles share.
func (d *Destination) kit() (*sqlcheck.Kit, *Source) {
	dsn := d.cfg.DSN.Reveal()
	k := sqlcheck.New(func(context.Context) (*sql.DB, error) {
		return sql.OpenDB(sessionConnector{Connector: go_ora.NewConnector(dsn)}), nil
	})
	as := &Source{addr: d.addr, user: d.user, service: d.service}
	k.Classify = as.failure
	return k, as
}

// Checks reach the database, sign in, and look at what every pipeline
// needs: a version and COMPATIBLE that the destination's SQL runs on, and
// tables in the user's own schema, for its staging.
func (d *Destination) Checks() []check.Check {
	k, as := d.kit()
	return append(as.connectionChecks(k), d.serverChecks(k)...)
}

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }

func (d *Destination) serverChecks(k *sqlcheck.Kit) []check.Check {
	version := k.Check("server.version", check.Compatibility, "Run on Oracle Database 19c or later", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		_, v, _, err := server(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("found", v)
		if major(v) < 19 {
			f := check.UnsupportedVersion.Fail("required", "19c or later")
			f.Fix = "Upgrade the database to 19c or later, whose SQL/JSON functions the destination's merges use."
			return f
		}
		p.Found("The database is version {found}.")
		return nil
	})
	compatible := k.Check("server.compatible", check.Compatibility, "Take names of up to 128 bytes", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		_, _, c, err := server(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("found", c)
		if n, minor := major(c), minorOf(c); n < 12 || n == 12 && minor < 2 {
			f := check.SettingMismatch.Fail("setting", "COMPATIBLE", "required", "12.2 or later")
			f.Fix = "As SYSDBA, run ALTER SYSTEM SET COMPATIBLE = '19.0.0' SCOPE = SPFILE; and restart the database. COMPATIBLE can't be lowered again."
			return f
		}
		p.Param("booleans", "NUMBER(1), 1 for true")
		if major(c) >= 23 {
			p.Param("booleans", "BOOLEAN")
		}
		p.Found("COMPATIBLE is {found}: names take up to 128 bytes, and booleans are written as {booleans}.")
		return nil
	})
	create := k.True("privilege.create-table", "Create tables in {user}'s own schema",
		privilege(d.user, "CREATE TABLE", "the database", "GRANT CREATE TABLE TO {user};"), nil,
		`SELECT LEAST(COUNT(*), 1) FROM session_privs WHERE privilege IN ('CREATE TABLE', 'CREATE ANY TABLE')`)
	create.Description = "The destination stages each epoch's rows in tables of the user's own schema, and keeps its fence and markers there."
	quota := k.Check("quota.tablespace", check.Quota, "Store tables in {user}'s default tablespace", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var tablespace string
		var unlimited, quotas int
		err := db.QueryRowContext(ctx, `SELECT u.default_tablespace,
			(SELECT COUNT(*) FROM session_privs WHERE privilege = 'UNLIMITED TABLESPACE'),
			(SELECT COUNT(*) FROM user_ts_quotas q WHERE q.tablespace_name = u.default_tablespace AND (q.max_bytes = -1 OR q.max_bytes > q.bytes))
			FROM user_users u`).Scan(&tablespace, &unlimited, &quotas)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("tablespace", tablespace)
		if unlimited+quotas == 0 {
			return noQuota.Fail()
		}
		p.Found("{user} can store tables in {tablespace}.")
		return nil
	})
	user := check.Params{"user": d.user}
	for _, c := range []*check.Check{&version, &compatible, &create, &quota} {
		c.Params, c.Field = user, "dsn"
	}
	return []check.Check{version, compatible, create, quota}
}

// minorOf is a version's second number; 0 if it has none.
func minorOf(version string) int {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return 0
	}
	n, _ := strconv.Atoi(parts[1])
	return n
}

// Preflight looks at what a pipeline's streams need: the schema its tables
// go in, there or creatable; and each stream's table, writable or creatable,
// named as Oracle can, keyed if it merges, and able to take its columns'
// types, which it warns of if not, as the pipeline then holds the column.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	k, _ := d.kit()
	checks := append([]check.Check{k.Connect()}, d.serverChecks(k)[2:]...)
	schema := req.Namespace
	if schema == "" {
		schema = strings.ToUpper(d.user)
	}
	target := k.Check("schema.write", check.Permission, "Write tables to the schema {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		user, ns, err := d.schemaOf(ctx, db, req.Namespace)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("schema", ns)
		if ns == user {
			p.Found("{schema} is {user}'s own schema.")
			return nil
		}
		var exists, create int
		if err := db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM all_users WHERE username = :1),
			(SELECT COUNT(*) FROM session_privs WHERE privilege = 'CREATE USER') FROM dual`, ns).Scan(&exists, &create); err != nil {
			return k.Failure(err)
		}
		switch {
		case exists > 0:
			p.Found("{schema} exists.")
		case create > 0:
			p.Found("{schema} doesn't exist, and the destination creates it, as a user that can't sign in.")
		default:
			return noSchema.Fail()
		}
		return nil
	})
	target.Params, target.Field = check.Params{"schema": schema, "user": d.user}, "dsn"
	checks = append(checks, target)
	n := checkNames{}
	for _, st := range req.Streams {
		s := req.Schemas[st.Name]
		table := st.Name
		if s != nil {
			table = targetTable(s, st.Name)
		}
		params := check.Params{"stream": st.Name, "table": quoteIdent(schema) + "." + quoteIdent(table), "user": d.user}
		names := check.Check{Name: n.name("stream", st.Name, "names"), Kind: check.Configuration, Title: "Name {table} and its columns as Oracle can", Params: params,
			Run: func(context.Context, *check.Probe) error { return nameFailure(table, s) }}
		mode := check.Check{Name: n.name("stream", st.Name, "mode"), Kind: check.Configuration, Title: "Write {stream} with {mode}",
			Run: func(context.Context, *check.Probe) error {
				if st.Mode != destination.Merge {
					return nil
				}
				if len(st.PrimaryKey) == 0 {
					return noKey.Fail("why", "has no primary key")
				}
				if s != nil {
					if _, err := keyColumns(s, st.PrimaryKey); err != nil {
						return noKey.Fail("why", err.Error())
					}
				}
				return nil
			}}
		mode.Params = check.Params{"stream": st.Name, "table": params["table"], "user": d.user, "mode": modeOf(st.Mode)}
		write := k.Check(n.name("stream", st.Name, "write"), check.Permission, "Write {stream} to {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			user, ns, err := d.schemaOf(ctx, db, req.Namespace)
			if err != nil {
				return k.Failure(err)
			}
			rel := quoteIdent(ns) + "." + quoteIdent(table)
			p.Param("table", rel)
			if ns == user {
				return nil
			}
			missing, grant, err := writable(ctx, db, ns, table)
			if err != nil {
				return k.Failure(err)
			}
			if len(missing) > 0 {
				p.Param("missing", strings.Join(missing, ", "))
				p.Param("grant", strings.NewReplacer("{table}", rel, "{user}", d.user).Replace(grant))
				return cantWrite.Fail()
			}
			return nil
		})
		write.Params, write.Needs = params, []string{"schema.write"}
		types := k.Check(n.name("stream", st.Name, "types"), check.Compatibility, "Take {stream}'s columns into {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			if s == nil {
				return nil
			}
			_, ns, err := d.schemaOf(ctx, db, req.Namespace)
			if err != nil {
				return k.Failure(err)
			}
			_, _, c, err := server(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			var key []string
			if st.Mode == destination.Merge {
				key, _ = keyColumns(s, st.PrimaryKey)
			}
			cols, err := destColumns(s, key, major(c) >= 23)
			if err != nil {
				f := check.ConfigInvalid.Wrap(err)
				f.Message, f.Fix = "{stream} has a column the destination can't write.", "Leave the column out of the pipeline, or cast it to a type the destination maps."
				return f
			}
			w := &writer{db: db, schema: ns}
			have, err := w.catalog(ctx, ns, []string{table})
			if err != nil {
				return k.Failure(err)
			}
			if have[table] == nil {
				return nil
			}
			rel := quoteIdent(ns) + "." + quoteIdent(table)
			_, _, refused := plan(rel, have[table], cols, st.AllowTypeChanges)
			if len(refused) == 0 {
				return nil
			}
			r := refused[0]
			p.Param("table", rel)
			p.Param("column", r.Column)
			p.Param("to", r.ToType)
			p.Param("reason", r.Reason)
			f := typeRefused.Fail()
			f.Fix = r.Fix
			if f.Fix == "" {
				f.Fix = "Write the stream to a new table, or drop the column from " + rel + " if its values can go."
			}
			return check.Warn(f)
		})
		types.Params, types.Needs = params, []string{names.Name}
		checks = append(checks, names, mode, write, types)
	}
	return checks
}

// schemaOf is the session's user, and the schema a pipeline's tables go in:
// its namespace, or the user's own.
func (d *Destination) schemaOf(ctx context.Context, db *sql.DB, namespace string) (string, string, error) {
	var user string
	if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM dual").Scan(&user); err != nil {
		return "", "", err
	}
	return user, cmpOr(namespace, user), nil
}

// writable is the privileges the user lacks to write to schema's table,
// and the grant that gives them, naming the table {table}: on the table if
// it exists, else the ANY privileges, as a table another user creates in a
// schema isn't that user's.
func writable(ctx context.Context, db *sql.DB, schema, table string) ([]string, string, error) {
	have := map[string]bool{}
	rows, err := db.QueryContext(ctx, `SELECT privilege FROM session_privs
		UNION SELECT privilege FROM all_tab_privs WHERE table_schema = :1 AND table_name = :2
		  AND (grantee = USER OR grantee = 'PUBLIC' OR grantee IN (SELECT role FROM session_roles))`, schema, table)
	if err != nil {
		return nil, "", err
	}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			_ = rows.Close()
			return nil, "", err
		}
		have[p] = true
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	var exists int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2", schema, table).Scan(&exists); err != nil {
		return nil, "", err
	}
	var missing []string
	if exists == 0 {
		for _, p := range append([]string{"CREATE ANY TABLE"}, mapped(tablePrivileges)...) {
			if !have[p] {
				missing = append(missing, p)
			}
		}
		return missing, "GRANT " + strings.Join(missing, ", ") + " TO {user};", nil
	}
	for _, p := range tablePrivileges {
		if !have[p] && !have[anyPrivileges[p]] {
			missing = append(missing, p)
		}
	}
	return missing, "GRANT " + strings.Join(missing, ", ") + " ON {table} TO {user};", nil
}

func mapped(privileges []string) []string {
	out := make([]string, len(privileges))
	for i, p := range privileges {
		out[i] = anyPrivileges[p]
	}
	return out
}

// nameFailure says which of a table's names Oracle can't take; nil if it
// takes them all.
func nameFailure(table string, s *arrow.Schema) error {
	names := [][2]string{{"table", table}}
	if s != nil {
		for _, f := range s.Fields() {
			names = append(names, [2]string{"column", f.Name})
		}
	}
	for _, n := range names {
		why := ""
		switch {
		case validName(n[0], n[1]) != nil:
			why = "is empty or holds a double quote or NUL, which Oracle's names can't"
		case len(n[1]) > DestinationCapabilities.MaxIdentifierLength:
			why = "is " + strconv.Itoa(len(n[1])) + " bytes, and Oracle's names take up to 128"
		}
		if why != "" {
			return badName.Fail("what", n[0], "name", n[1], "why", why)
		}
	}
	return nil
}

func modeOf(m destination.WriteMode) string {
	switch m {
	case destination.Overwrite:
		return "overwrite"
	case destination.Merge:
		return "merge"
	}
	return "append"
}
