package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	go_ora "github.com/sijms/go-ora/v2"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	"github.com/openetl-io/openetl/sdk/go/check/sqlcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.CheckLister = (*Source)(nil)
	_ source.Preflighter = (*Source)(nil)
	_ source.Checker     = (*Source)(nil)
)

const (
	configInvalidCode = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID
	deniedCode        = protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
)

var (
	accountLocked = check.Reason{
		Name: "account_locked", Code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
		Message: "The account {user} is locked.",
		Fix:     "Run ALTER USER {user} ACCOUNT UNLOCK; as a DBA.",
	}
	tableUnreadable = check.Reason{
		Name: "table_unreadable", Code: deniedCode,
		Message: "{table} doesn't exist, or {user} has no SELECT on it.",
		Fix:     "Run GRANT SELECT ON {table} TO {user}; as its owner or a DBA, or correct the table's name.",
	}
	someUnreadable = check.Reason{
		Name: "table_unreadable", Code: deniedCode,
		Message: "{user} can't read {unreadable} in {schema}, which the source offers as streams.",
		Fix:     "Run GRANT SELECT ON {schema}.<table> TO {user}; for each, as its owner or a DBA, or name the tables to read in tables.",
	}
	noTables = check.Reason{
		Name: "no_tables", Code: configInvalidCode,
		Message: "{user} can read no table in {schema}.",
		Fix:     "Set schema to the schema that holds the tables, as Oracle stores its name, or grant {user} SELECT on them.",
	}
	viewsUnreadable = check.Reason{
		Name: "privilege_missing", Code: deniedCode,
		Message: "{user} can't read {views}, which LogMiner's reads use.",
	}
	noCDC = check.Reason{
		Name: "cdc_unsupported", Code: configInvalidCode,
		Message: "{stream} can't be read with cdc: {why}.",
		Fix:     "Read {stream} with full_refresh or incremental.",
	}
	noCursor = check.Reason{
		Name: "cursor_missing", Code: configInvalidCode,
		Message: "{stream} is read incrementally, and has no column {cursor} to read it by.",
		Fix:     "Name one of its columns as the stream's cursor.",
	}
	badCursor = check.Reason{
		Name: "cursor_unsupported", Code: configInvalidCode,
		Message: "{stream}'s cursor {cursor} is of type {type}, which the source can't read incrementally by.",
		Fix:     "Name a NUMBER, VARCHAR2, CHAR, NVARCHAR2, NCHAR, RAW, DATE or TIMESTAMP column as the cursor, with or without a time zone.",
	}
	badMode = check.Reason{
		Name: "sync_mode_unsupported", Code: configInvalidCode,
		Message: "{stream} is read with {mode}, which the source doesn't have.",
		Fix:     "Read {stream} with full_refresh, incremental or cdc.",
	}
	notAllColumns = check.Reason{
		Name: "supplemental_logging_missing", Code: configInvalidCode,
		Message: "The redo log doesn't hold all of {table}'s columns for its changes, which a cdc read needs.",
		Fix:     "Run ALTER TABLE {table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS; as its owner or a user with ALTER on it.",
	}
	noArchives = check.Reason{
		Name: "archived_log_missing", Code: configInvalidCode,
		Message: "The database has no archived redo log on disk, so a read can't continue from before the online logs.",
		Fix:     "Check that the database archives its redo log, to a destination it keeps the files in.",
	}
	shortRetention = check.Reason{
		Name: "retention_short", Code: protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED,
		Message: "The archived redo log reaches back {reach}, less than twice the {interval} between the pipeline's syncs, so a late sync can find the redo it continues from gone.",
		Fix:     "Keep archived logs longer, with RMAN's CONFIGURE ARCHIVELOG DELETION POLICY or a larger DB_RECOVERY_FILE_DEST_SIZE, or sync more often.",
	}
)

// cdcOnly describes the items only a cdc read needs.
const cdcOnly = "Needed only to read tables with cdc: a pipeline's preflight fails without it if it reads one so."

// cdcViews are the views LogMiner's reads select from.
var cdcViews = []string{"V$DATABASE", "V$LOG", "V$LOGFILE", "V$ARCHIVED_LOG", "V$LOGMNR_CONTENTS", "V$TRANSACTION"}

// kit makes the checks' database handle. Each list has its own, so a run
// opens one and closes it.
func (s *Source) kit() *sqlcheck.Kit {
	dsn := s.cfg.DSN.Reveal()
	k := sqlcheck.New(func(context.Context) (*sql.DB, error) {
		return sql.OpenDB(sessionConnector{Connector: go_ora.NewConnector(dsn)}), nil
	})
	k.Classify = s.failure
	return k
}

// failure gives the server's ORA- errors their reasons; nil for other
// errors, which the kit classifies.
func (s *Source) failure(err error) *check.Failure {
	code := oraCode(err)
	switch code {
	case 0:
		return nil
	case 1017:
		return check.CredentialsRejected.Wrap(err)
	case 28000:
		return accountLocked.Wrap(err, "user", s.user)
	case 28001:
		f := check.CredentialsExpired.Wrap(err, "user", s.user)
		f.Message = "The password of {user} has expired."
		f.Fix = "Run ALTER USER {user} IDENTIFIED BY a new password, as {user} or a DBA, and update the dsn's secret."
		return f
	case 12514, 12505:
		f := check.NotFound.Wrap(err, "resource", "The service "+s.service+" at "+s.addr)
		f.Fix = "Correct the service in the dsn, oracle://user:password@host:port/service. lsnrctl services, run on the database's host, lists the services the listener knows, such as FREEPDB1."
		return f
	case 1045:
		f := check.PrivilegeMissing.Wrap(err, "principal", s.user, "privilege", "CREATE SESSION", "object", "the database")
		f.Fix = "Run GRANT CREATE SESSION TO {principal}; as a DBA."
		return f
	case 942:
		f := check.PrivilegeMissing.Wrap(err, "user", s.user)
		f.Message = "{user} can't read a table or view this needs, or it doesn't exist."
		f.Fix = "Grant {user} SELECT on the table or view the detail names. For a view V$X, run GRANT SELECT ON SYS.V_$X TO {user}; as SYSDBA."
		return f
	case 1031:
		f := check.PrivilegeMissing.Wrap(err)
		f.Message = "The database user lacks a privilege this needs."
		f.Fix = "Grant the connector's database user the privilege the detail names."
		return f
	}
	return check.Classify(classify(err))
}

// probe runs a query and reads its first row, if it has one: some errors
// come only then.
func probe(ctx context.Context, q queryer, query string, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	rows.Next()
	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}

// schemaName is the schema the source reads, as far as the config says:
// without one, the user's current schema, which is the user's own unless a
// logon trigger changes it.
func (s *Source) schemaName() string {
	if s.cfg.Schema != "" {
		return s.cfg.Schema
	}
	return strings.ToUpper(s.user)
}

// checkNames gives each check a name from parts, made distinct from those it
// gave before: names that differ only in case or punctuation get the same
// check.Name.
type checkNames map[string]int

func (n checkNames) name(parts ...string) string {
	name := check.Name(parts...)
	n[name]++
	if c := n[name]; c > 1 {
		name += "-" + strconv.Itoa(c)
	}
	return name
}

// connectionChecks reach the listener and sign in: net.dns, net.connect and
// db.connect, or db.connect alone for a dsn that names no host.
func (s *Source) connectionChecks(k *sqlcheck.Kit) []check.Check {
	connect := k.Connect()
	connect.Kind, connect.Field = check.Authentication, "dsn"
	connect.Title = "Sign in to {service} as {user}"
	if s.service == "" {
		connect.Title = "Sign in as {user}"
	}
	connect.Params = check.Params{"service": s.service, "user": s.user, "address": s.addr}
	if s.addr == "" {
		return []check.Check{connect}
	}
	checks := netcheck.Reach(s.addr)
	for i := range checks {
		checks[i].Field = "dsn"
	}
	connect.Needs = []string{"net.connect"}
	return append(checks, connect)
}

// Checks reach the database, sign in, and look at the schema and its
// tables, and at what cdc needs, which only warns: whether a pipeline reads
// with cdc is its preflight's to say.
func (s *Source) Checks() []check.Check {
	k := s.kit()
	checks := s.connectionChecks(k)
	exists := k.Check("schema.exists", check.Resource, "Find the schema {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		schema, err := s.owner(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("schema", schema)
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM all_users WHERE username = :1`, schema).Scan(&n); err != nil {
			return k.Failure(err)
		}
		if n == 0 {
			f := check.NotFound.Fail("resource", "The schema "+schema)
			f.Fix = "Set schema to a schema that exists, as Oracle stores its name, which is usually in upper case."
			return f
		}
		return nil
	})
	exists.Params, exists.Field = check.Params{"schema": s.schemaName()}, "schema"
	checks = append(checks, exists)
	if len(s.cfg.Tables) > 0 {
		n := checkNames{}
		for _, t := range s.cfg.Tables {
			checks = append(checks, s.tableCheck(k, n.name("table", t, "read"), t))
		}
	} else {
		checks = append(checks, s.tablesCheck(k))
	}
	for _, c := range s.cdcChecks(k) {
		c.Advisory = true
		checks = append(checks, c)
	}
	return checks
}

// Check is Checks run, its first failure as an error.
func (s *Source) Check(ctx context.Context) error { return check.Run(ctx, s.Checks()).Err() }

// tableCheck: the user can select from a table the config names.
func (s *Source) tableCheck(k *sqlcheck.Kit, name, table string) check.Check {
	c := k.Check(name, check.Permission, "Read {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		schema, err := s.owner(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		rel := quoteIdent(schema) + "." + quoteIdent(table)
		p.Param("table", rel)
		err = probe(ctx, db, "SELECT * FROM "+rel+" WHERE 1=0")
		if c := oraCode(err); c == 942 || c == 1031 {
			return tableUnreadable.Wrap(err)
		}
		return k.Failure(err)
	})
	c.Params = check.Params{"table": quoteIdent(s.schemaName()) + "." + quoteIdent(table), "user": s.user}
	c.Field, c.Needs = "tables", []string{"schema.exists"}
	return c
}

// tablesCheck: the user can select from the tables the source offers, and
// there's at least one.
func (s *Source) tablesCheck(k *sqlcheck.Kit) check.Check {
	c := k.Check("tables.read", check.Permission, "Read the tables of {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		schema, err := s.owner(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("schema", schema)
		tables, err := describe(ctx, db, schema, nil)
		if err != nil {
			return k.Failure(err)
		}
		var readable, unreadable []string
		for _, t := range tables {
			err := probe(ctx, db, "SELECT * FROM "+quoteIdent(schema)+"."+quoteIdent(t.name)+" WHERE 1=0")
			switch c := oraCode(err); {
			case err == nil:
				readable = append(readable, t.name)
			case c == 942 || c == 1031:
				unreadable = append(unreadable, t.name)
			default:
				return k.Failure(err)
			}
		}
		p.Param("tables", listed(readable))
		switch {
		case len(unreadable) > 0:
			p.Param("unreadable", listed(unreadable))
			return check.Warn(someUnreadable.Fail())
		case len(readable) == 0:
			return check.Warn(noTables.Fail())
		}
		p.Found("Tables {user} can read in {schema}: {tables}.")
		return nil
	})
	c.Params = check.Params{"schema": s.schemaName(), "user": s.user}
	c.Field, c.Needs = "schema", []string{"schema.exists"}
	return c
}

// listed names the first 20 of names, and how many more there are.
func listed(names []string) string {
	if len(names) <= 20 {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:20], ", "), len(names)-20)
}

// passed reports whether a check's Run passed.
func passed(err error) bool {
	f, ok := err.(*check.Failure)
	return err == nil || ok && f == nil
}

// cdcChecks are what a cdc read needs of the database and the user: the
// redo log archived, minimal supplemental logging, LogMiner's privileges
// and views, and then a LogMiner session that starts, which runs only once
// the others have passed, not warned.
func (s *Source) cdcChecks(k *sqlcheck.Kit) []check.Check {
	user := check.Params{"user": s.user}
	archive := k.Value("cdc.archivelog", "Archive the redo log", func(v string) error {
		if v != "ARCHIVELOG" {
			f := check.SettingMismatch.Fail("setting", "The database's log mode", "required", "ARCHIVELOG")
			f.Fix = "As SYSDBA in CDB$ROOT, with a fast recovery area set (DB_RECOVERY_FILE_DEST and DB_RECOVERY_FILE_DEST_SIZE), run SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN; and then ALTER PLUGGABLE DATABASE ALL OPEN;"
			return f
		}
		return nil
	}, `SELECT log_mode FROM v$database`)
	supp := k.Value("cdc.supplemental-logging", "Log the columns that identify a changed row", func(v string) error {
		if v != "YES" && v != "IMPLICIT" {
			f := check.SettingMismatch.Fail("setting", "The database's minimal supplemental logging", "required", "YES")
			f.Fix = "As SYSDBA in CDB$ROOT, run ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;"
			return f
		}
		return nil
	}, `SELECT supplemental_log_data_min FROM v$database`)
	for _, c := range []*check.Check{&archive, &supp} {
		run := c.Run
		c.Run = func(ctx context.Context, p *check.Probe) error {
			err := run(ctx, p)
			if oraCode(err) == 942 {
				return privilege(s.user, "SELECT", "V$DATABASE", "GRANT SELECT ON SYS.V_$DATABASE TO {user};")
			}
			return err
		}
		c.Params = user
	}
	mining := k.True("cdc.privilege.logmining", "Mine the redo log as {user}", privilege(s.user, "LOGMINING", "the database", "GRANT LOGMINING TO {user};"), nil,
		`SELECT count(*) FROM session_privs WHERE privilege = 'LOGMINING'`)
	pkg := k.True("cdc.privilege.dbms-logmnr", "Run SYS.DBMS_LOGMNR as {user}", privilege(s.user, "EXECUTE", "SYS.DBMS_LOGMNR", "GRANT EXECUTE ON SYS.DBMS_LOGMNR TO {user};"), nil,
		`SELECT count(*) FROM all_objects WHERE owner = 'SYS' AND object_name = 'DBMS_LOGMNR' AND object_type = 'PACKAGE'`)
	mining.Params, mining.Field = user, "dsn"
	pkg.Params, pkg.Field = user, "dsn"
	views := k.Check("cdc.privilege.views", check.Permission, "Read LogMiner's views as {user}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var missing, grants []string
		for _, v := range cdcViews {
			err := probe(ctx, db, "SELECT * FROM "+v+" WHERE 1=0")
			switch oraCode(err) {
			case 0:
				if err != nil {
					return k.Failure(err)
				}
			case 1306:
				// V$LOGMNR_CONTENTS outside a LogMiner session: it's readable.
			case 942:
				missing = append(missing, v)
				grants = append(grants, "GRANT SELECT ON SYS."+strings.Replace(v, "V$", "V_$", 1)+" TO {user};")
			default:
				return k.Failure(err)
			}
		}
		if len(missing) > 0 {
			p.Param("views", strings.Join(missing, ", "))
			f := viewsUnreadable.Fail()
			f.Fix = "As SYSDBA in the container the dsn connects to, run " + strings.Join(grants, " ")
			return f
		}
		p.Found("{user} can read " + strings.Join(cdcViews, ", ") + ".")
		return nil
	})
	views.Params, views.Field = user, "dsn"
	prior := []check.Check{archive, supp, mining, pkg, views}
	var mu sync.Mutex
	ok := map[string]bool{}
	var needs []string
	for i := range prior {
		run, name := prior[i].Run, prior[i].Name
		prior[i].Run = func(ctx context.Context, p *check.Probe) error {
			err := run(ctx, p)
			mu.Lock()
			ok[name] = passed(err)
			mu.Unlock()
			return err
		}
		needs = append(needs, name)
	}
	start := k.Check("cdc.logminer", check.Compatibility, "Start a LogMiner session", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		mu.Lock()
		for _, n := range needs {
			if !ok[n] {
				mu.Unlock()
				return check.Skip("needs %s, which didn't pass", n)
			}
		}
		mu.Unlock()
		conn, err := db.Conn(ctx)
		if err != nil {
			return k.Failure(err)
		}
		defer func() { _ = conn.Close() }()
		scn, err := currentSCN(ctx, conn)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("scn", scn)
		if err := startLogMiner(ctx, conn, scn, 0); err != nil {
			return classify(err)
		}
		endLogMiner(context.WithoutCancel(ctx), conn)
		p.Found("A LogMiner session started at SCN {scn}.")
		return nil
	})
	start.Needs = needs
	checks := append(prior, start)
	for i := range checks {
		checks[i].Description = cdcOnly
	}
	return checks
}

// privilege is the failure of a user without a privilege, which fix, run
// as SYSDBA, grants.
func privilege(user, priv, object, fix string) *check.Failure {
	f := check.PrivilegeMissing.Fail("principal", user, "privilege", priv, "object", object)
	f.Fix = "As SYSDBA in the container the dsn connects to, run " + fix
	return f
}

// Preflight looks at each stream as the pipeline reads it: there and
// readable, in a sync mode its table has, by a cursor the source reads by;
// and for cdc, what the database and the user need, the table's
// supplemental logging and a flashback read of it, and whether the archived
// redo log outlasts the schedule.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := s.kit()
	checks := []check.Check{k.Connect()}
	cdc := slices.ContainsFunc(req.Streams, func(cs source.ConfiguredStream) bool { return cs.SyncMode == source.CDC })
	if cdc {
		checks = append(checks, s.cdcChecks(k)...)
		checks = append(checks, retentionCheck(k, req.Interval))
	}
	// The schema's tables, described once for every stream.
	var mu sync.Mutex
	var tables []table
	var schema string
	var described error
	lookup := func(ctx context.Context, db *sql.DB) ([]table, string, error) {
		mu.Lock()
		defer mu.Unlock()
		if tables == nil && described == nil {
			if schema, described = s.owner(ctx, db); described == nil {
				tables, described = describe(ctx, db, schema, nil)
			}
		}
		return tables, schema, described
	}
	n := checkNames{}
	for _, cs := range req.Streams {
		params := check.Params{"stream": cs.Name, "table": quoteIdent(s.schemaName()) + "." + quoteIdent(cs.Name), "user": s.user}
		read := k.Check(n.name("stream", cs.Name, "read"), check.Permission, "Read {stream}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			tables, schema, err := lookup(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			rel := quoteIdent(schema) + "." + quoteIdent(cs.Name)
			p.Param("table", rel)
			p.Param("schema", schema)
			_, found := findTable(tables, cs.Name)
			if !found || len(s.cfg.Tables) > 0 && !slices.Contains(s.cfg.Tables, cs.Name) {
				f := check.NotFound.Fail()
				f.Message = "{stream} isn't a table of {schema} that the source offers."
				f.Fix = "Grant {user} SELECT on {table}, or add it to tables in the config."
				return f
			}
			err = probe(ctx, db, "SELECT * FROM "+rel+" WHERE 1=0")
			if c := oraCode(err); c == 942 || c == 1031 {
				return tableUnreadable.Wrap(err)
			}
			return k.Failure(err)
		})
		read.Params = params
		mode := k.Check(n.name("stream", cs.Name, "mode"), check.Compatibility, "Read {stream} with {mode}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			tables, _, err := lookup(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			t, _ := findTable(tables, cs.Name)
			return modeFailure(t, cs, p)
		})
		mode.Params, mode.Needs = check.Params{"stream": cs.Name, "mode": modeName(cs.SyncMode), "cursor": cs.Cursor}, []string{read.Name}
		checks = append(checks, read, mode)
		if cs.SyncMode != source.CDC {
			continue
		}
		supp := k.Check(n.name("stream", cs.Name, "supplemental-logging"), check.Compatibility, "Log every column of {table}'s changes", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			_, schema, err := lookup(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			p.Param("table", quoteIdent(schema)+"."+quoteIdent(cs.Name))
			var all string
			err = db.QueryRowContext(ctx, `SELECT CASE WHEN d.supplemental_log_data_all = 'YES' OR EXISTS (
			  SELECT 1 FROM all_log_groups g WHERE g.owner = :1 AND g.table_name = :2 AND g.log_group_type = 'ALL COLUMN LOGGING')
			  THEN 'YES' ELSE 'NO' END FROM v$database d`, schema, cs.Name).Scan(&all)
			switch {
			case err != nil:
				return k.Failure(err)
			case all != "YES":
				return notAllColumns.Fail()
			}
			return nil
		})
		supp.Params, supp.Needs = params, []string{mode.Name, "cdc.privilege.views"}
		flashback := k.Check(n.name("stream", cs.Name, "flashback"), check.Permission, "Read {table} as of an SCN", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			_, schema, err := lookup(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			rel := quoteIdent(schema) + "." + quoteIdent(cs.Name)
			p.Param("table", rel)
			scn, err := currentSCN(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			err = probe(ctx, db, "SELECT 1 FROM "+rel+" AS OF SCN :1 WHERE 1=0", scn)
			switch oraCode(err) {
			case 1031, 41900:
				f := check.PrivilegeMissing.Wrap(err, "principal", s.user, "privilege", "FLASHBACK", "object", rel)
				f.Fix = "Run GRANT FLASHBACK ON {table} TO {user}; as its owner or a DBA. Only another schema's tables need it."
				return f
			case 1466:
				// Its definition changed after the SCN: the privilege is there.
				return nil
			}
			return k.Failure(err)
		})
		flashback.Params, flashback.Needs = params, []string{read.Name, "cdc.privilege.views"}
		checks = append(checks, supp, flashback)
	}
	return checks
}

// modeFailure says why t can't be read as cs says, nil if it can.
func modeFailure(t table, cs source.ConfiguredStream, p *check.Probe) error {
	switch cs.SyncMode {
	case 0, source.FullRefresh:
		return nil
	case source.Incremental:
		ci := slices.IndexFunc(t.cols, func(c column) bool { return c.name == cs.Cursor })
		if ci < 0 {
			return noCursor.Fail()
		}
		if c := t.cols[ci]; !orderable(c) {
			p.Param("type", c.dataType)
			return badCursor.Fail()
		}
		return nil
	case source.CDC:
		why := t.noCDC
		if why == "" && !chunked(t) {
			why = "its key has a column of a type the source can't order by, such as FLOAT, BINARY_DOUBLE or an INTERVAL"
		}
		if why != "" {
			p.Param("why", why)
			return noCDC.Fail()
		}
		return nil
	}
	return badMode.Fail()
}

func modeName(m source.SyncMode) string {
	switch m {
	case 0, source.FullRefresh:
		return "full_refresh"
	case source.Incremental:
		return "incremental"
	case source.CDC:
		return "cdc"
	case source.Stream:
		return "stream"
	}
	return strconv.Itoa(int(m))
}

// retentionCheck warns when the archived redo log on disk reaches back less
// than twice the longest time between the pipeline's syncs: a sync that
// comes late then finds the redo it continues from gone.
func retentionCheck(k *sqlcheck.Kit, interval time.Duration) check.Check {
	c := k.Check("cdc.retention", check.Quota, "Keep the archived redo log between syncs", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		if interval <= 0 {
			p.Found("The pipeline has no regular schedule to keep the log for.")
			return nil
		}
		p.Param("interval", interval.String())
		var secs sql.NullFloat64
		err := db.QueryRowContext(ctx, `SELECT (SYSDATE - min(first_time)) * 86400 FROM v$archived_log
		  WHERE status = 'A' AND deleted = 'NO' AND standby_dest = 'NO'`).Scan(&secs)
		switch {
		case err != nil:
			return k.Failure(err)
		case !secs.Valid:
			return noArchives.Fail()
		}
		reach := time.Duration(secs.Float64 * float64(time.Second)).Round(time.Minute)
		p.Param("reach", reach.String())
		if reach < 2*interval {
			return shortRetention.Fail()
		}
		p.Found("The archived redo log reaches back {reach}, at least twice the {interval} between syncs.")
		return nil
	})
	c.Advisory, c.Needs = true, []string{"cdc.privilege.views"}
	c.Description = "Oracle deletes archived logs as RMAN's deletion policy or the recovery area's size says, not as the pipeline reads them."
	return c
}
