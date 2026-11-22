package clickhouse

import (
	"cmp"
	"context"
	"crypto/tls"
	"database/sql"
	"slices"
	"strconv"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	"github.com/openetl-io/openetl/sdk/go/check/sqlcheck"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.CheckLister      = (*Source)(nil)
	_ source.Preflighter      = (*Source)(nil)
	_ source.Checker          = (*Source)(nil)
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
	_ destination.Checker     = (*Destination)(nil)
)

const configInvalidCode = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID

// The server version the connectors are tested on; an older one warns.
var testedVersion = [2]int{26, 8}

var (
	untested = check.Reason{
		Name: "version_untested", Code: configInvalidCode,
		Message: "The server's version is {found}, older than {required}, the one the connector is tested on.",
		Fix:     "Upgrade the server to {required} or later, or try the pipeline's reads and writes on this one first.",
	}
	badCursor = check.Reason{
		Name: "cursor_unsupported", Code: configInvalidCode,
		Message: "{stream}'s cursor {cursor} is of type {type}, which the source can't read incrementally by.",
		Fix:     "Name a column of an integer, float, Decimal, Date, Date32, DateTime, DateTime64, String or FixedString type as the cursor.",
	}
	noChangeLog = check.Reason{
		Name: "sync_mode_unsupported", Code: configInvalidCode,
		Message: "{stream} is read with {mode}, and ClickHouse keeps no change log to read.",
		Fix:     "Read {stream} with full_refresh or incremental.",
	}
	settingLocked = check.Reason{
		Name: "setting_locked", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "{user} can't change {settings}, which the destination sets on its queries.",
		Fix:     "Give {user} a settings profile with readonly = 0, or 2, and no constraint on {settings}.",
	}
	readonlyUser = check.Reason{
		Name: "user_readonly", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "{principal}'s settings profile has readonly = {readonly}, which lets it write nothing, whatever its grants.",
		Fix:     "Give {principal} a settings profile with readonly = 0.",
	}
	unversioned = check.Reason{
		Name: "seq_unversioned", Code: configInvalidCode,
		Message: "{stream}'s _openetl_seq is of type {type}, which can't version a ReplacingMergeTree, so of a key's rows the one inserted last wins, and a hard delete can't be applied.",
		Fix:     "Merge {stream} from a source whose seq is an integer, a date, a timestamp or a decimal of scale 0, or write it with append or overwrite.",
	}
	keyTypeChange = check.Reason{
		Name: "key_type_unchangeable", Code: configInvalidCode,
		Message: "{table}'s column {column} is {found}, in its sorting, primary or partition key, and {stream} has it as {type}, which ClickHouse can't change it to, so the destination refuses the change and the column's new values are held.",
		Fix:     "Write {stream} to another table, or create {table} again with {column} as {type}.",
	}
	dottedDatabase = check.Reason{
		Name: "database_name_dotted", Code: configInvalidCode,
		Message: "{database} has a dot in its name, so a patch of {stream}'s held cells fails: joinGet takes its table as database.table.",
		Fix:     "Write {stream} to a database whose name has no dot, or have the pipeline hold no cells of it.",
	}
)

// checkKit makes the checks' database handle from c, as the connectors
// connect but without a database, which a check looks for itself, and
// without the destination's settings, which settings.write checks.
func checkKit(c connConfig) *sqlcheck.Kit {
	c.Database = ""
	k := sqlcheck.New(func(context.Context) (*sql.DB, error) {
		db := ch.OpenDB(c.options(1, nil))
		db.SetMaxOpenConns(1)
		return db, nil
	})
	k.Classify = failure
	return k
}

// failure gives the server's errors their reasons; nil for one the kit's
// own classification fits.
func failure(err error) *check.Failure {
	e, ok := exception(err)
	if !ok {
		return nil
	}
	var f *check.Failure
	switch e.Code {
	case codeAuthenticationFailed, codeUnknownUser, codeWrongPassword, codeRequiredPassword:
		f = check.CredentialsRejected.Wrap(err)
	case codeAccessDenied:
		f = check.PrivilegeMissing.Wrap(err)
		f.Message = "The database user lacks a privilege this needs."
		f.Fix = "Grant the connector's database user the privilege the detail names."
	case codeReadonly, codeSettingConstraint:
		f = check.PermissionDenied.Wrap(err)
		f.Message = "The user's settings profile doesn't let it change a setting the connector sets."
		f.Fix = "Give the user a settings profile with readonly = 0, or 2, and no constraint on the setting the detail names."
	case codeUnknownDatabase:
		f = check.NotFound.Wrap(err, "resource", "The database")
	case codeUnknownTable:
		f = check.NotFound.Wrap(err, "resource", "The table")
	case codeQuotaExceeded:
		f = check.QuotaExceeded.Wrap(err)
	case codeTooManySimultaneous, codeTimeoutExceeded, codeMemoryLimitExceeded, codeSocketTimeout, codeNetworkError,
		codeKeeperException, codeAllConnectionTriesed, codeTooManyParts, codeTableIsReadOnly:
		f = check.Unavailable.Wrap(err)
	default:
		return nil
	}
	return f.With("clickhouse_error", strconv.Itoa(int(e.Code)))
}

// connectionChecks reach the server, agree on TLS as the config says, sign
// in, and look at its version: net.dns, net.connect, net.tls, db.connect
// and server.version. The native protocol's secure port starts with TLS, so
// netcheck checks it.
func connectionChecks(c connConfig, k *sqlcheck.Kit) []check.Check {
	checks := netcheck.Reach(c.Addr)
	if c.TLS {
		checks = netcheck.ReachTLS(c.Addr, &tls.Config{MinVersion: tls.VersionTLS12})
	}
	for i := range checks {
		checks[i].Field = "addr"
	}
	if c.TLS {
		checks[len(checks)-1].Field = "tls"
	}
	connect := k.Connect()
	connect.Kind, connect.Title, connect.Field = check.Authentication, "Sign in as {user}", "user"
	connect.Params = check.Params{"user": c.User}
	connect.Needs = []string{checks[len(checks)-1].Name}

	version := k.Check("server.version", check.Compatibility, "Check the server's version", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var v string
		if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&v); err != nil {
			return k.Failure(err)
		}
		p.Param("found", v)
		p.Param("required", strconv.Itoa(testedVersion[0])+"."+strconv.Itoa(testedVersion[1]))
		if older(v, testedVersion) {
			return check.Warn(untested.Fail())
		}
		p.Found("ClickHouse {found}.")
		return nil
	})
	return append(checks, connect, version)
}

// older reports whether version, such as 26.8.37.168, is before major.minor;
// false for one it can't read.
func older(version string, than [2]int) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return major < than[0] || major == than[0] && minor < than[1]
}

// checkNames gives each check a name from parts, made distinct from those it
// gave before: tables whose names differ only in case or punctuation get the
// same check.Name.
type checkNames map[string]int

func (n checkNames) name(parts ...string) string {
	name := check.Name(parts...)
	n[name]++
	if c := n[name]; c > 1 {
		name += "-" + strconv.Itoa(c)
	}
	return name
}

// granted reports whether the user has grant, such as SELECT ON db.t.
func granted(ctx context.Context, db *sql.DB, grant string) (bool, error) {
	var ok uint8
	if err := db.QueryRowContext(ctx, "CHECK GRANT "+grant).Scan(&ok); err != nil {
		return false, err
	}
	return ok == 1, nil
}

// principal is the signed-in user, as GRANT names it.
func principal(ctx context.Context, db *sql.DB) (string, error) {
	var user string
	if err := db.QueryRowContext(ctx, "SELECT currentUser()").Scan(&user); err != nil {
		return "", err
	}
	return ident(user), nil
}

// Checks reach the server, sign in, and look at the database and the
// tables the source offers.
func (s *Source) Checks() []check.Check {
	k := checkKit(s.cfg.connConfig)
	user, database := s.cfg.User, s.cfg.Database
	checks := connectionChecks(s.cfg.connConfig, k)

	missing := check.NotFound.Fail()
	missing.Message, missing.Fix = "{database} doesn't exist, or {user} can't see it.", "Correct database in the config, or grant {user} SELECT on its tables."
	found := k.True("database.found", "Find the database {database}", missing, missing,
		"SELECT count() > 0 FROM system.databases WHERE name = ?", database)
	found.Kind, found.Field, found.Params = check.Resource, "database", check.Params{"database": database, "user": user}
	checks = append(checks, found)
	readNeeds := []string{"database.found"}

	if len(s.cfg.Tables) > 0 {
		tables := k.Check("tables.found", check.Resource, "Find the tables in {database}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			ts, err := s.tablesOf(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			var gone []string
			for _, want := range s.cfg.Tables {
				if !slices.ContainsFunc(ts, func(t table) bool { return t.name == want }) {
					gone = append(gone, want)
				}
			}
			if len(gone) > 0 {
				p.Param("missing", strings.Join(gone, ", "))
				f := check.NotFound.Fail()
				f.Message, f.Fix = "{missing} isn't a table or view in {database} that {user} can see.", "Correct tables in the config, or grant {user} SELECT on what it names."
				return f
			}
			return nil
		})
		tables.Field, tables.Params, tables.Needs = "tables", check.Params{"database": database, "user": user}, []string{"database.found"}
		checks = append(checks, tables)
		readNeeds = []string{"tables.found"}
	}

	read := k.Check("tables.read", check.Permission, "Read the tables of {database}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		ts, err := s.tablesOf(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		var unreadable []string
		for _, t := range ts {
			ok, err := granted(ctx, db, "SELECT ON "+qualified(database, t.name))
			if err != nil {
				return k.Failure(err)
			}
			if !ok {
				unreadable = append(unreadable, qualified(database, t.name))
			}
		}
		if len(unreadable) > 0 {
			return selectMissing(ctx, k, db, unreadable...)
		}
		if len(ts) == 0 {
			f := check.NotFound.Fail()
			f.Message, f.Fix = "{database} has no table or view that {user} can see, so the source offers no stream.", "Grant {user} SELECT on the tables to read, or correct database in the config."
			return check.Warn(f)
		}
		p.Param("count", len(ts))
		p.Found("{user} can read every table the source offers, {count} in all.")
		return nil
	})
	read.Field, read.Params, read.Needs = "tables", check.Params{"database": database, "user": user}, readNeeds
	return withoutDeadline(append(checks, read))
}

// withoutDeadline has checks run as noDeadline does, so that a readonly
// user's pass.
func withoutDeadline(checks []check.Check) []check.Check {
	for i, c := range checks {
		if run := c.Run; run != nil {
			checks[i].Run = func(ctx context.Context, p *check.Probe) error { return run(noDeadline{ctx}, p) }
		}
	}
	return checks
}

// selectMissing is the failure of a user without SELECT on objects.
func selectMissing(ctx context.Context, k *sqlcheck.Kit, db *sql.DB, objects ...string) error {
	who, err := principal(ctx, db)
	if err != nil {
		return k.Failure(err)
	}
	f := check.PrivilegeMissing.Fail("principal", who, "privilege", "SELECT", "object", strings.Join(objects, ", "), "grants", grantsOn("SELECT", objects))
	f.Fix = "Run GRANT {grants} TO {principal};"
	return f
}

// grantsOn is privilege on each of objects, as one GRANT lists them:
// SELECT ON a, SELECT ON b. A GRANT's ON takes one object.
func grantsOn(privilege string, objects []string) string {
	grants := make([]string, len(objects))
	for i, o := range objects {
		grants[i] = privilege + " ON " + o
	}
	return strings.Join(grants, ", ")
}

// tablesOf lists the tables the source offers, as tables does.
func (s *Source) tablesOf(ctx context.Context, db *sql.DB) ([]table, error) {
	rows, err := db.QueryContext(ctx, tablesQuery, s.cfg.Database)
	if err != nil {
		return nil, err
	}
	return s.readTables(rows)
}

// streamTable is the table a stream reads, which the source offers and the
// user can select from.
func (s *Source) streamTable(ctx context.Context, k *sqlcheck.Kit, db *sql.DB, name string) (table, error) {
	ts, err := s.tablesOf(ctx, db)
	if err != nil {
		return table{}, k.Failure(err)
	}
	i := slices.IndexFunc(ts, func(t table) bool { return t.name == name })
	if i < 0 {
		f := check.NotFound.Fail()
		f.Message = "{stream} isn't a table or view in {database} that {user} can see, or tables in the config leaves it out."
		f.Fix = "Leave {stream} out of the pipeline, add it to tables in the config, or grant {user} SELECT on {table}."
		return table{}, f
	}
	ok, err := granted(ctx, db, "SELECT ON "+qualified(s.cfg.Database, name))
	if err != nil {
		return table{}, k.Failure(err)
	}
	if !ok {
		return table{}, selectMissing(ctx, k, db, qualified(s.cfg.Database, name))
	}
	return ts[i], nil
}

// Preflight looks at each stream as the pipeline reads it: there and
// readable, the order a full refresh reads it in, its cursor one the source
// reads by, and its sync mode one ClickHouse has.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := checkKit(s.cfg.connConfig)
	database := s.cfg.Database
	checks := []check.Check{k.Connect()}
	n := checkNames{}
	for _, st := range req.Streams {
		params := check.Params{"stream": st.Name, "database": database, "table": qualified(database, st.Name), "user": s.cfg.User}
		switch st.SyncMode {
		case source.CDC, source.Stream:
			mode := map[source.SyncMode]string{source.CDC: "cdc", source.Stream: "stream"}[st.SyncMode]
			c := check.Check{Name: n.name("stream", st.Name, "mode"), Kind: check.Configuration, Title: "Read {stream} with {mode}",
				Params: check.Params{"stream": st.Name, "mode": mode}, Run: func(context.Context, *check.Probe) error { return noChangeLog.Fail() }}
			checks = append(checks, c)
			continue
		}
		read := k.Check(n.name("stream", st.Name, "read"), check.Permission, "Read {stream}", func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
			_, err := s.streamTable(ctx, k, db, st.Name)
			return err
		})
		read.Params = params
		checks = append(checks, read)
		switch st.SyncMode {
		case source.FullRefresh:
			c := k.Check(n.name("stream", st.Name, "order"), check.Compatibility, "Read {stream} in sorting-key order", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
				t, err := s.streamTable(ctx, k, db, st.Name)
				if err != nil {
					return err
				}
				if len(t.key) == 0 {
					p.Found("{stream} has no sorting key that starts with plain columns, other than floats and UUIDs, that hold no null, so it's read in one query, which a stopped read starts again.")
					return nil
				}
				var key []string
				for _, i := range t.key {
					key = append(key, t.cols[i].name)
				}
				p.Param("key", strings.Join(key, ", "))
				p.Param("chunk_rows", s.cfg.ChunkRows)
				p.Found("{stream} is read in order of {key}, {chunk_rows} rows a query, and a stopped read resumes after its last chunk.")
				return nil
			})
			c.Params, c.Needs = params, []string{read.Name}
			checks = append(checks, c)
		case source.Incremental:
			c := k.Check(n.name("stream", st.Name, "cursor"), check.Compatibility, "Read {stream} incrementally", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
				if st.Cursor == "" {
					f := check.ConfigInvalid.Fail()
					f.Message, f.Fix = "{stream} is read incrementally, and names no cursor.", "Name one of {stream}'s columns as its cursor."
					return f
				}
				t, err := s.streamTable(ctx, k, db, st.Name)
				if err != nil {
					return err
				}
				i := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.name == st.Cursor })
				if i < 0 {
					f := check.NotFound.Fail()
					f.Message, f.Fix = "{stream} has no column {cursor}.", "Name one of {stream}'s columns as its cursor."
					return f
				}
				p.Param("type", t.cols[i].typ)
				if !cursorType(t.cols[i].typ) {
					return badCursor.Fail()
				}
				p.Found("{stream} is read by {cursor}, of type {type}; rows whose {cursor} is NULL, or NaN, are never read.")
				return nil
			})
			c.Params, c.Needs = check.Params{"stream": st.Name, "cursor": st.Cursor}, []string{read.Name}
			checks = append(checks, c)
		}
	}
	return withoutDeadline(checks)
}

// Check is Checks run, its first failure as an error.
func (s *Source) Check(ctx context.Context) error { return check.Run(ctx, s.Checks()).Err() }

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }

// Checks reach the server, sign in, and look at the default database: that
// the user can create it, has the grants writes need in it, and may change
// the settings the destination's queries set.
func (d *Destination) Checks() []check.Check {
	k := checkKit(d.cfg.connConfig)
	return append(connectionChecks(d.cfg.connConfig, k), databaseChecks(k, d.cfg.Database, d.cfg.User, "database")...)
}

// databaseGrants are what writes do in their database, each an item: stage,
// build, rename and drop tables, add and widen columns, move partitions
// into the target, replace and drop an overwritten one's, delete old
// markers, and fill held cells.
var databaseGrants = []struct {
	name, title string
	privileges  []string
}{
	{"grant.tables", "Create and drop tables in {database}", []string{"CREATE TABLE", "DROP TABLE"}},
	{"grant.rows", "Insert into and select from tables in {database}", []string{"INSERT", "SELECT"}},
	{"grant.columns", "Add and widen columns in {database}", []string{"ALTER ADD COLUMN", "ALTER MODIFY COLUMN"}},
	{"grant.partitions", "Move, replace and drop partitions in {database}", []string{"ALTER MOVE PARTITION", "ALTER DELETE"}},
	{"grant.update", "Fill held cells in {database}", []string{"ALTER UPDATE"}},
}

// The system tables the destination reads, for the parts it moves and the
// mutation it kills when a patch fails.
var systemTables = []string{"system.parts", "system.mutations"}

// databaseChecks: database exists or the user can create it, has the grants
// writes need in it, and can change the settings the destination's queries
// set. field is the config's, empty for a pipeline's namespace.
func databaseChecks(k *sqlcheck.Kit, database, user, field string) []check.Check {
	object := ident(database) + ".*"
	params := check.Params{"database": database, "user": user}
	create := k.Check("database.create", check.Permission, "Find {database}, or create it", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var n uint64
		if err := db.QueryRowContext(ctx, "SELECT count() FROM system.databases WHERE name = ?", database).Scan(&n); err != nil {
			return k.Failure(err)
		}
		if n > 0 {
			p.Found("{database} exists.")
			return nil
		}
		if err := missingGrants(ctx, k, db, []string{"CREATE DATABASE"}, object); err != nil {
			if f, ok := err.(*check.Failure); ok && f.Reason == check.PrivilegeMissing.Name {
				f.Message = "{database} doesn't exist, and {principal} has no {privilege} on {object} to create it."
				f.Fix = "Create the database, or run GRANT {privilege} ON {object} TO {principal};"
			}
			return err
		}
		p.Found("{database} doesn't exist, and the first write creates it.")
		return nil
	})
	create.Field, create.Params = field, params
	checks := []check.Check{create}
	for _, g := range databaseGrants {
		c := k.Check(g.name, check.Permission, g.title, func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
			return missingGrants(ctx, k, db, g.privileges, object)
		})
		c.Field, c.Params = field, params
		checks = append(checks, c)
	}

	system := k.Check("grant.system", check.Permission, "Read the parts and mutations of tables", func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
		var missing []string
		for _, t := range systemTables {
			rows, err := db.QueryContext(ctx, "SELECT 1 FROM "+t+" LIMIT 0")
			if err == nil {
				err = rows.Close()
			}
			switch {
			case isCode(err, codeAccessDenied):
				missing = append(missing, t)
			case err != nil:
				return k.Failure(err)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		who, err := principal(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		f := check.PrivilegeMissing.Fail("principal", who, "privilege", "SELECT", "object", strings.Join(missing, ", "), "grants", grantsOn("SELECT", missing))
		f.Fix = "Run GRANT {grants} TO {principal};"
		return f
	})
	system.Params = params

	settings := k.Check("settings.write", check.Compatibility, "Set what the destination's queries set", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "SELECT 1 SETTINGS async_insert = 0, mutations_sync = 2, join_use_nulls = 0, max_partitions_per_insert_block = 0")
		if isCode(err, codeReadonly, codeSettingConstraint) {
			return settingLocked.Wrap(err)
		}
		if err != nil {
			return k.Failure(err)
		}
		p.Found("{user} can set {settings}.")
		return nil
	})
	settings.Params = check.Params{"user": user, "settings": "async_insert, mutations_sync, join_use_nulls and max_partitions_per_insert_block"}
	settings.Description = "The destination sets async_insert = 0 and mutations_sync = 2 so a write is in a part, and a change done, before it goes on, and join_use_nulls = 0 for the joins a merge makes and max_partitions_per_insert_block = 0 when it builds an epoch's table, so that rows of any number of a table's partitions land."
	return append(checks, system, settings)
}

// missingGrants is the failure of a user without one of privileges on
// object, naming them, with the GRANT that gives them; nil if it has them.
func missingGrants(ctx context.Context, k *sqlcheck.Kit, db *sql.DB, privileges []string, object string) error {
	var readonly string
	if err := db.QueryRowContext(ctx, "SELECT toString(getSetting('readonly'))").Scan(&readonly); err != nil {
		return k.Failure(err)
	}
	if readonly != "0" {
		who, err := principal(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		return readonlyUser.Fail("principal", who, "readonly", readonly)
	}
	var missing []string
	for _, priv := range privileges {
		ok, err := granted(ctx, db, priv+" ON "+object)
		if err != nil {
			return k.Failure(err)
		}
		if !ok {
			missing = append(missing, priv)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	who, err := principal(ctx, db)
	if err != nil {
		return k.Failure(err)
	}
	f := check.PrivilegeMissing.Fail("principal", who, "privilege", strings.Join(missing, ", "), "object", object)
	f.Fix = "Run GRANT {privilege} ON {object} TO {principal};"
	return f
}

// Preflight looks at the pipeline's database, as Checks looks at the
// config's, and at each stream as it would be written: a merged one has a
// key, and a seq that can version its table; an existing table's key
// columns take the stream's types; and its held cells can be patched.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	k := checkKit(d.cfg.connConfig)
	database := cmp.Or(req.Namespace, d.cfg.Database)
	field := ""
	if req.Namespace == "" {
		field = "database"
	}
	checks := append([]check.Check{k.Connect()}, databaseChecks(k, database, d.cfg.User, field)...)
	n := checkNames{}
	for _, st := range req.Streams {
		schema := req.Schemas[st.Name]
		table := st.Name
		if schema != nil {
			table = targetTable(schema, st.Name)
		}
		c := k.Check(n.name("stream", st.Name, "write"), check.Compatibility, "Write {stream} to {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			return d.preflightStream(ctx, k, db, p, database, table, st, schema)
		})
		c.Params = check.Params{"stream": st.Name, "table": qualified(database, table), "database": database}
		c.Needs = []string{"database.create"}
		checks = append(checks, c)
	}
	return checks
}

func (d *Destination) preflightStream(ctx context.Context, k *sqlcheck.Kit, db *sql.DB, p *check.Probe, database, table string, st destination.Stream, schema *arrow.Schema) error {
	if st.Mode == destination.Merge && len(st.PrimaryKey) == 0 {
		f := check.ConfigInvalid.Fail()
		f.Message, f.Fix = "{stream} merges, and has no primary key.", "Give the stream a primary key, or write it with append or overwrite."
		return f
	}
	var cols []column
	if schema != nil {
		var key []string
		if st.Mode == destination.Merge {
			var err error
			if key, err = sqlstage.KeyColumns(schema, st.PrimaryKey); err != nil {
				f := check.ConfigInvalid.Wrap(err)
				f.Message, f.Fix = "{stream} merges on a column its schema doesn't have.", "Give the stream a primary key of its columns."
				return f
			}
		}
		var err error
		cols, err = columnsOf(schema, func(c string) bool { return isMetadata(c) || st.Mode == destination.Merge && slices.Contains(key, c) })
		if err != nil {
			f := check.ConfigInvalid.Wrap(err)
			f.Message, f.Fix = "{stream} has a column the destination can't write.", "Leave the column the detail names out of the stream."
			return f
		}
		cols = targetColumns(cols)
	}
	var warning error
	if st.Mode == destination.Merge && schema != nil {
		_, hasOp := indexOf(cols, opColumn)
		typ := "none"
		if i, ok := indexOf(cols, seqColumn); ok {
			typ, _ = unwrapNullable(cols[i].typ)
		}
		if !versionType(typ) && !decimalOfScale0(typ) {
			p.Param("type", typ)
			switch {
			case hasOp && st.Deletes == destination.HardDelete:
				return unversioned.Fail()
			case typ != "none":
				warning = check.Warn(unversioned.Fail())
			}
		}
	}
	if strings.Contains(database, ".") && warning == nil {
		warning = check.Warn(dottedDatabase.Fail())
	}
	have, err := tableColumns(ctx, db, database, table)
	if err != nil {
		return k.Failure(err)
	}
	if len(have) == 0 {
		p.Found("{table} doesn't exist, and the first write creates it.")
		return warning
	}
	for _, c := range cols {
		h, ok := have[c.name]
		if !ok || !h.keyed {
			continue
		}
		// As the writer's alter decides it.
		want, nullable := unwrapNullable(c.typ)
		was, wasNullable := unwrapNullable(h.typ)
		if nullable && !wasNullable {
			c.typ = want
		}
		if !widens(c.typ, h.typ) && canonical(want) != canonical(was) {
			p.Param("column", c.name)
			p.Param("found", h.typ)
			p.Param("type", c.typ)
			return check.Warn(keyTypeChange.Fail())
		}
	}
	p.Found("{table} exists, and its key's columns take the stream's types.")
	return warning
}

// decimalOfScale0 reports whether typ is a Decimal of scale 0, which a
// merge target's version is computed from.
func decimalOfScale0(typ string) bool {
	m := decimalSpec.FindStringSubmatch(typ)
	return m != nil && m[2] == "0"
}

type tableColumn struct {
	typ   string
	keyed bool
}

// tableColumns are a table's columns by name, none if it doesn't exist.
func tableColumns(ctx context.Context, db *sql.DB, database, table string) (map[string]tableColumn, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, type, toUInt8(is_in_sorting_key OR is_in_primary_key OR is_in_partition_key) FROM system.columns WHERE database = ? AND table = ?", database, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]tableColumn{}
	for rows.Next() {
		var name, typ string
		var keyed uint8
		if err := rows.Scan(&name, &typ, &keyed); err != nil {
			return nil, err
		}
		out[name] = tableColumn{typ: typ, keyed: keyed == 1}
	}
	return out, rows.Err()
}
