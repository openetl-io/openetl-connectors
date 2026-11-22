package sqlserver

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

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
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
)

var (
	databaseClosed = check.Reason{
		Name:    "database_unavailable",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The login can't open the database {database}: it doesn't exist, or the login has no user in it.",
		Fix:     "Correct the database in dsn, or create a user for the login in it: CREATE USER ... FOR LOGIN ...;",
	}
	noKey = check.Reason{
		Name:    "primary_key_missing",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table} has no primary key, and cdc reads only tables with one.",
		Fix:     "Give {table} a primary key, or read it with full_refresh or incremental.",
	}
	variantKey = check.Reason{
		Name:    "primary_key_variant",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table}'s primary key has a sql_variant column, whose values arrive as text, which can't tell 1 from N'1', so the source reads it as a table without a key, and cdc reads only tables with one.",
		Fix:     "Read {table} with full_refresh or incremental, or give it a primary key of other types.",
	}
	notTracked = check.Reason{
		Name:    "change_tracking_off",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "Change tracking is off for {table}, or {principal} lacks VIEW CHANGE TRACKING on it.",
		Fix:     "Run ALTER TABLE {table} ENABLE CHANGE_TRACKING; and GRANT VIEW CHANGE TRACKING ON {table} TO {principal};",
	}
	notCaptured = check.Reason{
		Name:    "change_data_capture_off",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table} has no capture instance that {principal} can read.",
		Fix:     "Run EXEC sys.sp_cdc_enable_table @source_schema = N'{schema}', @source_name = N'{name}', @role_name = NULL; and GRANT SELECT ON SCHEMA::cdc TO {principal};",
	}
	otherCodePage = check.Reason{
		Name:    "code_page_differs",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{column} of {table} is a char, varchar or text column in a collation whose code page isn't the database's: change data capture keeps only the characters the database's code page has, and writes ? in the change table for the rest.",
		Fix:     "Make {column} nchar or nvarchar, or give it the database's collation, or read {table} with change tracking.",
	}
	badCursor = check.Reason{
		Name:    "cursor_invalid",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table} has no column {cursor} of a type an incremental read orders by.",
		Fix:     "Choose a cursor column of a number, date, datetime, text or rowversion type.",
	}
	retentionShort = check.Reason{
		Name:    "retention_short",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The database keeps changes for {retention}, and the pipeline can go {interval} between syncs.",
		Fix:     "Sync more often, or keep changes longer: {setting}.",
	}
	typeChange = check.Reason{
		Name:    "type_change_refused",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{column} of {table} is {from}, the stream's is {to}, and the pipeline doesn't allow type changes, so its values are held.",
		Fix:     "Run {statement}, or allow type changes in the pipeline.",
	}
	indexedChange = check.Reason{
		Name:    "type_change_indexed",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{column} of {table} is {from}, the stream's is {to}, and SQL Server changes a column in your index {index} only to a longer length, so its values are held.",
		Fix:     "Drop {index}, change {column} to {to}, and create {index} again.",
	}
	unencrypted = check.Reason{
		Name:    "connection_unencrypted",
		Code:    protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "The connection may not be encrypted after sign-in: the dsn's encrypt doesn't require TLS, and the login can't see that the server forces it.",
		Fix:     "Add encrypt=true to the dsn, with certificate or hostNameInCertificate to verify the server's certificate.",
	}
	tooWide = check.Reason{
		Name:    "too_many_columns",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{table} would have {columns} columns, and SQL Server takes 1024.",
		Fix:     "Leave columns out of the stream.",
	}
)

// server is what checks say of the server a DSN names.
type server struct {
	dsn string
	// host:port; empty for a named instance without a port, which SQL Server
	// Browser resolves.
	address   string
	database  string
	principal string
	// The dsn's encrypt is true or strict: TLS for the whole connection, or
	// no sign-in.
	encrypted bool
	// TLS, the sign-in's at least, checks the server's certificate.
	verified bool
}

// serverOf reads a DSN that NewSource or NewDestination has parsed.
func serverOf(dsn string) server {
	sv := server{dsn: dsn}
	cfg, _ := msdsn.Parse(dsn)
	sv.database = cfg.Database
	sv.principal = cmp.Or(cfg.User, "the connector's login")
	sv.encrypted = cfg.Encryption == msdsn.EncryptionRequired || cfg.Encryption == msdsn.EncryptionStrict
	sv.verified = cfg.TLSConfig != nil && (!cfg.TLSConfig.InsecureSkipVerify || cfg.TLSConfig.VerifyPeerCertificate != nil)
	if cfg.Port != 0 || cfg.Instance == "" {
		port := cfg.Port
		if port == 0 {
			port = 1433
		}
		sv.address = net.JoinHostPort(cfg.Host, strconv.FormatUint(port, 10))
	}
	return sv
}

// kit makes the checks of one list, which share a handle the run opens and
// closes.
func (sv server) kit() *sqlcheck.Kit {
	k := sqlcheck.New(func(context.Context) (*sql.DB, error) { return openDB(sv.dsn) })
	k.Classify = sv.failure
	return k
}

// failure gives the server's errors their reasons.
func (sv server) failure(err error) *check.Failure {
	if strings.Contains(err.Error(), "TLS Handshake") {
		f := check.TLSFailed.Wrap(err, "address", cmp.Or(sv.address, "the server"))
		f.Fix = "Check the dsn's encrypt, and that the server's certificate is one the dsn's certificate or trustServerCertificate trusts, naming its host or hostNameInCertificate."
		return f
	}
	me, ok := errors.AsType[mssql.Error](err)
	if !ok {
		return nil
	}
	var f *check.Failure
	switch me.Number {
	case 18456, 18452, 18486, 18487, 18488:
		f = check.CredentialsRejected.Wrap(err)
	case 4060:
		f = databaseClosed.Wrap(err, "database", sv.database)
	case 208:
		f = check.NotFound.Wrap(err, "resource", "The table")
	case 229, 230, 262, 297, 300, 916, 1088, 15151, 15247, 22902:
		f = check.PermissionDenied.Wrap(err)
		f.Fix = "Grant the connector's login the permission the detail names."
	default:
		return nil
	}
	return f.With("error_number", strconv.Itoa(int(me.Number)))
}

// connect reaches the server, signs in and checks the connection is
// encrypted: net.dns, net.connect, db.connect and db.tls. SQL Server starts
// TLS inside its own protocol, before sign-in, so a failed handshake fails
// db.connect, as TLSFailed.
func (sv server) connect(k *sqlcheck.Kit) []check.Check {
	var checks []check.Check
	if sv.address != "" {
		checks = netcheck.Reach(sv.address)
		for i := range checks {
			checks[i].Field = "dsn"
		}
	}
	c := k.Connect()
	c.Kind = check.Authentication
	c.Title = "Sign in to the database"
	c.Description = "Connects with the dsn's login, over TLS when the server or the dsn asks for it, and opens its database."
	c.Field = "dsn"
	if sv.address != "" {
		c.Needs = []string{"net.connect"}
	}
	tls := k.Check("db.tls", check.Connection, "Encrypt the connection", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		cert := "with a certificate the dsn verifies."
		if !sv.verified {
			cert = "with a certificate the dsn doesn't verify: its trustServerCertificate is true, as it is without encrypt unless the dsn sets it."
		}
		if sv.encrypted {
			p.Found("The dsn's encrypt requires TLS for the whole connection, " + cert)
			return nil
		}
		// Without VIEW SERVER STATE, the login can't see the session's
		// encryption.
		var on string
		err := db.QueryRowContext(ctx, "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID").Scan(&on)
		if err != nil || on != "TRUE" {
			return check.Warn(unencrypted.Fail())
		}
		p.Found("The server encrypts the connection, " + cert)
		return nil
	})
	tls.Field = "dsn"
	return append(checks, c, tls)
}

// version needs SQL Server major version min, or Azure SQL, whose
// versions don't compare.
func version(k *sqlcheck.Kit, min int, name string) check.Check {
	return k.Value("server.version", "Run on "+name+" or later", func(v string) error {
		major, _, _ := strings.Cut(v, ".")
		if n, err := strconv.Atoi(major); err == nil && n < min {
			return check.UnsupportedVersion.Fail("required", name)
		}
		return nil
	}, "SELECT CASE WHEN SERVERPROPERTY('EngineEdition') IN (5, 8) THEN N'Azure SQL' ELSE CONVERT(nvarchar(30), SERVERPROPERTY('ProductVersion')) END")
}

// schemaExists is a check that the user can see the schema.
func schemaExists(k *sqlcheck.Kit, name, schema, field string) check.Check {
	c := k.True(name, "Find the schema {schema}", nil, check.NotFound.Fail("resource", "The schema "+schema),
		"SELECT CASE WHEN SCHEMA_ID(@p1) IS NULL THEN NULL ELSE 1 END", schema)
	c.Kind, c.Field, c.Params = check.Resource, field, check.Params{"schema": schema}
	return c
}

// checkNames makes check names from data unique, adding the n-th repeat's number.
type checkNames map[string]int

func (n checkNames) of(parts ...string) string {
	name := check.Name(parts...)
	n[name]++
	if c := n[name]; c > 1 {
		return name + "-" + strconv.Itoa(c)
	}
	return name
}

// Checks reaches the server, signs in, and confirms the schema's tables can
// be read, and with change data capture what it needs. Whether a table is
// tracked or captured is the preflight's, for the streams a pipeline reads
// with cdc.
func (s *Source) Checks() []check.Check {
	sv := serverOf(s.cfg.DSN.Reveal())
	k := sv.kit()
	checks := sv.connect(k)
	checks = append(checks, version(k, 13, "SQL Server 2016"), schemaExists(k, "schema.exists", s.cfg.Schema, "schema"))
	n := checkNames{}
	if len(s.cfg.Tables) > 0 {
		for _, t := range s.cfg.Tables {
			c := k.Readable(n.of("table", t, "read"), qualified(s.cfg.Schema, t))
			c.Field, c.Needs = "tables", []string{"schema.exists"}
			checks = append(checks, c)
		}
	} else {
		c := k.Check("schema.read", check.Permission, "Read the tables and views in {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			// Without STRING_AGG, which SQL Server 2016 lacks.
			rows, err := db.QueryContext(ctx, `SELECT o.name, HAS_PERMS_BY_NAME(QUOTENAME(s.name) + N'.' + QUOTENAME(o.name), 'OBJECT', 'SELECT')
				FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE s.name = @p1 AND o.type IN ('U', 'V') AND o.is_ms_shipped = 0 ORDER BY o.name`, s.cfg.Schema)
			if err != nil {
				return k.Failure(err)
			}
			defer func() { _ = rows.Close() }()
			count := 0
			var unreadable []string
			for rows.Next() {
				var name string
				var readable int
				if err := rows.Scan(&name, &readable); err != nil {
					return k.Failure(err)
				}
				count++
				if readable == 0 {
					unreadable = append(unreadable, name)
				}
			}
			if err := rows.Err(); err != nil {
				return k.Failure(err)
			}
			p.Param("tables", count)
			if len(unreadable) > 0 {
				return check.PrivilegeMissing.Fail("principal", sv.principal, "privilege", "SELECT", "object", strings.Join(unreadable, ", "))
			}
			p.Found("Every table and view is readable: {tables} in all.")
			return nil
		})
		c.Field, c.Needs, c.Params = "schema", []string{"schema.exists"}, check.Params{"schema": s.cfg.Schema}
		checks = append(checks, c)
	}
	if s.cfg.CDCMethod == methodTracking {
		c := k.True("database.change-tracking", "Track the database's changes", check.SettingMismatch.Fail("setting", "CHANGE_TRACKING", "found", "OFF", "required", "ON"), nil,
			"SELECT count(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()")
		c.Kind, c.Field, c.Advisory = check.Compatibility, "cdcMethod", true
		c.Description = "cdc with change_tracking reads only tables of a database with change tracking on: ALTER DATABASE ... SET CHANGE_TRACKING = ON."
		return append(checks, c)
	}
	captured := k.True("database.change-data-capture", "Capture the database's changes", check.SettingMismatch.Fail("setting", "is_cdc_enabled", "found", "0", "required", "1"), nil,
		"SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()")
	captured.Kind, captured.Field = check.Compatibility, "cdcMethod"
	captured.Description = "cdc with change_data_capture reads the change tables of a database with change data capture on: EXEC sys.sp_cdc_enable_db."
	cdcRead := k.True("cdc.read", "Read the change tables", check.PrivilegeMissing.Fail("principal", sv.principal, "privilege", "SELECT", "object", "SCHEMA::cdc"), nil,
		"SELECT HAS_PERMS_BY_NAME(N'cdc', 'SCHEMA', 'SELECT')")
	cdcRead.Field, cdcRead.Needs = "cdcMethod", []string{"database.change-data-capture"}
	agent := k.Check("cdc.agent", check.Compatibility, "Run SQL Server Agent, whose capture job fills the change tables", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var allowed int
		if err := db.QueryRowContext(ctx, "SELECT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE')").Scan(&allowed); err != nil {
			return k.Failure(err)
		}
		if allowed != 1 {
			return check.Skip("seeing SQL Server Agent's state takes VIEW SERVER STATE, which the login doesn't have")
		}
		var status string
		err := db.QueryRowContext(ctx, "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE N'SQL Server Agent%'").Scan(&status)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// Azure SQL Managed Instance runs the capture job without one.
			return check.Skip("the server lists no SQL Server Agent")
		case err != nil:
			return k.Failure(err)
		}
		p.Param("status", status)
		if status != "Running" {
			return check.SettingMismatch.Fail("setting", "SQL Server Agent", "found", status, "required", "Running")
		}
		p.Found("SQL Server Agent is running.")
		return nil
	})
	agent.Field, agent.Needs = "cdcMethod", []string{"database.change-data-capture"}
	return append(checks, captured, cdcRead, agent)
}

// Preflight confirms each stream's table can be read in its mode: its
// cursor column, for cdc that it's tracked or captured with a primary key,
// and that the database keeps changes longer than the pipeline goes
// between syncs.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	sv := serverOf(s.cfg.DSN.Reveal())
	k := sv.kit()
	checks := sv.connect(k)
	n := checkNames{}
	cdc := false
	for _, st := range req.Streams {
		ident := qualified(s.cfg.Schema, st.Name)
		params := check.Params{"stream": st.Name, "table": ident, "schema": s.cfg.Schema, "name": st.Name, "principal": sv.principal}
		read := k.Readable(n.of("stream", st.Name, "read"), ident)
		read.Title = "Read {stream}"
		read.Params["stream"] = st.Name
		checks = append(checks, read)
		var c check.Check
		switch st.SyncMode {
		case source.Incremental:
			params["cursor"] = st.Cursor
			c = k.Check(n.of("stream", st.Name, "cursor"), check.Configuration, "Order {stream} by {cursor}", func(ctx context.Context, _ *check.Probe, db *sql.DB) error {
				var typ string
				err := db.QueryRowContext(ctx, "SELECT TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(@p1) AND name = @p2", ident, st.Cursor).Scan(&typ)
				switch {
				case errors.Is(err, sql.ErrNoRows):
					return badCursor.Fail()
				case err != nil:
					return k.Failure(err)
				case !cursorTypes[typ]:
					return badCursor.Fail()
				}
				return nil
			})
		case source.CDC:
			cdc = true
			c = s.cdcCheck(k, n.of("stream", st.Name, strings.ReplaceAll(s.cfg.CDCMethod, "_", "-")), ident)
		default:
			continue
		}
		c.Params = params
		c.Needs = append(c.Needs, read.Name)
		checks = append(checks, c)
	}
	if cdc && req.Interval > 0 {
		checks = append(checks, s.retentionCheck(k, req.Interval))
	}
	return checks
}

// cdcCheck is a check that ident can be read with cdc.
func (s *Source) cdcCheck(k *sqlcheck.Kit, name, ident string) check.Check {
	title := "Read {stream}'s changes through change tracking"
	if s.cfg.CDCMethod == methodCapture {
		title = "Read {stream}'s changes through change data capture"
	}
	return k.Check(name, check.Compatibility, title, func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var keyed int
		var ok sql.NullInt64
		// 0 without a primary key, 2 with one of a sql_variant column.
		const key = `CASE WHEN coalesce(OBJECTPROPERTY(OBJECT_ID(@p1), 'TableHasPrimaryKey'), 0) = 0 THEN 0
			WHEN EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
				JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
				WHERE i.object_id = OBJECT_ID(@p1) AND i.is_primary_key = 1 AND TYPE_NAME(c.system_type_id) = 'sql_variant') THEN 2 ELSE 1 END`
		q := "SELECT " + key + ", CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@p1))"
		if s.cfg.CDCMethod == methodCapture {
			q = "SELECT " + key + `, (SELECT TOP (1) 1 FROM cdc.change_tables ct WHERE ct.source_object_id = OBJECT_ID(@p1)
					AND HAS_PERMS_BY_NAME(N'cdc.' + QUOTENAME(ct.capture_instance + N'_CT'), 'OBJECT', 'SELECT') = 1)`
		}
		if err := db.QueryRowContext(ctx, q, ident).Scan(&keyed, &ok); err != nil {
			if f := k.Failure(err); s.cfg.CDCMethod == methodCapture && f.Code == protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
				return notCaptured.Wrap(err)
			}
			return k.Failure(err)
		}
		switch {
		case keyed == 0:
			return noKey.Fail()
		case keyed == 2:
			return variantKey.Fail()
		case !ok.Valid && s.cfg.CDCMethod == methodCapture:
			return notCaptured.Fail()
		case !ok.Valid:
			return notTracked.Fail()
		case s.cfg.CDCMethod != methodCapture:
			return nil
		}
		// The capture job passes such a column's values through the
		// database's code page.
		var column string
		err := db.QueryRowContext(ctx, `SELECT TOP (1) c.name FROM sys.columns c
			WHERE c.object_id = OBJECT_ID(@p1) AND TYPE_NAME(c.system_type_id) IN ('char', 'varchar', 'text')
				AND COLLATIONPROPERTY(c.collation_name, 'CodePage') <> COLLATIONPROPERTY(CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage')
			ORDER BY c.column_id`, ident).Scan(&column)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return k.Failure(err)
		}
		p.Param("column", column)
		return check.Warn(otherCodePage.Fail())
	})
}

// retentionCheck warns when the database may clean up a pipeline's changes
// before its next sync reads them.
func (s *Source) retentionCheck(k *sqlcheck.Kit, interval time.Duration) check.Check {
	c := k.Check("cdc.retention", check.Compatibility, "Keep changes between syncs", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var keep time.Duration
		var setting string
		if s.cfg.CDCMethod == methodTracking {
			var period, units int
			var cleanup bool
			err := db.QueryRowContext(ctx, "SELECT retention_period, retention_period_units, is_auto_cleanup_on FROM sys.change_tracking_databases WHERE database_id = DB_ID()").Scan(&period, &units, &cleanup)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return check.Skip("change tracking is off for the database")
			case err != nil:
				return k.Failure(err)
			case !cleanup:
				p.Found("Change tracking keeps every change: its auto cleanup is off.")
				return nil
			}
			keep = time.Duration(period) * map[int]time.Duration{1: time.Minute, 2: time.Hour, 3: 24 * time.Hour}[units]
			setting = "ALTER DATABASE ... SET CHANGE_TRACKING (CHANGE_RETENTION = ...)"
		} else {
			var minutes int64
			err := db.QueryRowContext(ctx, "SELECT retention FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup'").Scan(&minutes)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return check.Skip("the database has no cleanup job, so it keeps every change")
			case err != nil:
				return check.Skip("reading the cleanup job's retention takes SELECT on msdb.dbo.cdc_jobs, which the login doesn't have")
			}
			keep = time.Duration(minutes) * time.Minute
			setting = "EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = ..."
		}
		p.Param("retention", keep)
		p.Param("setting", setting)
		if keep <= interval {
			return check.Warn(retentionShort.Fail())
		}
		p.Found("The database keeps changes for {retention}, longer than the pipeline goes between syncs.")
		return nil
	})
	c.Params = check.Params{"interval": interval.String()}
	return c
}

// Checks reaches the server, signs in, and confirms the login can create
// tables, and write them in the default schema or create it.
func (d *Destination) Checks() []check.Check {
	sv := serverOf(d.cfg.DSN.Reveal())
	k := sv.kit()
	checks := sv.connect(k)
	compat := k.Value("database.compatibility", "Run the database at compatibility level 130 or later", func(v string) error {
		if n, err := strconv.Atoi(v); err != nil || n < 130 {
			return check.SettingMismatch.Fail("setting", "COMPATIBILITY_LEVEL", "required", "130 or later")
		}
		return nil
	}, "SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()")
	compat.Description = "Applying a merge reads JSON with OPENJSON, which takes compatibility level 130."
	create := k.True("database.create-table", "Create tables", check.PrivilegeMissing.Fail("principal", sv.principal, "privilege", "CREATE TABLE", "object", "the database"), nil,
		"SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')")
	return append(checks, version(k, 14, "SQL Server 2017"), compat, create, schemaWrite(k, sv, "schema.write", d.cfg.Schema, "schema"))
}

// schemaWrite is a check that the login can write tables in schema, or
// create it.
func schemaWrite(k *sqlcheck.Kit, sv server, name, schema, field string) check.Check {
	c := k.Check(name, check.Permission, "Write tables in the schema {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var exists, createSchema int
		var missing string
		err := db.QueryRowContext(ctx, `SELECT CASE WHEN SCHEMA_ID(@p1) IS NULL THEN 0 ELSE 1 END, HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE SCHEMA'),
			coalesce((SELECT STRING_AGG(v.value, N', ') FROM STRING_SPLIT(N'ALTER,SELECT,INSERT,UPDATE,DELETE', N',') v
				WHERE HAS_PERMS_BY_NAME(QUOTENAME(@p1), 'SCHEMA', v.value) = 0), N'')`, schema).Scan(&exists, &createSchema, &missing)
		switch {
		case err != nil:
			return k.Failure(err)
		case exists == 0 && createSchema == 0:
			return check.PrivilegeMissing.Fail("principal", sv.principal, "privilege", "CREATE SCHEMA", "object", "the database, to create "+schema)
		case exists == 0:
			p.Found("{schema} doesn't exist yet, and the login can create it.")
			return nil
		case missing != "":
			return check.PrivilegeMissing.Fail("principal", sv.principal, "privilege", missing, "object", "SCHEMA::"+schema)
		}
		return nil
	})
	c.Field, c.Params = field, check.Params{"schema": schema}
	return c
}

// Preflight confirms the namespace can be written, and that each stream's
// table can take its schema: it can be created or written, and it holds
// no column of a type the stream would change, unless the pipeline allows
// that.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	sv := serverOf(d.cfg.DSN.Reveal())
	k := sv.kit()
	checks := sv.connect(k)
	schema, field := req.Namespace, ""
	if schema == "" {
		schema, field = d.cfg.Schema, "schema"
	}
	checks = append(checks, schemaWrite(k, sv, "namespace.write", schema, field))
	n := checkNames{}
	for _, st := range req.Streams {
		sch := req.Schemas[st.Name]
		if sch == nil {
			continue
		}
		table := sqlstage.TargetTable(sch, st.Name)
		c := k.Check(n.of("stream", st.Name, "write"), check.Permission, "Write {stream} to {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			want, err := columnsOf(sch)
			if err != nil {
				return check.ConfigInvalid.Wrap(err)
			}
			if st.Mode == destination.Merge {
				if key, err := sqlstage.KeyColumns(sch, st.PrimaryKey); err == nil {
					keyed(want, key)
				}
			}
			ident := qualified(schema, table)
			rows, err := db.QueryContext(ctx, `SELECT c.name, TYPE_NAME(c.system_type_id), c.max_length, c.precision, c.scale, c.is_nullable, `+ownIndexes+`, `+theirIndex+` FROM sys.columns c
				WHERE c.object_id = OBJECT_ID(@p1) ORDER BY c.column_id`, ident)
			if err != nil {
				return k.Failure(err)
			}
			defer func() { _ = rows.Close() }()
			have := map[string]sourceColumn{}
			own := map[string][]string{}
			theirs := map[string]string{}
			for rows.Next() {
				var c sourceColumn
				var nullable bool
				var idx, their string
				if err := rows.Scan(&c.name, &c.typ, &c.maxLength, &c.precision, &c.scale, &nullable, &idx, &their); err != nil {
					return k.Failure(err)
				}
				c.notNull = !nullable
				have[c.name] = c
				if idx != "" {
					own[c.name] = strings.Split(idx, ",")
				}
				if their != "" {
					theirs[c.name] = their
				}
			}
			if err := rows.Err(); err != nil {
				return k.Failure(err)
			}
			added := 0
			for _, w := range want {
				if _, ok := have[w.name]; !ok {
					added++
				}
			}
			if len(have)+added > maxColumns {
				p.Param("columns", len(have)+added)
				return tooWide.Fail()
			}
			if len(have) == 0 {
				p.Found("{table} doesn't exist yet: the first sync creates it.")
				return nil
			}
			var missing string
			if err := db.QueryRowContext(ctx, `SELECT coalesce((SELECT STRING_AGG(v.value, N', ') FROM STRING_SPLIT(N'ALTER,SELECT,INSERT,UPDATE,DELETE', N',') v
				WHERE HAS_PERMS_BY_NAME(@p1, 'OBJECT', v.value) = 0), N'')`, ident).Scan(&missing); err != nil {
				return k.Failure(err)
			}
			if missing != "" {
				return check.PrivilegeMissing.Fail("principal", sv.principal, "privilege", missing, "object", ident)
			}
			for _, w := range want {
				h, ok := have[w.name]
				if !ok || w.value == nil || h.sqlType() == w.typ || widens(w.typ, h.sqlType()) {
					continue
				}
				p.Param("column", w.name)
				p.Param("from", h.sqlType())
				p.Param("to", w.typ)
				if ix := theirs[w.name]; ix != "" && !longer(h.sqlType(), w.typ) {
					p.Param("index", ix)
					return check.Warn(indexedChange.Fail())
				}
				if st.AllowTypeChanges {
					continue
				}
				p.Param("statement", alterColumn(ident, w.name, w.typ, h.notNull, own[w.name]))
				return check.Warn(typeChange.Fail())
			}
			return nil
		})
		c.Params = check.Params{"stream": st.Name, "table": table}
		c.Needs = append(c.Needs, "namespace.write")
		checks = append(checks, c)
	}
	return checks
}
