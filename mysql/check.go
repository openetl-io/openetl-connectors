package mysql

import (
	"cmp"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	endian "encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
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

const (
	configInvalidCode = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID
	transientCode     = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
)

var (
	plaintext = check.Reason{
		Name: "plaintext_connection", Code: transientCode,
		Message: "{address} doesn't take TLS, so the connection is in plaintext, as the config allows.",
		Fix:     "Give the server a certificate and key, with ssl_cert and ssl_key, and set tls.mode to verify-ca or verify-identity.",
	}
	noTLS = check.Reason{
		Name: "tls_refused", Code: configInvalidCode,
		Message: "{address} doesn't take TLS, and the config needs it.",
		Fix:     "Give the server a certificate and key, with ssl_cert and ssl_key, or set tls.mode to disabled.",
	}
	noKey = check.Reason{
		Name: "primary_key_missing", Code: configInvalidCode,
		Message: "{stream} has no primary key, which a cdc read needs.",
		Fix:     "Add a primary key to {table}, or read {stream} with full_refresh or incremental.",
	}
	keyUnsupported = check.Reason{
		Name: "primary_key_unsupported", Code: configInvalidCode,
		Message: "{stream}'s primary key has {column}, of type {type}, which doesn't order as it compares, so a cdc read can't read the table in chunks.",
		Fix:     "Read {stream} with full_refresh or incremental, or give it a primary key of integer, decimal, string, binary, date, time or year columns.",
	}
	badCursor = check.Reason{
		Name: "cursor_unsupported", Code: configInvalidCode,
		Message: "{stream}'s cursor {cursor} is of type {type}, which the source can't read incrementally by.",
		Fix:     "Name a column of an integer, decimal, date, datetime, timestamp, year, char, varchar or text type as the cursor.",
	}
	columnUnsupported = check.Reason{
		Name: "column_unsupported", Code: configInvalidCode,
		Message: "{table} has the column {column} of type {type}, which the source can't read.",
		Fix:     "Leave {table} out of the source's tables, or out of the pipeline.",
	}
	shortRetention = check.Reason{
		Name: "binlog_retention_short", Code: configInvalidCode,
		Message: "The server keeps binlog files for {retention}, less than the {interval} between the pipeline's syncs, so a sync can find its position purged and read its tables again from a new snapshot.",
		Fix:     "Set binlog_expire_logs_seconds to more than {interval_seconds} in the server's option file, and run SET GLOBAL binlog_expire_logs_seconds = {suggested_seconds};",
	}
)

// cdcOnly describes what the connection's check looks at only for cdc.
const cdcOnly = "Needed only to read tables with cdc: a pipeline's preflight fails without it if it does."

// newKit makes the checks' database handle from c, the connector's own
// connection config, TLS settings included. Each list has its own, so a run
// opens one and closes it.
func newKit(c *mysql.Config) *sqlcheck.Kit {
	k := sqlcheck.New(func(context.Context) (*sql.DB, error) {
		conn, err := connector(c)
		if err != nil {
			return nil, err
		}
		db := sql.OpenDB(conn)
		db.SetMaxOpenConns(1)
		return db, nil
	})
	k.Classify = failure
	return k
}

// failure gives the server's errors their reasons; nil for one the kit's
// own classification fits.
func failure(err error) *check.Failure {
	if errors.Is(err, mysql.ErrNoTLS) {
		return noTLS.Wrap(err)
	}
	n, ok := serverError(err)
	if !ok {
		return nil
	}
	var f *check.Failure
	switch n {
	case 1045, 1698:
		f = check.CredentialsRejected.Wrap(err)
	case 1044, 1142, 1143, 1227, 1370:
		f = check.PrivilegeMissing.Wrap(err)
		f.Message = "The database user lacks a privilege this needs."
		f.Fix = "Grant the connector's database user the privilege the detail names."
	case 1049:
		f = check.NotFound.Wrap(err, "resource", "The database")
	case 1146:
		f = check.NotFound.Wrap(err, "resource", "The table")
	case 1040, 1053, 1129, 1203, 1205, 1213, 1226, 1927, 2006, 2013, 3024:
		f = check.Unavailable.Wrap(err)
	default:
		return nil
	}
	return f.With("mysql_error", strconv.Itoa(int(n)))
}

// connectionChecks reach the server, agree on TLS as the config says, and
// sign in: net.dns, net.connect, db.tls and db.connect. A Unix socket has
// only db.connect.
func connectionChecks(c *mysql.Config, k *sqlcheck.Kit, t *TLSConfig) []check.Check {
	connect := k.Connect()
	connect.Kind = check.Authentication
	connect.Title = "Sign in as {user}"
	connect.Params = check.Params{"user": c.User}
	connect.Field = "dsn"
	if c.Net != "tcp" {
		return []check.Check{connect}
	}
	checks := netcheck.Reach(c.Addr)
	for i := range checks {
		checks[i].Field = "dsn"
	}
	connect.Needs = []string{"net.connect"}
	if c.TLS != nil {
		field := "dsn"
		if t != nil {
			field = "tls.mode"
			if t.CA != "" {
				field = "tls.ca"
			}
		}
		checks = append(checks, tlsCheck(c, field))
		connect.Needs = []string{"db.tls"}
	}
	return append(checks, connect)
}

// tlsCheck is db.tls: the server takes TLS, and its certificate is one the
// config trusts. MySQL starts TLS inside its own protocol, which
// netcheck.TLS doesn't speak.
func tlsCheck(c *mysql.Config, field string) check.Check {
	verified := !c.TLS.InsecureSkipVerify || c.TLS.VerifyPeerCertificate != nil
	return check.Check{
		Name: "db.tls", Kind: check.Connection, Title: "Agree on TLS with {address}", Field: field,
		Params: check.Params{"address": c.Addr}, Needs: []string{"net.connect"}, Timeout: netcheck.DialTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			st, err := handshake(ctx, c)
			var te *tlsError
			switch {
			case errors.Is(err, errNoTLS) && c.AllowFallbackToPlaintext:
				return check.Warn(plaintext.Fail())
			case errors.Is(err, errNoTLS):
				return noTLS.Fail()
			case err != nil && ctx.Err() != nil:
				return check.TimedOut.Wrap(err)
			case errors.As(err, &te):
				f := check.TLSFailed.Wrap(te.err)
				f.Fix = "Check that tls.ca holds the authority that signs the server's certificate, and under verify-identity that the certificate names the dsn's host: the certificates MySQL and MariaDB make for themselves name none, so they take verify-ca."
				return f
			case err != nil:
				if f := failure(err); f != nil {
					return f
				}
				return err
			}
			p.Param("tls_version", tls.VersionName(st.Version))
			if len(st.PeerCertificates) == 0 {
				p.Found("{tls_version}, with no certificate.")
				return nil
			}
			expires := st.PeerCertificates[0].NotAfter
			p.Param("expires", expires.UTC().Format(time.DateOnly))
			if verified {
				p.Found("{tls_version}, with a certificate the config trusts that expires on {expires}.")
			} else {
				p.Found("{tls_version}, with a certificate that expires on {expires}, which the config doesn't verify.")
			}
			if time.Until(expires) < netcheck.ExpiresSoon {
				return check.Warn(netcheck.CertificateExpiring.Fail())
			}
			return nil
		},
	}
}

var errNoTLS = errors.New("the server doesn't take TLS")

type tlsError struct{ err error }

func (e *tlsError) Error() string { return e.err.Error() }
func (e *tlsError) Unwrap() error { return e.err }

// The client's capability flags an SSL request sends.
const (
	clientLongPassword  = 1
	clientProtocol41    = 0x200
	clientSSL           = 0x800
	clientSecureConnect = 0x8000
)

// handshake reads the server's greeting, asks for TLS as the driver does,
// and makes the handshake with c's TLS config. An error packet in place of
// the greeting is a *mysql.MySQLError.
func handshake(ctx context.Context, c *mysql.Config) (*tls.ConnectionState, error) {
	d := net.Dialer{Timeout: netcheck.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	greeting, err := readPacket(conn)
	if err != nil {
		return nil, err
	}
	if len(greeting) >= 3 && greeting[0] == 0xff {
		return nil, &mysql.MySQLError{Number: endian.LittleEndian.Uint16(greeting[1:]), Message: string(greeting[3:])}
	}
	// Protocol 10: the version, the server's version ending in a NUL, a
	// connection ID of 4 bytes, 8 of auth data and a filler, then the low 2
	// bytes of its capability flags.
	end := slices.Index(greeting, 0)
	if len(greeting) == 0 || greeting[0] != 10 || end < 0 || len(greeting) < end+16 {
		return nil, errors.New("mysql: the server's greeting isn't one of protocol 10")
	}
	if endian.LittleEndian.Uint16(greeting[end+14:])&clientSSL == 0 {
		return nil, errNoTLS
	}
	// An SSL request: its length, 32, and sequence number 1; the flags; the
	// largest packet; utf8mb4's collation; 23 bytes of filler.
	req := make([]byte, 4+32)
	req[0], req[3] = 32, 1
	endian.LittleEndian.PutUint32(req[4:], clientLongPassword|clientProtocol41|clientSSL|clientSecureConnect)
	endian.LittleEndian.PutUint32(req[8:], 1<<24)
	req[12] = 45
	if _, err := conn.Write(req); err != nil {
		return nil, err
	}
	tc := tls.Client(conn, c.TLS.Clone())
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, &tlsError{err}
	}
	st := tc.ConnectionState()
	return &st, nil
}

func readPacket(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	b := make([]byte, int(h[0])|int(h[1])<<8|int(h[2])<<16)
	_, err := io.ReadFull(r, b)
	return b, err
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

// Checks reach the server, sign in, and look at the database's tables, and
// at what cdc needs, which only warns: whether a pipeline reads with cdc is
// its preflight's to say.
func (s *Source) Checks() []check.Check {
	k := newKit(s.conn)
	user, database := s.conn.User, s.cfg.Database
	checks := connectionChecks(s.conn, k, s.cfg.TLS)

	missing := check.NotFound.Fail()
	missing.Message = "{database} doesn't exist, or {user} has no privilege in it."
	missing.Fix = "Correct database in the config, or grant {user} SELECT on its tables."
	found := k.True("database.found", "Find the database {database}", missing, missing,
		`SELECT COUNT(*) > 0 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?`, database)
	found.Kind, found.Field, found.Params = check.Resource, "database", check.Params{"database": database, "user": user}
	checks = append(checks, found)
	readNeeds := []string{"database.found"}

	if len(s.cfg.Tables) > 0 {
		tables := k.Check("tables.found", check.Resource, "Find the tables in {database}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			srv, err := serverOf(ctx, db)
			if err != nil {
				return k.Failure(err)
			}
			ts, err := s.listTables(ctx, db, srv, "")
			if err != nil {
				return unlisted(k, err)
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
				f.Message, f.Fix = "{missing} isn't a table in {database} that {user} can see.", "Correct tables in the config, or grant {user} SELECT on what it names."
				return f
			}
			return nil
		})
		tables.Field, tables.Params, tables.Needs = "tables", check.Params{"database": database, "user": user}, []string{"database.found"}
		checks = append(checks, tables)
		readNeeds = []string{"tables.found"}
	}

	read := k.Check("tables.read", check.Permission, "Read the tables of {database}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		srv, err := serverOf(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		ts, err := s.listTables(ctx, db, srv, "")
		if err != nil {
			return unlisted(k, err)
		}
		var unreadable []string
		n := 0
		for _, t := range ts {
			if len(s.cfg.Tables) > 0 && !slices.Contains(s.cfg.Tables, t.name) {
				continue
			}
			n++
			if err := unsupported(t, p); err != nil {
				return err
			}
			err := selectNothing(ctx, db, database, t.name)
			switch code, _ := serverError(err); {
			case code == 1142 || code == 1143:
				unreadable = append(unreadable, quoteIdent(t.name))
			case err != nil:
				return k.Failure(err)
			}
		}
		if len(unreadable) > 0 {
			return check.PrivilegeMissing.Fail("principal", user, "privilege", "SELECT", "object", strings.Join(unreadable, ", "))
		}
		p.Param("count", n)
		p.Found("{user} can read every table the source offers, {count} in all.")
		return nil
	})
	read.Field, read.Params, read.Needs = "tables", check.Params{"database": database, "user": user}, readNeeds
	checks = append(checks, read)

	for _, c := range cdcChecks(k, user) {
		if !c.Advisory {
			c.Advisory, c.Description = true, cdcOnly
		}
		checks = append(checks, c)
	}
	retention := retentionCheck(k, 0)
	retention.Advisory = true
	return append(checks, retention)
}

// selectNothing selects no rows from a table, which fails without SELECT on
// it, as a 1142, or for a table that doesn't exist, as a 1146.
func selectNothing(ctx context.Context, db *sql.DB, database, name string) error {
	rows, err := db.QueryContext(ctx, "SELECT 1 FROM "+quoteIdent(database)+"."+quoteIdent(name)+" LIMIT 0")
	if err != nil {
		return err
	}
	return rows.Close()
}

// unlisted is the failure of a listing of tables: the server's, or a
// column in a character set the source can't decode.
func unlisted(k *sqlcheck.Kit, err error) error {
	if _, ok := serverError(err); ok {
		return k.Failure(err)
	}
	f := check.ConfigInvalid.Wrap(err)
	f.Message, f.Fix = "A table has a column the source can't read.", "Leave the table the detail names out of the source's tables, or out of the pipeline."
	return f
}

// unsupported is the failure of a table with a column the source can't
// read, naming the column in p.
func unsupported(t table, p *check.Probe) error {
	for _, c := range t.cols {
		if _, _, err := arrowTypes(c); err != nil {
			p.Param("table", t.name)
			p.Param("column", c.name)
			p.Param("type", c.columnType)
			return columnUnsupported.Fail()
		}
	}
	return nil
}

// cdcChecks are what a cdc read needs of the server and the user: its
// binlog settings, GTIDs, which only warn, and the privileges to read the
// binlog.
func cdcChecks(k *sqlcheck.Kit, user string) []check.Check {
	var checks []check.Check
	for _, b := range binlogSettings {
		c := k.Check("cdc."+b.name, check.Compatibility, b.title, func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			v, found, err := globalVariable(ctx, db, b.variable)
			switch {
			case err != nil:
				return k.Failure(err)
			case !found && b.optional:
				p.Found("The server has no {setting}.")
				return nil
			}
			p.Param("found", cmp.Or(v, "not set"))
			if !found || !b.ok(v) {
				f := check.SettingMismatch.Fail("required", b.required)
				f.Fix = b.fix
				return f
			}
			p.Found("{setting} is {found}.")
			return nil
		})
		c.Params = check.Params{"setting": b.variable}
		checks = append(checks, c)
	}

	gtid := k.Check("cdc.gtid-mode", check.Compatibility, "Follow the binlog by GTIDs", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		v, found, err := globalVariable(ctx, db, "gtid_mode")
		switch {
		case err != nil:
			return k.Failure(err)
		case !found:
			p.Found("The server has GTIDs whatever its settings, as MariaDB does.")
			return nil
		}
		p.Param("found", v)
		if !strings.EqualFold(v, "ON") {
			f := check.SettingMismatch.Fail("setting", "gtid_mode", "required", "ON")
			f.Message = "gtid_mode is {found}, so a cdc read can't continue on another server after a failover."
			f.Fix = "Set gtid_mode = ON and enforce_gtid_consistency = ON: on a running server, set gtid_mode to OFF_PERMISSIVE, then ON_PERMISSIVE, then ON."
			return f
		}
		p.Found("gtid_mode is ON.")
		return nil
	})
	gtid.Advisory = true
	gtid.Description = "GTIDs let a cdc read continue on another server of the topology after a failover. Without them it reads only the server it started on."
	checks = append(checks, gtid)

	slave := k.Check("cdc.replication", check.Permission, "Read the binlog as {user}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		g, principal, err := userGrants(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("principal", principal)
		if !g.has("REPLICATION SLAVE", "") {
			f := check.PrivilegeMissing.Fail("privilege", "REPLICATION SLAVE", "object", "*.*")
			f.Fix = "Run GRANT REPLICATION SLAVE ON *.* TO {principal};"
			return f
		}
		return nil
	})
	slave.Field, slave.Params = "dsn", check.Params{"user": user}

	monitor := k.Check("cdc.binlog-monitor", check.Permission, "Read where the binlog ends as {user}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		srv, err := serverOf(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("privilege", map[bool]string{false: "REPLICATION CLIENT", true: "BINLOG MONITOR"}[srv.mariadb])
		_, _, err = binlogStatus(ctx, db, srv)
		if code, _ := serverError(err); code == 1227 {
			_, principal, gerr := userGrants(ctx, db)
			if gerr != nil {
				return k.Failure(gerr)
			}
			p.Param("principal", principal)
			f := check.PrivilegeMissing.Wrap(err, "object", "*.*")
			f.Fix = "Run GRANT {privilege} ON *.* TO {principal};"
			return f
		}
		if errors.Is(err, errNoBinlog) {
			return check.Skip("the server has no binlog, as cdc.log-bin says")
		}
		return k.Failure(err)
	})
	monitor.Field, monitor.Params = "dsn", check.Params{"user": user}
	return append(checks, slave, monitor)
}

// Preflight looks at each stream as the pipeline reads it: there and
// readable, its cursor one the source reads by, and for cdc its key, the
// server's binlog, the user's privileges to read it, and how long the server
// keeps it against the pipeline's schedule.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := newKit(s.conn)
	user, database := s.conn.User, s.cfg.Database
	checks := []check.Check{k.Connect()}
	n := checkNames{}
	cdc := false
	for _, st := range req.Streams {
		params := check.Params{"stream": st.Name, "database": database, "table": quoteIdent(database) + "." + quoteIdent(st.Name), "principal": user}
		read := k.Check(n.name("stream", st.Name, "read"), check.Permission, "Read {stream}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			_, err := s.streamTable(ctx, k, db, p, st.Name)
			return err
		})
		read.Params = params
		checks = append(checks, read)
		switch st.SyncMode {
		case source.Incremental:
			c := k.Check(n.name("stream", st.Name, "cursor"), check.Compatibility, "Read {stream} incrementally", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
				if st.Cursor == "" {
					f := check.ConfigInvalid.Fail()
					f.Message, f.Fix = "{stream} is read incrementally, and names no cursor.", "Name one of {stream}'s columns as its cursor."
					return f
				}
				t, err := s.streamTable(ctx, k, db, p, st.Name)
				if err != nil {
					return err
				}
				i := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.name == st.Cursor })
				if i < 0 {
					f := check.NotFound.Fail()
					f.Message, f.Fix = "{stream} has no column {cursor}.", "Name one of {stream}'s columns as its cursor."
					return f
				}
				p.Param("type", t.cols[i].columnType)
				if !cursorKinds[t.cols[i].kind] {
					return badCursor.Fail()
				}
				p.Found("{stream} is read by {cursor}, of type {type}.")
				return nil
			})
			c.Params, c.Needs = check.Params{"stream": st.Name, "cursor": st.Cursor}, []string{read.Name}
			checks = append(checks, c)
		case source.CDC:
			cdc = true
			c := k.Check(n.name("stream", st.Name, "cdc"), check.Compatibility, "Read {stream}'s changes", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
				t, err := s.streamTable(ctx, k, db, p, st.Name)
				switch {
				case err != nil:
					return err
				case len(t.key) == 0:
					return noKey.Fail()
				case chunked(t):
					return nil
				}
				for _, i := range t.key {
					if !orderable[t.cols[i].kind] {
						p.Param("column", t.cols[i].name)
						p.Param("type", t.cols[i].columnType)
						break
					}
				}
				return keyUnsupported.Fail()
			})
			c.Params, c.Needs = params, []string{read.Name}
			checks = append(checks, c)
		}
	}
	if !cdc {
		return checks
	}
	return append(append(checks, cdcChecks(k, user)...), retentionCheck(k, req.Interval))
}

// streamTable is the table a stream reads, which the user can select from
// and the source offers, with columns it can read.
func (s *Source) streamTable(ctx context.Context, k *sqlcheck.Kit, db *sql.DB, p *check.Probe, name string) (table, error) {
	if err := selectNothing(ctx, db, s.cfg.Database, name); err != nil {
		switch code, _ := serverError(err); code {
		case 1142, 1143:
			return table{}, check.PrivilegeMissing.Wrap(err, "privilege", "SELECT", "object", quoteIdent(s.cfg.Database)+"."+quoteIdent(name))
		case 1146:
			f := check.NotFound.Wrap(err)
			f.Message, f.Fix = "{stream} isn't a table in {database}.", "Leave {stream} out of the pipeline, or create {table}."
			return table{}, f
		}
		return table{}, k.Failure(err)
	}
	srv, err := serverOf(ctx, db)
	if err != nil {
		return table{}, k.Failure(err)
	}
	ts, err := s.listTables(ctx, db, srv, name)
	if err != nil {
		return table{}, unlisted(k, err)
	}
	t, ok := findTable(ts, name)
	if !ok || len(s.cfg.Tables) > 0 && !slices.Contains(s.cfg.Tables, name) {
		f := check.NotFound.Fail()
		f.Message, f.Fix = "{stream} isn't a table the source offers: it's a view, or tables in the config leaves it out.", "Leave {stream} out of the pipeline, or add it to tables in the config."
		return table{}, f
	}
	return t, unsupported(t, p)
}

// retentionCheck: the server keeps binlog files longer than the pipeline
// goes between syncs.
func retentionCheck(k *sqlcheck.Kit, interval time.Duration) check.Check {
	c := k.Check("cdc.retention", check.Resource, "Keep the binlog between syncs", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		v, _, err := globalVariable(ctx, db, "binlog_expire_logs_seconds")
		if err != nil {
			return k.Failure(err)
		}
		secs, _ := strconv.ParseInt(v, 10, 64)
		if secs <= 0 {
			p.Found("The server doesn't purge binlog files by age.")
			return nil
		}
		keep := time.Duration(secs) * time.Second
		p.Param("retention", span(keep))
		if interval > 0 && keep < interval {
			p.Param("interval", span(interval))
			p.Param("interval_seconds", int64(interval.Seconds()))
			p.Param("suggested_seconds", 2*int64(interval.Seconds()))
			return shortRetention.Fail()
		}
		p.Found("The server keeps binlog files for {retention}.")
		return nil
	})
	c.Description = "A pipeline whose position's binlog file is purged reads its tables again from a new snapshot."
	return c
}

// span is d in days or hours where it's a whole number of them.
func span(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return strconv.FormatInt(int64(d/(24*time.Hour)), 10) + " days"
	case d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + " hours"
	}
	return d.String()
}

// writePrivileges are what every write needs on its database: its staging
// tables are created, indexed, written, read and dropped, and its fence and
// markers updated and deleted.
var writePrivileges = []string{"CREATE", "ALTER", "DROP", "SELECT", "INSERT", "UPDATE", "DELETE"}

// Checks reach the server, sign in, and look at the default database: that
// the user can create and write tables in it, or create it.
func (d *Destination) Checks() []check.Check {
	k := newKit(d.conn)
	c := databaseCheck(k, d.cfg.Database)
	c.Field = "database"
	return append(connectionChecks(d.conn, k, d.cfg.TLS), c)
}

func databaseCheck(k *sqlcheck.Kit, database string) check.Check {
	c := k.Check("database.create", check.Permission, "Create and write tables in {database}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		g, principal, err := userGrants(ctx, db)
		if err != nil {
			return k.Failure(err)
		}
		p.Param("principal", principal)
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", database).Scan(&n); err != nil {
			return k.Failure(err)
		}
		if n == 0 && !g.has("CREATE", database) {
			f := check.PrivilegeMissing.Fail("privilege", "CREATE", "object", quoteIdent(database)+".*")
			f.Message = "{database} doesn't exist, and {principal} has no {privilege} on {object} to create it."
			f.Fix = "Create the database, or run GRANT CREATE ON {object} TO {principal};"
			return f
		}
		if missing := g.missing(writePrivileges, database); len(missing) > 0 {
			f := check.PrivilegeMissing.Fail("privilege", strings.Join(missing, ", "), "object", quoteIdent(database)+".*")
			f.Fix = "Run GRANT {privilege} ON {object} TO {principal};"
			return f
		}
		if n == 0 {
			p.Found("{database} doesn't exist, and the first write creates it.")
		}
		return nil
	})
	c.Params = check.Params{"database": database}
	return c
}

// Preflight looks at the pipeline's database, and at each stream: a merged
// one has a key. database.create asks for what every write mode does in
// the whole database, staging tables included, so it covers each stream's
// table.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	k := newKit(d.conn)
	database := cmp.Or(req.Namespace, d.cfg.Database)
	db := databaseCheck(k, database)
	if req.Namespace == "" {
		db.Field = "database"
	}
	checks := []check.Check{k.Connect(), db}
	n := checkNames{}
	for _, st := range req.Streams {
		table := sqlstage.TableName(st.Name)
		if sch := req.Schemas[st.Name]; sch != nil {
			table = sqlstage.TargetTable(sch, st.Name)
		}
		ident := quoteIdent(database) + "." + quoteIdent(table)
		c := k.Check(n.name("stream", st.Name, "write"), check.Permission, "Write {stream} to {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			if st.Mode == destination.Merge && len(st.PrimaryKey) == 0 {
				f := check.ConfigInvalid.Fail()
				f.Message, f.Fix = "{stream} merges, and has no primary key.", "Give the stream a primary key, or write it with append or overwrite."
				return f
			}
			var exists int
			err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", database, table).Scan(&exists)
			if err != nil {
				return k.Failure(err)
			}
			if exists == 0 {
				p.Found("{table} doesn't exist, and the first write creates it.")
			}
			return nil
		})
		c.Params, c.Needs = check.Params{"stream": st.Name, "table": ident}, []string{"database.create"}
		checks = append(checks, c)
	}
	return checks
}

// Check is Checks run, its first failure as an error.
func (s *Source) Check(ctx context.Context) error { return check.Run(ctx, s.Checks()).Err() }

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }

// grants are the privileges SHOW GRANTS lists for the user, its own and its
// active roles', as both MySQL and MariaDB list a role's.
type grants []grant

type grant struct {
	global bool
	// A database's name, or a pattern of them with % and _ as LIKE has them,
	// for db.*; with table, db.table.
	db, table string
	// In upper case; ALL PRIVILEGES as ALL.
	privileges map[string]bool
}

// userGrants are the user's grants, and the user as a GRANT names it.
func userGrants(ctx context.Context, db *sql.DB) (grants, string, error) {
	var current string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&current); err != nil {
		return nil, "", err
	}
	principal := current
	if i := strings.LastIndex(current, "@"); i >= 0 {
		principal = quoteLiteral(current[:i]) + "@" + quoteLiteral(current[i+1:])
	}
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	var out grants
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, "", err
		}
		if g, ok := parseGrant(line); ok {
			out = append(out, g)
		}
	}
	return out, principal, rows.Err()
}

// parseGrant reads a line of SHOW GRANTS that grants privileges on *.*,
// db.* or db.table; false for any other, such as a role's grant.
func parseGrant(line string) (grant, bool) {
	rest, ok := strings.CutPrefix(line, "GRANT ")
	if !ok {
		return grant{}, false
	}
	// The privileges end at " ON " outside quotes and column lists.
	var privs []string
	depth, quoted, start, end := 0, false, 0, -1
	for i := 0; i < len(rest) && end < 0; i++ {
		switch c := rest[i]; {
		case c == '`':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && c == ',':
			privs, start = append(privs, rest[start:i]), i+1
		case depth == 0 && strings.HasPrefix(rest[i:], " ON "):
			privs, end = append(privs, rest[start:i]), i
		}
	}
	if end < 0 {
		return grant{}, false
	}
	obj := strings.TrimPrefix(rest[end+len(" ON "):], "TABLE ")
	db, star, obj, ok := identifier(obj)
	if !ok || !strings.HasPrefix(obj, ".") {
		return grant{}, false
	}
	table, tableStar, obj, ok := identifier(obj[1:])
	if !ok || !strings.HasPrefix(obj, " TO ") {
		return grant{}, false
	}
	g := grant{global: star && tableStar, privileges: map[string]bool{}}
	switch {
	case star:
	case tableStar:
		g.db = db
	default:
		g.db, g.table = db, table
	}
	if star && !tableStar {
		return grant{}, false
	}
	for _, p := range privs {
		p = strings.ToUpper(strings.TrimSpace(p))
		switch {
		case strings.Contains(p, "("):
			// A column's.
		case p == "ALL" || p == "ALL PRIVILEGES":
			g.privileges["ALL"] = true
		default:
			g.privileges[p] = true
		}
	}
	return g, true
}

// identifier reads a name, quoted with backticks or not, or *, from the start
// of s, and gives what follows it.
func identifier(s string) (name string, star bool, rest string, ok bool) {
	switch {
	case strings.HasPrefix(s, "*"):
		return "", true, s[1:], true
	case strings.HasPrefix(s, "`"):
		var b strings.Builder
		for i := 1; i < len(s); i++ {
			if s[i] != '`' {
				b.WriteByte(s[i])
				continue
			}
			if i+1 < len(s) && s[i+1] == '`' {
				b.WriteByte('`')
				i++
				continue
			}
			return b.String(), false, s[i+1:], true
		}
		return "", false, "", false
	}
	i := strings.IndexAny(s, ". ")
	if i <= 0 {
		return "", false, "", false
	}
	return s[:i], false, s[i:], true
}

// has says whether the user has privilege on every table of db, by a grant
// on it or a pattern that matches it, or globally; only globally for an
// empty db.
func (gs grants) has(privilege, db string) bool {
	for _, g := range gs {
		if !g.privileges[privilege] && (!g.privileges["ALL"] || privilege == "GRANT OPTION" || privilege == "PROXY") {
			continue
		}
		if g.global || db != "" && g.table == "" && like(g.db, db) {
			return true
		}
	}
	return false
}

func (gs grants) missing(privileges []string, db string) []string {
	var out []string
	for _, p := range privileges {
		if !gs.has(p, db) {
			out = append(out, p)
		}
	}
	return out
}

// like matches s against a grant's database pattern: % any run of
// characters, _ any one, and \ escapes the next.
func like(pattern, s string) bool {
	if pattern == "" {
		return s == ""
	}
	switch c := pattern[0]; {
	case c == '%':
		for i := 0; i <= len(s); i++ {
			if like(pattern[1:], s[i:]) {
				return true
			}
		}
		return false
	case s == "":
		return false
	case c == '_':
		return like(pattern[1:], s[1:])
	case c == '\\' && len(pattern) > 1:
		return pattern[1] == s[0] && like(pattern[2:], s[1:])
	}
	return pattern[0] == s[0] && like(pattern[1:], s[1:])
}

func serverError(err error) (uint16, bool) {
	if me, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return me.Number, true
	}
	if me, ok := errors.AsType[*gomysql.MyError](err); ok {
		return me.Code, true
	}
	return 0, false
}

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	n, ok := serverError(err)
	if !ok {
		switch {
		case errors.Is(err, context.Canceled):
			return err
		case errors.Is(err, driver.ErrBadConn), errors.Is(err, mysql.ErrInvalidConn), errors.Is(err, sql.ErrConnDone),
			errors.Is(err, replication.ErrSyncClosed):
			return sdk.WithCode(transientCode, err)
		}
		if _, ok := errors.AsType[net.Error](err); ok {
			return sdk.WithCode(transientCode, err)
		}
		return err
	}
	switch n {
	case 1045, 1698:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case 1044, 1142, 1143, 1227, 1370:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case 1049:
		return sdk.WithCode(configInvalidCode, err)
	case 1236:
		// The binlog the read would continue from is gone.
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, err)
	case 1040, 1053, 1203, 1205, 1213, 1226, 1927, 2006, 2013, 3024:
		return sdk.WithCode(transientCode, err)
	}
	return err
}
