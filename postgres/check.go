package postgres

import (
	"cmp"
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

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

// The relations the source offers, as tables() lists them.
const offered = `c.relkind IN ('r', 'p', 'v', 'm') AND NOT c.relispartition`

var (
	plaintext = check.Reason{
		Name: "plaintext_connection", Code: protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "{address} doesn't take TLS, so the connection is in plaintext, as the dsn's sslmode allows.",
		Fix:     "Turn ssl on in the server's configuration, and set sslmode=verify-full in the dsn.",
	}
	noTLS = check.Reason{
		Name: "tls_refused", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{address} doesn't take TLS, and the config needs it.",
		Fix:     "Turn ssl on in the server's configuration, or set sslmode=disable in the dsn and leave out tlsCa, tlsCert and tlsKey.",
	}
	notOwned = check.Reason{
		Name: "table_not_owned", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "{principal} doesn't own {table}, so a column the stream gains can't be added to it.",
		Fix:     "Run ALTER TABLE {table} OWNER TO {principal};",
	}
	noKey = check.Reason{
		Name: "primary_key_missing", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream} has no primary key, which a cdc read needs.",
		Fix:     "Add a primary key to {table}, or read {stream} with full_refresh or incremental.",
	}
	identity = check.Reason{
		Name: "replica_identity_unsupported", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream}'s replica identity is {identity}, and a cdc read needs DEFAULT or FULL.",
		Fix:     "Run ALTER TABLE {table} REPLICA IDENTITY DEFAULT;",
	}
	badCursor = check.Reason{
		Name: "cursor_unsupported", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream}'s cursor {cursor} is of type {type}, which the source can't read incrementally by.",
		Fix:     "Name a column of type smallint, integer, bigint, numeric, date, timestamp, timestamptz, text or varchar as the cursor.",
	}
	unpublished = check.Reason{
		Name: "table_not_published", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "The publication {publication} lacks {missing}, and {principal} can't add them: that takes owning the publication and the tables.",
		Fix:     "Run ALTER PUBLICATION {publication_sql} ADD TABLE {missing};",
	}
	noPublication = check.Reason{
		Name: "publication_missing", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "The publication {publication} doesn't exist, and {principal} can't create it: that takes CREATE on the database and owning the tables.",
		Fix:     "Run CREATE PUBLICATION {publication_sql} FOR TABLE {tables} WITH (publish_via_partition_root = true);",
	}
	slotUnusable = check.Reason{
		Name: "replication_slot_unusable", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The replication slot {slot} decodes with {plugin} for the database {slot_database}, and the source needs a pgoutput slot of {database}.",
		Fix:     "Drop it, with SELECT pg_drop_replication_slot('{slot}'), or name another slot in slot.",
	}
	slotInvalidated = check.Reason{
		Name: "replication_slot_invalidated", Code: protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST,
		Message: "The replication slot {slot} was invalidated, so a read with a position in it fails with POSITION_LOST until its tables are read again from a new snapshot.",
		Fix:     "Reset the pipeline's positions with openetl reset; then sync more often, or raise max_slot_wal_keep_size, so that the slot keeps up.",
	}
	slotsFull = check.Reason{
		Name: "replication_slots_full", Code: protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED,
		Message: "The server's {max} replication slots are all taken, so the slot {slot} can't be created.",
		Fix:     "Drop the slots no pipeline uses, or raise max_replication_slots and restart the server.",
	}
)

// newKit makes the checks' database handle from cc, the connector's own
// connection config, TLS settings included. Each list has its own, so a run
// opens one and closes it.
func newKit(cc *pgx.ConnConfig) *sqlcheck.Kit {
	return sqlcheck.New(func(context.Context) (*sql.DB, error) { return stdlib.OpenDB(*cc), nil })
}

// connectionChecks reach the server, agree on TLS as the dsn and tlsCa,
// tlsCert and tlsKey say, and sign in: net.dns, net.connect, db.tls and
// db.connect. A Unix socket has only db.connect. Of a dsn with several
// hosts, the first is reached and agreed with, which only advises: the
// connection goes to the first host that takes it, as db.connect's does.
func connectionChecks(cc *pgx.ConnConfig, k *sqlcheck.Kit, t TLS) []check.Check {
	connect := k.Connect()
	connect.Kind = check.Authentication
	connect.Title = "Sign in to {database} as {user}"
	connect.Params = check.Params{"database": cc.Database, "user": cc.User}
	connect.Field = "dsn"
	if strings.HasPrefix(cc.Host, "/") {
		return []check.Check{connect}
	}
	several := slices.ContainsFunc(cc.Fallbacks, func(f *pgconn.FallbackConfig) bool { return f.Host != cc.Host || f.Port != cc.Port })
	addr := net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port)))
	checks := netcheck.Reach(addr)
	connect.Needs = []string{"net.connect"}
	if cc.TLSConfig != nil {
		field := "dsn"
		if t.CA != "" {
			field = "tlsCa"
		}
		checks = append(checks, tlsCheck(cc, addr, field))
		connect.Needs = []string{"db.tls"}
	}
	for i := range checks {
		checks[i].Advisory = several
		if checks[i].Field == "" {
			checks[i].Field = "dsn"
		}
	}
	return append(checks, connect)
}

// tlsCheck is db.tls: the server takes TLS, and its certificate is one the
// config trusts. Postgres starts TLS inside its own protocol, which
// netcheck.TLS doesn't speak.
func tlsCheck(cc *pgx.ConnConfig, addr, field string) check.Check {
	plain := slices.ContainsFunc(cc.Fallbacks, func(f *pgconn.FallbackConfig) bool { return f.TLSConfig == nil })
	verified := !cc.TLSConfig.InsecureSkipVerify || cc.TLSConfig.VerifyPeerCertificate != nil
	return check.Check{
		Name: "db.tls", Kind: check.Connection, Title: "Agree on TLS with {address}", Field: field,
		Params: check.Params{"address": addr}, Needs: []string{"net.connect"}, Timeout: netcheck.DialTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			st, err := handshake(ctx, cc, addr)
			switch {
			case errors.Is(err, errNoTLS) && plain:
				return check.Warn(plaintext.Fail())
			case errors.Is(err, errNoTLS):
				return noTLS.Fail()
			case err != nil && ctx.Err() != nil:
				return check.TimedOut.Wrap(err)
			case isDial(err):
				return check.Classify(err).With("address", addr)
			case err != nil:
				f := check.TLSFailed.Wrap(err)
				f.Fix = "Check that tlsCa, or the dsn's sslrootcert, holds the authority that signs the server's certificate, and under sslmode=verify-full that the certificate names the dsn's host."
				return f
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
				p.Found("{tls_version}, with a certificate that expires on {expires}, which the dsn's sslmode doesn't verify.")
			}
			if time.Until(expires) < netcheck.ExpiresSoon {
				return check.Warn(netcheck.CertificateExpiring.Fail())
			}
			return nil
		},
	}
}

var errNoTLS = errors.New("the server doesn't take TLS")

// isDial says err is a failure to reach the server, before any TLS.
func isDial(err error) bool {
	oe, ok := errors.AsType[*net.OpError](err)
	return ok && oe.Op == "dial"
}

// handshake asks the server at addr for TLS, as pgconn does, and makes the
// handshake with cc's TLS config.
func handshake(ctx context.Context, cc *pgx.ConnConfig, addr string) (*tls.ConnectionState, error) {
	conn, err := cc.DialFunc(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	}
	if cc.SSLNegotiation != "direct" {
		// SSLRequest: its length, 8, and the code 80877103.
		if _, err := conn.Write([]byte{0, 0, 0, 8, 4, 210, 22, 47}); err != nil {
			return nil, err
		}
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			return nil, err
		}
		if b[0] != 'S' {
			return nil, errNoTLS
		}
	}
	tc := tls.Client(conn, cc.TLSConfig)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	st := tc.ConnectionState()
	return &st, nil
}

// checkNames gives each check a name from parts, made distinct from those it
// gave before: streams whose names differ only in case or punctuation get
// the same check.Name.
type checkNames map[string]int

func (n checkNames) name(parts ...string) string {
	name := check.Name(parts...)
	n[name]++
	if c := n[name]; c > 1 {
		name += "-" + strconv.Itoa(c)
	}
	return name
}

// Checks reach the server, sign in, and look at the schema's tables, and
// at what cdc needs, which only warns: whether a pipeline reads with cdc is
// its preflight's to say.
func (s *Source) Checks() []check.Check {
	k := newKit(s.conn)
	user, schema := s.conn.User, s.cfg.Schema
	checks := connectionChecks(s.conn, k, s.cfg.TLS)

	usage := k.True("schema.usage", "Use the schema {schema}",
		check.PrivilegeMissing.Fail("principal", user, "privilege", "USAGE", "object", "schema "+schema),
		check.NotFound.Fail("resource", "The schema "+schema),
		`SELECT has_schema_privilege(oid, 'USAGE') FROM pg_namespace WHERE nspname = $1`, schema)
	usage.Kind, usage.Field, usage.Params = check.Resource, "schema", check.Params{"schema": schema}
	checks = append(checks, usage)

	if len(s.cfg.Tables) > 0 {
		found := k.Check("tables.found", check.Resource, "Find the tables in {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			var missing sql.NullString
			err := db.QueryRowContext(ctx, `SELECT string_agg(t, ', ' ORDER BY t) FROM unnest($2::text[]) t
				WHERE NOT EXISTS (SELECT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = t AND `+offered+`)`,
				schema, s.cfg.Tables).Scan(&missing)
			if err != nil {
				return k.Failure(err)
			}
			if missing.Valid {
				p.Param("missing", missing.String)
				f := check.NotFound.Fail()
				f.Message, f.Fix = "{missing} isn't a table or view in {schema}.", "Correct tables in the config, or create what it names."
				return f
			}
			return nil
		})
		found.Params, found.Field, found.Needs = check.Params{"schema": schema}, "tables", []string{"schema.usage"}
		checks = append(checks, found)
	}

	read := k.Check("tables.read", check.Permission, "Read the tables and views of {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var n int
		var unreadable sql.NullString
		err := db.QueryRowContext(ctx, `SELECT count(*), string_agg(format('%I.%I', n.nspname, c.relname), ', ' ORDER BY c.relname) FILTER (WHERE NOT has_table_privilege(c.oid, 'SELECT'))
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND `+offered+` AND (coalesce(cardinality($2::text[]), 0) = 0 OR c.relname = ANY($2))`, schema, s.cfg.Tables).Scan(&n, &unreadable)
		if err != nil {
			return k.Failure(err)
		}
		if unreadable.Valid {
			return check.PrivilegeMissing.Fail("principal", user, "privilege", "SELECT", "object", unreadable.String)
		}
		p.Param("count", n)
		p.Found("{user} can read the {count} tables and views it's offered.")
		return nil
	})
	read.Params, read.Field, read.Needs = check.Params{"schema": schema, "user": user}, "tables", []string{"schema.usage"}
	checks = append(checks, read)

	// What cdc needs only warns here.
	for _, c := range cdcChecks(k, user) {
		c.Advisory = true
		c.Description = "Needed only to read tables with cdc: a pipeline's preflight fails without it if it does."
		checks = append(checks, c)
	}
	return checks
}

// cdcChecks are what a cdc read needs of the server and the user, besides
// tables and their publication and slot.
func cdcChecks(k *sqlcheck.Kit, user string) []check.Check {
	wal := k.Value("cdc.wal-level", "Decode the write-ahead log", func(v string) error {
		if v != "logical" {
			f := check.SettingMismatch.Fail("setting", "wal_level", "required", "logical")
			f.Fix = "Set wal_level = logical in the server's configuration, and restart the server."
			return f
		}
		return nil
	}, `SHOW wal_level`)

	denied := check.PrivilegeMissing.Fail("principal", user, "privilege", "REPLICATION", "object", "the server")
	denied.Message = "{principal} has neither the REPLICATION attribute nor the rds_replication role."
	denied.Fix = "Run ALTER ROLE {principal} WITH REPLICATION; or on Amazon RDS, GRANT rds_replication TO {principal};"
	repl := k.True("cdc.replication", "Stream changes as {user}", denied, nil,
		`SELECT rolreplication OR rolsuper OR EXISTS (SELECT FROM pg_roles g WHERE g.rolname = 'rds_replication' AND pg_has_role(r.oid, g.oid, 'MEMBER'))
		FROM pg_roles r WHERE r.rolname = current_user`)
	repl.Params, repl.Field = check.Params{"user": user}, "dsn"
	return []check.Check{wal, repl}
}

// Preflight looks at each stream as the pipeline reads it: there and
// readable, its cursor one the source reads by, and for cdc, its key and
// replica identity, and the server, publication and slot the read needs.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := newKit(s.conn)
	user, schema := s.conn.User, s.cfg.Schema
	checks := []check.Check{k.Connect()}
	n := checkNames{}
	var cdc []string
	for _, st := range req.Streams {
		params := check.Params{"stream": st.Name, "schema": schema, "table": pgx.Identifier{schema, st.Name}.Sanitize()}
		missing := check.NotFound.Fail()
		missing.Message = "{stream} isn't a table or view in {schema}."
		read := k.True(n.name("stream", st.Name, "read"), "Read {stream}",
			check.PrivilegeMissing.Fail("principal", user, "privilege", "SELECT", "object", params["table"]), missing,
			`SELECT has_table_privilege(c.oid, 'SELECT') FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2 AND `+offered, schema, st.Name)
		read.Params = params
		checks = append(checks, read)
		switch st.SyncMode {
		case source.Incremental:
			c := k.Check(n.name("stream", st.Name, "cursor"), check.Compatibility, "Read {stream} by {cursor}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
				var oid uint32
				var typ string
				err := db.QueryRowContext(ctx, `SELECT a.atttypid, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
					JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
					WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped`, schema, st.Name, st.Cursor).Scan(&oid, &typ)
				switch {
				case errors.Is(err, sql.ErrNoRows):
					f := check.NotFound.Fail()
					f.Message, f.Fix = "{stream} has no column {cursor}.", "Name one of its columns as the stream's cursor."
					return f
				case err != nil:
					return k.Failure(err)
				}
				p.Param("type", typ)
				if !cursorTypes[oid] {
					return badCursor.Fail()
				}
				return nil
			})
			c.Params, c.Needs = check.Params{"stream": st.Name, "cursor": st.Cursor}, []string{read.Name}
			checks = append(checks, c)
		case source.CDC:
			cdc = append(cdc, st.Name)
			c := k.Check(n.name("stream", st.Name, "cdc"), check.Compatibility, "Read {stream}'s changes", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
				var ident string
				var key bool
				err := db.QueryRowContext(ctx, `SELECT c.relreplident::text, EXISTS (SELECT FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
					FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`, schema, st.Name).Scan(&ident, &key)
				if err != nil {
					return k.Failure(err)
				}
				if !key {
					return noKey.Fail()
				}
				if ident != "d" && ident != "f" {
					p.Param("identity", map[string]string{"n": "NOTHING", "i": "USING INDEX"}[ident])
					return identity.Fail()
				}
				return nil
			})
			c.Params, c.Needs = params, []string{read.Name}
			checks = append(checks, c)
		}
	}
	if len(cdc) == 0 {
		return checks
	}
	keep := k.Value("cdc.wal-keep", "Bound the log a slot keeps", func(v string) error {
		if v == "-1" {
			f := check.SettingMismatch.Fail("setting", "max_slot_wal_keep_size", "required", "a limit, such as 100GB")
			f.Fix = "Set max_slot_wal_keep_size to a limit the server's disk has room for, such as 100GB."
			return f
		}
		return nil
	}, `SHOW max_slot_wal_keep_size`)
	keep.Advisory = true
	keep.Description = "A paused or failing pipeline's slot keeps the write-ahead log on the server's disk, up to this limit."
	checks = append(checks, cdcChecks(k, user)...)
	return append(checks, keep, s.publicationCheck(k, cdc), s.slotCheck(k, s.slotName(req.Pipeline)))
}

// publicationCheck: the publication has the tables, or the source can
// create it or add them, as ensurePublication does.
func (s *Source) publicationCheck(k *sqlcheck.Kit, tables []string) check.Check {
	pub, schema, user := s.cfg.Publication, s.cfg.Schema, s.conn.User
	idents := make([]string, len(tables))
	for i, t := range tables {
		idents[i] = pgx.Identifier{schema, t}.Sanitize()
	}
	c := k.Check("cdc.publication", check.Permission, "Publish the tables read with cdc in {publication}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		// Whether the user owns every table in want, as creating a
		// publication of them or adding them to one takes.
		owns := func(want []string) (bool, error) {
			var ok bool
			err := db.QueryRowContext(ctx, `SELECT coalesce(bool_and(pg_has_role(c.relowner, 'USAGE')), true) FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = ANY($2)`, schema, want).Scan(&ok)
			return ok, err
		}
		var all, owner bool
		err := db.QueryRowContext(ctx, `SELECT puballtables, pg_has_role(pubowner, 'USAGE') FROM pg_publication WHERE pubname = $1`, pub).Scan(&all, &owner)
		if errors.Is(err, sql.ErrNoRows) {
			var create bool
			if err := db.QueryRowContext(ctx, `SELECT has_database_privilege(current_database(), 'CREATE')`).Scan(&create); err != nil {
				return k.Failure(err)
			}
			mine, err := owns(tables)
			if err != nil {
				return k.Failure(err)
			}
			if !create || !mine {
				return noPublication.Fail()
			}
			p.Found("{publication} doesn't exist, and the first read creates it.")
			return nil
		}
		switch {
		case err != nil:
			return k.Failure(err)
		case all:
			p.Found("{publication} publishes every table.")
			return nil
		}
		var missing []string
		rows, err := db.QueryContext(ctx, `SELECT t FROM unnest($3::text[]) t WHERE NOT EXISTS
			(SELECT FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = t) ORDER BY t`, pub, schema, tables)
		if err != nil {
			return k.Failure(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				return k.Failure(err)
			}
			missing = append(missing, t)
		}
		if err := rows.Err(); err != nil {
			return k.Failure(err)
		}
		if len(missing) == 0 {
			p.Found("{publication} publishes the tables.")
			return nil
		}
		quoted := make([]string, len(missing))
		for i, t := range missing {
			quoted[i] = pgx.Identifier{schema, t}.Sanitize()
		}
		p.Param("missing", strings.Join(quoted, ", "))
		mine, err := owns(missing)
		if err != nil {
			return k.Failure(err)
		}
		if !owner || !mine {
			return unpublished.Fail()
		}
		p.Found("{publication} lacks {missing}, and the first read adds them.")
		return nil
	})
	c.Field = "publication"
	c.Params = check.Params{"publication": pub, "publication_sql": pgx.Identifier{pub}.Sanitize(), "tables": strings.Join(idents, ", "), "principal": user}
	return c
}

// slotCheck: the pipeline's slot is a pgoutput slot of this database, or
// the server has room to create it.
func (s *Source) slotCheck(k *sqlcheck.Kit, slot string) check.Check {
	c := k.Check("cdc.slot", check.Resource, "Read through the replication slot {slot}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var plugin, database, current string
		var lost bool
		err := db.QueryRowContext(ctx, `SELECT coalesce(plugin, ''), coalesce(database, ''), current_database(),
			coalesce(wal_status = 'lost', false) OR (to_jsonb(s) ->> 'invalidation_reason') IS NOT NULL
			FROM pg_replication_slots s WHERE slot_name = $1`, slot).Scan(&plugin, &database, &current, &lost)
		if errors.Is(err, sql.ErrNoRows) {
			var free bool
			var most string
			err := db.QueryRowContext(ctx, `SELECT count(*) < current_setting('max_replication_slots')::int, current_setting('max_replication_slots') FROM pg_replication_slots`).Scan(&free, &most)
			if err != nil {
				return k.Failure(err)
			}
			p.Param("max", most)
			if !free {
				return slotsFull.Fail()
			}
			p.Found("{slot} doesn't exist, and the first read creates it.")
			return nil
		}
		if err != nil {
			return k.Failure(err)
		}
		p.Param("plugin", plugin)
		p.Param("slot_database", database)
		p.Param("database", current)
		switch {
		case plugin != "pgoutput" || database != current:
			return slotUnusable.Fail()
		case lost:
			return check.Warn(slotInvalidated.Fail())
		}
		p.Found("{slot} is a pgoutput slot of {database}.")
		return nil
	})
	c.Field, c.Params = "slot", check.Params{"slot": slot}
	return c
}

// Checks reach the server, sign in, and look at the default schema: that
// the user can create tables in it, or create it.
func (d *Destination) Checks() []check.Check {
	cc := d.pool.ConnConfig
	k := newKit(cc)
	return append(connectionChecks(cc, k, d.cfg.TLS), d.schemaCheck(k, d.cfg.Schema))
}

func (d *Destination) schemaCheck(k *sqlcheck.Kit, schema string) check.Check {
	user := d.pool.ConnConfig.User
	c := k.Check("schema.create", check.Permission, "Create tables in {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var create bool
		var lacks sql.NullString
		err := db.QueryRowContext(ctx, `SELECT (SELECT string_agg(p, ', ') FROM unnest(ARRAY['USAGE', 'CREATE']) p WHERE NOT has_schema_privilege(n.oid, p))
			FROM pg_namespace n WHERE nspname = $1`, schema).Scan(&lacks)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			var database string
			if err := db.QueryRowContext(ctx, `SELECT has_database_privilege(current_database(), 'CREATE'), current_database()`).Scan(&create, &database); err != nil {
				return k.Failure(err)
			}
			if !create {
				f := check.PrivilegeMissing.Fail("principal", user, "privilege", "CREATE", "object", "database "+database)
				f.Message = "{schema} doesn't exist, and {principal} has no {privilege} on {object} to create it."
				return f
			}
			p.Found("{schema} doesn't exist, and the first write creates it.")
			return nil
		case err != nil:
			return k.Failure(err)
		case lacks.Valid:
			return check.PrivilegeMissing.Fail("principal", user, "privilege", lacks.String, "object", "schema "+schema)
		}
		return nil
	})
	c.Field, c.Params = "schema", check.Params{"schema": schema}
	return c
}

// Preflight looks at the pipeline's schema, and at each stream's table:
// one that exists takes what the stream's write mode does to it, and if the
// user doesn't own it, it has the stream's columns, to merge into, a
// unique index on the key, and to append with target deduplication, an
// index on _openetl_id. A hold's release updates any mode's table, which
// this doesn't ask of append and overwrite.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	cc := d.pool.ConnConfig
	k := newKit(cc)
	schema := cmp.Or(req.Namespace, d.cfg.Schema)
	user := cc.User
	checks := []check.Check{k.Connect(), d.schemaCheck(k, schema)}
	n := checkNames{}
	for _, st := range req.Streams {
		table, key := sqlstage.TableName(st.Name), st.PrimaryKey
		var fields []string
		// The schema's lack of the key, which fails a merge's writes.
		var keyErr error
		if sch := req.Schemas[st.Name]; sch != nil {
			table = sqlstage.TargetTable(sch, st.Name)
			if k, err := sqlstage.KeyColumns(sch, st.PrimaryKey); err == nil {
				key = k
			} else {
				keyErr = err
			}
			for _, f := range sch.Fields() {
				fields = append(fields, f.Name)
			}
		}
		// SELECT where an apply reads the target's rows: a merge's upsert and
		// delete, and an append's skip of the IDs the target has.
		privileges := []string{"INSERT"}
		switch {
		case st.Mode == destination.Merge:
			privileges = append(privileges, "SELECT", "UPDATE", "DELETE")
		case st.Mode == destination.Overwrite:
			privileges = append(privileges, "DELETE")
		case st.Dedupe == destination.DedupeTarget:
			privileges = append(privileges, "SELECT")
		}
		ident := pgx.Identifier{schema, table}.Sanitize()
		c := k.Check(n.name("stream", st.Name, "write"), check.Permission, "Write {stream} to {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			if st.Mode == destination.Merge && len(st.PrimaryKey) == 0 {
				f := check.ConfigInvalid.Fail()
				f.Message, f.Fix = "{stream} merges, and has no primary key.", "Give the stream a primary key, or write it with append or overwrite."
				return f
			}
			if st.Mode == destination.Merge && keyErr != nil {
				f := check.ConfigInvalid.Wrap(keyErr)
				f.Message, f.Fix = "{stream} merges on a primary key that isn't among its columns.", "Give the stream a primary key of its columns, or write it with append or overwrite."
				return f
			}
			// What the table lacks of the privileges and the stream's columns,
			// whether it has the unique index on the key that ensureKey
			// would otherwise create, and whether it has an _openetl_id index
			// for target-deduped append.
			var lacks, gains sql.NullString
			var owner, keyed, dedupeIndexed bool
			err := db.QueryRowContext(ctx, `SELECT
					(SELECT string_agg(p, ', ') FROM unnest($3::text[]) p WHERE NOT has_table_privilege(c.oid, p)),
					(SELECT string_agg(f, ', ') FROM unnest($4::text[]) f WHERE NOT EXISTS
						(SELECT FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = f AND a.attnum > 0 AND NOT a.attisdropped)),
					pg_has_role(c.relowner, 'USAGE'),
					EXISTS (SELECT FROM pg_index i WHERE i.indrelid = c.oid AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL
						AND i.indnkeyatts = cardinality($5::text[])
						AND (SELECT array_agg(a.attname::text ORDER BY a.attname) FROM pg_attribute a
							WHERE a.attrelid = i.indrelid AND a.attnum = ANY ((i.indkey::int2[])[0:i.indnkeyatts - 1]))
							= (SELECT array_agg(k ORDER BY k) FROM unnest($5::text[]) k)),
					EXISTS (SELECT FROM pg_index i
						JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = (i.indkey::int2[])[0]
						WHERE i.indrelid = c.oid AND i.indisvalid AND i.indpred IS NULL AND i.indnkeyatts >= 1
							AND a.attname = $6 AND a.attnum > 0 AND NOT a.attisdropped)
				FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')`, schema, table, privileges, fields, key, idColumn).Scan(&lacks, &gains, &owner, &keyed, &dedupeIndexed)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				p.Found("{table} doesn't exist, and the first write creates it.")
				return nil
			case err != nil:
				return k.Failure(err)
			case lacks.Valid:
				return check.PrivilegeMissing.Fail("privilege", lacks.String, "object", ident)
			case owner:
				return nil
			case gains.Valid:
				p.Param("columns", gains.String)
				f := notOwned.Fail()
				f.Message = "{principal} doesn't own {table}, so the columns it lacks, {columns}, can't be added to it."
				return f
			case st.Mode == destination.Merge && !keyed:
				f := notOwned.Fail()
				f.Message = "{principal} doesn't own {table}, so the unique index on its key that a merge needs can't be created."
				f.Fix = "Run ALTER TABLE {table} OWNER TO {principal}; or create a unique index on the stream's key."
				return f
			case st.Mode == destination.Append && st.Dedupe == destination.DedupeTarget && !dedupeIndexed:
				p.Param("index", pgx.Identifier{sqlstage.IndexName(table)}.Sanitize())
				f := notOwned.Fail()
				f.Message = "{principal} doesn't own {table}, so the index on _openetl_id that target deduplication needs can't be created."
				f.Fix = "Run ALTER TABLE {table} OWNER TO {principal}; or CREATE INDEX {index} ON {table} (\"_openetl_id\");"
				return f
			}
			return check.Warn(notOwned.Fail())
		})
		c.Params, c.Needs = check.Params{"stream": st.Name, "table": ident, "principal": user}, []string{"schema.create"}
		checks = append(checks, c)
	}
	return checks
}

// Check is Checks run, its first failure as an error.
func (s *Source) Check(ctx context.Context) error { return check.Run(ctx, s.Checks()).Err() }

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it TRANSIENT.
func classify(err error) error {
	pe, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		return err
	}
	switch {
	case pe.Code == "28P01" || pe.Code == "28000":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case pe.Code == "3D000":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	case pe.Code == "42501":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case strings.HasPrefix(pe.Code, "08") || pe.Code == "53300" || pe.Code == "57P03":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
