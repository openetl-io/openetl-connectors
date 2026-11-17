package redshift

import (
	"bytes"
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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	"github.com/openetl-io/openetl/sdk/go/check/sqlcheck"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

var (
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
	_ destination.Checker     = (*Destination)(nil)
)

var (
	plaintext = check.Reason{
		Name: "plaintext_connection", Code: protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "{address} doesn't take TLS, so the connection is in plaintext, as the dsn's sslmode allows.",
		Fix:     "Set the cluster's require_ssl parameter to true, and sslmode=verify-full in the dsn.",
	}
	noTLS = check.Reason{
		Name: "tls_refused", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{address} doesn't take TLS, and the dsn's sslmode needs it.",
		Fix:     "Check that the dsn names the cluster's endpoint, or set sslmode=disable in the dsn.",
	}
	notOwned = check.Reason{
		Name: "table_not_owned", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "{principal} doesn't own {table}, so a column the stream gains can't be added to it.",
		Fix:     "Run ALTER TABLE {table} OWNER TO {principal};",
	}
	tooWide = check.Reason{
		Name: "too_many_columns", Code: protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT,
		Message: "{table} would have {columns} columns, and Redshift takes 1600.",
		Fix:     "Write fewer of the stream's columns to {table}, or split the stream.",
	}
	refusedType = check.Reason{
		Name: "type_change_refused", Code: protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT,
		Message: "{table} has {changes}, which the stream would change, and the destination refuses to: the stream holds those columns.",
		Fix:     "Change the columns' types by hand, allow type changes in the pipeline's spec, or map the columns to new names.",
	}
	copyDenied = check.Reason{
		Name: "copy_denied", Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "COPY couldn't read {url} as the IAM role {role}.",
		Fix:     "Associate the role with the cluster or workgroup, let it s3:GetObject and s3:ListBucket on the bucket, and check that no bucket policy refuses S3's presigned URLs, which COPY of Parquet uses.",
	}
)

// newKit makes the checks' database handle from cc, with the session the
// writer's connections have.
func newKit(cc *pgx.ConnConfig) *sqlcheck.Kit {
	return sqlcheck.New(func(context.Context) (*sql.DB, error) {
		return stdlib.OpenDB(*cc, stdlib.OptionAfterConnect(session)), nil
	})
}

// Checks reach the cluster, sign in, and look at the default schema, the
// temporary tables a load needs, and the bucket: that the connector can
// upload a file to it and COPY can load it.
func (d *Destination) Checks() []check.Check {
	cc := d.pool.ConnConfig
	k := newKit(cc)
	checks := append(connectionChecks(cc, k), d.schemaCheck(k, d.cfg.Schema))

	temp := k.True("db.temp", "Create temporary tables in {database}",
		check.PrivilegeMissing.Fail("principal", cc.User, "privilege", "TEMP", "object", "database "+cc.Database), nil,
		`SELECT has_database_privilege(current_database(), 'TEMP')`)
	temp.Params = check.Params{"database": cc.Database}
	checks = append(checks, temp)

	st := &store{bucket: d.cfg.Bucket, prefix: strings.Trim(d.cfg.Prefix, "/")}
	key := st.key("_openetl_check_" + uuid.NewString())
	url := st.url(key)
	write := check.Check{
		Name: "s3.write", Kind: check.Permission, Title: "Upload a file to {url}", Field: "bucket",
		Params: check.Params{"url": url}, Timeout: 30 * time.Second,
		Run: func(ctx context.Context, p *check.Probe) error {
			s, err := d.store(ctx)
			if err != nil {
				return check.Classify(err)
			}
			body, err := probeFile()
			if err != nil {
				return check.Unknown.Wrap(err)
			}
			_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
			if err != nil {
				f := check.Classify(classifyS3(err))
				f.Fix = "Let the uploading identity s3:PutObject and s3:DeleteObject on the bucket's keys under the prefix, and check that region is the bucket's."
				return f
			}
			p.Cleanup(func(ctx context.Context) { s.remove(ctx, key) })
			return nil
		},
	}
	load := k.Check("s3.copy", check.Permission, "Load {url} with COPY", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return k.Failure(err)
		}
		defer func() { _ = tx.Rollback() }()
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT OCTET_LENGTH('\\')`).Scan(&n); err != nil {
			return k.Failure(err)
		}
		dia := dialect{escapes: n == 1}
		for _, q := range []string{
			`CREATE TEMP TABLE "_openetl_check" ("c0" bigint)`,
			`COPY "_openetl_check" FROM ` + dia.lit(url) + " " + d.copyAuth(dia.lit) + " FORMAT AS PARQUET",
		} {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (strings.Contains(pe.Message, "Access Denied") || strings.Contains(pe.Message, "not authorized")) {
					return copyDenied.Wrap(err)
				}
				return k.Failure(err)
			}
		}
		return nil
	})
	load.Field, load.Params = "iamRole", check.Params{"url": url, "role": d.cfg.IAMRole}
	load.Needs = []string{"s3.write", "db.temp"}
	return append(checks, write, load)
}

// probeFile is a Parquet file of one bigint column, c0, and one row.
func probeFile() ([]byte, error) {
	sch := arrow.NewSchema([]arrow.Field{{Name: "c0", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	defer rec.Release()
	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(sch, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	if err := fw.Write(rec); err != nil {
		return nil, err
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// connectionChecks reach the cluster, agree on TLS as the dsn says, and
// sign in: net.dns, net.connect, db.tls and db.connect.
func connectionChecks(cc *pgx.ConnConfig, k *sqlcheck.Kit) []check.Check {
	connect := k.Connect()
	connect.Kind = check.Authentication
	connect.Title = "Sign in to {database} as {user}"
	connect.Params = check.Params{"database": cc.Database, "user": cc.User}
	connect.Field = "dsn"
	addr := net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port)))
	checks := netcheck.Reach(addr)
	for i := range checks {
		checks[i].Field = "dsn"
	}
	connect.Needs = []string{"net.connect"}
	if cc.TLSConfig != nil {
		checks = append(checks, tlsCheck(cc, addr))
		connect.Needs = []string{"db.tls"}
	}
	return append(checks, connect)
}

// tlsCheck is db.tls: the cluster takes TLS, and its certificate is one the
// dsn trusts. Redshift starts TLS inside Postgres's protocol, which
// netcheck.TLS doesn't speak.
func tlsCheck(cc *pgx.ConnConfig, addr string) check.Check {
	plain := slices.ContainsFunc(cc.Fallbacks, func(f *pgconn.FallbackConfig) bool { return f.TLSConfig == nil })
	verified := !cc.TLSConfig.InsecureSkipVerify || cc.TLSConfig.VerifyPeerCertificate != nil
	return check.Check{
		Name: "db.tls", Kind: check.Connection, Title: "Agree on TLS with {address}", Field: "dsn",
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
			case err != nil:
				f := check.TLSFailed.Wrap(err)
				f.Fix = "Check that the dsn's sslrootcert holds the authority that signs the cluster's certificate, Amazon's, and under sslmode=verify-full that the dsn names the cluster's endpoint."
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
				p.Found("{tls_version}, with a certificate the dsn trusts that expires on {expires}.")
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
	tc := tls.Client(conn, cc.TLSConfig)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	st := tc.ConnectionState()
	return &st, nil
}

func (d *Destination) schemaCheck(k *sqlcheck.Kit, schema string) check.Check {
	user := d.pool.ConnConfig.User
	c := k.Check("schema.create", check.Permission, "Create tables in {schema}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
		var create bool
		err := db.QueryRowContext(ctx, `SELECT has_schema_privilege(nspname, 'CREATE') FROM pg_namespace WHERE nspname = $1`, schema).Scan(&create)
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
		case !create:
			return check.PrivilegeMissing.Fail("principal", user, "privilege", "CREATE", "object", "schema "+schema)
		}
		return nil
	})
	c.Field, c.Params = "schema", check.Params{"schema": schema}
	return c
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

// Preflight looks at the pipeline's schema, and at each stream's table: one
// that exists takes what the stream's write mode, and a hold's release, do
// to it; if the user doesn't own it, it has the stream's columns; it has
// room for them; and the types it has are ones the stream's take, or
// widens to when the pipeline allows it. A type it would refuse to change
// only warns, as the stream holds the column.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	cc := d.pool.ConnConfig
	k := newKit(cc)
	schema := cmp.Or(req.Namespace, d.cfg.Schema)
	checks := []check.Check{k.Connect(), d.schemaCheck(k, schema)}
	n := checkNames{}
	for _, st := range req.Streams {
		table := tableName(st.Name)
		sch := req.Schemas[st.Name]
		if sch != nil {
			table = targetTable(sch, st.Name)
		}
		privileges := []string{"SELECT", "INSERT", "UPDATE"}
		if st.Mode != destination.Append {
			privileges = append(privileges, "DELETE")
		}
		ident := quoteIdent(schema) + "." + quoteIdent(table)
		c := k.Check(n.name("stream", st.Name, "write"), check.Permission, "Write {stream} to {table}", func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			if st.Mode == destination.Merge && len(st.PrimaryKey) == 0 {
				f := check.ConfigInvalid.Fail()
				f.Message, f.Fix = "{stream} merges, and has no primary key.", "Give the stream a primary key, or write it with append or overwrite."
				return f
			}
			var want []column
			if sch != nil {
				var err error
				if want, err = columnsOf(sch); err != nil {
					p.Param("error", err.Error())
					f := check.ConfigInvalid.Fail()
					f.Message, f.Fix = "{stream} has a column the destination can't write: {error}.", "Leave the column out of the stream, or cast it to a type the destination takes."
					return f
				}
			}
			var owner bool
			err := db.QueryRowContext(ctx, `SELECT pg_get_userbyid(c.relowner) = current_user OR (SELECT usesuper FROM pg_user WHERE usename = current_user)
				FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`, schema, table).Scan(&owner)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				p.Found("{table} doesn't exist, and the first write creates it.")
				return nil
			case err != nil:
				return k.Failure(err)
			}
			var lacks []string
			for _, priv := range privileges {
				var has bool
				if err := db.QueryRowContext(ctx, `SELECT has_table_privilege($1, $2)`, ident, priv).Scan(&has); err != nil {
					return k.Failure(err)
				}
				if !has {
					lacks = append(lacks, priv)
				}
			}
			if len(lacks) > 0 {
				return check.PrivilegeMissing.Fail("privilege", strings.Join(lacks, ", "), "object", ident)
			}
			rows, err := db.QueryContext(ctx, `SELECT a.attname::varchar, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attisdistkey OR a.attsortkeyord <> 0
				FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped`, schema, table)
			if err != nil {
				return k.Failure(err)
			}
			var have []tableColumn
			for rows.Next() {
				var c tableColumn
				if err := rows.Scan(&c.name, &c.typ, &c.notNull, &c.key); err != nil {
					_ = rows.Close()
					return k.Failure(err)
				}
				c.typ = normType(c.typ)
				have = append(have, c)
			}
			if err := rows.Err(); err != nil {
				return k.Failure(err)
			}
			gains, changes := columnChanges(have, want, st.AllowTypeChanges)
			switch {
			case len(have)+len(gains) > maxColumns:
				p.Param("columns", len(have)+len(gains))
				return tooWide.Fail()
			case !owner && len(gains) > 0:
				p.Param("columns", strings.Join(gains, ", "))
				f := notOwned.Fail()
				f.Message = "{principal} doesn't own {table}, so the columns it lacks, {columns}, can't be added to it."
				return f
			case len(changes) > 0:
				p.Param("changes", strings.Join(changes, ", "))
				return check.Warn(refusedType.Fail())
			case !owner:
				return check.Warn(notOwned.Fail())
			}
			return nil
		})
		c.Params, c.Needs = check.Params{"stream": st.Name, "table": ident, "principal": cc.User}, []string{"schema.create"}
		checks = append(checks, c)
	}
	return checks
}

// columnChanges returns the columns of want that have lacks, and, of those
// it has, each whose type it would refuse to change to want's, as ApplySchema
// does.
func columnChanges(have []tableColumn, want []column, allow bool) (gains, refused []string) {
	for _, c := range want {
		i := slices.IndexFunc(have, func(x tableColumn) bool { return x.name == c.name })
		switch {
		case i < 0:
			gains = append(gains, c.name)
		case have[i].typ == c.typ || widens(c.typ, have[i].typ):
		case allow && widens(have[i].typ, c.typ) && !have[i].key && !have[i].notNull:
		default:
			refused = append(refused, c.name+" as "+have[i].typ+", not "+c.typ)
		}
	}
	return gains, refused
}

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }
