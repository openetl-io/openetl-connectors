package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

// parseDSN reads a DSN as the Go driver spells it,
// user:password@tcp(host:3306)/database?param=value, or as a URL,
// mysql://user:password@host:3306/database?param=value. It refuses a
// parameter the driver doesn't take that doesn't set a session variable,
// and a collation that isn't a name.
func parseDSN(dsn string) (*mysql.Config, error) {
	u, err := url.Parse(dsn)
	var c *mysql.Config
	if err != nil || (u.Scheme != "mysql" && u.Scheme != "mariadb") {
		if c, err = mysql.ParseDSN(dsn); err != nil {
			return nil, fmt.Errorf("mysql: dsn: %w", err)
		}
	} else {
		host := u.Host
		if u.Port() == "" {
			host = net.JoinHostPort(u.Hostname(), "3306")
		}
		pass, _ := u.User.Password()
		if c, err = mysql.ParseDSN(fmt.Sprintf("tcp(%s)/%s?%s", host, strings.TrimPrefix(u.Path, "/"), u.RawQuery)); err != nil {
			return nil, fmt.Errorf("mysql: dsn: %w", err)
		}
		c.User, c.Passwd = u.User.Username(), pass
	}
	for k := range c.Params {
		if _, ok := sessionVariable(k); !ok {
			return nil, fmt.Errorf("mysql: dsn: the parameter %q doesn't set a session variable, as wait_timeout or @@session.wait_timeout would", k)
		}
	}
	if c.Collation != "" && !plainName(strings.ToLower(c.Collation)) {
		return nil, fmt.Errorf("mysql: dsn: the collation %q isn't a collation's name", c.Collation)
	}
	return c, nil
}

// sessionVariable is the system variable that a DSN parameter's key sets
// in the session, as SET spells it: lowercase, without @@ and SESSION or
// LOCAL, dotted or as a keyword. ok is false for any other key, such as a
// global or a user variable, or one with more than a name in it.
func sessionVariable(key string) (name string, ok bool) {
	name = strings.ToLower(strings.TrimSpace(key))
	if rest, found := strings.CutPrefix(name, "@@"); found {
		name = rest
		for _, scope := range []string{"session.", "local."} {
			if rest, found := strings.CutPrefix(name, scope); found {
				name = rest
				break
			}
		}
	} else if f := strings.Fields(name); len(f) == 2 && (f[0] == "session" || f[0] == "local") {
		name = f[1]
	}
	return name, plainName(name)
}

func plainName(s string) bool {
	return s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyz0123456789_") == ""
}

// TLSConfig is how connections use TLS, in place of the DSN's tls parameter.
type TLSConfig struct {
	// disabled, preferred (TLS if the server has it, unverified), required
	// (unverified), verify-ca (the certificate's chain) or verify-identity
	// (its chain and host name). Default verify-identity.
	Mode string `json:"mode"`
	// PEM certificates of the authorities that sign the server's. Empty: the
	// system's.
	CA string `json:"ca"`
	// A client certificate and its key, PEM, for users that must present one.
	Cert string     `json:"cert"`
	Key  sdk.Secret `json:"key"`
}

// applyTLS sets c's TLS as t says; nil leaves the DSN's.
func applyTLS(c *mysql.Config, t *TLSConfig) error {
	if t == nil {
		return nil
	}
	if t.Mode == "disabled" {
		c.TLS, c.TLSConfig, c.AllowFallbackToPlaintext = nil, "false", false
		return nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CA != "" {
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM([]byte(t.CA)) {
			return errors.New("mysql: tls.ca has no PEM certificate")
		}
	}
	if t.Cert != "" || !t.Key.IsZero() {
		pair, err := tls.X509KeyPair([]byte(t.Cert), []byte(t.Key.Reveal()))
		if err != nil {
			return fmt.Errorf("mysql: tls.cert and tls.key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	switch t.Mode {
	case "preferred", "required":
		cfg.InsecureSkipVerify = true
	case "verify-ca":
		// Go verifies a chain only with the host name, so the chain is
		// verified here instead.
		cfg.InsecureSkipVerify = true
		roots := cfg.RootCAs
		cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(raw))
			for i, b := range raw {
				c, err := x509.ParseCertificate(b)
				if err != nil {
					return err
				}
				certs[i] = c
			}
			if len(certs) == 0 {
				return errors.New("mysql: the server sent no certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, c := range certs[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	case "", "verify-identity":
		cfg.ServerName, _, _ = net.SplitHostPort(c.Addr)
	default:
		return fmt.Errorf("mysql: tls.mode %q isn't disabled, preferred, required, verify-ca or verify-identity", t.Mode)
	}
	c.TLS, c.TLSConfig, c.AllowFallbackToPlaintext = cfg, "", t.Mode == "preferred"
	return nil
}

// connector makes c's connections, each in the session sessionSQL sets.
func connector(c *mysql.Config) (driver.Connector, error) {
	cfg, err := connConfig(c)
	if err != nil {
		return nil, err
	}
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: dsn: %w", err)
	}
	return sessionConnector{Connector: conn, set: sessionSQL(c)}, nil
}

// sessionConnector runs set on each connection once the driver has set it
// up, its DSN's parameters included, so that set has the last word.
type sessionConnector struct {
	driver.Connector
	set string
}

func (s sessionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := s.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	ex, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("mysql: the driver's connection doesn't run statements")
	}
	if _, err := ex.ExecContext(ctx, s.set, nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// sessionSQL sets what every session of the connector needs: utf8mb4, in
// the DSN's collation if it's one of utf8mb4's, as the escapes of
// quoteIdent, quoteLiteral and the driver need (in a character set such as
// gbk or sjis, a backslash or quote they add can end a multibyte character
// instead); UTC; and a SQL mode that stops writes of values a column can't
// hold and keeps backslash escapes, which quoteLiteral needs. The collation
// is set here and not in the handshake, which takes only those the driver
// knows.
func sessionSQL(c *mysql.Config) string {
	collation := strings.ToLower(c.Collation)
	if !strings.HasPrefix(collation, "utf8mb4_") || !plainName(collation) {
		collation = "utf8mb4_general_ci"
	}
	return "SET NAMES utf8mb4 COLLATE " + collation + ", time_zone = '+00:00', sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"
}

// connConfig is c as every connection of the connector needs it: values
// interpolated into statements, temporal values as text, which keeps zero
// dates apart from real ones, a handshake and a SET NAMES in utf8mb4 that
// leave out the DSN's charset and collation, and the DSN's parameters, each
// spelled as the session variable it sets, but for those sessionSQL sets.
func connConfig(c *mysql.Config) (*mysql.Config, error) {
	c = c.Clone()
	c.InterpolateParams = true
	c.ParseTime = false
	c.Loc = time.UTC
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if err := c.Apply(mysql.Charset("utf8mb4", "")); err != nil {
		return nil, fmt.Errorf("mysql: dsn: %w", err)
	}
	params := map[string]string{}
	for k, v := range c.Params {
		name, ok := sessionVariable(k)
		if !ok {
			return nil, fmt.Errorf("mysql: dsn: the parameter %q doesn't set a session variable", k)
		}
		switch name {
		case "time_zone", "sql_mode", "character_set_client", "character_set_connection", "character_set_results", "collation_connection":
		default:
			params[name] = v
		}
	}
	c.Params = params
	return c, nil
}

// server is what differs between MySQL and MariaDB.
type server struct {
	mariadb bool
	// The binary, case- and space-sensitive collation of utf8mb4 that the
	// destination's tables take, so that keys differing only in case or
	// trailing spaces stay apart.
	collation string
}

func open(ctx context.Context, c *mysql.Config, conns int) (*sql.DB, server, error) {
	conn, err := connector(c)
	if err != nil {
		return nil, server{}, err
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	db.SetConnMaxIdleTime(time.Minute)
	s, err := serverOf(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, server{}, classify(fmt.Errorf("mysql: connect: %w", err))
	}
	return db, s, nil
}

func serverOf(ctx context.Context, db *sql.DB) (server, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return server{}, err
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return server{mariadb: true, collation: "utf8mb4_nopad_bin"}, nil
	}
	return server{collation: "utf8mb4_0900_bin"}, nil
}

// quoteIdent quotes a name for MySQL.
func quoteIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// quoteLiteral quotes s as a string literal, for sessions whose SQL mode
// keeps backslash escapes, as connConfig's does.
func quoteLiteral(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\x00", `\0`, "\n", `\n`, "\r", `\r`, "\x1a", `\Z`)
	return "'" + r.Replace(s) + "'"
}

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = quoteIdent(n)
	}
	return strings.Join(q, ", ")
}
