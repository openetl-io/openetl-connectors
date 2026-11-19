package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
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
// mysql://user:password@host:3306/database?param=value.
func parseDSN(dsn string) (*mysql.Config, error) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "mysql" && u.Scheme != "mariadb") {
		c, err := mysql.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("mysql: dsn: %w", err)
		}
		return c, nil
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "3306")
	}
	pass, _ := u.User.Password()
	c, err := mysql.ParseDSN(fmt.Sprintf("tcp(%s)/%s?%s", host, strings.TrimPrefix(u.Path, "/"), u.RawQuery))
	if err != nil {
		return nil, fmt.Errorf("mysql: dsn: %w", err)
	}
	c.User, c.Passwd = u.User.Username(), pass
	return c, nil
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

// connConfig is c as every connection of the connector needs it: values
// interpolated into statements, temporal values as text, which keeps zero
// dates apart from real ones, sessions in UTC, and a SQL mode that stops
// writes of values a column can't hold.
//
// Sessions are in utf8mb4 whatever the DSN or the server say, in the DSN's
// collation if it's one of utf8mb4's, as the escapes of quoteIdent,
// quoteLiteral and the driver need: in a character set such as gbk or sjis,
// a backslash or quote they add can end a multibyte character instead. The
// SQL mode keeps backslash escapes, which quoteLiteral needs. The DSN's own
// settings of these are dropped, in whatever case.
func connConfig(c *mysql.Config) *mysql.Config {
	c = c.Clone()
	c.InterpolateParams = true
	c.ParseTime = false
	c.Loc = time.UTC
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	collation := "utf8mb4_general_ci"
	if strings.HasPrefix(c.Collation, "utf8mb4_") {
		collation = c.Collation
	}
	_ = c.Apply(mysql.Charset("utf8mb4", collation))
	params := map[string]string{}
	for k, v := range c.Params {
		name := strings.TrimPrefix(strings.ToLower(k), "@@")
		switch strings.TrimPrefix(strings.TrimPrefix(name, "session."), "local.") {
		case "time_zone", "sql_mode", "character_set_client", "character_set_connection", "character_set_results", "collation_connection":
		default:
			params[k] = v
		}
	}
	c.Params = params
	c.Params["time_zone"] = "'+00:00'"
	c.Params["sql_mode"] = "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"
	return c
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
	conn, err := mysql.NewConnector(connConfig(c))
	if err != nil {
		return nil, server{}, fmt.Errorf("mysql: dsn: %w", err)
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
