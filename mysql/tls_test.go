//go:build integration

package mysql

import (
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// containerFile is a file in a test server's container, which docker reads.
func containerFile(t *testing.T, container, path string) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "docker", "exec", container, "cat", path).Output()
	if err != nil {
		t.Skipf("docker can't read %s in the container %s: %v", path, container, err)
	}
	return string(out)
}

// cipher is the TLS cipher of a connection to dsn with tc; empty without TLS.
func cipher(t *testing.T, dsn string, tc *TLSConfig) (string, error) {
	t.Helper()
	c, err := parseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyTLS(c, tc); err != nil {
		return "", err
	}
	db, _, err := open(t.Context(), c, 1)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()
	var name, value string
	err = db.QueryRowContext(t.Context(), "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &value)
	return value, err
}

// otherCA is a CA's certificate that signed neither server's.
func otherCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "another CA"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// TestTLSOnMySQL connects to MySQL with each TLS mode, verifying its
// certificate with the CA it generated, and as a user that must present a
// client certificate, which the binlog connection presents too.
func TestTLSOnMySQL(t *testing.T) {
	srv := servers()[0]
	container := cmp.Or(os.Getenv("OPENETL_TEST_MYSQL_CONTAINER"), "mysql")
	ca := containerFile(t, container, "/var/lib/mysql/ca.pem")
	cert := containerFile(t, container, "/var/lib/mysql/client-cert.pem")
	key := containerFile(t, container, "/var/lib/mysql/client-key.pem")
	for _, c := range []struct {
		tc  *TLSConfig
		tls bool
		err string
	}{
		{&TLSConfig{Mode: "disabled"}, false, ""},
		{&TLSConfig{Mode: "preferred"}, true, ""},
		{&TLSConfig{Mode: "required"}, true, ""},
		{&TLSConfig{Mode: "verify-ca", CA: ca}, true, ""},
		// MySQL's certificate names no host.
		{&TLSConfig{CA: ca}, false, "certificate"},
		{&TLSConfig{Mode: "verify-ca", CA: otherCA(t)}, false, "certificate signed by unknown authority"},
	} {
		got, err := cipher(t, srv.dsn, c.tc)
		switch {
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%+v: %v, not an error that says %q", c.tc, err, c.err)
		case c.err == "" && err != nil:
			t.Errorf("%+v: %v", c.tc, err)
		case c.err == "" && (got != "") != c.tls:
			t.Errorf("%+v: cipher %q", c.tc, got)
		}
	}

	expectTLSItems(t, srv.dsn, ca)

	e := newSourceEnv(t, srv)
	e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
	e.exec("INSERT INTO $db.items VALUES (1)")
	user := "x_" + e.database[2:12]
	e.exec("CREATE USER " + quoteLiteral(user) + " IDENTIFIED BY 'pw' REQUIRE X509")
	t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "DROP USER "+quoteLiteral(user)) })
	e.exec("GRANT SELECT, CREATE, ALTER, DROP, INSERT, UPDATE, DELETE ON $db.* TO " + quoteLiteral(user))
	e.exec("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO " + quoteLiteral(user))
	c, err := parseDSN(srv.dsn)
	if err != nil {
		t.Fatal(err)
	}
	c.User, c.Passwd, c.DBName = user, "pw", e.database
	dsn := c.FormatDSN()
	if _, err := cipher(t, dsn, &TLSConfig{Mode: "verify-ca", CA: ca}); err == nil {
		t.Error("a user that must present a certificate connected without one")
	}
	withCert := &TLSConfig{Mode: "verify-ca", CA: ca, Cert: cert, Key: sdk.NewSecret(key)}
	if got, err := cipher(t, dsn, withCert); err != nil || got == "" {
		t.Fatalf("with a client certificate: %q, %v", got, err)
	}
	cfg, _ := json.Marshal(map[string]any{"dsn": dsn, "tls": map[string]string{"mode": "verify-ca", "ca": ca, "cert": cert, "key": key}})
	s, err := NewSource(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	first := read(t, s, cdc("tls", nil, "items"))
	e.exec("INSERT INTO $db.items VALUES (2)")
	next := read(t, s, cdc("tls", map[string]string{"items": first.state("items")}, "items"))
	if rows := next.rows("items"); len(rows) != 1 || rows[0]["openetl.op"] != source.OpInsert {
		t.Fatalf("the binlog over TLS gave %v", rows)
	}
	d, err := NewDestination(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestTLSOnMariaDB connects to MariaDB with each TLS mode. MariaDB makes a
// certificate of its own when it has none, which signs itself, and no CA
// that would take a client's: verify-ca takes that certificate as the CA.
func TestTLSOnMariaDB(t *testing.T) {
	srv := servers()[1]
	c, err := parseDSN(srv.dsn)
	if err != nil {
		t.Fatal(err)
	}
	var own string
	c.TLS = &tls.Config{InsecureSkipVerify: true, VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
		own = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw[0]}))
		return nil
	}}
	db, _, err := open(t.Context(), c, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	for _, c := range []struct {
		tc  *TLSConfig
		tls bool
		err string
	}{
		{&TLSConfig{Mode: "disabled"}, false, ""},
		{&TLSConfig{Mode: "preferred"}, true, ""},
		{&TLSConfig{Mode: "required"}, true, ""},
		{&TLSConfig{Mode: "verify-ca", CA: own}, true, ""},
		// The certificate names no host.
		{&TLSConfig{CA: own}, false, "certificate"},
		{&TLSConfig{Mode: "verify-ca", CA: otherCA(t)}, false, "certificate signed by unknown authority"},
	} {
		got, err := cipher(t, srv.dsn, c.tc)
		switch {
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%+v: %v, not an error that says %q", c.tc, err, c.err)
		case c.err == "" && err != nil:
			t.Errorf("%+v: %v", c.tc, err)
		case c.err == "" && (got != "") != c.tls:
			t.Errorf("%+v: cipher %q", c.tc, got)
		}
	}
	expectTLSItems(t, srv.dsn, own)

	e := newSourceEnv(t, srv)
	e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
	cfg, _ := json.Marshal(map[string]any{"dsn": srv.dsn, "database": e.database, "tls": map[string]string{"mode": "verify-ca", "ca": own}})
	s, err := NewSource(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := read(t, s, cdc("tls", nil, "items"))
	e.exec("INSERT INTO $db.items VALUES (1)")
	if rows := read(t, s, cdc("tls", map[string]string{"items": first.state("items")}, "items")).rows("items"); len(rows) != 1 {
		t.Fatalf("the binlog over TLS gave %v", rows)
	}
}
