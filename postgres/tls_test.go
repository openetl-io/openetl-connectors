//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// serveTLS turns TLS on in the test server, with a certificate for
// localhost that ca signs, and ca as the authority of clients'
// certificates. It writes them into the data directory, and turns TLS off
// again after the test.
func serveTLS(t *testing.T, ca authority) {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	cert, key := ca.issue(t, "localhost", true)
	var dir string
	if err := conn.QueryRow(ctx, "SELECT current_setting('data_directory')").Scan(&dir); err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for name, content := range map[string]string{"ssl_cert_file": cert, "ssl_key_file": key, "ssl_ca_file": ca.pem} {
		path := dir + "/openetl_test_" + name
		var oid uint32
		if err := conn.QueryRow(ctx, "SELECT lo_from_bytea(0, $1)", []byte(content)).Scan(&oid); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "SELECT lo_export($1, $2), lo_unlink($1)", oid, path); err != nil {
			t.Fatal(err)
		}
		// The server takes a key only its owner can read.
		if _, err := conn.Exec(ctx, "COPY (SELECT) TO PROGRAM "+quoteLiteral("chmod 600 "+path)); err != nil {
			t.Fatal(err)
		}
		settings[name] = path
	}
	settings["ssl"] = "on"
	reset := func() {
		c, err := pgx.Connect(context.Background(), dsn())
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		for name := range settings {
			_, _ = c.Exec(context.Background(), "ALTER SYSTEM RESET "+name)
		}
		_, _ = c.Exec(context.Background(), "SELECT pg_reload_conf()")
	}
	t.Cleanup(reset)
	for name, v := range settings {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET %s = %s", name, quoteLiteral(v))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	// The postmaster reloads in a moment; until then TLS is refused.
	for range 50 {
		c, err := pgx.Connect(ctx, withParams(t, dsn(), "sslmode=require"))
		if err == nil {
			_ = c.Close(ctx)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the server didn't turn TLS on")
}

// withParams is dsn with its query parameters replaced by params', and its
// host by host if given.
func withParams(t *testing.T, dsn, params string, host ...string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.RawQuery = params
	if len(host) > 0 {
		u.Host = host[0] + ":" + u.Port()
	}
	return u.String()
}

func tlsConfig(dsn string, ca, cert, key string, extra string) []byte {
	return fmt.Appendf(nil, `{"dsn": %q, "tlsCa": %q, "tlsCert": %q, "tlsKey": %q%s}`, dsn, ca, cert, key, extra)
}

// Both roles connect with a CA and a client certificate given in place, the
// source's replication connection too: the server's certificate is
// verified against the CA, by name under verify-full, and a wrong CA fails
// under require as well.
func TestTLSInPlace(t *testing.T) {
	ca := newAuthority(t)
	serveTLS(t, ca)
	clientCert, clientKey := ca.issue(t, "openetl-client", false)
	other := newAuthority(t)
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v text)")
	e.exec("INSERT INTO $schema.t VALUES (1, 'a'), (2, 'b')")
	full := withParams(t, dsn(), "sslmode=verify-full", "localhost")
	src := fmt.Sprintf(`, "schema": %q, "publication": %q`, e.schema, e.schema)

	s, err := NewSource(tlsConfig(full, ca.pem, clientCert, clientKey, src))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Check(t.Context()); err != nil {
		t.Fatalf("check: %v", err)
	}
	c := read(t, s, map[string]string{"t": ""})
	if got := c.column("t", "v"); len(got) != 2 {
		t.Errorf("read %v", got)
	}

	// A cdc read: its connections, the replication one included, present
	// the client certificate.
	pipeline := "tls" + strings.ToLower(rand.Text())[:10]
	t.Cleanup(func() {
		_ = s.Teardown(context.Background(), pipeline)
		_, _ = e.conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{e.schema}.Sanitize())
	})
	f := follow(t, s, pipeline, map[string]string{"t": ""})
	f.await(t, "the snapshot", func() bool { return len(f.column("t", "id")) == 2 })
	var dn string
	err = e.conn.QueryRow(t.Context(), `SELECT coalesce(l.client_dn, '') FROM pg_replication_slots r JOIN pg_stat_ssl l ON l.pid = r.active_pid
		WHERE r.slot_name = $1 AND l.ssl`, s.slotName(pipeline)).Scan(&dn)
	if err != nil || dn != "/CN=openetl-client" {
		t.Errorf("the replication connection's TLS: %q, %v", dn, err)
	}
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}

	d, err := NewDestination(tlsConfig(full, ca.pem, clientCert, clientKey, ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(t.Context()); err != nil {
		t.Errorf("destination check: %v", err)
	}
	// A writer's pool connects as the check does.
	w, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "tls", Namespace: e.schema})
	if err != nil {
		t.Fatalf("destination open: %v", err)
	}
	_ = w.Close(t.Context())
	// db.tls says what it agreed.
	r := check.Run(t.Context(), s.Checks())
	if it := r.Items[2].Rendered(); it.Name != "db.tls" || it.Status != check.Passed || !strings.Contains(it.Message, "a certificate the config trusts") {
		t.Errorf("db.tls: %s %s: %s", it.Name, it.Status, it.Message)
	}

	for name, cfg := range map[string][]byte{
		// The CA verifies the chain under require, as under verify-ca.
		"require": tlsConfig(withParams(t, dsn(), "sslmode=require", "127.0.0.1"), ca.pem, "", "", src),
		"prefer":  tlsConfig(withParams(t, dsn(), "sslmode=prefer", "127.0.0.1"), ca.pem, "", "", src),
	} {
		s, err := NewSource(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Check(t.Context()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, c := range map[string][2]string{
		"a wrong CA under require":       {withParams(t, dsn(), "sslmode=require"), other.pem},
		"a wrong CA under prefer":        {withParams(t, dsn(), "sslmode=prefer"), other.pem},
		"another name under verify-full": {withParams(t, dsn(), "sslmode=verify-full", "127.0.0.1"), ca.pem},
	} {
		s, err := NewSource(tlsConfig(c[0], c[1], "", "", src))
		if err != nil {
			t.Fatal(err)
		}
		r := check.Run(t.Context(), s.Checks())
		if st := statuses(r)["db.tls"]; st != "failed tls_failed" {
			t.Errorf("%s: db.tls %s", name, st)
		}
		d, err := NewDestination(tlsConfig(c[0], c[1], "", "", ""))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Check(t.Context()); err == nil {
			t.Errorf("destination, %s: connected", name)
		}
	}
}
