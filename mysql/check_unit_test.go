package mysql

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func itemNames(checks []check.Check) []string {
	out := make([]string, len(checks))
	for i, c := range checks {
		out[i] = c.Name
	}
	return out
}

var cdcItems = []string{"cdc.log-bin", "cdc.binlog-format", "cdc.binlog-row-image", "cdc.binlog-row-metadata", "cdc.binlog-row-value-options",
	"cdc.gtid-mode", "cdc.replication", "cdc.binlog-monitor"}

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	s, err := NewSource([]byte(`{"dsn": "u:p@tcp(db.example.com:3307)/app?tls=true", "tables": ["Orders", "orders"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(s.Checks()); err != nil {
		t.Error(err)
	}
	want := append([]string{"net.dns", "net.connect", "db.tls", "db.connect", "database.found", "tables.found", "tables.read"}, append(slices.Clone(cdcItems), "cdc.retention")...)
	if got := itemNames(s.Checks()); !slices.Equal(got, want) {
		t.Errorf("source checks %v, want %v", got, want)
	}
	for _, c := range s.Checks()[7:] {
		if !c.Advisory {
			t.Errorf("%s fails the check, which doesn't know whether a pipeline reads with cdc", c.Name)
		}
	}
	req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "Orders", SyncMode: source.CDC}, {Name: "orders", SyncMode: source.Incremental, Cursor: "at"}, {Name: "x"},
	}}
	pf := s.Preflight(req)
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}
	want = append([]string{"db.connect", "stream.orders.read", "stream.orders.cdc", "stream.orders.read-2", "stream.orders.cursor", "stream.x.read"},
		append(slices.Clone(cdcItems), "cdc.retention")...)
	if got := itemNames(pf); !slices.Equal(got, want) {
		t.Errorf("source preflight %v, want %v", got, want)
	}
	for _, c := range pf {
		if c.Advisory != (c.Name == "cdc.gtid-mode") {
			t.Errorf("%s: advisory %v in a preflight of a cdc read", c.Name, c.Advisory)
		}
	}
	// Without cdc, nothing of it.
	if got := itemNames(s.Preflight(source.PreflightRequest{Streams: req.Streams[2:]})); !slices.Equal(got, []string{"db.connect", "stream.x.read"}) {
		t.Errorf("preflight without cdc %v", got)
	}

	// Without TLS, with it turned off in the config, and through a socket.
	for config, want := range map[string][]string{
		`{"dsn": "u:p@tcp(db.example.com)/app"}`:                                          {"net.dns", "net.connect", "db.connect", "database.found", "tables.read"},
		`{"dsn": "mysql://u:p@db.example.com/app?tls=true", "tls": {"mode": "disabled"}}`: {"net.dns", "net.connect", "db.connect", "database.found", "tables.read"},
		`{"dsn": "u:p@unix(/run/mysqld/mysqld.sock)/app"}`:                                {"db.connect", "database.found", "tables.read"},
	} {
		s, err := NewSource([]byte(config))
		if err != nil {
			t.Fatal(err)
		}
		if got := itemNames(s.Checks()); !slices.Equal(got, append(append(want, cdcItems...), "cdc.retention")) {
			t.Errorf("%s: %v, want %v", config, got, want)
		}
	}

	d, err := NewDestination([]byte(`{"dsn": "u:p@tcp(db.example.com)/app", "tls": {"mode": "verify-ca"}}`))
	if err != nil {
		t.Fatal(err)
	}
	checks := d.Checks()
	if got, want := itemNames(checks), []string{"net.dns", "net.connect", "db.tls", "db.connect", "database.create"}; !slices.Equal(got, want) {
		t.Errorf("destination checks %v, want %v", got, want)
	}
	if checks[2].Field != "tls.mode" || checks[4].Field != "database" {
		t.Errorf("fields %q and %q", checks[2].Field, checks[4].Field)
	}
	dpf := d.Preflight(destination.PreflightRequest{Namespace: "n", Streams: []destination.Stream{{Name: "a b"}, {Name: "a.b", Mode: destination.Merge}}})
	if err := check.Validate(dpf); err != nil {
		t.Error(err)
	}
	if got, want := itemNames(dpf), []string{"db.connect", "database.create", "stream.a-b.write", "stream.a-b.write-2"}; !slices.Equal(got, want) {
		t.Errorf("destination preflight %v, want %v", got, want)
	}
	if dpf[1].Field != "" {
		t.Errorf("a namespace the pipeline names is no field of the config: %q", dpf[1].Field)
	}
}

func TestParseGrant(t *testing.T) {
	for line, want := range map[string]string{
		"GRANT USAGE ON *.* TO `u`@`%`": "*.* USAGE",
		"GRANT ALL PRIVILEGES ON *.* TO `openetl`@`%` IDENTIFIED BY PASSWORD '*DD0F' WITH GRANT OPTION": "*.* ALL",
		"GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO `r`":                                         "*.* BINLOG MONITOR,REPLICATION SLAVE",
		"GRANT SELECT, INSERT (`a`, `b`), LOCK TABLES ON `app`.* TO `u`@`%`":                            "app.* LOCK TABLES,SELECT",
		"GRANT SELECT ON `my``db`.`t 1` TO `u`@`%`":                                                     "my`db.t 1 SELECT",
		"GRANT ALLOW_NONEXISTENT_DEFINER,APPLICATION_PASSWORD_ADMIN ON *.* TO `root`@`localhost`":       "*.* ALLOW_NONEXISTENT_DEFINER,APPLICATION_PASSWORD_ADMIN",
		"GRANT SELECT ON TABLE `app`.`t` TO `u`@`%`":                                                    "app.t SELECT",
		"GRANT `r_cdc` TO `u`@`%`":                                                                      "",
		"GRANT EXECUTE ON PROCEDURE `app`.`p` TO `u`@`%`":                                               "",
		"SET DEFAULT ROLE `r` FOR `u`@`%`":                                                              "",
	} {
		g, ok := parseGrant(line)
		got := ""
		if ok {
			obj := g.db + ".*"
			switch {
			case g.global:
				obj = "*.*"
			case g.table != "":
				obj = g.db + "." + g.table
			}
			var privs []string
			for p := range g.privileges {
				privs = append(privs, p)
			}
			slices.Sort(privs)
			got = obj + " " + strings.Join(privs, ",")
		}
		if got != want {
			t.Errorf("%s: %q, want %q", line, got, want)
		}
	}

	var gs grants
	for _, line := range []string{
		"GRANT SELECT, INSERT, CREATE ON `app\\_%`.* TO `u`@`%`",
		"GRANT ALTER ON `other`.`t` TO `u`@`%`",
		"GRANT ALL PRIVILEGES ON `all`.* TO `u`@`%`",
	} {
		g, ok := parseGrant(line)
		if !ok {
			t.Fatal(line)
		}
		gs = append(gs, g)
	}
	for _, c := range []struct {
		privilege, db string
		want          bool
	}{
		{"SELECT", "app_1", true}, {"SELECT", "appx1", false}, {"CREATE", "app_", true}, {"DELETE", "app_1", false},
		// A grant on one table isn't one on the database.
		{"ALTER", "other", false},
		{"DELETE", "all", true}, {"GRANT OPTION", "all", false}, {"REPLICATION SLAVE", "", false},
	} {
		if got := gs.has(c.privilege, c.db); got != c.want {
			t.Errorf("%s on %s: %v", c.privilege, c.db, got)
		}
	}
	if got := gs.missing([]string{"CREATE", "DROP", "SELECT"}, "app_x"); !slices.Equal(got, []string{"DROP"}) {
		t.Errorf("missing %v", got)
	}
}

// fakeServer answers each connection with greeting, as a MySQL server's
// first packet, and closes it.
func fakeServer(t *testing.T, greeting []byte) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			n := len(greeting)
			_, _ = c.Write(append([]byte{byte(n), byte(n >> 8), byte(n >> 16), 0}, greeting...))
			_ = c.Close()
		}
	}()
	return l.Addr().String()
}

// db.tls against servers that don't take TLS, and one that refuses the
// connection with an error packet.
func TestTLSCheckWithoutTLS(t *testing.T) {
	// Protocol 10, a version, a connection ID, auth data and a filler, then
	// capability flags without CLIENT_SSL.
	plain := append(append([]byte{10}, "8.4.0\x00"...), make([]byte, 4+8+1)...)
	plain = append(plain, 0xff, 0xf7, 33, 2, 0)
	full := append([]byte{0xff, 0x10, 0x04}, "Too many connections"...)
	for _, c := range []struct {
		greeting []byte
		tls      string
		want     string
	}{
		{plain, "preferred", "warning plaintext_connection"},
		{plain, "required", "failed tls_refused"},
		{full, "required", "failed unavailable"},
	} {
		addr := fakeServer(t, c.greeting)
		cfg, err := parseDSN("u:p@tcp(" + addr + ")/app")
		if err != nil {
			t.Fatal(err)
		}
		if err := applyTLS(cfg, &TLSConfig{Mode: c.tls}); err != nil {
			t.Fatal(err)
		}
		c2 := tlsCheck(cfg, "tls.mode")
		c2.Needs = nil
		r := check.Run(context.Background(), []check.Check{c2})
		if r.Items == nil {
			t.Fatal(r.Err())
		}
		got := r.Items[0].Status.String()
		if f := r.Items[0].Failure; f != nil {
			got += " " + f.Reason
		}
		if got != c.want {
			t.Errorf("%s: %s, want %s: %s", c.tls, got, c.want, r.Items[0].Rendered().Message)
		}
	}
}
