package mongodb

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mnet"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// valid fails a list check.Validate refuses, or whose items name a field
// the config doesn't have.
func valid(t *testing.T, what string, checks []check.Check, fields ...string) {
	t.Helper()
	if err := check.Validate(checks); err != nil {
		t.Errorf("%s: %v", what, err)
	}
	for _, c := range checks {
		if c.Field != "" && !slices.Contains(fields, c.Field) {
			t.Errorf("%s: %s's field %q isn't one of the config's", what, c.Name, c.Field)
		}
	}
}

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	srcFields := []string{"uri", "database", "collections", "sampleSize", "discoveryConnections", "chunkDocuments", "packed"}
	for _, tc := range []struct {
		config string
		want   []string
	}{
		{`{"uri": "mongodb://u:p@db.example.com/?replicaSet=rs0", "database": "app"}`,
			[]string{"net.dns", "net.connect", "db.connect", "db.read", "cdc.replica-set", "cdc.watch", "cdc.images", "cdc.oplog"}},
		{`{"uri": "mongodb://u:p@db.example.com:27018/?tls=true", "database": "app", "collections": ["a"]}`,
			[]string{"net.dns", "net.connect", "net.tls", "db.connect", "collections.found", "db.read", "cdc.replica-set", "cdc.watch", "cdc.images", "cdc.oplog"}},
		{`{"uri": "mongodb://a.example.com,b.example.com:27018/", "database": "app"}`,
			[]string{"net.dns.a-example-com-27017", "net.connect.a-example-com-27017", "net.dns.b-example-com-27018", "net.connect.b-example-com-27018",
				"db.connect", "db.read", "cdc.replica-set", "cdc.watch", "cdc.images", "cdc.oplog"}},
	} {
		s, err := NewSource([]byte(tc.config))
		if err != nil {
			t.Fatal(err)
		}
		checks := s.Checks()
		valid(t, tc.config, checks, srcFields...)
		if got := connectortest.ItemNames(checks); !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.config, got, tc.want)
		}
		for _, c := range checks {
			multi := len(s.opts.Hosts) > 1 && c.Kind == check.Connection
			cdc := c.Name[:4] == "cdc."
			if c.Advisory != (multi || cdc) {
				t.Errorf("%s: %s is advisory: %v", tc.config, c.Name, c.Advisory)
			}
		}
		// One host: signing in needs it; several: it needs none of them.
		if i := slices.Index(connectortest.ItemNames(checks), "db.connect"); len(s.opts.Hosts) == 1 && len(checks[i].Needs) != 1 || len(s.opts.Hosts) > 1 && len(checks[i].Needs) != 0 {
			t.Errorf("%s: db.connect needs %v", tc.config, checks[i].Needs)
		}
	}

	s, err := NewSource([]byte(`{"uri": "mongodb://u:p@db.example.com/", "database": "app"}`))
	if err != nil {
		t.Fatal(err)
	}
	req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "Orders", SyncMode: source.CDC}, {Name: "orders", SyncMode: source.Incremental, Cursor: "at"}, {Name: "x"},
	}}
	pf := s.Preflight(req)
	valid(t, "source preflight", pf, srcFields...)
	want := []string{"db.connect", "stream.orders.read", "stream.orders.read-2", "stream.orders.cursor", "stream.x.read",
		"cdc.replica-set", "cdc.watch", "cdc.images", "cdc.oplog"}
	if got := connectortest.ItemNames(pf); !slices.Equal(got, want) {
		t.Errorf("source preflight %v, want %v", got, want)
	}
	for _, c := range pf {
		if c.Advisory != (c.Name == "cdc.images" || c.Name == "cdc.oplog") {
			t.Errorf("source preflight: %s is advisory: %v", c.Name, c.Advisory)
		}
	}
	if got := connectortest.ItemNames(s.Preflight(source.PreflightRequest{Streams: req.Streams[2:]})); !slices.Equal(got, []string{"db.connect", "stream.x.read"}) {
		t.Errorf("preflight without cdc %v", got)
	}

	d, err := NewDestination([]byte(`{"uri": "mongodb://u:p@db.example.com/?tls=true", "database": "raw"}`))
	if err != nil {
		t.Fatal(err)
	}
	dstFields := []string{"uri", "database", "extendedJSONKeys"}
	valid(t, "destination checks", d.Checks(), dstFields...)
	if got, want := connectortest.ItemNames(d.Checks()), []string{"net.dns", "net.connect", "net.tls", "db.connect", "db.write"}; !slices.Equal(got, want) {
		t.Errorf("destination checks %v, want %v", got, want)
	}
	str := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	num := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	dpf := d.Preflight(destination.PreflightRequest{Namespace: "n", Streams: []destination.Stream{
		{Name: "a b", Mode: destination.Merge, PrimaryKey: []string{"id"}}, {Name: "a.b", Mode: destination.Merge, PrimaryKey: []string{"id"}},
		{Name: "c", Mode: destination.Append, PrimaryKey: []string{"id"}},
	}, Schemas: map[string]*arrow.Schema{"a b": str, "a.b": num, "c": str}})
	valid(t, "destination preflight", dpf, dstFields...)
	// Only a merge by one text key has its _ids checked.
	want = []string{"db.connect", "db.write", "stream.a-b.write", "stream.a-b.indexes", "stream.a-b.keys",
		"stream.a-b.write-2", "stream.a-b.indexes-2", "stream.c.write", "stream.c.indexes"}
	if got := connectortest.ItemNames(dpf); !slices.Equal(got, want) {
		t.Errorf("destination preflight %v, want %v", got, want)
	}
}

func TestCan(t *testing.T) {
	str := func(s string) *string { return &s }
	priv := func(db, coll *string, any bool, actions ...string) privilege {
		var p privilege
		p.Resource.DB, p.Resource.Collection, p.Resource.AnyResource, p.Actions = db, coll, any, actions
		return p
	}
	c := &conn{principal: "u@admin", privileges: []privilege{
		priv(str(""), str(""), false, "find"),
		priv(str("app"), str(""), false, "insert"),
		priv(str("app"), str("orders"), false, "update"),
		priv(nil, nil, false, "listDatabases"),
	}}
	for _, tc := range []struct {
		action, db, coll string
		want             bool
	}{
		{"find", "app", "orders", true},
		{"find", "other", "x", true},
		{"find", "local", "oplog.rs", false},
		{"find", "app", "system.views", false},
		{"insert", "app", "x", true},
		{"insert", "app", "", true},
		{"insert", "other", "x", false},
		{"update", "app", "orders", true},
		{"update", "app", "users", false},
		{"listDatabases", "app", "", false},
		{"remove", "app", "x", false},
	} {
		if got := c.can(tc.action, tc.db, tc.coll); got != tc.want {
			t.Errorf("can(%s, %s, %s) = %v", tc.action, tc.db, tc.coll, got)
		}
	}
	if !(&conn{principal: "root@admin", privileges: []privilege{priv(nil, nil, true, "find")}}).can("find", "local", "oplog.rs") {
		t.Error("anyResource doesn't cover local")
	}
	if !(&conn{}).can("anything", "app", "x") {
		t.Error("without a user, the server decides")
	}
}

// A unique index fails what certainly breaks every write, and warns of what
// the rows' values may.
func TestUniqueConflict(t *testing.T) {
	idx := func(name string, key bson.D, extra ...bson.E) bson.Raw {
		b, err := bson.Marshal(append(bson.D{{Key: "name", Value: name}, {Key: "key", Value: key}, {Key: "unique", Value: true}}, extra...))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "email", Type: arrow.BinaryTypes.String}}, nil)
	merge := destination.Stream{Name: "s", Mode: destination.Merge, PrimaryKey: []string{"id"}}
	appendKeyed := destination.Stream{Name: "s", Mode: destination.Append, PrimaryKey: []string{"id"}}
	// As an item reports it.
	status := func(err error) string {
		it := check.Run(t.Context(), []check.Check{{Name: "x", Title: "x", Run: func(context.Context, *check.Probe) error { return err }}}).Items[0]
		if it.Failure != nil {
			return it.Status.String() + " " + it.Failure.Reason
		}
		return it.Status.String()
	}
	for _, tc := range []struct {
		name  string
		st    destination.Stream
		index bson.Raw
		want  string
	}{
		{"absent field", merge, idx("sku_1", bson.D{{Key: "sku", Value: 1}}), "failed unique_index_conflict"},
		{"absent but sparse", merge, idx("sku_1", bson.D{{Key: "sku", Value: 1}}, bson.E{Key: "sparse", Value: true}), "passed"},
		{"partial", merge, idx("sku_1", bson.D{{Key: "sku", Value: 1}}, bson.E{Key: "partialFilterExpression", Value: bson.D{{Key: "sku", Value: bson.D{{Key: "$exists", Value: true}}}}}), "passed"},
		{"merge on its key", merge, idx("id_1", bson.D{{Key: "id", Value: 1}}), "passed"},
		{"merge on another field", merge, idx("email_1", bson.D{{Key: "email", Value: 1}}), "warning unique_index_conflict"},
		{"append on its key", appendKeyed, idx("id_1", bson.D{{Key: "id", Value: 1}}), "warning unique_index_conflict"},
		{"append on key and more", appendKeyed, idx("id_email", bson.D{{Key: "id", Value: 1}, {Key: "email", Value: 1}}), "warning unique_index_conflict"},
		{"not unique", merge, func() bson.Raw {
			b, _ := bson.Marshal(bson.D{{Key: "name", Value: "sku_1"}, {Key: "key", Value: bson.D{{Key: "sku", Value: 1}}}})
			return b
		}(), "passed"},
		{"_id", merge, idx("_id_", bson.D{{Key: "_id", Value: 1}}), "passed"},
	} {
		if got := status(uniqueConflict(tc.st, sch, true, []bson.Raw{tc.index})); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}

// A mongodb+srv uri's records are looked up as it's parsed: a name that
// doesn't resolve is TRANSIENT, as DNS may answer later, not a bad config.
func TestSRVLookupFails(t *testing.T) {
	_, err := NewSource([]byte(`{"uri": "mongodb+srv://u:p@cluster0.example.invalid/", "database": "app"}`))
	if code := connectortest.CodeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("%s: %v", code, err)
	}
	if f := check.Classify(err); f.Reason != check.DNSFailed.Name {
		t.Errorf("classified as %s", f.Reason)
	}
}

// fakeConn is a connection signing in, as the handshake sees it.
type fakeConn struct{}

func (fakeConn) Read(context.Context) ([]byte, error) { return nil, io.EOF }
func (fakeConn) Write(context.Context, []byte) error  { return io.EOF }
func (fakeConn) Close() error                         { return nil }
func (fakeConn) Description() description.Server {
	return description.Server{Kind: description.ServerKindStandalone}
}
func (fakeConn) ID() string                 { return "fake" }
func (fakeConn) ServerConnectionID() *int64 { return nil }
func (fakeConn) DriverConnectionID() int64  { return 1 }
func (fakeConn) Address() address.Address   { return "db.internal:27017" }
func (fakeConn) Stale() bool                { return false }
func (fakeConn) OIDCTokenGenID() uint64     { return 0 }
func (fakeConn) SetOIDCTokenGenID(uint64)   {}

// fakeAuth signs in with err.
type fakeAuth struct{ err error }

func (a fakeAuth) Auth(context.Context, *driver.AuthConfig) error   { return a.err }
func (a fakeAuth) Reauth(context.Context, *driver.AuthConfig) error { return a.err }

// A sign-in that the network or a deadline cuts short is transient, though
// the driver wraps it as an authentication error; a server's or the
// client's refusal of the credentials is a rejection.
func TestSignInFailures(t *testing.T) {
	signIn := func(inner error) error {
		h := auth.Handshaker(nil, &auth.HandshakeOptions{Authenticator: fakeAuth{inner}})
		err := h.FinishHandshake(t.Context(), &mnet.Connection{ReadWriteCloser: fakeConn{}, Describer: fakeConn{}})
		if _, ok := errors.AsType[*auth.Error](err); !ok {
			t.Fatalf("the handshake's error %v", err)
		}
		// As the pool and then the client wrap it.
		err = topology.ConnectionError{ConnectionID: "fake", Wrapped: err}
		if de, ok := errors.AsType[driver.Error](err); ok {
			err = mongo.CommandError{Code: de.Code, Message: de.Message, Labels: de.Labels, Name: de.Name, Wrapped: err}
		}
		return err
	}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	for name, c := range map[string]struct {
		inner  error
		code   protocolv1.ErrorCode
		reason string
	}{
		"rejected":   {driver.Error{Code: codeAuthenticationFailed, Name: "AuthenticationFailed", Message: "Authentication failed."}, protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, "credentials_rejected"},
		"no key":     {errors.New("no AWS credentials"), protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, "credentials_rejected"},
		"reset":      {driver.Error{Message: reset.Error(), Labels: []string{driver.NetworkError}, Wrapped: reset}, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, ""},
		"bare reset": {reset, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, ""},
		"deadline":   {context.DeadlineExceeded, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, ""},
	} {
		err := signIn(c.inner)
		if got := connectortest.CodeOf(classify(err)); got != c.code {
			t.Errorf("%s: classify is %s, want %s", name, got, c.code)
		}
		f, _ := errors.AsType[*check.Failure](failure(err))
		if f == nil || f.Code != c.code || c.reason != "" && f.Reason != c.reason || c.reason == "" && f.Reason == "credentials_rejected" {
			t.Errorf("%s: failure is %+v, want %s %s", name, f, c.code, c.reason)
		}
	}
}
