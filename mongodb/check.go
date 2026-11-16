package mongodb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
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

// Server error codes.
const (
	codeUnauthorized         = 13
	codeAuthenticationFailed = 18
	codeNamespaceNotFound    = 26
	codeNamespaceExists      = 48
	codeCappedPositionLost   = 136
	codeChangeStreamFatal    = 280
	codeChangeStreamLost     = 286
	codeValidationFailure    = 121
	codeDuplicateKey         = 11000
)

const configInvalid = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID

var (
	standalone = check.Reason{
		Name: "change_streams_unavailable", Code: configInvalid,
		Message: "{hosts} is a standalone server, and change streams need a replica set or a sharded cluster.",
		Fix:     "Run mongod with --replSet, and initiate the replica set with rs.initiate(): a set of one member will do.",
	}
	noImages = check.Reason{
		Name: "pre_and_post_images_off", Code: configInvalid,
		Message: "Pre- and post-images are off for {collections}, so a cdc read has an update as its document is when it's read, and a delete with its _id alone.",
		Fix:     `On MongoDB 6.0 or later, run db.runCommand({collMod: "NAME", changeStreamPreAndPostImages: {enabled: true}}) for each.`,
	}
	shortOplog = check.Reason{
		Name: "oplog_window_short", Code: configInvalid,
		Message: "The oplog holds {window} of changes, and the pipeline goes up to {interval} between syncs, so a cdc stream may find its position dropped, and read its collection again.",
		Fix:     "Make the oplog bigger with replSetResizeOplog, or give it a minimum retention, or sync more often.",
	}
	notCollection = check.Reason{
		Name: "collection_unsupported", Code: configInvalid,
		Message: "{stream} is a {type} in {database}, and the source reads collections, in _id order.",
		Fix:     "Read the collection the {type} is of instead.",
	}
	unindexed = check.Reason{
		Name: "cursor_not_indexed", Code: configInvalid,
		Message: "No index of {stream} starts with {cursor}, so each incremental read sorts the collection to find its highest {cursor}, and scans it for the documents since the last read's.",
		Fix:     `Run db.getCollection("{stream}").createIndex({"{cursor}": 1})`,
	}
	unwritable = check.Reason{
		Name: "collection_unsupported", Code: configInvalid,
		Message: "{collection} is a {type}, which the destination doesn't write to.",
		Fix:     "Write {stream} to another collection, or drop {collection}.",
	}
	requires = check.Reason{
		Name: "validator_requires_fields", Code: configInvalid,
		Message: "{collection}'s validator requires {fields}, which {stream} doesn't have, so it refuses every document the stream writes.",
		Fix:     "Take them out of the validator's required, or have the stream carry them.",
	}
	closedValidator = check.Reason{
		Name: "validator_refuses_fields", Code: configInvalid,
		Message: "{collection}'s validator takes no field its properties don't name, and they don't name {fields}, which {stream} writes.",
		Fix:     "Name them in the validator's properties, or set its additionalProperties to true.",
	}
	heldTypes = check.Reason{
		Name: "type_change_refused", Code: configInvalid,
		Message: "{collection}'s validator doesn't take the types {stream} has in {columns}, so the stream holds those columns until it does.",
		Fix:     "Add the types to the fields' bsonType in the validator with collMod, or let the pipeline change types.",
	}
	uniqueNull = check.Reason{
		Name: "unique_index_conflict", Code: configInvalid,
		Message: "{collection}'s unique index {index} is on {fields}, which {stream} doesn't write, so each document it writes indexes them as null, and the second is refused.",
		Fix:     "Drop the index, or make it sparse or partial, or have the stream carry {fields}.",
	}
	uniqueVersions = check.Reason{
		Name: "unique_index_conflict", Code: configInvalid,
		Message: "{stream} appends a document for each version of a row, and {collection}'s unique index {index} is on {fields}, of its key, so a row's second version, if it has one, is refused.",
		Fix:     "Drop the index, or write the stream with merge, unless its rows never change.",
	}
	uniqueValues = check.Reason{
		Name: "unique_index_conflict", Code: configInvalid,
		Message: "{collection}'s unique index {index} is on {fields}, which isn't one document per row of {stream}, so a row whose {fields} repeat another document's is refused.",
		Fix:     "Drop the index if the stream's rows may repeat those values.",
	}
	keysAsText = check.Reason{
		Name: "key_mismatch", Code: configInvalid,
		Message: "{collection} has documents whose _id is an ObjectId, and with extendedJSONKeys off, {stream}'s key {key} is written as the text it is, so its rows don't find those documents: if they're the stream's, read from MongoDB, a merge writes a second document beside each.",
		Fix:     "Set extendedJSONKeys to true if the stream is read from MongoDB, or write it to a collection of its own.",
	}
	keysAsBSON = check.Reason{
		Name: "key_mismatch", Code: configInvalid,
		Message: "{collection} has documents whose _id is text as the MongoDB source spells an _id, and with extendedJSONKeys on, {stream}'s key {key} is written as the value it spells, so its rows may not find those documents.",
		Fix:     "Set extendedJSONKeys to false if those documents were written with it off, or write the stream to a collection of its own.",
	}
)

// How long db.connect waits to sign in.
const signInTimeout = 20 * time.Second

func clientOptions(uri string) (*options.ClientOptions, error) {
	opts := options.Client().ApplyURI(uri).SetAppName("openetl")
	if err := opts.Validate(); err != nil {
		code := configInvalid
		// A mongodb+srv uri's records are looked up as it's parsed.
		if _, ok := errors.AsType[*net.DNSError](err); ok {
			code = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
		}
		return nil, sdk.WithCode(code, fmt.Errorf("mongodb: uri: %w", err))
	}
	return opts, nil
}

// connect makes a client; it reaches the server at its first operation.
func connect(uri string) (*mongo.Client, error) {
	opts, err := clientOptions(uri)
	if err != nil {
		return nil, err
	}
	c, err := mongo.Connect(opts)
	if err != nil {
		return nil, sdk.WithCode(configInvalid, fmt.Errorf("mongodb: uri: %w", err))
	}
	return c, nil
}

// conn is a check list's client: db.connect connects it, and the checks
// after it, which need db.connect, share it until the run ends.
type conn struct {
	opts   *options.ClientOptions
	client *mongo.Client
	// Who db.connect signed in as; empty without a user, where the server's
	// access control, if it has any, decides alone.
	principal, user, authDB string
	privileges              []privilege
}

// privilege is one of connectionStatus's authenticatedUserPrivileges.
type privilege struct {
	Resource struct {
		DB          *string `bson:"db"`
		Collection  *string `bson:"collection"`
		AnyResource bool    `bson:"anyResource"`
	} `bson:"resource"`
	Actions []string `bson:"actions"`
}

// netChecks reach each host of the uri, or of its SRV record, and agree on
// TLS with it where the uri asks for TLS, as MongoDB does from the
// connection's start. With several hosts, each has its own, named after it,
// that only warn: the driver needs one to find the rest. needs is what
// db.connect needs of them.
func netChecks(opts *options.ClientOptions) (checks []check.Check, needs []string) {
	hosts := slices.DeleteFunc(slices.Clone(opts.Hosts), func(h string) bool { return strings.HasSuffix(h, ".sock") })
	for _, h := range hosts {
		addr := h
		if _, _, err := net.SplitHostPort(h); err != nil {
			addr = net.JoinHostPort(h, "27017")
		}
		cs := netcheck.Reach(addr)
		if opts.TLSConfig != nil {
			cs = netcheck.ReachTLS(addr, opts.TLSConfig)
		}
		for i := range cs {
			cs[i].Field = "uri"
			if len(hosts) > 1 {
				suffix := "." + check.Name(addr)
				cs[i].Name += suffix
				for j := range cs[i].Needs {
					cs[i].Needs[j] += suffix
				}
				cs[i].Advisory = true
			}
		}
		checks = append(checks, cs...)
	}
	if len(hosts) == 1 {
		needs = []string{checks[len(checks)-1].Name}
	}
	return checks, needs
}

// signIn is db.connect: it connects, and reads who the uri signs in as and
// what they may do.
func (c *conn) signIn(needs []string) check.Check {
	title, params := "Connect without signing in", check.Params{}
	if a := c.opts.Auth; a != nil && a.Username != "" {
		title, params = "Sign in as {user}", check.Params{"user": a.Username}
	}
	return check.Check{
		Name: "db.connect", Kind: check.Authentication, Title: title, Field: "uri", Params: params, Needs: needs, Timeout: signInTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := mongo.Connect(c.opts)
			if err != nil {
				return failure(err)
			}
			p.Cleanup(func(ctx context.Context) { _ = cl.Disconnect(ctx) })
			var st struct {
				AuthInfo struct {
					Users      []struct{ User, DB string } `bson:"authenticatedUsers"`
					Privileges []privilege                 `bson:"authenticatedUserPrivileges"`
				} `bson:"authInfo"`
			}
			cmd := bson.D{{Key: "connectionStatus", Value: 1}, {Key: "showPrivileges", Value: true}}
			if err := cl.Database("admin").RunCommand(ctx, cmd).Decode(&st); err != nil {
				return failure(err)
			}
			c.client = cl
			if len(st.AuthInfo.Users) == 0 {
				p.Found("The uri names no user, so the server's access control, if it has any, decides what the connector may do.")
				return nil
			}
			u := st.AuthInfo.Users[0]
			c.user, c.authDB, c.principal, c.privileges = u.User, u.DB, u.User+"@"+u.DB, st.AuthInfo.Privileges
			p.Param("principal", c.principal)
			p.Found("Signed in as {principal}.")
			return nil
		},
	}
}

// check is a check that runs with the client db.connect connected.
func (c *conn) check(name string, kind check.Kind, title string, run func(context.Context, *check.Probe, *mongo.Client) error) check.Check {
	return check.Check{
		Name: name, Kind: kind, Title: title, Needs: []string{"db.connect"},
		Run: func(ctx context.Context, p *check.Probe) error { return run(ctx, p, c.client) },
	}
}

// can reports whether the user may take action on coll in db, or with coll
// empty on db itself. Without a user, it reports true.
func (c *conn) can(action, db, coll string) bool {
	if c.principal == "" {
		return true
	}
	for _, p := range c.privileges {
		r := p.Resource
		switch {
		case !slices.Contains(p.Actions, action):
		case r.AnyResource:
			return true
		case r.DB == nil || r.Collection == nil:
		// An empty db is any but local and config, and an empty collection
		// any but a system one.
		case (*r.DB == db || *r.DB == "" && db != "local" && db != "config") &&
			(*r.Collection == coll || *r.Collection == "" && !strings.HasPrefix(coll, "system.")):
			return true
		}
	}
	return false
}

// missing is PrivilegeMissing: the user has no privilege, such as "find
// action", on object, which role on db grants.
func (c *conn) missing(privilege, object, role, db string) *check.Failure {
	who := cmpOr(c.principal, "A connection with no user")
	f := check.PrivilegeMissing.Fail("principal", who, "privilege", privilege, "object", object,
		"role", role, "role_db", db, "user", c.user, "auth_db", c.authDB)
	f.Fix = `Grant {principal} the {role} role on {role_db}, as db.getSiblingDB("{auth_db}").grantRolesToUser("{user}", [{role: "{role}", db: "{role_db}"}]) does.`
	return f
}

// refused is err as a check's failure: missing as the server refused it.
func (c *conn) refused(err error, privilege, object, role, db string) error {
	if se, ok := errors.AsType[mongo.ServerError](err); ok && se.HasErrorCode(codeUnauthorized) {
		f := c.missing(privilege, object, role, db)
		f.Detail = err.Error()
		return f
	}
	return failure(err)
}

// failure is a driver's error as a check's: refused credentials and access
// by the server's codes, before check.Classify, which would see the network
// error a refused handshake is wrapped in.
func failure(err error) error {
	if _, ok := errors.AsType[*auth.Error](err); ok {
		return check.CredentialsRejected.Wrap(err)
	}
	if se, ok := errors.AsType[mongo.ServerError](err); ok {
		switch {
		case se.HasErrorCode(codeAuthenticationFailed):
			return check.CredentialsRejected.Wrap(err)
		case se.HasErrorCode(codeUnauthorized):
			return check.PermissionDenied.Wrap(err)
		}
	}
	return check.Classify(classify(err))
}

// actions is "X action", or "X, Y or Z action", for PrivilegeMissing's
// {privilege}.
func actions(names []string) string {
	if len(names) == 1 {
		return names[0] + " action"
	}
	return strings.Join(names[:len(names)-1], ", ") + " or " + names[len(names)-1] + " action"
}

// listed is names joined, the first ten of them.
func listed(names []string) string {
	if len(names) > 10 {
		return strings.Join(names[:10], ", ") + " and " + strconv.Itoa(len(names)-10) + " more"
	}
	return strings.Join(names, ", ")
}

// checkNames gives each check a name from parts, made distinct from those it
// gave before: collections whose names differ only in case or punctuation
// get the same check.Name.
type checkNames map[string]int

func (n checkNames) name(parts ...string) string {
	name := check.Name(parts...)
	n[name]++
	if c := n[name]; c > 1 {
		name += "-" + strconv.Itoa(c)
	}
	return name
}

// Checks reach the deployment, sign in, and look at the database's
// collections, and at what cdc needs, which only warns: whether a pipeline
// reads with cdc is its preflight's to say.
func (s *Source) Checks() []check.Check {
	c := &conn{opts: s.opts}
	db := s.cfg.Database
	checks, needs := netChecks(s.opts)
	checks = append(checks, c.signIn(needs))
	if len(s.cfg.Collections) > 0 {
		found := c.check("collections.found", check.Resource, "Find the collections in {database}", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
			specs, err := cl.Database(db).ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$in", Value: s.cfg.Collections}}}})
			if err != nil {
				return c.refused(err, "listCollections action", "the database "+db, "read", db)
			}
			var missing, other []string
			for _, name := range s.cfg.Collections {
				i := slices.IndexFunc(specs, func(sp mongo.CollectionSpecification) bool { return sp.Name == name })
				switch {
				case i < 0:
					missing = append(missing, name)
				case specs[i].Type != "collection" || strings.HasPrefix(name, "system."):
					other = append(other, name)
				}
			}
			if len(missing) > 0 {
				f := check.NotFound.Fail("missing", listed(missing))
				f.Message, f.Fix = "{missing} isn't a collection in {database}.", "Correct collections in the config, or create what it names."
				return f
			}
			if len(other) > 0 {
				f := check.ConfigInvalid.Fail("other", listed(other))
				f.Message, f.Fix = "{other} is a view, a time series or a system collection, which the source doesn't read.", "Take it out of collections."
				return f
			}
			return nil
		})
		found.Params, found.Field = check.Params{"database": db}, "collections"
		checks = append(checks, found)
	}
	read := c.check("db.read", check.Permission, "Read the collections of {database}", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
		names, err := s.listCollections(ctx, cl.Database(db))
		if err != nil {
			return c.refused(err, "listCollections action", "the database "+db, "read", db)
		}
		var unread []string
		for _, n := range names {
			if !c.can("find", db, n) {
				unread = append(unread, db+"."+n)
			}
		}
		if len(unread) > 0 {
			return c.missing("find action", listed(unread), "read", db)
		}
		p.Param("count", len(names))
		p.Found("It offers {count} collections, and can read them.")
		return nil
	})
	read.Params, read.Field = check.Params{"database": db}, "database"
	checks = append(checks, read)
	for _, ch := range s.cdcChecks(c, nil) {
		ch.Advisory = true
		ch.Description = "Needed only to read collections with cdc: a pipeline's preflight fails without it if it does."
		checks = append(checks, ch)
	}
	return append(checks, s.imagesCheck(c, nil), s.oplogCheck(c, 0))
}

// cdcChecks are what a cdc read of names, or with nil of every collection
// offered, can't do without: change streams, and the user's changeStream
// action on each.
func (s *Source) cdcChecks(c *conn, names []string) []check.Check {
	db := s.cfg.Database
	set := c.check("cdc.replica-set", check.Compatibility, "Open change streams", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
		ok, err := changeStreams(ctx, cl)
		switch {
		case err != nil:
			return failure(err)
		case !ok:
			return standalone.Fail("hosts", strings.Join(c.opts.Hosts, ", "))
		}
		p.Found("The deployment is a replica set or a sharded cluster.")
		return nil
	})
	watch := c.check("cdc.watch", check.Permission, "Watch the changes of {database}'s collections", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
		list := names
		if list == nil {
			var err error
			if list, err = s.listCollections(ctx, cl.Database(db)); err != nil {
				return c.refused(err, "listCollections action", "the database "+db, "read", db)
			}
		}
		var denied []string
		for _, n := range list {
			if !c.can("changeStream", db, n) {
				denied = append(denied, db+"."+n)
			}
		}
		if len(denied) > 0 {
			return c.missing("changeStream action", listed(denied), "read", db)
		}
		return nil
	})
	watch.Params = check.Params{"database": db}
	return []check.Check{set, watch}
}

// imagesCheck warns of collections among names, or with nil of those
// offered, that keep no pre- and post-images, which cdc reads without.
func (s *Source) imagesCheck(c *conn, names []string) check.Check {
	db := s.cfg.Database
	ch := c.check("cdc.images", check.Compatibility, "Read updates as they were made", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
		filter := bson.D{{Key: "type", Value: "collection"}}
		if names != nil {
			filter = append(filter, bson.E{Key: "name", Value: bson.D{{Key: "$in", Value: names}}})
		}
		specs, err := cl.Database(db).ListCollectionSpecifications(ctx, filter)
		if err != nil {
			return failure(err)
		}
		var off []string
		for _, sp := range specs {
			if strings.HasPrefix(sp.Name, "system.") || names == nil && len(s.cfg.Collections) > 0 && !slices.Contains(s.cfg.Collections, sp.Name) {
				continue
			}
			if on, _ := sp.Options.Lookup("changeStreamPreAndPostImages", "enabled").BooleanOK(); !on {
				off = append(off, sp.Name)
			}
		}
		slices.Sort(off)
		if len(off) > 0 {
			return noImages.Fail("collections", listed(off))
		}
		return nil
	})
	ch.Advisory = true
	ch.Description = "Only for collections read with cdc, which works without them: an update then arrives as its document is when it's read."
	return ch
}

// oplogCheck reads the oplog's window, which says how long a cdc stream's
// position lasts between syncs, and warns where it's shorter than interval,
// unless interval is 0.
func (s *Source) oplogCheck(c *conn, interval time.Duration) check.Check {
	ch := c.check("cdc.oplog", check.Resource, "Read the oplog's window", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
		first, last, err := oplogEnds(ctx, cl)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return check.Skip("the deployment has no oplog")
		}
		if err != nil {
			return c.refused(err, "find action", "local.oplog.rs", "read", "local")
		}
		window := seconds(last.T, first.T)
		p.Param("window", window)
		p.Found("The oplog holds {window} of changes.")
		if interval > 0 && window < interval {
			p.Param("interval", interval)
			return shortOplog.Fail()
		}
		return nil
	})
	ch.Advisory = true
	ch.Description = "Only for collections read with cdc: without it, the source can't say how close a stream's position is to the oplog's end."
	return ch
}

// Preflight looks at each stream as the pipeline reads it: a collection the
// user can read, an incremental one's cursor named and indexed, and for cdc,
// the change streams and access it needs, and the pre- and post-images and
// oplog window, which only warn.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	c := &conn{opts: s.opts}
	db := s.cfg.Database
	checks := []check.Check{c.signIn(nil)}
	n := checkNames{}
	var cdc []string
	for _, st := range req.Streams {
		params := check.Params{"stream": st.Name, "database": db}
		read := c.check(n.name("stream", st.Name, "read"), check.Permission, "Read {stream}", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
			specs, err := cl.Database(db).ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: st.Name}})
			switch {
			case err != nil:
				return c.refused(err, "listCollections action", "the database "+db, "read", db)
			case len(specs) == 0:
				f := check.NotFound.Fail()
				f.Message, f.Fix = "{stream} isn't a collection in {database}.", "Create it, or take the stream out of the pipeline."
				return f
			case specs[0].Type != "collection" || strings.HasPrefix(st.Name, "system."):
				return notCollection.Fail("type", cmpOr(specs[0].Type, "system collection"))
			case !c.can("find", db, st.Name):
				return c.missing("find action", db+"."+st.Name, "read", db)
			}
			return nil
		})
		read.Params = params
		checks = append(checks, read)
		switch st.SyncMode {
		case source.Incremental:
			cur := c.check(n.name("stream", st.Name, "cursor"), check.Compatibility, "Read {stream} by {cursor}", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
				if st.Cursor == "" {
					f := check.ConfigInvalid.Fail()
					f.Message, f.Fix = "{stream} is read incrementally, and names no cursor field.", "Name one of its top-level fields as the stream's cursor."
					return f
				}
				specs, err := cl.Database(db).Collection(st.Name).Indexes().ListSpecifications(ctx)
				if err != nil {
					return failure(err)
				}
				for _, ix := range specs {
					if keys, _ := ix.KeysDocument.Elements(); len(keys) > 0 && keys[0].Key() == st.Cursor {
						p.Param("index", ix.Name)
						p.Found("The index {index} starts with {cursor}.")
						return nil
					}
				}
				return check.Warn(unindexed.Fail())
			})
			cur.Params, cur.Needs = check.Params{"stream": st.Name, "cursor": st.Cursor}, []string{read.Name}
			checks = append(checks, cur)
		case source.CDC:
			cdc = append(cdc, st.Name)
		}
	}
	if len(cdc) == 0 {
		return checks
	}
	checks = append(checks, s.cdcChecks(c, cdc)...)
	return append(checks, s.imagesCheck(c, cdc), s.oplogCheck(c, req.Interval))
}

// writeActions are what the destination does in a database: create, write,
// index, rename into place and drop collections.
var writeActions = []string{"find", "insert", "update", "remove", "createCollection", "createIndex", "listIndexes", "renameCollectionSameDB", "dropCollection"}

// Checks reach the deployment, sign in, and look at what the user may do in
// the database.
func (d *Destination) Checks() []check.Check {
	c := &conn{opts: d.opts}
	checks, needs := netChecks(d.opts)
	return append(checks, c.signIn(needs), c.writeCheck(d.cfg.Database))
}

func (c *conn) writeCheck(db string) check.Check {
	ch := c.check("db.write", check.Permission, "Write collections in {database}", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
		if _, err := cl.Database(db).ListCollectionNames(ctx, bson.D{}); err != nil {
			return c.refused(err, "listCollections action", "the database "+db, "readWrite", db)
		}
		var lacks []string
		for _, a := range writeActions {
			if !c.can(a, db, fenceCollection) {
				lacks = append(lacks, a)
			}
		}
		if len(lacks) > 0 {
			return c.missing(actions(lacks), "the database "+db, "readWrite", db)
		}
		return nil
	})
	ch.Params, ch.Field = check.Params{"database": db}, "database"
	return ch
}

// Preflight looks at the pipeline's database, and at each stream's
// collection: one that exists is a collection whose validator takes what the
// stream writes, and whose unique indexes don't refuse it as the stream's
// mode and key write it; and it warns where a merged one has _ids of a kind
// extendedJSONKeys doesn't write.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	c := &conn{opts: d.opts}
	db := cmpOr(req.Namespace, d.cfg.Database)
	checks := []check.Check{c.signIn(nil), c.writeCheck(db)}
	n := checkNames{}
	for _, st := range req.Streams {
		sch, known := req.Schemas[st.Name]
		if !known || sch == nil {
			sch, known = arrow.NewSchema(nil, nil), false
		}
		coll := targetCollection(sch, st.Name)
		params := check.Params{"stream": st.Name, "collection": db + "." + coll, "key": strings.Join(st.PrimaryKey, ", ")}
		write := c.check(n.name("stream", st.Name, "write"), check.Compatibility, "Write {stream} to {collection}", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
			if st.Mode == destination.Merge && len(st.PrimaryKey) == 0 {
				f := check.ConfigInvalid.Fail()
				f.Message, f.Fix = "{stream} merges, and has no primary key.", "Give the stream a primary key, or write it with append or overwrite."
				return f
			}
			specs, err := cl.Database(db).ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: coll}})
			switch {
			case err != nil:
				return failure(err)
			case len(specs) == 0:
				p.Found("{collection} doesn't exist, and the first write creates it.")
				return nil
			case specs[0].Type != "collection":
				return unwritable.Fail("type", specs[0].Type)
			}
			v := enforced(specs[0].Options)
			if v == nil || !known {
				return nil
			}
			return writable(p, c, v, st, sch, db, coll)
		})
		write.Params, write.Needs = params, []string{"db.write"}
		checks = append(checks, write)

		indexes := c.check(n.name("stream", st.Name, "indexes"), check.Compatibility, "Write {stream} past {collection}'s unique indexes", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
			cur, err := cl.Database(db).Collection(coll).Indexes().List(ctx)
			if se, ok := errors.AsType[mongo.ServerError](err); ok && se.HasErrorCode(codeNamespaceNotFound) {
				return nil
			}
			if err != nil {
				return failure(err)
			}
			var specs []bson.Raw
			if err := cur.All(ctx, &specs); err != nil {
				return failure(err)
			}
			return uniqueConflict(st, sch, known, specs)
		})
		indexes.Params, indexes.Needs = params, []string{write.Name}
		checks = append(checks, indexes)

		if st.Mode != destination.Merge || len(st.PrimaryKey) != 1 || !known {
			continue
		}
		if i := index(sch, st.PrimaryKey[0]); i < 0 || !isString(sch.Field(i).Type) {
			continue
		}
		keys := c.check(n.name("stream", st.Name, "keys"), check.Configuration, "Find {stream}'s rows in {collection} by _id", func(ctx context.Context, p *check.Probe, cl *mongo.Client) error {
			// What the other setting of extendedJSONKeys writes.
			filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "objectId"}}}}
			if d.cfg.ExtendedJSONKeys {
				filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: `^([0-9a-f]{24}$|\{"\$|")`}}}}
			}
			err := cl.Database(db).Collection(coll).FindOne(ctx, filter, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Err()
			switch {
			case errors.Is(err, mongo.ErrNoDocuments):
				return nil
			case err != nil:
				return failure(err)
			case d.cfg.ExtendedJSONKeys:
				return check.Warn(keysAsBSON.Fail())
			}
			return check.Warn(keysAsText.Fail())
		})
		keys.Params, keys.Needs, keys.Field = params, []string{write.Name}, "extendedJSONKeys"
		checks = append(checks, keys)
	}
	return checks
}

// writable says whether v, the validator of db.coll, takes the documents st
// writes, with sch's columns: it fails where it requires a field they
// don't have or refuses one they do, and warns of the columns whose types
// it doesn't take, which the stream holds, unless the stream may change
// types and the user may widen the validator.
func writable(p *check.Probe, c *conn, v *jsonSchema, st destination.Stream, sch *arrow.Schema, db, coll string) error {
	var cols []string
	for _, f := range sch.Fields() {
		cols = append(cols, f.Name)
	}
	var lacks []string
	for _, r := range v.required() {
		if r != "_id" && !slices.Contains(cols, r) {
			lacks = append(lacks, r)
		}
	}
	if len(lacks) > 0 {
		return requires.Fail("fields", listed(lacks))
	}
	if closed, props := v.closed(); closed {
		var extra []string
		for _, col := range cols {
			if col != "_id" && !slices.Contains(props, col) {
				extra = append(extra, col)
			}
		}
		if len(extra) > 0 {
			return closedValidator.Fail("fields", listed(extra))
		}
	}
	var held []string
	for _, f := range sch.Fields() {
		if strings.HasPrefix(f.Name, "_openetl_") || f.Name == "_id" {
			continue
		}
		_, to, err := converter(f)
		if err != nil {
			return err
		}
		if from := v.kinds(f.Name); to != "" && from != nil && !accepts(from, to) {
			held = append(held, f.Name)
		}
	}
	if len(held) == 0 {
		return nil
	}
	p.Param("columns", listed(held))
	if !st.AllowTypeChanges {
		return check.Warn(heldTypes.Fail())
	}
	if !c.can("collMod", db, coll) {
		return c.missing("collMod action", db+"."+coll, "dbAdmin", db)
	}
	p.Found("{collection}'s validator doesn't take the types {stream} has in {columns}, and the first write adds them.")
	return nil
}

// uniqueConflict says whether a collection's unique indexes, specs, refuse
// what st writes: it fails where every document the stream writes repeats a
// value an index is on, and warns where the stream's rows may: a key's
// versions, appended, repeat it only if the source changes rows.
func uniqueConflict(st destination.Stream, sch *arrow.Schema, known bool, specs []bson.Raw) error {
	var warn error
	for _, s := range specs {
		name, _ := s.Lookup("name").StringValueOK()
		unique, _ := s.Lookup("unique").BooleanOK()
		if _, partial := s.Lookup("partialFilterExpression").DocumentOK(); name == "_id_" || !unique || partial {
			continue
		}
		sparse, _ := s.Lookup("sparse").BooleanOK()
		elems, _ := s.Lookup("key").Document().Elements()
		fields := make([]string, len(elems))
		absent := 0
		for i, e := range elems {
			fields[i] = e.Key()
			top, _, _ := strings.Cut(fields[i], ".")
			if known && top != "_id" && index(sch, top) < 0 {
				absent++
			}
		}
		if slices.Contains(fields, "_id") {
			continue
		}
		kv := []string{"index", name, "fields", strings.Join(fields, ", ")}
		onKey := len(st.PrimaryKey) > 0 && !slices.ContainsFunc(fields, func(f string) bool { return !slices.Contains(st.PrimaryKey, f) })
		if absent == len(fields) {
			if sparse {
				continue
			}
			return uniqueNull.Fail(kv...)
		}
		switch {
		case keyed(st) && !slices.ContainsFunc(st.PrimaryKey, func(k string) bool { return !slices.Contains(fields, k) }):
			// A document per key, which the index takes.
		case warn != nil:
		case st.Mode == destination.Append && onKey:
			warn = check.Warn(uniqueVersions.Fail(kv...))
		default:
			warn = check.Warn(uniqueValues.Fail(kv...))
		}
	}
	return warn
}

// Check is Checks run, its first failure as an error.
func (s *Source) Check(ctx context.Context) error { return check.Run(ctx, s.Checks()).Err() }

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	code := func(c protocolv1.ErrorCode) error { return sdk.WithCode(c, err) }
	if _, ok := errors.AsType[*auth.Error](err); ok {
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	}
	if se, ok := errors.AsType[mongo.ServerError](err); ok {
		switch {
		case se.HasErrorCode(codeAuthenticationFailed):
			return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
		case se.HasErrorCode(codeUnauthorized):
			return code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
		case se.HasErrorCode(codeChangeStreamLost) || se.HasErrorCode(codeChangeStreamFatal) || se.HasErrorCode(codeCappedPositionLost):
			return code(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST)
		case se.HasErrorCode(codeValidationFailure) || se.HasErrorCode(codeDuplicateKey):
			return code(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID)
		case se.HasErrorLabel("TransientTransactionError") || se.HasErrorLabel("RetryableWriteError") || se.HasErrorLabel("ResumableChangeStreamError"):
			return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
		}
	}
	if _, ok := errors.AsType[topology.ServerSelectionError](err); ok || mongo.IsNetworkError(err) || mongo.IsTimeout(err) && !errors.Is(err, context.Canceled) {
		return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	}
	return err
}
