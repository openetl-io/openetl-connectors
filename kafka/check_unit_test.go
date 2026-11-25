package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func itemsOf(checks []check.Check) []string {
	out := make([]string, len(checks))
	for i, c := range checks {
		out[i] = c.Name
	}
	return out
}

// statuses are a report's items' statuses by name, failures with their
// reason.
func statuses(r check.Report) map[string]string {
	out := map[string]string{}
	for _, it := range r.Items {
		s := it.Status.String()
		if it.Failure != nil {
			s += ":" + it.Failure.Reason
		}
		out[it.Name] = s
	}
	return out
}

// rendered is a report's item of that name, its templates filled in.
func rendered(t *testing.T, r check.Report, name string) check.Item {
	t.Helper()
	for _, it := range r.Items {
		if it.Name == name {
			return it.Rendered()
		}
	}
	t.Fatalf("no item %s", name)
	return check.Item{}
}

// mskServerless makes c answer as MSK Serverless did: INVALID_PARTITIONS to
// a topic to be created with the brokers' default number of partitions,
// whether or not its replication factor is the brokers' too, and
// INVALID_REQUEST to a describe of a broker's configs.
func mskServerless(c *kfake.Cluster) {
	c.ControlKey(int16(kmsg.CreateTopics), func(kr kmsg.Request) (kmsg.Response, error, bool) {
		req := kr.(*kmsg.CreateTopicsRequest)
		resp := req.ResponseKind().(*kmsg.CreateTopicsResponse)
		for _, rt := range req.Topics {
			if rt.NumPartitions != -1 {
				return nil, nil, false
			}
			st := kmsg.NewCreateTopicsResponseTopic()
			st.Topic, st.ErrorCode = rt.Topic, kerr.InvalidPartitions.Code
			resp.Topics = append(resp.Topics, st)
		}
		c.KeepControl()
		return resp, nil, true
	})
	refuseBrokerConfigs(c, kerr.InvalidRequest.Code)
}

// refuseBrokerConfigs makes c answer every describe of a broker's configs
// with code.
func refuseBrokerConfigs(c *kfake.Cluster, code int16) {
	c.ControlKey(int16(kmsg.DescribeConfigs), func(kr kmsg.Request) (kmsg.Response, error, bool) {
		req := kr.(*kmsg.DescribeConfigsRequest)
		resp := req.ResponseKind().(*kmsg.DescribeConfigsResponse)
		for _, r := range req.Resources {
			if r.ResourceType != kmsg.ConfigResourceTypeBroker {
				return nil, nil, false
			}
			rr := kmsg.NewDescribeConfigsResponseResource()
			rr.ResourceName, rr.ResourceType, rr.ErrorCode = r.ResourceName, r.ResourceType, code
			resp.Resources = append(resp.Resources, rr)
		}
		c.KeepControl()
		return resp, nil, true
	})
}

// refuseCreates makes c answer every creation of a topic with code.
func refuseCreates(c *kfake.Cluster, code int16) {
	c.ControlKey(int16(kmsg.CreateTopics), func(kr kmsg.Request) (kmsg.Response, error, bool) {
		req := kr.(*kmsg.CreateTopicsRequest)
		resp := req.ResponseKind().(*kmsg.CreateTopicsResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewCreateTopicsResponseTopic()
			st.Topic, st.ErrorCode = rt.Topic, code
			resp.Topics = append(resp.Topics, st)
		}
		c.KeepControl()
		return resp, nil, true
	})
}

// The lists are valid for any config and request, topics whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	s, err := NewSource([]byte(`{"brokers": ["a:9092", "b:9092"], "tls": true, "saslMechanism": "scram-sha-512", "saslUsername": "u", "saslPassword": "p",
		"schemaRegistryUrl": "http://r:8081", "format": "avro", "topics": ["a.b", "a-b"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(s.Checks()); err != nil {
		t.Error(err)
	}
	want := []string{"broker.a-9092.connect", "broker.a-9092.tls", "broker.b-9092.connect", "broker.b-9092.tls", "kafka.auth", "cluster.advertised",
		"registry.connect", "registry.auth", "topics.read"}
	if got := itemsOf(s.Checks()); !slices.Equal(got, want) {
		t.Errorf("source checks %v, want %v", got, want)
	}
	for _, c := range s.Checks()[:4] {
		if !c.Advisory {
			t.Errorf("%s isn't advisory with two brokers", c.Name)
		}
	}
	req := source.PreflightRequest{Pipeline: "p", Interval: time.Hour, Streams: []source.ConfiguredStream{{Name: "a.b", SyncMode: source.Stream}, {Name: "a-b"}}}
	pf := s.Preflight(req)
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}
	want = []string{"stream.a-b.mode", "stream.a-b.read", "stream.a-b.retention", "stream.a-b.schema",
		"stream.a-b-2.mode", "stream.a-b-2.read", "stream.a-b-2.retention", "stream.a-b-2.schema", "group.commit"}
	if got := itemsOf(pf); !slices.Equal(got, want) {
		t.Errorf("source preflight %v, want %v", got, want)
	}
	// Without a schedule, nothing of retention; with JSON, nothing of the
	// registry; with one broker, its checks fail.
	s, err = NewSource([]byte(`{"brokers": ["k:9092"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := itemsOf(s.Checks()), []string{"broker.k-9092.connect", "kafka.auth", "cluster.advertised", "topics.list"}; !slices.Equal(got, want) {
		t.Errorf("plain source checks %v, want %v", got, want)
	}
	if s.Checks()[0].Advisory {
		t.Error("the only broker's check is advisory")
	}
	if got, want := itemsOf(s.Preflight(source.PreflightRequest{Streams: req.Streams[:1]})), []string{"stream.a-b.mode", "stream.a-b.read", "group.commit"}; !slices.Equal(got, want) {
		t.Errorf("plain source preflight %v, want %v", got, want)
	}

	d, err := NewDestination([]byte(`{"brokers": ["k:9092"], "tlsCa": "` + strings.ReplaceAll(testCA(t), "\n", `\n`) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := itemsOf(d.Checks()), []string{"broker.k-9092.connect", "broker.k-9092.tls", "kafka.auth", "cluster.advertised", "markers.topic", "cluster.transaction-timeout"}; !slices.Equal(got, want) {
		t.Errorf("destination checks %v, want %v", got, want)
	}
	if f := d.Checks()[1].Field; f != "tlsCa" {
		t.Errorf("the TLS check's field is %q", f)
	}
	dpf := d.Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "x y"}, {Name: "x.y", Mode: destination.Merge}}})
	if err := check.Validate(dpf); err != nil {
		t.Error(err)
	}
	if got, want := itemsOf(dpf), []string{"stream.x-y.mode", "stream.x-y.write", "stream.x-y-2.mode", "stream.x-y-2.write", "transactions.id"}; !slices.Equal(got, want) {
		t.Errorf("destination preflight %v, want %v", got, want)
	}
	// A merge stream fails its mode, before any I/O.
	if err := dpf[2].Run(t.Context(), &check.Probe{}); check.Classify(err).Reason != "write_mode_unsupported" {
		t.Errorf("a merge stream's mode: %v", err)
	}
}

// Configs that can't work are refused when the connector starts.
func TestConfigRefused(t *testing.T) {
	for _, cfg := range []string{
		`{"brokers": []}`,
		`{"brokers": ["no-port"]}`,
		`{"brokers": ["k:9092"], "saslMechanism": "gssapi"}`,
		`{"brokers": ["k:9092"], "saslMechanism": "plain"}`,
		`{"brokers": ["k:9092"], "saslMechanism": "oauthbearer", "oauthTokenUrl": "https://idp/token"}`,
		`{"brokers": ["k:9092"], "saslMechanism": "oauthbearer", "oauthTokenUrl": "idp", "oauthClientId": "c", "oauthClientSecret": "s"}`,
		`{"brokers": ["k:9092"], "saslMechanism": "aws-msk-iam", "awsAccessKeyId": "AKIA"}`,
		`{"brokers": ["k:9092"], "tlsCa": "not a certificate"}`,
		`{"brokers": ["k:9092"], "tlsCert": "x"}`,
	} {
		if _, err := NewSource([]byte(cfg)); err == nil {
			t.Errorf("%s was taken", cfg)
		}
	}
}

// OAUTHBEARER takes a token from the client-credentials grant, with the
// scopes asked for, sends it with the extensions, and keeps it until it
// expires.
func TestOAuthBearer(t *testing.T) {
	var asked atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		id, secret, _ := r.BasicAuth()
		if r.FormValue("grant_type") != "client_credentials" || r.FormValue("scope") != "kafka read" || id != "client" || secret != "s3cret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error": "invalid_client"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "token_type": "bearer", "expires_in": 3600})
	}))
	defer srv.Close()
	c := Connection{Brokers: []string{"k:9092"}, SASLMechanism: saslOAuth, OAuthTokenURL: srv.URL, OAuthClientID: "client",
		OAuthClientSecret: secret("s3cret"), OAuthScopes: []string{"kafka", "read"}, OAuthExtensions: map[string]string{"logicalCluster": "lkc-1"}}
	if err := c.validate("json"); err != nil {
		t.Fatal(err)
	}
	if c.mech.Name() != "OAUTHBEARER" {
		t.Fatalf("mechanism %s", c.mech.Name())
	}
	for range 2 {
		_, first, err := c.mech.Authenticate(context.Background(), "k:9092")
		if err != nil {
			t.Fatal(err)
		}
		if want := "n,,\x01auth=Bearer tok\x01logicalCluster=lkc-1\x01\x01"; string(first) != want {
			t.Errorf("first message %q, want %q", first, want)
		}
	}
	if n := asked.Load(); n != 1 {
		t.Errorf("the token was asked for %d times", n)
	}
	c.OAuthClientSecret = secret("wrong")
	if err := c.validate("json"); err != nil {
		t.Fatal(err)
	}
	_, _, err := c.mech.Authenticate(context.Background(), "k:9092")
	if code := codeOf(classify(err)); code != "ERROR_CODE_AUTH_INVALID" {
		t.Errorf("a rejected client: %s, %v", code, err)
	}
}

// AWS_MSK_IAM signs its challenge with the config's key, in the broker's
// region.
func TestMSKIAM(t *testing.T) {
	// Static keys don't depend on the host's AWS config, even a broken one.
	t.Setenv("AWS_PROFILE", "openetl-missing-profile")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing"))
	c := Connection{Brokers: []string{"b-1.c.kafka.eu-west-2.amazonaws.com:9098"}, SASLMechanism: saslMSKIAM,
		AWSAccessKeyID: secret("AKIATEST"), AWSSecretAccessKey: secret("shh")}
	if err := c.validate("json"); err != nil {
		t.Fatal(err)
	}
	if c.mech.Name() != "AWS_MSK_IAM" {
		t.Fatalf("mechanism %s", c.mech.Name())
	}
	_, first, err := c.mech.Authenticate(context.Background(), "b-1.c.kafka.eu-west-2.amazonaws.com:9098")
	if err != nil {
		t.Fatal(err)
	}
	var msg map[string]string
	if err := json.Unmarshal(first, &msg); err != nil {
		t.Fatal(err)
	}
	if msg["action"] != "kafka-cluster:Connect" || !strings.HasPrefix(msg["x-amz-credential"], "AKIATEST/") || !strings.Contains(msg["x-amz-credential"], "/eu-west-2/kafka-cluster/") || msg["x-amz-signature"] == "" {
		t.Errorf("challenge %v", msg)
	}
	if strings.Contains(string(first), "shh") {
		t.Error("the challenge holds the secret key")
	}
}

func secret(v string) sdk.Secret { return sdk.NewSecret(v) }

// codeOf is the protocol code in err's chain, by name.
func codeOf(err error) string {
	var c interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &c) {
		return c.ErrorCode().String()
	}
	return "none"
}

// testCA is a new self-signed CA certificate, PEM.
func testCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "untrusted-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// transactionLimit reads the brokers' transaction.max.timeout.ms, is 0 only
// where the credentials may not describe their configs, is
// errNoBrokerConfigs only where the brokers answer INVALID_REQUEST, as MSK
// Serverless's do, and fails on any other error.
func TestTransactionLimit(t *testing.T) {
	limit := func(code int16) (time.Duration, error) {
		c := fakeCluster(t)
		if code != 0 {
			c.ControlKey(int16(kmsg.DescribeConfigs), func(kr kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				req := kr.(*kmsg.DescribeConfigsRequest)
				resp := req.ResponseKind().(*kmsg.DescribeConfigsResponse)
				for _, r := range req.Resources {
					rr := kmsg.NewDescribeConfigsResponseResource()
					rr.ResourceName, rr.ResourceType, rr.ErrorCode = r.ResourceName, r.ResourceType, code
					resp.Resources = append(resp.Resources, rr)
				}
				return resp, nil, true
			})
		}
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		return transactionLimit(t.Context(), kadm.NewClient(cl))
	}
	if got, err := limit(0); err != nil || got != 15*time.Minute {
		t.Errorf("the brokers' limit = %v, %v", got, err)
	}
	if got, err := limit(kerr.ClusterAuthorizationFailed.Code); err != nil || got != 0 {
		t.Errorf("without DESCRIBE_CONFIGS = %v, %v", got, err)
	}
	if _, err := limit(kerr.InvalidRequest.Code); !errors.Is(err, errNoBrokerConfigs) || !errors.Is(err, kerr.InvalidRequest) {
		t.Errorf("MSK Serverless's answer = %v", err)
	}
	if _, err := limit(kerr.UnknownServerError.Code); !errors.Is(err, kerr.UnknownServerError) || errors.Is(err, errNoBrokerConfigs) {
		t.Errorf("a broker's error = %v", err)
	}
}

// On a cluster that answers as MSK Serverless did, the destination's check
// fails the marker topic with a config error that says to set partitions,
// rather than an internal one, and skips the brokers'
// transaction.max.timeout.ms, which it can't read, saying what it must be.
// The preflight fails a stream's new topic as the check does the marker
// topic. With partitions set, the check passes, the skip aside.
func TestDestinationCheckOnMSKServerless(t *testing.T) {
	c := fakeCluster(t)
	mskServerless(c)
	dest := func(extra map[string]any) *Destination {
		d, err := NewDestination(fakeConfig(t, c.ListenAddrs(), extra))
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	r := check.Run(t.Context(), dest(nil).Checks())
	if got := statuses(r); got["markers.topic"] != "failed:partitions_required" || got["cluster.transaction-timeout"] != "skipped" {
		t.Errorf("without partitions: %v", got)
	}
	if code := codeOf(r.Err()); code != "ERROR_CODE_CONFIG_INVALID" {
		t.Errorf("without partitions, the check fails with %s: %v", code, r.Err())
	}
	if it := rendered(t, r, "markers.topic"); !strings.Contains(it.Message, "_openetl_markers needs partitions set") || it.Failure.Fix != "Set partitions to the number of partitions to create topics with." {
		t.Errorf("markers.topic: %s; fix: %s", it.Message, it.Failure.Fix)
	}
	it := rendered(t, r, "cluster.transaction-timeout")
	for _, s := range []string{"MSK Serverless", "transaction.max.timeout.ms", "at least twice the commit interval and a minute", "1m20s for the default interval of 10s", "15m0s for the longest, 7m0s"} {
		if !strings.Contains(it.Message, s) {
			t.Errorf("cluster.transaction-timeout's message doesn't say %q: %s", s, it.Message)
		}
	}
	pf := check.Run(t.Context(), dest(nil).Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "orders"}}}))
	if got := statuses(pf)["stream.orders.write"]; got != "failed:partitions_required" {
		t.Errorf("a new topic's preflight without partitions: %s", got)
	}

	r = check.Run(t.Context(), dest(map[string]any{"partitions": 3}).Checks())
	if got := statuses(r); got["markers.topic"] != "passed" || got["cluster.transaction-timeout"] != "skipped" || !r.OK() {
		t.Errorf("with partitions: %v, %v", got, r.Err())
	}
	pf = check.Run(t.Context(), dest(map[string]any{"partitions": 3}).Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "orders"}}}))
	if got := statuses(pf)["stream.orders.write"]; got != "passed" {
		t.Errorf("a new topic's preflight with partitions: %s", got)
	}
}

// The check of the brokers' transaction.max.timeout.ms passes a limit that
// fits, fails one too low and an error other than MSK Serverless's, and
// skips where the credentials may not describe the brokers' configs, as it
// did before MSK Serverless's skip.
func TestTransactionTimeoutCheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []kfake.Opt
		// A broker resource's error code; 0 for none.
		code int16
		want string
	}{
		{"the brokers' default", nil, 0, "passed"},
		{"a limit too low", []kfake.Opt{kfake.BrokerConfigs(map[string]string{"transaction.max.timeout.ms": "60000"})}, 0, "failed:setting_mismatch"},
		{"a broker's error", nil, kerr.UnknownServerError.Code, "failed:unknown"},
		{"no DESCRIBE_CONFIGS", nil, kerr.ClusterAuthorizationFailed.Code, "skipped"},
	} {
		c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, tc.opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if tc.code != 0 {
			refuseBrokerConfigs(c, tc.code)
		}
		d, err := NewDestination(fakeConfig(t, c.ListenAddrs(), nil))
		if err != nil {
			t.Fatal(err)
		}
		r := check.Run(t.Context(), d.Checks())
		if got := statuses(r)["cluster.transaction-timeout"]; got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
		if it := rendered(t, r, "cluster.transaction-timeout"); tc.want == "skipped" && !strings.Contains(it.Message, "DESCRIBE_CONFIGS") {
			t.Errorf("%s: skipped with %q", tc.name, it.Message)
		}
	}
}

// A topic the brokers won't create with the config's partitions or
// replication factor, or with their defaults where it sets none, fails the
// check with a config error that names the setting. Other refusals fail as
// they did.
func TestCheckOfATopicTheBrokersWontCreate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra map[string]any
		// What the brokers answer every creation with; 0: kfake's own
		// answer.
		code int16
		want string
	}{
		{"partitions refused", map[string]any{"partitions": 3}, kerr.InvalidPartitions.Code, "failed:partitions_refused"},
		{"no default replication factor", nil, kerr.InvalidReplicationFactor.Code, "failed:replication_factor_required"},
		{"more replicas than brokers", map[string]any{"replicationFactor": 2}, 0, "failed:replication_factor_refused"},
		{"no CREATE", nil, kerr.TopicAuthorizationFailed.Code, "failed:privilege_missing"},
		{"a policy", nil, kerr.PolicyViolation.Code, "failed:unknown"},
	} {
		c := fakeCluster(t)
		if tc.code != 0 {
			refuseCreates(c, tc.code)
		}
		d, err := NewDestination(fakeConfig(t, c.ListenAddrs(), tc.extra))
		if err != nil {
			t.Fatal(err)
		}
		r := check.Run(t.Context(), d.Checks())
		if got := statuses(r)["markers.topic"]; got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}

// PLAIN without TLS signs in with a warning: its password crosses the
// network as it is. SCRAM's doesn't.
func TestPlainWithoutTLSWarns(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.EnableSASL(), kfake.Superuser("PLAIN", "alice", "s3cret"), kfake.Superuser("SCRAM-SHA-256", "alice", "s3cret"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for mech, want := range map[string]string{saslPlain: "warning:credentials_unencrypted", saslScram256: "passed"} {
		s, err := NewSource(fakeConfig(t, c.ListenAddrs(), map[string]any{"topicPattern": ".*", "saslMechanism": mech, "saslUsername": "alice", "saslPassword": "s3cret"}))
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range check.Run(t.Context(), s.Checks()).Items {
			got := it.Status.String()
			if it.Failure != nil {
				got += ":" + it.Failure.Reason
			}
			if it.Name == "kafka.auth" && got != want {
				t.Errorf("%s: kafka.auth %s, want %s", mech, got, want)
			}
		}
	}
}
