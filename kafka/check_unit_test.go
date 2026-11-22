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
// where the credentials may not describe their configs, and fails on any
// other error, rather than skipping the check.
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
	if _, err := limit(kerr.InvalidRequest.Code); !errors.Is(err, kerr.InvalidRequest) {
		t.Errorf("a broker's error = %v", err)
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
