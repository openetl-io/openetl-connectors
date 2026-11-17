//go:build integration

package kafka

import (
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The SASL_SSL broker: TLS with a test CA, and the users admin (PLAIN,
// superuser), limited (PLAIN, no ACLs) and scram (SCRAM-SHA-256 and -512,
// superuser), with OAUTHBEARER's unsecured tokens, as
// .github/actions/services/action.yml starts it.
func saslBrokers() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_KAFKA_SASL_BROKERS"), "127.0.0.1:9094")
}

const (
	adminPassword   = "admin-secret"
	limitedPassword = "limited-secret"
	scramPassword   = "scram-secret"
)

// saslCA is the CA that signs the SASL broker's certificate: the last of the
// chain it sends.
func saslCA(t testing.TB) string {
	t.Helper()
	conn, err := tls.Dial("tcp", saslBrokers(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("the SASL broker (OPENETL_TEST_KAFKA_SASL_BROKERS): %v", err)
	}
	defer conn.Close()
	chain := conn.ConnectionState().PeerCertificates
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: chain[len(chain)-1].Raw}))
}

// saslConfig is a config for the SASL broker, as user with mechanism.
func saslConfig(t testing.TB, mechanism, user, password string, extra map[string]any) []byte {
	t.Helper()
	m := map[string]any{"brokers": []string{saslBrokers()}, "tlsCa": saslCA(t), "saslMechanism": mechanism, "saslUsername": user, "saslPassword": password}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// saslAdmin is a client of the SASL broker as admin.
func saslAdmin(t testing.TB, more ...kgo.Opt) *kgo.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(saslCA(t)))
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(saslBrokers()), kgo.DialTLSConfig(&tls.Config{RootCAs: pool}),
		kgo.SASL(plain.Auth{User: "admin", Pass: adminPassword}.AsMechanism())}, more...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func saslTopic(t testing.TB, topic string, n int) {
	t.Helper()
	cl := saslAdmin(t, kgo.DefaultProduceTopic(topic))
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), 2, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	var recs []*kgo.Record
	for i := range n {
		recs = append(recs, &kgo.Record{Value: []byte(`{"id":` + strconv.Itoa(i) + `}`)})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// readAll reads topic to its end with the source.
func readAll(t *testing.T, s *Source, topic string) int {
	t.Helper()
	c := newCapture()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := s.Read(ctx, source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream}}}, c); err != nil {
		t.Fatal(err)
	}
	return c.count(topic)
}

// Each mechanism signs in over TLS verified against the config's CA, and
// the source reads and the destination writes through it.
func TestSASLMechanisms(t *testing.T) {
	topic := uniq("sasl")
	saslTopic(t, topic, 20)
	for _, tc := range []struct{ mechanism, user, password string }{
		{"plain", "admin", adminPassword},
		{"scram-sha-256", "scram", scramPassword},
		{"scram-sha-512", "scram", scramPassword},
	} {
		t.Run(tc.mechanism, func(t *testing.T) {
			s, err := NewSource(saslConfig(t, tc.mechanism, tc.user, tc.password, map[string]any{"topics": []string{topic}}))
			if err != nil {
				t.Fatal(err)
			}
			allPass(t, "source check", check.Run(t.Context(), s.Checks()))
			if n := readAll(t, s, topic); n != 20 {
				t.Errorf("read %d records", n)
			}
			d, err := NewDestination(saslConfig(t, tc.mechanism, tc.user, tc.password, nil))
			if err != nil {
				t.Fatal(err)
			}
			allPass(t, "destination check", check.Run(t.Context(), d.Checks()))
			out := uniq("sasl_out")
			allPass(t, "destination preflight", check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: out, Mode: destination.Append}}})))
			w := openWriter(t, d, uniq("p"), 1, out)
			e := destination.Epoch{Generation: 1, N: 1}
			write(t, w, e, out, 0, 5)
			st, err := w.PreCommit(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); err != nil {
				t.Fatal(err)
			}
			recs, err := readCommitted(t.Context(), saslAdmin(t), out)
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != 5 {
				t.Errorf("wrote %d records", len(recs))
			}
		})
	}
}

// A wrong password, a CA that didn't sign the broker's certificate and a
// client certificate the broker doesn't trust each fail, with their reasons
// and codes; and a user without ACLs is denied by name.
func TestSASLFailures(t *testing.T) {
	topic := uniq("sasl_acl")
	saslTopic(t, topic, 3)

	s, err := NewSource(saslConfig(t, "scram-sha-512", "scram", "wrong", map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	r := check.Run(t.Context(), s.Checks())
	want(t, "wrong password", r, map[string]string{"broker.127-0-0-1-9094.tls": "passed", "kafka.auth": "failed:credentials_rejected", "topics.read": "skipped"})
	if f := check.Classify(r.Err()); f == nil || f.Code.String() != "ERROR_CODE_AUTH_INVALID" {
		t.Errorf("wrong password: %v", r.Err())
	}
	if _, err := s.Discover(t.Context()); codeOf(err) != "ERROR_CODE_AUTH_INVALID" {
		t.Errorf("wrong password, discover: %s %v", codeOf(err), err)
	}

	other, err := NewSource(saslConfig(t, "plain", "admin", adminPassword, map[string]any{"tlsCa": testCA(t)}))
	if err != nil {
		t.Fatal(err)
	}
	want(t, "another CA", check.Run(t.Context(), other.Checks()), map[string]string{"broker.127-0-0-1-9094.tls": "failed:tls_failed", "kafka.auth": "skipped"})

	// The broker asks for a client certificate, and verifies one it gets.
	cert, key := forged(t)
	mtls, err := NewSource(saslConfig(t, "plain", "admin", adminPassword, map[string]any{"tlsCert": cert, "tlsKey": key}))
	if err != nil {
		t.Fatal(err)
	}
	want(t, "forged client certificate", check.Run(t.Context(), mtls.Checks()), map[string]string{"kafka.auth": "failed:tls_failed"})

	limited, err := NewSource(saslConfig(t, "plain", "limited", limitedPassword, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	want(t, "no ACLs", check.Run(t.Context(), limited.Checks()), map[string]string{"kafka.auth": "passed", "topics.read": "failed:privilege_missing"})
	// DESCRIBE, but not READ: the topic's authorized operations say so.
	adm := kadm.NewClient(saslAdmin(t))
	acl := kadm.NewACLs().Allow("User:limited").Topics(topic).Operations(kadm.OpDescribe).ResourcePatternType(kadm.ACLPatternLiteral)
	if _, err := adm.CreateACLs(t.Context(), acl); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = adm.DeleteACLs(context.Background(), acl) })
	awaitFor(t, "the ACL", 30*time.Second, func() bool {
		r := check.Run(t.Context(), limited.Checks())
		return statuses(r)["topics.read"] == "failed:privilege_missing" && r.Items[len(r.Items)-1].Rendered().Message == "User:limited has no READ on the topic "+topic+"."
	})
	d, err := NewDestination(saslConfig(t, "plain", "limited", limitedPassword, nil))
	if err != nil {
		t.Fatal(err)
	}
	r = check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: topic, Mode: destination.Append}}}))
	want(t, "no WRITE", r, map[string]string{"stream." + check.Name(topic) + ".write": "failed:privilege_missing", "transactions.id": "failed:privilege_missing"})
}

// OAUTHBEARER signs in with a token from a client-credentials grant, which
// the broker validates as an unsecured JWT whose subject is admin.
func TestSASLOAuthBearer(t *testing.T) {
	topic := uniq("sasl_oauth")
	saslTopic(t, topic, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, secret, _ := r.BasicAuth(); id != "openetl" || secret != "client-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error": "invalid_client"}`))
			return
		}
		now := time.Now().Unix()
		enc := base64.RawURLEncoding.EncodeToString
		claims, _ := json.Marshal(map[string]any{"sub": "admin", "iat": now, "exp": now + 3600})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": enc([]byte(`{"alg":"none"}`)) + "." + enc(claims) + ".", "token_type": "bearer", "expires_in": 3600})
	}))
	defer srv.Close()
	cfg := func(secret string) []byte {
		b, err := json.Marshal(map[string]any{"brokers": []string{saslBrokers()}, "tlsCa": saslCA(t), "saslMechanism": "oauthbearer",
			"oauthTokenUrl": srv.URL, "oauthClientId": "openetl", "oauthClientSecret": secret, "topics": []string{topic}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	s, err := NewSource(cfg("client-secret"))
	if err != nil {
		t.Fatal(err)
	}
	allPass(t, "oauth", check.Run(t.Context(), s.Checks()))
	if n := readAll(t, s, topic); n != 4 {
		t.Errorf("read %d records", n)
	}
	bad, err := NewSource(cfg("wrong"))
	if err != nil {
		t.Fatal(err)
	}
	r := check.Run(t.Context(), bad.Checks())
	want(t, "a rejected client", r, map[string]string{"kafka.auth": "failed:credentials_rejected"})
	if f := check.Classify(r.Err()); f == nil || f.Code.String() != "ERROR_CODE_AUTH_INVALID" {
		t.Errorf("a rejected client: %v", r.Err())
	}
}

// forged is a client certificate and key that claim the issuer that signs
// the SASL broker's certificate, which TLS then sends to it, but that issuer
// didn't sign.
func forged(t *testing.T) (string, string) {
	t.Helper()
	block, _ := pem.Decode([]byte(saslCA(t)))
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issuer := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: ca.Subject, RawSubject: ca.RawSubject, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "client"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, issuer, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	k, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: k}))
}
