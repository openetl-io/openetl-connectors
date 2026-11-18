package postgres

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestTLSConfigErrors(t *testing.T) {
	ca := newAuthority(t).pem
	for name, c := range map[string]struct{ dsn, extra, want string }{
		"no TLS":            {"postgres://u@h/db?sslmode=disable", `, "tlsCa": "x"`, "sslmode"},
		"TLS allowed":       {"postgres://u@h/db?sslmode=allow", `, "tlsCa": "x"`, "sslmode"},
		"a cert, no key":    {"postgres://u@h/db?sslmode=require", `, "tlsCert": "x"`, "go together"},
		"a key, no cert":    {"postgres://u@h/db?sslmode=require", `, "tlsKey": "x"`, "go together"},
		"no PEM":            {"postgres://u@h/db?sslmode=require", `, "tlsCa": "not PEM"`, "no PEM"},
		"a bad client pair": {"postgres://u@h/db?sslmode=require", `, "tlsCert": "x", "tlsKey": "y"`, "aren't"},
	} {
		cfg := fmt.Sprintf(`{"dsn": %q%s}`, c.dsn, c.extra)
		if _, err := NewSource([]byte(cfg)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("source, %s: %v", name, err)
		}
		if _, err := NewDestination([]byte(cfg)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("destination, %s: %v", name, err)
		}
	}
	// Plaintext attempts are dropped where TLS is only preferred.
	s, err := NewSource([]byte(`{"dsn": "postgres://u@h/db?sslmode=prefer", "tlsCa": ` + fmt.Sprintf("%q", ca) + `}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range s.conn.Fallbacks {
		if f.TLSConfig == nil {
			t.Error("a plaintext fallback is left")
		}
	}
}

// authority is a throwaway certificate authority.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

func newAuthority(t *testing.T) authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "openetl test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return authority{cert: cert, key: key, pem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

// issue signs a certificate for name, a server's DNS name or a client's
// common name; it returns the certificate's PEM and its key's.
func (a authority) issue(t *testing.T, name string, server bool) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if server {
		tmpl.DNSNames, tmpl.ExtKeyUsage = []string{name}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}))
}
