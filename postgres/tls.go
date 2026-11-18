package postgres

import (
	"crypto/tls"
	"crypto/x509"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

// TLS is what both roles' configs say of TLS besides the dsn: PEM given in
// place, where the dsn's sslrootcert, sslcert and sslkey name files.
type TLS struct {
	// PEM certificates of the authorities that sign the server's
	// certificate.
	CA string `json:"tlsCa"`
	// A PEM client certificate, and its key.
	Cert string     `json:"tlsCert"`
	Key  sdk.Secret `json:"tlsKey"`
}

// apply sets cfg's TLS from t. Either makes TLS required: plaintext
// attempts the dsn's sslmode allows are dropped, and sslmode=disable is an
// error. With a CA, the server's certificate must chain to it, and also
// name the host if sslmode is verify-full; under require and prefer as under
// verify-ca, as libpq does with a root certificate.
func (t TLS) apply(cfg *pgconn.Config) error {
	if t.CA == "" && t.Cert == "" && t.Key.IsZero() {
		return nil
	}
	if (t.Cert == "") != t.Key.IsZero() {
		return errors.New("postgres: tlsCert and tlsKey go together")
	}
	if cfg.TLSConfig == nil {
		return errors.New("postgres: the dsn's sslmode is disable or allow, and tlsCa, tlsCert and tlsKey need TLS: use require, verify-ca or verify-full")
	}
	var roots *x509.CertPool
	if t.CA != "" {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(t.CA)) {
			return errors.New("postgres: tlsCa has no PEM certificate")
		}
	}
	var certs []tls.Certificate
	if t.Cert != "" {
		pair, err := tls.X509KeyPair([]byte(t.Cert), []byte(t.Key.Reveal()))
		if err != nil {
			return errors.New("postgres: tlsCert and tlsKey aren't a PEM certificate and its key")
		}
		certs = []tls.Certificate{pair}
	}
	set := func(c *tls.Config) {
		if roots != nil {
			c.RootCAs = roots
			if c.InsecureSkipVerify {
				// Verify the chain, without the host.
				c.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
					return verifyChain(raw, roots)
				}
			}
		}
		if certs != nil {
			c.Certificates = certs
		}
	}
	set(cfg.TLSConfig)
	fallbacks := cfg.Fallbacks[:0]
	for _, f := range cfg.Fallbacks {
		if f.TLSConfig != nil {
			set(f.TLSConfig)
			fallbacks = append(fallbacks, f)
		}
	}
	cfg.Fallbacks = fallbacks
	return nil
}

func verifyChain(raw [][]byte, roots *x509.CertPool) error {
	if len(raw) == 0 {
		return errors.New("postgres: the server sent no certificate")
	}
	certs := make([]*x509.Certificate, len(raw))
	for i, b := range raw {
		c, err := x509.ParseCertificate(b)
		if err != nil {
			return err
		}
		certs[i] = c
	}
	opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
	for _, c := range certs[1:] {
		opts.Intermediates.AddCert(c)
	}
	_, err := certs[0].Verify(opts)
	return err
}
