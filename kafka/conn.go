// Package kafka has the Kafka source and destination. Topics are streams.
// The destination writes each row as one record, its value the whole row and
// its key the primary key, in a transaction per epoch, with the epoch's marker
// in the same transaction. The source reads each record as one row, with its
// partition, offset, timestamp, key and headers, and resumes from each
// partition's offset.
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
)

// Connection is the config both connectors share: brokers, TLS, SASL, the
// schema registry and the format.
type Connection struct {
	Brokers               []string   `json:"brokers"`
	TLS                   bool       `json:"tls"`
	TLSCA                 string     `json:"tlsCa"`
	TLSCert               string     `json:"tlsCert"`
	TLSKey                sdk.Secret `json:"tlsKey"`
	TLSServerName         string     `json:"tlsServerName"`
	TLSInsecureSkipVerify bool       `json:"tlsInsecureSkipVerify"`
	// plain, scram-sha-256 or scram-sha-512; empty for none.
	SASLMechanism string     `json:"saslMechanism"`
	SASLUsername  string     `json:"saslUsername"`
	SASLPassword  sdk.Secret `json:"saslPassword"`
	records.Config
}

func (c *Connection) validate(formats ...string) error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: brokers is empty")
	}
	if err := c.Validate(formats...); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	switch c.SASLMechanism {
	case "", "plain", "scram-sha-256", "scram-sha-512":
	default:
		return fmt.Errorf("kafka: saslMechanism %q is none of plain, scram-sha-256 and scram-sha-512", c.SASLMechanism)
	}
	if (c.TLSCert == "") != c.TLSKey.IsZero() {
		return errors.New("kafka: tlsCert and tlsKey go together")
	}
	return nil
}

func (c *Connection) tlsConfig() (*tls.Config, error) {
	if !c.TLS && c.TLSCA == "" && c.TLSCert == "" {
		return nil, nil
	}
	cfg := &tls.Config{ServerName: c.TLSServerName, InsecureSkipVerify: c.TLSInsecureSkipVerify, MinVersion: tls.VersionTLS12} //nolint:gosec // the user's choice
	if c.TLSCA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(c.TLSCA)) {
			return nil, errors.New("kafka: tlsCa holds no PEM certificate")
		}
		cfg.RootCAs = pool
	}
	if c.TLSCert != "" {
		cert, err := tls.X509KeyPair([]byte(c.TLSCert), []byte(c.TLSKey.Reveal()))
		if err != nil {
			return nil, fmt.Errorf("kafka: tlsCert and tlsKey: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// opts are the client options every client of the connection takes.
func (c *Connection) opts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID("openetl")}
	tc, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, kgo.Dialer((&tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tc}).DialContext))
	}
	user, pass := c.SASLUsername, c.SASLPassword.Reveal()
	switch c.SASLMechanism {
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
	}
	return opts, nil
}

func (c *Connection) client(more ...kgo.Opt) (*kgo.Client, error) {
	opts, err := c.opts()
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(append(opts, more...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: client: %w", err)
	}
	return cl, nil
}
