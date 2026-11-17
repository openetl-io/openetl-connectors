// Package kafka has the Kafka source and destination. Topics are streams.
// The destination writes each row as one record, its value the whole row and
// its key the primary key, in a transaction per epoch, with the epoch's marker
// in the same transaction. The source reads each record as one row, with its
// partition, offset, timestamp, key and headers, and resumes from each
// partition's offset.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
)

// The SASL mechanisms saslMechanism names.
const (
	saslPlain     = "plain"
	saslScram256  = "scram-sha-256"
	saslScram512  = "scram-sha-512"
	saslOAuth     = "oauthbearer"
	saslMSKIAM    = "aws-msk-iam"
	tokenTimeout  = 30 * time.Second
	dialTimeout   = 10 * time.Second
	defaultClient = "openetl"
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
	// One of the sasl constants; empty for none.
	SASLMechanism string     `json:"saslMechanism"`
	SASLUsername  string     `json:"saslUsername"`
	SASLPassword  sdk.Secret `json:"saslPassword"`
	// oauthbearer: a token from an OAuth 2.0 client-credentials grant.
	OAuthTokenURL     string            `json:"oauthTokenUrl"`
	OAuthClientID     string            `json:"oauthClientId"`
	OAuthClientSecret sdk.Secret        `json:"oauthClientSecret"`
	OAuthScopes       []string          `json:"oauthScopes"`
	OAuthExtensions   map[string]string `json:"oauthExtensions"`
	// aws-msk-iam: without a key, the AWS default credentials.
	AWSAccessKeyID     sdk.Secret `json:"awsAccessKeyId"`
	AWSSecretAccessKey sdk.Secret `json:"awsSecretAccessKey"`
	records.Config

	// Built once by validate, so every client shares its tokens.
	mech sasl.Mechanism
	tc   *tls.Config
}

func (c *Connection) validate(formats ...string) error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: brokers is empty")
	}
	for _, b := range c.Brokers {
		if _, _, err := net.SplitHostPort(b); err != nil {
			return fmt.Errorf("kafka: broker %q isn't host:port", b)
		}
	}
	if err := c.Validate(formats...); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	if (c.TLSCert == "") != c.TLSKey.IsZero() {
		return errors.New("kafka: tlsCert and tlsKey go together")
	}
	tc, err := c.tlsConfig()
	if err != nil {
		return err
	}
	c.tc = tc
	switch c.SASLMechanism {
	case "":
	case saslPlain, saslScram256, saslScram512:
		if c.SASLUsername == "" {
			return fmt.Errorf("kafka: saslMechanism %s needs saslUsername", c.SASLMechanism)
		}
	case saslOAuth:
		if c.OAuthTokenURL == "" || c.OAuthClientID == "" || c.OAuthClientSecret.IsZero() {
			return errors.New("kafka: saslMechanism oauthbearer needs oauthTokenUrl, oauthClientId and oauthClientSecret")
		}
		if u, err := url.Parse(c.OAuthTokenURL); err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
			return fmt.Errorf("kafka: oauthTokenUrl %q isn't an http or https URL", c.OAuthTokenURL)
		}
	case saslMSKIAM:
		if c.AWSAccessKeyID.IsZero() != c.AWSSecretAccessKey.IsZero() {
			return errors.New("kafka: awsAccessKeyId and awsSecretAccessKey go together")
		}
	default:
		return fmt.Errorf("kafka: saslMechanism %q is none of plain, scram-sha-256, scram-sha-512, oauthbearer and aws-msk-iam", c.SASLMechanism)
	}
	c.mech = c.mechanism()
	return nil
}

// usesTLS: tls, or a certificate that implies it.
func (c *Connection) usesTLS() bool { return c.TLS || c.TLSCA != "" || c.TLSCert != "" }

func (c *Connection) tlsConfig() (*tls.Config, error) {
	if !c.usesTLS() {
		return nil, nil
	}
	cfg := &tls.Config{ServerName: c.TLSServerName, InsecureSkipVerify: c.TLSInsecureSkipVerify, MinVersion: tls.VersionTLS12}
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

// mechanism is the configured SASL mechanism; nil for none.
func (c *Connection) mechanism() sasl.Mechanism {
	user, pass := c.SASLUsername, c.SASLPassword.Reveal()
	switch c.SASLMechanism {
	case saslPlain:
		return plain.Auth{User: user, Pass: pass}.AsMechanism()
	case saslScram256:
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()
	case saslScram512:
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()
	case saslOAuth:
		cc := &clientcredentials.Config{
			ClientID: c.OAuthClientID, ClientSecret: c.OAuthClientSecret.Reveal(),
			TokenURL: c.OAuthTokenURL, Scopes: c.OAuthScopes,
		}
		// The token source caches its token until shortly before it expires.
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Timeout: tokenTimeout})
		ts := cc.TokenSource(ctx)
		return oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			tok, err := ts.Token()
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("kafka: an OAuth token from %s: %w", c.OAuthTokenURL, err)
			}
			return oauth.Auth{Token: tok.AccessToken, Extensions: c.OAuthExtensions}, nil
		})
	case saslMSKIAM:
		return saslaws.ManagedStreamingIAM(c.awsAuth())
	}
	return nil
}

// awsAuth signs MSK IAM's challenge with the config's key, or else the AWS
// default credentials, loaded once and renewed as they expire. The region
// is the broker's host's.
func (c *Connection) awsAuth() func(context.Context) (saslaws.Auth, error) {
	var (
		mu    sync.Mutex
		creds aws.CredentialsProvider
	)
	return func(ctx context.Context) (saslaws.Auth, error) {
		mu.Lock()
		if creds == nil {
			var opts []func(*awsconfig.LoadOptions) error
			if !c.AWSAccessKeyID.IsZero() {
				opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID.Reveal(), c.AWSSecretAccessKey.Reveal(), "")))
			}
			ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
			if err != nil {
				mu.Unlock()
				return saslaws.Auth{}, fmt.Errorf("kafka: AWS config: %w", err)
			}
			creds = ac.Credentials
		}
		p := creds
		mu.Unlock()
		v, err := p.Retrieve(ctx)
		if err != nil {
			return saslaws.Auth{}, fmt.Errorf("kafka: AWS credentials: %w", err)
		}
		return saslaws.Auth{AccessKey: v.AccessKeyID, SecretKey: v.SecretAccessKey, SessionToken: v.SessionToken, UserAgent: defaultClient}, nil
	}
}

// opts are the client options every client of the connection takes.
func (c *Connection) opts() []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(defaultClient)}
	if c.tc != nil {
		opts = append(opts, kgo.Dialer((&tls.Dialer{NetDialer: &net.Dialer{Timeout: dialTimeout}, Config: c.tc}).DialContext))
	}
	if c.mech != nil {
		opts = append(opts, kgo.SASL(c.mech))
	}
	return opts
}

func (c *Connection) client(more ...kgo.Opt) (*kgo.Client, error) {
	cl, err := kgo.NewClient(append(c.opts(), more...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: client: %w", err)
	}
	return cl, nil
}
