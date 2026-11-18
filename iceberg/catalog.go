package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// CatalogConfig is how the destination reaches the catalog and the tables'
// files.
type CatalogConfig struct {
	// rest or glue. Default rest.
	Catalog string `json:"catalog"`
	// The REST catalog's URI.
	URI string `json:"uri"`
	// The REST catalog's warehouse, or Glue's location for new tables.
	Warehouse string `json:"warehouse"`
	// The tables' when the pipeline names no namespace. Default "default".
	Namespace string `json:"namespace"`
	// REST: a bearer token, or OAuth2 client credentials as id:secret.
	Token      sdk.Secret `json:"token"`
	Credential sdk.Secret `json:"credential"`
	// AWS, for Glue and S3. Without a key, the AWS default credentials.
	Region          string     `json:"region"`
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	S3Endpoint      string     `json:"s3Endpoint"`
	// More catalog and file IO properties.
	Properties map[string]string `json:"properties"`
}

// decodeConfig reads config into cfg, which has CatalogConfig's defaults
// set, refusing unknown fields.
func decodeConfig(config []byte, cfg any) error {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("iceberg: config: %w", err))
	}
	return nil
}

func defaultCatalogConfig() CatalogConfig {
	return CatalogConfig{Catalog: "rest", Namespace: "default"}
}

// conn is the way to the catalog.
type conn struct {
	cfg   CatalogConfig
	props ice.Properties
}

func newConn(cfg CatalogConfig) (*conn, error) {
	invalid := func(err error) error { return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err) }
	switch cfg.Catalog {
	case "rest":
		if cfg.URI == "" {
			return nil, invalid(errors.New("iceberg: a REST catalog needs a uri"))
		}
	case "glue":
	default:
		return nil, invalid(fmt.Errorf("iceberg: catalog %q is neither rest nor glue", cfg.Catalog))
	}
	if cfg.AccessKeyID.IsZero() != cfg.SecretAccessKey.IsZero() {
		return nil, invalid(errors.New("iceberg: accessKeyId and secretAccessKey go together"))
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	props := ice.Properties{}
	for k, v := range cfg.Properties {
		props[k] = v
	}
	if cfg.S3Endpoint != "" {
		props[iceio.S3EndpointURL] = cfg.S3Endpoint
	}
	if cfg.Region != "" {
		props[iceio.S3Region] = cfg.Region
	}
	if !cfg.AccessKeyID.IsZero() {
		props[iceio.S3AccessKeyID] = cfg.AccessKeyID.Reveal()
		props[iceio.S3SecretAccessKey] = cfg.SecretAccessKey.Reveal()
	}
	return &conn{cfg: cfg, props: props}, nil
}

// glueEndpoint is the Glue endpoint the properties name; empty for AWS's.
func (c *conn) glueEndpoint() string { return c.props["glue.endpoint"] }

// open connects to the catalog. A REST catalog answers its config, and
// with OAuth2 client credentials hands out a token, before it returns.
func (c *conn) open(ctx context.Context) (catalog.Catalog, error) {
	if c.cfg.Catalog == "glue" {
		var opts []func(*awsconfig.LoadOptions) error
		if c.cfg.Region != "" {
			opts = append(opts, awsconfig.WithRegion(c.cfg.Region))
		}
		if !c.cfg.AccessKeyID.IsZero() {
			opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.cfg.AccessKeyID.Reveal(), c.cfg.SecretAccessKey.Reveal(), "")))
		}
		ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("iceberg: AWS config: %w", err))
		}
		if ep := c.glueEndpoint(); ep != "" {
			ac.BaseEndpoint = aws.String(ep)
		}
		g, err := glue.NewCatalog(glue.WithAwsConfig(ac), glue.WithAwsProperties(glue.AwsProperties(c.props)))
		if err != nil {
			return nil, fmt.Errorf("iceberg: glue: %w", err)
		}
		return g, nil
	}
	opts := []rest.Option{rest.WithAdditionalProps(c.props)}
	if c.cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(c.cfg.Warehouse))
	}
	if !c.cfg.Token.IsZero() {
		opts = append(opts, rest.WithOAuthToken(c.cfg.Token.Reveal()))
	}
	if !c.cfg.Credential.IsZero() {
		opts = append(opts, rest.WithCredential(c.cfg.Credential.Reveal()))
	}
	r, err := rest.NewCatalog(ctx, "openetl", c.cfg.URI, opts...)
	if err != nil {
		return nil, classify(fmt.Errorf("iceberg: catalog %s: %w", c.cfg.URI, err))
	}
	return r, nil
}

// namespace is ns, or the config's, as a catalog identifier.
func (c *conn) namespace(ns string) []string {
	if ns == "" {
		ns = c.cfg.Namespace
	}
	return strings.Split(ns, ".")
}

// classify gives the catalog's and the store's refusals their codes, and
// failures to reach them TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return err
	}
	code := protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
	switch {
	case errors.Is(err, context.Canceled):
		return err
	case errors.Is(err, rest.ErrUnauthorized), errors.Is(err, rest.ErrOAuthError):
		code = protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
	case errors.Is(err, rest.ErrAuthorizationExpired):
		code = protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED
	case errors.Is(err, rest.ErrForbidden):
		code = protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	case errors.Is(err, rest.ErrServiceUnavailable), errors.Is(err, rest.ErrServerError):
		code = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
	case errors.Is(err, catalog.ErrNoSuchNamespace), errors.Is(err, catalog.ErrNoSuchTable):
		code = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID
	}
	if ae, ok := errors.AsType[smithy.APIError](err); ok && code == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		switch ae.ErrorCode() {
		case "UnrecognizedClientException", "InvalidSignatureException", "InvalidClientTokenId", "SignatureDoesNotMatch", "InvalidAccessKeyId", "IncompleteSignature":
			code = protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
		case "ExpiredTokenException", "ExpiredToken", "RequestExpired":
			code = protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED
		case "AccessDeniedException", "AccessDenied", "AllAccessDisabled":
			code = protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
		case "ThrottlingException", "SlowDown", "RequestLimitExceeded":
			code = protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED
		case "InternalServiceException", "InternalError", "ServiceUnavailable":
			code = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
		}
	}
	if _, ok := errors.AsType[net.Error](err); ok && code == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		code = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
	}
	if code == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return err
	}
	return sdk.WithCode(code, err)
}
