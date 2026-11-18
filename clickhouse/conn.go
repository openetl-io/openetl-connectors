// Package clickhouse has the ClickHouse source, which reads tables in full
// or from a cursor column, in sorting-key order, and the ClickHouse
// destination, which stages each epoch in tables of its own and moves their
// parts into the target, behind a fence, with a marker per stream.
package clickhouse

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// connConfig is how both roles reach the server.
type connConfig struct {
	// host:port of the native protocol: 9000, or 9440 with TLS.
	Addr     string     `json:"addr"`
	User     string     `json:"user"`
	Password sdk.Secret `json:"password"`
	Database string     `json:"database"`
	TLS      bool       `json:"tls"`
}

func (c connConfig) check(role string) error {
	switch {
	case c.Addr == "":
		return fmt.Errorf("clickhouse: %s config needs an addr", role)
	case c.Database == "":
		return fmt.Errorf("clickhouse: %s config's database can't be empty", role)
	}
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("clickhouse: addr %q isn't host:port: %w", c.Addr, err)
	}
	return nil
}

// decodeConfig decodes a role's config into v, which embeds connConfig,
// refusing fields it doesn't have.
func decodeConfig(config []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("clickhouse: config: %w", err)
	}
	return nil
}

// options are the driver's options for a pool of at most conns
// connections, which send settings with each query.
func (c connConfig) options(conns int, settings ch.Settings) *ch.Options {
	opts := &ch.Options{
		Addr:         []string{c.Addr},
		Auth:         ch.Auth{Database: c.Database, Username: c.User, Password: c.Password.Reveal()},
		DialTimeout:  10 * time.Second,
		Settings:     settings,
		MaxOpenConns: conns,
		MaxIdleConns: conns,
		Compression:  &ch.Compression{Method: ch.CompressionLZ4},
		ClientInfo:   ch.ClientInfo{Products: []struct{ Name, Version string }{{Name: "openetl", Version: "1"}}},
	}
	if c.TLS {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return opts
}

// open returns a pool of at most conns connections, which send settings
// with each query; it connects on first use.
func (c connConfig) open(conns int, settings ch.Settings) (driver.Conn, error) {
	conn, err := ch.Open(c.options(conns, settings))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	return conn, nil
}

// connect opens a pool and pings the server.
func (c connConfig) connect(ctx context.Context, conns int, settings ch.Settings) (driver.Conn, error) {
	conn, err := c.open(conns, settings)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, classify(fmt.Errorf("clickhouse: connect to %s: %w", c.Addr, err))
	}
	return conn, nil
}

// Server error codes the connectors act on.
const (
	codeUnknownTable         = 60
	codeUnknownDatabase      = 81
	codeTableAlreadyExists   = 57
	codeIncompatibleColumns  = 122
	codeAccessDenied         = 497
	codeAuthenticationFailed = 516
	codeUnknownUser          = 192
	codeWrongPassword        = 193
	codeRequiredPassword     = 194
	codeTooManySimultaneous  = 202
	codeTimeoutExceeded      = 159
	codeTooManyParts         = 252
	codeMemoryLimitExceeded  = 241
	codeReadonly             = 164
	codeSettingConstraint    = 452
	codeQuotaExceeded        = 201
	codeSocketTimeout        = 209
	codeNetworkError         = 210
	codeTableIsReadOnly      = 242
	codeKeeperException      = 999
	codeAllConnectionTriesed = 279
)

// exception is the server's error in err, if it has one.
func exception(err error) (*ch.Exception, bool) {
	return errors.AsType[*ch.Exception](err)
}

func isCode(err error, codes ...int32) bool {
	e, ok := exception(err)
	if !ok {
		return false
	}
	for _, c := range codes {
		if e.Code == c {
			return true
		}
	}
	return false
}

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it, or that pass, TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := exception(err); ok {
		switch e.Code {
		case codeAuthenticationFailed, codeUnknownUser, codeWrongPassword, codeRequiredPassword:
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
		case codeAccessDenied, codeReadonly:
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
		case codeUnknownDatabase:
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
		case codeQuotaExceeded:
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, err)
		case codeTooManySimultaneous, codeTimeoutExceeded, codeTooManyParts, codeMemoryLimitExceeded,
			codeSocketTimeout, codeNetworkError, codeTableIsReadOnly, codeKeeperException, codeAllConnectionTriesed:
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		return err
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if _, ok := errors.AsType[net.Error](err); ok || strings.Contains(err.Error(), "connection refused") {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}

// ident quotes a ClickHouse identifier.
func ident(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), "`", "\\`") + "`"
}

// literal quotes a ClickHouse string literal.
func literal(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", `\'`) + "'"
}

func qualified(db, table string) string { return ident(db) + "." + ident(table) }
