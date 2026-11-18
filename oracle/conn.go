// Package oracle has the Oracle Database source, which reads tables in full,
// by a cursor column or by their changes in the redo log through LogMiner,
// and the Oracle destination.
package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// sessionSettings spell dates, timestamps and numbers in every session as
// the source parses them, in UTC, both in what queries return and in
// LogMiner's SQL_REDO; and compare and sort text by its bytes, so a key's
// order is the same in every query.
const sessionSettings = `ALTER SESSION SET NLS_DATE_FORMAT = 'SYYYY-MM-DD HH24:MI:SS'
	NLS_TIMESTAMP_FORMAT = 'SYYYY-MM-DD HH24:MI:SS.FF9'
	NLS_TIMESTAMP_TZ_FORMAT = 'SYYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'
	NLS_NUMERIC_CHARACTERS = '.,'
	NLS_SORT = BINARY
	NLS_COMP = BINARY
	TIME_ZONE = '+00:00'`

// parseDSN checks a DSN as go-ora takes it: oracle://user:password@host:port/service.
func parseDSN(dsn string) error {
	if !strings.HasPrefix(strings.ToLower(dsn), "oracle://") {
		return errors.New("oracle: the dsn is a URL, oracle://user:password@host:port/service")
	}
	_, err := go_ora.ParseConfig(dsn)
	if _, ok := errors.AsType[*url.Error](err); ok {
		// It quotes the URL, escaped, and its cause can quote a piece of the
		// password, such as one a / cuts short.
		return errors.New("oracle: the dsn isn't a URL: percent-encode the characters a URL reserves, such as / as %2F and @ as %40, in its user and password")
	}
	if err != nil {
		return errors.New(redacted("oracle: dsn: "+err.Error(), dsn))
	}
	return nil
}

// open connects to the database, every session with sessionSettings, and
// checks it answers.
func open(ctx context.Context, dsn string, conns int) (*sql.DB, error) {
	db := sql.OpenDB(sessionConnector{Connector: go_ora.NewConnector(dsn)})
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, classify(fmt.Errorf("oracle: connect: %w", err))
	}
	return db, nil
}

// sessionConnector sets each new session's settings.
type sessionConnector struct {
	driver.Connector
}

func (c sessionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	ex, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("oracle: the driver's connection can't execute statements")
	}
	if _, err := ex.ExecContext(ctx, sessionSettings, nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// oraCode is the ORA- error number of err, 0 if it isn't one.
func oraCode(err error) int {
	if oe, ok := errors.AsType[*network.OracleError](err); ok {
		return oe.ErrCode
	}
	return 0
}

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	code := oraCode(err)
	if code == 0 {
		switch {
		case errors.Is(err, context.Canceled):
			return err
		case errors.Is(err, driver.ErrBadConn), errors.Is(err, sql.ErrConnDone), errors.Is(err, io.EOF),
			errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		if _, ok := errors.AsType[net.Error](err); ok {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		return err
	}
	switch code {
	case 1017, 1005, 28000, 28001, 28009, 28040, 12638:
		// Invalid credentials, a locked or expired account.
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case 1031, 1045, 1749, 41900, 65040:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case 12505, 12514, 12154, 1918:
		// No such service or user.
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	case 1291, 1292, 1284:
		// The redo the read would continue from is gone.
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, err)
	case 18, 20, 51, 54, 60, 1012, 1033, 1034, 1089, 1090, 1092, 3113, 3114, 3135, 4021, 4068, 8177,
		12170, 12500, 12516, 12518, 12519, 12520, 12521, 12526, 12527, 12528, 12537, 12541, 12543, 12547, 12560, 12571, 25408, 30006, 38500:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}

func configInvalid(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf(format, args...))
}

func positionLost(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf(format, args...))
}

func transient(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf(format, args...))
}

func denied(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, fmt.Errorf(format, args...))
}

// quoteIdent quotes an identifier as Oracle spells it: exactly, case and
// all.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
