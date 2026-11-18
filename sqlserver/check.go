package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	mssql "github.com/microsoft/go-mssqldb"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func configInvalid(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf(format, args...))
}

func positionLost(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf(format, args...))
}

func transient(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf(format, args...))
}

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &coded) {
		return err
	}
	me, ok := errors.AsType[mssql.Error](err)
	if !ok {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return err
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNREFUSED),
			errors.Is(err, syscall.ECONNRESET), errors.Is(err, sql.ErrConnDone):
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		if _, ok := errors.AsType[net.Error](err); ok {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		if _, ok := errors.AsType[mssql.StreamError](err); ok {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		return err
	}
	switch me.Number {
	case 18456, 18452, 18486, 18487, 18488:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case 4060, 911:
		// The database doesn't exist, or the login may not open it.
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	case 229, 230, 262, 297, 300, 916, 1088, 15151, 15247, 22902:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case 1205, 1222, 701, 8645, 10928, 10929, 40197, 40501, 40613, 49918, 49919, 49920, 233, 10053, 10054, 10060, 64, -2:
		// Deadlock victims, lock timeouts, memory pressure, and Azure's
		// throttling and failovers.
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
