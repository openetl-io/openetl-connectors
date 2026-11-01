package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var (
	_ source.Checker      = (*Source)(nil)
	_ destination.Checker = (*Destination)(nil)
)

// Check connects, and confirms the schema exists and every table in it can be
// read.
func (s *Source) Check(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var usage bool
	err = conn.QueryRow(ctx, `SELECT has_schema_privilege(oid, 'USAGE') FROM pg_namespace WHERE nspname = $1`, s.cfg.Schema).Scan(&usage)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("postgres: schema %q doesn't exist", s.cfg.Schema))
	case err != nil:
		return classify(err)
	case !usage:
		return denied("postgres: no USAGE on schema %q", s.cfg.Schema)
	}
	var unreadable []string
	err = conn.QueryRow(ctx, `SELECT coalesce(array_agg(c.relname ORDER BY c.relname), '{}') FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') AND NOT has_table_privilege(c.oid, 'SELECT')`, s.cfg.Schema).Scan(&unreadable)
	if err != nil {
		return classify(err)
	}
	if len(unreadable) > 0 {
		return denied("postgres: no SELECT on %s in schema %q", strings.Join(unreadable, ", "), s.cfg.Schema)
	}
	return nil
}

// Check connects, and confirms the default schema can take new tables, or
// can be created.
func (d *Destination) Check(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, d.pool.ConnConfig)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var create bool
	err = conn.QueryRow(ctx, `SELECT has_schema_privilege(oid, 'CREATE') FROM pg_namespace WHERE nspname = $1`, d.cfg.Schema).Scan(&create)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if err := conn.QueryRow(ctx, `SELECT has_database_privilege(current_database(), 'CREATE')`).Scan(&create); err != nil {
			return classify(err)
		}
		if !create {
			return denied("postgres: schema %q doesn't exist, and there's no CREATE on the database to create it", d.cfg.Schema)
		}
	case err != nil:
		return classify(err)
	case !create:
		return denied("postgres: no CREATE on schema %q", d.cfg.Schema)
	}
	return nil
}

func denied(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, fmt.Errorf(format, args...))
}

// classify gives the server's refusals their taxonomy codes.
func classify(err error) error {
	pe, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}
	switch {
	case pe.Code == "28P01" || pe.Code == "28000":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case pe.Code == "3D000":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	case pe.Code == "42501":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case strings.HasPrefix(pe.Code, "08") || pe.Code == "53300" || pe.Code == "57P03":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
