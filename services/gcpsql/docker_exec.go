package gcpsql

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var errResultTooLarge = errors.New("the SQL result exceeds the 10 MB response limit")

// ExecuteSQL implements SQLExecutor over the simple query protocol, so a batch of statements yields one result each.
func (e *dockerEngine) ExecuteSQL(ctx context.Context, req ExecRequest) (ExecOutcome, error) {
	h, p := e.Endpoint()

	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s sslmode=disable connect_timeout=5",
		h, p, req.Database))
	if err != nil {
		return ExecOutcome{}, fmt.Errorf("pg config: %w", err)
	}

	cfg.User, cfg.Password = cmp.Or(req.User, pgAdminUser), req.Password

	var notices []ExecMessage

	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		notices = append(notices, ExecMessage{Severity: n.Severity, Message: n.Severity + ": " + n.Message})
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return dbOutcome(err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	results, err := readPGResults(conn.PgConn().Exec(ctx, req.SQL), req)
	if err != nil {
		return dbOutcome(err)
	}

	return ExecOutcome{Results: results, Messages: notices}, nil
}

// dbOutcome reports a database error as the outcome; anything else is a transport failure.
func dbOutcome(err error) (ExecOutcome, error) {
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok {
		return ExecOutcome{Failure: fmt.Sprintf("%s: %s (SQLSTATE %s)", pe.Severity, pe.Message, pe.Code)}, nil
	}

	if errors.Is(err, errResultTooLarge) || errors.Is(err, context.DeadlineExceeded) {
		return ExecOutcome{Failure: err.Error()}, nil
	}

	return ExecOutcome{}, fmt.Errorf("sql: %w", err)
}

func readPGResults(mrr *pgconn.MultiResultReader, req ExecRequest) ([]ExecResult, error) {
	types := pgtype.NewMap()
	budget := req.MaxBytes

	var out []ExecResult

	for mrr.NextResult() {
		res, err := readPGResult(mrr.ResultReader(), types, req, &budget)
		if err != nil {
			_ = mrr.Close()

			return nil, err
		}

		out = append(out, res)
	}

	if err := mrr.Close(); err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}

	return out, nil
}

func readPGResult(rr *pgconn.ResultReader, types *pgtype.Map, req ExecRequest, budget *int) (ExecResult, error) {
	var res ExecResult

	for _, fd := range rr.FieldDescriptions() {
		res.Columns = append(res.Columns, ExecColumn{Name: fd.Name, Type: pgTypeName(types, fd.DataTypeOID)})
	}

	for rr.NextRow() {
		if req.RowLimit > 0 && int64(len(res.Rows)) >= req.RowLimit {
			continue
		}

		row := ExecRow{Values: make([]ExecValue, 0, len(res.Columns))}

		for _, v := range rr.Values() {
			*budget -= len(v) + 1
			if v == nil {
				row.Values = append(row.Values, ExecValue{NullValue: true})
			} else {
				row.Values = append(row.Values, ExecValue{Value: string(v)})
			}
		}

		if *budget < 0 {
			if !req.Partial {
				_, _ = rr.Close()

				return ExecResult{}, errResultTooLarge
			}

			res.PartialResult = true

			break
		}

		res.Rows = append(res.Rows, row)
	}

	tag, err := rr.Close()
	if err != nil {
		return ExecResult{}, err //nolint:wrapcheck // carried to dbOutcome
	}

	if len(res.Columns) == 0 {
		res.Message = tag.String()
	}

	return res, nil
}

func pgTypeName(m *pgtype.Map, oid uint32) string {
	if t, ok := m.TypeForOID(oid); ok {
		return strings.ToUpper(t.Name)
	}

	return "UNKNOWN"
}
