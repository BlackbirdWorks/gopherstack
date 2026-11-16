package gcpsql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// ExecuteSQL implements SQLExecutor; statements run one by one on one connection so each reports its own result.
func (e *mysqlEngine) ExecuteSQL(ctx context.Context, req ExecRequest) (ExecOutcome, error) {
	h, p := e.srv.Endpoint()
	user := cmp.Or(req.User, mysqlAdminUser)

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = user, req.Password, "tcp"
	cfg.Addr, cfg.DBName, cfg.Timeout = net.JoinHostPort(h, strconv.Itoa(p)), req.Database, connectTimeout
	cfg.InterpolateParams = true

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return ExecOutcome{}, fmt.Errorf("mysql config: %w", err)
	}

	pool := sql.OpenDB(connector)
	defer pool.Close()

	conn, err := pool.Conn(ctx)
	if err != nil {
		return mysqlOutcome(err)
	}
	defer conn.Close()

	var out ExecOutcome

	budget := req.MaxBytes

	for _, stmt := range splitStatements(req.SQL) {
		res, rerr := runMySQLStatement(ctx, conn, stmt, req, &budget)
		if rerr != nil {
			return mysqlOutcome(rerr)
		}

		out.Results = append(out.Results, res)
	}

	out.Messages = mysqlWarnings(ctx, conn)

	return out, nil
}

func mysqlOutcome(err error) (ExecOutcome, error) {
	if me, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return ExecOutcome{Failure: fmt.Sprintf("ERROR %d (%s): %s", me.Number, me.SQLState, me.Message)}, nil
	}

	if errors.Is(err, errResultTooLarge) || errors.Is(err, context.DeadlineExceeded) {
		return ExecOutcome{Failure: err.Error()}, nil
	}

	return ExecOutcome{}, fmt.Errorf("sql: %w", err)
}

func runMySQLStatement(
	ctx context.Context, conn *sql.Conn, stmt string, req ExecRequest, budget *int,
) (ExecResult, error) {
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return ExecResult{}, err //nolint:wrapcheck // carried to mysqlOutcome
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return ExecResult{}, err //nolint:wrapcheck // carried to mysqlOutcome
	}

	var res ExecResult

	for _, t := range types {
		res.Columns = append(res.Columns, ExecColumn{Name: t.Name(), Type: t.DatabaseTypeName()})
	}

	if len(res.Columns) == 0 {
		_ = rows.Close()

		return affectedResult(ctx, conn, res)
	}

	if err = scanMySQLRows(rows, &res, req, budget); err != nil {
		return ExecResult{}, err
	}

	return res, rows.Err() //nolint:wrapcheck // carried to mysqlOutcome
}

func scanMySQLRows(rows *sql.Rows, res *ExecResult, req ExecRequest, budget *int) error {
	width := len(res.Columns)

	for rows.Next() {
		vals := make([]sql.NullString, width)
		ptrs := make([]any, width)

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return err //nolint:wrapcheck // carried to mysqlOutcome
		}

		if req.RowLimit > 0 && int64(len(res.Rows)) >= req.RowLimit {
			continue
		}

		row := ExecRow{Values: make([]ExecValue, width)}

		for i, v := range vals {
			*budget -= len(v.String) + 1
			row.Values[i] = ExecValue{Value: v.String, NullValue: !v.Valid}
		}

		if *budget < 0 {
			if !req.Partial {
				return errResultTooLarge
			}

			res.PartialResult = true

			return nil
		}

		res.Rows = append(res.Rows, row)
	}

	return nil
}

func affectedResult(ctx context.Context, conn *sql.Conn, res ExecResult) (ExecResult, error) {
	var n int64

	if err := conn.QueryRowContext(ctx, "SELECT ROW_COUNT()").Scan(&n); err != nil {
		return ExecResult{}, err //nolint:wrapcheck // carried to mysqlOutcome
	}

	res.Message = "Query OK, " + strconv.FormatInt(max(n, 0), 10) + " rows affected"

	return res, nil
}

// mysqlWarnings returns the warnings of the last statement, which is what MySQL keeps.
func mysqlWarnings(ctx context.Context, conn *sql.Conn) []ExecMessage {
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []ExecMessage

	for rows.Next() {
		var level, msg string

		var code int

		if rows.Scan(&level, &code, &msg) != nil {
			break
		}

		out = append(out, ExecMessage{Severity: strings.ToUpper(level), Message: msg})
	}

	if rows.Err() != nil {
		return nil
	}

	return out
}

// splitStatements cuts sqlText at top-level semicolons, honouring quotes, backticks and comments.
func splitStatements(sqlText string) []string {
	var out []string

	start := 0

	for i := 0; i < len(sqlText); i++ {
		i = skipLiteral(sqlText, i)
		if i < len(sqlText) && sqlText[i] == ';' {
			out = appendStatement(out, sqlText[start:i])
			start = i + 1
		}
	}

	return appendStatement(out, sqlText[start:])
}

func appendStatement(out []string, s string) []string {
	if s = strings.TrimSpace(s); s != "" && !commentOnly(s) {
		return append(out, s)
	}

	return out
}

// skipLiteral returns the index of the first byte at or after i that is outside a quoted string, backtick name or
// comment.
func skipLiteral(s string, i int) int {
	for i < len(s) {
		c := s[i]

		switch {
		case c == '\'' || c == '"' || c == '`':
			i = closeQuote(s, i)
		case c == '#' || (c == '-' && strings.HasPrefix(s[i:], "-- ")):
			i = skipTo(s, i, "\n") + 1
		case c == '/' && strings.HasPrefix(s[i:], "/*"):
			i = skipTo(s, i+len("/*"), "*/") + 1
		default:
			return i
		}
	}

	return i
}

// closeQuote returns the index after the quote that closes the one opened at s[i].
func closeQuote(s string, i int) int {
	q := s[i]

	for i++; i < len(s); i++ {
		switch {
		case s[i] == '\\' && q != '`':
			i++
		case s[i] == q:
			return i + 1
		}
	}

	return len(s)
}

// skipTo returns the index of the last byte of the first sep at or after from, or the end of s.
func skipTo(s string, from int, sep string) int {
	if i := strings.Index(s[from:], sep); i >= 0 {
		return from + i + len(sep) - 1
	}

	return len(s) - 1
}

func commentOnly(s string) bool {
	for s != "" {
		s = strings.TrimSpace(s)

		switch {
		case strings.HasPrefix(s, "#") || strings.HasPrefix(s, "-- "):
			_, rest, found := strings.Cut(s, "\n")
			if !found {
				return true
			}

			s = rest
		case strings.HasPrefix(s, "/*"):
			_, rest, found := strings.Cut(s, "*/")
			if !found {
				return true
			}

			s = rest
		default:
			return false
		}
	}

	return true
}
