package gcpsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const maxQueryCells = 20_000_000

var errTooLarge = errors.New("result is too large")

// Query implements Engine; the first returned row holds the column names.
func (e *dockerEngine) Query(ctx context.Context, db, query string) ([][]string, error) {
	conn, err := e.open(db)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}

	out := [][]string{cols}

	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err = rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("sql: %w", err)
		}

		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = v.String
		}

		if out = append(out, row); len(out)*len(cols) > maxQueryCells {
			return nil, errTooLarge
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}

	return out, nil
}

func (e *dockerEngine) quoteTable(table string) string {
	parts := strings.Split(table, ".")

	if e.kind == kindPostgres {
		return pgx.Identifier(parts).Sanitize()
	}

	for i, p := range parts {
		parts[i] = myIdent(p)
	}

	return strings.Join(parts, ".")
}

func (e *dockerEngine) quoteColumn(c string) string {
	if e.kind == kindPostgres {
		return pgx.Identifier{c}.Sanitize()
	}

	return myIdent(c)
}

// InsertRows implements Engine.
func (e *dockerEngine) InsertRows(ctx context.Context, db, table string, columns []string, rows [][]string) error {
	conn, err := e.open(db)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sql: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt := e.insertStmt(table, columns, len(rows[0]))

	for _, r := range rows {
		args := make([]any, len(r))
		for i, v := range r {
			args[i] = v
		}

		if _, err = tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("sql: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

func (e *dockerEngine) insertStmt(table string, columns []string, width int) string {
	var cols, marks []string

	for i := range width {
		if i < len(columns) {
			cols = append(cols, e.quoteColumn(columns[i]))
		}

		if e.kind == kindPostgres {
			marks = append(marks, "$"+strconv.Itoa(i+1))
		} else {
			marks = append(marks, "?")
		}
	}

	colList := ""
	if len(cols) > 0 {
		colList = " (" + strings.Join(cols, ", ") + ")"
	}

	return "INSERT INTO " + e.quoteTable(table) + colList + " VALUES (" + strings.Join(marks, ", ") + ")"
}
