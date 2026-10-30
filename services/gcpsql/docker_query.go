package gcpsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

func (e *dockerEngine) quoteTable(ctx context.Context, q rowQuerier, table string) (string, error) {
	return e.quoteDotted(ctx, q, table)
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

	stmt, err := e.insertStmt(ctx, conn, table, columns, len(rows[0]))
	if err != nil {
		return err
	}

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

func (e *dockerEngine) insertStmt(
	ctx context.Context, q rowQuerier, table string, columns []string, width int,
) (string, error) {
	var cols, marks []string

	for i := range width {
		if i < len(columns) {
			col, err := e.quoteIdent(ctx, q, columns[i])
			if err != nil {
				return "", err
			}

			cols = append(cols, col)
		}

		marks = append(marks, "$"+strconv.Itoa(i+1))
	}

	colList := ""
	if len(cols) > 0 {
		colList = " (" + strings.Join(cols, ", ") + ")"
	}

	tbl, err := e.quoteTable(ctx, q, table)
	if err != nil {
		return "", err
	}

	return "INSERT INTO " + tbl + colList + " VALUES (" + strings.Join(marks, ", ") + ")", nil
}
