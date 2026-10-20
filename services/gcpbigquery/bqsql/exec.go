package bqsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Effect is a catalog change a statement asks the caller to apply.
type Effect struct {
	Options    map[string]string
	Ref        TableRef
	Op         string
	ViewQuery  string
	AlterOp    string
	Fields     []Field
	Rows       [][]any
	OrReplace  bool
	IfNotExist bool
	IfExists   bool
	Cascade    bool
}

// Effect operations.
const (
	OpWrite        = "WRITE"
	OpCreateTable  = "CREATE_TABLE"
	OpCreateView   = "CREATE_VIEW"
	OpCreateSchema = "CREATE_SCHEMA"
	OpDropTable    = "DROP_TABLE"
	OpDropView     = "DROP_VIEW"
	OpDropSchema   = "DROP_SCHEMA"
	OpAlter        = "ALTER"
)

// Result is the outcome of one statement.
type Result struct {
	Statement string
	Fields    []Field
	Rows      [][]any
	Effects   []Effect
	Affected  int64
	Inserted  int64
	Updated   int64
	Deleted   int64
	Kind      StmtKind
}

// Run executes st. With dryRun set, queries are only analysed and the result carries the schema alone.
func Run(ctx context.Context, st *Statement, in *Input, dryRun bool) (*Result, error) {
	c := newCompiler(in)

	switch st.Kind {
	case StmtQuery:
		return c.runQuery(ctx, st, dryRun)
	case StmtInsert, StmtUpdate, StmtDelete, StmtMerge, StmtTruncate:
		return c.runDML(ctx, st, dryRun)
	case StmtCreateTable, StmtCreateView, StmtCreateSchema, StmtDrop, StmtAlter:
		return c.runDDL(ctx, st, dryRun)
	}

	return nil, unsupported("statement")
}

func (c *compiler) runQuery(ctx context.Context, st *Statement, dryRun bool) (*Result, error) {
	cq, err := c.compileQuery(st.query, nil)
	if err != nil {
		return nil, err
	}

	res := &Result{Kind: StmtQuery, Statement: litSelect, Fields: ColumnFields(cq.cols)}
	if dryRun {
		return res, nil
	}

	db, err := openDB(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err = c.materialize(ctx, db); err != nil {
		return nil, err
	}

	res.Rows, err = queryRows(ctx, db, cq.sql, res.Fields)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func openDB(ctx context.Context) (*sql.DB, error) {
	registerUDFs()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open query engine: %w", err)
	}

	db.SetMaxOpenConns(1)

	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("open query engine: %w", err)
	}

	return db, nil
}

func affinity(t Type) string {
	switch t.Kind {
	case KindInt, KindBool:
		return litInteger
	case KindFloat, KindNumeric, KindBigNumeric:
		return "REAL"
	case KindBytes:
		return "BLOB"
	case KindUnknown, KindString, KindTimestamp, KindDate, KindTime, KindDatetime, KindJSON, KindGeography,
		KindStruct, KindArray:
		return "TEXT"
	}

	return "TEXT"
}

func (c *compiler) materialize(ctx context.Context, db *sql.DB) error {
	for _, lt := range c.loads {
		if err := loadTable(ctx, db, lt); err != nil {
			return err
		}
	}

	return nil
}

func loadTable(ctx context.Context, db *sql.DB, lt *loadedTable) error {
	cols := make([]string, len(lt.td.Fields))
	marks := make([]string, len(lt.td.Fields))

	for i, f := range lt.td.Fields {
		cols[i] = quoteIdent(f.Name) + " " + affinity(f.SQLType())
		marks[i] = "?"
	}

	if _, err := db.ExecContext(
		ctx,
		"CREATE TABLE "+quoteIdent(lt.name)+" ("+strings.Join(cols, ", ")+")",
	); err != nil {
		return mapSQLiteErr(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return mapSQLiteErr(err)
	}

	//nolint:gosec // identifiers are quoted and values are bound
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+quoteIdent(lt.name)+" VALUES ("+strings.Join(marks, ", ")+")")
	if err != nil {
		_ = tx.Rollback()

		return mapSQLiteErr(err)
	}

	defer stmt.Close()

	args := make([]any, len(lt.td.Fields))

	for _, row := range lt.td.Rows {
		for i, f := range lt.td.Fields {
			var v any
			if i < len(row) {
				v = row[i]
			}

			args[i] = ToSQLite(f, v)
		}

		if _, err = stmt.ExecContext(ctx, args...); err != nil {
			_ = tx.Rollback()

			return mapSQLiteErr(err)
		}
	}

	return mapSQLiteErr(tx.Commit())
}

func queryRows(ctx context.Context, db *sql.DB, query string, fields []Field) ([][]any, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, mapSQLiteErr(err)
	}
	defer rows.Close()

	var out [][]any

	for rows.Next() {
		raw := make([]any, len(fields))
		ptrs := make([]any, len(fields))

		for i := range raw {
			ptrs[i] = &raw[i]
		}

		if err = rows.Scan(ptrs...); err != nil {
			return nil, mapSQLiteErr(err)
		}

		row := make([]any, len(fields))

		for i, f := range fields {
			if row[i], err = FromSQLite(f, raw[i]); err != nil {
				return nil, err
			}
		}

		out = append(out, row)
	}

	return out, mapSQLiteErr(rows.Err())
}

func mapSQLiteErr(err error) error {
	if err == nil {
		return nil
	}

	msg := err.Error()

	if _, rest, found := strings.Cut(msg, "bq: "); found {
		return &Error{Kind: ErrRuntime, Message: strings.TrimSpace(rest)}
	}

	if strings.Contains(msg, "context canceled") || strings.Contains(msg, "interrupted") {
		return &Error{Kind: ErrRuntime, Message: "Query was cancelled"}
	}

	return &Error{Kind: ErrInvalid, Message: "Query could not be executed: " + msg}
}
