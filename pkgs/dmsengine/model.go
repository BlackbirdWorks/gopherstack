// Package dmsengine moves rows between databases and in-process stores for the DMS emulator: table-mapping
// evaluation, full load, and change data capture from MySQL binlogs and PostgreSQL logical decoding.
package dmsengine

import (
	"context"
	"strings"
	"time"
)

// Kind is the value class a column's text form belongs to.
type Kind int

// Column value classes.
const (
	KindString Kind = iota
	KindInt
	KindDecimal
	KindFloat
	KindBool
	KindTime
	KindBytes
	KindJSON
)

// Family groups engine names that share a SQL dialect.
type Family int

// Engine families.
const (
	FamilyOther Family = iota
	FamilyMySQL
	FamilyPostgres
	FamilyS3
	FamilyDynamoDB
	FamilyKinesis
)

// FamilyOf maps a DMS engine name to its family.
func FamilyOf(engine string) Family {
	switch strings.ToLower(engine) {
	case "mysql", "mariadb", "aurora", "aurora-mysql":
		return FamilyMySQL
	case "postgres", "aurora-postgresql":
		return FamilyPostgres
	case "s3":
		return FamilyS3
	case "dynamodb":
		return FamilyDynamoDB
	case "kinesis":
		return FamilyKinesis
	default:
		return FamilyOther
	}
}

// Column describes one source column. Native is the engine's own type text (e.g. "varchar(20)").
type Column struct {
	Name     string
	Native   string
	Kind     Kind
	Nullable bool
	// DMS is set for columns whose type comes from a DMS data-type (S3 sources, add-column, change-data-type).
	DMS DMSType
}

// Table is a source table; PK lists primary-key column names in key order.
type Table struct {
	Schema  string
	Name    string
	Columns []Column
	PK      []string
}

// Key is "schema.table".
func (t Table) Key() string { return t.Schema + "." + t.Name }

// Row holds a column's text form: nil (NULL), string, or []byte for binary columns. Values line up with a
// table's Columns.
type Row []any

// Op is a change operation.
type Op string

// Change operations.
const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// Change is one captured row change; Before is set for updates and deletes when the source logs it.
type Change struct {
	Time     time.Time
	Schema   string
	Table    string
	Op       Op
	Position string
	Before   Row
	After    Row
	Commit   bool
}

// StreamStart says where change capture begins.
type StreamStart struct {
	Time     time.Time
	Position string
}

// Source reads tables and change streams.
type Source interface {
	Ping(ctx context.Context) error
	Tables(ctx context.Context) ([]Table, error)
	Scan(ctx context.Context, t Table, fn func(Row) error) error
	// Position captures the current change position; for PostgreSQL it also creates the replication slot.
	Position(ctx context.Context) (string, error)
	// Stream delivers changes from start until ctx is done or fn fails. Rows are in the table's column order.
	Stream(
		ctx context.Context,
		start StreamStart,
		tables func(schema, table string) (Table, bool),
		fn func(Change) error,
	) error
	// Release drops source-side resources such as replication slots.
	Release(ctx context.Context) error
	Close() error
}

// PrepMode is FullLoadSettings.TargetTablePrepMode.
type PrepMode string

// Target table preparation modes.
const (
	PrepDropAndCreate  PrepMode = "DROP_AND_CREATE"
	PrepTruncateBefore PrepMode = "TRUNCATE_BEFORE_LOAD"
	PrepDoNothing      PrepMode = "DO_NOTHING"
)

// TargetColumn is a column on the target; Source indexes the originating source column.
type TargetColumn struct {
	Expr *Expr
	Column
	Source int
	// Computed marks an add-column column evaluated from Expr; Before marks a before-image column.
	Computed bool
	Before   bool
}

// TargetTable is a source table after mapping rules.
type TargetTable struct {
	Schema       string
	Name         string
	SourceSchema string
	SourceName   string
	Columns      []TargetColumn
	PK           []string
	KeyName      string
	SourceFamily Family
	KeyUnique    bool
	// HasBeforeImage is set when add-before-image-columns added columns.
	HasBeforeImage bool
}

// Key is "schema.table" on the target.
func (t TargetTable) Key() string { return t.Schema + "." + t.Name }

// ProjectCtx carries what computed and before-image columns need to fill in one row.
type ProjectCtx struct {
	Time     time.Time
	Now      time.Time
	Before   Row
	Op       Op
	Position string
	Full     bool
}

// Project selects and orders a full-load source row's values for the target.
func (t TargetTable) Project(r Row) Row {
	return t.ProjectWith(r, ProjectCtx{Full: true, Now: time.Now()})
}

// ProjectWith is Project for a change row: r is the row image, pc.Before feeds the before-image columns.
func (t TargetTable) ProjectWith(r Row, pc ProjectCtx) Row {
	if r == nil {
		return nil
	}

	ec := &EvalCtx{Row: r, Op: pc.Op, Time: pc.Time, Now: pc.Now, Position: pc.Position, Full: pc.Full}
	out := make(Row, len(t.Columns))

	for i, c := range t.Columns {
		switch {
		case c.Computed:
			if c.Expr != nil {
				out[i] = c.DMS.Cast(c.Expr.Eval(ec))
			}
		case c.Before:
			if pc.Before != nil && c.Source < len(pc.Before) && !pc.Full {
				out[i] = pc.Before[c.Source]
			}
		case c.Source < len(r):
			out[i] = r[c.Source]
		}
	}

	return out
}

// Target receives rows and changes.
type Target interface {
	Ping(ctx context.Context) error
	Prepare(ctx context.Context, t TargetTable, mode PrepMode) error
	Load(ctx context.Context, t TargetTable, rows []Row) error
	EndLoad(ctx context.Context, t TargetTable) error
	Apply(ctx context.Context, t TargetTable, c Change) error
	Commit(ctx context.Context) error
	Close() error
}
