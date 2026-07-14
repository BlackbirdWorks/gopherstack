package rdsdata

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	// modernc.org/sqlite registers the pure-Go "sqlite" database/sql driver
	// (no cgo), so the Data API can execute real SQL against an in-memory
	// engine on every platform the rest of gopherstack builds for.
	_ "modernc.org/sqlite"
)

// errNoEngineTx is returned when a statement references a transaction that has
// no live engine-side *sql.Tx (e.g. it was created before this process start).
var errNoEngineTx = errors.New("no engine transaction")

// resourceDB bundles an in-memory database with the keep-alive connection that
// keeps its shared-cache backing store from being reclaimed by the pool.
type resourceDB struct {
	db        *sql.DB
	keepAlive *sql.Conn
}

// sqlEngine backs the RDS Data API with real, per-resource in-memory SQLite
// databases. Each (region, resourceARN) pair maps to its own database so that
// statements issued against different Aurora clusters stay isolated.
type sqlEngine struct {
	dbs   map[string]*resourceDB
	txs   map[string]*sql.Tx
	nonce string
	mu    sync.Mutex
}

// newSQLEngine constructs an empty engine. The nonce (the engine's own pointer
// address) is folded into every database name so that two engine instances
// never alias the same process-global shared-cache in-memory store.
func newSQLEngine() *sqlEngine {
	e := &sqlEngine{
		dbs:   make(map[string]*resourceDB),
		txs:   make(map[string]*sql.Tx),
		nonce: "",
		mu:    sync.Mutex{},
	}
	e.nonce = fmt.Sprintf("%p", e)

	return e
}

// querier is satisfied by both *sql.DB and *sql.Tx.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// dbKey derives a stable, process-unique identifier for a resource database.
func dbKey(nonce, region, resourceARN string) string {
	sum := sha256.Sum256([]byte(nonce + "\x00" + region + "\x00" + resourceARN))

	return hex.EncodeToString(sum[:])
}

// dbFor returns the database for a resource, opening it lazily. The caller must
// hold e.mu.
func (e *sqlEngine) dbFor(ctx context.Context, region, resourceARN string) (*sql.DB, error) {
	key := dbKey(e.nonce, region, resourceARN)
	if rdb, ok := e.dbs[key]; ok {
		return rdb.db, nil
	}

	// A shared-cache, in-memory database persists only while at least one
	// connection stays open; the keep-alive connection guarantees that for the
	// lifetime of the engine while still letting the pool open more.
	dsn := "file:" + key + "?mode=memory&cache=shared"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open resource db: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("pin resource db: %w", err)
	}

	e.dbs[key] = &resourceDB{db: db, keepAlive: conn}

	return db, nil
}

// execute runs a single SQL statement against a resource database, or against
// an open transaction when transactionID is set, and returns the result set.
func (e *sqlEngine) execute(
	ctx context.Context,
	region, resourceARN, statement, transactionID string,
	params []SQLParameter,
) ([][]Field, []ColumnMetadata, int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var run querier

	if transactionID != "" {
		tx, ok := e.txs[transactionID]
		if !ok {
			return nil, nil, 0, errNoEngineTx
		}

		run = tx
	} else {
		db, err := e.dbFor(ctx, region, resourceARN)
		if err != nil {
			return nil, nil, 0, err
		}

		run = db
	}

	return runStatement(ctx, run, statement, params)
}

// beginTx opens an engine-side transaction bound to txID. The caller must have
// already validated/allocated txID. Errors are advisory; a missing engine tx
// degrades to autocommit execution.
func (e *sqlEngine) beginTx(ctx context.Context, region, resourceARN, txID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	db, err := e.dbFor(ctx, region, resourceARN)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	e.txs[txID] = tx

	return nil
}

// finalizeTx commits or rolls back the engine transaction for txID, if any.
func (e *sqlEngine) finalizeTx(txID string, commit bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, ok := e.txs[txID]
	if !ok {
		return
	}

	delete(e.txs, txID)

	if commit {
		_ = tx.Commit()

		return
	}

	_ = tx.Rollback()
}

// reset closes every open database and transaction.
func (e *sqlEngine) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for id, tx := range e.txs {
		_ = tx.Rollback()
		delete(e.txs, id)
	}

	for key, rdb := range e.dbs {
		_ = rdb.keepAlive.Close()
		_ = rdb.db.Close()
		delete(e.dbs, key)
	}
}

// replay best-effort re-applies a sequence of recorded statements to rebuild
// table state after a snapshot restore. Read-only and failing statements are
// ignored so a partial log never aborts the restore.
func (e *sqlEngine) replay(ctx context.Context, region string, stmts []ExecutedStatement) {
	for _, st := range stmts {
		if isQuery(st.SQL) {
			continue
		}

		_, _, _, _ = e.execute(ctx, region, st.ResourceARN, st.SQL, "", nil)
	}
}

// runStatement dispatches to the query or exec path based on the leading
// keyword and shapes the driver result into the Data API record model.
func runStatement(
	ctx context.Context,
	run querier,
	statement string,
	params []SQLParameter,
) ([][]Field, []ColumnMetadata, int64, error) {
	args := namedArgs(params)

	if isQuery(statement) {
		rows, err := run.QueryContext(ctx, statement, args...)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("query: %w", err)
		}
		defer func() { _ = rows.Close() }()

		records, columns, scanErr := scanRows(rows)
		if scanErr != nil {
			return nil, nil, 0, scanErr
		}

		return records, columns, 0, nil
	}

	res, err := run.ExecContext(ctx, statement, args...)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("exec: %w", err)
	}

	updated, _ := res.RowsAffected()

	return [][]Field{}, []ColumnMetadata{}, updated, nil
}

// queryLeadKeywords are the statement prefixes that produce a result set.
//
//nolint:gochecknoglobals // immutable lookup set
var queryLeadKeywords = map[string]struct{}{
	"SELECT": {}, "WITH": {}, "VALUES": {}, "PRAGMA": {}, "EXPLAIN": {},
}

// isQuery reports whether a statement returns rows rather than an update count.
func isQuery(statement string) bool {
	trimmed := strings.TrimLeft(statement, " \t\r\n(")

	end := strings.IndexAny(trimmed, " \t\r\n(")
	if end < 0 {
		end = len(trimmed)
	}

	_, ok := queryLeadKeywords[strings.ToUpper(trimmed[:end])]

	return ok
}

// namedArgs converts Data API SQL parameters into database/sql named arguments.
func namedArgs(params []SQLParameter) []any {
	args := make([]any, 0, len(params))
	for _, p := range params {
		args = append(args, sql.Named(p.Name, fieldToValue(p.Value)))
	}

	return args
}

// fieldToValue unwraps a Data API Field into a driver-compatible Go value.
func fieldToValue(f Field) any {
	switch {
	case f.IsNull != nil && *f.IsNull:
		return nil
	case f.StringValue != nil:
		return *f.StringValue
	case f.LongValue != nil:
		return *f.LongValue
	case f.DoubleValue != nil:
		return *f.DoubleValue
	case f.BooleanValue != nil:
		return *f.BooleanValue
	case f.BlobValue != nil:
		return f.BlobValue
	default:
		return nil
	}
}

// JDBC-style type codes (java.sql.Types) reported in ColumnMetadata.Type,
// chosen per the SQLite type affinity a column resolves to (see
// sqliteAffinity).
const (
	jdbcTypeDecimal = 3
	jdbcTypeInteger = 4
	jdbcTypeDouble  = 8
	jdbcTypeVarchar = 12
	jdbcTypeBlob    = 2004
)

// AWS ColumnMetadata.Nullable codes: 0 = no nulls, 1 = nullable,
// 2 = nullability unknown.
const (
	columnNoNulls         = 0
	columnNullable        = 1
	columnNullableUnknown = 2
)

// sqliteAffinity classifies a declared column type name into one of
// SQLite's five type affinities, applying the determination rules in the
// order documented at https://sqlite.org/datatype3.html#type_affinity
// section 3.1 (first match wins). A column with no declared type (e.g. the
// result of a literal SELECT expression) resolves to BLOB affinity per rule 3.
func sqliteAffinity(decltype string) string {
	switch {
	case strings.Contains(decltype, "INT"):
		return "INTEGER"
	case strings.Contains(decltype, "CHAR"), strings.Contains(decltype, "CLOB"), strings.Contains(decltype, "TEXT"):
		return "TEXT"
	case strings.Contains(decltype, "BLOB"), decltype == "":
		return "BLOB"
	case strings.Contains(decltype, "REAL"), strings.Contains(decltype, "FLOA"), strings.Contains(decltype, "DOUB"):
		return "REAL"
	default:
		return "NUMERIC"
	}
}

// columnMetadataFor builds a ColumnMetadata for one result column. The
// pure-Go SQLite driver exposes only the declared type name, nullability (it
// always reports "nullable, known" -- see modernc.org/sqlite's
// rows.ColumnTypeNullable), and decimal size (never known); schemaName,
// tableName, isAutoIncrement, and arrayBaseColumnType have no equivalent in
// database/sql's ColumnType and are left at their zero values.
func columnMetadataFor(ct *sql.ColumnType) ColumnMetadata {
	decltype := ct.DatabaseTypeName()

	nullableCode := int32(columnNullableUnknown)
	if nullable, ok := ct.Nullable(); ok {
		if nullable {
			nullableCode = columnNullable
		} else {
			nullableCode = columnNoNulls
		}
	}

	precision, scale, hasPrecision := ct.DecimalSize()
	if !hasPrecision {
		precision, scale = 0, 0
	}

	meta := ColumnMetadata{
		Name:      ct.Name(),
		Label:     ct.Name(),
		TypeName:  decltype,
		Nullable:  nullableCode,
		Precision: int32(precision),
		Scale:     int32(scale),
	}

	switch sqliteAffinity(decltype) {
	case "INTEGER":
		meta.Type = jdbcTypeInteger
		meta.IsSigned = true
	case "TEXT":
		meta.Type = jdbcTypeVarchar
		meta.IsCaseSensitive = true
	case "REAL":
		meta.Type = jdbcTypeDouble
		meta.IsSigned = true
	case "NUMERIC":
		meta.Type = jdbcTypeDecimal
		meta.IsSigned = true
	default: // BLOB, or no declared type
		meta.Type = jdbcTypeBlob
	}

	return meta
}

// scanRows materialises an *sql.Rows cursor into the Data API record model.
func scanRows(rows *sql.Rows) ([][]Field, []ColumnMetadata, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("column types: %w", err)
	}

	columns := make([]ColumnMetadata, len(cols))
	for i, ct := range cols {
		columns[i] = columnMetadataFor(ct)
	}

	records := [][]Field{}

	for rows.Next() {
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))

		for i := range values {
			pointers[i] = &values[i]
		}

		if scanErr := rows.Scan(pointers...); scanErr != nil {
			return nil, nil, fmt.Errorf("scan row: %w", scanErr)
		}

		record := make([]Field, len(cols))
		for i, v := range values {
			record[i] = fieldFromValue(v)
		}

		records = append(records, record)
	}

	if iterErr := rows.Err(); iterErr != nil {
		return nil, nil, fmt.Errorf("iterate rows: %w", iterErr)
	}

	return records, columns, nil
}

// fieldFromValue maps a scanned driver value into a Data API Field.
func fieldFromValue(v any) Field {
	isNull := true

	switch typed := v.(type) {
	case nil:
		return Field{IsNull: &isNull}
	case int64:
		return Field{LongValue: &typed}
	case float64:
		return Field{DoubleValue: &typed}
	case bool:
		return Field{BooleanValue: &typed}
	case string:
		return Field{StringValue: &typed}
	case []byte:
		return Field{BlobValue: typed}
	default:
		s := fmt.Sprintf("%v", typed)

		return Field{StringValue: &s}
	}
}
