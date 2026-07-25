package rdsdata

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
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
// The result-set shaping (resultSetOptions) is read from ctx -- see
// resultSetOptionsContextKey in store.go.
func (e *sqlEngine) execute(
	ctx context.Context,
	region, resourceARN, statement, transactionID string,
	params []SQLParameter,
) ([][]Field, []ColumnMetadata, int64, []Field, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var run querier

	if transactionID != "" {
		tx, ok := e.txs[transactionID]
		if !ok {
			return nil, nil, 0, nil, errNoEngineTx
		}

		run = tx
	} else {
		db, err := e.dbFor(ctx, region, resourceARN)
		if err != nil {
			return nil, nil, 0, nil, err
		}

		run = db
	}

	return runStatement(ctx, run, statement, params, getResultSetOptions(ctx))
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

		_, _, _, _, _ = e.execute(ctx, region, st.ResourceARN, st.SQL, "", nil)
	}
}

// runStatement dispatches to the query or exec path based on the leading
// keyword and shapes the driver result into the Data API record model.
// opts controls how numeric result columns are shaped (real AWS
// ExecuteStatementInput.ResultSetOptions); it is ignored on the exec path,
// which never produces a result set.
func runStatement(
	ctx context.Context,
	run querier,
	statement string,
	params []SQLParameter,
	opts resultSetOptions,
) ([][]Field, []ColumnMetadata, int64, []Field, error) {
	args := namedArgs(params)

	if isQuery(statement) {
		rows, err := run.QueryContext(ctx, statement, args...)
		if err != nil {
			return nil, nil, 0, nil, fmt.Errorf("query: %w", err)
		}
		defer func() { _ = rows.Close() }()

		records, columns, scanErr := scanRows(rows, opts)
		if scanErr != nil {
			return nil, nil, 0, nil, scanErr
		}

		return records, columns, 0, nil, nil
	}

	res, err := run.ExecContext(ctx, statement, args...)
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("exec: %w", err)
	}

	updated, _ := res.RowsAffected()
	generated := generatedFieldsFor(ctx, run, statement, res)

	return [][]Field{}, []ColumnMetadata{}, updated, generated, nil
}

// insertIntoTableRe extracts the target table name from a simple, unquoted
// "INSERT [OR <resolution>] INTO <table>" statement. Statements that quote or
// bracket-escape the table identifier don't match; generatedFieldsFor
// degrades safely to no generated fields in that case rather than risking an
// injected identifier.
var insertIntoTableRe = regexp.MustCompile(`(?is)^\s*INSERT\s+(?:OR\s+\w+\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)`)

// generatedFieldsFor returns the GeneratedFields for a just-executed
// non-query statement. Real AWS populates this with the value assigned to an
// auto-increment/serial column by an INSERT (and documents that it isn't
// supported by Aurora PostgreSQL at all -- see UpdateResult in models.go).
// This mock recognizes the SQLite equivalent: a target table with exactly
// one INTEGER PRIMARY KEY column, which SQLite documents as a rowid alias
// (https://sqlite.org/lang_createtable.html#rowid), and surfaces
// res.LastInsertId() for it. Every other shape -- UPDATE/DELETE/DDL, or an
// INSERT into a table with no such column -- returns an empty slice.
func generatedFieldsFor(ctx context.Context, run querier, statement string, res sql.Result) []Field {
	m := insertIntoTableRe.FindStringSubmatch(statement)
	if m == nil {
		return []Field{}
	}

	if !hasRowIDAliasColumn(ctx, run, m[1]) {
		return []Field{}
	}

	id, err := res.LastInsertId()
	if err != nil || id == 0 {
		return []Field{}
	}

	return []Field{{LongValue: &id}}
}

// hasRowIDAliasColumn reports whether table declares exactly one INTEGER
// PRIMARY KEY column (a composite primary key, or a primary key of any other
// declared type, does not create a rowid alias per SQLite's documented
// rules). table is only ever a regexp-validated bare identifier (see
// insertIntoTableRe), so it is safe to interpolate directly into the PRAGMA
// statement -- database/sql has no bind-parameter support for PRAGMA targets.
func hasRowIDAliasColumn(ctx context.Context, run querier, table string) bool {
	rows, err := run.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return false
	}
	defer func() { _ = rows.Close() }()

	pkCount := 0
	isIntegerPK := false

	for rows.Next() {
		var cid, notnull, pk int

		var name, ctype string

		var dflt any

		if scanErr := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); scanErr != nil {
			return false
		}

		if pk > 0 {
			pkCount++
			isIntegerPK = strings.Contains(strings.ToUpper(ctype), "INT")
		}
	}

	if rows.Err() != nil {
		return false
	}

	return pkCount == 1 && isIntegerPK
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

// scanRows materialises an *sql.Rows cursor into the Data API record model,
// applying opts (real AWS ExecuteStatementInput.ResultSetOptions) to shape
// each column's values -- see shapeField.
func scanRows(rows *sql.Rows, opts resultSetOptions) ([][]Field, []ColumnMetadata, error) {
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
			record[i] = shapeField(fieldFromValue(v), columns[i], opts)
		}

		records = append(records, record)
	}

	if iterErr := rows.Err(); iterErr != nil {
		return nil, nil, fmt.Errorf("iterate rows: %w", iterErr)
	}

	return records, columns, nil
}

// ResultSetOptions enum values (types.LongReturnType / types.DecimalReturnType
// in the real SDK). Both enums default to their first listed value when the
// request omits resultSetOptions entirely.
const (
	longReturnTypeLong            = "LONG"
	longReturnTypeString          = "STRING"
	decimalReturnTypeString       = "STRING"
	decimalReturnTypeDoubleOrLong = "DOUBLE_OR_LONG"
)

// shapeField applies resultSetOptions to a scanned Field, per the real
// ExecuteStatementInput.ResultSetOptions doc comments (types.go):
//   - LONG columns (JDBC INTEGER affinity): default LONG keeps longValue;
//     STRING renders the integer as a stringValue.
//   - DECIMAL columns (JDBC DECIMAL/NUMERIC affinity, i.e. no INT/CHAR/BLOB/
//     REAL keyword in the declared type): default STRING renders the value
//     as a stringValue; DOUBLE_OR_LONG parses it back to a longValue (no
//     fractional part) or doubleValue (fractional part) instead.
//
// meta.Type distinguishes the two cases; every other JDBC type (VARCHAR,
// BLOB, DOUBLE) and every NULL value pass through unchanged -- neither enum
// applies to them.
func shapeField(f Field, meta ColumnMetadata, opts resultSetOptions) Field {
	switch meta.Type {
	case jdbcTypeInteger:
		if opts.LongReturnType == longReturnTypeString {
			return longFieldAsString(f)
		}
	case jdbcTypeDecimal:
		if opts.DecimalReturnType == decimalReturnTypeDoubleOrLong {
			return decimalFieldAsDoubleOrLong(f)
		}

		return decimalFieldAsString(f)
	}

	return f
}

// longFieldAsString renders a longValue as a stringValue; f passes through
// unchanged if it isn't a longValue (e.g. it's NULL).
func longFieldAsString(f Field) Field {
	if f.LongValue == nil {
		return f
	}

	s := strconv.FormatInt(*f.LongValue, 10)

	return Field{StringValue: &s}
}

// decimalFieldAsString renders a numeric field as a stringValue, matching
// real AWS's default DecimalReturnType=STRING; f passes through unchanged if
// it's neither a longValue nor a doubleValue (e.g. it's NULL, or the driver
// already produced a string for this NUMERIC-affinity column).
func decimalFieldAsString(f Field) Field {
	switch {
	case f.LongValue != nil:
		s := strconv.FormatInt(*f.LongValue, 10)

		return Field{StringValue: &s}
	case f.DoubleValue != nil:
		s := strconv.FormatFloat(*f.DoubleValue, 'f', -1, 64)

		return Field{StringValue: &s}
	default:
		return f
	}
}

// decimalFieldAsDoubleOrLong converts a stringValue-shaped numeric field back
// to a longValue (whole number) or doubleValue (has a fractional part), per
// DecimalReturnType=DOUBLE_OR_LONG. f passes through unchanged if it isn't a
// stringValue (e.g. the driver already produced a long/double, or it's NULL).
func decimalFieldAsDoubleOrLong(f Field) Field {
	if f.StringValue == nil {
		return f
	}

	if iv, err := strconv.ParseInt(*f.StringValue, 10, 64); err == nil {
		return Field{LongValue: &iv}
	}

	if dv, err := strconv.ParseFloat(*f.StringValue, 64); err == nil {
		return Field{DoubleValue: &dv}
	}

	return f
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
