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
	"sync/atomic"

	// modernc.org/sqlite registers the pure-Go "sqlite" database/sql driver
	// (no cgo), so the Data API can execute real SQL against an in-memory
	// engine on every platform the rest of gopherstack builds for. It is
	// imported by name (not blank) because columnOriginInfo below also uses
	// its exported ColumnInfo type.
	sqlitedriver "modernc.org/sqlite"
)

// errNoEngineTx is returned when a statement references a transaction that has
// no live engine-side *sql.Tx (e.g. it was created before this process start,
// or the process restored a snapshot that recorded the transaction as still
// active but couldn't restore its engine-side state -- see Restore in
// persistence.go).
var errNoEngineTx = errors.New("no engine transaction")

// resourceDB bundles an in-memory database with the keep-alive connection that
// keeps its shared-cache backing store from being reclaimed by the pool.
type resourceDB struct {
	db        *sql.DB
	keepAlive *sql.Conn
}

// engineTx bundles an open engine-side transaction with the cancel func for
// the context it was opened under. That context is derived from the engine's
// own lifetime (sqlEngine.baseCtx), not the caller's per-request context --
// see beginTx's doc comment for why that distinction is the fix for
// gopherstack-wh8gv.
type engineTx struct {
	tx     *sql.Tx
	cancel context.CancelFunc
}

// sqlEngine backs the RDS Data API with real, per-resource in-memory SQLite
// databases. Each (region, resourceARN) pair maps to its own database so that
// statements issued against different Aurora clusters stay isolated.
//
// baseCtx/baseCancel give every engine-side transaction a lifetime tied to
// the engine itself rather than to whichever HTTP request happened to call
// BeginTransaction; see beginTx.
type sqlEngine struct {
	dbs        map[string]*resourceDB
	txs        map[string]*engineTx
	baseCtx    context.Context
	baseCancel context.CancelFunc
	nonce      string
	mu         sync.Mutex
}

// engineSeq is a process-wide counter folded into every engine's nonce
// (below) so that no two sqlEngine instances ever alias the same
// process-global shared-cache in-memory SQLite database. A pointer address
// alone isn't enough: Go's GC doesn't move heap objects, but it does reuse a
// collected object's address for a later allocation, and none of this
// package's tests explicitly close their engine's *sql.DB, so a later
// engine that happens to land at a prior, still-referenced-by-nothing-else
// engine's old address would otherwise compute the identical dbKey for the
// same (region, resourceARN) pair -- silently sharing one SQLite database,
// and its rows, between what should be two fully isolated backends.
// Confirmed by reproduction: `go test -race -count=20 -run Transaction`
// intermittently failed with an extra row in a table a test expected to
// have inserted into only once, before this fix.
//
//nolint:gochecknoglobals // process-wide uniqueness counter, not mutable config
var engineSeq atomic.Uint64

// newSQLEngine constructs an empty engine. See engineSeq for why the nonce
// is a counter, not just the engine's own pointer address.
func newSQLEngine() *sqlEngine {
	baseCtx, baseCancel := context.WithCancel(context.Background())

	e := &sqlEngine{
		dbs:        make(map[string]*resourceDB),
		txs:        make(map[string]*engineTx),
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
		nonce:      "",
		mu:         sync.Mutex{},
	}
	e.nonce = fmt.Sprintf("%d-%p", engineSeq.Add(1), e)

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

	var originDB *sql.DB

	if transactionID != "" {
		et, ok := e.txs[transactionID]
		if !ok {
			return nil, nil, 0, nil, errNoEngineTx
		}

		run = et.tx
	} else {
		db, err := e.dbFor(ctx, region, resourceARN)
		if err != nil {
			return nil, nil, 0, nil, err
		}

		run = db
		// originDB backs columnOriginInfo's conn.Raw lookup (see
		// applyColumnOrigin). Only available here: *sql.Tx has no equivalent
		// to *sql.Conn.Raw, so a statement run inside a BeginTransaction
		// transaction can't use this path -- see PARITY.md.
		originDB = db
	}

	return runStatement(ctx, run, originDB, statement, params, getResultSetOptions(ctx))
}

// beginTx opens an engine-side transaction bound to txID, under a context
// derived from the engine's own lifetime (e.baseCtx) rather than ctx (the
// caller's per-request context). database/sql.DB.BeginTx documents: "The
// provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction" (database/sql/sql.go:1866-1868, go1.27 stdlib). Binding to a
// per-request context meant a BeginTransaction call's *sql.Tx was silently
// rolled back the instant that HTTP response finished, and every later
// ExecuteStatement/BatchExecuteStatement against that transactionId then hit
// sql.ErrTxDone -- previously swallowed into a fabricated empty-success
// envelope (gopherstack-wh8gv). ctx itself is still used to open/pin the
// resource database (dbFor): that call's context only bounds waiting for a
// connection, not the returned *sql.DB's lifetime, so it carries no similar
// landmine. The caller must have already validated/allocated txID.
func (e *sqlEngine) beginTx(ctx context.Context, region, resourceARN, txID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	db, err := e.dbFor(ctx, region, resourceARN)
	if err != nil {
		return err
	}

	txCtx, cancel := context.WithCancel(e.baseCtx)

	tx, err := db.BeginTx(txCtx, nil)
	if err != nil {
		cancel()

		return fmt.Errorf("begin tx: %w", err)
	}

	e.txs[txID] = &engineTx{tx: tx, cancel: cancel}

	return nil
}

// finalizeTx commits or rolls back the engine transaction for txID, if any,
// and cancels its engine-owned context (see beginTx).
func (e *sqlEngine) finalizeTx(txID string, commit bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	et, ok := e.txs[txID]
	if !ok {
		return
	}

	delete(e.txs, txID)

	if commit {
		_ = et.tx.Commit()
	} else {
		_ = et.tx.Rollback()
	}

	et.cancel()
}

// reset closes every open database and transaction, then replaces baseCtx/
// baseCancel so the engine remains usable for new transactions afterward
// (Reset() is a test/operational reset of backend state, not a permanent
// shutdown).
func (e *sqlEngine) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for id, et := range e.txs {
		_ = et.tx.Rollback()
		et.cancel()
		delete(e.txs, id)
	}

	for key, rdb := range e.dbs {
		_ = rdb.keepAlive.Close()
		_ = rdb.db.Close()
		delete(e.dbs, key)
	}

	e.baseCancel()
	e.baseCtx, e.baseCancel = context.WithCancel(context.Background())
}

// isDeadTransactionError reports whether err reflects a transaction id that
// can no longer be used at the engine level: either txID was never opened
// there (errNoEngineTx -- e.g. after a snapshot Restore, which cannot
// reconstruct an open *sql.Tx), or database/sql itself refused a further
// operation on an already-committed/rolled-back *sql.Tx (sql.ErrTxDone,
// database/sql/sql.go:2233-2235, go1.27 stdlib: "ErrTxDone is returned by any
// operation that is performed on a transaction that has already been
// committed or rolled back"). Both cases mean the same thing to a caller:
// the transaction id is gone.
func isDeadTransactionError(err error) bool {
	return errors.Is(err, errNoEngineTx) || errors.Is(err, sql.ErrTxDone)
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
	originDB *sql.DB,
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

		records, columns, scanErr := scanRows(ctx, run, originDB, statement, rows, opts)
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
// rules).
func hasRowIDAliasColumn(ctx context.Context, run querier, table string) bool {
	_, ok := rowIDAliasColumn(ctx, run, table)

	return ok
}

// isRowIDAliasColumn reports whether column is table's sole rowid-alias
// INTEGER PRIMARY KEY column -- the signal applyColumnOrigin uses for
// ColumnMetadata.IsAutoIncrement (real AWS: "a value that indicates whether
// the column increments automatically").
func isRowIDAliasColumn(ctx context.Context, run querier, table, column string) bool {
	name, ok := rowIDAliasColumn(ctx, run, table)

	return ok && name == column
}

// rowIDAliasColumn returns the name of table's INTEGER PRIMARY KEY column
// when it declares exactly one such column (SQLite's documented rowid alias,
// https://sqlite.org/lang_createtable.html#rowid); ("", false) otherwise.
// table is only ever a name the SQLite engine itself resolved (see
// insertIntoTableRe's regexp-validated capture, or the real
// sqlite3_column_table_name value columnOriginInfo returns), so it is safe
// to interpolate directly into the PRAGMA statement -- database/sql has no
// bind-parameter support for PRAGMA targets.
func rowIDAliasColumn(ctx context.Context, run querier, table string) (string, bool) {
	rows, err := run.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return "", false
	}
	defer func() { _ = rows.Close() }()

	pkCount := 0

	var pkName string

	isIntegerPK := false

	for rows.Next() {
		var cid, notnull, pk int

		var name, ctype string

		var dflt any

		if scanErr := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); scanErr != nil {
			return "", false
		}

		if pk > 0 {
			pkCount++
			pkName = name
			isIntegerPK = strings.Contains(strings.ToUpper(ctype), "INT")
		}
	}

	if rows.Err() != nil || pkCount != 1 || !isIntegerPK {
		return "", false
	}

	return pkName, true
}

// columnOriginInfo resolves each result column's source table/database/
// origin-column name via the real sqlite3_column_table_name /
// sqlite3_column_database_name / sqlite3_column_origin_name C APIs, which
// modernc.org/sqlite@v1.58.0 exposes through *sql.Conn.Raw (see conn.go's
// ColumnInfo) -- database/sql's own sql.ColumnType has no such accessor.
// Returns nil when originDB is nil (statement ran inside a transaction; see
// runStatement), the driver conn doesn't implement the accessor, or opening
// a fresh connection fails, so callers degrade to the historical
// zero-valued fields. A column that doesn't resolve to an unambiguous table
// column (an expression, function call, or constant) reports an empty
// TableName, per the accessor's own documented contract -- not an error.
func columnOriginInfo(ctx context.Context, originDB *sql.DB, statement string) []sqlitedriver.ColumnInfo {
	if originDB == nil {
		return nil
	}

	conn, err := originDB.Conn(ctx)
	if err != nil {
		return nil
	}
	defer func() { _ = conn.Close() }()

	var info []sqlitedriver.ColumnInfo

	_ = conn.Raw(func(driverConn any) error {
		ci, ok := driverConn.(interface {
			ColumnInfo(query string) ([]sqlitedriver.ColumnInfo, error)
		})
		if !ok {
			return nil
		}

		info, _ = ci.ColumnInfo(statement)

		return nil
	})

	return info
}

// applyColumnOrigin fills ColumnMetadata.SchemaName/TableName/
// IsAutoIncrement from columnOriginInfo, matching each result column by
// position. DatabaseName ("main" for the default, unattached database) is
// reported as SchemaName -- the closest signal SQLite exposes to a
// PostgreSQL/MySQL schema, since SQLite databases have no schema catalog of
// their own. Left at the zero value (unmodified) for any column
// columnOriginInfo can't resolve, or when it returns nil entirely.
func applyColumnOrigin(ctx context.Context, run querier, originDB *sql.DB, statement string, columns []ColumnMetadata) {
	origin := columnOriginInfo(ctx, originDB, statement)

	for i := range columns {
		if i >= len(origin) || origin[i].TableName == "" {
			continue
		}

		columns[i].TableName = origin[i].TableName
		columns[i].SchemaName = origin[i].DatabaseName
		columns[i].IsAutoIncrement = isRowIDAliasColumn(ctx, run, origin[i].TableName, origin[i].OriginName)
	}
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

// columnMetadataFor builds a ColumnMetadata for one result column from
// database/sql's sql.ColumnType: the declared type name, nullability (the
// pure-Go driver always reports "nullable, known" -- see
// modernc.org/sqlite's rows.ColumnTypeNullable), and decimal size (never
// known). SchemaName/TableName/IsAutoIncrement are filled in afterward by
// applyColumnOrigin, which uses the driver's own column-origin accessor
// (sql.ColumnType itself has no such accessor). ArrayBaseColumnType is left
// at 0: this mock's result columns are never array-typed (see PARITY.md).
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
// each column's values -- see shapeField. originDB (nil inside a
// transaction) backs applyColumnOrigin's SchemaName/TableName/
// IsAutoIncrement lookup.
func scanRows(
	ctx context.Context, run querier, originDB *sql.DB, statement string, rows *sql.Rows, opts resultSetOptions,
) ([][]Field, []ColumnMetadata, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("column types: %w", err)
	}

	columns := make([]ColumnMetadata, len(cols))
	for i, ct := range cols {
		columns[i] = columnMetadataFor(ct)
	}

	applyColumnOrigin(ctx, run, originDB, statement, columns)

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
