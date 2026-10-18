package redshiftdata

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

const (
	engineStatementTimeout = 45 * time.Second
	nullableUnknown        = 1
	pgTextMaxLength        = math.MaxInt32

	precisionInt2   = 5
	precisionInt4   = 10
	precisionInt8   = 19
	precisionFloat4 = 8
	precisionFloat8 = 17
)

var errMissingParameter = errors.New("parameter is not provided")

type pgxExecutor struct {
	open  func(EngineLogin) (*sql.DB, error)
	store atomic.Pointer[storeHolder]
}

// NewPGXExecutor returns an SQLExecutor that speaks the Postgres wire protocol.
func NewPGXExecutor() SQLExecutor { return &pgxExecutor{open: openPostgres} }

func openPostgres(l EngineLogin) (*sql.DB, error) {
	name := l.DBName
	if name == "" {
		name = l.User
	}

	u := url.URL{
		Scheme: "postgres", User: url.UserPassword(l.User, l.Password), Host: l.Addr, Path: "/" + name,
		RawQuery: "sslmode=disable&connect_timeout=5&default_query_exec_mode=simple_protocol",
	}

	return sql.Open("pgx", u.String())
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (x *pgxExecutor) ExecuteSQL(
	ctx context.Context, login EngineLogin, sqls []string, params []SQLParameter, atomic bool,
) []SQLOutcome {
	ctx, cancel := context.WithTimeout(ctx, engineStatementTimeout)
	defer cancel()

	db, err := x.open(login)
	if err != nil {
		return []SQLOutcome{{Err: formatPGError(err)}}
	}

	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		return []SQLOutcome{{Err: formatPGError(err)}}
	}

	defer func() { _ = conn.Close() }()

	s3 := &s3Runner{conn: conn}
	if h := x.store.Load(); h != nil {
		s3.store = h.store
	}

	if !atomic {
		return runStatements(ctx, conn, s3, sqls, params)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return []SQLOutcome{{Err: formatPGError(err)}}
	}

	outcomes := runStatements(ctx, tx, s3, sqls, params)

	if failedAny(outcomes) {
		_ = tx.Rollback()

		return outcomes
	}

	if err = tx.Commit(); err != nil {
		outcomes[len(outcomes)-1].Err = formatPGError(err)
	}

	return outcomes
}

func failedAny(outcomes []SQLOutcome) bool {
	for _, o := range outcomes {
		if o.Err != nil {
			return true
		}
	}

	return false
}

func runStatements(
	ctx context.Context, q querier, s3 *s3Runner, sqls []string, params []SQLParameter,
) []SQLOutcome {
	outcomes := make([]SQLOutcome, 0, len(sqls))

	for _, stmt := range sqls {
		rs, err := runStatement(ctx, q, s3, stmt, params)
		outcomes = append(outcomes, SQLOutcome{Result: rs, Err: err})

		if err != nil {
			break
		}
	}

	return outcomes
}

func runStatement(
	ctx context.Context, q querier, s3 *s3Runner, stmt string, params []SQLParameter,
) (*ResultSet, error) {
	if handled, err := s3.run(ctx, q, stmt); handled {
		return nil, err
	}

	query, args := stmt, []any(nil)

	if len(params) > 0 {
		var err error

		if query, args, err = BindNamedParameters(stmt, params); err != nil {
			return nil, err
		}
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, formatPGError(err)
	}

	defer func() { _ = rows.Close() }()

	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, formatPGError(err)
	}

	if len(cts) == 0 {
		for rows.Next() {
			continue
		}

		if err = rows.Err(); err != nil {
			return nil, formatPGError(err)
		}

		return nil, nil //nolint:nilnil // a statement without a result set has no ResultSet
	}

	return scanRows(rows, cts)
}

func scanRows(rows *sql.Rows, cts []*sql.ColumnType) (*ResultSet, error) {
	rs := &ResultSet{Columns: make([]ColumnMetadata, len(cts)), Rows: [][]Field{}}
	types := make([]string, len(cts))

	for i, ct := range cts {
		rs.Columns[i] = columnMetadata(ct)
		types[i] = ct.DatabaseTypeName()
	}

	for rows.Next() {
		vals := make([]any, len(cts))
		ptrs := make([]any, len(cts))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, formatPGError(err)
		}

		rec := make([]Field, len(cts))
		for i, v := range vals {
			rec[i] = FieldFromValue(types[i], v)
		}

		rs.Rows = append(rs.Rows, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, formatPGError(err)
	}

	return rs, nil
}

func columnMetadata(ct *sql.ColumnType) ColumnMetadata {
	typeName := strings.ToLower(ct.DatabaseTypeName())
	md := ColumnMetadata{Name: ct.Name(), Label: ct.Name(), TypeName: typeName, Nullable: nullableUnknown}

	if nullable, ok := ct.Nullable(); ok && !nullable {
		md.Nullable = 0
	}

	switch typeName {
	case "int2":
		md.Precision, md.IsSigned = precisionInt2, true
	case pgInt4:
		md.Precision, md.IsSigned = precisionInt4, true
	case "int8":
		md.Precision, md.IsSigned = precisionInt8, true
	case "float4":
		md.Precision, md.IsSigned = precisionFloat4, true
	case "float8":
		md.Precision, md.IsSigned = precisionFloat8, true
	case "numeric":
		md.IsSigned = true

		if p, s, ok := ct.DecimalSize(); ok {
			md.Precision, md.Scale = clampInt32(p), clampInt32(s)
		}
	case "varchar", "bpchar", "text", "char":
		md.IsCaseSensitive = true

		if n, ok := ct.Length(); ok && n < pgTextMaxLength {
			md.Length = clampInt32(n)
		}
	}

	return md
}

func clampInt32(v int64) int32 {
	return int32(min(max(v, 0), math.MaxInt32))
}

// FieldFromValue maps a database/sql scanned value of a Postgres column type to a Data API Field.
func FieldFromValue(dbType string, v any) Field {
	switch x := v.(type) {
	case nil:
		return Field{IsNull: true}
	case bool:
		return Field{BooleanValue: &x}
	case int64:
		return Field{LongValue: &x}
	case int32:
		return longField(int64(x))
	case int16:
		return longField(int64(x))
	case int:
		return longField(int64(x))
	case float64:
		return doubleField(x)
	case float32:
		return doubleField(float64(x))
	case string:
		return Field{StringValue: &x}
	case time.Time:
		s := formatTime(dbType, x)

		return Field{StringValue: &s}
	case []byte:
		return bytesField(dbType, x)
	default:
		return stringField(compositeString(v))
	}
}

func longField(v int64) Field { return Field{LongValue: &v} }

func stringField(s string) Field { return Field{StringValue: &s} }

func doubleField(v float64) Field {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return stringField(strconv.FormatFloat(v, 'g', -1, 64))
	}

	return Field{DoubleValue: &v}
}

func bytesField(dbType string, b []byte) Field {
	if strings.EqualFold(dbType, "BYTEA") {
		s := base64.StdEncoding.EncodeToString(b)

		return Field{BlobValue: &s}
	}

	return stringField(string(b))
}

func compositeString(v any) string {
	switch reflect.ValueOf(v).Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		if raw, err := json.Marshal(v); err == nil {
			return string(raw)
		}
	default:
	}

	return fmt.Sprint(v)
}

func formatTime(dbType string, t time.Time) string {
	switch strings.ToUpper(dbType) {
	case "DATE":
		return t.Format("2006-01-02")
	case "TIMESTAMPTZ":
		return t.Format("2006-01-02 15:04:05.999999-07")
	default:
		return t.Format("2006-01-02 15:04:05.999999")
	}
}

// formatPGError renders a driver error the way Redshift reports statement errors.
func formatPGError(err error) error {
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		return fmt.Errorf("ERROR: %s", pg.Message) //nolint:err113 // message mirrors the engine's text
	}

	return fmt.Errorf("could not run the statement: %w", err)
}

// BindNamedParameters rewrites :name placeholders to positional $n arguments; quoted text, comments and
// :: casts are left alone.
func BindNamedParameters(stmt string, params []SQLParameter) (string, []any, error) {
	values := make(map[string]string, len(params))
	for _, p := range params {
		values[p.Name] = p.Value
	}

	var (
		out   strings.Builder
		args  []any
		index = map[string]int{}
	)

	for i := 0; i < len(stmt); {
		end, skipped := skipLiteral(stmt, i)
		if skipped {
			out.WriteString(stmt[i:end])
			i = end

			continue
		}

		c := stmt[i]
		if c != ':' || i+1 >= len(stmt) {
			out.WriteByte(c)
			i++

			continue
		}

		if stmt[i+1] == ':' {
			out.WriteString("::")
			i += 2

			continue
		}

		j := i + 1
		for j < len(stmt) && isNameByte(stmt[j]) {
			j++
		}

		name := stmt[i+1 : j]
		if name == "" {
			out.WriteByte(c)
			i++

			continue
		}

		val, ok := values[name]
		if !ok {
			return "", nil, fmt.Errorf("ERROR: %w: %s", errMissingParameter, name)
		}

		n, seen := index[name]
		if !seen {
			args = append(args, val)
			n = len(args)
			index[name] = n
		}

		out.WriteString("$" + strconv.Itoa(n))
		i = j
	}

	return out.String(), args, nil
}

func isNameByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// skipLiteral returns the end of a quoted literal or comment starting at i; skipped is false otherwise.
func skipLiteral(s string, i int) (int, bool) {
	switch {
	case s[i] == '\'' || s[i] == '"':
		q := s[i]

		for j := i + 1; j < len(s); j++ {
			if s[j] == q {
				if j+1 < len(s) && s[j+1] == q {
					j++

					continue
				}

				return j + 1, true
			}
		}

		return len(s), true
	case strings.HasPrefix(s[i:], "--"):
		if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
			return i + nl, true
		}

		return len(s), true
	default:
		return i, false
	}
}
