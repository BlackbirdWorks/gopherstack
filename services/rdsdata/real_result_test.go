package rdsdata //nolint:testpackage // white-box tests of the unexported placeholder, mapping and engine code.

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealFieldMapping(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 1, 2, 3, 4, 5, 600000000, time.UTC)

	tests := []struct {
		value any
		name  string
		kind  string
		want  Field
		meta  ColumnMetadata
	}{
		{
			name: "pg_date", kind: kindPostgres, value: ts, meta: ColumnMetadata{Type: jdbcDate, TypeName: "date"},
			want: Field{StringValue: new("2024-01-02")},
		},
		{
			name: "pg_timestamp", kind: kindPostgres, value: ts,
			meta: ColumnMetadata{Type: jdbcTimestamp, TypeName: "timestamp"},
			want: Field{StringValue: new("2024-01-02 03:04:05.6")},
		},
		{
			name: "pg_json_bytes_as_string", kind: kindPostgres, value: []byte(`{"a":1}`),
			meta: ColumnMetadata{Type: jdbcOther, TypeName: "jsonb"}, want: Field{StringValue: new(`{"a":1}`)},
		},
		{
			name: "pg_bytea_blob", kind: kindPostgres, value: []byte{1, 2},
			meta: ColumnMetadata{Type: jdbcBinary, TypeName: "bytea"}, want: Field{BlobValue: []byte{1, 2}},
		},
		{
			name: "pg_int_array", kind: kindPostgres, value: []byte("{1,2,3}"),
			meta: ColumnMetadata{Type: jdbcArray, TypeName: "_int4", ArrayBaseColumnType: jdbcTypeInteger},
			want: Field{ArrayValue: &ArrayValue{LongValues: []int64{1, 2, 3}}},
		},
		{
			name: "pg_int_array_as_string", kind: kindPostgres, value: "{4,5}",
			meta: ColumnMetadata{Type: jdbcArray, TypeName: "_int4", ArrayBaseColumnType: jdbcTypeInteger},
			want: Field{ArrayValue: &ArrayValue{LongValues: []int64{4, 5}}},
		},
		{
			name: "pg_text_array_quoted", kind: kindPostgres, value: []byte(`{a,"b,c","d\"e"}`),
			meta: ColumnMetadata{Type: jdbcArray, TypeName: "_text", ArrayBaseColumnType: jdbcTypeVarchar},
			want: Field{ArrayValue: &ArrayValue{StringValues: []string{"a", "b,c", `d"e`}}},
		},
		{
			name: "pg_bool_array", kind: kindPostgres, value: []byte("{t,f}"),
			meta: ColumnMetadata{Type: jdbcArray, TypeName: "_bool", ArrayBaseColumnType: jdbcBit},
			want: Field{ArrayValue: &ArrayValue{BooleanValues: []bool{true, false}}},
		},
		{
			name: "mysql_int_text_protocol", kind: kindMySQL, value: []byte("42"),
			meta: ColumnMetadata{Type: jdbcTypeInteger, TypeName: "INT"}, want: Field{LongValue: new(int64(42))},
		},
		{
			name: "mysql_double", kind: kindMySQL, value: []byte("1.5"),
			meta: ColumnMetadata{Type: jdbcTypeDouble, TypeName: "DOUBLE"}, want: Field{DoubleValue: new(1.5)},
		},
		{
			name: "mysql_decimal_string", kind: kindMySQL, value: []byte("12.50"),
			meta: ColumnMetadata{Type: jdbcTypeDecimal, TypeName: "DECIMAL"}, want: Field{StringValue: new("12.50")},
		},
		{
			name: "mysql_bit_boolean", kind: kindMySQL, value: []byte{1},
			meta: ColumnMetadata{Type: jdbcBit, TypeName: "BIT"}, want: Field{BooleanValue: new(true)},
		},
		{
			name: "mysql_unsigned_overflow_string", kind: kindMySQL, value: uint64(1<<63 + 5),
			meta: ColumnMetadata{Type: jdbcBigInt, TypeName: "UNSIGNED BIGINT"},
			want: Field{StringValue: new("9223372036854775813")},
		},
		{
			name: "mysql_blob", kind: kindMySQL, value: []byte{9},
			meta: ColumnMetadata{Type: jdbcLongVarBin, TypeName: "BLOB"}, want: Field{BlobValue: []byte{9}},
		},
		{
			name: "null", kind: kindMySQL, value: nil, meta: ColumnMetadata{Type: jdbcTypeVarchar},
			want: Field{IsNull: new(true)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, err := realField(tt.kind, tt.meta, tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, f)
		})
	}
}

func TestRealFieldUnsupported(t *testing.T) {
	t.Parallel()

	arrayMeta := ColumnMetadata{Type: jdbcArray, TypeName: "_int4"}

	tests := []struct {
		value any
		name  string
		meta  ColumnMetadata
	}{
		{name: "null_array_element", value: []byte("{1,NULL}"), meta: arrayMeta},
		{name: "multidimensional", value: []byte("{{1},{2}}"), meta: arrayMeta},
		{name: "unknown_go_type", value: struct{}{}, meta: ColumnMetadata{TypeName: "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := realField(kindPostgres, tt.meta, tt.value)
			require.Error(t, err)

			var api *apiError

			require.ErrorAs(t, err, &api)
			assert.Equal(t, "UnsupportedResultException", api.code)
		})
	}
}

func TestShapeRealField(t *testing.T) {
	t.Parallel()

	str := "12.0"
	n := int64(5)

	tests := []struct {
		opts resultSetOptions
		name string
		in   Field
		want Field
		meta ColumnMetadata
	}{
		{
			name: "long_as_string", in: Field{LongValue: &n}, meta: ColumnMetadata{Type: jdbcBigInt},
			opts: resultSetOptions{LongReturnType: longReturnTypeString}, want: Field{StringValue: new("5")},
		},
		{
			name: "decimal_double_or_long",
			in:   Field{StringValue: &str},
			meta: ColumnMetadata{Type: jdbcNumeric},
			opts: resultSetOptions{
				DecimalReturnType: decimalReturnTypeDoubleOrLong,
			},
			want: Field{DoubleValue: new(12.0)},
		},
		{
			name: "default_unchanged", in: Field{StringValue: &str}, meta: ColumnMetadata{Type: jdbcNumeric},
			want: Field{StringValue: &str},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, shapeRealField(tt.in, tt.meta, tt.opts))
		})
	}
}

var (
	errRefused = errors.New("refused")
	errLeaky   = errors.New("boom password=hunter2")
)

func TestMapDriverError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err      error
		name     string
		wantCode string
		wantNot  bool
	}{
		{name: "nil", err: nil},
		{
			name:     "pg_syntax",
			err:      &pgconn.PgError{Code: "42601", Message: "syntax error"},
			wantCode: "DatabaseErrorException",
		},
		{name: "pg_auth", err: &pgconn.PgError{Code: "28P01", Message: "bad"}, wantCode: "InvalidSecretException"},
		{name: "pg_cancel", err: &pgconn.PgError{Code: "57014"}, wantCode: "StatementTimeoutException"},
		{name: "pg_conn_class", err: &pgconn.PgError{Code: "08006"}, wantCode: "DatabaseUnavailableException"},
		{
			name:     "mysql_syntax",
			err:      &mysql.MySQLError{Number: 1064, Message: "bad sql"},
			wantCode: "DatabaseErrorException",
		},
		{
			name:     "mysql_denied",
			err:      &mysql.MySQLError{Number: 1045, Message: "denied"},
			wantCode: "InvalidSecretException",
		},
		{name: "mysql_lock_timeout", err: &mysql.MySQLError{Number: 1205}, wantCode: "StatementTimeoutException"},
		{name: "deadline", err: context.DeadlineExceeded, wantCode: "StatementTimeoutException"},
		{name: "net", err: &net.OpError{Op: "dial", Err: errRefused}, wantCode: "DatabaseUnavailableException"},
		{name: "unknown", err: errLeaky, wantCode: "DatabaseErrorException"},
		{name: "tx_done", err: sql.ErrTxDone, wantNot: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mapDriverError(tt.err)

			switch {
			case tt.err == nil:
				require.NoError(t, got)
			case tt.wantNot:
				require.ErrorIs(t, got, ErrTransactionNotFound)
			default:
				var api *apiError

				require.ErrorAs(t, got, &api)
				assert.Equal(t, tt.wantCode, api.code)
				assert.NotContains(t, api.msg, "hunter2")
			}
		})
	}
}
