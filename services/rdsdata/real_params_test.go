package rdsdata //nolint:testpackage // white-box tests of the unexported placeholder, mapping and engine code.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strParam(name, v string) SQLParameter {
	return SQLParameter{Name: name, Value: Field{StringValue: &v}}
}

func longParam(name string, v int64) SQLParameter {
	return SQLParameter{Name: name, Value: Field{LongValue: &v}}
}

func TestTranslatePlaceholders(t *testing.T) {
	t.Parallel()

	hinted := strParam("d", "2024-01-02")
	hinted.TypeHint = typeHintDate

	tests := []struct {
		name          string
		kind          string
		sql           string
		wantSQL       string
		params        []SQLParameter
		wantArgs      []any
		wantReturning bool
	}{
		{
			name: "pg_positional", kind: kindPostgres, sql: "SELECT :a::int, :b",
			params:  []SQLParameter{longParam("a", 1), strParam("b", "x")},
			wantSQL: "SELECT $1::int, $2", wantArgs: []any{int64(1), "x"},
		},
		{
			name: "pg_reused_name_one_slot", kind: kindPostgres, sql: "SELECT :a + :a",
			params:  []SQLParameter{longParam("a", 7)},
			wantSQL: "SELECT $1 + $1", wantArgs: []any{int64(7)},
		},
		{
			name: "pg_cast_is_not_a_param", kind: kindPostgres, sql: "SELECT 1::int, now()::date",
			wantSQL: "SELECT 1::int, now()::date", wantArgs: nil,
		},
		{
			name: "pg_quoted_string_untouched", kind: kindPostgres, sql: "SELECT ':x', :y, 'it''s :x'",
			params:  []SQLParameter{strParam("y", "v")},
			wantSQL: "SELECT ':x', $1, 'it''s :x'", wantArgs: []any{"v"},
		},
		{
			name: "pg_quoted_identifier_untouched", kind: kindPostgres, sql: `SELECT "a:b" FROM t WHERE c = :c`,
			params:  []SQLParameter{longParam("c", 2)},
			wantSQL: `SELECT "a:b" FROM t WHERE c = $1`, wantArgs: []any{int64(2)},
		},
		{
			name: "pg_dollar_quote_untouched", kind: kindPostgres, sql: "SELECT $$ :x $$, $tag$ :x $tag$, :y",
			params:  []SQLParameter{strParam("y", "v")},
			wantSQL: "SELECT $$ :x $$, $tag$ :x $tag$, $1", wantArgs: []any{"v"},
		},
		{
			name: "pg_comments_untouched", kind: kindPostgres, sql: "SELECT 1 -- :x\n, :y /* :x /* :x */ :x */",
			params:  []SQLParameter{strParam("y", "v")},
			wantSQL: "SELECT 1 -- :x\n, $1 /* :x /* :x */ :x */", wantArgs: []any{"v"},
		},
		{
			name: "pg_escape_string_backslash", kind: kindPostgres, sql: `SELECT E'\':x', :y`,
			params:  []SQLParameter{strParam("y", "v")},
			wantSQL: `SELECT E'\':x', $1`, wantArgs: []any{"v"},
		},
		{
			name: "pg_array_slice_not_a_param", kind: kindPostgres, sql: "SELECT a[1:n] FROM t WHERE b = :b",
			params:  []SQLParameter{longParam("b", 3)},
			wantSQL: "SELECT a[1:n] FROM t WHERE b = $1", wantArgs: []any{int64(3)},
		},
		{
			name: "pg_hint_casts", kind: kindPostgres, sql: "INSERT INTO t VALUES (:d)",
			params:  []SQLParameter{hinted},
			wantSQL: "INSERT INTO t VALUES ($1::date)", wantArgs: []any{"2024-01-02"},
		},
		{
			name: "pg_returning", kind: kindPostgres, sql: "INSERT INTO t(a) VALUES (:a) returning id",
			params:  []SQLParameter{longParam("a", 1)},
			wantSQL: "INSERT INTO t(a) VALUES ($1) returning id", wantArgs: []any{int64(1)}, wantReturning: true,
		},
		{
			name: "pg_returning_in_string_ignored", kind: kindPostgres, sql: "SELECT 'RETURNING'",
			wantSQL: "SELECT 'RETURNING'",
		},
		{
			name: "mysql_question_per_occurrence", kind: kindMySQL, sql: "SELECT :a, :b, :a",
			params:  []SQLParameter{longParam("a", 1), strParam("b", "x")},
			wantSQL: "SELECT ?, ?, ?", wantArgs: []any{int64(1), "x", int64(1)},
		},
		{
			name: "mysql_backtick_and_backslash", kind: kindMySQL, sql: "SELECT `a:b`, '\\':x', :y",
			params:  []SQLParameter{strParam("y", "v")},
			wantSQL: "SELECT `a:b`, '\\':x', ?", wantArgs: []any{"v"},
		},
		{
			name: "mysql_hash_comment_and_user_var", kind: kindMySQL, sql: "SET @v:=:a # :x\n",
			params:  []SQLParameter{longParam("a", 5)},
			wantSQL: "SET @v:=? # :x\n", wantArgs: []any{int64(5)},
		},
		{
			name: "null_value", kind: kindMySQL, sql: "SELECT :n",
			params:  []SQLParameter{{Name: "n", Value: Field{IsNull: new(true)}}},
			wantSQL: "SELECT ?", wantArgs: []any{nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := translatePlaceholders(tt.kind, tt.sql, tt.params)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, got.SQL)
			assert.Equal(t, tt.wantArgs, got.Args)
			assert.Equal(t, tt.wantReturning, got.Returning)
		})
	}
}

func TestTranslatePlaceholdersMissingParam(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{kindPostgres, kindMySQL} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			_, err := translatePlaceholders(kind, "SELECT :missing", nil)
			require.ErrorIs(t, err, ErrValidation)
		})
	}
}

func TestRealIsQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kind      string
		sql       string
		returning bool
		want      bool
	}{
		{name: "select", kind: kindPostgres, sql: "SELECT 1", want: true},
		{name: "insert", kind: kindPostgres, sql: "INSERT INTO t VALUES (1)", want: false},
		{name: "insert_returning", kind: kindPostgres, sql: "INSERT INTO t VALUES (1)", returning: true, want: true},
		{name: "mysql_show", kind: kindMySQL, sql: "show tables", want: true},
		{name: "pg_show_is_exec", kind: kindPostgres, sql: "show tables", want: false},
		{name: "pg_table", kind: kindPostgres, sql: "TABLE t", want: true},
		{name: "empty", kind: kindMySQL, sql: "  ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, realIsQuery(tt.kind, tt.sql, tt.returning))
		})
	}
}
