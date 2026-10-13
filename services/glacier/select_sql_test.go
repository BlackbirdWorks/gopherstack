package glacier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseSelectExpression_Valid verifies that the Glacier Select SQL subset
// (see services/glacier/select.go's package doc for the grammar) accepts every
// documented construct: SELECT *, column lists, aliasing, qualified column
// references, WHERE with AND/OR and every comparison operator, quoted string
// literals (including escaped quotes), and numeric literals.
func TestParseSelectExpression_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "select_star", expr: "SELECT * FROM archive"},
		{name: "select_star_lowercase", expr: "select * from archive"},
		{name: "select_star_aliased_table", expr: "SELECT * FROM archive s"},
		{name: "select_columns", expr: "SELECT _1, _2 FROM archive"},
		{name: "select_qualified_columns", expr: "SELECT s._1, s._2 FROM archive s"},
		{name: "select_with_as_alias", expr: "SELECT _1 AS col1, _2 AS col2 FROM archive"},
		{name: "select_header_name_column", expr: "SELECT name, age FROM archive"},
		{name: "where_eq_string", expr: "SELECT * FROM archive WHERE _1 = 'active'"},
		{name: "where_eq_escaped_quote", expr: "SELECT * FROM archive WHERE _1 = 'it''s'"},
		{name: "where_ne", expr: "SELECT * FROM archive WHERE _1 != 'x'"},
		{name: "where_altne", expr: "SELECT * FROM archive WHERE _1 <> 'x'"},
		{name: "where_lt", expr: "SELECT * FROM archive WHERE _2 < 100"},
		{name: "where_le", expr: "SELECT * FROM archive WHERE _2 <= 100"},
		{name: "where_gt", expr: "SELECT * FROM archive WHERE _2 > 100"},
		{name: "where_ge", expr: "SELECT * FROM archive WHERE _2 >= 100"},
		{name: "where_negative_number", expr: "SELECT * FROM archive WHERE _2 > -5"},
		{name: "where_decimal_number", expr: "SELECT * FROM archive WHERE _2 > 3.14"},
		{name: "where_and", expr: "SELECT * FROM archive WHERE _1 = 'a' AND _2 > 1"},
		{name: "where_or", expr: "SELECT * FROM archive WHERE _1 = 'a' OR _1 = 'b'"},
		{
			name: "where_and_or_mixed",
			expr: "SELECT * FROM archive WHERE _1 = 'a' AND _2 > 1 OR _1 = 'b' AND _2 > 2",
		},
		{name: "where_lowercase_keywords", expr: "SELECT * FROM archive where _1 = 'a' and _2 > 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseSelectQuery(tt.expr)
			assert.NoError(t, err, "expression: %s", tt.expr)
		})
	}
}

// TestParseSelectExpression_Invalid verifies that malformed expressions are rejected
// with a parse error rather than panicking or silently misparsing. This also covers
// LIMIT: real S3 Glacier Select's SELECT command documents LIMIT as "(Amazon S3
// Select only)" ("S3 Glacier Select does not support the LIMIT clause" --
// doc_source/s3-glacier-select-sql-reference-select.md,
// awsdocs/amazon-glacier-developer-guide), so any LIMIT clause is a rejected
// construct here, not merely an out-of-range value.
func TestParseSelectExpression_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "empty", expr: ""},
		{name: "missing_select", expr: "* FROM archive"},
		{name: "missing_from", expr: "SELECT *"},
		{name: "missing_table", expr: "SELECT * FROM"},
		{name: "not_sql_at_all", expr: "DROP TABLE archive"},
		{name: "unterminated_string", expr: "SELECT * FROM archive WHERE _1 = 'unterminated"},
		{name: "unknown_character", expr: "SELECT * FROM archive WHERE _1 = @foo"},
		{name: "missing_operator", expr: "SELECT * FROM archive WHERE _1 'a'"},
		{name: "missing_literal", expr: "SELECT * FROM archive WHERE _1 ="},
		{name: "missing_where_predicate", expr: "SELECT * FROM archive WHERE"},
		{name: "trailing_garbage", expr: "SELECT * FROM archive EXTRA TOKENS"},
		{name: "limit", expr: "SELECT * FROM archive LIMIT 10"},
		{name: "where_and_limit", expr: "SELECT * FROM archive WHERE _1 = 'a' LIMIT 5"},
		{name: "limit_zero", expr: "SELECT * FROM archive LIMIT 0"},
		{name: "limit_not_a_number", expr: "SELECT * FROM archive LIMIT abc"},
		{name: "limit_negative", expr: "SELECT * FROM archive LIMIT -1"},
		{name: "dangling_comma", expr: "SELECT _1, FROM archive"},
		{name: "dangling_dot", expr: "SELECT s. FROM archive"},
		{name: "as_without_alias", expr: "SELECT _1 AS FROM archive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseSelectQuery(tt.expr)
			assert.Error(t, err, "expression: %s", tt.expr)
		})
	}
}

const selectExprCSV = "name,age,city\nalice,30,Paris\nbob,25,Berlin\ncarol,40,paris\ndave,,Rome\n"

func runSelectExpr(t *testing.T, expr string) (string, error) {
	t.Helper()

	sp := &selectParametersDTO{
		Expression:          expr,
		ExpressionType:      "SQL",
		InputSerialization:  &inputSerializationDTO{Csv: &csvInputDTO{FileHeaderInfo: "USE"}},
		OutputSerialization: &outputSerializationDTO{Csv: &csvOutputDTO{}},
	}

	out, err := executeSelect([]byte(selectExprCSV), sp)

	return string(out), err
}

func TestSelectExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{
			name: "parens_group",
			expr: "SELECT name FROM archive WHERE (age < 26 OR age > 35) AND city <> 'Rome'",
			want: "bob\ncarol\n",
		},
		{name: "not", expr: "SELECT name FROM archive WHERE NOT age > 26 AND age >= 0", want: "bob\n"},
		{name: "between", expr: "SELECT name FROM archive WHERE age BETWEEN 26 AND 35", want: "alice\n"},
		{
			name: "not_between",
			expr: "SELECT name FROM archive WHERE age NOT BETWEEN 26 AND 35 AND name <> 'dave'",
			want: "bob\ncarol\n",
		},
		{name: "in", expr: "SELECT name FROM archive WHERE city IN ('Paris', 'Rome')", want: "alice\ndave\n"},
		{name: "not_in", expr: "SELECT name FROM archive WHERE city NOT IN ('Paris', 'Rome')", want: "bob\ncarol\n"},
		{name: "like_prefix", expr: "SELECT name FROM archive WHERE name LIKE 'a%'", want: "alice\n"},
		{name: "like_single", expr: "SELECT name FROM archive WHERE name LIKE 'bo_'", want: "bob\n"},
		{name: "not_like", expr: "SELECT name FROM archive WHERE name NOT LIKE '%a%'", want: "bob\n"},
		{name: "like_escape", expr: "SELECT name FROM archive WHERE city LIKE 'P\\aris' ESCAPE '\\'", want: "alice\n"},
		{
			name: "arithmetic",
			expr: "SELECT name, age * 2 + 1 FROM archive WHERE age % 10 = 0",
			want: "alice,61\ncarol,81\n",
		},
		{name: "arithmetic_where", expr: "SELECT name FROM archive WHERE age - 5 = 25", want: "alice\n"},
		{name: "division", expr: "SELECT age / 4 FROM archive WHERE name = 'bob'", want: "6.25\n"},
		{name: "cast_int", expr: "SELECT CAST(age AS INT) + 1 FROM archive WHERE name = 'carol'", want: "41\n"},
		{name: "cast_string", expr: "SELECT CAST(age AS STRING) FROM archive WHERE name = 'bob'", want: "25\n"},
		{name: "cast_bad_is_null", expr: "SELECT CAST(city AS INT) FROM archive WHERE name = 'bob'", want: "\n"},
		{name: "coalesce", expr: "SELECT COALESCE(CAST(age AS INT), 0) FROM archive WHERE name = 'dave'", want: "0\n"},
		{name: "nullif", expr: "SELECT NULLIF(city, 'Berlin') FROM archive WHERE name = 'bob'", want: "\n"},
		{
			name: "nullif_differs",
			expr: "SELECT NULLIF(city, 'Berlin') FROM archive WHERE name = 'alice'",
			want: "Paris\n",
		},
		{name: "unary_minus", expr: "SELECT name FROM archive WHERE -age < -35", want: "carol\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := runSelectExpr(t, tt.expr)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSelectExpressionRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "unclosed_paren", expr: "SELECT * FROM archive WHERE (age > 1"},
		{name: "bare_value_where", expr: "SELECT * FROM archive WHERE age"},
		{name: "arithmetic_where", expr: "SELECT * FROM archive WHERE age + 1"},
		{name: "unknown_function", expr: "SELECT BOGUS(age) FROM archive"},
		{name: "bad_cast_type", expr: "SELECT CAST(age AS WIDGET) FROM archive"},
		{name: "not_without_operator", expr: "SELECT * FROM archive WHERE age NOT 5"},
		{name: "between_missing_and", expr: "SELECT * FROM archive WHERE age BETWEEN 1 5"},
		{name: "nullif_arity", expr: "SELECT NULLIF(age) FROM archive"},
		{name: "empty_in", expr: "SELECT * FROM archive WHERE age IN ()"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseSelectQuery(tt.expr)
			assert.Error(t, err)
		})
	}
}
