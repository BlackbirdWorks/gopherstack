package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestParseSelectExpression_Valid verifies that the Glacier Select SQL subset
// (see services/glacier/select.go's package doc for the grammar) accepts every
// documented construct: SELECT *, column lists, aliasing, qualified column
// references, WHERE with AND/OR and every comparison operator, quoted string
// literals (including escaped quotes), numeric literals, and LIMIT.
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
		{name: "limit", expr: "SELECT * FROM archive LIMIT 10"},
		{name: "where_and_limit", expr: "SELECT * FROM archive WHERE _1 = 'a' LIMIT 5"},
		{name: "limit_zero", expr: "SELECT * FROM archive LIMIT 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := glacier.ParseSelectExpression(tt.expr)
			assert.NoError(t, err, "expression: %s", tt.expr)
		})
	}
}

// TestParseSelectExpression_Invalid verifies that malformed expressions are rejected
// with a parse error rather than panicking or silently misparsing.
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
		{name: "limit_not_a_number", expr: "SELECT * FROM archive LIMIT abc"},
		{name: "limit_negative", expr: "SELECT * FROM archive LIMIT -1"},
		{name: "dangling_comma", expr: "SELECT _1, FROM archive"},
		{name: "dangling_dot", expr: "SELECT s. FROM archive"},
		{name: "as_without_alias", expr: "SELECT _1 AS FROM archive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := glacier.ParseSelectExpression(tt.expr)
			assert.Error(t, err, "expression: %s", tt.expr)
		})
	}
}
