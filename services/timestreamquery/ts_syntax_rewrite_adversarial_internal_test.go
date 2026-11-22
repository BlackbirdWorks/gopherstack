package timestreamquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeQueryLeavesLookalikesAlone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		q    string
	}{
		{"position in string", `SELECT 'position(a IN b)'`},
		{"extract in string", `SELECT 'extract(dow FROM t)'`},
		{"in keyword string", `SELECT 'x IN y'`},
		{"quantifier in string", `SELECT 'a = ANY (SELECT 1)'`},
		{"values in string", `SELECT 'VALUES 1, 2'`},
		{"values quoted identifier", `SELECT "values", "all", "position" FROM t`},
		{"column named position", `SELECT position FROM t WHERE position > 1`},
		{"column named all", `SELECT "all" FROM t WHERE "any" = 1`},
		{"column named values", `SELECT values FROM t`},
		{"regexp lambda in string", `SELECT 'regexp_replace(a, b, x -> x)'`},
		{"escaped quote", `SELECT 'it''s position(a IN b)'`},
		{"position one arg", `SELECT position(a) FROM t`},
		{"extract plain field", `SELECT extract(year FROM time) FROM t`},
		{"in list", `SELECT a IN (1, 2) FROM t`},
		{"unterminated", `SELECT position(a IN b`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.q, normalizeQuery(tt.q))
		})
	}
}

func TestNormalizeQueryRewrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		q    string
		want string
	}{
		{"position", `SELECT position('a' IN b)`, `SELECT strpos(b, 'a')`},
		{"position newline", "SELECT position(\n'a'\nIN\nb)", "SELECT strpos(b, 'a')"},
		{
			"position nested paren",
			`SELECT position('a' IN concat(b, 'in', c))`,
			`SELECT strpos(concat(b, 'in', c), 'a')`,
		},
		{"position string with in", `SELECT position('x in y' IN b)`, `SELECT strpos(b, 'x in y')`},
		{"extract dow", `SELECT extract(DOW FROM t)`, `SELECT day_of_week(t)`},
		{"values bare", `SELECT * FROM (VALUES 1, 2) AS t(c)`, `SELECT * FROM (VALUES (1), (2)) AS t(c)`},
		{"comment keyword", "SELECT 1 -- position(a IN b)", "SELECT 1"},
		{"not in values", `SELECT 1 WHERE a NOT IN (VALUES 1, 2)`, `SELECT 1 WHERE a NOT IN (VALUES (1), (2))`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, normalizeQuery(tt.q))
		})
	}
}
