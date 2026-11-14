package timestreamquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery_BooleanExpressionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantRows  [][]string
		wantTypes []string
	}{
		{
			"comparison", `SELECT 1 < 2, 2 <= 1`, [][]string{{"true", "false"}},
			[]string{"_col0:BOOLEAN", "_col1:BOOLEAN"},
		},
		{"and or not", `SELECT 1 = 1 AND 2 = 3 AS a, NOT (1 = 1) AS n, 1 = 2 OR 3 = 3 AS o`,
			[][]string{{"false", "false", "true"}}, []string{"a:BOOLEAN", "n:BOOLEAN", "o:BOOLEAN"}},
		{"is null", `SELECT NULL IS NULL AS x, 1 IS NOT NULL AS y`, [][]string{{"true", "true"}},
			[]string{"x:BOOLEAN", "y:BOOLEAN"}},
		{"like between in", `SELECT 'abc' LIKE 'a%' AS l, 5 BETWEEN 1 AND 9 AS b, 3 IN (1, 2) AS i`,
			[][]string{{"true", "true", "false"}}, []string{"l:BOOLEAN", "b:BOOLEAN", "i:BOOLEAN"}},
		{"literals", `SELECT true, false AS f`, [][]string{{"true", "false"}}, []string{"_col0:BOOLEAN", "f:BOOLEAN"}},
		{"functions", `SELECT contains(ARRAY[1,2], 2) AS c, regexp_like('abc', 'b') AS r`,
			[][]string{{"true", "true"}}, []string{"c:BOOLEAN", "r:BOOLEAN"}},
		{"column filter",
			`SELECT measure_value::double > 5 AS big FROM db.t ` +
				`WHERE measure_name = 'cpu' AND time > ago(1h) ORDER BY time`,
			[][]string{{"true"}, {"true"}}, []string{"big:BOOLEAN"}},
		{
			"cte alias propagates", `WITH x AS (SELECT 1 < 2 AS ok) SELECT ok FROM x`,
			[][]string{{"true"}}, []string{"ok:BOOLEAN"},
		},
		{"empty result", `SELECT measure_value::double > 5 AS big FROM db.t WHERE measure_name = 'nope'`,
			[][]string{}, []string{"big:BOOLEAN"}},
		{"arithmetic stays bigint", `SELECT 1 + 2 AS s, CASE WHEN 1 < 2 THEN 1 ELSE 0 END AS c`,
			[][]string{{"3", "1"}}, []string{"s:BIGINT", "c:BIGINT"}},
		{"string literal with operator", `SELECT 'a > b' AS s`, [][]string{{"a > b"}}, []string{"s:VARCHAR"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, types := queryRows(t, tt.query)
			assert.Equal(t, tt.wantRows, rows)
			assert.Equal(t, tt.wantTypes, types)
		})
	}
}
