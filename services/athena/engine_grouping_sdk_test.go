package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSDK_GroupingSetsComposition(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		sql   string
		cols  []string
		types []string
		want  [][]string
	}{
		{
			name: "union all with plain select",
			sql: "SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) " +
				"UNION ALL SELECT 'x', 99 ORDER BY 2, 1",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"books", "2"}, {"games", "2"}, {"", "4"}, {"x", "99"}},
		},
		{
			name: "union of rollup and cube",
			sql: "SELECT category FROM sales GROUP BY ROLLUP (category) UNION " +
				"SELECT category FROM sales GROUP BY CUBE (category) ORDER BY 1",
			cols:  []string{"category"},
			types: []string{"varchar"},
			want:  [][]string{{"books"}, {"games"}, {""}},
		},
		{
			name: "except plain group",
			sql: "SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) " +
				"EXCEPT SELECT category, count(*) FROM sales GROUP BY category ORDER BY 1",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"", "4"}},
		},
		{
			name: "grouping sets in the second operand",
			sql: "SELECT 'x' AS category, 1 AS n UNION ALL SELECT category, count(*) FROM sales " +
				"GROUP BY GROUPING SETS ((category), ()) ORDER BY 2, 1",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"x", "1"}, {"books", "2"}, {"games", "2"}, {"", "4"}},
		},
		{
			name: "with clause and set operation",
			sql: "WITH s AS (SELECT * FROM sales WHERE amount > 5) " +
				"SELECT category, count(*) AS n FROM s GROUP BY ROLLUP (category) " +
				"UNION ALL SELECT 'z', 0 ORDER BY 2, 1",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"z", "0"}, {"games", "1"}, {"books", "2"}, {"", "3"}},
		},
		{
			name:  "order by aggregate outside the select list",
			sql:   "SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) ORDER BY sum(amount) DESC",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"", "4"}, {"games", "2"}, {"books", "2"}},
		},
		{
			name: "order by grouping function with limit",
			sql: "SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) " +
				"ORDER BY grouping(category), category DESC LIMIT 2",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"games", "2"}, {"books", "2"}},
		},
		{
			name: "order by mixes alias and hidden key",
			sql: "SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) " +
				"ORDER BY n, max(amount) DESC",
			cols:  []string{"category", "n"},
			types: []string{"varchar", "bigint"},
			want:  [][]string{{"games", "2"}, {"books", "2"}, {"", "4"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, salesClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
			assert.Equal(t, tc.cols, got.cols)
			assert.Equal(t, tc.types, got.types)
		})
	}
}
