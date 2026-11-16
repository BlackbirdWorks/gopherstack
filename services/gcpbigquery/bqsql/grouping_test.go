package bqsql_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const salesCTE = `WITH s AS (
	SELECT 'east' AS region, 'a' AS prod, 1 AS n UNION ALL
	SELECT 'east', 'b', 2 UNION ALL
	SELECT 'west', 'a', 4 UNION ALL
	SELECT 'west', 'a', 8)
`

func TestGroupingSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			"rollup",
			salesCTE + `SELECT region, prod, SUM(n) AS total FROM s GROUP BY ROLLUP(region, prod)
				ORDER BY region, prod`,
			`[[null,null,"15"],["east",null,"3"],["east","a","1"],["east","b","2"],["west",null,"12"],["west","a","12"]]`,
		},
		{
			"cube",
			salesCTE + `SELECT region, prod, SUM(n) AS total FROM s GROUP BY CUBE(region, prod)
				ORDER BY region, prod`,
			`[[null,null,"15"],[null,"a","13"],[null,"b","2"],["east",null,"3"],["east","a","1"],["east","b","2"],
			["west",null,"12"],["west","a","12"]]`,
		},
		{
			"grouping sets",
			salesCTE + `SELECT region, prod, SUM(n) AS total FROM s GROUP BY GROUPING SETS (region, prod, ())
				ORDER BY region, prod`,
			`[[null,null,"15"],[null,"a","13"],[null,"b","2"],["east",null,"3"],["west",null,"12"]]`,
		},
		{
			"grouping function",
			salesCTE + `SELECT region, GROUPING(region) AS g, SUM(n) AS total FROM s GROUP BY ROLLUP(region)
				ORDER BY g, region`,
			`[["east","0","3"],["west","0","12"],[null,"1","15"]]`,
		},
		{
			"plain plus rollup",
			salesCTE + `SELECT region, prod, SUM(n) AS total FROM s GROUP BY region, ROLLUP(prod)
				ORDER BY region, prod`,
			`[["east",null,"3"],["east","a","1"],["east","b","2"],["west",null,"12"],["west","a","12"]]`,
		},
		{
			"composite rollup element",
			salesCTE + `SELECT region, prod, SUM(n) AS total FROM s GROUP BY ROLLUP((region, prod))
				ORDER BY region, prod`,
			`[[null,null,"15"],["east","a","1"],["east","b","2"],["west","a","12"]]`,
		},
		{
			"having and ordinal",
			salesCTE + `SELECT region, SUM(n) AS total FROM s GROUP BY ROLLUP(1) HAVING SUM(n) > 5 ORDER BY 2`,
			`[["west","12"],[null,"15"]]`,
		},
		{
			"empty input grand total",
			`SELECT x, COUNT(*) AS c FROM UNNEST([1]) AS x WHERE x > 5 GROUP BY ROLLUP(x)`,
			`[[null,"0"]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)

			got, merr := json.Marshal(wireRows(res), jsonx.V1())
			require.NoError(t, merr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestRecursiveWith(t *testing.T) {
	t.Parallel()

	res, err := runSQL(t, newCatalog(t), `WITH RECURSIVE t AS (
		SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT SUM(n) AS s, COUNT(*) AS c FROM t`)
	require.NoError(t, err)

	got, merr := json.Marshal(wireRows(res), jsonx.V1())
	require.NoError(t, merr)
	assert.JSONEq(t, `[["15","5"]]`, string(got))

	_, err = runSQL(t, newCatalog(t), `WITH RECURSIVE t AS (
		SELECT 1 AS n UNION ALL SELECT n + 1 FROM t) SELECT COUNT(*) FROM t`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iteration")
}
