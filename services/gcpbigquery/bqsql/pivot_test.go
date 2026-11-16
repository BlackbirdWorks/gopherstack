package bqsql_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	wideCTE = `WITH produce AS (
		SELECT 'Kale' AS product, 51 AS q1, 23 AS q2, 45 AS q3, 3 AS q4 UNION ALL
		SELECT 'Apple', 77, 0, 25, NULL)
	`
	longCTE = `WITH sales AS (
		SELECT 'Kale' AS product, 2020 AS yr, 51 AS amount, 'Q1' AS quarter UNION ALL
		SELECT 'Kale', 2020, 23, 'Q2' UNION ALL
		SELECT 'Apple', 2020, 77, 'Q1' UNION ALL
		SELECT 'Apple', 2020, 5, 'Q1' UNION ALL
		SELECT 'Apple', 2021, 8, 'Q2')
	`
)

func TestPivotUnpivot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			"unpivot",
			wideCTE + `SELECT * FROM produce UNPIVOT(sales FOR quarter IN (q1, q2, q3, q4)) ORDER BY product, quarter`,
			`[["Apple","77","q1"],["Apple","0","q2"],["Apple","25","q3"],["Kale","51","q1"],["Kale","23","q2"],
			["Kale","45","q3"],["Kale","3","q4"]]`,
		},
		{
			"unpivot include nulls",
			wideCTE + `SELECT * FROM produce UNPIVOT INCLUDE NULLS (sales FOR quarter IN (q3, q4)) WHERE product = 'Apple'
				ORDER BY quarter`,
			`[["Apple","77","0","25","q3"],["Apple","77","0",null,"q4"]]`,
		},
		{
			"unpivot multi column",
			wideCTE + `SELECT * FROM produce UNPIVOT((first_half, second_half) FOR semesters IN
				((q1, q2) AS 'semester_1', (q3, q4) AS 'semester_2')) ORDER BY product, semesters`,
			`[["Apple","77","0","semester_1"],["Apple","25",null,"semester_2"],["Kale","51","23","semester_1"],
			["Kale","45","3","semester_2"]]`,
		},
		{
			"unpivot int alias",
			wideCTE + `SELECT product, n, v FROM produce UNPIVOT(v FOR n IN (q1 AS 1, q2 AS 2)) WHERE product = 'Kale'
				ORDER BY n`,
			`[["Kale","1","51"],["Kale","2","23"]]`,
		},
		{
			"pivot",
			longCTE + `SELECT * FROM (SELECT product, amount, quarter FROM sales)
				PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3')) ORDER BY product`,
			`[["Apple","82","8",null],["Kale","51","23",null]]`,
		},
		{
			"pivot groups other columns",
			longCTE + `SELECT * FROM sales PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2')) ORDER BY product, yr`,
			`[["Apple","2020","82",null],["Apple","2021",null,"8"],["Kale","2020","51","23"]]`,
		},
		{
			"pivot alias and prefix",
			longCTE + `SELECT * FROM (SELECT product, amount, quarter FROM sales)
				PIVOT(SUM(amount) AS total, COUNT(*) AS n FOR quarter IN ('Q1' AS first, 'Q2')) AS p ORDER BY product`,
			`[["Apple","82","2","8","1"],["Kale","51","1","23","1"]]`,
		},
		{
			"pivot integer value name",
			`SELECT * FROM (SELECT 'a' AS k, 1 AS code, 5 AS v UNION ALL SELECT 'a', 2, 7)
				PIVOT(SUM(v) FOR code IN (1, 2))`,
			`[["a","5","7"]]`,
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

func TestPivotColumnNames(t *testing.T) {
	t.Parallel()

	res, err := runSQL(t, newCatalog(t), longCTE+`SELECT * FROM (SELECT product, amount, quarter FROM sales)
		PIVOT(SUM(amount) AS total FOR quarter IN ('Q1', 'Q2' AS second))`)
	require.NoError(t, err)

	names := make([]string, len(res.Fields))
	for i, f := range res.Fields {
		names[i] = f.Name
	}

	assert.Equal(t, []string{"product", "total_Q1", "total_second"}, names)

	_, err = runSQL(t, newCatalog(t), longCTE+`SELECT * FROM sales PIVOT(SUM(amount), COUNT(*) FOR quarter IN ('Q1'))`)
	require.Error(t, err)
	assert.Equal(t, bqsql.ErrInvalid, bqsql.AsError(err).Kind)
}
