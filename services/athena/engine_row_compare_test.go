package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSDK_RowComparisons(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"less numeric fieldwise", "SELECT ROW(2, 'a') < ROW(10, 'a')", [][]string{{"true"}}},
		{"greater later field", "SELECT ROW(1, 'b') > ROW(1, 'a')", [][]string{{"true"}}},
		{"less equal", "SELECT ROW(1, 2) <= ROW(1, 2)", [][]string{{"true"}}},
		{"not equal", "SELECT ROW(1, 2) <> ROW(1, 3)", [][]string{{"true"}}},
		{
			"names ignored",
			"SELECT CAST(ROW(1, 2) AS ROW(a int, b int)) = CAST(ROW(1, 2) AS ROW(x int, y int))",
			[][]string{{"true"}},
		},
		{"named vs anonymous", "SELECT CAST(ROW(1, 'z') AS ROW(a int, b varchar)) = ROW(1, 'z')", [][]string{{"true"}}},
		{
			"column vs constructor",
			"SELECT count(*) FROM docs WHERE s = CAST(ROW('ann', 30) AS ROW(name varchar, age int))",
			[][]string{{"1"}},
		},
		{"column ordering", "SELECT id FROM docs WHERE s > ROW('ann', 99) ORDER BY id", [][]string{{"2"}}},
		{"order by column", "SELECT id FROM docs ORDER BY s DESC", [][]string{{"2"}, {"1"}}},
		{"order by constructor numeric", "SELECT id FROM docs ORDER BY ROW(10 - id, 'k')", [][]string{{"2"}, {"1"}}},
		{"max column", "SELECT max(s) FROM docs", [][]string{{"{name=bob, age=25}"}}},
		{"min column", "SELECT min(s) FROM docs", [][]string{{"{name=ann, age=30}"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, sdkRun(t, docsClient(t), tc.sql).rows)
		})
	}
}
