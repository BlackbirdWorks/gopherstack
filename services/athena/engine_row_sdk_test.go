package athena_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const castXY = "CAST(ROW(1, 'a') AS ROW(x int, y varchar))"

func TestSDK_RowValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		typ  string
		want string
	}{
		{"anonymous constructor", "SELECT ROW(1, 'a')", "row", "[1, a]"},
		{"anonymous with null", "SELECT ROW(NULL, 2.5)", "row", "[null, 2.5]"},
		{"named by cast", "SELECT " + castXY, "row", "{x=1, y=a}"},
		{
			"field order is declaration order",
			"SELECT CAST(ROW(1, 2) AS ROW(zed int, alpha int))",
			"row",
			"{zed=1, alpha=2}",
		},
		{"cast converts field types", "SELECT CAST(ROW(1.5, 2) AS ROW(a bigint, b varchar))", "row", "{a=2, b=2}"},
		{
			"nested row",
			"SELECT CAST(ROW(1, ROW(2, 3)) AS ROW(a int, b ROW(c int, d int)))",
			"row",
			"{a=1, b={c=2, d=3}}",
		},
		{"field by name on cast", "SELECT " + castXY + ".y", "varchar", "a"},
		{"field by name case insensitive", "SELECT " + castXY + ".X", "bigint", "1"},
		{"field through subquery", "SELECT r.y FROM (SELECT " + castXY + " AS r)", "varchar", "a"},
		{"nested field", "SELECT r.b.d FROM (SELECT CAST(ROW(1, ROW(2, 3)) AS ROW(a int, b ROW(c int, d int))) AS r)",
			"bigint", "3"},
		{"subscript", "SELECT ROW(1, 'a')[2]", "varchar", "a"},
		{"subscript of named", "SELECT " + castXY + "[1]", "bigint", "1"},
		{"row in array", "SELECT ARRAY[ROW(1, 'a'), ROW(2, 'b')]", "array", "[[1, a], [2, b]]"},
		{"row in map", "SELECT MAP(ARRAY['k'], ARRAY[CAST(ROW(1, 2) AS ROW(a int, b int))])", "map", "{k={a=1, b=2}}"},
		{"cast row array", "SELECT CAST(ARRAY[ROW(1, 'a')] AS ARRAY(ROW(n int, s varchar)))", "array", "[{n=1, s=a}]"},
		{"equal rows", "SELECT ROW(1, 'a') = ROW(1, 'a')", "boolean", "true"},
		{"different rows", "SELECT ROW(1, 'a') = ROW(1, 'b')", "boolean", "false"},
		{"row in cte", "WITH t AS (SELECT " + castXY + " AS r) SELECT r.x + 1 FROM t", "bigint", "2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, [][]string{{tc.want}}, got.rows)
			assert.Equal(t, []string{tc.typ}, got.types)
		})
	}
}

func TestSDK_RowFailures(t *testing.T) {
	t.Parallel()

	cases := []struct{ name, sql, want string }{
		{
			"field count mismatch",
			"SELECT CAST(ROW(1, 2) AS ROW(a int))",
			"Cannot cast ROW with 2 fields to ROW with 1 fields",
		},
		{"subscript range", "SELECT ROW(1, 2)[3]", "ROW subscript out of range"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, sdkFailure(t, docsClient(t), tc.sql), tc.want)
		})
	}
}

func TestSDK_RowFromStructColumn(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"field", "SELECT s.name FROM docs ORDER BY id", [][]string{{"ann"}, {"bob"}}},
		{
			"cast to ordered row",
			"SELECT CAST(s AS ROW(name varchar, age int)) FROM docs ORDER BY id",
			[][]string{{"{name=ann, age=30}"}, {"{name=bob, age=25}"}},
		},
		{"row of columns", "SELECT ROW(id, s.name) FROM docs ORDER BY id", [][]string{{"[1, ann]"}, {"[2, bob]"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
		})
	}
}

func TestSDK_RowCTASWritesNestedJSON(t *testing.T) {
	t.Parallel()

	b, store := newEngineBackend(t, map[string][]byte{"lake/docs/a.json": []byte(docsData)},
		map[string]*athena.GlueTableDef{"shop/docs": docsDef()})
	client := newTestAthenaClient(t, athena.NewHandler(b))

	sdkRun(t, client, "CREATE DATABASE shop")
	sdkRun(t, client, "CREATE TABLE shop.out WITH (format = 'JSON', external_location = 's3://lake/out/') AS "+
		"SELECT id, CAST(ROW(id, s.name) AS ROW(n int, who varchar)) AS r, ROW(id, 'z') AS anon FROM docs WHERE id = 1")

	store.mu.Lock()
	defer store.mu.Unlock()

	var body strings.Builder

	for k, v := range store.objects {
		if strings.HasPrefix(k, "lake/out/") {
			body.Write(v)
		}
	}

	require.NotContains(t, body.String(), `\u0000`)
	assert.Contains(t, body.String(), `"r":{"n":1,"who":"ann"}`)
	assert.Contains(t, body.String(), `"anon":[1,"z"]`)
}
