package bqsql_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

type fakeCatalog struct {
	tables map[string]*bqsql.TableData
}

func (f *fakeCatalog) Table(ref bqsql.TableRef) (*bqsql.TableData, bool) {
	t, ok := f.tables[ref.Dataset+"."+ref.Table]

	return t, ok
}

func (f *fakeCatalog) DatasetExists(_, ds string) bool { return ds == "ds" }

func fieldsJSON(t *testing.T, s string) []bqsql.Field {
	t.Helper()

	var fs []bqsql.Field

	require.NoError(t, json.Unmarshal([]byte(s), &fs))
	require.NoError(t, bqsql.NormalizeSchema(fs))

	return fs
}

func rowsJSON(t *testing.T, fs []bqsql.Field, s string) [][]any {
	t.Helper()

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()

	var raw []map[string]any

	require.NoError(t, dec.Decode(&raw))

	out := make([][]any, len(raw))

	for i, r := range raw {
		row, err := bqsql.ParseRecord(fs, r, bqsql.ParseOptions{})
		require.NoError(t, err)

		out[i] = row
	}

	return out
}

func newCatalog(t *testing.T) *fakeCatalog {
	t.Helper()

	people := fieldsJSON(t, `[
		{"name":"id","type":"INTEGER","mode":"REQUIRED"},
		{"name":"name","type":"STRING"},
		{"name":"score","type":"FLOAT"},
		{"name":"active","type":"BOOLEAN"},
		{"name":"born","type":"DATE"},
		{"name":"seen","type":"TIMESTAMP"},
		{"name":"tags","type":"STRING","mode":"REPEATED"},
		{"name":"addr","type":"RECORD","fields":[{"name":"city","type":"STRING"},{"name":"zip","type":"INTEGER"}]},
		{"name":"orders","type":"RECORD","mode":"REPEATED","fields":[
			{"name":"sku","type":"STRING"},{"name":"qty","type":"INTEGER"}]}
	]`)

	rows := rowsJSON(t, people, `[
		{"id":1,"name":"ann","score":1.5,"active":true,"born":"1990-05-17","seen":"2024-01-02 03:04:05 UTC","tags":["a","b"],
		 "addr":{"city":"Paris","zip":75001},"orders":[{"sku":"x","qty":2},{"sku":"y","qty":3}]},
		{"id":2,"name":"bob","score":2.5,"active":false,"born":"1985-12-01","seen":"2024-02-03T00:00:00Z","tags":[],
		 "addr":{"city":"Rome","zip":100},"orders":[]},
		{"id":3,"name":null,"score":null,"active":null,"born":null,"seen":null,"tags":["c"],"addr":null,
		 "orders":[{"sku":"z","qty":5}]}
	]`)

	return &fakeCatalog{tables: map[string]*bqsql.TableData{
		"ds.people": {Ref: bqsql.TableRef{Project: "p", Dataset: "ds", Table: "people"}, Fields: people, Rows: rows},
	}}
}

func runSQL(t *testing.T, cat *fakeCatalog, sql string, params ...bqsql.Param) (*bqsql.Result, error) {
	t.Helper()

	st, err := bqsql.ParseStatement(sql)
	if err != nil {
		return nil, err
	}

	return bqsql.Run(t.Context(), st, &bqsql.Input{Catalog: cat, Project: "p", Dataset: "ds", Params: params}, false)
}

func wireRows(res *bqsql.Result) [][]any {
	out := make([][]any, len(res.Rows))

	for i, r := range res.Rows {
		cells := bqsql.Row(res.Fields, r)["f"].([]map[string]any)
		row := make([]any, len(cells))

		for j, c := range cells {
			row[j] = c["v"]
		}

		out[i] = row
	}

	return out
}

func TestSelectQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"count", "SELECT COUNT(*) AS n FROM people", `[["3"]]`},
		{
			"filter order",
			"SELECT id, name FROM `p.ds.people` WHERE id > 1 ORDER BY id DESC",
			`[["3",null],["2","bob"]]`,
		},
		{"nested field", "SELECT addr.city FROM people WHERE id = 1", `[["Paris"]]`},
		{"unnest", "SELECT id, t FROM people, UNNEST(tags) AS t ORDER BY id, t", `[["1","a"],["1","b"],["3","c"]]`},
		{
			"unnest offset",
			"SELECT t, o FROM people, UNNEST(tags) AS t WITH OFFSET AS o WHERE id = 1 ORDER BY o",
			`[["a","0"],["b","1"]]`,
		},
		{
			"unnest struct",
			"SELECT o.sku, o.qty FROM people p, UNNEST(p.orders) o ORDER BY o.sku",
			`[["x","2"],["y","3"],["z","5"]]`,
		},
		{
			"except",
			"SELECT * EXCEPT (tags, addr, orders, seen, born, active, score) FROM people WHERE id = 2",
			`[["2","bob"]]`,
		},
		{
			"replace",
			"SELECT * EXCEPT (tags, addr, orders, seen, born, active, score) " +
				"REPLACE (UPPER(name) AS name) FROM people WHERE id = 1",
			`[["1","ANN"]]`,
		},
		{"safe divide", "SELECT SAFE_DIVIDE(1, 0), SAFE_DIVIDE(6, 3)", `[[null,"2.0"]]`},
		{
			"group",
			"SELECT active, COUNT(*) c FROM people GROUP BY active ORDER BY c DESC, active",
			`[[null,"1"],["false","1"],["true","1"]]`,
		},
		{
			"array agg",
			"SELECT ARRAY_AGG(name IGNORE NULLS ORDER BY name) AS names FROM people",
			`[[[{"v":"ann"},{"v":"bob"}]]]`,
		},
		{"struct literal", "SELECT STRUCT(1 AS a, 'x' AS b) AS s", `[[{"f":[{"v":"1"},{"v":"x"}]}]]`},
		{"array literal", "SELECT [1, 2, 3] AS a, ARRAY_LENGTH([1,2,3])", `[[[{"v":"1"},{"v":"2"},{"v":"3"}],"3"]]`},
		{
			"date funcs",
			"SELECT DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH), " +
				"DATE_DIFF(DATE '2024-03-01', DATE '2024-01-01', MONTH), EXTRACT(YEAR FROM born) FROM people WHERE id = 1",
			`[["2024-02-29","2","1990"]]`,
		},
		{"timestamp", "SELECT TIMESTAMP_ADD(seen, INTERVAL 1 HOUR) FROM people WHERE id = 1", `[["1.704168245E9"]]`},
		{
			"case",
			"SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END FROM people ORDER BY id LIMIT 2",
			`[["one"],["other"]]`,
		},
		{
			"window",
			"SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) rn FROM people ORDER BY id",
			`[["1","3"],["2","2"],["3","1"]]`,
		},
		{"cte", "WITH x AS (SELECT id FROM people) SELECT SUM(id) FROM x", `[["6"]]`},
		{"union", "SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY a DESC", `[["3"],["2"],["1"]]`},
		{
			"join",
			"SELECT a.id, b.id FROM people a JOIN people b ON a.id + 1 = b.id ORDER BY a.id",
			`[["1","2"],["2","3"]]`,
		},
		{"in unnest", "SELECT id FROM people WHERE 'c' IN UNNEST(tags)", `[["3"]]`},
		{
			"string funcs",
			"SELECT CONCAT('a','b'), SUBSTR('hello', 2, 3), REGEXP_CONTAINS('abc', r'b+'), SPLIT('a,b', ',')",
			`[["ab","ell","true",[{"v":"a"},{"v":"b"}]]]`,
		},
		{
			"cast",
			"SELECT CAST('12' AS INT64), SAFE_CAST('x' AS INT64), CAST(1 AS STRING), CAST(TRUE AS STRING)",
			`[["12",null,"1","true"]]`,
		},
		{"like", "SELECT name FROM people WHERE name LIKE 'a%'", `[["ann"]]`},
		{"typed compare", "SELECT id FROM people WHERE born > '1988-01-01'", `[["1"]]`},
		{"qualify", "SELECT id FROM people QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1", `[["1"]]`},
		{"as struct", "SELECT AS STRUCT id, name FROM people WHERE id = 1", `[[{"f":[{"v":"1"},{"v":"ann"}]}]]`},
		{
			"array subquery",
			"SELECT ARRAY(SELECT id FROM people ORDER BY id) AS ids",
			`[[[{"v":"1"},{"v":"2"},{"v":"3"}]]]`,
		},
		{"index", "SELECT tags[OFFSET(1)], tags[SAFE_OFFSET(5)] FROM people WHERE id = 1", `[["b",null]]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)

			if tt.want == "" {
				return
			}

			got, merr := json.Marshal(wireRows(res))
			require.NoError(t, merr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestQueryErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		msg  string
		kind bqsql.ErrKind
	}{
		{"missing table", "SELECT * FROM nope", "Not found: Table p:ds.nope", bqsql.ErrNotFound},
		{"unknown column", "SELECT zzz FROM people", "Unrecognized name: zzz", bqsql.ErrInvalid},
		{"division by zero", "SELECT 1 / 0", "division by zero", bqsql.ErrRuntime},
		{"syntax", "SELEC 1", "Syntax error", bqsql.ErrInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), tt.sql)
			require.Error(t, err)

			e := bqsql.AsError(err)
			assert.Equal(t, tt.kind, e.Kind)
			assert.Contains(t, e.Message, tt.msg)
		})
	}
}

func TestParams(t *testing.T) {
	t.Parallel()

	named := []bqsql.Param{{Name: "min", Field: bqsql.Field{Type: "INTEGER"}, Value: "2"}}
	res, err := runSQL(t, newCatalog(t), "SELECT id FROM people WHERE id >= @min ORDER BY id", named...)
	require.NoError(t, err)
	assert.Len(t, res.Rows, 2)

	pos := []bqsql.Param{{Field: bqsql.Field{Type: "STRING"}, Value: "bob"}}
	res, err = runSQL(t, newCatalog(t), "SELECT id FROM people WHERE name = ?", pos...)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{"2"}}, res.Rows)

	arr := []bqsql.Param{{Name: "ids", Field: bqsql.Field{Type: "INTEGER", Mode: "REPEATED"}, Value: []any{"1", "3"}}}
	res, err = runSQL(t, newCatalog(t), "SELECT id FROM people WHERE id IN UNNEST(@ids) ORDER BY id", arr...)
	require.NoError(t, err)
	assert.Equal(t, [][]any{{"1"}, {"3"}}, res.Rows)
}

func TestDML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		affected int64
		rows     int
	}{
		{"insert values", "INSERT INTO people (id, name) VALUES (4, 'dan'), (5, 'eve')", 2, 5},
		{"insert select", "INSERT INTO people (id, name) SELECT id + 10, name FROM people", 3, 6},
		{"update", "UPDATE people SET name = 'x', score = score * 2 WHERE id <= 2", 2, 3},
		{"update struct field", "UPDATE people SET addr.city = 'Oslo' WHERE id = 1", 1, 3},
		{"delete", "DELETE FROM people WHERE id = 1", 1, 2},
		{"truncate", "TRUNCATE TABLE people", 3, 0},
		{"merge", `MERGE people T USING (SELECT 2 AS id, 'bobby' AS name UNION ALL SELECT 9, 'new') S ON T.id = S.id
			WHEN MATCHED THEN UPDATE SET name = S.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (S.id, S.name)`, 2, 4},
		{"merge delete", `MERGE people T USING (SELECT 1 AS id) S ON T.id = S.id WHEN MATCHED THEN DELETE`, 1, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.affected, res.Affected)
			require.Len(t, res.Effects, 1)
			assert.Len(t, res.Effects[0].Rows, tt.rows)
		})
	}
}

func TestDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		op   string
		rows int
	}{
		{
			"create table",
			"CREATE TABLE ds.t1 (a INT64 NOT NULL, b ARRAY<STRING>, c STRUCT<x INT64, y STRING>)",
			bqsql.OpCreateTable,
			0,
		},
		{"ctas", "CREATE TABLE ds.t2 AS SELECT id, name FROM people", bqsql.OpCreateTable, 3},
		{"view", "CREATE VIEW ds.v1 AS SELECT id FROM people", bqsql.OpCreateView, 0},
		{"drop", "DROP TABLE people", bqsql.OpDropTable, 0},
		{"schema", "CREATE SCHEMA newds", bqsql.OpCreateSchema, 0},
		{"alter add", "ALTER TABLE people ADD COLUMN extra STRING", bqsql.OpAlter, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)
			require.Len(t, res.Effects, 1)
			assert.Equal(t, tt.op, res.Effects[0].Op)
			assert.Len(t, res.Effects[0].Rows, tt.rows)
		})
	}
}
