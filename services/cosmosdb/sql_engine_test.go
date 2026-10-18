package cosmosdb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func engineDocs(t *testing.T) []cosmosdb.DocumentInfo {
	t.Helper()

	return docInfos(t,
		`{"id":"1","name":"alice","age":30,"cat":"u","tags":["a","b"],"addr":{"city":"X","zip":"1"},`+
			`"items":[{"sku":"s1","qty":2,"price":1.5},{"sku":"s2","qty":1,"price":10}]}`,
		`{"id":"2","name":"Bob","age":25,"cat":"u","tags":["b"],"addr":{"city":"Y"},"items":[]}`,
		`{"id":"3","name":"carol","age":40,"cat":"v","tags":[],"items":[{"sku":"s1","qty":5,"price":1.5}]}`,
	)
}

func TestExecuteQuery_Engine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		query  string
		want   string
		params []cosmosdb.QueryParameter
	}{
		{name: "count value", query: "SELECT VALUE COUNT(1) FROM c", want: `[3]`},
		{name: "count with where", query: "SELECT VALUE COUNT(1) FROM c WHERE c.age > 26", want: `[2]`},
		{
			name:  "aggregates",
			query: "SELECT COUNT(1) AS n, SUM(c.age) AS s, AVG(c.age) AS a, MIN(c.age) AS mn, MAX(c.age) AS mx FROM c",
			want:  `[{"a":31.666666666666668,"mn":25,"mx":40,"n":3,"s":95}]`,
		},
		{name: "count over empty", query: "SELECT VALUE COUNT(1) FROM c WHERE c.age > 99", want: `[0]`},
		{name: "sum over empty is undefined", query: "SELECT SUM(c.age) AS s FROM c WHERE c.age > 99", want: `[{}]`},
		{
			name:  "group by",
			query: "SELECT c.cat, COUNT(1) AS n FROM c GROUP BY c.cat",
			want:  `[{"cat":"u","n":2},{"cat":"v","n":1}]`,
		},
		{
			name:  "group by with sum",
			query: "SELECT c.cat, SUM(c.age) AS total FROM c GROUP BY c.cat",
			want:  `[{"cat":"u","total":55},{"cat":"v","total":40}]`,
		},
		{
			name:  "join",
			query: "SELECT c.id, i.sku FROM c JOIN i IN c.items",
			want:  `[{"id":"1","sku":"s1"},{"id":"1","sku":"s2"},{"id":"3","sku":"s1"}]`,
		},
		{
			name:  "join with filter and expression",
			query: "SELECT c.id, i.qty * i.price AS total FROM c JOIN i IN c.items WHERE i.qty >= 2",
			want:  `[{"id":"1","total":3},{"id":"3","total":7.5}]`,
		},
		{
			name:  "join count per document",
			query: "SELECT c.id, COUNT(1) AS n FROM c JOIN i IN c.items GROUP BY c.id",
			want:  `[{"id":"1","n":2},{"id":"3","n":1}]`,
		},
		{
			name:  "exists subquery",
			query: "SELECT c.id FROM c WHERE EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = 'a')",
			want:  `[{"id":"1"}]`,
		},
		{
			name:  "array subquery",
			query: "SELECT c.id, ARRAY(SELECT VALUE i.sku FROM i IN c.items) AS skus FROM c",
			want:  `[{"id":"1","skus":["s1","s2"]},{"id":"2","skus":[]},{"id":"3","skus":["s1"]}]`,
		},
		{
			name:  "scalar subquery",
			query: "SELECT c.id, (SELECT VALUE SUM(i.qty) FROM i IN c.items) AS q FROM c",
			want:  `[{"id":"1","q":3},{"id":"2"},{"id":"3","q":5}]`,
		},
		{name: "upper", query: "SELECT VALUE UPPER(c.name) FROM c", want: `["ALICE","BOB","CAROL"]`},
		{name: "lower", query: "SELECT VALUE LOWER(c.name) FROM c WHERE c.id = '2'", want: `["bob"]`},
		{name: "concat", query: "SELECT VALUE CONCAT(c.name, '-', c.id) FROM c WHERE c.id = '1'", want: `["alice-1"]`},
		{name: "contains", query: "SELECT VALUE c.id FROM c WHERE CONTAINS(c.name, 'ar')", want: `["3"]`},
		{
			name:  "contains ignore case",
			query: "SELECT VALUE c.id FROM c WHERE CONTAINS(c.name, 'OB', true)",
			want:  `["2"]`,
		},
		{name: "startswith", query: "SELECT VALUE c.id FROM c WHERE STARTSWITH(c.name, 'b', true)", want: `["2"]`},
		{name: "endswith", query: "SELECT VALUE c.id FROM c WHERE ENDSWITH(c.name, 'l')", want: `["3"]`},
		{name: "length", query: "SELECT VALUE LENGTH(c.name) FROM c", want: `[5,3,5]`},
		{name: "substring", query: "SELECT VALUE SUBSTRING(c.name, 1, 3) FROM c", want: `["lic","ob","aro"]`},
		{name: "replace", query: "SELECT VALUE REPLACE(c.name, 'l', 'L') FROM c WHERE c.id = '1'", want: `["aLice"]`},
		{name: "trim", query: "SELECT VALUE TRIM('  hi ') FROM c WHERE c.id = '1'", want: `["hi"]`},
		{name: "abs", query: "SELECT VALUE ABS(-5) FROM c WHERE c.id = '1'", want: `[5]`},
		{name: "ceiling", query: "SELECT VALUE CEILING(1.2) FROM c WHERE c.id = '1'", want: `[2]`},
		{name: "floor", query: "SELECT VALUE FLOOR(1.8) FROM c WHERE c.id = '1'", want: `[1]`},
		{name: "round", query: "SELECT VALUE ROUND(c.age / 7) FROM c WHERE c.id = '1'", want: `[4]`},
		{
			name: "type checks",
			query: "SELECT VALUE [IS_DEFINED(c.addr.zip), IS_NULL(c.x), IS_NUMBER(c.age), IS_STRING(c.name), " +
				"IS_ARRAY(c.tags), IS_OBJECT(c.addr)] FROM c WHERE c.id = '2'",
			want: `[[false,false,true,true,true,true]]`,
		},
		{name: "array contains", query: "SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS(c.tags, 'a')", want: `["1"]`},
		{name: "array length", query: "SELECT VALUE ARRAY_LENGTH(c.tags) FROM c", want: `[2,1,0]`},
		{name: "array slice", query: "SELECT VALUE ARRAY_SLICE(c.tags, 1) FROM c WHERE c.id = '1'", want: `[["b"]]`},
		{
			name:  "array contains partial object",
			query: "SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS(c.items, {\"sku\": \"s2\"}, true)",
			want:  `["1"]`,
		},
		{
			name:  "datetime add",
			query: "SELECT VALUE DateTimeAdd('day', 1, '2024-01-01T00:00:00.0000000Z') FROM c WHERE c.id = '1'",
			want:  `["2024-01-02T00:00:00.0000000Z"]`,
		},
		{
			name:  "datetime part",
			query: "SELECT VALUE DateTimePart('year', '2024-03-05T00:00:00.0000000Z') FROM c WHERE c.id = '1'",
			want:  `[2024]`,
		},
		{
			name:  "current datetime is a string",
			query: "SELECT VALUE [IS_STRING(GetCurrentDateTime()), IS_NUMBER(GetCurrentTimestamp())] FROM c WHERE c.id = '1'",
			want:  `[[true,true]]`,
		},
		{
			name:  "order by multiple",
			query: "SELECT VALUE c.id FROM c ORDER BY c.cat DESC, c.age ASC",
			want:  `["3","2","1"]`,
		},
		{name: "offset limit", query: "SELECT VALUE c.id FROM c ORDER BY c.age OFFSET 1 LIMIT 1", want: `["1"]`},
		{
			name:  "offset limit params",
			query: "SELECT VALUE c.id FROM c ORDER BY c.age OFFSET @o LIMIT @l",
			params: []cosmosdb.QueryParameter{
				{Name: "@o", Value: json.Number("0")},
				{Name: "@l", Value: json.Number("2")},
			},
			want: `["2","1"]`,
		},
		{name: "distinct value", query: "SELECT DISTINCT VALUE c.cat FROM c", want: `["u","v"]`},
		{name: "top value", query: "SELECT TOP 2 VALUE c.id FROM c", want: `["1","2"]`},
		{name: "in", query: "SELECT VALUE c.id FROM c WHERE c.cat IN ('v', 'w')", want: `["3"]`},
		{name: "not in", query: "SELECT VALUE c.id FROM c WHERE c.cat NOT IN ('v')", want: `["1","2"]`},
		{name: "between", query: "SELECT VALUE c.id FROM c WHERE c.age BETWEEN 26 AND 35", want: `["1"]`},
		{name: "like", query: "SELECT VALUE c.id FROM c WHERE c.name LIKE 'a%'", want: `["1"]`},
		{name: "like single char", query: "SELECT VALUE c.id FROM c WHERE c.name LIKE '_ob'", want: `["2"]`},
		{
			name:  "ternary",
			query: "SELECT VALUE (c.age > 28 ? 'old' : 'young') FROM c",
			want:  `["old","young","old"]`,
		},
		{name: "arithmetic", query: "SELECT VALUE c.age + 1 FROM c WHERE c.id = '1'", want: `[31]`},
		{name: "string concat operator", query: "SELECT VALUE c.name || '!' FROM c WHERE c.id = '2'", want: `["Bob!"]`},
		{
			name:  "object literal",
			query: "SELECT {\"n\": c.name, \"t\": c.tags} AS o FROM c WHERE c.id = '2'",
			want:  `[{"o":{"n":"Bob","t":["b"]}}]`,
		},
		{name: "nested value skips undefined", query: "SELECT VALUE c.addr.city FROM c", want: `["X","Y"]`},
		{
			name:  "bracket access",
			query: "SELECT VALUE [c[\"name\"], c.items[0].sku] FROM c WHERE c.id = '1'",
			want:  `[["alice","s1"]]`,
		},
		{
			name:  "unnamed projections",
			query: "SELECT c.age + 1, c.name, 7 FROM c WHERE c.id = '1'",
			want:  `[{"$1":31,"$2":7,"name":"alice"}]`,
		},
		{name: "coalesce", query: "SELECT VALUE c.addr.zip ?? 'none' FROM c", want: `["1","none","none"]`},
		{
			name:  "not equal across types",
			query: "SELECT VALUE c.id FROM c WHERE c.name != 5 AND c.id = '1'",
			want:  `["1"]`,
		},
		{
			name:  "deep object equality",
			query: "SELECT VALUE c.id FROM c WHERE c.addr = {\"city\": \"Y\"}",
			want:  `["2"]`,
		},
	}

	docs := engineDocs(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, err := cosmosdb.ExecuteQuery(tt.query, tt.params, docs)
			require.NoError(t, err)

			got, err := json.Marshal(rows)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))

			// ORDER BY / array result order matters, which JSONEq preserves for arrays.
		})
	}
}

func TestExecuteQuery_EngineErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "unknown function", query: "SELECT VALUE NOPE(c.id) FROM c"},
		{name: "wrong arity", query: "SELECT VALUE UPPER(c.name, c.name) FROM c"},
		{name: "unknown alias in join source", query: "SELECT c.id FROM c JOIN i IN x.items"},
		{name: "unknown alias in subquery", query: "SELECT c.id FROM c WHERE EXISTS(SELECT VALUE t FROM t IN y.tags)"},
		{name: "missing from", query: "SELECT c.id WHERE c.id = 1"},
		{name: "dangling not", query: "SELECT * FROM c WHERE c.id NOT 3"},
		{name: "unterminated object", query: "SELECT {\"a\": 1 FROM c"},
		{name: "bad escape", query: `SELECT * FROM c WHERE c.id = 'a\q'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := cosmosdb.ExecuteQuery(tt.query, nil, nil)
			require.ErrorIs(t, err, cosmosdb.ErrQueryParse)
		})
	}
}

func TestExecuteQuery_BadLimitParam(t *testing.T) {
	t.Parallel()

	_, err := cosmosdb.ExecuteQuery("SELECT * FROM c OFFSET @o LIMIT 1", nil, engineDocs(t))
	require.ErrorIs(t, err, cosmosdb.ErrQueryParse)
}
