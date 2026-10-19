package athena_test

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const serdeJSON = "org.openx.data.jsonserde.JsonSerDe"

const docsData = `{"id":1,"nums":[1,2,3],"m":{"a":1,"b":2},"s":{"name":"ann","age":30},"tags":["x","y"]}
{"id":2,"nums":[4,5],"m":{"a":10},"s":{"name":"bob","age":25},"tags":[]}
`

func docsDef() *athena.GlueTableDef {
	return &athena.GlueTableDef{
		Name: "docs", Location: "s3://lake/docs/", SerdeLibrary: serdeJSON,
		Columns: []athena.Column{
			{Name: "id", Type: "int"}, {Name: "nums", Type: "array<int>"},
			{Name: "m", Type: "map<string,int>"}, {Name: "s", Type: "struct<name:string,age:int>"},
			{Name: "tags", Type: "array<string>"},
		},
	}
}

type sdkResult struct {
	cols  []string
	types []string
	rows  [][]string
}

// sdkRun drives the official athena client: StartQueryExecution then GetQueryResults.
func sdkRun(t *testing.T, client *athenasdk.Client, query string) sdkResult {
	t.Helper()

	ctx := t.Context()

	start, err := client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		WorkGroup:             aws.String("primary"),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{Database: aws.String("shop")},
		ResultConfiguration:   &athenatypes.ResultConfiguration{OutputLocation: aws.String("s3://out/res/")},
	})
	require.NoError(t, err)

	var state athenatypes.QueryExecutionState

	reason := ""

	require.Eventually(t, func() bool {
		got, gerr := client.GetQueryExecution(
			ctx,
			&athenasdk.GetQueryExecutionInput{QueryExecutionId: start.QueryExecutionId},
		)
		require.NoError(t, gerr)

		state = got.QueryExecution.Status.State
		reason = aws.ToString(got.QueryExecution.Status.StateChangeReason)

		return state != athenatypes.QueryExecutionStateQueued && state != athenatypes.QueryExecutionStateRunning
	}, 10*time.Second, time.Millisecond)
	require.Equal(t, athenatypes.QueryExecutionStateSucceeded, state, reason)

	var res sdkResult

	var token *string

	for first := true; ; first = false {
		page, perr := client.GetQueryResults(ctx, &athenasdk.GetQueryResultsInput{
			QueryExecutionId: start.QueryExecutionId, NextToken: token, MaxResults: aws.Int32(1000),
		})
		require.NoError(t, perr)

		rows := page.ResultSet.Rows

		if first {
			for _, ci := range page.ResultSet.ResultSetMetadata.ColumnInfo {
				res.cols = append(res.cols, aws.ToString(ci.Name))
				res.types = append(res.types, aws.ToString(ci.Type))
			}

			if len(rows) > 0 {
				rows = rows[1:]
			}
		}

		for _, r := range rows {
			cells := make([]string, len(r.Data))
			for i, d := range r.Data {
				cells[i] = aws.ToString(d.VarCharValue)
			}

			res.rows = append(res.rows, cells)
		}

		if token = page.NextToken; token == nil {
			return res
		}
	}
}

func sdkClientFor(t *testing.T, objects map[string][]byte, defs map[string]*athena.GlueTableDef) *athenasdk.Client {
	t.Helper()

	b, _ := newEngineBackend(t, objects, defs)

	return newTestAthenaClient(t, athena.NewHandler(b))
}

func docsClient(t *testing.T) *athenasdk.Client {
	t.Helper()

	return sdkClientFor(t, map[string][]byte{"lake/docs/a.json": []byte(docsData)},
		map[string]*athena.GlueTableDef{"shop/docs": docsDef()})
}

func TestSDK_TrinoSubscriptAndStructAccess(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"array subscript", "SELECT nums[1], nums[3] FROM docs WHERE id = 1", [][]string{{"1", "3"}}},
		{"array literal subscript", "SELECT ARRAY[10, 20, 30][2]", [][]string{{"20"}}},
		{"map subscript", "SELECT m['a'], m['b'] FROM docs WHERE id = 1", [][]string{{"1", "2"}}},
		{"element_at map missing", "SELECT element_at(m, 'zz') FROM docs WHERE id = 1", [][]string{{""}}},
		{"element_at negative", "SELECT element_at(nums, -1) FROM docs ORDER BY id", [][]string{{"3"}, {"5"}}},
		{"nested subscript", "SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]][2][1]", [][]string{{"3"}}},
		{"subscript expression", "SELECT nums[1 + 1] FROM docs WHERE id = 2", [][]string{{"5"}}},
		{"struct field", "SELECT s.name, s.age FROM docs ORDER BY id", [][]string{{"ann", "30"}, {"bob", "25"}}},
		{"struct field qualified", "SELECT d.s.name FROM docs d WHERE d.s.age > 26", [][]string{{"ann"}}},
		{"struct field case", "SELECT S.NAME FROM docs WHERE id = 2", [][]string{{"bob"}}},
		{"struct field in where", "SELECT id FROM docs WHERE s.name = 'bob'", [][]string{{"2"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
		})
	}
}

func TestSDK_TrinoSubscriptErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, sql, reason string
	}{
		{
			"out of bounds",
			"SELECT nums[9] FROM docs",
			"Array subscript must be less than or equal to array length: 9 > 3",
		},
		{"zero index", "SELECT nums[0] FROM docs", "SQL array indices start at 1"},
		{"missing key", "SELECT m['zz'] FROM docs", "Key not present in map: zz"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reason := sdkFailure(t, docsClient(t), tc.sql)
			assert.Contains(t, reason, "INVALID_FUNCTION_ARGUMENT")
			assert.Contains(t, reason, tc.reason)
		})
	}
}

func TestSDK_TrinoLambdas(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{
			"transform",
			"SELECT transform(nums, x -> x * 2) FROM docs ORDER BY id",
			[][]string{{"[2, 4, 6]"}, {"[8, 10]"}},
		},
		{"transform literal", "SELECT transform(ARRAY['a', 'b'], s -> upper(s))", [][]string{{"[A, B]"}}},
		{"filter", "SELECT filter(nums, x -> x % 2 = 1) FROM docs ORDER BY id", [][]string{{"[1, 3]"}, {"[5]"}}},
		{
			"filter correlated",
			"SELECT filter(nums, x -> x > id + 1) FROM docs ORDER BY id",
			[][]string{{"[3]"}, {"[4, 5]"}},
		},
		{
			"reduce sum",
			"SELECT reduce(nums, 0, (s, x) -> s + x, s -> s) FROM docs ORDER BY id",
			[][]string{{"6"}, {"9"}},
		},
		{
			"reduce with finisher",
			"SELECT reduce(nums, 0, (s, x) -> s + x, s -> s * 10) FROM docs WHERE id = 1",
			[][]string{{"60"}},
		},
		{"reduce empty", "SELECT reduce(tags, 7, (s, x) -> s + 1, s -> s) FROM docs WHERE id = 2", [][]string{{"7"}}},
		{"any_match", "SELECT any_match(nums, x -> x > 4) FROM docs ORDER BY id", [][]string{{"0"}, {"1"}}},
		{"all_match", "SELECT all_match(nums, x -> x > 1) FROM docs ORDER BY id", [][]string{{"0"}, {"1"}}},
		{"none_match", "SELECT none_match(nums, x -> x > 4) FROM docs ORDER BY id", [][]string{{"1"}, {"0"}}},
		{"any_match empty", "SELECT any_match(tags, x -> x = 'x') FROM docs ORDER BY id", [][]string{{"1"}, {"0"}}},
		{
			"map_filter",
			"SELECT map_filter(m, (k, v) -> v > 1) FROM docs ORDER BY id",
			[][]string{{"{b=2}"}, {"{a=10}"}},
		},
		{
			"transform_values",
			"SELECT transform_values(m, (k, v) -> v * 10) FROM docs WHERE id = 1",
			[][]string{{"{a=10, b=20}"}},
		},
		{
			"transform_keys",
			"SELECT transform_keys(m, (k, v) -> upper(k)) FROM docs WHERE id = 1",
			[][]string{{"{A=1, B=2}"}},
		},
		{
			"zip_with",
			"SELECT zip_with(ARRAY[1, 2, 3], ARRAY[10, 20, 30], (a, b) -> a + b)",
			[][]string{{"[11, 22, 33]"}},
		},
		{
			"zip_with uneven",
			"SELECT zip_with(ARRAY[1, 2], ARRAY[10], (a, b) -> coalesce(a, 0) + coalesce(b, 0))",
			[][]string{{"[11, 2]"}},
		},
		{
			"nested lambda",
			"SELECT transform(ARRAY[1, 2], x -> cardinality(filter(nums, y -> y > x))) FROM docs WHERE id = 1",
			[][]string{{"[2, 1]"}},
		},
		{"null array", "SELECT transform(element_at(ARRAY[ARRAY[1]], 5), x -> x)", [][]string{{""}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
		})
	}
}

func TestSDK_TrinoComplexDisplay(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		sql   string
		want  [][]string
		types []string
	}{
		{"array column", "SELECT nums FROM docs ORDER BY id", [][]string{{"[1, 2, 3]"}, {"[4, 5]"}}, []string{"array"}},
		{"map column", "SELECT m FROM docs ORDER BY id", [][]string{{"{a=1, b=2}"}, {"{a=10}"}}, []string{"map"}},
		{
			"row column",
			"SELECT s FROM docs ORDER BY id",
			[][]string{{"{age=30, name=ann}"}, {"{age=25, name=bob}"}},
			[]string{"row"},
		},
		{"empty array", "SELECT tags FROM docs ORDER BY id", [][]string{{"[x, y]"}, {"[]"}}, []string{"array"}},
		{
			"array literal",
			"SELECT ARRAY[1, 2], ARRAY['a', NULL]",
			[][]string{{"[1, 2]", "[a, null]"}},
			[]string{"array", "array"},
		},
		{"nested", "SELECT ARRAY[ARRAY[1], ARRAY[2, 3]]", [][]string{{"[[1], [2, 3]]"}}, []string{"array"}},
		{"map literal", "SELECT MAP(ARRAY['k'], ARRAY[1])", [][]string{{"{k=1}"}}, []string{"map"}},
		{"split", "SELECT split('a,b', ',')", [][]string{{"[a, b]"}}, []string{"array"}},
		{"array_agg", "SELECT array_agg(id) FROM docs", [][]string{{"[1, 2]"}}, []string{"array"}},
		{
			"json_extract stays json",
			`SELECT json_extract('{"a":[1,2]}', '$.a')`,
			[][]string{{"[1,2]"}},
			[]string{"varchar"},
		},
		{"json string literal", `SELECT '[1,2]'`, [][]string{{"[1,2]"}}, []string{"varchar"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
			assert.Equal(t, tc.types, got.types)
		})
	}
}

func TestSDK_TrinoIntervals(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{
			"timestamp plus day",
			"SELECT TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '1' DAY",
			[][]string{{"2024-02-01 10:00:00.000"}},
		},
		{"date minus month", "SELECT DATE '2024-03-31' - INTERVAL '1' MONTH", [][]string{{"2024-02-29"}}},
		{"date plus year", "SELECT DATE '2024-02-29' + INTERVAL '1' YEAR", [][]string{{"2025-02-28"}}},
		{
			"hours",
			"SELECT TIMESTAMP '2024-01-01 23:00:00' + INTERVAL '3' HOUR",
			[][]string{{"2024-01-02 02:00:00.000"}},
		},
		{"plural unit", "SELECT DATE '2024-01-01' + INTERVAL '2' DAYS", [][]string{{"2024-01-03"}}},
		{"week", "SELECT DATE '2024-01-01' + INTERVAL '1' WEEK", [][]string{{"2024-01-08"}}},
		{"year to month", "SELECT DATE '2024-01-15' + INTERVAL '1-2' YEAR TO MONTH", [][]string{{"2025-03-15"}}},
		{
			"day to second",
			"SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 02:03:04' DAY TO SECOND",
			[][]string{{"2024-01-02 02:03:04.000"}},
		},
		{
			"function operand",
			"SELECT date_trunc('month', DATE '2024-05-17') + INTERVAL '1' MONTH",
			[][]string{{"2024-06-01"}},
		},
		{
			"cast operand",
			"SELECT CAST('2024-01-01 00:00:00' AS timestamp) + INTERVAL '90' MINUTE",
			[][]string{{"2024-01-01 01:30:00.000"}},
		},
		{
			"where clause",
			"SELECT id FROM docs WHERE DATE '2024-01-01' + INTERVAL '1' DAY = DATE '2024-01-02' AND id = 1",
			[][]string{{"1"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, docsClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
		})
	}
}

func salesClient(t *testing.T) *athenasdk.Client {
	t.Helper()

	return sdkClientFor(
		t,
		map[string][]byte{"lake/sales/a.csv": []byte(salesData)},
		map[string]*athena.GlueTableDef{
			"shop/sales": salesDef(serdeCSV, map[string]string{"skip.header.line.count": "1"}),
		},
	)
}

func TestSDK_TrinoGroupingSets(t *testing.T) {
	t.Parallel()

	const yr = "extract(year FROM day)"

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{
			"rollup one",
			"SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) ORDER BY category",
			[][]string{{"books", "2"}, {"games", "2"}, {"", "4"}},
		},
		{
			"rollup two",
			"SELECT category, " + yr + " AS y, sum(amount) AS s FROM sales GROUP BY ROLLUP (category, y) ORDER BY 1, 2",
			[][]string{
				{"books", "2024", "30.5"}, {"books", "", "30.5"},
				{"games", "2024", "5.0"}, {"games", "2025", "50.0"}, {"games", "", "55.0"},
				{"", "", "85.5"},
			},
		},
		{
			"cube",
			"SELECT category, " + yr + " AS y, count(*) AS n FROM sales GROUP BY CUBE (category, " + yr + ") ORDER BY 1, 2",
			[][]string{
				{"books", "2024", "2"}, {"books", "", "2"},
				{"games", "2024", "1"}, {"games", "2025", "1"}, {"games", "", "2"},
				{"", "2024", "3"}, {"", "2025", "1"}, {"", "", "4"},
			},
		},
		{
			"grouping sets",
			"SELECT category, " + yr + " AS y, count(*) AS n FROM sales " +
				"GROUP BY GROUPING SETS ((category), (" + yr + "), ()) ORDER BY 1, 2",
			[][]string{{"books", "", "2"}, {"games", "", "2"}, {"", "2024", "3"}, {"", "2025", "1"}, {"", "", "4"}},
		},
		{
			"grouping function",
			"SELECT category, grouping(category) AS g, count(*) AS n FROM sales GROUP BY ROLLUP (category) ORDER BY g, category",
			[][]string{{"books", "0", "2"}, {"games", "0", "2"}, {"", "1", "4"}},
		},
		{
			"grouping two args",
			"SELECT category, " + yr + " AS y, grouping(category, " + yr + ") AS g FROM sales " +
				"GROUP BY GROUPING SETS ((category), (" + yr + "), (category, " + yr + ")) ORDER BY g, 1, 2",
			[][]string{
				{"books", "2024", "0"}, {"games", "2024", "0"}, {"games", "2025", "0"},
				{"books", "", "1"}, {"games", "", "1"},
				{"", "2024", "2"}, {"", "2025", "2"},
			},
		},
		{
			"having",
			"SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category) HAVING count(*) > 2 ORDER BY 1",
			[][]string{{"", "4"}},
		},
		{
			"mixed plain and rollup",
			"SELECT category, " + yr + " AS y, count(*) AS n FROM sales GROUP BY category, ROLLUP (" + yr + ") ORDER BY 1, 2",
			[][]string{
				{"books", "2024", "2"},
				{"books", "", "2"},
				{"games", "2024", "1"},
				{"games", "2025", "1"},
				{"games", "", "2"},
			},
		},
		{
			"rollup ordinal and where",
			"SELECT category, sum(amount) AS s FROM sales WHERE amount > 5 GROUP BY ROLLUP (1) ORDER BY 1",
			[][]string{{"books", "30.5"}, {"games", "50.0"}, {"", "80.5"}},
		},
		{
			"subquery",
			"SELECT max(n) FROM (SELECT category, count(*) AS n FROM sales GROUP BY ROLLUP (category))",
			[][]string{{"4"}},
		},
		{
			"order by desc nulls default",
			"SELECT category FROM sales GROUP BY ROLLUP (category) ORDER BY category DESC",
			[][]string{{"games"}, {"books"}, {""}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, salesClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
		})
	}
}

func TestSDK_TrinoOrderByNullsLast(t *testing.T) {
	t.Parallel()

	got := sdkRun(t, salesClient(t),
		"SELECT x FROM (SELECT id AS x FROM sales UNION ALL SELECT NULL) ORDER BY x")
	assert.Equal(t, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {""}}, got.rows)
}

func (f *engineS3) listedPrefixes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := append([]string(nil), f.listed...)
	sort.Strings(out)

	return out
}

func projectedDef(params map[string]string, keys ...athena.Column) *athena.GlueTableDef {
	return &athena.GlueTableDef{
		Name: "logs", Location: "s3://lake/logs/", SerdeLibrary: serdeCSV, Parameters: params,
		Columns:       []athena.Column{{Name: "id", Type: "int"}, {Name: "msg", Type: "string"}},
		PartitionKeys: keys,
	}
}

func TestSDK_PartitionProjection(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC()
	day := func(back int) string { return today.AddDate(0, 0, -back).Format("2006-01-02") }

	cases := []struct {
		params  map[string]string
		objects map[string]string
		name    string
		sql     string
		failure string
		keys    []athena.Column
		want    [][]string
		listed  []string
	}{
		{
			name: "enum ignores unlisted partitions",
			params: map[string]string{
				"projection.enabled": "true", "projection.region.type": "enum", "projection.region.values": "us, eu",
			},
			keys: []athena.Column{{Name: "region", Type: "string"}},
			objects: map[string]string{
				"lake/logs/region=us/a.csv": "1,u\n",
				"lake/logs/region=eu/a.csv": "2,e\n",
				"lake/logs/region=ap/a.csv": "3,a\n",
			},
			sql:    "SELECT region, count(*) FROM logs GROUP BY region ORDER BY region",
			want:   [][]string{{"eu", "1"}, {"us", "1"}},
			listed: []string{"lake/logs/region=eu", "lake/logs/region=us"},
		},
		{
			name: "integer range pruned by between",
			params: map[string]string{
				"projection.enabled": "true", "projection.year.type": "integer", "projection.year.range": "2020,2025",
			},
			keys: []athena.Column{{Name: "year", Type: "int"}},
			objects: map[string]string{
				"lake/logs/year=2022/a.csv": "1,a\n", "lake/logs/year=2023/a.csv": "2,b\n",
				"lake/logs/year=2024/a.csv": "3,c\n", "lake/logs/year=2025/a.csv": "4,d\n",
			},
			sql:    "SELECT id FROM logs WHERE year BETWEEN 2023 AND 2024 ORDER BY id",
			want:   [][]string{{"2"}, {"3"}},
			listed: []string{"lake/logs/year=2023", "lake/logs/year=2024"},
		},
		{
			name: "integer interval and digits",
			params: map[string]string{
				"projection.enabled": "true", "projection.m.type": "integer", "projection.m.range": "1,7",
				"projection.m.interval": "3", "projection.m.digits": "2",
			},
			keys: []athena.Column{{Name: "m", Type: "int"}},
			objects: map[string]string{
				"lake/logs/m=01/a.csv": "1,a\n", "lake/logs/m=02/a.csv": "2,b\n",
				"lake/logs/m=04/a.csv": "3,c\n", "lake/logs/m=07/a.csv": "4,d\n",
			},
			sql:  "SELECT id FROM logs ORDER BY id",
			want: [][]string{{"1"}, {"3"}, {"4"}},
		},
		{
			name: "date with template and range predicate",
			params: map[string]string{
				"projection.enabled": "true", "projection.dt.type": "date", "projection.dt.format": "yyyy/MM/dd",
				"projection.dt.range": "2024/01/01,2024/01/04", "storage.location.template": "s3://lake/logs/${dt}/",
			},
			keys: []athena.Column{{Name: "dt", Type: "string"}},
			objects: map[string]string{
				"lake/logs/2024/01/01/a.csv": "1,a\n", "lake/logs/2024/01/02/a.csv": "2,b\n",
				"lake/logs/2024/01/03/a.csv": "3,c\n", "lake/logs/2024/01/05/a.csv": "9,z\n",
			},
			sql:    "SELECT id FROM logs WHERE dt >= '2024/01/02' ORDER BY id",
			want:   [][]string{{"2"}, {"3"}},
			listed: []string{"lake/logs/2024/01/02", "lake/logs/2024/01/03", "lake/logs/2024/01/04"},
		},
		{
			name: "date NOW relative range",
			params: map[string]string{
				"projection.enabled": "true", "projection.dt.type": "date", "projection.dt.format": "yyyy-MM-dd",
				"projection.dt.range": "NOW-2DAYS,NOW",
			},
			keys: []athena.Column{{Name: "dt", Type: "string"}},
			objects: map[string]string{
				"lake/logs/dt=" + day(0) + "/a.csv": "1,a\n", "lake/logs/dt=" + day(2) + "/a.csv": "2,b\n",
				"lake/logs/dt=" + day(3) + "/a.csv": "3,old\n",
			},
			sql:  "SELECT id FROM logs ORDER BY id",
			want: [][]string{{"1"}, {"2"}},
		},
		{
			name: "date monthly interval unit",
			params: map[string]string{
				"projection.enabled": "true", "projection.dt.type": "date", "projection.dt.format": "yyyy-MM",
				"projection.dt.range": "2024-01,2024-03",
			},
			keys: []athena.Column{{Name: "dt", Type: "string"}},
			objects: map[string]string{
				"lake/logs/dt=2024-01/a.csv": "1,a\n", "lake/logs/dt=2024-02/a.csv": "2,b\n",
				"lake/logs/dt=2024-03/a.csv": "3,c\n", "lake/logs/dt=2024-04/a.csv": "4,d\n",
			},
			sql:  "SELECT count(*) FROM logs",
			want: [][]string{{"3"}},
		},
		{
			name:    "injected with filter",
			params:  map[string]string{"projection.enabled": "true", "projection.tenant.type": "injected"},
			keys:    []athena.Column{{Name: "tenant", Type: "string"}},
			objects: map[string]string{"lake/logs/tenant=a/x.csv": "1,a\n", "lake/logs/tenant=b/x.csv": "2,b\n"},
			sql:     "SELECT id FROM logs WHERE tenant = 'b'",
			want:    [][]string{{"2"}},
			listed:  []string{"lake/logs/tenant=b"},
		},
		{
			name:    "injected without filter fails",
			params:  map[string]string{"projection.enabled": "true", "projection.tenant.type": "injected"},
			keys:    []athena.Column{{Name: "tenant", Type: "string"}},
			objects: map[string]string{"lake/logs/tenant=a/x.csv": "1,a\n"},
			sql:     "SELECT id FROM logs",
			failure: "INVALID_TABLE_PROPERTY: Column 'tenant' uses injected partition projection",
		},
		{
			name: "disabled projection reads nothing without partitions",
			params: map[string]string{
				"projection.enabled": "false", "projection.region.type": "enum", "projection.region.values": "us",
			},
			keys:    []athena.Column{{Name: "region", Type: "string"}},
			objects: map[string]string{"lake/logs/region=us/a.csv": "1,u\n"},
			sql:     "SELECT count(*) FROM logs",
			want:    [][]string{{"0"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			objects := map[string][]byte{}
			for k, v := range tc.objects {
				objects[k] = []byte(v)
			}

			b, store := newEngineBackend(t, objects,
				map[string]*athena.GlueTableDef{"shop/logs": projectedDef(tc.params, tc.keys...)})
			client := newTestAthenaClient(t, athena.NewHandler(b))

			if tc.failure != "" {
				assert.Contains(t, sdkFailure(t, client, tc.sql), tc.failure)

				return
			}

			assert.Equal(t, tc.want, sdkRun(t, client, tc.sql).rows)

			if tc.listed != nil {
				assert.Equal(t, tc.listed, trimListed(store.listedPrefixes()))
			}
		})
	}
}

func trimListed(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.TrimSuffix(s, "/")
	}

	return out
}

// sdkFailure runs a query expected to FAIL and returns its StateChangeReason.
func sdkFailure(t *testing.T, client *athenasdk.Client, query string) string {
	t.Helper()

	start, err := client.StartQueryExecution(t.Context(), &athenasdk.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		WorkGroup:             aws.String("primary"),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{Database: aws.String("shop")},
		ResultConfiguration:   &athenatypes.ResultConfiguration{OutputLocation: aws.String("s3://out/res/")},
	})
	require.NoError(t, err)

	var reason string

	require.Eventually(t, func() bool {
		got, gerr := client.GetQueryExecution(t.Context(),
			&athenasdk.GetQueryExecutionInput{QueryExecutionId: start.QueryExecutionId})
		require.NoError(t, gerr)

		if got.QueryExecution.Status.State != athenatypes.QueryExecutionStateFailed {
			return false
		}

		reason = aws.ToString(got.QueryExecution.Status.StateChangeReason)

		return true
	}, 10*time.Second, time.Millisecond)

	return reason
}

func TestSDK_CTASOfComplexValuesKeepsJSON(t *testing.T) {
	t.Parallel()

	b, store := newEngineBackend(t, map[string][]byte{"lake/docs/a.json": []byte(docsData)},
		map[string]*athena.GlueTableDef{"shop/docs": docsDef()})
	client := newTestAthenaClient(t, athena.NewHandler(b))

	sdkRun(t, client, "CREATE DATABASE shop")
	sdkRun(t, client, "CREATE TABLE shop.out WITH (format = 'JSON', external_location = 's3://lake/out/') AS "+
		"SELECT id, nums, split('a,b', ',') AS parts FROM docs WHERE id = 1")

	store.mu.Lock()
	defer store.mu.Unlock()

	var body strings.Builder

	for k, v := range store.objects {
		if strings.HasPrefix(k, "lake/out/") {
			body.Write(v)
		}
	}

	assert.NotContains(t, body.String(), "\x01")
	assert.Contains(t, body.String(), `"parts"`)
}
