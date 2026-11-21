package bqsql_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

var encodedDecimal = regexp.MustCompile(`#[0-9a-f]{64}`)

const lookalike = "#8000000000000000000000000000000070d8f2fc87ca26b70e4f336000000000"

func leakCatalog(t *testing.T) *fakeCatalog {
	t.Helper()

	fs := fieldsJSON(t, `[
		{"name":"id","type":"INTEGER"},
		{"name":"n","type":"NUMERIC"},
		{"name":"b","type":"BIGNUMERIC"},
		{"name":"s","type":"STRING"},
		{"name":"arr","type":"NUMERIC","mode":"REPEATED"},
		{"name":"rec","type":"RECORD","fields":[
			{"name":"x","type":"NUMERIC"},{"name":"y","type":"BIGNUMERIC"},{"name":"s","type":"STRING"}]},
		{"name":"recs","type":"RECORD","mode":"REPEATED","fields":[
			{"name":"x","type":"NUMERIC"},{"name":"tag","type":"STRING"}]}
	]`)

	rows := rowsJSON(t, fs, `[
		{"id":1,"n":"1.5","b":"0.00000000000000000000000000000000000001","s":"`+lookalike+`","arr":["2.5","3"],
		 "rec":{"x":"4.25","y":"5","s":"`+lookalike+`"},"recs":[{"x":"6.5","tag":"a"},{"x":"7","tag":"b"}]},
		{"id":2,"n":"-2","b":"-7.5","s":"plain","arr":[],"rec":null,"recs":[]},
		{"id":3,"n":null,"b":null,"s":null,"arr":[],"rec":null,"recs":[]}
	]`)

	return &fakeCatalog{tables: map[string]*bqsql.TableData{
		"ds.t": {Ref: bqsql.TableRef{Project: "p", Dataset: "ds", Table: "t"}, Fields: fs, Rows: rows},
	}}
}

func TestNumericNeverLeaksEncoding(t *testing.T) {
	t.Parallel()

	queries := []string{
		"SELECT n, b, arr, rec, recs FROM t",
		"SELECT * FROM t",
		"SELECT TO_JSON_STRING(n), TO_JSON_STRING(b), TO_JSON_STRING(arr), TO_JSON_STRING(rec), TO_JSON_STRING(recs) FROM t",
		"SELECT TO_JSON_STRING(STRUCT(n, b, s, arr)) FROM t",
		"SELECT TO_JSON_STRING([STRUCT(n AS a), STRUCT(b AS a)]) FROM t",
		"SELECT FORMAT('%s|%t|%T', n, n, b) FROM t",
		"SELECT FORMAT('%s|%t|%T', arr, rec, recs) FROM t",
		"SELECT FORMAT('%s %s', s, n) FROM t",
		"SELECT CAST(n AS STRING), CAST(b AS STRING) FROM t",
		"SELECT CAST(arr AS STRING) FROM t",
		"SELECT CAST(rec AS STRING) FROM t",
		"SELECT CAST(arr AS ARRAY<STRING>), CAST(arr AS ARRAY<FLOAT64>), CAST(arr AS ARRAY<INT64>) FROM t",
		"SELECT CAST(rec AS STRUCT<x STRING, y FLOAT64, s STRING>) FROM t",
		"SELECT CAST(recs AS ARRAY<STRUCT<x STRING, tag STRING>>) FROM t",
		"SELECT n || 'x' FROM t",
		"SELECT s || CAST(n AS STRING) FROM t",
		"SELECT CONCAT(s, n) FROM t",
		"SELECT CONCAT(s, CAST(b AS STRING)) FROM t",
		"SELECT STRING_AGG(n) FROM t",
		"SELECT STRING_AGG(CAST(n AS STRING), ',') FROM t",
		"SELECT ARRAY_TO_STRING(arr, ',') FROM t",
		"SELECT ARRAY_TO_STRING([n], ',') FROM t",
		"SELECT ARRAY_TO_STRING(ARRAY(SELECT CAST(a AS STRING) FROM UNNEST(arr) a), ',') FROM t",
		"SELECT n LIKE '%1%' FROM t",
		"SELECT n = s FROM t",
		"SELECT n < s FROM t",
		"SELECT n IS DISTINCT FROM s FROM t",
		"SELECT s IN (n) FROM t",
		"SELECT n BETWEEN s AND s FROM t",
		"SELECT IF(id > 1, s, n) FROM t",
		"SELECT COALESCE(s, n) FROM t",
		"SELECT IFNULL(s, n) FROM t",
		"SELECT NULLIF(s, n) FROM t",
		"SELECT CASE WHEN id = 1 THEN n ELSE s END FROM t",
		"SELECT n AS v FROM t UNION ALL SELECT s FROM t",
		"SELECT s AS v FROM t UNION DISTINCT SELECT n FROM t",
		"SELECT ['x', n] FROM t",
		"SELECT GREATEST(s, n) FROM t",
		"SELECT SEARCH(rec, '4.25'), SEARCH(arr, '2.5'), SEARCH(recs, '6.5') FROM t",
		"SELECT UPPER(n), LENGTH(n), SUBSTR(b, 1, 5), TRIM(n), REPLACE(n, '1', '2') FROM t",
		"SELECT ARRAY_CONCAT(arr, [1, 2]), [1] || arr, ARRAY_REVERSE(arr) FROM t",
		"SELECT x FROM t, UNNEST(arr) AS x",
		"SELECT r.x, r.tag FROM t, UNNEST(recs) AS r",
		"SELECT ARRAY(SELECT AS STRUCT n, b) FROM t",
		"SELECT STRUCT(n, b, arr) FROM t",
		"SELECT MIN(n), MAX(b), SUM(n), AVG(b), ANY_VALUE(n), ARRAY_AGG(n) FROM t",
		"SELECT APPROX_TOP_COUNT(n, 2), APPROX_QUANTILES(b, 2) FROM t",
		"SELECT rec, COUNT(*) FROM t GROUP BY rec",
		"SELECT DISTINCT arr FROM t",
		"SELECT n, LAG(n, 1, 0) OVER (ORDER BY id), FIRST_VALUE(b) OVER (ORDER BY id) FROM t",
		"SELECT * FROM (SELECT id, n, b FROM t) UNPIVOT (v FOR c IN (id, n, b))",
		"SELECT ERROR(CAST(n AS STRING)) FROM t WHERE id = 1",
		"SELECT CAST(s AS NUMERIC) FROM t",
		"SELECT PARSE_NUMERIC(s) FROM t",
		"SELECT s, TO_JSON_STRING(STRUCT(s, n AS n2)), UPPER(s) FROM t",
		"SELECT TO_JSON_STRING(STRUCT(rec.s AS s, n AS n)) FROM t",
	}

	for _, sql := range queries {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, leakCatalog(t), sql)
			if err != nil {
				assert.NotRegexp(t, encodedDecimal, err.Error())

				return
			}

			var sb strings.Builder

			fmt.Fprint(&sb, wireRows(res))

			for _, r := range res.Rows {
				fmt.Fprint(&sb, r)
			}

			body := sb.String()

			if encodedDecimal.MatchString(body) {
				assert.Contains(t, body, lookalike, "only a genuine STRING may carry the lookalike text")
				assert.Equal(t, 0, countOutsideStrings(body), "an encoded decimal reached the client: %s", body)
			}
		})
	}
}

func countOutsideStrings(body string) int {
	rest := regexp.MustCompile(regexpQuote(lookalike)).ReplaceAllString(body, "")

	return len(encodedDecimal.FindAllString(rest, -1))
}

func regexpQuote(s string) string { return regexp.QuoteMeta(s) }

func TestStringLookalikeIsNotADecimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want [][]any
	}{
		{"column", "SELECT s FROM t WHERE id = 1", [][]any{{lookalike}}},
		{
			"upper",
			"SELECT UPPER(s) FROM t WHERE id = 1",
			[][]any{{"#8000000000000000000000000000000070D8F2FC87CA26B70E4F336000000000"}},
		},
		{
			"json struct",
			"SELECT TO_JSON_STRING(STRUCT(n, s)) FROM t WHERE id = 1",
			[][]any{{`{"n":1.5,"s":"` + lookalike + `"}`}},
		},
		{
			"json nested record",
			"SELECT TO_JSON_STRING(rec) FROM t WHERE id = 1",
			[][]any{{`{"x":4.25,"y":5,"s":"` + lookalike + `"}`}},
		},
		{"format", "SELECT FORMAT('%s|%s', s, n) FROM t WHERE id = 1", [][]any{{lookalike + "|1.5"}}},
		{"format control", "SELECT FORMAT('%s|%s', '\\x01N1', n) FROM t WHERE id = 1", [][]any{{"\x01N1|1.5"}}},
		{"concat", "SELECT s || 'x' FROM t WHERE id = 1", [][]any{{lookalike + "x"}}},
		{"group", "SELECT s, COUNT(*) FROM t WHERE id = 1 GROUP BY s", [][]any{{lookalike, "1"}}},
		{"nested string", "SELECT rec.s FROM t WHERE id = 1", [][]any{{lookalike}}},
		{
			"typed cast",
			"SELECT CAST(NUMERIC '1' AS STRING) = '1', s = '" + lookalike + "' FROM t WHERE id = 1",
			[][]any{{"true", "true"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, leakCatalog(t), tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.want, wireRows(res))
		})
	}
}
