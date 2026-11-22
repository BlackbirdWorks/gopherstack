package bqsql_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func hexBytes(h string) string {
	out := make([]byte, 0, len(h)/2)

	for i := 0; i+1 < len(h); i += 2 {
		var b byte

		for _, c := range h[i : i+2] {
			b <<= 4

			switch {
			case c >= 'a':
				b |= byte(c-'a') + 10
			default:
				b |= byte(c - '0')
			}
		}

		out = append(out, b)
	}

	return b64(string(out))
}

type fnCase struct {
	name string
	sql  string
	want [][]any
}

func runFnCases(t *testing.T, tests []fnCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.want, wireRows(res))
		})
	}
}

// Examples from https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions and
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions.
func TestNetAndFarmFunctions(t *testing.T) {
	t.Parallel()

	runFnCases(t, []fnCase{
		{"ip from v4", "SELECT NET.IP_FROM_STRING('48.49.50.51')", [][]any{{b64("0123")}}},
		{"ip from v6", "SELECT NET.IP_FROM_STRING('::1')", [][]any{{hexBytes("00000000000000000000000000000001")}}},
		{
			"ip from v6 full",
			"SELECT NET.IP_FROM_STRING('3031:3233:3435:3637:3839:4041:4243:4445')",
			[][]any{{b64("0123456789@ABCDE")}},
		},
		{
			"ip from mapped",
			"SELECT NET.IP_FROM_STRING('::ffff:192.0.2.128')",
			[][]any{{hexBytes("00000000000000000000ffffc0000280")}},
		},
		{"safe ip cidr", "SELECT NET.SAFE_IP_FROM_STRING('48.49.50.51/32')", [][]any{{nil}}},
		{"safe ip short", "SELECT NET.SAFE_IP_FROM_STRING('48.49.50')", [][]any{{nil}}},
		{"safe ip bad", "SELECT NET.SAFE_IP_FROM_STRING('::wxyz')", [][]any{{nil}}},
		{"ip to string v4", "SELECT NET.IP_TO_STRING(b'0123')", [][]any{{"48.49.50.51"}}},
		{
			"ip to string v6",
			"SELECT NET.IP_TO_STRING(b'0123456789@ABCDE')",
			[][]any{{"3031:3233:3435:3637:3839:4041:4243:4445"}},
		},
		{
			"ip to string mapped",
			"SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('::ffff:192.0.2.128'))",
			[][]any{{"::ffff:192.0.2.128"}},
		},
		{"mask 20", "SELECT NET.IP_NET_MASK(4, 20)", [][]any{{hexBytes("fffff000")}}},
		{"mask 16 1", "SELECT NET.IP_NET_MASK(16, 1)", [][]any{{hexBytes("80000000000000000000000000000000")}}},
		{"trunc 11", "SELECT NET.IP_TRUNC(b'\\xaa\\xbb\\xcc\\xdd', 11)", [][]any{{hexBytes("aaa00000")}}},
		{"trunc 12", "SELECT NET.IP_TRUNC(b'\\xaa\\xbb\\xcc\\xdd', 12)", [][]any{{hexBytes("aab00000")}}},
		{
			"trunc v6",
			"SELECT NET.IP_TRUNC(b'0123456789@ABCDE', 80)",
			[][]any{{hexBytes("30313233343536373839" + "000000000000")}},
		},
		{
			"v4 from int",
			"SELECT NET.IPV4_FROM_INT64(11259375), NET.IPV4_FROM_INT64(-2)",
			[][]any{{hexBytes("00abcdef"), hexBytes("fffffffe")}},
		},
		{"v4 to int", "SELECT NET.IPV4_TO_INT64(b'\\x00\\xab\\xcd\\xef')", [][]any{{"11259375"}}},
		{
			"host",
			"SELECT NET.HOST('//user:password@a.b:80/path?query'), NET.HOST('https://[::1]:80'), " +
				"NET.HOST('mailto:?to=&subject=&body=')",
			[][]any{{"a.b", "[::1]", "mailto"}},
		},
		{
			"host more",
			"SELECT NET.HOST('http://abc.xyz'), NET.HOST('    www.Example.Co.UK    '), NET.HOST('')",
			[][]any{{"abc.xyz", "www.Example.Co.UK", nil}},
		},
		{
			"suffix",
			"SELECT NET.PUBLIC_SUFFIX('http://abc.xyz'), NET.PUBLIC_SUFFIX('    www.Example.Co.UK    '), " +
				"NET.PUBLIC_SUFFIX('//user:password@a.b:80/path?query'), NET.PUBLIC_SUFFIX('https://[::1]:80')",
			[][]any{{"xyz", "Co.UK", nil, nil}},
		},
		{
			"suffix idn",
			"SELECT NET.PUBLIC_SUFFIX('http://例子.卷筒纸.中国'), NET.REG_DOMAIN('http://例子.卷筒纸.中国')",
			[][]any{{"中国", "卷筒纸.中国"}},
		},
		{
			"reg domain",
			"SELECT NET.REG_DOMAIN('http://abc.xyz'), NET.REG_DOMAIN('    www.Example.Co.UK    '), " +
				"NET.REG_DOMAIN('mailto:?to=&subject=&body='), NET.REG_DOMAIN('xyz')",
			[][]any{{"abc.xyz", "Example.Co.UK", nil, nil}},
		},
		{
			"farm",
			"SELECT FARM_FINGERPRINT('1footrue'), FARM_FINGERPRINT('2applefalse'), " +
				"FARM_FINGERPRINT('3true'), FARM_FINGERPRINT(CAST(NULL AS STRING))",
			[][]any{{"-1541654101129638711", "2794438866806483259", "-4880158226897771312", nil}},
		},
		{"farm bytes", "SELECT FARM_FINGERPRINT(b'3true')", [][]any{{"-4880158226897771312"}}},
	})
}

func TestNetFunctionErrors(t *testing.T) {
	t.Parallel()

	for name, sql := range map[string]string{
		"ip bad":       "SELECT NET.IP_FROM_STRING('48.49.50')",
		"to string":    "SELECT NET.IP_TO_STRING(b'abc')",
		"mask size":    "SELECT NET.IP_NET_MASK(5, 1)",
		"mask prefix":  "SELECT NET.IP_NET_MASK(4, 33)",
		"trunc prefix": "SELECT NET.IP_TRUNC(b'abcd', 33)",
		"v4 range":     "SELECT NET.IPV4_FROM_INT64(4294967296)",
		"v4 len":       "SELECT NET.IPV4_TO_INT64(b'abc')",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), sql)
			require.Error(t, err)
		})
	}
}

// Examples from https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions.
func TestGeographyFunctions(t *testing.T) {
	t.Parallel()

	const (
		poly   = "ST_GEOGFROMTEXT('POLYGON((1 1, 20 1, 10 20, 1 1))')"
		square = "ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')"
		inner  = "ST_GEOGFROMTEXT('POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))')"
	)

	runFnCases(t, []fnCase{
		{
			"point",
			"SELECT ST_GEOGPOINT(1, 2), ST_ASTEXT(ST_GEOGPOINT(181, 2))",
			[][]any{{"POINT(1 2)", "POINT(-179 2)"}},
		},
		{
			"xy",
			"SELECT ST_X(ST_GEOGPOINT(i, i + 1)), ST_Y(ST_GEOGPOINT(i, i + 1)) FROM UNNEST([0, 5, 12]) AS i ORDER BY i",
			[][]any{{"0.0", "1.0"}, {"5.0", "6.0"}, {"12.0", "13.0"}},
		},
		{
			"contains",
			"SELECT ST_CONTAINS(" + poly + ", ST_GEOGPOINT(0, 0)), ST_CONTAINS(" + poly + ", ST_GEOGPOINT(1, 1)), " +
				"ST_CONTAINS(" + poly + ", ST_GEOGPOINT(10, 10)), ST_WITHIN(ST_GEOGPOINT(10, 10), " + poly + ")",
			[][]any{{"false", "false", "true", "true"}},
		},
		{
			"intersects",
			"SELECT ST_INTERSECTS(" + poly + ", ST_GEOGPOINT(1, 1)), " +
				"ST_INTERSECTS(" + poly + ", ST_GEOGPOINT(0, 0)), " +
				"ST_INTERSECTS(ST_GEOGFROMTEXT('LINESTRING(0 0, 4 4)'), ST_GEOGFROMTEXT('LINESTRING(0 4, 4 0)'))",
			[][]any{{"true", "false", "true"}},
		},
		{
			"polygon contains polygon",
			"SELECT ST_CONTAINS(" + square + ", " + inner + "), ST_CONTAINS(" + inner + ", " + square + ")",
			[][]any{{"true", "false"}},
		},
		{
			"hole",
			"SELECT ST_CONTAINS(g, ST_GEOGPOINT(5, 5)), ST_CONTAINS(g, ST_GEOGPOINT(1, 1)) FROM (SELECT " +
				"ST_GEOGFROMTEXT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 6 4, 6 6, 4 6, 4 4))') AS g)",
			[][]any{{"false", "true"}},
		},
		{
			"text roundtrip",
			"SELECT ST_ASTEXT(ST_GEOGFROMTEXT(' multipoint( (1 2), (3 4) ) ')), " +
				"ST_ASTEXT(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY')), " +
				"ST_ASTEXT(ST_GEOGFROMTEXT('LINESTRING(1 2,3 4)', planar => TRUE))",
			[][]any{{"MULTIPOINT(1 2, 3 4)", "GEOMETRYCOLLECTION EMPTY", "LINESTRING(1 2, 3 4)"}},
		},
		{
			"invalid polygon",
			"SELECT SAFE.ST_GEOGFROMTEXT('POLYGON((0 -1, 2 1, 2 -1, 0 1))')",
			[][]any{{nil}},
		},
		{
			"geojson",
			"SELECT ST_ASTEXT(ST_GEOGFROMGEOJSON('{\"type\":\"LineString\",\"coordinates\":[[1,2],[3,4]]}')), " +
				"ST_ASGEOJSON(ST_GEOGPOINT(1.5, 2))",
			[][]any{{"LINESTRING(1 2, 3 4)", `{ "type": "Point", "coordinates": [1.5, 2] }`}},
		},
		{
			"makeline",
			"SELECT ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(2, 3))), " +
				"ST_ASTEXT(ST_MAKELINE(ST_GEOGPOINT(1, 2), ST_GEOGPOINT(1, 2))), " +
				"ST_ASTEXT(ST_MAKELINE([ST_GEOGFROMTEXT('LINESTRING(1 2, 2 3)'), " +
				"ST_GEOGFROMTEXT('LINESTRING(2 3, 3 4)'), ST_GEOGPOINT(4, 5)]))",
			[][]any{{"LINESTRING(1 2, 2 3)", "POINT(1 2)", "LINESTRING(1 2, 2 3, 3 4, 4 5)"}},
		},
		{
			"makeline overlap",
			"SELECT ST_ASTEXT(ST_MAKELINE([ST_MAKELINE(ST_GEOGPOINT(1, 0), ST_GEOGPOINT(10, 0)), " +
				"ST_MAKELINE(ST_GEOGPOINT(10, 0), ST_GEOGPOINT(5, 0)), ST_MAKELINE(ST_GEOGPOINT(5, 0), ST_GEOGPOINT(5, 4))]))",
			[][]any{{"MULTILINESTRING((1 0, 5 0, 10 0), (5 0, 5 4))"}},
		},
		{
			"makeline retrace",
			"SELECT ST_ASTEXT(ST_MAKELINE([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(2, 0), ST_GEOGPOINT(0, 0)])), " +
				"ST_ASTEXT(ST_MAKELINE([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 0), ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 1)])), " +
				"ST_ASTEXT(ST_MAKELINE([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 4), ST_GEOGPOINT(0, 2), ST_GEOGPOINT(0, 6)]))",
			[][]any{{"LINESTRING(0 0, 2 0)", "LINESTRING(1 0, 0 0, 0 1)", "LINESTRING(0 0, 0 2, 0 4, 0 6)"}},
		},
		{
			"makeline touch",
			"SELECT ST_ASTEXT(ST_MAKELINE([ST_GEOGPOINT(0, 0), ST_GEOGPOINT(2, 0), ST_GEOGPOINT(1, 1), ST_GEOGPOINT(1, 0)]))",
			[][]any{{"LINESTRING(0 0, 2 0, 1 1, 1 0)"}},
		},
		{
			"makeline null",
			"SELECT ST_MAKELINE(ST_GEOGPOINT(1, 2), CAST(NULL AS GEOGRAPHY)), " +
				"ST_MAKELINE([ST_GEOGPOINT(1, 2), CAST(NULL AS GEOGRAPHY)])",
			[][]any{{nil, nil}},
		},
		{
			"makepolygon",
			"SELECT ST_ASTEXT(ST_MAKEPOLYGON(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 0, 1 1, 0 0)')))",
			[][]any{{"POLYGON((0 0, 1 0, 1 1, 0 0))"}},
		},
		{
			"distance",
			"SELECT ROUND(ST_DISTANCE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 1)), 1), ST_DISTANCE(ST_GEOGPOINT(1, 1), " +
				poly + "), ST_DISTANCE(ST_GEOGPOINT(0, 0), ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))",
			[][]any{{"111195.1", "0.0", nil}},
		},
		{
			"dwithin",
			"SELECT ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 1), 112000), " +
				"ST_DWITHIN(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 1), 111000)",
			[][]any{{"true", "false"}},
		},
		{
			"length",
			"SELECT ROUND(ST_LENGTH(ST_GEOGFROMTEXT('LINESTRING(0 0, 0 1, 0 2)')), 1), " +
				"ST_LENGTH(ST_GEOGPOINT(1, 1)), " +
				"ST_LENGTH(" + poly + ")",
			[][]any{{"222390.2", "0.0", "0.0"}},
		},
		{
			"area",
			"SELECT ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) BETWEEN 1.2e10 AND 1.25e10, " +
				"ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')) BETWEEN 1.2e10 AND 1.25e10, " +
				"ST_AREA(ST_GEOGPOINT(1, 1))",
			[][]any{{"true", "true", "0.0"}},
		},
		{
			"centroid",
			"SELECT ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('MULTIPOINT(0 0, 0 2)'))), " +
				"ST_ASTEXT(ST_CENTROID(ST_GEOGFROMTEXT('LINESTRING(0 0, 0 4)'))), " +
				"ROUND(ST_X(p), 4), ROUND(ST_Y(p), 4) " +
				"FROM (SELECT ST_CENTROID(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')) AS p)",
			[][]any{{"POINT(0 1)", "POINT(0 2)", "1.0", "1.0001"}},
		},
		{
			"null",
			"SELECT ST_ASTEXT(CAST(NULL AS GEOGRAPHY)), ST_GEOGPOINT(NULL, 1)",
			[][]any{{nil, nil}},
		},
	})
}

func TestGeographyErrors(t *testing.T) {
	t.Parallel()

	for name, sql := range map[string]string{
		"latitude":             "SELECT ST_GEOGPOINT(0, 91)",
		"x of line":            "SELECT ST_X(ST_GEOGFROMTEXT('LINESTRING(1 2, 3 4)'))",
		"x of empty":           "SELECT ST_X(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION EMPTY'))",
		"badwkt":               "SELECT ST_GEOGFROMTEXT('POINT(1)')",
		"z coords":             "SELECT ST_GEOGFROMTEXT('POINT Z (1 2 3)')",
		"oriented":             "SELECT ST_GEOGFROMTEXT('POINT(1 2)', oriented => TRUE, planar => TRUE)",
		"feature":              "SELECT ST_GEOGFROMGEOJSON('{\"type\":\"Feature\",\"geometry\":null}')",
		"open loop":            "SELECT ST_MAKEPOLYGON(ST_GEOGFROMTEXT('LINESTRING(0 0, 1 0, 1 1)'))",
		"makeline half circle": "SELECT ST_MAKELINE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(180, 0))",
		"makeline pole pair":   "SELECT ST_MAKELINE(ST_GEOGPOINT(0, 90), ST_GEOGPOINT(30, -90))",
		"makeline antipode":    "SELECT ST_MAKELINE([ST_GEOGPOINT(1, 1), ST_GEOGPOINT(10, 20), ST_GEOGPOINT(-170, -20)])",
		"spheroid":             "SELECT ST_DISTANCE(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(1, 1), TRUE)",
		"named other":          "SELECT ABS(x => 1)",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), sql)
			require.Error(t, err)
		})
	}
}

// Semantics from https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions.
func TestHLLFunctions(t *testing.T) {
	t.Parallel()

	const perCountry = "(SELECT country, HLL_COUNT.INIT(customer %s) AS sketch FROM (" +
		"SELECT 'UA' AS country, 'c1' AS customer UNION ALL SELECT 'UA', 'c2' UNION ALL SELECT 'UA', 'c2' " +
		"UNION ALL SELECT 'CZ', 'c3' UNION ALL SELECT 'BR', 'c1' UNION ALL SELECT 'BR', NULL) GROUP BY country)"

	runFnCases(t, []fnCase{
		{"extract", "SELECT HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x)) FROM UNNEST([1, 2, 3, 3, NULL]) AS x", [][]any{{"3"}}},
		{"per group", "SELECT country, HLL_COUNT.EXTRACT(sketch) FROM " + fmtPer(perCountry, "") + " ORDER BY country",
			[][]any{{"BR", "1"}, {"CZ", "1"}, {"UA", "2"}}},
		{"merge", "SELECT HLL_COUNT.MERGE(sketch) FROM " + fmtPer(perCountry, ", 10"), [][]any{{"3"}}},
		{"merge partial", "SELECT HLL_COUNT.EXTRACT(s) FROM (SELECT HLL_COUNT.MERGE_PARTIAL(sketch) AS s FROM " +
			fmtPer(perCountry, ", 12") + ")", [][]any{{"3"}}},
		{"merge empty", "SELECT HLL_COUNT.MERGE(s), HLL_COUNT.MERGE_PARTIAL(s) FROM UNNEST([CAST(NULL AS BYTES)]) AS s",
			[][]any{{"0", nil}}},
		{"extract null", "SELECT HLL_COUNT.EXTRACT(CAST(NULL AS BYTES))", [][]any{{"0"}}},
		{"all null init", "SELECT HLL_COUNT.INIT(x) FROM UNNEST([CAST(NULL AS INT64)]) AS x", [][]any{{nil}}},
		{"large", "SELECT ABS(HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x)) - 50000) < 1000 " +
			"FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS x",
			[][]any{{"true"}}},
		{"large merge low precision", "SELECT ABS(HLL_COUNT.MERGE(s) - 50000) < 3000 FROM (SELECT " +
			"HLL_COUNT.INIT(x, 12) AS s " +
			"FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS x GROUP BY MOD(x, 3) UNION ALL SELECT " +
			"HLL_COUNT.INIT(x, 14) FROM UNNEST([1, 2]) AS x)",
			[][]any{{"true"}}},
		{
			"strings and bytes",
			"SELECT HLL_COUNT.EXTRACT(HLL_COUNT.INIT(b)) FROM UNNEST([b'a', b'b', b'a']) AS b",
			[][]any{{"2"}},
		},
	})

	for name, sql := range map[string]string{
		"mixed types": "SELECT HLL_COUNT.MERGE(s) FROM (SELECT HLL_COUNT.INIT(x) AS s FROM UNNEST([1]) AS x " +
			"UNION ALL SELECT HLL_COUNT.INIT(y) FROM UNNEST(['a']) AS y)",
		"bad sketch":    "SELECT HLL_COUNT.EXTRACT(b'nope')",
		"precision low": "SELECT HLL_COUNT.INIT(x, 9) FROM UNNEST([1]) AS x",
		"precision hi":  "SELECT HLL_COUNT.INIT(x, 25) FROM UNNEST([1]) AS x",
		"bad input":     "SELECT HLL_COUNT.INIT(x) FROM UNNEST([1.5]) AS x",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), sql)
			require.Error(t, err)
		})
	}
}

func fmtPer(tmpl, prec string) string { return strings.Replace(tmpl, "%s", prec, 1) }

// Examples from https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions and
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/operators.
func TestIntervalFunctions(t *testing.T) {
	t.Parallel()

	runFnCases(t, []fnCase{
		{
			"make",
			"SELECT MAKE_INTERVAL(1, 6, 15), MAKE_INTERVAL(hour => 10, second => 20), " +
				"MAKE_INTERVAL(1, minute => 5, day => 2)",
			[][]any{{"1-6 15 0:0:0", "0-0 0 10:0:20", "1-0 2 0:5:0"}},
		},
		{
			"justify days",
			"SELECT JUSTIFY_DAYS(INTERVAL 29 DAY), JUSTIFY_DAYS(INTERVAL -30 DAY), JUSTIFY_DAYS(INTERVAL 31 DAY), " +
				"JUSTIFY_DAYS(INTERVAL -65 DAY), JUSTIFY_DAYS(INTERVAL 370 DAY)",
			[][]any{{"0-0 29 0:0:0", "-0-1 0 0:0:0", "0-1 1 0:0:0", "-0-2 -5 0:0:0", "1-0 10 0:0:0"}},
		},
		{
			"justify hours",
			"SELECT JUSTIFY_HOURS(INTERVAL 23 HOUR), JUSTIFY_HOURS(INTERVAL -24 HOUR), " +
				"JUSTIFY_HOURS(INTERVAL 47 HOUR), JUSTIFY_HOURS(INTERVAL -12345 MINUTE)",
			[][]any{{"0-0 0 23:0:0", "0-0 -1 0:0:0", "0-0 1 23:0:0", "0-0 -8 -13:45:0"}},
		},
		{
			"justify interval",
			"SELECT JUSTIFY_INTERVAL(INTERVAL '29 49:00:00' DAY TO SECOND)",
			[][]any{{"0-1 1 1:0:0"}},
		},
		{
			"extract",
			"SELECT EXTRACT(YEAR FROM i), EXTRACT(MONTH FROM i), EXTRACT(DAY FROM i), EXTRACT(HOUR FROM i), " +
				"EXTRACT(MINUTE FROM i), EXTRACT(SECOND FROM i), EXTRACT(MILLISECOND FROM i), " +
				"EXTRACT(MICROSECOND FROM i) FROM UNNEST([INTERVAL '1-2 3 4:5:6.789999' YEAR TO SECOND, " +
				"INTERVAL '0-13 370 48:61:61' YEAR TO SECOND]) AS i",
			[][]any{{"1", "2", "3", "4", "5", "6", "789", "789999"}, {"1", "1", "370", "49", "2", "1", "0", "0"}},
		},
		{
			"extract signs",
			"SELECT EXTRACT(HOUR FROM i), EXTRACT(MINUTE FROM i) " +
				"FROM UNNEST([INTERVAL '10 -12:30' DAY TO MINUTE]) AS i",
			[][]any{{"-12", "-30"}},
		},
		{
			"extract ym sign",
			"SELECT EXTRACT(YEAR FROM i), EXTRACT(MONTH FROM i) " +
				"FROM UNNEST([INTERVAL '-22-6 10 -12:30' YEAR TO MINUTE]) AS i",
			[][]any{{"-22", "-6"}},
		},
		{
			"date plus",
			"SELECT DATE '2021-04-20' + INTERVAL 25 HOUR, TIMESTAMP '2021-05-02 00:01:02.345+00' - INTERVAL 10 SECOND",
			[][]any{{"2021-04-21T01:00:00", "1.619913652345E9"}},
		},
		{
			"mul div",
			"SELECT INTERVAL '1:2:3' HOUR TO SECOND * 10, INTERVAL 35 SECOND * 4, INTERVAL 10 YEAR / 3, " +
				"INTERVAL 1 MONTH / 12",
			[][]any{{"0-0 0 10:20:30", "0-0 0 0:2:20", "3-4 0 0:0:0", "0-0 2 12:0:0"}},
		},
		{
			"diff",
			"SELECT DATE '2021-05-20' - DATE '2020-04-19', " +
				"TIMESTAMP '2021-06-01 12:34:56.789' - TIMESTAMP '2021-05-31 00:00:00'",
			[][]any{{"0-0 396 0:0:0", "0-0 0 36:34:56.789"}},
		},
		{
			"interval arithmetic",
			"SELECT INTERVAL 1 YEAR + INTERVAL 2 MONTH, INTERVAL 1 DAY - INTERVAL 2 DAY, -INTERVAL 3 HOUR, " +
				"2 * INTERVAL 1 DAY",
			[][]any{{"1-2 0 0:0:0", "0-0 -1 0:0:0", "0-0 0 -3:0:0", "0-0 2 0:0:0"}},
		},
		{
			"compare",
			"SELECT INTERVAL 1 MONTH = INTERVAL 30 DAY, INTERVAL 1 DAY < INTERVAL 25 HOUR, " +
				"INTERVAL 1 YEAR > INTERVAL 13 MONTH",
			[][]any{{"true", "true", "false"}},
		},
		{
			"cast",
			"SELECT CAST('1-2 3 4:5:6' AS INTERVAL), CAST('P1Y2M3DT4H5M6S' AS INTERVAL), " +
				"CAST(INTERVAL 5 DAY AS STRING)",
			[][]any{{"1-2 3 4:5:6", "1-2 3 4:5:6", "0-0 5 0:0:0"}},
		},
		{
			"parts",
			"SELECT INTERVAL 2 QUARTER, INTERVAL 1 WEEK, INTERVAL 1500 MILLISECOND, INTERVAL '2-11' YEAR TO MONTH, " +
				"INTERVAL '8 20 17' MONTH TO HOUR",
			[][]any{{"0-6 0 0:0:0", "0-0 7 0:0:0", "0-0 0 0:0:1.5", "2-11 0 0:0:0", "0-8 20 17:0:0"}},
		},
		{
			"null",
			"SELECT CAST(NULL AS INTERVAL) IS NULL, MAKE_INTERVAL(NULL)",
			[][]any{{"true", nil}},
		},
	})

	for name, sql := range map[string]string{
		"range":       "SELECT INTERVAL 10001 YEAR",
		"bad text":    "SELECT INTERVAL 'x' DAY TO SECOND",
		"bad range":   "SELECT INTERVAL '1' SECOND TO YEAR",
		"extract bad": "SELECT EXTRACT(WEEK FROM INTERVAL 1 DAY)",
		"cmp mixed":   "SELECT INTERVAL 1 DAY = 1",
		"div zero":    "SELECT INTERVAL 1 DAY / 0",
		"float part":  "SELECT INTERVAL 1.5 DAY",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), sql)
			require.Error(t, err)
		})
	}
}

// Examples from https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/search_functions and
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/text-analysis.
func TestSearchFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want      any
		name, sql string
	}{
		{
			"true",
			"delimiter terms",
			"SELECT SEARCH('foobar-example', 'foobar example')",
		},
		{
			"true",
			"constant query",
			"SELECT SEARCH('foobar-example', CONCAT('foo', 'bar'))",
		},
		{
			"false",
			"unsplit query",
			"SELECT SEARCH('foobar-example', 'foobarexample')",
		},
		{
			"true",
			"escaped delimiter",
			"SELECT SEARCH('foobar-example', 'foobar\\\\&example')",
		},
		{
			"true",
			"raw escape",
			"SELECT SEARCH('foobar-example', R'foobar\\&example')",
		},
		{
			"false",
			"backtick mismatch",
			"SELECT SEARCH('foobar-example', '`foobar&example`')",
		},
		{
			"true",
			"backtick exact",
			"SELECT SEARCH('foobar&example', '`foobar&example`')",
		},
		{
			"true",
			"order irrelevant",
			"SELECT SEARCH('foobar-example', 'example foobar')",
		},
		{
			"true",
			"case folded",
			"SELECT SEARCH('foobar-example', 'Foobar Example')",
		},
		{
			"false",
			"backtick case",
			"SELECT SEARCH('foobar-example', '`Foobar`')",
		},
		{
			"false",
			"backtick data",
			"SELECT SEARCH('`foobar-example`', '`foobar-example`')",
		},
		{
			"true",
			"backtick after delim",
			"SELECT SEARCH('foobar@example.com', '`example.com`')",
		},
		{
			"true",
			"backtick middle",
			"SELECT SEARCH('a foobar-example b', '`foobar-example`')",
		},
		{
			"false",
			"array fields",
			"SELECT SEARCH(['foobar', 'example'], 'foobar example')",
		},
		{
			"true",
			"array one field",
			"SELECT SEARCH(['foobar example', 'x'], 'foobar example')",
		},
		{
			"true",
			"backtick dot",
			"SELECT SEARCH('foobar.example', '`foobar`')",
		},
		{
			"true",
			"backtick dot2",
			"SELECT SEARCH('foobar..example', '`foobar.`')",
		},
		{
			"false",
			"backtick dot3",
			"SELECT SEARCH('foobar.example', '`foobar.`')",
		},
		{
			"true",
			"and",
			"SELECT SEARCH('foo bar baz', 'foo AND bar')",
		},
		{
			"true",
			"or",
			"SELECT SEARCH('foo', 'foo OR bar')",
		},
		{
			"false",
			"paren and",
			"SELECT SEARCH('foo', 'foo AND (bar OR baz)')",
		},
		{
			"true",
			"phrase",
			"SELECT SEARCH('foo bar baz', '\"foo bar\"')",
		},
		{
			"true",
			"phrase baz",
			"SELECT SEARCH('foo baz.bar', '\"foo baz\"')",
		},
		{
			"false",
			"phrase gap",
			"SELECT SEARCH('foo baz.bar', '\"foo bar\"')",
		},
		{
			"true",
			"phrase case",
			"SELECT SEARCH('Foo bar baz', '\"foo Bar\"')",
		},
		{
			"true",
			"phrase delimiter",
			"SELECT SEARCH('foo-bar baz', '\"foo&bar\"')",
		},
		{
			"false",
			"phrase backticks",
			"SELECT SEARCH('foo bar baz', '\"`foo bar`\"')",
		},
		{
			"false",
			"phrase far",
			"SELECT SEARCH('foo else bar', '\"foo bar\"')",
		},
		{
			"false",
			"term and phrase",
			"SELECT SEARCH('foo-bar-baz', 'bar \"foo&baz\"')",
		},
		{
			"false",
			"custom delimiter",
			"SELECT SEARCH('foo bar baz', '\"foo\\\\ bar\"', analyzer_options => '{\"delimiters\": [\" \"]}')",
		},
		{
			"true",
			"custom delimiter two",
			"SELECT SEARCH('foo? bar', '\"foo? bar\"', analyzer_options => '{\"delimiters\": [\" \", \"?\"]}')",
		},
		{
			"true",
			"stop words",
			"SELECT SEARCH('read the book now', '\"read the book\"', analyzer_options => " +
				"'{\"token_filters\": [{\"stop_words\": [\"the\"]}]}')",
		},
		{
			"false",
			"pattern phrase",
			"SELECT SEARCH('abcdef', '\"c d\"', analyzer => 'PATTERN_ANALYZER', analyzer_options => " +
				"'{\"patterns\": [\"(?:cd)|[a-z]\"]}')",
		},
		{
			"true",
			"pattern ant",
			"SELECT SEARCH('ant orange apple avocado', '\"ant apple\"', analyzer => 'PATTERN_ANALYZER', " +
				"analyzer_options => '{\"patterns\": [\"a[a-z]\"]}')",
		},
		{
			"false",
			"pattern default",
			"SELECT SEARCH('Please use foobar@example.com as your email.', 'exam', analyzer => 'PATTERN_ANALYZER')",
		},
		{
			"true",
			"pattern default hit",
			"SELECT SEARCH('Please use foobar@example.com as your email.', 'foobar', analyzer => 'PATTERN_ANALYZER')",
		},
		{
			"true",
			"noop equal",
			"SELECT SEARCH('foobar', 'foobar', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"false",
			"noop backtick",
			"SELECT SEARCH('foobar', '`foobar`', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"false",
			"noop partial",
			"SELECT SEARCH('foobar example', 'foobar', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"false",
			"noop or",
			"SELECT SEARCH('foo', 'foo OR bar', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"false",
			"email exam",
			"SELECT SEARCH('Please use foobar@example.com as your email.', 'exam')",
		},
		{
			"true",
			"email domain",
			"SELECT SEARCH('Please use foobar@example.com as your email.', 'example.com')",
		},
		{
			"false",
			"email phrase",
			"SELECT SEARCH('Please use foobar@example.com as your email.', '\"as email\"')",
		},
		{
			"false",
			"email custom",
			"SELECT SEARCH('Please use foobar@example.com as your email.', 'example', " +
				"analyzer_options => '{\"delimiters\": [\" \", \"@\"]}')",
		},
		{
			"true",
			"email custom full",
			"SELECT SEARCH('Please use foobar@example.com as your email.', 'foobar@example.com', " +
				"analyzer_options => '{\"delimiters\": [\" \", \"@\"]}')",
		},
		{
			"true",
			"phrase logical",
			"SELECT SEARCH(R'foo bar', R'\"foo AND (bar OR baz)\"')",
		},
		{
			"false",
			"phrase logical miss",
			"SELECT SEARCH(R'foo', R'\"foo AND (bar OR baz)\"')",
		},
		{
			"true",
			"phrase keeps literal and",
			"SELECT SEARCH(R'Bonnie and Clyde', R'\"Bonnie AND Clyde\"')",
		},
		{
			"true",
			"phrase keeps literal or",
			"SELECT SEARCH(R'stay in or out', R'\"in OR out\"')",
		},
		{
			"false",
			"phrase lowercase and is a term",
			"SELECT SEARCH(R'foo bar', R'\"foo and bar\"')",
		},
		{
			"true",
			"phrase unbalanced stays phrase",
			"SELECT SEARCH(R'foo and bar', R'\"foo AND ( bar\"')",
		},
		{
			"false",
			"phrase noop analyzer",
			"SELECT SEARCH(R'foo bar', R'\"foo AND bar\"', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"false",
			"phrase deep nesting",
			"SELECT SEARCH(R'x', R'\"" + strings.Repeat("(", 5000) + "a AND b" + strings.Repeat(")", 5000) + "\"')",
		},
		{
			"true",
			"phrase or",
			"SELECT SEARCH(R'foo bar', R'\"zzz OR bar\"')",
		},
		{
			"true",
			"implicit and",
			"SELECT SEARCH(R'foo bar baz', R'foo bar')",
		},
		{
			"true",
			"empty noop",
			"SELECT SEARCH('', '', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"false",
			"noop phrase",
			"SELECT SEARCH(R'foo bar baz', R'\"foo bar\"', analyzer => 'NO_OP_ANALYZER')",
		},
		{
			"true",
			"struct",
			"SELECT SEARCH(STRUCT('a b' AS x, 'soup' AS y), 'soup')",
		},
		{
			[]any{"true", "false"},
			"json values",
			"SELECT SEARCH(JSON '{\"k\": \"soup\"}', 'soup'), SEARCH(JSON '{\"soup\": 1}', 'soup')",
		},
		{
			"true",
			"json keys",
			"SELECT SEARCH(JSON '{\"soup\": 1}', 'soup', json_scope => 'JSON_KEYS')",
		},
		{
			"false",
			"non string",
			"SELECT SEARCH(1, '1')",
		},
		{
			nil,
			"null data",
			"SELECT SEARCH(CAST(NULL AS STRING), 'x')",
		},
		{
			"i|like|pie|you|like|pie|they|like|2|pies",
			"analyze log",
			"SELECT ARRAY_TO_STRING(TEXT_ANALYZE('I like pie, you like-pie, they like 2 PIEs.'), '|')",
		},
		{
			"like|pie|you|like|pie|they|like|pies",
			"analyze pattern",
			"SELECT ARRAY_TO_STRING(TEXT_ANALYZE('I like pie, you like-pie, they like 2 PIEs.', analyzer => " +
				"'PATTERN_ANALYZER'), '|')",
		},
		{
			"I like pie",
			"analyze noop",
			"SELECT ARRAY_TO_STRING(TEXT_ANALYZE('I like pie', analyzer => 'NO_OP_ANALYZER'), '|')",
		},
		{
			"i|like|❶|you|like|ño",
			"analyze icu",
			"SELECT ARRAY_TO_STRING(TEXT_ANALYZE('I like ❶ ② pies, you like Ño PIEs', analyzer_options => " +
				"'{\"token_filters\": [{\"normalizer\": {\"mode\": \"ICU_NORMALIZE\"}}, {\"stop_words\": " +
				"[\"pies\", \"2\"]}]}'), '|')",
		},
		{
			"i|like|❶|2|you|like|ño|pies",
			"analyze icu reorder",
			"SELECT ARRAY_TO_STRING(TEXT_ANALYZE('I like ❶ ② pies, you like Ño PIEs', analyzer_options => " +
				"'{\"token_filters\": [{\"stop_words\": [\"pies\", \"2\"]}, {\"normalizer\": {\"mode\": " +
				"\"ICU_NORMALIZE\"}}]}'), '|')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)

			want := []any{tt.want}
			if l, ok := tt.want.([]any); ok {
				want = l
			}

			assert.Equal(t, [][]any{want}, wireRows(res))
		})
	}

	for name, sql := range map[string]string{
		"null query":  "SELECT SEARCH('foobarexample', NULL)",
		"empty query": "SELECT SEARCH('foobarexample', '')",
		"not":         "SELECT SEARCH('a', 'NOT b')",
		"analyzer":    "SELECT SEARCH('a', 'a', analyzer => 'X')",
		"scope":       "SELECT SEARCH(JSON '{}', 'a', json_scope => 'X')",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, newCatalog(t), sql)
			require.Error(t, err)
		})
	}
}
