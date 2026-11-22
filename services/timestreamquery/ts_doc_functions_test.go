package timestreamquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

const docVals = `FROM (VALUES 1, 2, 3, 4) AS t(c)`

const docPairs = `FROM (VALUES (('a', 1)), (('b', 2)), (('c', 3)), (('d', 4))) AS t(c1, c2)`

const docXY = `FROM (VALUES ((1, 1)), ((2, 2)), ((3, 3)), ((4, 4))) AS t(c1, c2)`

// Examples and results come from the Timestream Developer Guide function reference pages.
func TestQuery_ReferenceFunctionExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"abs", `SELECT abs(-3)`, "3"},
		{"cbrt", `SELECT cbrt(27)`, "3.0"},
		{"ceil", `SELECT ceil(1.2)`, "2.0"},
		{"degrees", `SELECT degrees(pi())`, "180.0"},
		{"from_base", `SELECT from_base('ff', 16)`, "255"},
		{"from_base neg", `SELECT from_base('-101', 2)`, "-5"},
		{"to_base", `SELECT to_base(255, 16)`, "ff"},
		{"to_base neg", `SELECT to_base(-5, 2)`, "-101"},
		{"log2", `SELECT log2(8)`, "3.0"},
		{"mod", `SELECT mod(7, 3)`, "1"},
		{"sign", `SELECT sign(-4)`, "-1"},
		{"truncate", `SELECT truncate(2.7)`, "2.0"},
		{"infinity", `SELECT infinity()`, "Infinity"},
		{"is_nan", `SELECT is_nan(nan())`, "true"},
		{"strrpos", `SELECT strrpos('abcabc', 'b')`, "5"},
		{"strrpos inst", `SELECT strrpos('abcabcabc', 'b', 2)`, "5"},
		{"strrpos miss", `SELECT strrpos('abc', 'z')`, "0"},
		{"strpos inst", `SELECT strpos('abcabcabc', 'b', 2)`, "5"},
		{"hamming", `SELECT hamming_distance('abc', 'abd')`, "1"},
		{"split limit", `SELECT split('a,b,c', ',', 2)`, "[a, b,c]"},
		{"reverse str", `SELECT reverse('abc')`, "cba"},
		{"replace 2", `SELECT replace('abcb', 'b')`, "ac"},
		{"array_distinct", `SELECT array_distinct(ARRAY[1,2,2,3])`, "[1, 2, 3]"},
		{"array_intersect", `SELECT array_intersect(ARRAY[1,2,3], ARRAY[3,4,5])`, "[3]"},
		{"array_union", `SELECT array_union(ARRAY[1,2,3], ARRAY[3,4,5])`, "[1, 2, 3, 4, 5]"},
		{"array_except", `SELECT array_except(ARRAY[1,2,3], ARRAY[3,4,5])`, "[1, 2]"},
		{"array_join", `SELECT array_join(ARRAY[1,2,3], ';', '')`, "1;2;3"},
		{"array_max", `SELECT array_max(ARRAY[1,2,3])`, "3"},
		{"array_min", `SELECT array_min(ARRAY[1,2,3])`, "1"},
		{"array_position", `SELECT array_position(ARRAY[3,4,5,9], 5)`, "3"},
		{"array_remove", `SELECT array_remove(ARRAY[3,4,5,9], 4)`, "[3, 5, 9]"},
		{"array_sort", `SELECT array_sort(ARRAY[6,8,2,9,3])`, "[2, 3, 6, 8, 9]"},
		{"arrays_overlap", `SELECT arrays_overlap(ARRAY[6,8,2,9,3], ARRAY[6,8])`, "true"},
		{"cardinality", `SELECT cardinality(ARRAY[6,8,2,9,3])`, "5"},
		{
			"concat arrays",
			`SELECT concat(ARRAY[6,8,2,9,3], ARRAY[11,32], ARRAY[6,8,2,0,14])`,
			"[6, 8, 2, 9, 3, 11, 32, 6, 8, 2, 0, 14]",
		},
		{"element_at", `SELECT element_at(ARRAY[6,8,2,9,3], 1)`, "6"},
		{"repeat", `SELECT repeat(1, 3)`, "[1, 1, 1]"},
		{"reverse arr", `SELECT reverse(ARRAY[6,8,2,9,3])`, "[3, 9, 2, 8, 6]"},
		{"sequence 2", `SELECT sequence(3, 8)`, "[3, 4, 5, 6, 7, 8]"},
		{"sequence down", `SELECT sequence(8, 3)`, "[8, 7, 6, 5, 4, 3]"},
		{"sequence 3", `SELECT sequence(3, 15, 2)`, "[3, 5, 7, 9, 11, 13, 15]"},
		{
			"sequence ts 1d",
			`SELECT sequence('2023-04-02 19:26:12.941000000', '2023-04-04 19:26:12.941000000', 1d)`,
			"[2023-04-02 19:26:12.941000000, 2023-04-03 19:26:12.941000000, 2023-04-04 19:26:12.941000000]",
		},
		{"slice", `SELECT slice(ARRAY[6,8,2,9,3], 1, 3)`, "[6, 8, 2]"},
		{
			"zip",
			`SELECT zip(ARRAY[6,8,2,9,3], ARRAY[15,24])`,
			"[(6, 15), (8, 24), (2, <null>), (9, <null>), (3, <null>)]",
		},
		{"bit_count", `SELECT bit_count(19, 8)`, "3"},
		{"bitwise_and", `SELECT bitwise_and(12, 7)`, "4"},
		{"bitwise_not", `SELECT bitwise_not(12)`, "-13"},
		{"bitwise_or", `SELECT bitwise_or(12, 7)`, "15"},
		{"bitwise_xor", `SELECT bitwise_xor(12, 7)`, "11"},
		{"regexp_extract_all", `SELECT regexp_extract_all('example expect complex', 'ex\w')`, "[exa, exp]"},
		{"regexp_extract_all g", `SELECT regexp_extract_all('example expect complex', '(ex)(\w)', 2)`, "[a, p]"},
		{"regexp_extract", `SELECT regexp_extract('example expect', 'ex\w')`, "exa"},
		{"regexp_extract g", `SELECT regexp_extract('example expect', '(ex)(\w)', 2)`, "a"},
		{"regexp_like", `SELECT regexp_like('example', 'ex')`, "true"},
		{"regexp_replace 2", `SELECT regexp_replace('example expect', 'expect')`, "example "},
		{"regexp_replace 3", `SELECT regexp_replace('example expect', 'expect', 'surprise')`, "example surprise"},
		{"regexp_replace lambda", `SELECT regexp_replace('example', '(\w)', x -> upper(x[1]))`, "EXAMPLE"},
		{"regexp_split", `SELECT regexp_split('example', 'x')`, "[e, ample]"},
		{"arbitrary", `SELECT arbitrary(t.c) ` + docVals, "1"},
		{"array_agg", `SELECT array_agg(t.c) ` + docVals, "[1, 2, 3, 4]"},
		{"avg", `SELECT avg(t.c) ` + docVals, "2.5"},
		{"bool_and", `SELECT bool_and(t.c) FROM (VALUES true, true, false, true) AS t(c)`, "false"},
		{"bool_or", `SELECT bool_or(t.c) FROM (VALUES true, true, false, true) AS t(c)`, "true"},
		{"every", `SELECT every(t.c) FROM (VALUES true, true, false, true) AS t(c)`, "false"},
		{"count_if", `SELECT count_if(t.c) FROM (VALUES true, true, false, true) AS t(c)`, "3"},
		{"geometric_mean", `SELECT geometric_mean(t.c) ` + docVals, "2.213363839400643"},
		{"max_by", `SELECT max_by(t.c1, t.c2) ` + docPairs, "d"},
		{"max_by n", `SELECT max_by(t.c1, t.c2, 2) ` + docPairs, "[d, c]"},
		{"min_by", `SELECT min_by(t.c1, t.c2) ` + docPairs, "a"},
		{"min_by n", `SELECT min_by(t.c1, t.c2, 2) ` + docPairs, "[a, b]"},
		{"max n", `SELECT max(t.c, 2) ` + docVals, "[4, 3]"},
		{"min n", `SELECT min(t.c, 2) ` + docVals, "[1, 2]"},
		{"sum", `SELECT sum(t.c) ` + docVals, "10"},
		{"bitwise_and_agg", `SELECT bitwise_and_agg(t.c) FROM (VALUES 1, -3) AS t(c)`, "1"},
		{"bitwise_or_agg", `SELECT bitwise_or_agg(t.c) FROM (VALUES 1, -3) AS t(c)`, "-3"},
		{"approx_distinct", `SELECT approx_distinct(t.c) FROM (VALUES 1, 2, 3, 4, 8) AS t(c)`, "5"},
		{"approx_distinct e", `SELECT approx_distinct(t.c, 0.2) FROM (VALUES 1, 2, 3, 4, 8) AS t(c)`, "5"},
		{"approx_percentile", `SELECT approx_percentile(t.c, 0.4) ` + docVals, "2"},
		{"approx_percentile arr", `SELECT approx_percentile(t.c, ARRAY[0.1, 0.8, 0.8]) ` + docVals, "[1, 4, 4]"},
		{"approx_percentile w", `SELECT approx_percentile(t.c, 1, 0.1) ` + docVals, "1"},
		{"approx_percentile w arr", `SELECT approx_percentile(t.c, 1, ARRAY[0.1, 0.8, 0.8]) ` + docVals, "[1, 4, 4]"},
		{"approx_percentile w acc", `SELECT approx_percentile(t.c, 1, 0.1, 0.5) ` + docVals, "1"},
		{"corr", `SELECT corr(t.c1, t.c2) ` + docXY, "1.0"},
		{"covar_pop", `SELECT covar_pop(t.c1, t.c2) ` + docXY, "1.25"},
		{"covar_samp", `SELECT covar_samp(t.c1, t.c2) ` + docXY, "1.6666666666666667"},
		{"regr_intercept", `SELECT regr_intercept(t.c1, t.c2) ` + docXY, "0.0"},
		{"regr_slope", `SELECT regr_slope(t.c1, t.c2) ` + docXY, "1.0"},
		{"skewness", `SELECT skewness(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "0.8978957037987335"},
		{"stddev_pop", `SELECT stddev_pop(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "2.4166091947189146"},
		{"stddev_samp", `SELECT stddev_samp(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "2.701851217221259"},
		{"stddev", `SELECT stddev(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "2.701851217221259"},
		{"var_pop", `SELECT var_pop(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "5.840000000000001"},
		{"var_samp", `SELECT var_samp(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "7.300000000000001"},
		{"variance", `SELECT variance(t.c1) FROM (VALUES 1, 2, 3, 4, 8) AS t(c1)`, "7.300000000000001"},
		{"greatest", `SELECT greatest(1, 5, 3)`, "5"},
		{"least", `SELECT least(4, 2, 3)`, "2"},
		{"nullif", `SELECT nullif(1, 1)`, "<null>"},
		{"to_unixtime", `SELECT to_unixtime(TIMESTAMP '2022-06-17 17:44:43.771000000')`, "1.6554878837710001E9"},
		{"cume_dist", `SELECT cume_dist() OVER (ORDER BY t.c) ` + docVals + ` ORDER BY t.c LIMIT 1`, "0.25"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, _ := runFn(t, tt.query)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}

func runFnErr(t *testing.T, q string) error {
	t.Helper()

	_, err := fnBackend(t).QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})

	return err
}

func TestQuery_ReferenceFunctionEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"union null", `SELECT array_union(NULL, ARRAY[1])`, "<null>"},
		{"intersect null", `SELECT array_intersect(ARRAY[1], NULL)`, "<null>"},
		{"except dups", `SELECT array_except(ARRAY[1,1,2,2], ARRAY[3])`, "[1, 2]"},
		{"intersect order", `SELECT array_intersect(ARRAY[3,2,1,2], ARRAY[2,3])`, "[3, 2]"},
		{"union text", `SELECT array_union(ARRAY['a'], ARRAY['b','a'])`, "[a, b]"},
		{"intersect numeric eq", `SELECT array_intersect(ARRAY[1.0], ARRAY[1.0, 2.0])`, "[1.0]"},
		{"overlap null elem", `SELECT arrays_overlap(ARRAY[1, NULL], ARRAY[2])`, "<null>"},
		{"overlap hit with null", `SELECT arrays_overlap(ARRAY[1, NULL], ARRAY[1])`, "true"},
		{"overlap none", `SELECT arrays_overlap(ARRAY[1], ARRAY[2])`, "false"},
		{"overlap null arg", `SELECT arrays_overlap(NULL, ARRAY[2])`, "<null>"},
		{"repeat null elem", `SELECT repeat(NULL, 2)`, "[<null>, <null>]"},
		{"repeat null count", `SELECT repeat(1, NULL)`, "<null>"},
		{"repeat zero", `SELECT repeat(1, 0)`, "[]"},
		{"repeat limit", `SELECT cardinality(repeat(1, 10000))`, "10000"},
		{"repeat string", `SELECT repeat('ab', 2)`, "[ab, ab]"},
		{"shuffle size", `SELECT cardinality(shuffle(ARRAY[1,2,3,4]))`, "4"},
		{"shuffle null", `SELECT shuffle(NULL)`, "<null>"},
		{"zip three", `SELECT zip(ARRAY[1], ARRAY[2], ARRAY[3])`, "[(1, 2, 3)]"},
		{"zip null", `SELECT zip(ARRAY[1], NULL)`, "<null>"},
		{"reverse string", `SELECT reverse('abc')`, "cba"},
		{"concat strings", `SELECT concat('a', 'b')`, "ab"},
		{"concat empty arrays", `SELECT concat(ARRAY[1], ARRAY[2])`, "[1, 2]"},
		{"from_base upper", `SELECT from_base('FF', 16)`, "255"},
		{"from_base null", `SELECT from_base(NULL, 16)`, "<null>"},
		{"to_base null", `SELECT to_base(NULL, 16)`, "<null>"},
		{"to_base 36", `SELECT to_base(35, 36)`, "z"},
		{"strrpos empty", `SELECT strrpos('abc', '')`, "1"},
		{"strrpos null", `SELECT strrpos(NULL, 'a')`, "<null>"},
		{"strrpos unicode", `SELECT strrpos('héllo hé', 'hé')`, "7"},
		{"strpos instance miss", `SELECT strpos('abc', 'b', 2)`, "0"},
		{"replace null", `SELECT replace('abc', NULL)`, "<null>"},
		{"replace empty", `SELECT replace('abc', '', '-')`, "-a-b-c-"},
		{"hamming null", `SELECT hamming_distance(NULL, 'a')`, "<null>"},
		{"bit_count neg", `SELECT bit_count(-1, 8)`, "8"},
		{"bit_count max", `SELECT bit_count(-1, 64)`, "64"},
		{"bit_count null", `SELECT bit_count(NULL, 8)`, "<null>"},
		{"bitwise_not null", `SELECT bitwise_not(NULL)`, "<null>"},
		{"sequence one", `SELECT sequence(2, 2)`, "[2]"},
		{"sequence neg step", `SELECT sequence(5, 1, -2)`, "[5, 3, 1]"},
		{
			"sequence ts default step",
			`SELECT sequence(TIMESTAMP '2023-04-02 00:00:00', TIMESTAMP '2023-04-03 00:00:00')`,
			"[2023-04-02 00:00:00.000000000, 2023-04-03 00:00:00.000000000]",
		},
		{"bitwise_and_agg null", `SELECT bitwise_and_agg(c) FROM (VALUES 6, NULL, 3) AS t(c)`, "2"},
		{"bitwise_or_agg empty", `SELECT bitwise_or_agg(c) FROM (VALUES 6) AS t(c) WHERE c > 10`, "<null>"},
		{
			"bitwise_and_agg group",
			`SELECT bitwise_and_agg(c) FROM (VALUES (1, 3), (1, 5), (2, 7)) AS t(g, c) GROUP BY g ORDER BY g`,
			"1",
		},
		{"bitwise_or_agg filter", `SELECT bitwise_or_agg(c) FILTER (WHERE c < 4) FROM (VALUES 1, 2, 8) AS t(c)`, "3"},
		{"bitwise_or_agg distinct", `SELECT bitwise_or_agg(DISTINCT c) FROM (VALUES 1, 1, 2) AS t(c)`, "3"},
		{
			"bitwise_and_agg window",
			`SELECT bitwise_and_agg(c) OVER (ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ` +
				`FROM (VALUES 3, 6, 5) AS t(c) ORDER BY c DESC LIMIT 1`,
			"4",
		},
		{"skewness two rows", `SELECT skewness(c) FROM (VALUES 1, 2) AS t(c)`, "<null>"},
		{"skewness ignores null", `SELECT skewness(c) FROM (VALUES 1, 2, 3, 4, 8, NULL) AS t(c)`, "0.8978957037987335"},
		{"regr_slope no variance", `SELECT regr_slope(y, x) FROM (VALUES (1, 2), (2, 2)) AS t(y, x)`, "<null>"},
		{
			"regr_slope null pair",
			`SELECT regr_slope(y, x) FROM (VALUES (1, 1), (2, 2), (NULL, 9), (3, 3)) AS t(y, x)`,
			"1.0",
		},
		{"regr_intercept", `SELECT regr_intercept(y, x) FROM (VALUES (3, 1), (5, 2), (7, 3)) AS t(y, x)`, "1.0"},
		{
			"regr_slope group",
			`SELECT regr_slope(y, x) FROM (VALUES (1, 1, 1), (2, 2, 1), (4, 1, 2), (8, 2, 2)) AS t(y, x, g) ` +
				`GROUP BY g ORDER BY g`,
			"1.0",
		},
		{
			"max_by n null key",
			`SELECT max_by(a, b, 2) FROM (VALUES ('x', 1), ('y', NULL), ('z', 3)) AS t(a, b)`,
			"[z, x]",
		},
		{"max_by n more than rows", `SELECT max_by(a, b, 5) FROM (VALUES ('x', 1), ('z', 3)) AS t(a, b)`, "[z, x]"},
		{"min_by n empty", `SELECT min_by(a, b, 2) FROM (VALUES ('x', 1)) AS t(a, b) WHERE b > 5`, "<null>"},
		{
			"min_by n group",
			`SELECT min_by(a, b, 1) FROM (VALUES ('x', 1, 1), ('y', 2, 1), ('z', 0, 2)) AS t(a, b, g) GROUP BY g ORDER BY g`,
			"[x]",
		},
		{"weighted pct", `SELECT approx_percentile(c, w, 0.5) FROM (VALUES (1, 1), (10, 9)) AS t(c, w)`, "10"},
		{"weighted pct heavy low", `SELECT approx_percentile(c, w, 0.5) FROM (VALUES (1, 9), (10, 1)) AS t(c, w)`, "1"},
		{
			"weighted pct array",
			`SELECT approx_percentile(c, w, ARRAY[0.05, 0.95]) FROM (VALUES (1, 1), (10, 9)) AS t(c, w)`,
			"[1, 10]",
		},
		{"weighted pct accuracy", `SELECT approx_percentile(c, 1, 0.5, 0.01) FROM (VALUES 1, 2, 3) AS t(c)`, "2"},
		{"weighted pct empty", `SELECT approx_percentile(c, 1, 0.5) FROM (VALUES 1) AS t(c) WHERE c > 5`, "<null>"},
		{"weighted pct null value", `SELECT approx_percentile(c, 1, 0.5) FROM (VALUES 1, NULL, 3) AS t(c)`, "1"},
		{"approx_distinct nulls", `SELECT approx_distinct(c, 0.1) FROM (VALUES 1, 1, NULL, 2) AS t(c)`, "2"},
		{"approx_distinct text", `SELECT approx_distinct(c, 0.05) FROM (VALUES 'a', 'b', 'a') AS t(c)`, "2"},
		{"approx_distinct empty", `SELECT approx_distinct(c, 0.05) FROM (VALUES 'a') AS t(c) WHERE c = 'z'`, "0"},
		{
			"regexp lambda groups",
			`SELECT regexp_replace('a1b22', '([a-z])(\d+)', x -> x[2] || x[1])`, "1a22b",
		},
		{"regexp lambda null subject", `SELECT regexp_replace(NULL, 'a', x -> 'z')`, "<null>"},
		{"regexp lambda no match", `SELECT regexp_replace('abc', '(z)', x -> upper(x[1]))`, "abc"},
		{
			"regexp lambda nested",
			`SELECT regexp_replace('ab', '(a)', x -> regexp_replace(x[1], '(a)', y -> 'Z'))`,
			"Zb",
		},
		{"regexp lambda literal", `SELECT regexp_replace('x -> (a)', '(a)', y -> 'b')`, "x -> (b)"},
		{"extract dow", `SELECT extract(DOW FROM TIMESTAMP '2019-10-12 23:10:34')`, "6"},
		{"extract yow", `SELECT extract(YOW FROM TIMESTAMP '2019-01-01 00:00:00')`, "2019"},
		{"extract quarter", `SELECT extract(QUARTER FROM TIMESTAMP '2019-10-12 23:10:34')`, "4"},
		{"extract week", `SELECT extract(WEEK FROM TIMESTAMP '2019-10-12 23:10:34')`, "41"},
		{"extract doy", `SELECT extract(DOY FROM TIMESTAMP '2019-10-12 23:10:34')`, "285"},
		{"extract day_of_year", `SELECT extract(DAY_OF_YEAR FROM TIMESTAMP '2019-10-12 23:10:34')`, "285"},
		{"extract day_of_month", `SELECT extract(DAY_OF_MONTH FROM TIMESTAMP '2019-10-12 23:10:34')`, "12"},
		{"extract day_of_week", `SELECT extract(DAY_OF_WEEK FROM DATE '2019-10-12')`, "6"},
		{"extract nested", `SELECT extract(QUARTER FROM date_add('month', 3, TIMESTAMP '2019-10-12 23:10:34'))`, "1"},
		{"extract literal", `SELECT 'extract(DOW FROM x)'`, "extract(DOW FROM x)"},
		{"position", `SELECT position('b' IN 'abc')`, "2"},
		{"position missing", `SELECT position('z' IN 'abc')`, "0"},
		{"position expr", `SELECT position(lower('B') IN upper('abc') || 'b')`, "4"},
		{"position literal", `SELECT 'position(a IN b)'`, "position(a IN b)"},
		{"hour of time", `SELECT hour(TIME '10:20:30.5')`, "10"},
		{"minute of time", `SELECT minute(TIME '10:20:30')`, "20"},
		{"second of time", `SELECT second(TIME '10:20:30')`, "30"},
		{"millisecond of time", `SELECT millisecond(TIME '10:20:30.5')`, "500"},
		{"nanosecond of time", `SELECT nanosecond(TIME '10:20:30.5')`, "500000000"},
		{"day of interval", `SELECT day(1d + 2h)`, "1"},
		{"day_of_month of interval", `SELECT day_of_month(3d)`, "3"},
		{"hour of interval", `SELECT hour(1d + 2h)`, "2"},
		{"minute of interval", `SELECT minute(90m)`, "30"},
		{"second of interval", `SELECT second(90s)`, "30"},
		{"millisecond of interval", `SELECT millisecond(1500ms)`, "500"},
		{"nanosecond of interval", `SELECT nanosecond(5ns)`, "5"},
		{"hour of negative interval", `SELECT hour(-36h)`, "-12"},
		{"month of interval", `SELECT month(14month)`, "2"},
		{"year of interval", `SELECT year(14month)`, "1"},
		{"date_add time", `SELECT date_add('hour', 1, TIME '10:00:00')`, "11:00:00.000000000"},
		{"date_add time wraps", `SELECT date_add('hour', 30, TIME '10:00:00')`, "16:00:00.000000000"},
		{"date_add time negative", `SELECT date_add('minute', -61, TIME '00:30:00')`, "23:29:00.000000000"},
		{"date_add time null", `SELECT date_add('hour', NULL, TIME '10:00:00')`, "<null>"},
		{"date_diff time", `SELECT date_diff('second', TIME '10:00:00', TIME '10:01:00')`, "60"},
		{"date_diff time negative", `SELECT date_diff('hour', TIME '10:00:00', TIME '08:30:00')`, "-1"},
		{"all true", `SELECT 1 < ALL (VALUES 2, 3)`, "true"},
		{"all false", `SELECT 3 > ALL (VALUES 1, 5)`, "false"},
		{"all empty", `SELECT 3 > ALL (SELECT 1 WHERE false)`, "true"},
		{"all null", `SELECT 3 > ALL (VALUES 1, NULL)`, "<null>"},
		{"all false beats null", `SELECT 3 > ALL (VALUES 5, NULL)`, "false"},
		{"any true", `SELECT 5 > ANY (VALUES 2, 9)`, "true"},
		{"any empty", `SELECT 3 > ANY (SELECT 1 WHERE false)`, "false"},
		{"any null", `SELECT 3 > ANY (VALUES 5, NULL)`, "<null>"},
		{"any true beats null", `SELECT 3 > ANY (VALUES 1, NULL)`, "true"},
		{"some equal", `SELECT 1 = SOME (VALUES 1, 2)`, "true"},
		{"any select", `SELECT 'a' = ANY (SELECT host FROM db.t)`, "true"},
		{
			"all in where",
			`SELECT count(*) FROM db.t WHERE measure_value::double >= ALL (SELECT measure_value::double FROM db.t)`,
			"1",
		},
		{
			"all with and",
			`SELECT count(*) FROM db.t WHERE host = 'a' AND measure_value::double < ANY (VALUES 15, 25)`,
			"2",
		},
		{"quantified literal", `SELECT 'x > ALL (VALUES 1)'`, "x > ALL (VALUES 1)"},
		{"values scalar rows", `SELECT sum(c) FROM (VALUES 1, 2, 3) AS t(c)`, "6"},
		{"values row rows", `SELECT sum(a * b) FROM (VALUES (1, 2), (3, 4)) AS t(a, b)`, "14"},
		{"values paired rows", `SELECT sum(a * b) FROM (VALUES ((1, 2)), ((3, 4))) AS t(a, b)`, "14"},
		{"values expr rows", `SELECT sum(c) FROM (VALUES (1 + 2) * 3, 4) AS t(c)`, "13"},
		{"values string with comma", `SELECT count(*) FROM (VALUES 'a, b', 'c') AS t(c)`, "2"},
		{"values in literal", `SELECT 'VALUES 1, 2'`, "VALUES 1, 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, _ := runFn(t, tt.query)
			require.NotEmpty(t, rows)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}

func TestQuery_ReferenceFunctionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"repeat negative",
			`SELECT repeat(1, -1)`,
			"count argument of repeat function must be greater than or equal to 0",
		},
		{"repeat too big", `SELECT repeat(1, 10001)`, "must be less than or equal to 10000"},
		{"array_union scalar", `SELECT array_union(1, ARRAY[1])`, "operand does not match its static type"},
		{"zip one array", `SELECT zip(ARRAY[1])`, "wrong number of arguments"},
		{
			"zip six arrays",
			`SELECT zip(ARRAY[1], ARRAY[1], ARRAY[1], ARRAY[1], ARRAY[1], ARRAY[1])`,
			"wrong number of arguments",
		},
		{"from_base bad digit", `SELECT from_base('zz', 16)`, "Not a valid base-16 number: zz"},
		{"from_base overflow", `SELECT from_base('ffffffffffffffffff', 16)`, "Not a valid base-16 number"},
		{"from_base radix", `SELECT from_base('1', 1)`, "Radix must be between 2 and 36"},
		{"to_base radix", `SELECT to_base(1, 37)`, "Radix must be between 2 and 36"},
		{"strrpos instance", `SELECT strrpos('a', 'a', 0)`, "'instance' must be a positive number"},
		{"strpos instance", `SELECT strpos('a', 'a', -1)`, "'instance' must be a positive number"},
		{"hamming length", `SELECT hamming_distance('a', 'ab')`, "must have the same length"},
		{
			"bit_count range",
			`SELECT bit_count(19, 2)`,
			"Number must be representable with the bits specified. 19 can not be represented with 2 bits",
		},
		{"bit_count bits low", `SELECT bit_count(1, 1)`, "Bits specified in bit_count must be between 2 and 64, got 1"},
		{
			"bit_count bits high",
			`SELECT bit_count(1, 65)`,
			"Bits specified in bit_count must be between 2 and 64, got 65",
		},
		{
			"max_by n zero",
			`SELECT max_by(a, b, 0) FROM (VALUES ('x', 1)) AS t(a, b)`,
			"third argument of max_by must be a positive integer",
		},
		{
			"min_by n big",
			`SELECT min_by(a, b, 10001) FROM (VALUES ('x', 1)) AS t(a, b)`,
			"third argument of min_by must be less than or equal to 10000; found 10001",
		},
		{
			"weight zero",
			`SELECT approx_percentile(c, 0, 0.5) FROM (VALUES 1) AS t(c)`,
			"weight must be an integer of at least one",
		},
		{
			"weighted percentage",
			`SELECT approx_percentile(c, 1, 1.5) FROM (VALUES 1) AS t(c)`,
			"percentage must be between 0 and 1",
		},
		{
			"weighted accuracy",
			`SELECT approx_percentile(c, 1, 0.5, 1.0) FROM (VALUES 1) AS t(c)`,
			"accuracy must be greater than 0 and less than 1",
		},
		{
			"approx_distinct error",
			`SELECT approx_distinct(c, 0.001) FROM (VALUES 1) AS t(c)`,
			"Max standard error must be in [0.0040625, 0.26000]: 0.001",
		},
		{"regexp bad pattern", `SELECT regexp_replace('a', '(', x -> 'b')`, "Invalid regular expression"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := runFnErr(t, tt.query)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestSDKQuery_ReferenceFunctions(t *testing.T) {
	t.Parallel()

	client := newSDKQueryClient(t)

	tests := []struct {
		name  string
		query string
		typ   string
		value string
	}{
		{"array_intersect", `SELECT array_intersect(ARRAY[1,2,3], ARRAY[3,4,5])`, "ARRAY(INTEGER)", "[3]"},
		{"array_union", `SELECT array_union(ARRAY['a'], ARRAY['b','a'])`, "ARRAY(VARCHAR)", "[a,b]"},
		{"array_except", `SELECT array_except(ARRAY[1,2,3], ARRAY[3])`, "ARRAY(INTEGER)", "[1,2]"},
		{"repeat", `SELECT repeat('ab', 2)`, "ARRAY(VARCHAR)", "[ab,ab]"},
		{"repeat double", `SELECT repeat(1.5, 2)`, "ARRAY(DOUBLE)", "[1.5,1.5]"},
		{"zip", `SELECT zip(ARRAY[1,2], ARRAY['a'])`, "ARRAY(ROW(:INTEGER,:VARCHAR))", "[(1,a),(2,null)]"},
		{"from_base", `SELECT from_base('ff', 16)`, "BIGINT", "255"},
		{"to_base", `SELECT to_base(255, 2)`, "VARCHAR", "11111111"},
		{"strrpos", `SELECT strrpos('abcabc', 'b')`, "BIGINT", "5"},
		{"strpos instance", `SELECT strpos('abcabc', 'b', 2)`, "BIGINT", "5"},
		{"bit_count", `SELECT bit_count(9, 64)`, "BIGINT", "2"},
		{"quantified all", `SELECT 1 < ALL (VALUES 2, 3)`, "BOOLEAN", "true"},
		{"position", `SELECT position('b' IN 'abc')`, "BIGINT", "2"},
		{"extract dow", `SELECT extract(DOW FROM TIMESTAMP '2019-10-12 23:10:34')`, "BIGINT", "6"},
		{"hour of time", `SELECT hour(TIME '10:20:30')`, "BIGINT", "10"},
		{"hour of interval", `SELECT hour(1d + 2h)`, "BIGINT", "2"},
		{"date_add time", `SELECT date_add('hour', 1, TIME '10:00:00')`, "TIME", "11:00:00.000000000"},
		{"bitwise_not", `SELECT bitwise_not(0)`, "BIGINT", "-1"},
		{"hamming_distance", `SELECT hamming_distance('a','b')`, "BIGINT", "1"},
		{"arrays_overlap", `SELECT arrays_overlap(ARRAY[1], ARRAY[2])`, "BOOLEAN", "false"},
		{"infinity", `SELECT infinity()`, "DOUBLE", "Infinity"},
		{"nan", `SELECT nan()`, "DOUBLE", "NaN"},
		{"nan arithmetic", `SELECT nan() + 1`, "DOUBLE", "NaN"},
		{"nan compare", `SELECT nan() = nan()`, "BOOLEAN", "false"},
		{"sqrt negative", `SELECT sqrt(-1)`, "DOUBLE", "NaN"},
		{"ln zero", `SELECT ln(0)`, "DOUBLE", "-Infinity"},
		{"inf minus inf", `SELECT infinity() - infinity()`, "DOUBLE", "NaN"},
		{"replace two args", `SELECT replace('abc','b')`, "VARCHAR", "ac"},
		{"reverse array", `SELECT reverse(ARRAY[1,2,3])`, "ARRAY(INTEGER)", "[3,2,1]"},
		{"concat arrays", `SELECT concat(ARRAY[1], ARRAY[2])`, "ARRAY(INTEGER)", "[1,2]"},
		{"sequence down", `SELECT sequence(3, 1)`, "ARRAY(INTEGER)", "[3,2,1]"},
		{"regexp_replace lambda", `SELECT regexp_replace('abc', '(b)', x -> upper(x[1]))`, "VARCHAR", "aBc"},
		{
			"bitwise_and_agg",
			`SELECT bitwise_and_agg(measure_value::bigint) FROM db.t WHERE measure_name = 'n'`,
			"BIGINT",
			"7",
		},
		{
			"bitwise_or_agg",
			`SELECT bitwise_or_agg(measure_value::bigint) FROM db.t WHERE measure_name = 'n'`,
			"BIGINT",
			"7",
		},
		{
			"skewness",
			`SELECT skewness(measure_value::double) FROM db.t WHERE measure_name = 'cpu'`,
			"DOUBLE",
			"0.4261133876306897",
		},
		{
			"regr_slope",
			`SELECT regr_slope(measure_value::double, measure_value::double) FROM db.t WHERE measure_name = 'cpu'`,
			"DOUBLE",
			"1.0",
		},
		{
			"regr_intercept",
			`SELECT regr_intercept(measure_value::double, measure_value::double) FROM db.t WHERE measure_name = 'cpu'`,
			"DOUBLE",
			"0.0",
		},
		{
			"max_by n",
			`SELECT max_by(measure_value::double, time, 2) FROM db.t WHERE measure_name = 'cpu'`,
			"ARRAY(DOUBLE)",
			"[30.0,10.0]",
		},
		{
			"min_by n",
			`SELECT min_by(measure_value::double, time, 2) FROM db.t WHERE measure_name = 'cpu'`,
			"ARRAY(DOUBLE)",
			"[1.0,10.0]",
		},
		{
			"approx_percentile weight",
			`SELECT approx_percentile(measure_value::double, 2, 0.5) FROM db.t WHERE measure_name = 'cpu'`,
			"DOUBLE",
			"10.0",
		},
		{
			"approx_percentile weight array",
			`SELECT approx_percentile(measure_value::double, 1, ARRAY[0.1, 0.9]) FROM db.t WHERE measure_name = 'cpu'`,
			"ARRAY(DOUBLE)",
			"[1.0,30.0]",
		},
		{
			"approx_distinct error bound",
			`SELECT approx_distinct(host, 0.1) FROM db.t WHERE measure_name = 'cpu'`,
			"BIGINT",
			"1",
		},
		{"interval add", `SELECT 1h + 30m`, "INTERVAL_DAY_TO_SECOND", "0 01:30:00.000000000"},
		{"interval sub", `SELECT 1h - 90m`, "INTERVAL_DAY_TO_SECOND", "-0 00:30:00.000000000"},
		{"interval mul", `SELECT 2 * 1h`, "INTERVAL_DAY_TO_SECOND", "0 02:00:00.000000000"},
		{"interval div", `SELECT 1h / 4`, "INTERVAL_DAY_TO_SECOND", "0 00:15:00.000000000"},
		{"interval year month", `SELECT 1year + 2month`, "INTERVAL_YEAR_TO_MONTH", "1-2"},
		{"interval month literal", `SELECT 12month`, "INTERVAL_YEAR_TO_MONTH", "1-0"},
		{"ago interval sum", `SELECT ago(2m + 30s)`, "TIMESTAMP", "2026-10-31 11:57:30.000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			typ, value := queryLabels(t, client, tt.query)
			assert.Equal(t, tt.typ, typ)
			assert.Equal(t, tt.value, value)
		})
	}
}
