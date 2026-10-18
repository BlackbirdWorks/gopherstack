package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestEngine_PrestoFunctions(t *testing.T) {
	t.Parallel()

	events := "id,payload,ts,tags\n" +
		"1,\"{\"\"user\"\":{\"\"name\"\":\"\"ann\"\"},\"\"n\"\":5}\",2024-03-05T10:30:00Z,a|b|c\n" +
		"2,\"{\"\"user\"\":{\"\"name\"\":\"\"bob\"\"},\"\"n\"\":7}\",2024-03-06T11:45:30Z,d\n"

	def := &athena.GlueTableDef{
		Name: "events", Location: "s3://lake/events/", SerdeLibrary: serdeCSV,
		Parameters: map[string]string{"skip.header.line.count": "1"},
		Columns: []athena.Column{
			{Name: "id", Type: "int"}, {Name: "payload", Type: "string"},
			{Name: "ts", Type: "string"}, {Name: "tags", Type: "string"},
		},
	}

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{
			"regexp_extract",
			"SELECT regexp_extract(tags, '([a-z])\\|([a-z])', 2) FROM events WHERE id = 1",
			[][]string{{"b"}},
		},
		{
			"regexp_extract no group",
			"SELECT regexp_extract(tags, '[a-c]+') FROM events WHERE id = 1",
			[][]string{{"a"}},
		},
		{"regexp_replace", "SELECT regexp_replace(tags, '\\|', '-') FROM events WHERE id = 1", [][]string{{"a-b-c"}}},
		{
			"json_extract_scalar",
			"SELECT json_extract_scalar(payload, '$.user.name') AS n FROM events ORDER BY id",
			[][]string{{"ann"}, {"bob"}},
		},
		{
			"json_extract_scalar number",
			"SELECT json_extract_scalar(payload, '$.n') AS n FROM events WHERE id = 2",
			[][]string{{"7"}},
		},
		{
			"json_extract keeps json",
			"SELECT json_extract(payload, '$.user') AS u FROM events WHERE id = 1",
			[][]string{{`{"name":"ann"}`}},
		},
		{
			"from_iso8601_timestamp",
			"SELECT date_trunc('hour', from_iso8601_timestamp(ts)) AS h FROM events WHERE id = 2",
			[][]string{{"2024-03-06 11:00:00.000"}},
		},
		{
			"date_parse",
			"SELECT date_parse('2024-03-05 10:30', '%Y-%m-%d %H:%i') AS d FROM events WHERE id = 1",
			[][]string{{"2024-03-05 10:30:00.000"}},
		},
		{
			"date_format",
			"SELECT date_format(from_iso8601_timestamp(ts), '%Y/%m/%d %H') AS d FROM events WHERE id = 1",
			[][]string{{"2024/03/05 10"}},
		},
		{
			"format_datetime",
			"SELECT format_datetime(from_iso8601_timestamp(ts), 'yyyy-MM-dd') AS d FROM events WHERE id = 1",
			[][]string{{"2024-03-05"}},
		},
		{
			"to_unixtime",
			"SELECT to_unixtime(from_iso8601_timestamp(ts)) AS u FROM events WHERE id = 1",
			[][]string{{"1709634600.0"}},
		},
		{
			"from_unixtime",
			"SELECT from_unixtime(1709634600) AS u FROM events WHERE id = 1",
			[][]string{{"2024-03-05 10:30:00.000"}},
		},
		{
			"day_of_week",
			"SELECT day_of_week(from_iso8601_timestamp(ts)) AS d FROM events WHERE id = 1",
			[][]string{{"2"}},
		},
		{"split element_at", "SELECT element_at(split(tags, '|'), 2) AS x FROM events WHERE id = 1", [][]string{{"b"}}},
		{
			"split negative index",
			"SELECT element_at(split(tags, '|'), -1) AS x FROM events WHERE id = 1",
			[][]string{{"c"}},
		},
		{"cardinality", "SELECT cardinality(split(tags, '|')) AS n FROM events ORDER BY id", [][]string{{"3"}, {"1"}}},
		{
			"array literal contains",
			"SELECT contains(ARRAY['x', 'y'], 'y') AS c FROM events WHERE id = 1",
			[][]string{{"1"}},
		},
		{"array_join", "SELECT array_join(ARRAY[1, 2, 3], '-') AS j FROM events WHERE id = 1", [][]string{{"1-2-3"}}},
		{"array_agg", "SELECT cardinality(array_agg(id)) AS n FROM events", [][]string{{"2"}}},
		{
			"unnest",
			"SELECT t FROM events CROSS JOIN UNNEST(split(tags, '|')) AS u(t) WHERE id = 1 ORDER BY t",
			[][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			"unnest ordinality",
			"SELECT t, n FROM events CROSS JOIN UNNEST(split(tags, '|')) WITH ORDINALITY AS u(t, n) WHERE id = 1 ORDER BY n",
			[][]string{{"a", "1"}, {"b", "2"}, {"c", "3"}},
		},
		{"unnest literal", "SELECT sum(x) AS s FROM UNNEST(ARRAY[1, 2, 3]) AS u(x)", [][]string{{"6"}}},
		{
			"unnest map",
			"SELECT k, v FROM UNNEST(MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS u(k, v) ORDER BY k",
			[][]string{{"a", "1"}, {"b", "2"}},
		},
		{"count_if", "SELECT count_if(id > 1) AS c FROM events", [][]string{{"1"}}},
		{"max_by", "SELECT max_by(id, ts) AS m FROM events", [][]string{{"2"}}},
		{"min_by", "SELECT min_by(id, ts) AS m FROM events", [][]string{{"1"}}},
		{"approx_percentile", "SELECT approx_percentile(id, 0.5) AS p FROM events", [][]string{{"1.0"}}},
		{"arbitrary", "SELECT arbitrary(id) AS a FROM events WHERE id = 2", [][]string{{"2"}}},
		{"stddev_pop", "SELECT stddev_pop(id) AS s FROM events", [][]string{{"0.5"}}},
		{"bool_or", "SELECT bool_or(id > 1) AS b FROM events", [][]string{{"1"}}},
		{"lpad", "SELECT lpad('7', 3, '0') AS p FROM events WHERE id = 1", [][]string{{"007"}}},
		{"reverse", "SELECT reverse('abc') AS r FROM events WHERE id = 1", [][]string{{"cba"}}},
		{"split_part", "SELECT split_part('a-b-c', '-', 2) AS s FROM events WHERE id = 1", [][]string{{"b"}}},
		{"md5", "SELECT md5('a') AS h FROM events WHERE id = 1", [][]string{{"0cc175b9c0f1b6a831c399e269772661"}}},
		{"to_hex", "SELECT to_hex('ab') AS h FROM events WHERE id = 1", [][]string{{"6162"}}},
		{
			"greatest least",
			"SELECT greatest(1, 5, 3) AS g, least(4, 2, 9) AS l FROM events WHERE id = 1",
			[][]string{{"5", "2"}},
		},
		{
			"url_extract_host",
			"SELECT url_extract_host('https://example.com/a?b=1') AS h FROM events WHERE id = 1",
			[][]string{{"example.com"}},
		},
		{
			"url_extract_parameter",
			"SELECT url_extract_parameter('https://e.com/a?b=1&c=2', 'c') AS p FROM events WHERE id = 1",
			[][]string{{"2"}},
		},
		{
			"right join",
			"SELECT a.id, b.id AS bid FROM events a RIGHT JOIN events b ON a.id = b.id - 1 ORDER BY b.id",
			[][]string{{"", "1"}, {"1", "2"}},
		},
		{
			"window",
			"SELECT id, row_number() OVER (ORDER BY id DESC) AS rn FROM events ORDER BY id",
			[][]string{{"1", "2"}, {"2", "1"}},
		},
		{"cte", "WITH x AS (SELECT id FROM events WHERE id > 1) SELECT count(*) AS c FROM x", [][]string{{"1"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newEngineBackend(t, map[string][]byte{"lake/events/a.csv": []byte(events)},
				map[string]*athena.GlueTableDef{"shop/events": def})

			id := start(t, b, tc.sql)
			qe := waitDone(t, b, id)
			require.Equal(t, "SUCCEEDED", qe.Status.State, qe.Status.StateChangeReason)
			assert.Equal(t, tc.want, resultRows(t, b, id))
		})
	}
}
