package cloudwatchlogs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// runInsights starts a query against the group, returns result rows as maps.
func runInsights(
	t *testing.T, b *cloudwatchlogs.InMemoryBackend, query string,
) []map[string]string {
	t.Helper()

	qid := "q-" + t.Name()
	_, err := b.StartQuery(context.Background(), qid, query, []string{"/j"}, 0, 0)
	require.NoError(t, err)

	results, _, _, err := b.GetQueryResults(qid)
	require.NoError(t, err)

	rows := make([]map[string]string, 0, len(results))
	for _, r := range results {
		m := make(map[string]string, len(r))
		for _, f := range r {
			m[f.Field] = f.Value
		}

		rows = append(rows, m)
	}

	return rows
}

// jsonInsightsBackend seeds a backend with three JSON log events.
func jsonInsightsBackend(t *testing.T) *cloudwatchlogs.InMemoryBackend {
	t.Helper()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "/j", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "/j", "s")
	require.NoError(t, err)

	_, err = b.PutLogEvents(context.Background(), "/j", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: `{"service":"api","latency":100,"status":200}`, Timestamp: 1000},
		{Message: `{"service":"api","latency":200,"status":500}`, Timestamp: 2000},
		{Message: `{"service":"db","latency":50,"status":200}`, Timestamp: 3000},
	})
	require.NoError(t, err)

	return b
}

func TestInsightsEngine_FilterComparisons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantLen int
	}{
		{name: "eq", query: "filter status = 200", wantLen: 2},
		{name: "neq", query: "filter status != 200", wantLen: 1},
		{name: "gt", query: "filter latency > 100", wantLen: 1},
		{name: "gte", query: "filter latency >= 100", wantLen: 2},
		{name: "lt", query: "filter latency < 100", wantLen: 1},
		{name: "and", query: `filter status = 200 and service = "api"`, wantLen: 1},
		{name: "or", query: `filter status = 500 or service = "db"`, wantLen: 2},
		{name: "not", query: "filter not (status = 200)", wantLen: 1},
		{name: "paren", query: `filter (status = 200 or status = 500) and service = "api"`, wantLen: 2},
		{name: "in", query: `filter service in ("api")`, wantLen: 2},
		{name: "not_in", query: `filter service not in ("api")`, wantLen: 1},
		{name: "like_regex", query: "filter service like /ap/", wantLen: 2},
		{name: "like_string", query: `filter service like "db"`, wantLen: 1},
		{name: "not_like", query: "filter service not like /ap/", wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := jsonInsightsBackend(t)
			rows := runInsights(t, b, tt.query)
			assert.Len(t, rows, tt.wantLen)
		})
	}
}

func TestInsightsEngine_Stats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  map[string]string
		name  string
		query string
		byKey string
		byVal string
		rows  int
	}{
		{
			name:  "count_by_service_api",
			query: "stats count(*) as c by service",
			byKey: "service", byVal: "api",
			want: map[string]string{"c": "2"}, rows: 2,
		},
		{
			name:  "avg_latency_by_service_db",
			query: "stats avg(latency) as a by service",
			byKey: "service", byVal: "db",
			want: map[string]string{"a": "50"}, rows: 2,
		},
		{
			name:  "sum_no_by",
			query: "stats sum(latency) as total",
			byKey: "", byVal: "",
			want: map[string]string{"total": "350"}, rows: 1,
		},
		{
			name:  "count_distinct",
			query: "stats count_distinct(status) as d",
			byKey: "", byVal: "",
			want: map[string]string{"d": "2"}, rows: 1,
		},
		{
			name:  "min_max",
			query: "stats min(latency) as lo, max(latency) as hi",
			byKey: "", byVal: "",
			want: map[string]string{"lo": "50", "hi": "200"}, rows: 1,
		},
		{
			name:  "pct_median",
			query: "stats pct(latency, 50) as p50",
			byKey: "", byVal: "",
			want: map[string]string{"p50": "100"}, rows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := jsonInsightsBackend(t)
			rows := runInsights(t, b, tt.query)
			require.Len(t, rows, tt.rows)

			row := rows[0]
			if tt.byKey != "" {
				row = findRow(t, rows, tt.byKey, tt.byVal)
			}

			for k, v := range tt.want {
				assert.Equal(t, v, row[k], "field %s", k)
			}
		})
	}
}

// findRow returns the result row whose key column equals val.
func findRow(t *testing.T, rows []map[string]string, key, val string) map[string]string {
	t.Helper()

	for _, r := range rows {
		if r[key] == val {
			return r
		}
	}

	require.Failf(t, "row not found", "no row with %s=%s", key, val)

	return nil
}

func TestInsightsEngine_MultiGroupBy(t *testing.T) {
	t.Parallel()

	b := jsonInsightsBackend(t)
	rows := runInsights(t, b, "stats count(*) as c by service, status")

	// (api,200),(api,500),(db,200) => 3 distinct groups.
	assert.Len(t, rows, 3)
}

func TestInsightsEngine_Arithmetic(t *testing.T) {
	t.Parallel()

	b := jsonInsightsBackend(t)
	rows := runInsights(t, b, `fields service, latency*2 as double | filter service = "db"`)
	require.Len(t, rows, 1)
	assert.Equal(t, "100", rows[0]["double"])
}

func TestInsightsEngine_SortMultiKey(t *testing.T) {
	t.Parallel()

	b := jsonInsightsBackend(t)
	rows := runInsights(t, b, "fields service, latency | sort service asc, latency desc")
	require.Len(t, rows, 3)

	// api rows first (asc service), within api latency desc: 200 then 100; then db.
	assert.Equal(t, "api", rows[0]["service"])
	assert.Equal(t, "200", rows[0]["latency"])
	assert.Equal(t, "api", rows[1]["service"])
	assert.Equal(t, "100", rows[1]["latency"])
	assert.Equal(t, "db", rows[2]["service"])
}

func TestInsightsEngine_Dedup(t *testing.T) {
	t.Parallel()

	b := jsonInsightsBackend(t)
	rows := runInsights(t, b, "fields service | dedup service")
	assert.Len(t, rows, 2)
}

func TestInsightsEngine_JSONFieldExtraction(t *testing.T) {
	t.Parallel()

	b := jsonInsightsBackend(t)
	rows := runInsights(t, b, `fields service, status | filter status = 500`)
	require.Len(t, rows, 1)
	assert.Equal(t, "api", rows[0]["service"])
	assert.Equal(t, "500", rows[0]["status"])
}

func TestInsightsEngine_Parse(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "/j", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "/j", "s")
	require.NoError(t, err)
	_, err = b.PutLogEvents(context.Background(), "/j", "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: "GET /api 200 130", Timestamp: 1000},
		{Message: "POST /login 401 55", Timestamp: 2000},
	})
	require.NoError(t, err)

	tests := []struct {
		name      string
		query     string
		wantField string
		wantVal   string
		wantLen   int
	}{
		{
			name:      "glob",
			query:     `parse @message "* * * *" as method, path, code, ms | fields method | filter code = 200`,
			wantField: "method", wantVal: "GET", wantLen: 1,
		},
		{
			name:      "regex_named",
			query:     `parse @message /(?<method>\w+) (?<path>\S+)/ | fields method, path | filter method = "POST"`,
			wantField: "path", wantVal: "/login", wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qid := "q-" + tt.name
			_, startErr := b.StartQuery(context.Background(), qid, tt.query, []string{"/j"}, 0, 0)
			require.NoError(t, startErr)

			results, _, _, getErr := b.GetQueryResults(qid)
			require.NoError(t, getErr)
			require.Len(t, results, tt.wantLen)

			got := ""
			for _, f := range results[0] {
				if f.Field == tt.wantField {
					got = f.Value
				}
			}

			assert.Equal(t, tt.wantVal, got)
		})
	}
}
