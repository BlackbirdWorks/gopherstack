package ocilogging_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingsearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dataRows(resp loggingsearch.SearchLogsResponse) []map[string]any {
	out := make([]map[string]any, 0, len(resp.Results))

	for _, r := range resp.Results {
		row, _ := (*r.Data).(map[string]any)
		out = append(out, row)
	}

	return out
}

func TestSearchProjectionOperators(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)
	start := common.SDKTime{Time: s.clock.Now().Add(-time.Hour)}
	end := common.SDKTime{Time: s.clock.Now().Add(time.Hour)}
	lvl := ` | where data.status > 0`

	tests := []struct {
		name  string
		tail  string
		want  []map[string]any
		field []string
	}{
		{
			"select with alias", lvl + ` | select data.level, data.status + 1 as next | sort by next asc | limit 2`,
			[]map[string]any{{"data.level": "INFO", "next": 201.0}, {"data.level": "WARN", "next": 405.0}},
			[]string{"data.level", "next"},
		},
		{
			"extend functions",
			lvl + ` | where data.status = 500 | select concat(upper(data.level), '-', substr(data.msg, 1, 2)) as tag`,
			[]map[string]any{{"tag": "ERROR-oo"}},
			[]string{"tag"},
		},
		{
			"dedup keeps newest",
			lvl + ` | dedup data.level | select data.level, data.status`,
			[]map[string]any{
				{"data.level": "ERROR", "data.status": 503.0},
				{"data.level": "WARN", "data.status": 404.0},
				{"data.level": "INFO", "data.status": 200.0},
			},
			[]string{"data.level", "data.status"},
		},
		{
			"summarize by",
			lvl + ` | summarize count() as n, sum(data.status) as total, max(data.status) as top by data.level` +
				` | sort by n desc, data.level asc`,
			[]map[string]any{{"data.level": "ERROR", "n": 2.0, "total": 1003.0, "top": 503.0},
				{"data.level": "INFO", "n": 1.0, "total": 200.0, "top": 200.0},
				{"data.level": "WARN", "n": 1.0, "total": 404.0, "top": 404.0}},
			[]string{"data.level", "n", "total", "top"},
		},
		{
			"summarize whole stream",
			` | summarize count() as n, avg(data.status) as mean, count(data.status) as withstatus`,
			[]map[string]any{{"n": 5.0, "mean": 401.75, "withstatus": 4.0}},
			[]string{"n", "mean", "withstatus"},
		},
		{
			"contains and not",
			` | where contains_ci(data.msg, 'BO', false) and not(isnull(data.level)) | select data.msg`,
			[]map[string]any{{"data.msg": "boom"}},
			[]string{"data.msg"},
		},
		{
			"arithmetic where", ` | where data.status / 100 = 5 | select data.msg`,
			[]map[string]any{{"data.msg": "boom"}}, []string{"data.msg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := s.search.SearchLogs(t.Context(), loggingsearch.SearchLogsRequest{
				TimeStart: &start, TimeEnd: &end, SearchQuery: new(s.scope + tt.tail), IsReturnFieldInfo: new(true),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, dataRows(resp))

			names := make([]string, 0, len(resp.Fields))
			for _, f := range resp.Fields {
				names = append(names, *f.FieldName)
			}

			assert.Equal(t, tt.field, names)
		})
	}
}

func TestSearchExtendKeepsEntries(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)
	start := common.SDKTime{Time: s.clock.Now().Add(-time.Hour)}
	end := common.SDKTime{Time: s.clock.Now().Add(time.Hour)}

	resp, err := s.search.SearchLogs(t.Context(), loggingsearch.SearchLogsRequest{
		TimeStart: &start, TimeEnd: &end,
		SearchQuery: new(s.scope + ` | where data.level = 'WARN' | extend lower(data.level) as lv`),
	})
	require.NoError(t, err)

	rows := dataRows(resp)
	require.Len(t, rows, 1)
	assert.Equal(t, "warn", rows[0]["lv"])
	assert.Contains(t, rows[0], "logContent")
	assert.Equal(t, fmt.Sprint(rows[0]["regionId"]), rows[0]["regionId"])
}
