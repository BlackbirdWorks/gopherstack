package ocilogging_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/oracle/oci-go-sdk/v65/loggingsearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type searchEnv struct {
	*env
	group logging.LogGroup
	log   logging.Log
	scope string
}

// seededSearch ingests five entries spaced ten seconds apart, oldest first.
func seededSearch(t *testing.T) *searchEnv {
	t.Helper()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "grp")
	l := e.mkLog(t, g, "app", nil)

	for _, s := range []struct {
		data string
		off  time.Duration
	}{
		{`{"level":"INFO","status":200,"msg":"started"}`, -50 * time.Second},
		{`{"level":"ERROR","status":500,"msg":"boom"}`, -40 * time.Second},
		{`{"level":"WARN","status":404,"msg":"missing"}`, -30 * time.Second},
		{`{"level":"ERROR","status":503,"msg":"overloaded"}`, -20 * time.Second},
		{`plain ERROR line`, -10 * time.Second},
	} {
		e.put(t, l, s.off, s.data)
	}

	return &searchEnv{
		env:   e,
		group: g,
		log:   l,
		scope: fmt.Sprintf(`search "%s/%s/%s"`, testCompartment, *g.DisplayName, *l.DisplayName),
	}
}

func labels(resp loggingsearch.SearchLogsResponse) []string {
	out := make([]string, 0, len(resp.Results))

	for _, r := range resp.Results {
		row, _ := (*r.Data).(map[string]any)
		if n, ok := row["count"]; ok {
			out = append(out, fmt.Sprint(n))

			continue
		}

		content, _ := row["logContent"].(map[string]any)

		switch d := content["data"].(type) {
		case map[string]any:
			out = append(out, fmt.Sprint(d["msg"]))
		default:
			out = append(out, fmt.Sprint(d))
		}
	}

	return out
}

func TestSearchLogsQueries(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)

	tests := []struct {
		name  string
		tail  string
		limit *int
		want  []string
	}{
		{"all newest first", "", nil, []string{"plain ERROR line", "overloaded", "missing", "boom", "started"}},
		{"equals", ` | where data.level = 'ERROR'`, nil, []string{"overloaded", "boom"}},
		{"where optional", ` | data.level = 'WARN'`, nil, []string{"missing"}},
		{"and number", ` | where data.level = 'ERROR' and data.status >= 503`, nil, []string{"overloaded"}},
		{"or", ` | where data.level = 'INFO' or data.status = 404`, nil, []string{"missing", "started"}},
		{"not group", ` | where not (data.level = 'INFO' or data.level = 'WARN')`, nil,
			[]string{"plain ERROR line", "overloaded", "boom"}},
		{
			"not equals keeps missing",
			` | where data.level != 'ERROR'`,
			nil,
			[]string{"plain ERROR line", "missing", "started"},
		},
		{"in list", ` | where data.level in ('WARN', 'INFO')`, nil, []string{"missing", "started"}},
		{
			"wildcard logContent",
			` | where logContent = '*ERROR*'`,
			nil,
			[]string{"plain ERROR line", "overloaded", "boom"},
		},
		{"wildcard field", ` | where data.msg = 'o*'`, nil, []string{"overloaded"}},
		{
			"sort asc",
			` | sort by datetime asc`,
			nil,
			[]string{"started", "boom", "missing", "overloaded", "plain ERROR line"},
		},
		{"sort field desc limit", ` | where data.status > 0 | sort by data.status desc | limit 2`, nil,
			[]string{"overloaded", "boom"}},
		{
			"sort then limit one",
			` | where data.level = 'ERROR' | sort by datetime desc | limit 1`,
			nil,
			[]string{"overloaded"},
		},
		{"top", ` | where data.status > 0 | top 2 by data.status`, nil, []string{"overloaded", "boom"}},
		{"top asc", ` | where data.status > 0 | top 1 by data.status asc`, nil, []string{"started"}},
		{"count", ` | where data.level = 'ERROR' | count`, nil, []string{"2"}},
		{"api limit", "", new(2), []string{"plain ERROR line", "overloaded"}},
		{"datetime filter", fmt.Sprintf(` | where datetime < %d`, s.clock.Now().Add(-35*time.Second).UnixMilli()), nil,
			[]string{"boom", "started"}},
		{"quoted path", ` | where "data"."level" = 'WARN'`, nil, []string{"missing"}},
		{"group scope", "", nil, []string{"plain ERROR line", "overloaded", "missing", "boom", "started"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := s.scope + tt.tail
			if tt.name == "group scope" {
				q = fmt.Sprintf(`search "%s/%s"`, testCompartment, *s.group.Id)
			}

			resp := s.query(t, q, tt.limit, nil)
			assert.Equal(t, tt.want, labels(resp))
			assert.Equal(t, len(tt.want), *resp.Summary.ResultCount)
			assert.NotEmpty(t, resp.Fields)
		})
	}
}

func TestSearchResultShape(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)
	resp := s.query(t, s.scope+` | where data.msg = 'boom'`, nil, nil)
	require.Len(t, resp.Results, 1)

	row, ok := (*resp.Results[0].Data).(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(s.clock.Now().Add(-40*time.Second).UnixMilli()), row["datetime"], 1)
	assert.Equal(t, "us-ashburn-1", row["regionId"])

	ce, ok := row["logContent"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "app", ce["source"])
	assert.Equal(t, "app.requests", ce["type"])
	assert.Equal(t, "1.0", ce["specversion"])

	oracle, ok := ce["oracle"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, *s.log.Id, oracle["logid"])
	assert.Equal(t, *s.group.Id, oracle["loggroupid"])
	assert.Equal(t, testCompartment, oracle["compartmentid"])
}

func TestSearchLogsPaging(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)

	var (
		got  []string
		page *string
	)

	pages := 0

	for {
		resp := s.query(t, s.scope, new(2), page)
		got = append(got, labels(resp)...)
		pages++

		if resp.OpcNextPage == nil {
			break
		}

		page = resp.OpcNextPage
	}

	assert.Equal(t, 3, pages)
	assert.Equal(t, []string{"plain ERROR line", "overloaded", "missing", "boom", "started"}, got)
}

func TestSearchLogsErrors(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)
	start := common.SDKTime{Time: s.clock.Now().Add(-time.Hour)}
	end := common.SDKTime{Time: s.clock.Now().Add(time.Hour)}

	tests := []struct {
		name  string
		query string
		start common.SDKTime
		end   common.SDKTime
		limit *int
		code  string
	}{
		{"no search keyword", `where a = 1`, start, end, nil, "InvalidParameter"},
		{"unquoted scope", `search app`, start, end, nil, "InvalidParameter"},
		{"unterminated string", s.scope + ` | where a = 'x`, start, end, nil, "InvalidParameter"},
		{"unsupported select", s.scope + ` | select data.level`, start, end, nil, "InvalidParameter"},
		{"unsupported summarize", s.scope + ` | summarize count() by level`, start, end, nil, "InvalidParameter"},
		{"bad limit operand", s.scope + ` | limit abc`, start, end, nil, "InvalidParameter"},
		{"missing paren", s.scope + ` | where (a = 1`, start, end, nil, "InvalidParameter"},
		{"missing group", fmt.Sprintf(`search "%s/nope"`, testCompartment), start, end, nil, "InvalidParameter"},
		{"missing log", fmt.Sprintf(`search "%s/grp/nope"`, testCompartment), start, end, nil, "InvalidParameter"},
		{"reversed range", s.scope, end, start, nil, "InvalidParameter"},
		{"limit too large", s.scope, start, end, new(5000), "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.search.SearchLogs(t.Context(), loggingsearch.SearchLogsRequest{
				TimeStart: &tt.start, TimeEnd: &tt.end, SearchQuery: new(tt.query),
				Limit: tt.limit,
			})
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestSearchTimeRange(t *testing.T) {
	t.Parallel()

	s := seededSearch(t)
	now := s.clock.Now()

	tests := []struct {
		name       string
		start, end time.Time
		want       []string
	}{
		{"window", now.Add(-45 * time.Second), now.Add(-25 * time.Second), []string{"missing", "boom"}},
		{"end exclusive", now.Add(-50 * time.Second), now.Add(-40 * time.Second), []string{"started"}},
		{"before everything", now.Add(-2 * time.Hour), now.Add(-time.Hour), []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			start, end := common.SDKTime{Time: tt.start}, common.SDKTime{Time: tt.end}

			resp, err := s.search.SearchLogs(t.Context(), loggingsearch.SearchLogsRequest{
				TimeStart: &start, TimeEnd: &end, SearchQuery: new(s.scope),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, labels(resp))
		})
	}
}

func TestRetentionExpiry(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "grp")
	short := e.mkLog(t, g, "short", nil)
	long := e.mkLog(t, g, "long", func(d *logging.CreateLogDetails) { d.RetentionDuration = new(60) })

	e.put(t, short, 0, "short entry")
	e.put(t, long, 0, "long entry")

	count := func(log logging.Log) int {
		start, end := common.SDKTime{
			Time: e.clock.Now().Add(-100 * 24 * time.Hour),
		}, common.SDKTime{
			Time: e.clock.Now().Add(time.Hour),
		}

		resp, err := e.search.SearchLogs(t.Context(), loggingsearch.SearchLogsRequest{
			TimeStart: &start, TimeEnd: &end,
			SearchQuery: new(fmt.Sprintf(`search "%s/%s/%s"`, testCompartment, *g.Id, *log.Id)),
		})
		require.NoError(t, err)

		return len(resp.Results)
	}

	assert.Equal(t, 1, count(short))
	assert.Equal(t, 1, count(long))

	e.clock.Advance(29 * 24 * time.Hour)
	assert.Equal(t, 1, count(short))

	e.clock.Advance(2 * 24 * time.Hour)
	assert.Equal(t, 0, count(short), "30 day retention elapsed")
	assert.Equal(t, 1, count(long), "60 day retention still holds")

	e.clock.Advance(30 * 24 * time.Hour)
	assert.Equal(t, 0, count(long))
}

func TestPutLogsValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "grp")
	l := e.mkLog(t, g, "app", nil)
	off := e.mkLog(t, g, "off", func(d *logging.CreateLogDetails) { d.IsEnabled = new(false) })

	const now = "2026-10-29T10:00:00.000Z"

	entry := `{"data":"x","id":"1"}`
	batch := func(entries string) string {
		return `{"source":"s","type":"t","defaultlogentrytime":"` + now + `","entries":[` + entries + `]}`
	}
	body := func(spec, batches string) string {
		return `{"specversion":"` + spec + `","logEntryBatches":[` + batches + `]}`
	}

	tests := []struct {
		name   string
		logID  string
		body   string
		code   string
		status int
	}{
		{"ok", *l.Id, body("1.0", batch(entry)), "", 200},
		{"bad specversion", *l.Id, body("2.0", batch(entry)), "InvalidParameter", 400},
		{"no batches", *l.Id, body("1.0", ""), "InvalidParameter", 400},
		{"no entries", *l.Id, body("1.0", batch("")), "InvalidParameter", 400},
		{
			"missing source",
			*l.Id,
			body("1.0", `{"type":"t","defaultlogentrytime":"`+now+`","entries":[`+entry+`]}`),
			"MissingParameter",
			400,
		},
		{
			"missing default time",
			*l.Id,
			body("1.0", `{"source":"s","type":"t","entries":[`+entry+`]}`),
			"MissingParameter",
			400,
		},
		{"missing entry id", *l.Id, body("1.0", batch(`{"data":"x"}`)), "MissingParameter", 400},
		{"missing entry data", *l.Id, body("1.0", batch(`{"id":"1"}`)), "MissingParameter", 400},
		{
			"entry too large",
			*l.Id,
			body("1.0", batch(`{"id":"1","data":"`+strings.Repeat("a", 1<<20)+`"}`)),
			"PayloadTooLarge",
			413,
		},
		{"not json", *l.Id, `nope`, "CannotParseRequest", 400},
		{"unknown log", "ocid1.log.oc1..ghost", body("1.0", batch(entry)), "NotAuthorizedOrNotFound", 404},
		{"disabled log", *off.Id, body("1.0", batch(entry)), "IncorrectState", 409},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, out := rawDo(t, http.MethodPost, e.base+"/20200831/logs/"+tt.logID+"/actions/push", tt.body)
			assert.Equal(t, tt.status, status, out)

			if tt.code != "" {
				assert.Contains(t, out, tt.code)
			}
		})
	}
}

func TestPutLogsTruncatesLongData(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "grp")
	l := e.mkLog(t, g, "app", nil)

	e.put(t, l, 0, strings.Repeat("z", 10001))

	resp := e.query(t, fmt.Sprintf(`search "%s/%s/%s"`, testCompartment, *g.Id, *l.Id), nil, nil)
	require.Len(t, resp.Results, 1)
	assert.Len(t, labels(resp)[0], 10000)
}

func TestLogCreatingRejectsIngestion(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "grp")

	_, err := e.mgmt.CreateLog(t.Context(), logging.CreateLogRequest{
		LogGroupId:  g.Id,
		DisplayName: new("fresh"), LogType: logging.CreateLogDetailsLogTypeCustom,
	})
	require.NoError(t, err)

	list, err := e.mgmt.ListLogs(t.Context(), logging.ListLogsRequest{LogGroupId: g.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	status, body := rawDo(
		t,
		http.MethodPost,
		e.base+"/20200831/logs/"+*list.Items[0].Id+"/actions/push",
		`{"specversion":"1.0","logEntryBatches":[{"source":"s","type":"t",`+
			`"defaultlogentrytime":"2026-10-29T10:00:00Z","entries":[{"data":"x","id":"1"}]}]}`,
	)
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, body, "IncorrectState")
}
