package rum_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	evtNav     = "com.amazon.rum.performance_navigation_event"
	evtJSError = "com.amazon.rum.js_error_event"
	evtSession = "com.amazon.rum.session_start_event"
	evtHTTP    = "com.amazon.rum.http_event"
)

func (f *fixture) base() float64 { return float64(f.clock.now().Unix()) }

func chromeMeta(page string) string {
	return `{"browserName":"Chrome","deviceType":"desktop","osName":"Linux","pageId":"` + page + `"}`
}

func TestRealClient_Events(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testPutAndGetEvents, "put_get"},
		{testEventFilters, "filters"},
		{testEventTimeRange, "time_range"},
		{testEventPagination, "pagination"},
		{testPutEventsErrors, "put_errors"},
		{testGetEventsErrors, "get_errors"},
		{testEventsToCloudWatchLogs, "cloudwatch_logs"},
		{testEventsActivateMonitor, "activate"},
		{testDeleteDropsEvents, "delete_drops"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testPutAndGetEvents(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")
	base := f.base()

	require.NoError(t, f.putEvents(t, id, "web",
		eventSpec{id: "e1", typ: evtSession, details: `{}`, meta: chromeMeta("/home"), ts: base},
		eventSpec{id: "e2", typ: evtNav, details: `{"duration":1500}`, meta: chromeMeta("/home"), ts: base + 1},
	))

	out, err := f.client.GetAppMonitorData(t.Context(), &awsrum.GetAppMonitorDataInput{
		Name: aws.String("web"), TimeRange: &types.TimeRange{After: int64(base) - 10},
	})
	require.NoError(t, err)
	require.Len(t, out.Events, 2)

	first := mustJSON(t, out.Events[0])
	assert.Equal(t, "e1", first["event_id"])
	assert.Equal(t, evtSession, first["event_type"])
	assert.Equal(t, id, first["application_Id"])
	assert.Equal(t, "web", first["application_name"])
	assert.Equal(t, "batch-1", first["batch_id"])
	assert.Equal(t, "sess-1", first["sessionId"])
	assert.Equal(t, "Chrome", first["metadata"].(map[string]any)["browserName"])

	second := mustJSON(t, out.Events[1])
	assert.InDelta(t, 1500, second["event_details"].(map[string]any)["duration"], 0)
	assert.Equal(t, 2, f.backend.EventCount())
}

func testEventFilters(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")
	base := f.base()

	require.NoError(t, f.putEvents(
		t,
		id,
		"web",
		eventSpec{
			id:      "e1",
			typ:     evtNav,
			details: `{}`,
			meta:    `{"browserName":"Chrome","pageId":"/a","deviceType":"desktop","osName":"Linux"}`,
			ts:      base,
		},
		eventSpec{
			id:      "e2",
			typ:     evtJSError,
			details: `{}`,
			meta:    `{"browserName":"Firefox","pageId":"/b","deviceType":"mobile","osName":"Android"}`,
			ts:      base + 1,
		},
		eventSpec{
			id:      "e3",
			typ:     evtNav,
			details: `{}`,
			meta:    `{"browserName":"Firefox","pageId":"/a","deviceType":"desktop","osName":"Linux"}`,
			ts:      base + 2,
		},
	))

	cases := []struct {
		name    string
		filters []types.QueryFilter
		want    []string
	}{
		{"none", nil, []string{"e1", "e2", "e3"}},
		{
			"browser",
			[]types.QueryFilter{{Name: aws.String("Browser"), Values: []string{"Firefox"}}},
			[]string{"e2", "e3"},
		},
		{
			"browser_values",
			[]types.QueryFilter{{Name: aws.String("Browser"), Values: []string{"Firefox", "Chrome"}}},
			[]string{"e1", "e2", "e3"},
		},
		{"page", []types.QueryFilter{{Name: aws.String("Page"), Values: []string{"/a"}}}, []string{"e1", "e3"}},
		{"device", []types.QueryFilter{{Name: aws.String("Device"), Values: []string{"mobile"}}}, []string{"e2"}},
		{"os", []types.QueryFilter{{Name: aws.String("OS"), Values: []string{"Linux"}}}, []string{"e1", "e3"}},
		{
			"event_type",
			[]types.QueryFilter{{Name: aws.String("EventType"), Values: []string{evtJSError}}},
			[]string{"e2"},
		},
		{"and", []types.QueryFilter{
			{Name: aws.String("Browser"), Values: []string{"Firefox"}},
			{Name: aws.String("Page"), Values: []string{"/a"}},
		}, []string{"e3"}},
		{"invert", []types.QueryFilter{{Name: aws.String("Invert"), Values: []string{"Chrome"}}}, []string{"e2", "e3"}},
		{"country_unset", []types.QueryFilter{{Name: aws.String("Country"), Values: []string{"US"}}}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := f.client.GetAppMonitorData(t.Context(), &awsrum.GetAppMonitorDataInput{
				Name: aws.String("web"), TimeRange: &types.TimeRange{After: int64(base) - 10}, Filters: tc.filters,
			})
			require.NoError(t, err)

			var got []string
			for _, e := range out.Events {
				got = append(got, mustJSON(t, e)["event_id"].(string))
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func testEventTimeRange(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")
	base := f.base()

	require.NoError(t, f.putEvents(t, id, "web",
		eventSpec{id: "old", typ: evtNav, details: `{}`, ts: base - 100},
		eventSpec{id: "mid", typ: evtNav, details: `{}`, ts: base},
		eventSpec{id: "new", typ: evtNav, details: `{}`, ts: base + 100},
	))

	cases := []struct {
		name          string
		want          []string
		after, before int64
	}{
		{"all", []string{"old", "mid", "new"}, int64(base) - 200, 0},
		{"after", []string{"mid", "new"}, int64(base) - 50, 0},
		{"window", []string{"mid"}, int64(base) - 50, int64(base) + 50},
		{"millis", []string{"mid"}, (int64(base) - 50) * 1000, (int64(base) + 50) * 1000},
		{"empty", nil, int64(base) + 500, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := f.client.GetAppMonitorData(t.Context(), &awsrum.GetAppMonitorDataInput{
				Name: aws.String("web"), TimeRange: &types.TimeRange{After: tc.after, Before: tc.before},
			})
			require.NoError(t, err)

			var got []string
			for _, e := range out.Events {
				got = append(got, mustJSON(t, e)["event_id"].(string))
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func testEventPagination(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")
	base := f.base()

	require.NoError(t, f.putEvents(t, id, "web",
		eventSpec{id: "e1", typ: evtNav, details: `{}`, ts: base},
		eventSpec{id: "e2", typ: evtNav, details: `{}`, ts: base + 1},
		eventSpec{id: "e3", typ: evtNav, details: `{}`, ts: base + 2},
	))

	in := &awsrum.GetAppMonitorDataInput{
		Name:       aws.String("web"),
		TimeRange:  &types.TimeRange{After: int64(base) - 1},
		MaxResults: 2,
	}

	p1, err := f.client.GetAppMonitorData(t.Context(), in)
	require.NoError(t, err)
	require.Len(t, p1.Events, 2)
	require.NotNil(t, p1.NextToken)

	in.NextToken = p1.NextToken
	p2, err := f.client.GetAppMonitorData(t.Context(), in)
	require.NoError(t, err)
	require.Len(t, p2.Events, 1)
	assert.Nil(t, p2.NextToken)
	assert.Equal(t, "e3", mustJSON(t, p2.Events[0])["event_id"])
}

func testPutEventsErrors(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")
	base := f.base()

	cases := []struct {
		err  error
		name string
		code string
	}{
		{
			f.putEvents(
				t,
				"11111111-1111-1111-1111-111111111111",
				"web",
				eventSpec{id: "e", typ: evtNav, details: `{}`, ts: base},
			),
			"unknown_monitor",
			"ResourceNotFoundException",
		},
		{
			f.putEvents(t, id, "web", eventSpec{id: "e", typ: evtNav, details: `not json`, ts: base}),
			"bad_details",
			"ValidationException",
		},
		{
			f.putEvents(t, id, "web", eventSpec{id: "e", typ: evtNav, details: `{}`, meta: `{`, ts: base}),
			"bad_metadata",
			"ValidationException",
		},
		{
			f.putEvents(t, id, "web", eventSpec{id: "", typ: evtNav, details: `{}`, ts: base}),
			"no_event_id",
			"ValidationException",
		},
		{f.putEvents(t, id, "web"), "no_events", "ValidationException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.err)
			assert.Equal(t, tc.code, apiCode(tc.err))
		})
	}

	assert.Equal(t, 0, f.backend.EventCount(), "rejected batches store nothing")
}

func testGetEventsErrors(t *testing.T, f *fixture) {
	t.Helper()

	f.createMonitor(t, "web")

	cases := []struct {
		in   awsrum.GetAppMonitorDataInput
		name string
		code string
	}{
		{
			awsrum.GetAppMonitorDataInput{Name: aws.String("nope"), TimeRange: &types.TimeRange{After: 1}},
			"missing_monitor",
			"ResourceNotFoundException",
		},
		{
			awsrum.GetAppMonitorDataInput{Name: aws.String("web"), TimeRange: &types.TimeRange{}},
			"no_after",
			"ValidationException",
		},
		{
			awsrum.GetAppMonitorDataInput{
				Name: aws.String("web"), TimeRange: &types.TimeRange{After: 1},
				Filters: []types.QueryFilter{{Name: aws.String("Planet"), Values: []string{"Mars"}}},
			},
			"bad_filter", "ValidationException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.client.GetAppMonitorData(t.Context(), &tc.in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func testEventsToCloudWatchLogs(t *testing.T, f *fixture) {
	t.Helper()

	logged := f.createMonitor(t, "logged", func(in *awsrum.CreateAppMonitorInput) { in.CwLogEnabled = aws.Bool(true) })
	plain := f.createMonitor(t, "plain")
	base := f.base()

	require.NoError(t, f.putEvents(t, logged, "logged",
		eventSpec{id: "late", typ: evtNav, details: `{}`, ts: base + 5},
		eventSpec{id: "early", typ: evtNav, details: `{}`, ts: base},
	))
	require.NoError(t, f.putEvents(t, plain, "plain", eventSpec{id: "x", typ: evtNav, details: `{}`, ts: base}))

	events := f.logs.logged("/aws/vendedlogs/RUMService_logged" + logged[:8])
	require.Len(t, events, 2)
	assert.Equal(t, "early", mustJSON(t, events[0].Message)["event_id"], "log events are in time order")
	assert.Equal(t, int64(base)*1000, events[0].Timestamp)
	assert.Equal(t, int64(base+5)*1000, events[1].Timestamp)

	assert.Empty(t, f.logs.logged("/aws/vendedlogs/RUMService_plain"+plain[:8]))
}

func testEventsActivateMonitor(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")

	got, err := f.client.GetAppMonitor(t.Context(), &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.Equal(t, types.StateEnumCreated, got.AppMonitor.State)

	require.NoError(t, f.putEvents(t, id, "web", eventSpec{id: "e", typ: evtNav, details: `{}`, ts: f.base()}))

	got, err = f.client.GetAppMonitor(t.Context(), &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.Equal(t, types.StateEnumActive, got.AppMonitor.State)
}

func testDeleteDropsEvents(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "web")
	require.NoError(t, f.putEvents(t, id, "web", eventSpec{id: "e", typ: evtNav, details: `{}`, ts: f.base()}))
	require.Equal(t, 1, f.backend.EventCount())

	_, err := f.client.DeleteAppMonitor(t.Context(), &awsrum.DeleteAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.Equal(t, 0, f.backend.EventCount())

	assert.Equal(
		t,
		"ResourceNotFoundException",
		apiCode(f.putEvents(t, id, "web", eventSpec{id: "e", typ: evtNav, details: `{}`, ts: f.base()})),
	)
}
