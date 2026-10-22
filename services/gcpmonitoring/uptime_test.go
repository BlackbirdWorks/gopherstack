package gcpmonitoring_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const checkPassed = "monitoring.googleapis.com/uptime_check/check_passed"

type target struct {
	body   atomic.Value
	srv    *httptest.Server
	host   string
	status atomic.Int32
	hits   atomic.Int32
	port   int32
}

func newTarget(t *testing.T) *target {
	t.Helper()

	tg := &target{}
	tg.status.Store(http.StatusOK)
	tg.body.Store("service is ok")
	tg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.hits.Add(1)

		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/health", http.StatusFound)

			return
		}

		w.WriteHeader(int(tg.status.Load()))
		_, _ = w.Write([]byte(tg.body.Load().(string)))
	}))
	t.Cleanup(tg.srv.Close)

	u, err := url.Parse(tg.srv.URL)
	require.NoError(t, err)

	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	n, err := strconv.Atoi(port)
	require.NoError(t, err)

	tg.host, tg.port = host, int32(n)

	return tg
}

func (tg *target) check(name string, hc *monitoringpb.UptimeCheckConfig_HttpCheck) *monitoringpb.UptimeCheckConfig {
	hc.Port = tg.port

	return &monitoringpb.UptimeCheckConfig{
		DisplayName: name,
		Resource: &monitoringpb.UptimeCheckConfig_MonitoredResource{MonitoredResource: &monitoredres.MonitoredResource{
			Type: "uptime_url", Labels: map[string]string{"host": tg.host},
		}},
		CheckRequestType: &monitoringpb.UptimeCheckConfig_HttpCheck_{HttpCheck: hc},
		Period:           durationpb.New(time.Minute),
		Timeout:          durationpb.New(5 * time.Second),
	}
}

func (e *env) createCheck(t *testing.T, cfg *monitoringpb.UptimeCheckConfig) *monitoringpb.UptimeCheckConfig {
	t.Helper()

	got, err := e.uptime(t).CreateUptimeCheckConfig(t.Context(),
		&monitoringpb.CreateUptimeCheckConfigRequest{Parent: proj, UptimeCheckConfig: cfg})
	require.NoError(t, err)

	return got
}

func (e *env) passed(t *testing.T) map[string][]bool {
	t.Helper()

	out := map[string][]bool{}

	for _, ts := range e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter:   `metric.type = "` + checkPassed + `"`,
		Interval: interval(epoch().Add(-time.Hour), e.clk.now()),
	}) {
		for _, p := range ts.GetPoints() {
			loc := ts.GetMetric().GetLabels()["checker_location"]
			out[loc] = append(out[loc], p.GetValue().GetBoolValue())
		}
	}

	return out
}

func TestUptimeCheckProbesOnTheInjectedClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t)
	cfg := e.createCheck(t, tg.check("health", &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/health"}))

	assert.Regexp(t, `^projects/test-project/uptimeCheckConfigs/health-[0-9a-f]{6}$`, cfg.GetName())
	assert.Equal(t, "/health", cfg.GetHttpCheck().GetPath())
	assert.Equal(t, monitoringpb.UptimeCheckConfig_HttpCheck_GET, cfg.GetHttpCheck().GetRequestMethod())

	b := e.handler.Backend
	b.Tick(t.Context())
	assert.EqualValues(t, 1, tg.hits.Load())

	b.Tick(t.Context())
	assert.EqualValues(t, 1, tg.hits.Load(), "period has not elapsed on the injected clock")

	e.clk.advanceMinute()
	tg.status.Store(http.StatusInternalServerError)
	b.Tick(t.Context())
	assert.EqualValues(t, 2, tg.hits.Load())

	got := e.passed(t)
	require.Len(t, got, 6, "one series per checker location")

	for loc, vals := range got {
		assert.Equal(t, []bool{false, true}, vals, loc)
	}

	id := cfg.GetName()[len(proj+"/uptimeCheckConfigs/"):]
	series := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter: `metric.type = "` + checkPassed + `" AND metric.labels.check_id = "` + id +
			`" AND metric.labels.checker_location = "europe"`,
		Interval: interval(epoch().Add(-time.Hour), e.clk.now()),
	})
	require.Len(t, series, 1)
	assert.Equal(t, "uptime_url", series[0].GetResource().GetType())
	assert.Equal(t, tg.host, series[0].GetResource().GetLabels()["host"])

	latency := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter: `metric.type = "monitoring.googleapis.com/uptime_check/request_latency" AND ` +
			`metric.labels.checker_location = "europe"`,
		Interval:    interval(epoch().Add(-time.Hour), e.clk.now()),
		Aggregation: agg(time.Minute, monitoringpb.Aggregation_ALIGN_PERCENTILE_50, 0),
	})
	require.Len(t, latency, 1)
	assert.Len(t, latency[0].GetPoints(), 2)
}

func matcher(
	content string, m monitoringpb.UptimeCheckConfig_ContentMatcher_ContentMatcherOption,
) []*monitoringpb.UptimeCheckConfig_ContentMatcher {
	return []*monitoringpb.UptimeCheckConfig_ContentMatcher{{Content: content, Matcher: m}}
}

func TestUptimeCheckHTTPBehaviour(t *testing.T) {
	t.Parallel()

	tests := []struct {
		http   *monitoringpb.UptimeCheckConfig_HttpCheck
		name   string
		body   string
		match  []*monitoringpb.UptimeCheckConfig_ContentMatcher
		status int32
		want   bool
	}{
		{name: "ok", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"}, body: "ok", status: 200, want: true},
		{name: "server error", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"}, status: 500},
		{
			name: "follows redirect", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/redirect"},
			status: 200, want: true,
		},
		{
			name: "accepted 503",
			http: &monitoringpb.UptimeCheckConfig_HttpCheck{
				Path: "/", AcceptedResponseStatusCodes: []*monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode{
					{
						StatusCode: &monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_StatusValue{
							StatusValue: 503,
						},
					},
				},
			},
			status: 503, want: true,
		},
		{
			name: "accepted 4xx class",
			http: &monitoringpb.UptimeCheckConfig_HttpCheck{
				Path: "/", AcceptedResponseStatusCodes: []*monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode{
					{StatusCode: &monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_StatusClass_{
						StatusClass: monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_STATUS_CLASS_4XX,
					}},
				},
			},
			status: 404, want: true,
		},
		{
			name: "contains hit", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"},
			match: matcher("needle", monitoringpb.UptimeCheckConfig_ContentMatcher_CONTAINS_STRING),
			body:  "a needle here", status: 200, want: true,
		},
		{
			name: "contains miss", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"},
			match: matcher("needle", monitoringpb.UptimeCheckConfig_ContentMatcher_CONTAINS_STRING),
			body:  "haystack", status: 200,
		},
		{
			name: "not contains hit", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"},
			match: matcher("error", monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_CONTAINS_STRING),
			body:  "an error", status: 200,
		},
		{
			name: "regex hit", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"},
			match: matcher(`v\d+`, monitoringpb.UptimeCheckConfig_ContentMatcher_MATCHES_REGEX),
			body:  "build v42", status: 200, want: true,
		},
		{
			name: "not regex hit", http: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"},
			match: matcher(`v\d+`, monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_MATCHES_REGEX),
			body:  "build v42", status: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tg := newTarget(t)
			tg.status.Store(tt.status)
			tg.body.Store(tt.body)

			cfg := tg.check(tt.name, tt.http)
			cfg.ContentMatchers = tt.match
			e.createCheck(t, cfg)
			e.handler.Backend.Tick(t.Context())

			got := e.passed(t)
			require.Len(t, got, 6)
			assert.Equal(t, []bool{tt.want}, got["usa-oregon"])
		})
	}
}

func TestUptimeCheckTCPAndRegions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t)
	cfg := tg.check("tcp", &monitoringpb.UptimeCheckConfig_HttpCheck{})
	cfg.CheckRequestType = &monitoringpb.UptimeCheckConfig_TcpCheck_{
		TcpCheck: &monitoringpb.UptimeCheckConfig_TcpCheck{Port: tg.port},
	}
	cfg.SelectedRegions = []monitoringpb.UptimeCheckRegion{
		monitoringpb.UptimeCheckRegion_EUROPE,
		monitoringpb.UptimeCheckRegion_USA,
	}
	e.createCheck(t, cfg)

	e.handler.Backend.Tick(t.Context())

	got := e.passed(t)
	assert.Len(t, got, 4, "europe plus the three USA locations")
	assert.Equal(t, []bool{true}, got["europe"])

	tg.srv.Close()
	e.clk.advanceMinute()
	e.handler.Backend.Tick(t.Context())
	assert.Equal(t, []bool{false, true}, e.passed(t)["europe"])
}

func TestUptimeCheckAlertFires(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t)
	e.createCheck(t, tg.check("health", &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/"}))

	cond := thresholdCondition(
		"check failing",
		`metric.type = "`+checkPassed+`"`,
		monitoringpb.ComparisonType_COMPARISON_GT,
		0,
		0,
	)
	cond.GetConditionThreshold().Aggregations = []*monitoringpb.Aggregation{
		agg(
			time.Minute,
			monitoringpb.Aggregation_ALIGN_NEXT_OLDER,
			monitoringpb.Aggregation_REDUCE_COUNT_FALSE,
			"resource.labels.host",
		),
	}
	e.createPolicy(t, &monitoringpb.AlertPolicy{
		DisplayName:          "uptime",
		Combiner:             monitoringpb.AlertPolicy_OR,
		Conditions:           []*monitoringpb.AlertPolicy_Condition{cond},
		NotificationChannels: []string{e.pubsubChannel(t)},
	})

	e.handler.Backend.Tick(t.Context())
	assert.Empty(t, e.pub.all(), "healthy")

	tg.status.Store(http.StatusServiceUnavailable)
	e.clk.advanceMinute()
	e.handler.Backend.Tick(t.Context())

	require.Len(t, e.pub.all(), 1)
	assert.Equal(t, "open", decode(t, e.pub.all()[0]).Incident.State)
}

func TestUptimeCheckCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t)
	c := e.uptime(t)
	created := e.createCheck(t, tg.check("My Check!", &monitoringpb.UptimeCheckConfig_HttpCheck{}))

	assert.Contains(t, created.GetName(), "/uptimeCheckConfigs/my-check-")
	assert.Equal(t, "/", created.GetHttpCheck().GetPath())
	assert.Equal(t, tg.port, created.GetHttpCheck().GetPort())
	assert.Equal(t, "test-project", created.GetMonitoredResource().GetLabels()["project_id"])

	upd, err := c.UpdateUptimeCheckConfig(t.Context(), &monitoringpb.UpdateUptimeCheckConfigRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "period"}},
		UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{
			Name:        created.GetName(),
			DisplayName: "Renamed",
			Period:      durationpb.New(5 * time.Minute),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Renamed", upd.GetDisplayName())
	assert.Equal(t, 5*time.Minute, upd.GetPeriod().AsDuration())
	assert.Equal(t, created.GetName(), upd.GetName())

	it := c.ListUptimeCheckConfigs(t.Context(), &monitoringpb.ListUptimeCheckConfigsRequest{Parent: proj})
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, created.GetName(), first.GetName())
	_, err = it.Next()
	assert.Equal(t, iterator.Done, err)

	ips := c.ListUptimeCheckIps(t.Context(), &monitoringpb.ListUptimeCheckIpsRequest{})
	ip, err := ips.Next()
	require.NoError(t, err)
	assert.NotEmpty(t, ip.GetLocation())

	require.NoError(
		t,
		c.DeleteUptimeCheckConfig(t.Context(), &monitoringpb.DeleteUptimeCheckConfigRequest{Name: created.GetName()}),
	)

	_, err = c.GetUptimeCheckConfig(t.Context(), &monitoringpb.GetUptimeCheckConfigRequest{Name: created.GetName()})
	assert.Equal(t, "NotFound", codeOf(err).String())
}

func TestUptimeCheckValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t)

	tests := []struct {
		mutate func(*monitoringpb.UptimeCheckConfig)
		name   string
	}{
		{func(c *monitoringpb.UptimeCheckConfig) { c.DisplayName = "" }, "no display name"},
		{func(c *monitoringpb.UptimeCheckConfig) { c.Resource = nil }, "no resource"},
		{func(c *monitoringpb.UptimeCheckConfig) { c.CheckRequestType = nil }, "no check type"},
		{func(c *monitoringpb.UptimeCheckConfig) { c.Period = durationpb.New(2 * time.Minute) }, "bad period"},
		{func(c *monitoringpb.UptimeCheckConfig) { c.Timeout = durationpb.New(2 * time.Minute) }, "bad timeout"},
		{func(c *monitoringpb.UptimeCheckConfig) {
			c.IsInternal = true //nolint:staticcheck // deprecated field must still be rejected
		}, "internal"},
		{func(c *monitoringpb.UptimeCheckConfig) {
			c.ContentMatchers = []*monitoringpb.UptimeCheckConfig_ContentMatcher{
				{Content: "$.a", Matcher: monitoringpb.UptimeCheckConfig_ContentMatcher_MATCHES_JSON_PATH},
			}
		}, "json path matcher"},
		{func(c *monitoringpb.UptimeCheckConfig) {
			c.ContentMatchers = []*monitoringpb.UptimeCheckConfig_ContentMatcher{
				{Content: "(", Matcher: monitoringpb.UptimeCheckConfig_ContentMatcher_MATCHES_REGEX},
			}
		}, "bad regex"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tg.check("c", &monitoringpb.UptimeCheckConfig_HttpCheck{})
			tt.mutate(cfg)

			_, err := e.uptime(t).CreateUptimeCheckConfig(t.Context(),
				&monitoringpb.CreateUptimeCheckConfigRequest{Parent: proj, UptimeCheckConfig: cfg})
			assert.Equal(t, "InvalidArgument", codeOf(err).String(), "%v", err)
		})
	}
}
