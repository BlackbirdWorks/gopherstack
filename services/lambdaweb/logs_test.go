package lambdaweb_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

type logOpts struct {
	appLevel types.ApplicationLogLevel
	sysLevel types.SystemLogLevel
	group    string
	emit     func(spec lambdaweb.LaunchSpec, line func(stderr bool, msg string))
}

func newLoggedEnv(t *testing.T, o logOpts) (*dataEnv, *fakeLogs) {
	t.Helper()

	env := newTestEnv(t)
	logs := &fakeLogs{}
	env.backend.SetLogSink(logs)

	l := &fakeLauncher{logs: o.emit}
	env.backend.SetLauncher(l)

	rc := revisionConfig()
	lc := &types.LoggingConfig{ApplicationLogLevel: o.appLevel, SystemLogLevel: o.sysLevel}

	if o.group != "" {
		lc.LogGroup = aws.String(o.group)
	}

	rc.ServiceConfig.TelemetryConfig = &types.TelemetryConfig{LoggingConfig: lc}

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("app"), RevisionConfig: rc, EndpointConfig: endpointConfig(),
	})
	require.NoError(t, err)
	env.settle()

	return &dataEnv{
		testEnv: env, launcher: l, domain: aws.ToString(created.Endpoint.DomainName),
		rev1: aws.ToString(created.Revision.RevisionId), http: &http.Client{Timeout: 30 * time.Second},
	}, logs
}

func events(t *testing.T, logs *fakeLogs, group string) []map[string]any {
	t.Helper()

	var out []map[string]any

	for _, line := range logs.snapshot(group) {
		var ev map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &ev), "log line %q is a JSON event", line)

		out = append(out, ev)
	}

	return out
}

func eventTypes(evs []map[string]any) []string {
	var out []string

	for _, ev := range evs {
		if typ, ok := ev["type"].(string); ok {
			out = append(out, typ)
		}
	}

	return out
}

func appEvents(evs []map[string]any) []map[string]any {
	var out []map[string]any

	for _, ev := range evs {
		if _, ok := ev["type"]; !ok {
			out = append(out, ev)
		}
	}

	return out
}

func TestLogs_ApplicationEventsAreStructured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		stderr      bool
		line        string
		wantLevel   string
		wantMessage any
	}{
		{name: "stdout text", line: "hello", wantLevel: "INFO", wantMessage: "hello"},
		{name: "stderr is error", stderr: true, line: "bad", wantLevel: "ERROR", wantMessage: "bad"},
		{
			name: "object stays structured", line: `{"method":"GET","statusCode":200}`, wantLevel: "INFO",
			wantMessage: map[string]any{"method": "GET", "statusCode": float64(200)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs := newLoggedEnv(t, logOpts{
				emit: func(_ lambdaweb.LaunchSpec, line func(bool, string)) { line(tc.stderr, tc.line) },
			})

			resp := d.get(t)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			app := appEvents(events(t, logs, "/aws/lambda/web/app"))
			require.Len(t, app, 1)

			ev := app[0]
			assert.Equal(t, tc.wantLevel, ev["level"])
			assert.Equal(t, tc.wantMessage, ev["message"])
			assert.Equal(t, "us-east-1", ev["region"])
			assert.Equal(t, resp.Header.Get("Lambda-Web-Request-Id"), ev["requestId"])
			assert.Equal(t, "prod", ev["endpointName"])

			ts, err := time.Parse("2006-01-02T15:04:05.000Z07:00", ev["timestamp"].(string))
			require.NoError(t, err)
			assert.Equal(t, d.clock.Now().Truncate(time.Millisecond), ts, "timestamps follow the injectable clock")

			assert.Equal(t, app, appEvents(events(t, logs, "/aws/lambda/web/app.local")), ".local mirrors the group")
		})
	}
}

func TestLogs_ApplicationLogLevelFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level types.ApplicationLogLevel
		name  string
		want  int
	}{
		{name: "info keeps both", level: types.ApplicationLogLevelInfo, want: 2},
		{name: "warn drops stdout", level: types.ApplicationLogLevelWarn, want: 1},
		{name: "error keeps stderr", level: types.ApplicationLogLevelError, want: 1},
		{name: "fatal behaves as error", level: types.ApplicationLogLevelFatal, want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs := newLoggedEnv(t, logOpts{
				appLevel: tc.level,
				emit: func(_ lambdaweb.LaunchSpec, line func(bool, string)) {
					line(false, "out")
					line(true, "err")
				},
			})

			require.Equal(t, http.StatusOK, d.get(t).StatusCode)
			assert.Len(t, appEvents(events(t, logs, "/aws/lambda/web/app")), tc.want)
		})
	}
}

func TestLogs_SystemEventsFollowSystemLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		level types.SystemLogLevel
		want  []string
	}{
		{
			name: "info", level: types.SystemLogLevelInfo,
			want: []string{"platform.initStart", "platform.initReport", "platform.invokeStart"},
		},
		{
			name: "debug adds requestComplete", level: types.SystemLogLevelDebug,
			want: []string{
				"platform.initStart", "platform.initReport", "platform.invokeStart", "platform.requestComplete",
			},
		},
		{name: "warn only failures", level: types.SystemLogLevelWarn, want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs := newLoggedEnv(t, logOpts{sysLevel: tc.level})
			require.Equal(t, http.StatusOK, d.get(t).StatusCode)

			assert.Equal(t, tc.want, eventTypes(events(t, logs, "/aws/lambda/web/app")))
		})
	}
}

func TestLogs_RequestCompleteRecord(t *testing.T) {
	t.Parallel()

	d, logs := newLoggedEnv(t, logOpts{sysLevel: types.SystemLogLevelDebug})
	resp := d.do(t, http.MethodGet, d.domain, "/missing", nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	var rec map[string]any

	for _, ev := range events(t, logs, "/aws/lambda/web/app") {
		if ev["type"] == "platform.requestComplete" {
			rec = ev["record"].(map[string]any)

			assert.Equal(t, "DEBUG", ev["level"])
		}
	}

	require.NotNil(t, rec)
	assert.Equal(t, resp.Header.Get("Lambda-Web-Request-Id"), rec["requestId"])
	assert.Equal(t, "prod", rec["endpointName"])
	assert.InDelta(t, 404, rec["result"], 0)
	assert.Contains(t, rec["metrics"], "durationMs")
	assert.Contains(t, rec["metrics"], "responseBytes")
}

func TestLogs_EnvironmentLifecycleEvents(t *testing.T) {
	t.Parallel()

	d, logs := newLoggedEnv(t, logOpts{})
	resp := d.get(t)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	d.clock.Advance(6 * time.Minute)
	d.backend.SweepEnvironments()

	require.Eventually(t, func() bool {
		return len(eventTypes(events(t, logs, "/aws/lambda/web/app"))) == 6
	}, 5*time.Second, 5*time.Millisecond)

	evs := events(t, logs, "/aws/lambda/web/app")
	assert.Equal(t, []string{
		"platform.initStart", "platform.initReport", "platform.invokeStart",
		"platform.invokeReport", "platform.spinDownStart", "platform.spinDownReport",
	}, eventTypes(evs))

	start := evs[0]["record"].(map[string]any)
	assert.Equal(t, "app", start["functionName"])
	assert.Equal(t, d.rev1, start["revisionId"])
	assert.Equal(t, "native-http", start["initializationType"])

	report := evs[3]["record"].(map[string]any)
	assert.Equal(t, "success", report["status"])
	assert.InDelta(t, 1, report["metrics"].(map[string]any)["totalRequests"], 0)
	assert.Equal(t, start["instanceId"], report["instanceId"])
}

func TestLogs_PeriodicInvokeSummary(t *testing.T) {
	t.Parallel()

	d, logs := newLoggedEnv(t, logOpts{})
	require.Equal(t, http.StatusOK, d.get(t).StatusCode)

	d.clock.Advance(61 * time.Second)
	d.backend.SweepEnvironments()

	var summary map[string]any

	for _, ev := range events(t, logs, "/aws/lambda/web/app") {
		if ev["type"] == "platform.invokeSummary" {
			summary = ev["record"].(map[string]any)
		}
	}

	require.NotNil(t, summary)
	assert.InDelta(t, 1, summary["metrics"].(map[string]any)["requests"], 0)
}

func TestLogs_CustomGroupAndStream(t *testing.T) {
	t.Parallel()

	d, logs := newLoggedEnv(t, logOpts{
		group: "/my-app/web-logs",
		emit:  func(_ lambdaweb.LaunchSpec, line func(bool, string)) { line(false, "hi") },
	})

	resp := d.get(t)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Len(t, appEvents(events(t, logs, "/my-app/web-logs")), 1)
	assert.Len(t, appEvents(events(t, logs, "/my-app/web-logs.local")), 1)
	assert.Empty(t, logs.snapshot("/aws/lambda/web/app"))

	streams := logs.streamsOf("/my-app/web-logs")
	require.Len(t, streams, 1)
	assert.Equal(t, streams[0], resp.Header.Get("Lambda-Web-Logstream"), "the header names the environment's stream")
	assert.Regexp(t, `^\d{4}/\d{2}/\d{2}/\[`+d.rev1+`\][0-9a-f]{32}$`, streams[0])
}

func TestLogs_FailedInitReport(t *testing.T) {
	t.Parallel()

	d, logs := newLoggedEnv(t, logOpts{sysLevel: types.SystemLogLevelWarn})
	d.launcher.err = lambdaweb.ErrCodeUnavailable

	require.Equal(t, http.StatusConflict, d.get(t).StatusCode)

	evs := events(t, logs, "/aws/lambda/web/app")
	require.Len(t, evs, 1)
	assert.Equal(t, "platform.initReport", evs[0]["type"])
	assert.Equal(t, "WARN", evs[0]["level"])

	rec := evs[0]["record"].(map[string]any)
	assert.Equal(t, "failure", rec["status"])
	assert.Equal(t, "Runtime.ImportCodeError", rec["errorType"])
}
