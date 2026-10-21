package gcprun_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltype "google.golang.org/genproto/googleapis/logging/type"
)

type chanSink struct{ ch chan *loggingpb.LogEntry }

func (s chanSink) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	for _, e := range req.GetEntries() {
		s.ch <- e
	}

	return nil
}

func nextEntry(t *testing.T, ch <-chan *loggingpb.LogEntry) *loggingpb.LogEntry {
	t.Helper()

	select {
	case e := <-ch:
		return e
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no log entry arrived")

		return nil
	}
}

func logEnv(t *testing.T, lines ...fakeLine) (*env, chan *loggingpb.LogEntry) {
	t.Helper()

	e := newEnv(t, true)
	e.rt.lines = lines

	ch := make(chan *loggingpb.LogEntry, 64)
	e.handler.Backend.SetLogSink(chanSink{ch: ch})

	return e, ch
}

func TestLogCaptureOutput(t *testing.T) {
	t.Parallel()

	e, ch := logEnv(t,
		fakeLine{text: "hello"},
		fakeLine{text: "oops", stderr: true},
		fakeLine{text: `{"severity":"WARNING","message":"m","k":"v"}`},
	)
	svc := e.createService(t, "web", "example/web:1")
	e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), "/")

	rev := svc.GetLatestReadyRevision()
	rev = rev[strings.LastIndex(rev, "/")+1:]

	tests := []struct {
		name    string
		check   func(t *testing.T, e *loggingpb.LogEntry)
		logName string
	}{
		{"stdout text", func(t *testing.T, e *loggingpb.LogEntry) {
			t.Helper()
			assert.Equal(t, "hello", e.GetTextPayload())
			assert.Equal(t, ltype.LogSeverity_DEFAULT, e.GetSeverity())
		}, "projects/test-project/logs/run.googleapis.com%2Fstdout"},
		{"stderr text", func(t *testing.T, e *loggingpb.LogEntry) {
			t.Helper()
			assert.Equal(t, "oops", e.GetTextPayload())
			assert.Equal(t, ltype.LogSeverity_ERROR, e.GetSeverity())
		}, "projects/test-project/logs/run.googleapis.com%2Fstderr"},
		{"structured json", func(t *testing.T, e *loggingpb.LogEntry) {
			t.Helper()
			assert.Equal(t, ltype.LogSeverity_WARNING, e.GetSeverity())
			assert.Equal(t, "v", e.GetJsonPayload().GetFields()["k"].GetStringValue())
		}, "projects/test-project/logs/run.googleapis.com%2Fstdout"},
	}

	for _, tt := range tests {
		got := nextEntry(t, ch)

		assert.Equal(t, tt.logName, got.GetLogName(), tt.name)
		assert.Equal(t, "cloud_run_revision", got.GetResource().GetType(), tt.name)
		assert.Equal(t, map[string]string{
			"project_id": "test-project", "service_name": "web", "revision_name": rev,
			"location": "us-central1", "configuration_name": "web",
		}, got.GetResource().GetLabels(), tt.name)
		tt.check(t, got)
	}
}

func TestLogCaptureRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		status int32
		sev    ltype.LogSeverity
	}{
		{name: "served", path: "/hello", status: 200, sev: ltype.LogSeverity_INFO},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, ch := logEnv(t)
			svc := e.createService(t, "web", "example/web:1")
			code, _ := e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), tt.path)
			require.Equal(t, 200, code)

			got := nextEntry(t, ch)
			assert.Equal(t, "projects/test-project/logs/run.googleapis.com%2Frequests", got.GetLogName())
			assert.Equal(t, tt.sev, got.GetSeverity())
			assert.Equal(t, tt.status, got.GetHttpRequest().GetStatus())
			assert.Equal(t, "GET", got.GetHttpRequest().GetRequestMethod())
			assert.Contains(t, got.GetHttpRequest().GetRequestUrl(), tt.path)
			assert.Equal(t, "web", got.GetResource().GetLabels()["service_name"])
		})
	}
}

func TestLogCaptureStopsOnRemoval(t *testing.T) {
	t.Parallel()

	e, ch := logEnv(t, fakeLine{text: "up"})
	svc := e.createService(t, "web", "example/web:1")
	e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), "/")

	assert.Equal(t, "up", nextEntry(t, ch).GetTextPayload())
	assert.Equal(t, int64(1), e.rt.streams.Load())

	e.handler.Backend.Reset()

	assert.Zero(t, e.rt.streams.Load())
	assert.Zero(t, e.rt.running())
}

func TestLogCaptureDisabledWithoutSink(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.lines = []fakeLine{{text: "ignored"}}
	svc := e.createService(t, "web", "example/web:1")
	code, _ := e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), "/")

	assert.Equal(t, 200, code)
	assert.Zero(t, e.rt.streams.Load())
}
