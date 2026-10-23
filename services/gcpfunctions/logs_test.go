package gcpfunctions_test

import (
	"context"
	"net/http"
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

func deployed(t *testing.T, e *env) (string, string) {
	t.Helper()

	fn := e.createFunction(t, "hello", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	return hostOf(fn), fn.GetServiceConfig().GetRevision()
}

func TestLogCaptureOutput(t *testing.T) {
	t.Parallel()

	e, ch := logEnv(t,
		fakeLine{text: "hello"},
		fakeLine{text: "oops", stderr: true},
		fakeLine{text: `{"severity":"ERROR","message":"m","k":"v"}`},
	)
	host, rev := deployed(t, e)
	code, _ := e.call(t, http.MethodGet, host, "/", "")
	require.Equal(t, http.StatusOK, code)

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
			assert.Equal(t, ltype.LogSeverity_ERROR, e.GetSeverity())
			assert.Equal(t, "v", e.GetJsonPayload().GetFields()["k"].GetStringValue())
		}, "projects/test-project/logs/run.googleapis.com%2Fstdout"},
	}

	for _, tt := range tests {
		got := nextEntry(t, ch)

		assert.Equal(t, tt.logName, got.GetLogName(), tt.name)
		assert.Equal(t, "cloud_run_revision", got.GetResource().GetType(), tt.name)
		assert.Equal(t, map[string]string{
			"project_id": "test-project", "service_name": "hello", "revision_name": rev,
			"location": "us-central1", "configuration_name": "hello",
		}, got.GetResource().GetLabels(), tt.name)
		tt.check(t, got)
	}

	req := nextEntry(t, ch)
	assert.Equal(t, "projects/test-project/logs/run.googleapis.com%2Frequests", req.GetLogName())
	assert.Equal(t, int32(http.StatusOK), req.GetHttpRequest().GetStatus())
}

func TestLogCaptureStopsOnRemoval(t *testing.T) {
	t.Parallel()

	e, ch := logEnv(t, fakeLine{text: "up"})
	host, _ := deployed(t, e)
	e.call(t, http.MethodGet, host, "/", "")

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
	host, _ := deployed(t, e)
	code, _ := e.call(t, http.MethodGet, host, "/", "")

	assert.Equal(t, http.StatusOK, code)
	assert.Zero(t, e.rt.streams.Load())
}
