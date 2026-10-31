package gcpfunctions_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltype "google.golang.org/genproto/googleapis/logging/type"

	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
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

	// The request entry and the container output come from separate streams, so
	// it may arrive anywhere; output lines keep their own order.
	var req *loggingpb.LogEntry

	output := make([]*loggingpb.LogEntry, 0, len(tests))

	for range len(tests) + 1 {
		got := nextEntry(t, ch)
		if got.GetLogName() == "projects/test-project/logs/run.googleapis.com%2Frequests" {
			req = got

			continue
		}

		output = append(output, got)
	}

	require.NotNil(t, req, "request log entry")
	require.Len(t, output, len(tests))

	for i, tt := range tests {
		got := output[i]

		assert.Equal(t, tt.logName, got.GetLogName(), tt.name)
		assert.Equal(t, "cloud_run_revision", got.GetResource().GetType(), tt.name)
		assert.Equal(t, map[string]string{
			"project_id": "test-project", "service_name": "hello", "revision_name": rev,
			"location": "us-central1", "configuration_name": "hello",
		}, got.GetResource().GetLabels(), tt.name)
		tt.check(t, got)
	}

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

func TestLogRequestEntryForEventDeliveries(t *testing.T) {
	t.Parallel()

	const requests = "projects/test-project/logs/run.googleapis.com%2Frequests"

	tests := []struct {
		deliver    func(t *testing.T, e *env, fn *functionspb.Function)
		name       string
		reply      int32
		wantStatus int32
		wantSev    ltype.LogSeverity
	}{
		{func(t *testing.T, e *env, fn *functionspb.Function) {
			t.Helper()

			code, _ := e.call(t, http.MethodPost, "localhost", "/_gcf/events/"+fn.GetName(), envelope)
			require.Equal(t, http.StatusOK, code)
		}, "pubsub ok", http.StatusOK, http.StatusOK, ltype.LogSeverity_INFO},
		{func(t *testing.T, e *env, fn *functionspb.Function) {
			t.Helper()
			e.call(t, http.MethodPost, "localhost", "/_gcf/events/"+fn.GetName(), envelope)
		}, "pubsub failing", http.StatusInternalServerError, http.StatusInternalServerError, ltype.LogSeverity_ERROR},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, ch := logEnv(t)
			e.rt.reply.Store(tt.reply)

			fn := e.createFunction(t, "ev", eventSpec(e.upload(t, zipOf(t, nodeFilesFn())), 0))
			tt.deliver(t, e, fn)

			got := nextEntry(t, ch)
			require.Equal(t, requests, got.GetLogName())
			assert.Equal(t, "cloud_run_revision", got.GetResource().GetType())
			assert.Equal(t, tt.wantStatus, got.GetHttpRequest().GetStatus())
			assert.Equal(t, tt.wantSev, got.GetSeverity())
			assert.Equal(t, http.MethodPost, got.GetHttpRequest().GetRequestMethod())
			assert.NotEmpty(t, got.GetLabels()["instanceId"])
		})
	}
}

func TestLogRequestEntryForStorageTrigger(t *testing.T) {
	t.Parallel()

	e, ch := logEnv(t)
	triggerFn(t, e, typeFinalized, 0, filter("bucket", "b1"))
	e.handler.Backend.HandleStorageEvent(gcpfunctions.StorageEvent{
		Type: "finalized", Bucket: "b1", Name: "o", Data: []byte(`{}`),
	})

	got := nextEntry(t, ch)
	assert.Equal(t, "projects/test-project/logs/run.googleapis.com%2Frequests", got.GetLogName())
	assert.Equal(t, int32(http.StatusOK), got.GetHttpRequest().GetStatus())
}
