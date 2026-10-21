package gcpserve_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltype "google.golang.org/genproto/googleapis/logging/type"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

type sinkRecorder struct {
	entries []*loggingpb.LogEntry
	mu      sync.Mutex
}

func (s *sinkRecorder) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, req.GetEntries()...)

	return nil
}

type chunk struct {
	data   string
	stderr bool
}

type scriptedStreamer struct{ chunks []chunk }

func (s scriptedStreamer) StreamLogs(_ context.Context, _ string, stdout, stderr io.Writer) error {
	for _, c := range s.chunks {
		w := stdout
		if c.stderr {
			w = stderr
		}

		if _, err := io.WriteString(w, c.data); err != nil {
			return err
		}
	}

	return nil
}

var _ container.LogStreamer = scriptedStreamer{}

func target() gcpserve.LogTarget {
	return gcpserve.LogTarget{
		Project:  "proj",
		Resource: gcpserve.CloudRunResource("proj", "svc", "svc-00001-abc", "us-central1"),
		Labels:   map[string]string{"instanceId": "c1"},
	}
}

func TestStreamLogsLines(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("x", 40<<10)

	tests := []struct {
		name   string
		want   []string
		chunks []chunk
	}{
		{
			name:   "split writes",
			chunks: []chunk{{data: "hel"}, {data: "lo\nwor"}, {data: "ld\n"}},
			want:   []string{"hello", "world"},
		},
		{name: "unterminated tail", chunks: []chunk{{data: "a\nb"}}, want: []string{"a", "b"}},
		{name: "blank and crlf", chunks: []chunk{{data: "\n\r\nx\r\n"}}, want: []string{"x"}},
		{
			name:   "long line bounded",
			chunks: []chunk{{data: long + "\n"}},
			want:   []string{long[:16<<10], long[:16<<10], long[:8<<10]},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sink := &sinkRecorder{}
			require.NoError(t, gcpserve.StreamLogs(t.Context(), scriptedStreamer{tt.chunks}, "c1", target(), sink))

			got := make([]string, 0, len(sink.entries))
			for _, e := range sink.entries {
				got = append(got, e.GetTextPayload())
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStreamLogsUnsupported(t *testing.T) {
	t.Parallel()

	err := gcpserve.StreamLogs(t.Context(), struct{}{}, "c1", target(), &sinkRecorder{})
	require.ErrorIs(t, err, container.ErrLogsUnsupported)
}

func TestLineEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(t *testing.T, e *loggingpb.LogEntry)
		name   string
		stream string
		line   string
	}{
		{
			name: "stdout text", stream: "stdout", line: "plain",
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.Equal(t, "projects/proj/logs/run.googleapis.com%2Fstdout", e.GetLogName())
				assert.Equal(t, "plain", e.GetTextPayload())
				assert.Equal(t, ltype.LogSeverity_DEFAULT, e.GetSeverity())
				assert.Equal(t, "cloud_run_revision", e.GetResource().GetType())
				assert.Equal(t, map[string]string{
					"project_id": "proj", "service_name": "svc", "revision_name": "svc-00001-abc",
					"location": "us-central1", "configuration_name": "svc",
				}, e.GetResource().GetLabels())
				assert.Equal(t, "c1", e.GetLabels()["instanceId"])
			},
		},
		{
			name: "stderr is error", stream: "stderr", line: "boom",
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.Equal(t, "projects/proj/logs/run.googleapis.com%2Fstderr", e.GetLogName())
				assert.Equal(t, ltype.LogSeverity_ERROR, e.GetSeverity())
			},
		},
		{
			name: "json object", stream: "stdout", line: `{"a":1,"b":"x"}`,
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.InDelta(t, 1, e.GetJsonPayload().GetFields()["a"].GetNumberValue(), 0)
				assert.Equal(t, "x", e.GetJsonPayload().GetFields()["b"].GetStringValue())
				assert.Empty(t, e.GetTextPayload())
			},
		},
		{
			name: "structured specials", stream: "stderr",
			line: `{"severity":"warning","message":"m","logging.googleapis.com/trace":"abc",` +
				`"logging.googleapis.com/spanId":"s1",` +
				`"logging.googleapis.com/trace_sampled":true,"logging.googleapis.com/labels":{"k":"v"},` +
				`"logging.googleapis.com/sourceLocation":{"file":"f.go","line":"7","function":"fn"},"extra":true}`,
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.Equal(t, ltype.LogSeverity_WARNING, e.GetSeverity())
				assert.Equal(t, "projects/proj/traces/abc", e.GetTrace())
				assert.Equal(t, "s1", e.GetSpanId())
				assert.True(t, e.GetTraceSampled())
				assert.Equal(t, "v", e.GetLabels()["k"])
				assert.Equal(t, "c1", e.GetLabels()["instanceId"])
				assert.Equal(t, "f.go", e.GetSourceLocation().GetFile())
				assert.Equal(t, int64(7), e.GetSourceLocation().GetLine())

				f := e.GetJsonPayload().GetFields()
				assert.Equal(t, "m", f["message"].GetStringValue())
				assert.True(t, f["extra"].GetBoolValue())
				assert.NotContains(t, f, "severity")
				assert.NotContains(t, f, "logging.googleapis.com/trace")
			},
		},
		{
			name: "message only becomes text", stream: "stdout", line: `{"severity":"INFO","message":"hi"}`,
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.Equal(t, "hi", e.GetTextPayload())
				assert.Equal(t, ltype.LogSeverity_INFO, e.GetSeverity())
			},
		},
		{
			name: "unknown severity kept", stream: "stdout", line: `{"severity":"LOUD","x":1}`,
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.Equal(t, ltype.LogSeverity_DEFAULT, e.GetSeverity())
				assert.Contains(t, e.GetJsonPayload().GetFields(), "severity")
			},
		},
		{
			name: "invalid json is text", stream: "stdout", line: `{not json`,
			check: func(t *testing.T, e *loggingpb.LogEntry) {
				t.Helper()
				assert.Equal(t, "{not json", e.GetTextPayload())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.check(t, target().LineEntry(tt.stream, tt.line))
		})
	}
}
