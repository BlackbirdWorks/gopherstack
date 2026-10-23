package gcpcloudbuild

import (
	"bytes"
	"context"
	"strings"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	cloudBuildLogID = "cloudbuild"
	maxLogBytes     = 32 << 20
	maxLogLine      = 32 << 10
	maskedValue     = "******"
)

// buildLog collects a build's output, forwarding each line to Cloud Logging when the logging mode allows it.
type buildLog struct {
	sink     gcpserve.LogSink
	now      func() time.Time
	resource *monitoredres.MonitoredResource
	mu       *lockmetrics.RWMutex
	logName  string
	masked   []string
	buf      bytes.Buffer
	toBucket bool
}

func newBuildLog(b *cloudbuildpb.Build, sink gcpserve.LogSink, now func() time.Time) *buildLog {
	mode := b.GetOptions().GetLogging()
	toLogging := mode != cloudbuildpb.BuildOptions_GCS_ONLY &&
		mode != cloudbuildpb.BuildOptions_NONE

	l := &buildLog{
		now:      now,
		mu:       lockmetrics.New("gcpcloudbuild.log"),
		logName:  gcpserve.LogName(b.GetProjectId(), cloudBuildLogID),
		toBucket: logsToBucket(mode),
		resource: &monitoredres.MonitoredResource{Type: "build", Labels: map[string]string{
			"project_id": b.GetProjectId(), "build_id": b.GetId(), "build_trigger_id": b.GetBuildTriggerId(),
		}},
	}

	if toLogging {
		l.sink = sink
	}

	return l
}

// mask hides secret values in everything logged from now on.
func (l *buildLog) mask(values ...string) {
	l.mu.Lock("mask")
	defer l.mu.Unlock()

	for _, v := range values {
		if v != "" {
			l.masked = append(l.masked, v)
		}
	}
}

func (l *buildLog) line(text string) {
	l.mu.Lock("line")

	for _, m := range l.masked {
		text = strings.ReplaceAll(text, m, maskedValue)
	}

	if l.buf.Len() < maxLogBytes {
		l.buf.WriteString(text + "\n")
	}

	sink := l.sink

	l.mu.Unlock()

	if sink == nil {
		return
	}

	_ = sink.WriteLogEntries(
		context.Background(),
		&loggingpb.WriteLogEntriesRequest{Entries: []*loggingpb.LogEntry{{
			LogName: l.logName, Resource: l.resource, Timestamp: timestamppb.New(l.now()),
			Payload: &loggingpb.LogEntry_TextPayload{TextPayload: text},
		}}},
	)
}

// system logs a phase marker such as BUILD or Starting Step #0.
func (l *buildLog) system(text string) { l.line(text) }

// step logs text attributed to a step.
func (l *buildLog) step(label, text string) { l.line(label + ": " + text) }

// text returns everything logged so far.
func (l *buildLog) text() []byte {
	l.mu.RLock("text")
	defer l.mu.RUnlock()

	return bytes.Clone(l.buf.Bytes())
}

// stepWriter splits a container output stream into lines of the build log.
type stepWriter struct {
	l       *buildLog
	label   string
	pending []byte
}

func (l *buildLog) writer(label string) *stepWriter { return &stepWriter{l: l, label: label} }

func (w *stepWriter) Write(p []byte) (int, error) {
	w.pending = append(w.pending, p...)

	for {
		i := bytes.IndexByte(w.pending, '\n')
		if i < 0 {
			break
		}

		w.emit(string(w.pending[:i]))
		w.pending = w.pending[i+1:]
	}

	if len(w.pending) > maxLogLine {
		w.flush()
	}

	return len(p), nil
}

func (w *stepWriter) emit(line string) {
	w.l.step(w.label, strings.TrimRight(line, "\r"))
}

func (w *stepWriter) flush() {
	if len(w.pending) > 0 {
		w.emit(string(w.pending))
		w.pending = nil
	}
}

// logsToBucket reports whether the logging mode keeps a copy of the log in the logs bucket.
func logsToBucket(mode cloudbuildpb.BuildOptions_LoggingMode) bool {
	switch mode {
	case cloudbuildpb.BuildOptions_CLOUD_LOGGING_ONLY, cloudbuildpb.BuildOptions_NONE,
		cloudbuildpb.BuildOptions_STACKDRIVER_ONLY: //nolint:staticcheck // still accepted for old clients
		return false
	default:
		return true
	}
}
