package gcpserve

import (
	"bytes"
	"context"
	"net/url"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// maxLogLine bounds one captured line; longer lines are split.
const maxLogLine = 16 << 10

// Container output streams, named as in the Cloud Run log ids.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// LogSink receives log entries; *gcplogging.InMemoryBackend satisfies it.
type LogSink interface {
	WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) error
}

// LogTarget says which monitored resource a workload's log entries belong to.
type LogTarget struct {
	Resource *monitoredres.MonitoredResource
	Labels   map[string]string
	Project  string
}

// LogName returns projects/{project}/logs/{id} with id URL-escaped, as the Logging API expects.
func LogName(project, id string) string {
	return "projects/" + project + "/logs/" + url.PathEscape(id)
}

// CloudRunResource returns the cloud_run_revision resource that Cloud Run and 2nd-gen functions log under.
func CloudRunResource(project, service, revision, location string) *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{
		Type: "cloud_run_revision",
		Labels: map[string]string{
			"project_id":         project,
			"service_name":       service,
			"revision_name":      revision,
			"location":           location,
			"configuration_name": service,
		},
	}
}

// CloudRunJobResource returns the cloud_run_job resource that Cloud Run job tasks log under.
func CloudRunJobResource(project, job, location string) *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{
		Type: "cloud_run_job",
		Labels: map[string]string{
			"project_id": project,
			"job_name":   job,
			"location":   location,
		},
	}
}

// StreamLogs follows container id's output and writes each line to sink until the container goes away or ctx ends.
// rt must implement container.LogStreamer.
func StreamLogs(ctx context.Context, rt any, id string, t LogTarget, sink LogSink) error {
	ls, ok := rt.(container.LogStreamer)
	if !ok {
		return container.ErrLogsUnsupported
	}

	emit := func(stream string) func(string) {
		return func(line string) { t.write(ctx, sink, t.LineEntry(stream, line)) }
	}

	out := &lineWriter{emit: emit(StreamStdout)}
	errw := &lineWriter{emit: emit(StreamStderr)}

	err := ls.StreamLogs(ctx, id, out, errw)

	out.flush()
	errw.flush()

	return err
}

func (t LogTarget) write(ctx context.Context, sink LogSink, e *loggingpb.LogEntry) {
	_ = sink.WriteLogEntries(context.WithoutCancel(ctx), &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{e},
	})
}

// lineWriter splits a byte stream into lines of at most maxLogLine bytes.
type lineWriter struct {
	emit func(string)
	buf  []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	n := len(p)

	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			w.add(p)

			break
		}

		w.add(p[:i])
		w.flush()

		p = p[i+1:]
	}

	return n, nil
}

func (w *lineWriter) add(p []byte) {
	for len(p) > 0 {
		if len(w.buf) == maxLogLine {
			w.flush()
		}

		take := min(maxLogLine-len(w.buf), len(p))
		w.buf = append(w.buf, p[:take]...)
		p = p[take:]
	}
}

func (w *lineWriter) flush() {
	line := bytes.TrimRight(w.buf, "\r")
	if len(line) > 0 {
		w.emit(string(line))
	}

	w.buf = w.buf[:0]
}
