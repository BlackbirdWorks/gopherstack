package gcpserve

import (
	"context"
	"maps"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/durationpb"
)

// RequestRecorder wraps a ResponseWriter to remember the status code and response size.
type RequestRecorder struct {
	http.ResponseWriter

	status int
	size   int64
}

// NewRequestRecorder wraps w.
func NewRequestRecorder(w http.ResponseWriter) *RequestRecorder {
	return &RequestRecorder{ResponseWriter: w}
}

// WriteHeader records the status.
func (r *RequestRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}

	r.ResponseWriter.WriteHeader(code)
}

// Write records the size.
func (r *RequestRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}

	n, err := r.ResponseWriter.Write(p)
	r.size += int64(n)

	return n, err
}

// Flush forwards to the underlying writer so streaming responses keep working.
func (r *RequestRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *RequestRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Status returns the recorded status, 200 when the handler wrote nothing.
func (r *RequestRecorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}

	return r.status
}

// LogRequest writes the run.googleapis.com/requests entry for a finished request.
func (t LogTarget) LogRequest(
	ctx context.Context,
	sink LogSink,
	r *http.Request,
	rec *RequestRecorder,
	took time.Duration,
) {
	t.write(ctx, sink, t.RequestEntry(r, rec.Status(), rec.size, took))
}

// RequestEntry builds the request-log entry with its httpRequest fields.
func (t LogTarget) RequestEntry(r *http.Request, status int, respSize int64, took time.Duration) *loggingpb.LogEntry {
	e := &loggingpb.LogEntry{
		LogName:  LogName(t.Project, "run.googleapis.com/requests"),
		Resource: t.Resource,
		Labels:   maps.Clone(t.Labels),
		Severity: requestSeverity(status),
		HttpRequest: &ltype.HttpRequest{
			RequestMethod: r.Method,
			RequestUrl:    "http://" + r.Host + r.URL.RequestURI(),
			RequestSize:   max(r.ContentLength, 0),
			Status:        int32(min(max(status, 0), math.MaxInt32)),
			ResponseSize:  respSize,
			UserAgent:     r.UserAgent(),
			RemoteIp:      remoteIP(r.RemoteAddr),
			Referer:       r.Referer(),
			Latency:       durationpb.New(took),
			Protocol:      r.Proto,
		},
	}

	t.applyTraceHeader(e, r.Header.Get("X-Cloud-Trace-Context"))

	return e
}

func requestSeverity(status int) ltype.LogSeverity {
	switch {
	case status >= http.StatusInternalServerError:
		return ltype.LogSeverity_ERROR
	case status >= http.StatusBadRequest:
		return ltype.LogSeverity_WARNING
	default:
		return ltype.LogSeverity_INFO
	}
}

func remoteIP(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}

	return addr
}

// applyTraceHeader maps "TRACE_ID[/SPAN_ID][;o=1]" onto the entry's trace fields.
func (t LogTarget) applyTraceHeader(e *loggingpb.LogEntry, h string) {
	if h == "" {
		return
	}

	h, opts, _ := strings.Cut(h, ";")
	trace, _, _ := strings.Cut(h, "/")

	if trace == "" {
		return
	}

	e.Trace = "projects/" + t.Project + "/traces/" + trace
	e.TraceSampled = opts == "o=1"
}
