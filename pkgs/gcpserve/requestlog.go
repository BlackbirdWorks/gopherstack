package gcpserve

import (
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/durationpb"
)

// RequestRecorder remembers the status and response size of a proxied request without wrapping the ResponseWriter.
type RequestRecorder struct {
	status int
	size   int64
}

// NewRequestRecorder returns an empty recorder; Attach it to the proxy before serving.
func NewRequestRecorder() *RequestRecorder { return &RequestRecorder{} }

// Attach records p's upstream status and body size and its error responses.
func (r *RequestRecorder) Attach(p *httputil.ReverseProxy) {
	p.ModifyResponse = func(resp *http.Response) error {
		r.status = resp.StatusCode
		resp.Body = &countingBody{ReadCloser: resp.Body, n: &r.size}

		return nil
	}

	onErr := p.ErrorHandler
	p.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		r.status = http.StatusBadGateway
		if errors.Is(req.Context().Err(), context.DeadlineExceeded) {
			r.status = http.StatusGatewayTimeout
		}

		if onErr != nil {
			onErr(w, req, err)

			return
		}

		w.WriteHeader(r.status)
	}
}

type countingBody struct {
	io.ReadCloser

	n *int64
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	*c.n += int64(n)

	return n, err
}

// Size returns the response bytes read from the upstream.
func (r *RequestRecorder) Size() int64 { return r.size }

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
	t.write(ctx, sink, t.RequestEntry(r, rec.Status(), rec.Size(), took))
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
