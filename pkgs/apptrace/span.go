// Package apptrace keeps a bounded, in-memory trace of every AWS API call
// that passes through the router, plus child spans for cross-service
// deliveries (SNS to SQS, EventBridge targets, ...) linked to the request that
// triggered them.
package apptrace

import (
	"context"
	"time"
)

// Span kinds.
const (
	KindRequest  = "request"
	KindInternal = "internal"
)

// Detail is the opt-in capture of headers and truncated, redacted bodies.
type Detail struct {
	RequestHeaders  map[string]string `json:"requestHeaders,omitempty"`
	ResponseHeaders map[string]string `json:"responseHeaders,omitempty"`
	Query           string            `json:"query,omitempty"`
	RequestBody     string            `json:"requestBody,omitempty"`
	ResponseBody    string            `json:"responseBody,omitempty"`
	RequestTrunc    bool              `json:"requestBodyTruncated,omitempty"`
	ResponseTrunc   bool              `json:"responseBodyTruncated,omitempty"`
}

// Span is one recorded API call or internal delivery. It is immutable once
// handed to [Tracer.Record].
type Span struct {
	Start         time.Time `json:"time"`
	Detail        *Detail   `json:"detail,omitempty"`
	tracer        *Tracer
	ID            string  `json:"id"`
	TraceID       string  `json:"traceId"`
	ParentID      string  `json:"parentId,omitempty"`
	Kind          string  `json:"kind"`
	Service       string  `json:"service"`
	Operation     string  `json:"operation"`
	Resource      string  `json:"resource,omitempty"`
	Region        string  `json:"region,omitempty"`
	Account       string  `json:"account,omitempty"`
	Principal     string  `json:"principal,omitempty"`
	AccessKeyID   string  `json:"accessKeyId,omitempty"`
	Method        string  `json:"method,omitempty"`
	Path          string  `json:"path,omitempty"`
	ErrorCode     string  `json:"errorCode,omitempty"`
	LatencyMs     float64 `json:"latencyMs"`
	RequestBytes  int64   `json:"requestBytes"`
	ResponseBytes int64   `json:"responseBytes"`
	Seq           uint64  `json:"seq"`
	Status        int     `json:"status"`
	HasDetail     bool    `json:"hasDetail,omitempty"`
}

type ctxKey struct{}

// WithSpan returns ctx carrying s as the current span.
func WithSpan(ctx context.Context, s *Span) context.Context {
	return context.WithValue(ctx, ctxKey{}, s)
}

// FromContext returns the current span, or nil when the request is not traced.
func FromContext(ctx context.Context) *Span {
	s, _ := ctx.Value(ctxKey{}).(*Span)

	return s
}

// SetOperation records the service, operation and resource the service layer
// resolved for this request. Safe on a nil span.
func (s *Span) SetOperation(service, operation, resource string) {
	if s == nil {
		return
	}

	s.Service, s.Operation, s.Resource = service, operation, resource
}

// ChildInfo describes an internal delivery to trace.
type ChildInfo struct {
	Service   string
	Operation string
	Resource  string
	Path      string
	Region    string
	Account   string
}

// StartChild opens a child span under the span in ctx. It returns ctx
// unchanged and a nil span when ctx is untraced; [Span.Finish] is nil-safe.
func StartChild(ctx context.Context, info ChildInfo) (context.Context, *Span) {
	parent := FromContext(ctx)
	if parent == nil || parent.tracer == nil || !parent.tracer.Enabled() {
		return ctx, nil
	}

	child := &Span{
		ID:        parent.tracer.childID(parent.TraceID),
		TraceID:   parent.TraceID,
		ParentID:  parent.ID,
		Kind:      KindInternal,
		Start:     time.Now(),
		Service:   info.Service,
		Operation: info.Operation,
		Resource:  info.Resource,
		Path:      info.Path,
		Region:    info.Region,
		Account:   info.Account,
		tracer:    parent.tracer,
	}

	return WithSpan(ctx, child), child
}

type tracerKey struct{}

// WithTracer returns ctx carrying t so background work with no originating
// request can open root spans via [StartRoot].
func WithTracer(ctx context.Context, t *Tracer) context.Context {
	return context.WithValue(ctx, tracerKey{}, t)
}

// TracerFrom returns the tracer stored by [WithTracer], or nil.
func TracerFrom(ctx context.Context) *Tracer {
	t, _ := ctx.Value(tracerKey{}).(*Tracer)

	return t
}

// StartRoot opens a parentless internal span that begins a new trace, for
// work (such as event-source-mapping polls) with no originating request.
// Returns ctx and a nil span when no enabled tracer is in ctx.
func StartRoot(ctx context.Context, info ChildInfo) (context.Context, *Span) {
	t := TracerFrom(ctx)
	if t == nil || !t.Enabled() {
		return ctx, nil
	}

	id := t.childID("root")
	root := &Span{
		ID: id, TraceID: id, Kind: KindInternal, Start: time.Now(),
		Service: info.Service, Operation: info.Operation, Resource: info.Resource, Path: info.Path,
		Region: info.Region, Account: info.Account, tracer: t,
	}

	return WithSpan(ctx, root), root
}

// Link returns base carrying the identity of the span in ctx, so work handed
// to a detached goroutine (whose context is not derived from the request)
// still nests under the originating request. It returns base unchanged when
// ctx is untraced.
func Link(ctx, base context.Context) context.Context {
	parent := FromContext(ctx)
	if parent == nil || parent.tracer == nil || !parent.tracer.Enabled() {
		return base
	}

	return WithSpan(base, &Span{ID: parent.ID, TraceID: parent.TraceID, tracer: parent.tracer})
}

// FinishErr commits a child span, recording err (if any) as the error code.
// Safe on nil.
func (s *Span) FinishErr(err error) {
	if s == nil {
		return
	}

	if err != nil {
		s.Finish(errorLabel)

		return
	}

	s.Finish("")
}

const errorLabel = "DeliveryFailed"

// Finish commits a child span. errCode is empty on success. Safe on nil.
func (s *Span) Finish(errCode string) {
	if s == nil || s.tracer == nil {
		return
	}

	s.LatencyMs = float64(time.Since(s.Start)) / float64(time.Millisecond)
	s.Status = 200
	s.ErrorCode = errCode

	if errCode != "" {
		s.Status = 500
	}

	s.tracer.Record(s)
}
