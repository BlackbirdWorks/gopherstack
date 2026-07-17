package xray

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"
)

// GetTraceSummaries returns all trace summaries sorted by start time (newest first).
func (b *InMemoryBackend) GetTraceSummaries() []Trace {
	b.mu.RLock("GetTraceSummaries")
	defer b.mu.RUnlock()

	all := b.traces.All()
	out := make([]Trace, 0, len(all))

	for _, t := range all {
		cp := *t
		cp.Segments = make([]string, len(t.Segments))
		copy(cp.Segments, t.Segments)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].StartTime.After(out[j].StartTime)
	})

	return out
}

// GetTrace returns the trace with the given ID, or nil if not found.
func (b *InMemoryBackend) GetTrace(traceID string) *Trace {
	b.mu.RLock("GetTrace")
	defer b.mu.RUnlock()

	t, ok := b.traces.Get(traceID)
	if !ok {
		return nil
	}

	cp := *t
	cp.Segments = make([]string, len(t.Segments))
	copy(cp.Segments, t.Segments)

	return &cp
}

// GetParsedSegments returns a copy of the parsed segments for a given trace ID.
func (b *InMemoryBackend) GetParsedSegments(traceID string) []*Segment {
	b.mu.RLock("GetParsedSegments")
	defer b.mu.RUnlock()

	segs := b.traceSegments.Get(traceID)
	out := make([]*Segment, len(segs))
	copy(out, segs)

	return out
}

// GetAllParsedSegments returns all parsed segments, keyed by trace ID.
func (b *InMemoryBackend) GetAllParsedSegments() map[string][]*Segment {
	b.mu.RLock("GetAllParsedSegments")
	defer b.mu.RUnlock()

	out := make(map[string][]*Segment)

	for _, t := range b.traces.All() {
		segs := b.traceSegments.Get(t.TraceID)
		if len(segs) == 0 {
			continue
		}

		cp := make([]*Segment, len(segs))
		copy(cp, segs)
		out[t.TraceID] = cp
	}

	return out
}

// extractRootHTTP populates HTTP fields from the root segment's HTTP data.
// Returns the updated (or newly created) TraceSummaryHTTP pointer.
func extractRootHTTP(segHTTP *SegmentHTTP, existing *TraceSummaryHTTP) *TraceSummaryHTTP {
	if segHTTP == nil {
		return existing
	}

	if segHTTP.Request != nil {
		if existing == nil {
			existing = &TraceSummaryHTTP{}
		}

		existing.HTTPURL = segHTTP.Request.URL
		existing.HTTPMethod = segHTTP.Request.Method
		existing.ClientIP = segHTTP.Request.ClientIP
		existing.UserAgent = segHTTP.Request.UserAgent
	}

	if segHTTP.Response != nil {
		if existing == nil {
			existing = &TraceSummaryHTTP{}
		}

		existing.HTTPStatus = segHTTP.Response.Status
	}

	return existing
}

// BuildTraceSummary derives TraceSummaryData from parsed segments.
// accumulateUserFromAnnotations checks segment annotations for a "user" key and
// appends the value to summary.Users when not already present.
func accumulateUserFromAnnotations(summary *TraceSummaryData, seg *Segment, seenUsers map[string]bool) {
	userVal, ok := seg.Annotations["user"]
	if !ok {
		return
	}

	userStr, isStr := userVal.(string)
	if !isStr || userStr == "" || seenUsers[userStr] {
		return
	}

	seenUsers[userStr] = true
	summary.Users = append(summary.Users, userStr)
}

// accumulateServiceID records the service identity from seg into summary.ServiceIDs when not yet seen.
func accumulateServiceID(summary *TraceSummaryData, seg *Segment, seen map[serviceKey]bool) {
	svcType := seg.Origin
	if svcType == "" {
		svcType = seg.Namespace
	}

	key := serviceKey{Name: seg.Name, Type: svcType}
	if !seen[key] {
		seen[key] = true
		summary.ServiceIDs = append(summary.ServiceIDs, TraceSummaryServiceID{Name: seg.Name, Type: svcType})
	}
}

// BuildTraceSummary derives TraceSummaryData from parsed segments.
func BuildTraceSummary(traceID string, segs []*Segment) TraceSummaryData {
	summary := TraceSummaryData{
		TraceID:     traceID,
		ApproxTime:  float64(time.Now().Unix()),
		Annotations: map[string]any{},
	}

	if len(segs) == 0 {
		summary.IsPartial = true

		return summary
	}

	var minStart, maxEnd float64

	seen := map[serviceKey]bool{}
	seenUsers := map[string]bool{}
	hasRoot := false

	for _, seg := range segs {
		accumulateTraceSummaryFlags(&summary, seg)

		if seg.StartTime > 0 && (minStart == 0 || seg.StartTime < minStart) {
			minStart = seg.StartTime
		}

		if seg.EndTime > maxEnd {
			maxEnd = seg.EndTime
		}

		maps.Copy(summary.Annotations, seg.Annotations)
		accumulateUserFromAnnotations(&summary, seg, seenUsers)
		accumulateServiceID(&summary, seg, seen)

		// Root segment has no parent.
		if seg.ParentID == "" {
			hasRoot = true
			summary.EntryPoint = seg.Name
			summary.HTTP = extractRootHTTP(seg.HTTP, summary.HTTP)
		}
	}

	if !hasRoot {
		summary.IsPartial = true
	}

	if maxEnd > 0 && minStart > 0 {
		summary.Duration = maxEnd - minStart
	}

	// ResponseTime: root segment duration or overall duration.
	summary.ResponseTime = summary.Duration

	return summary
}

// accumulateTraceSummaryFlags copies boolean fault/error/throttle flags from a segment.
func accumulateTraceSummaryFlags(summary *TraceSummaryData, seg *Segment) {
	if seg.Fault {
		summary.HasFault = true
	}

	if seg.Error {
		summary.HasError = true
	}

	if seg.Throttle {
		summary.HasThrottle = true
	}
}

// evaluateHTTPStatusFilter matches `http.status = N` expressions.
func evaluateHTTPStatusFilter(expr string, http *TraceSummaryHTTP) bool {
	parts := strings.Fields(expr)
	if len(parts) != filterParts3 || parts[1] != "=" {
		return false
	}

	n, err := strconv.Atoi(parts[2])
	if err != nil || http == nil {
		return false
	}

	return http.HTTPStatus == n
}

// compareResponseTime applies a comparison operator to response time.
func compareResponseTime(op string, rt, n float64) bool {
	switch op {
	case ">":
		return rt > n
	case ">=":
		return rt >= n
	case "<":
		return rt < n
	case "<=":
		return rt <= n
	case "=":
		return rt == n
	}

	return false
}

// evaluateResponseTimeFilter matches `responsetime OP N.N` expressions.
func evaluateResponseTimeFilter(expr string, rt float64) bool {
	parts := strings.Fields(expr)
	if len(parts) != filterParts3 {
		return false
	}

	n, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return false
	}

	return compareResponseTime(parts[1], rt, n)
}

// evaluateAnnotationFilter matches `annotation.KEY = "VALUE"` expressions.
func evaluateAnnotationFilter(expr string, annotations map[string]any) bool {
	parts := strings.SplitN(expr, "=", filterParts2)
	if len(parts) != filterParts2 {
		return false
	}

	key := strings.TrimSpace(parts[0])
	key = key[len("annotation."):]
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, `"`)

	v, ok := annotations[key]

	return ok && fmt.Sprintf("%v", v) == val
}

// evaluateFilter checks a trace summary against a simple filter expression.
// Supported tokens:
//   - `fault`                      — trace has fault
//   - `error`                      — trace has error
//   - `http.status = N`            — HTTP status equals N
//   - `responsetime > N.N`         — response time comparison (also >=, <, <=, =)
//   - `annotation.KEY = "VALUE"`   — annotation match
//
// Empty expression always returns true.
func evaluateFilter(expr string, summary TraceSummaryData) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}

	lower := strings.ToLower(expr)

	switch lower {
	case "fault":
		return summary.HasFault
	case "error":
		return summary.HasError
	case "throttle":
		return summary.HasThrottle
	}

	if strings.HasPrefix(lower, "http.status") {
		return evaluateHTTPStatusFilter(expr, summary.HTTP)
	}

	if strings.HasPrefix(lower, "responsetime") {
		return evaluateResponseTimeFilter(expr, summary.ResponseTime)
	}

	if strings.HasPrefix(lower, "annotation.") {
		return evaluateAnnotationFilter(expr, summary.Annotations)
	}

	return false
}

// EvaluateFilter is exported for tests.
func EvaluateFilter(expr string, summary TraceSummaryData) bool {
	return evaluateFilter(expr, summary)
}

const (
	// maxBatchGetTraces is the maximum number of trace IDs in a BatchGetTraces call.
	maxBatchGetTraces = 5
)

const (
	// filterParts3 is the expected length of a 3-part filter expression.
	filterParts3 = 3
)

const (
	// filterParts2 is the expected length of a 2-part filter expression.
	filterParts2 = 2
)
