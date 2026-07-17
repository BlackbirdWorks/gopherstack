package xray

import (
	"strconv"
	"time"
)

// AddTraceRetrievalInternal seeds a trace retrieval token directly for testing.
func (b *InMemoryBackend) AddTraceRetrievalInternal(retrieval TraceRetrieval) {
	b.mu.Lock("AddTraceRetrievalInternal")
	defer b.mu.Unlock()

	b.traceRetrievals.Put(&retrieval)
}

// CancelTraceRetrieval marks a trace retrieval as cancelled.
// If the token is not found the operation is a no-op (idempotent).
func (b *InMemoryBackend) CancelTraceRetrieval(retrievalToken string) {
	b.mu.Lock("CancelTraceRetrieval")
	defer b.mu.Unlock()

	b.traceRetrievals.Delete(retrievalToken)
}

// GetRetrievedTracesGraph returns the status and services for a retrieval token.
// If the token is not found a COMPLETE status is returned.
func (b *InMemoryBackend) GetRetrievedTracesGraph(retrievalToken string) (string, []*Trace) {
	b.mu.RLock("GetRetrievedTracesGraph")
	defer b.mu.RUnlock()

	tr, ok := b.traceRetrievals.Get(retrievalToken)
	if !ok {
		return traceRetrievalStatusComplete, nil
	}

	return tr.Status, nil
}

// StartTraceRetrieval creates a new retrieval job for the given trace IDs and returns a token.
func (b *InMemoryBackend) StartTraceRetrieval(traceIDs []string) string {
	b.mu.Lock("StartTraceRetrieval")
	defer b.mu.Unlock()

	now := time.Now()
	token := "retrieval-" + strconv.FormatInt(now.UnixNano(), 10)

	retrieval := &TraceRetrieval{
		RetrievalToken: token,
		StartTime:      now,
		Status:         traceRetrievalStatusComplete,
	}

	b.traceRetrievals.Put(retrieval)
	b.retrievalTimes[token] = now

	// Pre-populate results using stored traces that match the requested IDs.
	if b.retrievedTraces == nil {
		b.retrievedTraces = make(map[string][]*Trace)
	}

	results := make([]*Trace, 0, len(traceIDs))

	for _, id := range traceIDs {
		if t, ok := b.traces.Get(id); ok {
			cp := *t
			results = append(results, &cp)
		}
	}

	b.retrievedTraces[token] = results

	return token
}

// ListRetrievedTraces returns the status and traces associated with a retrieval token.
func (b *InMemoryBackend) ListRetrievedTraces(retrievalToken string) (string, []*Trace) {
	b.mu.RLock("ListRetrievedTraces")
	defer b.mu.RUnlock()

	tr, ok := b.traceRetrievals.Get(retrievalToken)
	if !ok {
		return traceRetrievalStatusComplete, nil
	}

	traces := b.retrievedTraces[retrievalToken]

	out := make([]*Trace, len(traces))
	for i, t := range traces {
		cp := *t
		out[i] = &cp
	}

	return tr.Status, out
}

const (
	// traceRetrievalStatusComplete is the retrieval status returned for unknown tokens.
	traceRetrievalStatusComplete = "COMPLETE"
)
