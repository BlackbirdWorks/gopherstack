package xray

import (
	"fmt"
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
// Returns ErrTraceRetrievalNotFound if the token was never created by StartTraceRetrieval
// (real AWS declares ResourceNotFoundException for CancelTraceRetrieval on unknown tokens).
func (b *InMemoryBackend) CancelTraceRetrieval(retrievalToken string) error {
	b.mu.Lock("CancelTraceRetrieval")
	defer b.mu.Unlock()

	if !b.traceRetrievals.Delete(retrievalToken) {
		return fmt.Errorf("%w: retrieval token %s not found", ErrTraceRetrievalNotFound, retrievalToken)
	}

	return nil
}

// GetRetrievedTracesGraph returns the status and a service graph built from
// the traces the retrieval token matched (b.retrievedTraces, the same store
// ListRetrievedTraces reads). Returns ErrTraceRetrievalNotFound if the token
// was never created by StartTraceRetrieval.
func (b *InMemoryBackend) GetRetrievedTracesGraph(retrievalToken string) (string, []map[string]any, error) {
	b.mu.RLock("GetRetrievedTracesGraph")
	defer b.mu.RUnlock()

	tr, ok := b.traceRetrievals.Get(retrievalToken)
	if !ok {
		return "", nil, fmt.Errorf("%w: retrieval token %s not found", ErrTraceRetrievalNotFound, retrievalToken)
	}

	filtered := map[string][]*Segment{}

	for _, t := range b.retrievedTraces[retrievalToken] {
		if segs := b.traceSegments.Get(t.TraceID); len(segs) > 0 {
			filtered[t.TraceID] = segs
		}
	}

	if len(filtered) == 0 {
		return tr.Status, []map[string]any{}, nil
	}

	return tr.Status, buildServiceGraph(filtered), nil
}

// StartTraceRetrieval creates a new retrieval job for the given trace IDs and
// returns a token. Only traces whose StartTime falls within [rangeStart,
// rangeEnd] (inclusive, per api_op_StartTraceRetrieval.go's doc comments) are
// included in the retrieval's results, matching real X-Ray's required
// StartTime/EndTime request time range.
func (b *InMemoryBackend) StartTraceRetrieval(traceIDs []string, rangeStart, rangeEnd time.Time) string {
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

	// Pre-populate results using stored traces that match the requested IDs
	// and fall within the requested time range.
	if b.retrievedTraces == nil {
		b.retrievedTraces = make(map[string][]*Trace)
	}

	results := make([]*Trace, 0, len(traceIDs))

	for _, id := range traceIDs {
		t, ok := b.traces.Get(id)
		if !ok {
			continue
		}

		if t.StartTime.Before(rangeStart) || t.StartTime.After(rangeEnd) {
			continue
		}

		cp := *t
		results = append(results, &cp)
	}

	b.retrievedTraces[token] = results

	return token
}

// ListRetrievedTraces returns the status and traces associated with a retrieval token.
// Returns ErrTraceRetrievalNotFound if the token was never created by StartTraceRetrieval.
func (b *InMemoryBackend) ListRetrievedTraces(retrievalToken string) (string, []*Trace, error) {
	b.mu.RLock("ListRetrievedTraces")
	defer b.mu.RUnlock()

	tr, ok := b.traceRetrievals.Get(retrievalToken)
	if !ok {
		return "", nil, fmt.Errorf("%w: retrieval token %s not found", ErrTraceRetrievalNotFound, retrievalToken)
	}

	traces := b.retrievedTraces[retrievalToken]

	out := make([]*Trace, len(traces))
	for i, t := range traces {
		cp := *t
		out[i] = &cp
	}

	return tr.Status, out, nil
}

const (
	// traceRetrievalStatusComplete is the retrieval status assigned to a newly
	// started trace retrieval; gopherstack completes retrievals synchronously.
	traceRetrievalStatusComplete = "COMPLETE"
)
