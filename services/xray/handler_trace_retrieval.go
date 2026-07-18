package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// segmentDoc is used to extract timing and ID from a raw segment JSON for the retrieval response.
type segmentDoc struct {
	ID        string  `json:"id"`
	StartTime float64 `json:"start_time"`
	EndTime   float64 `json:"end_time"`
}

type listRetrievedTracesInput struct {
	RetrievalToken string `json:"RetrievalToken"`
	NextToken      string `json:"NextToken"`
	MaxResults     int    `json:"MaxResults"`
}

// buildTraceView converts a raw Trace into the map shape returned by ListRetrievedTraces.
func buildTraceView(t *Trace) map[string]any {
	segs := make([]any, 0, len(t.Segments))

	var minStart, maxEnd float64

	for _, rawSeg := range t.Segments {
		var doc segmentDoc
		if err := json.Unmarshal([]byte(rawSeg), &doc); err == nil {
			segs = append(segs, map[string]any{
				"Document": rawSeg,
				"Id":       doc.ID,
			})

			if doc.StartTime > 0 && (minStart == 0 || doc.StartTime < minStart) {
				minStart = doc.StartTime
			}

			if doc.EndTime > maxEnd {
				maxEnd = doc.EndTime
			}
		}
	}

	duration := 0.0
	if maxEnd > minStart && minStart > 0 {
		duration = maxEnd - minStart
	}

	return map[string]any{
		"Id":       t.TraceID,
		"Duration": duration,
		"Segments": segs,
	}
}

func (h *Handler) handleListRetrievedTraces(_ context.Context, body []byte) ([]byte, error) {
	var in listRetrievedTracesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RetrievalToken == "" {
		return nil, fmt.Errorf("%w: RetrievalToken is required", errInvalidRequest)
	}

	status, traces := h.Backend.ListRetrievedTraces(in.RetrievalToken)

	traceViews := make([]map[string]any, 0, len(traces))
	for _, t := range traces {
		traceViews = append(traceViews, buildTraceView(t))
	}

	pg := page.New(traceViews, in.NextToken, in.MaxResults, defaultTracesPageSize)
	resp := map[string]any{
		"RetrievalStatus": status,
		"Traces":          pg.Data,
	}
	if pg.Next != "" {
		resp[keyNextToken] = pg.Next
	}

	return json.Marshal(resp)
}

type startTraceRetrievalInput struct {
	TraceIDs  []string `json:"TraceIds"`
	StartTime float64  `json:"StartTime"`
	EndTime   float64  `json:"EndTime"`
}

func (h *Handler) handleStartTraceRetrieval(_ context.Context, body []byte) ([]byte, error) {
	var in startTraceRetrievalInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceIDs) == 0 {
		return nil, fmt.Errorf("%w: TraceIds is required", errInvalidRequest)
	}

	token := h.Backend.StartTraceRetrieval(in.TraceIDs)

	return json.Marshal(map[string]any{
		"RetrievalToken": token,
	})
}

const (
	defaultTracesPageSize = 100
)

type cancelTraceRetrievalInput struct {
	RetrievalToken string `json:"RetrievalToken"`
}

func (h *Handler) handleCancelTraceRetrieval(_ context.Context, body []byte) ([]byte, error) {
	var in cancelTraceRetrievalInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RetrievalToken == "" {
		return nil, fmt.Errorf("%w: RetrievalToken is required", errInvalidRequest)
	}

	h.Backend.CancelTraceRetrieval(in.RetrievalToken)

	return json.Marshal(map[string]any{})
}

type getRetrievedTracesGraphInput struct {
	RetrievalToken string `json:"RetrievalToken"`
	NextToken      string `json:"NextToken"`
}

func (h *Handler) handleGetRetrievedTracesGraph(_ context.Context, body []byte) ([]byte, error) {
	var in getRetrievedTracesGraphInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RetrievalToken == "" {
		return nil, fmt.Errorf("%w: RetrievalToken is required", errInvalidRequest)
	}

	status, _ := h.Backend.GetRetrievedTracesGraph(in.RetrievalToken)

	return json.Marshal(map[string]any{
		"RetrievalStatus": status,
		keyServices:       []any{},
		keyNextToken:      "",
	})
}
