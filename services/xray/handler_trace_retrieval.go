package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
// The real RetrievedTrace shape's list-of-documents field is called "Spans" (types.Span,
// {Document, Id}), not "Segments" -- a real SDK client's deserializer only recognizes the
// "Spans" key (see awsRestjson1_deserializeDocumentRetrievedTrace), so sending "Segments"
// silently produces an empty Spans slice for every retrieved trace.
func buildTraceView(t *Trace) map[string]any {
	spans := make([]any, 0, len(t.Segments))

	var minStart, maxEnd float64

	for _, rawSeg := range t.Segments {
		var doc segmentDoc
		if err := json.Unmarshal([]byte(rawSeg), &doc); err == nil {
			spans = append(spans, map[string]any{
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
		"Spans":    spans,
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

	status, traces, err := h.Backend.ListRetrievedTraces(in.RetrievalToken)
	if err != nil {
		return nil, err
	}

	traceViews := make([]map[string]any, 0, len(traces))
	for _, t := range traces {
		traceViews = append(traceViews, buildTraceView(t))
	}

	pg := page.New(traceViews, in.NextToken, in.MaxResults, defaultTracesPageSize)
	resp := map[string]any{
		"RetrievalStatus": status,
		"Traces":          pg.Data,
		// TraceFormat is always XRAY: gopherstack only ever stores raw X-Ray segment
		// JSON (never OpenTelemetry-format spans), so it can never legitimately be OTEL.
		"TraceFormat": "XRAY",
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

	if in.StartTime == 0 || in.EndTime == 0 {
		return nil, fmt.Errorf("%w: StartTime and EndTime are required", errInvalidRequest)
	}

	token := h.Backend.StartTraceRetrieval(
		in.TraceIDs,
		time.Unix(int64(in.StartTime), 0),
		time.Unix(int64(in.EndTime), 0),
	)

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

	if err := h.Backend.CancelTraceRetrieval(in.RetrievalToken); err != nil {
		return nil, err
	}

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

	status, services, err := h.Backend.GetRetrievedTracesGraph(in.RetrievalToken)
	if err != nil {
		return nil, err
	}

	pg := page.New(services, in.NextToken, 0, defaultServiceGraphPageSize)

	return json.Marshal(map[string]any{
		"RetrievalStatus": status,
		keyServices:       pg.Data,
		keyNextToken:      pg.Next,
	})
}
