package xray

import (
	"context"
	"encoding/json"
	"fmt"
)

type putTraceSegmentsInput struct {
	TraceSegmentDocuments []string `json:"TraceSegmentDocuments"`
}

func (h *Handler) handlePutTraceSegments(_ context.Context, body []byte) ([]byte, error) {
	var in putTraceSegmentsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceSegmentDocuments) > maxTraceSegmentsPerCall {
		return nil, fmt.Errorf("%w: PutTraceSegments accepts at most %d documents per call, got %d",
			errInvalidRequest, maxTraceSegmentsPerCall, len(in.TraceSegmentDocuments))
	}

	for i, doc := range in.TraceSegmentDocuments {
		if len(doc) > maxSegmentDocumentBytes {
			return nil, fmt.Errorf("%w: segment document %d exceeds maximum size of %d bytes (got %d)",
				errInvalidRequest, i, maxSegmentDocumentBytes, len(doc))
		}
	}

	unprocessed := h.Backend.PutTraceSegments(in.TraceSegmentDocuments)

	type unprocessedSegment struct {
		ID        string `json:"Id"`
		ErrorCode string `json:"ErrorCode,omitempty"`
		Message   string `json:"Message,omitempty"`
	}

	out := make([]unprocessedSegment, 0, len(unprocessed))
	for _, id := range unprocessed {
		out = append(out, unprocessedSegment{ID: id, ErrorCode: "InvalidSegment", Message: "failed to parse segment"})
	}

	return json.Marshal(map[string]any{
		"UnprocessedTraceSegments": out,
	})
}
