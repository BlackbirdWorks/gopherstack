package xray

import (
	"context"
	"encoding/json"
	"time"
)

type telemetryRecordInput struct {
	Timestamp              float64 `json:"Timestamp"`
	SegmentsReceivedCount  int32   `json:"SegmentsReceivedCount"`
	SegmentsSentCount      int32   `json:"SegmentsSentCount"`
	SegmentsSpilloverCount int32   `json:"SegmentsSpilloverCount"`
	SegmentsRejectedCount  int32   `json:"SegmentsRejectedCount"`
}

type putTelemetryRecordsInput struct {
	TelemetryRecords []telemetryRecordInput `json:"TelemetryRecords"`
}

func (h *Handler) handlePutTelemetryRecords(_ context.Context, body []byte) ([]byte, error) {
	var in putTelemetryRecordsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	records := make([]TelemetryRecord, 0, len(in.TelemetryRecords))

	for _, r := range in.TelemetryRecords {
		ts := time.Now()
		if r.Timestamp > 0 {
			ts = time.Unix(int64(r.Timestamp), 0)
		}

		records = append(records, TelemetryRecord{
			Timestamp:              ts,
			SegmentsReceivedCount:  r.SegmentsReceivedCount,
			SegmentsSentCount:      r.SegmentsSentCount,
			SegmentsSpilloverCount: r.SegmentsSpilloverCount,
			SegmentsRejectedCount:  r.SegmentsRejectedCount,
		})
	}

	if len(records) > 0 {
		h.Backend.PutTelemetryRecords(records)
	}

	return json.Marshal(map[string]any{})
}
