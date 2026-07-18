package dax

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type describeEventsRequest struct {
	SourceName string      `json:"SourceName"`
	SourceType string      `json:"SourceType"`
	StartTime  json.Number `json:"StartTime"`
	EndTime    json.Number `json:"EndTime"`
	NextToken  string      `json:"NextToken"`
	MaxResults int         `json:"MaxResults"`
	Duration   int         `json:"Duration"` // minutes to look back; applied when StartTime is absent
}

type eventResponse struct {
	SourceName string `json:"SourceName"`
	SourceType string `json:"SourceType"`
	Message    string `json:"Message"`
	// Date is epoch seconds (float64); see nodeResponse.NodeCreateTime.
	Date float64 `json:"Date"`
}

// parseEpochSeconds converts a wire-format epoch-seconds JSON number (the
// awsjson1.1 Timestamp wire format DAX uses -- see smithytime.ParseEpochSeconds
// in the real SDK deserializer) into a time.Time, preserving sub-second
// precision carried in the fractional part.
func parseEpochSeconds(n json.Number) (time.Time, error) {
	f, err := n.Float64()
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sec := int64(f)
	nsec := int64((f - float64(sec)) * float64(time.Second))

	return time.Unix(sec, nsec).UTC(), nil
}

func (h *Handler) handleDescribeEvents(body []byte) (any, error) {
	var req describeEventsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	var startTime, endTime *time.Time

	if req.StartTime != "" {
		t, err := parseEpochSeconds(req.StartTime)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid StartTime format", errInvalidRequest)
		}

		startTime = &t
	}

	if req.EndTime != "" {
		t, err := parseEpochSeconds(req.EndTime)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid EndTime format", errInvalidRequest)
		}

		endTime = &t
	}

	// Duration (minutes) sets StartTime to now - Duration when StartTime is absent.
	if req.Duration > 0 && startTime == nil {
		t := time.Now().UTC().Add(-time.Duration(req.Duration) * time.Minute)
		startTime = &t
	}

	events, nextToken, err := h.Backend.DescribeEvents(
		req.SourceName,
		req.SourceType,
		startTime,
		endTime,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]eventResponse, 0, len(events))
	for _, ev := range events {
		items = append(items, eventResponse{
			SourceName: ev.SourceName,
			SourceType: ev.SourceType,
			Message:    ev.Message,
			Date:       awstime.Epoch(ev.Date),
		})
	}

	result := map[string]any{
		"Events": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}
