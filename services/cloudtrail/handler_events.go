package cloudtrail

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- LookupEvents ---

type lookupEventsBody struct {
	StartTime        *int64            `json:"StartTime"`
	EndTime          *int64            `json:"EndTime"`
	NextToken        string            `json:"NextToken"`
	EventCategory    string            `json:"EventCategory"`
	LookupAttributes []LookupAttribute `json:"LookupAttributes"`
	MaxResults       int32             `json:"MaxResults"`
}

func (h *Handler) handleLookupEvents(c *echo.Context, body []byte) error {
	var in lookupEventsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	input := LookupEventsInput{
		LookupAttributes: in.LookupAttributes,
		MaxResults:       in.MaxResults,
		NextToken:        in.NextToken,
		EventCategory:    in.EventCategory,
	}
	if in.StartTime != nil {
		t := time.Unix(*in.StartTime, 0).UTC()
		input.StartTime = &t
	}
	if in.EndTime != nil {
		t := time.Unix(*in.EndTime, 0).UTC()
		input.EndTime = &t
	}

	out := h.Backend.LookupEvents(input)

	resp := map[string]any{"Events": toLookupEventsWire(out.Events)}
	if out.NextToken != "" {
		resp["NextToken"] = out.NextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// lookupEventsEventWire is the real LookupEventsOutput Event shape
// (cloudtrail@v1.58.4 types.go:283) -- unlike the persisted Event, it has no
// top-level EventCategory field (that only appears nested in the
// CloudTrailEvent JSON string).
type lookupEventsEventWire struct {
	EventID         string          `json:"EventId"`
	EventName       string          `json:"EventName"`
	EventSource     string          `json:"EventSource"`
	Username        string          `json:"Username,omitempty"`
	ReadOnly        string          `json:"ReadOnly,omitempty"`
	AccessKeyID     string          `json:"AccessKeyId,omitempty"`
	CloudTrailEvent string          `json:"CloudTrailEvent,omitempty"`
	Resources       []EventResource `json:"Resources,omitempty"`
	EventTime       float64         `json:"EventTime"`
}

func toLookupEventsWire(events []Event) []lookupEventsEventWire {
	out := make([]lookupEventsEventWire, 0, len(events))
	for _, ev := range events {
		out = append(out, lookupEventsEventWire{
			EventTime:       awstime.Epoch(ev.EventTime),
			EventID:         ev.EventID,
			EventName:       ev.EventName,
			EventSource:     ev.EventSource,
			Username:        ev.Username,
			ReadOnly:        ev.ReadOnly,
			AccessKeyID:     ev.AccessKeyID,
			CloudTrailEvent: ev.CloudTrailEvent,
			Resources:       ev.Resources,
		})
	}

	return out
}
