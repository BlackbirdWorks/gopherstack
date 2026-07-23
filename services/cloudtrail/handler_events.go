package cloudtrail

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
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

	resp := map[string]any{"Events": out.Events}
	if out.NextToken != "" {
		resp["NextToken"] = out.NextToken
	}

	return c.JSON(http.StatusOK, resp)
}
