package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// insightEntityJSON matches types.InsightEntity (used by ListInsights and
// DescribeInsightDetails) and types.InsightFeedbackEntity (used by
// InsightFeedback, which only ever carries Type=DomainName but shares the
// same {Type, Value} shape on the wire).
type insightEntityJSON struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// listInsightsRequest is the JSON request body for ListInsights. SortOrder/
// MaxResults/NextToken/TimeRange are accepted but unused: this backend has no
// analytics engine to generate insights in the first place (see insights.go),
// so there is nothing to sort, paginate, or time-filter -- the result is
// always an empty list once the entity itself validates.
type listInsightsRequest struct {
	Entity insightEntityJSON `json:"Entity"`
}

// describeInsightDetailsRequest is the JSON request body for
// DescribeInsightDetails.
type describeInsightDetailsRequest struct {
	Entity    insightEntityJSON `json:"Entity"`
	InsightID string            `json:"InsightId"`
}

// insightFeedbackRequest is the JSON request body for InsightFeedback.
type insightFeedbackRequest struct {
	Entity       insightEntityJSON `json:"Entity"`
	InsightID    string            `json:"InsightId"`
	Thumbs       string            `json:"Thumbs"`
	FeedbackText string            `json:"FeedbackText"`
}

// handleListInsights handles POST /2021-01-01/opensearch/insights.
func (h *Handler) handleListInsights(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req listInsightsRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	if valErr := h.Backend.ValidateInsightEntity(req.Entity.Type, req.Entity.Value); valErr != nil {
		h.writeInsightError(r, w, valErr)

		return
	}

	// No analytics engine backs this emulator (see insights.go) -- an empty
	// list is the honest result, not a fabricated one.
	h.writeJSON(r, w, map[string]any{"Insights": []any{}})
}

// handleDescribeInsightDetails handles POST /2021-01-01/opensearch/insight-details.
func (h *Handler) handleDescribeInsightDetails(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req describeInsightDetailsRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	if valErr := h.Backend.ValidateInsightEntity(req.Entity.Type, req.Entity.Value); valErr != nil {
		h.writeInsightError(r, w, valErr)

		return
	}

	// No insight can genuinely exist without an analytics engine to have
	// produced it (see ListInsights above) -- reporting details for any
	// InsightId would be fabrication, so this always 404s/409s instead.
	h.writeInsightError(r, w, ErrInsightNotFound)
}

// handleInsightFeedback handles POST /2021-01-01/opensearch/insight-feedback.
func (h *Handler) handleInsightFeedback(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req insightFeedbackRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	if valErr := h.Backend.ValidateInsightEntity(req.Entity.Type, req.Entity.Value); valErr != nil {
		h.writeInsightError(r, w, valErr)

		return
	}

	// Same reasoning as DescribeInsightDetails: no insight ever genuinely
	// exists, so feedback against any InsightId is rejected rather than
	// silently reporting a fabricated SUCCESS.
	h.writeInsightError(r, w, ErrInsightNotFound)
}

// writeInsightError maps insight errors to their documented HTTP status
// codes -- ResourceNotFoundException at 409, matching the same "application"
// API family convention as the other newer op families in this service.
func (h *Handler) writeInsightError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDomainNotFound), errors.Is(err, ErrInsightNotFound):
		h.writeError(r, w, http.StatusConflict, "ResourceNotFoundException", err.Error())
	default:
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	}
}
