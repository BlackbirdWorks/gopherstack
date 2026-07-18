package macie2

import (
	"encoding/json"
	"net/http"
)

func parseUsagePath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /usage
		if method == http.MethodGet {
			return opGetUsageTotals, ""
		}
	case depthResource: // /usage/statistics
		if parts[1] == segStatistics && method == http.MethodPost {
			return opGetUsageStatistics, ""
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchUsageOps(op, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetUsageStatistics:
		result, code, err := h.handleGetUsageStatistics(body)

		return result, code, true, err

	case opGetUsageTotals:
		timeRange := extractQueryParam(query, "timeRange")
		result, code, err := h.handleGetUsageTotals(timeRange)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetUsageStatistics(body []byte) (any, int, error) {
	var req struct {
		SortBy     map[string]any   `json:"sortBy"`
		NextToken  string           `json:"nextToken"`
		FilterBy   []map[string]any `json:"filterBy"`
		MaxResults int              `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	records, nextToken, err := h.Backend.GetUsageStatistics(req.FilterBy, req.MaxResults, req.NextToken, "")
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"records": records}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleGetUsageTotals(timeRange string) (any, int, error) {
	totals, err := h.Backend.GetUsageTotals(timeRange)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"usageTotals": totals}, http.StatusOK, nil
}
