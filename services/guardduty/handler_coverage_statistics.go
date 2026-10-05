package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) handleGetCoverageStatistics(detectorID string, body []byte) (any, int, error) {
	var req struct {
		StatisticsType []string `json:"statisticsType"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	if len(req.StatisticsType) == 0 {
		return nil, http.StatusBadRequest, ErrValidation
	}

	stats, err := h.Backend.GetCoverageStatistics(detectorID, req.StatisticsType)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleListCoverage(detectorID string, body []byte) (any, int, error) {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int32  `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	if _, err := decodeToken(req.NextToken); err != nil || req.MaxResults < 0 {
		return nil, http.StatusBadRequest, ErrValidation
	}

	resources, err := h.Backend.ListCoverage(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"resources": orEmptyAny(coverageToAny(resources))}, http.StatusOK, nil
}

func coverageToAny(resources []map[string]any) []map[string]any {
	if resources == nil {
		return []map[string]any{}
	}

	return resources
}
