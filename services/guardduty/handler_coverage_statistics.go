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

func (h *Handler) handleListCoverage(detectorID string) (any, int, error) {
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
