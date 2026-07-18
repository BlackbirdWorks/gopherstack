package guardduty

import "net/http"

func (h *Handler) handleGetCoverageStatistics(detectorID string) (any, int, error) {
	stats, err := h.Backend.GetCoverageStatistics(detectorID)
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
