package guardduty

import "net/http"

func (h *Handler) handleGetUsageStatistics(detectorID string) (any, int, error) {
	stats, err := h.Backend.GetUsageStatistics(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleGetRemainingFreeTrialDays(
	detectorID string,
	body []byte, //nolint:revive,unparam // existing issue.
) (any, int, error) {
	result, err := h.Backend.GetRemainingFreeTrialDays(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return result, http.StatusOK, nil
}
