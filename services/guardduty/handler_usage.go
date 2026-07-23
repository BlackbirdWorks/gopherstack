package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) handleGetUsageStatistics(detectorID string, body []byte) (any, int, error) {
	var req struct {
		UsageCriteria *struct {
			AccountIDs []string `json:"accountIds"`
			Features   []string `json:"features"`
		} `json:"usageCriteria"`
		UsageStatisticType string `json:"usageStatisticType"`
		Unit               string `json:"unit"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	q := UsageQuery{StatisticType: req.UsageStatisticType, Unit: req.Unit}
	if req.UsageCriteria != nil {
		q.AccountIDs = req.UsageCriteria.AccountIDs
		q.Features = req.UsageCriteria.Features
	}

	stats, err := h.Backend.GetUsageStatistics(detectorID, q)
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
