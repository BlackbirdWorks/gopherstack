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
		UsageStatisticType string `json:"usageStatisticsType"`
		Unit               string `json:"unit"`
		NextToken          string `json:"nextToken"`
		MaxResults         int32  `json:"maxResults"`
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

	next, err := pageUsageStatistics(stats, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if next != "" {
		stats["nextToken"] = next
	}

	return stats, http.StatusOK, nil
}

// pageUsageStatistics trims every list of the usageStatistics object to one page and
// returns the continuation token when any list was truncated.
func pageUsageStatistics(stats map[string]any, maxResults int32, nextToken string) (string, error) {
	offset, err := decodeToken(nextToken)
	if err != nil {
		return "", ErrValidation
	}

	usage, ok := stats["usageStatistics"].(map[string]any)
	if !ok {
		return "", nil
	}

	size := resolvePageSize(int(maxResults))
	next := ""

	for key, v := range usage {
		list, isList := v.([]any)
		if !isList {
			continue
		}

		page, tok := paginate(list, offset, size)
		usage[key] = page

		if tok != "" {
			next = tok
		}
	}

	return next, nil
}

func (h *Handler) handleGetRemainingFreeTrialDays(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	result, err := h.Backend.GetRemainingFreeTrialDays(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return result, http.StatusOK, nil
}
