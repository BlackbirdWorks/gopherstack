package macie2

import (
	"encoding/json"
	"net/http"
)

func parseDatasourcesPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthResource: // /datasources/{s3|search-resources}
		switch parts[1] {
		case "s3":
			if method == http.MethodPost {
				return opDescribeBuckets, ""
			}
		case "search-resources":
			if method == http.MethodPost {
				return opSearchResources, ""
			}
		}
	case 3: //nolint:mnd // existing issue.
		if parts[1] == "s3" && parts[2] == segStatistics &&
			method == http.MethodPost {
			return opGetBucketStatistics, ""
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchBucketOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opDescribeBuckets:
		result, code, err := h.handleDescribeBuckets(body)

		return result, code, true, err

	case opGetBucketStatistics:
		result, code, err := h.handleGetBucketStatistics(body)

		return result, code, true, err

	case opSearchResources:
		result, code, err := h.handleSearchResources(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleDescribeBuckets(body []byte) (any, int, error) {
	var req struct {
		Criteria map[string]any `json:"criteria"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	buckets, err := h.Backend.DescribeBuckets(req.Criteria)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"buckets": buckets}, http.StatusOK, nil
}

func (h *Handler) handleGetBucketStatistics(body []byte) (any, int, error) {
	var req struct {
		AccountID string `json:"accountId"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	stats, err := h.Backend.GetBucketStatistics(req.AccountID)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleSearchResources(body []byte) (any, int, error) {
	var req struct {
		BucketCriteria map[string]any `json:"bucketCriteria"`
		SortCriteria   map[string]any `json:"sortCriteria"`
		NextToken      string         `json:"nextToken"`
		MaxResults     int            `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	results, nextToken, err := h.Backend.SearchResources(req.BucketCriteria, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"matchingResources": results}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}
