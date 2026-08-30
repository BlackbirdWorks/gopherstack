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

type bucketCriterionWire struct {
	Prefix *string  `json:"prefix"`
	Gt     *int64   `json:"gt"`
	Gte    *int64   `json:"gte"`
	Lt     *int64   `json:"lt"`
	Lte    *int64   `json:"lte"`
	Eq     []string `json:"eq"`
	Neq    []string `json:"neq"`
}

func (h *Handler) handleDescribeBuckets(body []byte) (any, int, error) {
	var req struct {
		Criteria     map[string]bucketCriterionWire `json:"criteria"`
		SortCriteria *struct {
			AttributeName string `json:"attributeName"`
			OrderBy       string `json:"orderBy"`
		} `json:"sortCriteria"`
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	criteria := make(map[string]BucketCriterion, len(req.Criteria))
	for k, v := range req.Criteria {
		criteria[k] = BucketCriterion(v)
	}

	var sortBy *BucketSortCriteria
	if req.SortCriteria != nil {
		sortBy = &BucketSortCriteria{AttributeName: req.SortCriteria.AttributeName, OrderBy: req.SortCriteria.OrderBy}
	}

	buckets, nextToken, err := h.Backend.DescribeBuckets(criteria, sortBy, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	if nextToken != "" {
		return map[string]any{"buckets": buckets, "nextToken": nextToken}, http.StatusOK, nil
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

type searchResourcesTagCriterionPairWire struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type searchResourcesSimpleCriterionWire struct {
	Comparator string   `json:"comparator"`
	Key        string   `json:"key"`
	Values     []string `json:"values"`
}

type searchResourcesTagCriterionWire struct {
	Comparator string                                `json:"comparator"`
	TagValues  []searchResourcesTagCriterionPairWire `json:"tagValues"`
}

type searchResourcesCriterionWire struct {
	SimpleCriterion *searchResourcesSimpleCriterionWire `json:"simpleCriterion"`
	TagCriterion    *searchResourcesTagCriterionWire    `json:"tagCriterion"`
}

type searchResourcesCriteriaBlockWire struct {
	And []searchResourcesCriterionWire `json:"and"`
}

type searchResourcesBucketCriteriaWire struct {
	Includes *searchResourcesCriteriaBlockWire `json:"includes"`
	Excludes *searchResourcesCriteriaBlockWire `json:"excludes"`
}

type searchResourcesSortCriteriaWire struct {
	AttributeName string `json:"attributeName"`
	OrderBy       string `json:"orderBy"`
}

func toSearchResourcesCriterion(w searchResourcesCriterionWire) SearchResourcesCriterion {
	c := SearchResourcesCriterion{}

	if w.SimpleCriterion != nil {
		c.SimpleCriterion = &SearchResourcesSimpleCriterion{
			Comparator: w.SimpleCriterion.Comparator,
			Key:        w.SimpleCriterion.Key,
			Values:     w.SimpleCriterion.Values,
		}
	}

	if w.TagCriterion != nil {
		pairs := make([]SearchResourcesTagCriterionPair, 0, len(w.TagCriterion.TagValues))
		for _, p := range w.TagCriterion.TagValues {
			pairs = append(pairs, SearchResourcesTagCriterionPair(p))
		}

		c.TagCriterion = &SearchResourcesTagCriterion{Comparator: w.TagCriterion.Comparator, TagValues: pairs}
	}

	return c
}

func toSearchResourcesBlock(w *searchResourcesCriteriaBlockWire) *SearchResourcesCriteriaBlock {
	if w == nil {
		return nil
	}

	and := make([]SearchResourcesCriterion, 0, len(w.And))
	for _, c := range w.And {
		and = append(and, toSearchResourcesCriterion(c))
	}

	return &SearchResourcesCriteriaBlock{And: and}
}

func toSearchResourcesBucketCriteria(w *searchResourcesBucketCriteriaWire) *SearchResourcesBucketCriteria {
	if w == nil {
		return nil
	}

	return &SearchResourcesBucketCriteria{
		Includes: toSearchResourcesBlock(w.Includes),
		Excludes: toSearchResourcesBlock(w.Excludes),
	}
}

func (h *Handler) handleSearchResources(body []byte) (any, int, error) {
	var req struct {
		BucketCriteria *searchResourcesBucketCriteriaWire `json:"bucketCriteria"`
		SortCriteria   *searchResourcesSortCriteriaWire   `json:"sortCriteria"`
		NextToken      string                             `json:"nextToken"`
		MaxResults     int                                `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	var sortBy *SearchResourcesSortCriteria
	if req.SortCriteria != nil {
		sortBy = &SearchResourcesSortCriteria{
			AttributeName: req.SortCriteria.AttributeName,
			OrderBy:       req.SortCriteria.OrderBy,
		}
	}

	results, nextToken, err := h.Backend.SearchResources(
		toSearchResourcesBucketCriteria(req.BucketCriteria), sortBy, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"matchingResources": results}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}
