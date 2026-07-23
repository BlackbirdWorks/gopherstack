package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) dispatchFindingOps(op, path string, body []byte) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opGetFindings:
		result, code, err := h.handleGetFindings(detectorID, body)

		return result, code, true, err

	case opListFindings:
		result, code, err := h.handleListFindings(detectorID, body)

		return result, code, true, err

	case opArchiveFindings:
		code, err := h.handleArchiveFindings(detectorID, body)

		return nil, code, true, err

	case opUnarchiveFindings:
		code, err := h.handleUnarchiveFindings(detectorID, body)

		return nil, code, true, err

	case opCreateSampleFindings:
		code, err := h.handleCreateSampleFindings(detectorID, body)

		return nil, code, true, err

	case opGetFindingsStatistics:
		result, code, err := h.handleGetFindingsStatistics(detectorID, body)

		return result, code, true, err

	case opUpdateFindingsFeedback:
		code, err := h.handleUpdateFindingsFeedback(detectorID, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetFindings(detectorID string, body []byte) (any, int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	findings, err := h.Backend.GetFindings(detectorID, req.FindingIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"findings": findings}, http.StatusOK, nil
}

func (h *Handler) handleListFindings(detectorID string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria *FindingCriteria `json:"findingCriteria"`
		SortCriteria    *SortCriteria    `json:"sortCriteria"`
		NextToken       string           `json:"nextToken"`
		MaxResults      int32            `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	q := FindingsQuery{NextToken: req.NextToken, MaxResults: req.MaxResults}
	if req.FindingCriteria != nil {
		q.Criteria = req.FindingCriteria.Criterion
	}

	if req.SortCriteria != nil {
		q.SortAttr = req.SortCriteria.AttributeName
		q.SortOrder = req.SortCriteria.OrderBy
	}

	ids, nextToken, err := h.Backend.ListFindings(detectorID, q)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	resp := map[string]any{"findingIds": orEmptyAny(ids)}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleArchiveFindings(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.ArchiveFindings(detectorID, req.FindingIDs); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUnarchiveFindings(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UnarchiveFindings(detectorID, req.FindingIDs); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleCreateSampleFindings(detectorID string, body []byte) (int, error) {
	var req struct {
		FindingTypes []string `json:"findingTypes"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.CreateSampleFindings(detectorID, req.FindingTypes); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingsStatistics(detectorID string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria *FindingCriteria `json:"findingCriteria"`
		GroupBy         string           `json:"groupBy"`
		OrderBy         string           `json:"orderBy"`
		MaxResults      int32            `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	q := FindingStatisticsQuery{GroupBy: req.GroupBy, OrderBy: req.OrderBy}
	if req.FindingCriteria != nil {
		q.Criteria = req.FindingCriteria.Criterion
	}

	stats, err := h.Backend.GetFindingsStatistics(detectorID, q)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleUpdateFindingsFeedback(detectorID string, body []byte) (int, error) {
	var req struct {
		Feedback   string   `json:"feedback"`
		Comments   string   `json:"comments"`
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateFindingsFeedback(detectorID, req.FindingIDs, req.Feedback); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}
