package guardduty

import (
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func (h *Handler) dispatchInvestigationOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateInvestigation:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateInvestigation(detectorID, body)

		return result, code, true, err

	case opGetInvestigation:
		detectorID, investigationID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetInvestigation(detectorID, investigationID)

		return result, code, true, err

	case opListInvestigations:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListInvestigations(detectorID, body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateInvestigation(detectorID string, body []byte) (any, int, error) {
	var req struct {
		TriggerPrompt string `json:"triggerPrompt"`
		ClientToken   string `json:"clientToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	inv, err := h.Backend.CreateInvestigation(detectorID, req.TriggerPrompt)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{keyInvestigationID: inv.InvestigationID}, http.StatusOK, nil
}

func (h *Handler) handleGetInvestigation(detectorID, investigationID string) (any, int, error) {
	inv, err := h.Backend.GetInvestigation(detectorID, investigationID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"investigation": investigationDetailWire(inv)}, http.StatusOK, nil
}

func (h *Handler) handleListInvestigations(detectorID string, body []byte) (any, int, error) {
	var req struct {
		SortCriteria *InvestigationSortCriteria `json:"sortCriteria"`
		NextToken    string                     `json:"nextToken"`
		MaxResults   int32                      `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	q := InvestigationsQuery{NextToken: req.NextToken, MaxResults: req.MaxResults}
	if req.SortCriteria != nil {
		q.SortAttr = req.SortCriteria.AttributeName
		q.SortOrder = req.SortCriteria.OrderBy
	}

	investigations, nextToken, err := h.Backend.ListInvestigations(detectorID, q)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	out := make([]map[string]any, 0, len(investigations))
	for _, inv := range investigations {
		out = append(out, investigationSummaryWire(inv))
	}

	resp := map[string]any{"investigations": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

// InvestigationSortCriteria mirrors types.InvestigationSortCriteria's wire
// shape (attributeName/orderBy) for request parsing.
type InvestigationSortCriteria struct {
	AttributeName string `json:"attributeName"`
	OrderBy       string `json:"orderBy"`
}

// investigationDetailWire builds the wire shape for the single-investigation
// GetInvestigationOutput.Investigation member (types.Investigation).
//
// Cloud, Confidence, EndTime, Error, Metadata, Risk, RiskLevel, and Summary
// are real *optional* members that AWS only populates once analysis actually
// runs (Cloud/Metadata) or completes/fails (the rest). This backend has no
// analysis engine, so every investigation stays RUNNING forever and all of
// these are correctly omitted below -- never fabricated with invented
// severity, threat indicators, related findings, or anomaly counts.
func investigationDetailWire(inv *Investigation) map[string]any {
	return map[string]any{
		keyInvestigationID: inv.InvestigationID,
		keyStatus:          inv.Status,
		"triggerPrompt":    inv.TriggerPrompt,
		"triggeredBy":      inv.TriggeredBy,
		"startTime":        awstime.Epoch(inv.StartTime),
	}
}

// investigationSummaryWire builds the wire shape for one entry in
// ListInvestigationsOutput.Investigations (types.InvestigationSummary).
//
// title is deliberately never populated: it is a real optional member this
// backend has no way to generate honestly (it would require the same
// analysis this backend cannot perform), so it stays absent rather than
// being derived from triggerPrompt or otherwise invented. accountId reflects
// the same triggeredBy account CreateInvestigation recorded, consistent with
// this backend's single-account model.
func investigationSummaryWire(inv *Investigation) map[string]any {
	return map[string]any{
		keyInvestigationID: inv.InvestigationID,
		keyAccountIDField:  inv.TriggeredBy,
		keyStatus:          inv.Status,
		"triggerPrompt":    inv.TriggerPrompt,
		"startTime":        awstime.Epoch(inv.StartTime),
	}
}
