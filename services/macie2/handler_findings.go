package macie2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseFindingsPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /findings
		if method == http.MethodPost {
			return opListFindings, ""
		}
	case depthResource: // /findings/{action}
		if method == http.MethodPost {
			switch parts[1] {
			case "describe":
				return opGetFindings, ""
			case "sample":
				return opCreateSampleFindings, ""
			case segStatistics:
				return opGetFindingStatistics, ""
			}
		}
	case 3: //nolint:mnd // existing issue.
		if parts[2] == "reveal" && method == http.MethodGet {
			return opGetSensitiveDataOccurrences, parts[1]
		}
	case 4: //nolint:mnd // existing issue.
		if parts[2] == "reveal" && parts[3] == "availability" && method == http.MethodGet {
			return opGetSensitiveDataOccurrencesAvailability, parts[1]
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchFindingOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetFindings:
		result, code, err := h.handleGetFindings(body)

		return result, code, true, err

	case opListFindings:
		result, code, err := h.handleListFindings(body)

		return result, code, true, err

	case opCreateSampleFindings:
		code, err := h.handleCreateSampleFindings(body)

		return nil, code, true, err

	case opGetFindingStatistics:
		result, code, err := h.handleGetFindingStatistics(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetFindings(body []byte) (any, int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	findings, err := h.Backend.GetFindings(req.FindingIDs)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"findings": findings}, http.StatusOK, nil
}

func (h *Handler) handleListFindings(body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		SortCriteria    *struct {
			AttributeName string `json:"attributeName"`
			OrderBy       string `json:"orderBy"`
		} `json:"sortCriteria"`
		MaxResults *int32 `json:"maxResults"`
		NextToken  string `json:"nextToken"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	limit := 0
	if req.MaxResults != nil {
		limit = int(*req.MaxResults)
	}

	var sortBy *FindingSortCriteria
	if req.SortCriteria != nil {
		sortBy = &FindingSortCriteria{
			AttributeName: req.SortCriteria.AttributeName, OrderBy: req.SortCriteria.OrderBy,
		}
	}

	ids, next, err := h.Backend.ListFindings(req.FindingCriteria, sortBy, limit, req.NextToken)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"findingIds": ids}
	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleCreateSampleFindings(body []byte) (int, error) {
	var req struct {
		FindingTypes []string `json:"findingTypes"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.CreateSampleFindings(req.FindingTypes); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingStatistics(body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		GroupBy         string         `json:"groupBy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	groups, err := h.Backend.GetFindingStatistics(req.GroupBy, req.FindingCriteria)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"countsByGroup": groups}, http.StatusOK, nil
}

func parseFindingsPubCfgPath(method string, _ []string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetFindingsPublicationConfiguration, ""
	case http.MethodPut:
		return opPutFindingsPublicationConfiguration, ""
	}

	return opUnknown, ""
}

func (h *Handler) dispatchFindingsPublicationConfigOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetFindingsPublicationConfiguration:
		result, code, err := h.handleGetFindingsPublicationConfiguration()

		return result, code, true, err

	case opPutFindingsPublicationConfiguration:
		code, err := h.handlePutFindingsPublicationConfiguration(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetFindingsPublicationConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetFindingsPublicationConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return cfg, http.StatusOK, nil
}

func (h *Handler) handlePutFindingsPublicationConfiguration(body []byte) (int, error) {
	var cfg FindingsPublicationConfig

	if err := json.Unmarshal(body, &cfg); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	// ClientToken is an idempotency token the SDK client auto-fills; accepted
	// on the wire but GetFindingsPublicationConfigurationOutput has no such
	// member, so it must never be persisted/echoed back.
	cfg.ClientToken = ""

	if err := h.Backend.PutFindingsPublicationConfiguration(&cfg); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
