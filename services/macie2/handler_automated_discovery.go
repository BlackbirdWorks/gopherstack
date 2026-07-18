package macie2

import (
	"encoding/json"
	"net/http"
)

func parseAutomatedDiscoveryPath(method string, parts []string) (string, string) {
	// /automated-discovery/configuration
	// /automated-discovery/accounts
	if len(parts) < 2 { //nolint:mnd // existing issue.
		return opUnknown, ""
	}

	switch parts[1] {
	case keyConfiguration:
		switch method {
		case http.MethodGet:
			return opGetAutomatedDiscoveryConfiguration, ""
		case http.MethodPut:
			return opUpdateAutomatedDiscoveryConfiguration, ""
		}
	case "accounts":
		switch method {
		case http.MethodGet:
			return opListAutomatedDiscoveryAccounts, ""
		case http.MethodPatch:
			return opBatchUpdateAutomatedDiscoveryAccounts, ""
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchAutomatedDiscoveryOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetAutomatedDiscoveryConfiguration:
		result, code, err := h.handleGetAutomatedDiscoveryConfiguration()

		return result, code, true, err

	case opUpdateAutomatedDiscoveryConfiguration:
		code, err := h.handleUpdateAutomatedDiscoveryConfiguration(body)

		return nil, code, true, err

	case opListAutomatedDiscoveryAccounts:
		result, code, err := h.handleListAutomatedDiscoveryAccounts()

		return result, code, true, err

	case opBatchUpdateAutomatedDiscoveryAccounts:
		code, err := h.handleBatchUpdateAutomatedDiscoveryAccounts(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetAutomatedDiscoveryConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetAutomatedDiscoveryConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return cfg, http.StatusOK, nil
}

func (h *Handler) handleUpdateAutomatedDiscoveryConfiguration(body []byte) (int, error) {
	var req struct {
		AutoEnableOrganizationMembers string `json:"autoEnableOrganizationMembers"`
		Status                        string `json:"status"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateAutomatedDiscoveryConfiguration(
		req.AutoEnableOrganizationMembers, req.Status,
	); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListAutomatedDiscoveryAccounts() (any, int, error) {
	accounts, err := h.Backend.ListAutomatedDiscoveryAccounts()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{keyItems: accounts}, http.StatusOK, nil
}

func (h *Handler) handleBatchUpdateAutomatedDiscoveryAccounts(body []byte) (int, error) {
	var req struct {
		Accounts []AutoDiscoveryAccountUpdate `json:"accounts"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.BatchUpdateAutomatedDiscoveryAccounts(req.Accounts); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
