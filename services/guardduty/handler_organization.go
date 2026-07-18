package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) dispatchOrgOps(op, path string, body []byte) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opEnableOrganizationAdminAccount:
		code, err := h.handleEnableOrganizationAdminAccount(body)

		return nil, code, true, err

	case opDisableOrganizationAdminAccount:
		code, err := h.handleDisableOrganizationAdminAccount(body)

		return nil, code, true, err

	case opListOrganizationAdminAccounts:
		result, code := h.handleListOrganizationAdminAccounts()

		return result, code, true, nil

	case opDescribeOrganizationConfiguration:
		result, code, err := h.handleDescribeOrganizationConfiguration(detectorID)

		return result, code, true, err

	case opUpdateOrganizationConfiguration:
		code, err := h.handleUpdateOrganizationConfiguration(detectorID, body)

		return nil, code, true, err

	case opGetOrganizationStatistics:
		result, code := h.handleGetOrganizationStatistics()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func parseAdminPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case 1: // /admin
		if method == http.MethodGet {
			return opListOrganizationAdminAccounts, ""
		}
	case 2: //nolint:mnd // existing issue.
		switch parts[1] {
		case "enable":
			if method == http.MethodPost {
				return opEnableOrganizationAdminAccount, ""
			}
		case "disable":
			if method == http.MethodPost {
				return opDisableOrganizationAdminAccount, ""
			}
		}
	}

	return opUnknown, ""
}

func (h *Handler) handleEnableOrganizationAdminAccount(body []byte) (int, error) {
	var req struct {
		AdminAccountID string `json:"adminAccountId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.EnableOrganizationAdminAccount(req.AdminAccountID); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisableOrganizationAdminAccount(body []byte) (int, error) {
	var req struct {
		AdminAccountID string `json:"adminAccountId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.DisableOrganizationAdminAccount(req.AdminAccountID); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListOrganizationAdminAccounts() (any, int) {
	accounts := h.Backend.ListOrganizationAdminAccounts()

	out := make([]map[string]any, 0, len(accounts))
	for _, a := range accounts {
		out = append(out, map[string]any{
			"adminAccountId": a.AdminAccountID,
			"adminStatus":    a.AdminStatus,
		})
	}

	return map[string]any{"adminAccounts": out}, http.StatusOK
}

func (h *Handler) handleDescribeOrganizationConfiguration(detectorID string) (any, int, error) {
	cfg, err := h.Backend.DescribeOrganizationConfiguration(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"autoEnable":                cfg.AutoEnable,
		"memberAccountLimitReached": cfg.MemberAccountLimitReached,
		"dataSources":               cfg.DataSources,
		"features":                  cfg.Features, //nolint:goconst // existing issue.
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateOrganizationConfiguration(detectorID string, body []byte) (int, error) {
	var req struct {
		Features   []OrgFeature `json:"features"`
		AutoEnable bool         `json:"autoEnable"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateOrganizationConfiguration(detectorID, req.AutoEnable, req.Features); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetOrganizationStatistics() (any, int) {
	stats := h.Backend.GetOrganizationStatistics()

	return stats, http.StatusOK
}
