package macie2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseAdminPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /admin
		switch method {
		case http.MethodGet:
			return opListOrganizationAdminAccounts, ""
		case http.MethodPost:
			return opEnableOrganizationAdminAccount, ""
		case http.MethodDelete:
			return opDisableOrganizationAdminAccount, ""
		}
	case depthResource: // /admin/configuration
		if parts[1] == keyConfiguration {
			switch method {
			case http.MethodGet:
				return opDescribeOrganizationConfiguration, ""
			case http.MethodPatch:
				return opUpdateOrganizationConfiguration, ""
			}
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchOrganizationOps(op, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opEnableOrganizationAdminAccount:
		code, err := h.handleEnableOrganizationAdminAccount(body)

		return nil, code, true, err

	case opDisableOrganizationAdminAccount:
		accountID := extractQueryParam(query, "adminAccountId")
		code, err := h.handleDisableOrganizationAdminAccount(accountID)

		return nil, code, true, err

	case opListOrganizationAdminAccounts:
		result, code, err := h.handleListOrganizationAdminAccounts()

		return result, code, true, err

	case opDescribeOrganizationConfiguration:
		result, code, err := h.handleDescribeOrganizationConfiguration()

		return result, code, true, err

	case opUpdateOrganizationConfiguration:
		code, err := h.handleUpdateOrganizationConfiguration(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleEnableOrganizationAdminAccount(body []byte) (int, error) {
	var req struct {
		AdminAccountID string `json:"adminAccountId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.EnableOrganizationAdminAccount(req.AdminAccountID); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisableOrganizationAdminAccount(accountID string) (int, error) {
	if err := h.Backend.DisableOrganizationAdminAccount(accountID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListOrganizationAdminAccounts() (any, int, error) {
	accounts, err := h.Backend.ListOrganizationAdminAccounts()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"adminAccounts": accounts}, http.StatusOK, nil
}

func (h *Handler) handleDescribeOrganizationConfiguration() (any, int, error) {
	cfg, err := h.Backend.DescribeOrganizationConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return cfg, http.StatusOK, nil
}

func (h *Handler) handleUpdateOrganizationConfiguration(body []byte) (int, error) {
	var req struct {
		AutoEnable bool `json:"autoEnable"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateOrganizationConfiguration(req.AutoEnable); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
