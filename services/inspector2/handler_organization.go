package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opEnableDelegatedAdminAccount  = "EnableDelegatedAdminAccount"
	opDisableDelegatedAdminAccount = "DisableDelegatedAdminAccount"
	opGetDelegatedAdminAccount     = "GetDelegatedAdminAccount"
	opListDelegatedAdminAccounts   = "ListDelegatedAdminAccounts"

	opDescribeOrganizationConfiguration = "DescribeOrganizationConfiguration"
	opUpdateOrganizationConfiguration   = "UpdateOrganizationConfiguration"

	pathDelegatedAdminEnable  = "/delegatedadminaccounts/enable"
	pathDelegatedAdminDisable = "/delegatedadminaccounts/disable"
	pathDelegatedAdminGet     = "/delegatedadminaccounts/get"
	pathDelegatedAdminList    = "/delegatedadminaccounts/list"

	pathOrgConfigDescribe = "/organizationconfiguration/describe"
	pathOrgConfigUpdate   = "/organizationconfiguration/update"

	keyDelegatedAdminAccount = "delegatedAdminAccountId"
)

func (h *Handler) handleEnableDelegatedAdminAccount(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		DelegatedAdminAccountID string `json:"delegatedAdminAccountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if enableErr := h.Backend.EnableDelegatedAdminAccount(req.DelegatedAdminAccountID); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDelegatedAdminAccount: req.DelegatedAdminAccountID,
		keyStatus:                statusEnabled,
	})
}

func (h *Handler) handleDisableDelegatedAdminAccount(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		DelegatedAdminAccountID string `json:"delegatedAdminAccountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if disErr := h.Backend.DisableDelegatedAdminAccount(req.DelegatedAdminAccountID); disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDelegatedAdminAccount: req.DelegatedAdminAccountID,
		keyStatus:                "DISABLE_IN_PROGRESS",
	})
}

func (h *Handler) handleGetDelegatedAdminAccount(c *echo.Context) error {
	d, err := h.Backend.GetDelegatedAdminAccount()
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"delegatedAdmin": d})
}

func (h *Handler) handleListDelegatedAdminAccounts(c *echo.Context) error {
	accounts, err := h.Backend.ListDelegatedAdminAccounts()
	if err != nil {
		return h.mapError(c, err)
	}

	if accounts == nil {
		accounts = []*DelegatedAdminAccount{}
	}

	return c.JSON(http.StatusOK, map[string]any{"delegatedAdminAccounts": accounts})
}

func (h *Handler) handleDescribeOrganizationConfiguration(c *echo.Context) error {
	cfg := h.Backend.DescribeOrganizationConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"autoEnable":             cfg.AutoEnable,
		"maxAccountLimitReached": cfg.MaxAccountLimitReached,
	})
}

func (h *Handler) handleUpdateOrganizationConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AutoEnable map[string]bool `json:"autoEnable"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	autoEnable := false
	for _, v := range req.AutoEnable {
		if v {
			autoEnable = true

			break
		}
	}

	if updateErr := h.Backend.UpdateOrganizationConfiguration(
		OrgConfiguration{AutoEnable: autoEnable},
	); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	// Real UpdateOrganizationConfigurationOutput carries the resulting
	// autoEnable settings
	// (awsRestjson1_deserializeOpDocumentUpdateOrganizationConfigurationOutput
	// in the pinned inspector2 SDK's deserializers.go), not an empty
	// envelope. req.AutoEnable already carries the real per-scan-type wire
	// keys (codeRepository/ec2/ecr/lambda/lambdaCode) as submitted, so echo
	// it back rather than reconstructing from the backend's collapsed bool.
	return c.JSON(http.StatusOK, map[string]any{"autoEnable": req.AutoEnable})
}
