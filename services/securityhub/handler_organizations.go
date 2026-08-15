package securityhub

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func classifyOrganizationPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/organization/configuration":
		return opDescribeOrganizationConfiguration, ""
	case method == http.MethodPost && path == "/organization/configuration":
		return opUpdateOrganizationConfiguration, ""
	case method == http.MethodPost && path == "/organization/admin/enable":
		return opEnableOrganizationAdminAccount, ""
	case method == http.MethodPost && path == "/organization/admin/disable":
		return opDisableOrganizationAdminAccount, ""
	case method == http.MethodGet && path == "/organization/admin":
		return opListOrganizationAdminAccounts, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleDescribeOrganizationConfiguration(c *echo.Context) error {
	cfg := h.Backend.DescribeOrganizationConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"AutoEnable":                cfg.AutoEnable,
		"MemberAccountLimitReached": cfg.MemberAccountLimitReached,
		"AutoEnableStandards":       cfg.AutoEnableStandards,
		"OrganizationConfiguration": map[string]any{
			"ConfigurationType": cfg.OrganizationConfigurationType,
		},
	})
}

func (h *Handler) handleUpdateOrganizationConfiguration(c *echo.Context, body map[string]any) error {
	autoEnable, _ := body["AutoEnable"].(bool)
	autoEnableStandards, _ := body["AutoEnableStandards"].(string)

	orgConfigType := ""

	if oc, ok := body["OrganizationConfiguration"].(map[string]any); ok {
		orgConfigType, _ = oc["ConfigurationType"].(string)
	}

	if err := h.Backend.UpdateOrganizationConfiguration(autoEnable, autoEnableStandards, orgConfigType); err != nil {
		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleEnableOrganizationAdminAccount(c *echo.Context, body map[string]any) error {
	accountID, _ := body["AdminAccountId"].(string)

	if accountID == "" {
		return typedErrorResponse(c, http.StatusBadRequest, "InvalidInputException", "AdminAccountId is required")
	}

	if err := h.Backend.EnableOrganizationAdminAccount(accountID); err != nil {
		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableOrganizationAdminAccount(c *echo.Context, body map[string]any) error {
	accountID, _ := body["AdminAccountId"].(string)

	if accountID == "" {
		return typedErrorResponse(c, http.StatusBadRequest, "InvalidInputException", "AdminAccountId is required")
	}

	if err := h.Backend.DisableOrganizationAdminAccount(accountID); err != nil {
		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// defaultSecurityHubFeature is the real ListOrganizationAdminAccountsInput.Feature
// default ("Defaults to Security Hub CSPM if not specified" --
// securityhub@v1.75.4 api_op_ListOrganizationAdminAccounts.go).
const defaultSecurityHubFeature = "SecurityHub"

func (h *Handler) handleListOrganizationAdminAccounts(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	feature := c.QueryParam("Feature")
	if feature == "" {
		feature = defaultSecurityHubFeature
	}

	accounts, next := h.Backend.ListOrganizationAdminAccounts(nextToken, maxResults)

	var out []map[string]any //nolint:prealloc // existing issue.

	for _, a := range accounts {
		out = append(out, map[string]any{
			keyAccountID: a.AccountId,
			"Status":     a.Status,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	// Real ListOrganizationAdminAccountsOutput always echoes Feature (the
	// request's filter, or its default) -- confirmed
	// api_op_ListOrganizationAdminAccounts.go. This backend doesn't track
	// admin accounts per-feature, so the echo isn't filtered by it, only
	// reflected back.
	resp := map[string]any{"AdminAccounts": out, "Feature": feature}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// organizationsOpHandlers returns the Organization operation dispatch table
// for handleREST.
func (h *Handler) organizationsOpHandlers(c *echo.Context, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opDescribeOrganizationConfiguration: func() error { return h.handleDescribeOrganizationConfiguration(c) },
		opUpdateOrganizationConfiguration:   func() error { return h.handleUpdateOrganizationConfiguration(c, body) },
		opEnableOrganizationAdminAccount:    func() error { return h.handleEnableOrganizationAdminAccount(c, body) },
		opDisableOrganizationAdminAccount:   func() error { return h.handleDisableOrganizationAdminAccount(c, body) },
		opListOrganizationAdminAccounts:     func() error { return h.handleListOrganizationAdminAccounts(c) },
	}
}
