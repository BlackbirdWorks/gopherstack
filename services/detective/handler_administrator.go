package detective

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleStartMonitoringMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn  string `json:"GraphArn"`
		AccountId string `json:"AccountId"` //nolint:revive,staticcheck // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.AccountId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "AccountId is required"))
	}

	if startErr := h.Backend.StartMonitoringMember(req.GraphArn, req.AccountId); startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableOrganizationAdminAccount(c *echo.Context) error {
	if disErr := h.Backend.DisableOrganizationAdminAccount(); disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleEnableOrganizationAdminAccount(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountId string `json:"AccountId"` //nolint:revive,staticcheck // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.AccountId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "AccountId is required"))
	}

	if enableErr := h.Backend.EnableOrganizationAdminAccount(req.AccountId); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListOrganizationAdminAccounts(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	admins, nextToken, listErr := h.Backend.ListOrganizationAdminAccounts(req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	adminList := make([]map[string]any, 0, len(admins))
	for _, a := range admins {
		adminList = append(adminList, map[string]any{
			keyAccountID:     a.AccountID,
			"DelegationTime": a.DelegationTime.UTC().Format("2006-01-02T15:04:05.000Z"),
			keyGraphArn:      a.GraphARN,
		})
	}

	resp := map[string]any{
		"Administrators": adminList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
