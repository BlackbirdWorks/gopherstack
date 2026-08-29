package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opListAccountPermissions = "ListAccountPermissions"

	pathAccountPermissionsList = "/accountpermissions/list"
)

// handleToggle handles POST /enable and POST /disable.
func (h *Handler) handleToggle(c *echo.Context, enable bool) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceTypes []string `json:"resourceTypes"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if enable {
		err = h.Backend.Enable(req.ResourceTypes)
	} else {
		err = h.Backend.Disable(req.ResourceTypes)
	}

	if err != nil {
		return h.mapError(c, err)
	}

	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		keyAccounts: []map[string]any{
			{
				keyAccountID:      status.AccountID,
				keyResourceStatus: buildResourceStatus(status),
				keyStatus:         status.Status,
			},
		},
		keyFailedAccounts: []any{},
	})
}

// handleBatchGetAccountStatus handles POST /status/batch/get. Unlike
// Enable/Disable (whose Account shape is a flat resourceStatus of Status
// strings), BatchGetAccountStatus returns the richer AccountState shape:
// resourceState nests a State object per resource type (status, errorCode,
// errorMessage), and the top-level state is itself a State object rather
// than a bare status string.
func (h *Handler) handleBatchGetAccountStatus(c *echo.Context) error {
	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		keyAccounts: []map[string]any{
			{
				keyAccountID:     status.AccountID,
				keyResourceState: buildResourceState(status),
				"state":          buildState(status.Status),
			},
		},
		keyFailedAccounts: []any{},
	})
}

// buildState renders an Inspector2 State object: a required status alongside
// the (unpopulated, in the absence of an error) errorCode/errorMessage
// members every State response carries.
func buildState(status string) map[string]any {
	return map[string]any{
		keyStatus:       status,
		keyErrorCode:    "",
		keyErrorMessage: nil,
	}
}

// buildResourceState constructs the resourceState map used by
// BatchGetAccountStatus, keying each resource type to its own State object.
func buildResourceState(status *AccountStatusResponse) map[string]any {
	return map[string]any{
		"ec2":    buildState(status.Ec2Status),
		"ecr":    buildState(status.EcrStatus),
		"lambda": buildState(status.LambdaStatus),
	}
}

// buildResourceStatus constructs the resourceStatus map.
func buildResourceStatus(status *AccountStatusResponse) map[string]any {
	return map[string]any{
		"ec2":    status.Ec2Status,
		"ecr":    status.EcrStatus,
		"lambda": status.LambdaStatus,
	}
}

// handleGetConfiguration handles POST /configuration/get.
func (h *Handler) handleGetConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"ec2Configuration": map[string]any{
			"scanModeState": map[string]any{
				"scanMode":       cfg.Ec2ScanMode,
				"scanModeStatus": scanModeStatusSuccess,
			},
		},
		"ecrConfiguration": map[string]any{
			"rescanDurationState": map[string]any{
				"rescanDuration": cfg.EcrRescanDuration,
				keyStatus:        statusEnabled,
				keyUpdatedAt:     nil,
			},
		},
	})
}

// handleUpdateConfiguration handles POST /configuration/update.
func (h *Handler) handleUpdateConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Ec2Configuration *struct {
			ScanMode string `json:"scanMode"`
		} `json:"ec2Configuration"`
		EcrConfiguration *struct {
			RescanDuration string `json:"rescanDuration"`
		} `json:"ecrConfiguration"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	var ec2ScanMode, ecrRescanDuration string

	if req.Ec2Configuration != nil {
		ec2ScanMode = req.Ec2Configuration.ScanMode
	}

	if req.EcrConfiguration != nil {
		ecrRescanDuration = req.EcrConfiguration.RescanDuration
	}

	if updateErr := h.Backend.UpdateConfiguration(ec2ScanMode, ecrRescanDuration); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListAccountPermissions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Service string `json:"service"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	perms, listErr := h.Backend.ListAccountPermissions(req.Service)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if perms == nil {
		perms = []*AccountPermission{}
	}

	return c.JSON(http.StatusOK, map[string]any{"permissions": perms})
}
