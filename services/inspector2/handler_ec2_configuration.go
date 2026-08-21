package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opGetEc2DeepInspectionConfiguration        = "GetEc2DeepInspectionConfiguration"
	opUpdateEc2DeepInspectionConfiguration     = "UpdateEc2DeepInspectionConfiguration"
	opUpdateOrgEc2DeepInspectionConfiguration  = "UpdateOrgEc2DeepInspectionConfiguration"
	opBatchGetMemberEc2DeepInspectionStatus    = "BatchGetMemberEc2DeepInspectionStatus"
	opBatchUpdateMemberEc2DeepInspectionStatus = "BatchUpdateMemberEc2DeepInspectionStatus"

	pathEc2DeepConfigGet     = "/ec2deepinspectionconfiguration/get"
	pathEc2DeepConfigUpdate  = "/ec2deepinspectionconfiguration/update"
	pathEc2DeepOrgUpdate     = "/ec2deepinspectionconfiguration/org/update"
	pathEc2MemberBatchGet    = "/ec2deepinspectionstatus/member/batch/get"
	pathEc2MemberBatchUpdate = "/ec2deepinspectionstatus/member/batch/update"
)

// GetEc2DeepInspectionConfigurationOutput (inspector2@v1.54.1 deserializers.go's
// awsRestjson1_deserializeOpDocumentGetEc2DeepInspectionConfigurationOutput)
// has exactly errorMessage/orgPackagePaths/packagePaths/status -- no
// "ec2ScanModeState" member at all.
func (h *Handler) handleGetEc2DeepInspectionConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetEc2DeepInspectionConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		keyErrorMessage: cfg.ErrorMessage,
		"packagePaths":  cfg.PackagePaths,
		keyStatus:       cfg.Status,
	})
}

func (h *Handler) handleUpdateEc2DeepInspectionConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		PackagePaths []string `json:"packagePaths"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if updateErr := h.Backend.UpdateEc2DeepInspectionConfiguration(req.PackagePaths); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	cfg := h.Backend.GetEc2DeepInspectionConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		keyErrorMessage: cfg.ErrorMessage,
		"packagePaths":  cfg.PackagePaths,
		keyStatus:       cfg.Status,
	})
}

func (h *Handler) handleUpdateOrgEc2DeepInspectionConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Ec2ScanModeConfig *struct {
			ScanMode string `json:"scanMode"`
		} `json:"ec2ScanModeConfig"`
		OrgPackagePaths []string `json:"orgPackagePaths"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if updateErr := h.Backend.UpdateOrgEc2DeepInspectionConfiguration(req.OrgPackagePaths); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleBatchGetMemberEc2DeepInspectionStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	statuses := h.Backend.BatchGetMemberEc2DeepInspectionStatus(req.AccountIDs)

	return c.JSON(http.StatusOK, map[string]any{
		keyAccountIDs:      statuses,
		"failedAccountIds": []any{},
	})
}

func (h *Handler) handleBatchUpdateMemberEc2DeepInspectionStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []struct {
			AccountID              string `json:"accountId"`
			ActivateDeepInspection bool   `json:"activateDeepInspection"`
		} `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	updates := make([]*MemberEc2DeepInspectionStatus, 0, len(req.AccountIDs))

	for _, a := range req.AccountIDs {
		status := statusDisabled
		if a.ActivateDeepInspection {
			status = statusEnabled
		}

		updates = append(updates, &MemberEc2DeepInspectionStatus{
			AccountID: a.AccountID,
			Status:    status,
		})
	}

	updated := h.Backend.BatchUpdateMemberEc2DeepInspectionStatus(updates)

	return c.JSON(http.StatusOK, map[string]any{
		keyAccountIDs:      updated,
		"failedAccountIds": []any{},
	})
}
