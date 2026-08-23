package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type backupSelectionDoc struct {
	Conditions    *selectionConditionsJSON `json:"Conditions,omitempty"`
	SelectionName string                   `json:"SelectionName"`
	IamRoleArn    string                   `json:"IamRoleArn,omitempty"`
	Resources     []string                 `json:"Resources,omitempty"`
	NotResources  []string                 `json:"NotResources,omitempty"`
	ListOfTags    []tagConditionJSON       `json:"ListOfTags,omitempty"`
}

type createBackupSelectionBody struct {
	CreatorRequestID string             `json:"CreatorRequestId,omitempty"`
	BackupSelection  backupSelectionDoc `json:"BackupSelection"`
}

func (h *Handler) handleCreateBackupSelection(c *echo.Context, planID string, body []byte) error {
	if planID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupPlanId is required"),
		)
	}

	var in createBackupSelectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	sel, err := h.Backend.CreateBackupSelection(
		planID,
		in.BackupSelection.SelectionName,
		in.BackupSelection.IamRoleArn,
		in.BackupSelection.Resources,
		in.BackupSelection.NotResources,
		tagConditionsFromJSON(in.BackupSelection.ListOfTags),
		selectionConditionsFromJSON(in.BackupSelection.Conditions),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanID: sel.BackupPlanID,
		keySelectionID:  sel.SelectionID,
		keyCreationDate: epochSeconds(sel.CreationTime),
	})
}

func (h *Handler) handleGetBackupSelection(c *echo.Context, resource string) error {
	planID, selID, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	sel, err := h.Backend.GetBackupSelection(planID, selID)
	if err != nil {
		return h.handleError(c, err)
	}

	selDoc := map[string]any{
		"SelectionName": sel.SelectionName,
		keyIamRoleArn:   sel.IAMRoleArn,
	}
	if len(sel.Resources) > 0 {
		selDoc["Resources"] = sel.Resources
	}
	if len(sel.NotResources) > 0 {
		selDoc["NotResources"] = sel.NotResources
	}
	if len(sel.ListOfTags) > 0 {
		tags := make([]map[string]any, 0, len(sel.ListOfTags))
		for _, tc := range sel.ListOfTags {
			tags = append(tags, map[string]any{
				"ConditionType":  tc.ConditionType,
				"ConditionKey":   tc.ConditionKey,
				"ConditionValue": tc.ConditionValue,
			})
		}
		selDoc["ListOfTags"] = tags
	}
	if sel.Conditions != nil {
		selDoc["Conditions"] = sel.Conditions
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanID:   sel.BackupPlanID,
		keySelectionID:    sel.SelectionID,
		keyCreationDate:   epochSeconds(sel.CreationTime),
		"BackupSelection": selDoc,
	})
}

func (h *Handler) handleListBackupSelections(c *echo.Context, planID string) error {
	if planID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupPlanId is required"),
		)
	}

	sels, err := h.Backend.ListBackupSelections(planID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(sels))
	for _, sel := range sels {
		item := map[string]any{
			keyBackupPlanID: sel.BackupPlanID,
			keySelectionID:  sel.SelectionID,
			"SelectionName": sel.SelectionName,
			keyCreationDate: epochSeconds(sel.CreationTime),
		}
		setOptionalStr(item, "IamRoleArn", sel.IAMRoleArn)
		items = append(items, item)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupSelectionsList": items,
	})
}

func (h *Handler) handleDeleteBackupSelection(c *echo.Context, resource string) error {
	planID, selID, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteBackupSelection(planID, selID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Copy job handlers ---
