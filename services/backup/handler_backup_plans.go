package backup

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

type createBackupPlanBody struct {
	BackupPlanTags map[string]string `json:"BackupPlanTags"`
	BackupPlan     backupPlanBodyDoc `json:"BackupPlan"`
}

func (h *Handler) handleCreateBackupPlan(c *echo.Context, body []byte) error {
	var in createBackupPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.BackupPlan.BackupPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp(
				"ValidationException",
				fmt.Sprintf("%s: BackupPlanName is required", errInvalidRequest),
			),
		)
	}

	p, err := h.Backend.CreateBackupPlanValidated(
		in.BackupPlan.BackupPlanName,
		rulesFromJSON(in.BackupPlan.Rules),
		advancedSettingsFromJSON(in.BackupPlan.AdvancedBackupSettings),
		in.BackupPlanTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	createResp := map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
		keyCreationDate:  epochSeconds(p.CreationTime),
	}
	if len(p.AdvancedBackupSettings) > 0 {
		createResp["AdvancedBackupSettings"] = advancedSettingsToJSON(p.AdvancedBackupSettings)
	}

	return c.JSON(http.StatusOK, createResp)
}

func (h *Handler) handleGetBackupPlan(c *echo.Context, id string) error {
	p, err := h.Backend.GetBackupPlan(id)
	if err != nil {
		return h.handleError(c, err)
	}

	planDoc := map[string]any{
		keyBackupPlanName: p.BackupPlanName,
		keyRules:          rulesToJSON(p.Rules),
	}
	if len(p.AdvancedBackupSettings) > 0 {
		planDoc["AdvancedBackupSettings"] = advancedSettingsToJSON(p.AdvancedBackupSettings)
	}
	resp := map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
		keyCreationDate:  epochSeconds(p.CreationTime),
		"BackupPlan":     planDoc,
	}
	if p.UpdateTime != nil {
		resp["LastExecutionDate"] = epochSeconds(*p.UpdateTime)
	}
	if p.Tags != nil {
		if t := p.Tags.Clone(); len(t) > 0 {
			resp["Tags"] = t
		}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupPlans(c *echo.Context) error {
	q := c.Request().URL.Query()
	f := ListPlansFilter{
		NextToken:  q.Get("nextToken"),
		MaxResults: parseInt(q.Get("maxResults")),
	}

	plans, nextToken := h.Backend.ListBackupPlansPaged(f)
	items := make([]map[string]any, 0, len(plans))

	for _, p := range plans {
		item := map[string]any{
			keyBackupPlanName: p.BackupPlanName,
			keyBackupPlanArn:  p.BackupPlanArn,
			keyBackupPlanID:   p.BackupPlanID,
			keyVersionID:      p.VersionID,
			keyCreationDate:   epochSeconds(p.CreationTime),
		}
		if p.UpdateTime != nil {
			item["LastExecutionDate"] = epochSeconds(*p.UpdateTime)
		}
		items = append(items, item)
	}

	resp := map[string]any{"BackupPlansList": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

type updateBackupPlanBody struct {
	BackupPlan backupPlanBodyDoc `json:"BackupPlan"`
}

func (h *Handler) handleUpdateBackupPlan(c *echo.Context, id string, body []byte) error {
	var in updateBackupPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	p, err := h.Backend.UpdateBackupPlanValidated(
		id,
		rulesFromJSON(in.BackupPlan.Rules),
		advancedSettingsFromJSON(in.BackupPlan.AdvancedBackupSettings),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
	}
	if p.UpdateTime != nil {
		resp["UpdateDate"] = epochSeconds(*p.UpdateTime)
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteBackupPlan(c *echo.Context, id string) error {
	p, err := h.Backend.DeleteBackupPlanChecked(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
		"DeletionDate":   epochSeconds(time.Now()),
	})
}

// --- Job handlers ---
