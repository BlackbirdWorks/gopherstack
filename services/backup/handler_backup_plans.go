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
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.BackupPlan.BackupPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp(
				"MissingParameterValueException",
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
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
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

// builtinBackupPlanTemplate is one of the backup plan templates AWS Backup
// ships out of the box (surfaced by ListBackupPlanTemplates / GetBackupPlanFromTemplate).
type builtinBackupPlanTemplate struct {
	ID    string
	Name  string
	Rules []Rule
}

const (
	// builtinTemplateDefaultVault is the target vault name used by the
	// built-in backup plan templates, matching the AWS console default.
	builtinTemplateDefaultVault = "Default"

	builtinTemplateRetentionDays35  = 35
	builtinTemplateRetentionDays90  = 90
	builtinTemplateRetentionDays365 = 365
	builtinTemplateColdStorageDays  = 30
)

// builtinBackupPlanTemplates mirrors the built-in backup plan templates AWS
// Backup provides (the same ones shown under "Backup plan templates" in the console).
func builtinBackupPlanTemplates() []builtinBackupPlanTemplate {
	return []builtinBackupPlanTemplate{
		{
			ID:   "1n5nA02m8Z",
			Name: "Daily-35day-Retention",
			Rules: []Rule{
				{
					RuleName:           "DailyRule",
					TargetVaultName:    builtinTemplateDefaultVault,
					ScheduleExpression: "cron(0 5 ? * * *)",
					Lifecycle:          &Lifecycle{DeleteAfterDays: builtinTemplateRetentionDays35},
				},
			},
		},
		{
			ID:   "2m6oB13n9A",
			Name: "Daily-Weekly-Monthly-1yr-Retention",
			Rules: []Rule{
				{
					RuleName:           "DailyRule",
					TargetVaultName:    builtinTemplateDefaultVault,
					ScheduleExpression: "cron(0 5 ? * * *)",
					Lifecycle:          &Lifecycle{DeleteAfterDays: builtinTemplateRetentionDays35},
				},
				{
					RuleName:           "WeeklyRule",
					TargetVaultName:    builtinTemplateDefaultVault,
					ScheduleExpression: "cron(0 5 ? * 1 *)",
					Lifecycle: &Lifecycle{
						MoveToColdStorageAfterDays: builtinTemplateColdStorageDays,
						DeleteAfterDays:            builtinTemplateRetentionDays90,
					},
				},
				{
					RuleName:           "MonthlyRule",
					TargetVaultName:    builtinTemplateDefaultVault,
					ScheduleExpression: "cron(0 5 1 * ? *)",
					Lifecycle: &Lifecycle{
						MoveToColdStorageAfterDays: builtinTemplateColdStorageDays,
						DeleteAfterDays:            builtinTemplateRetentionDays365,
					},
				},
			},
		},
	}
}

// lookupBuiltinBackupPlanTemplate finds a built-in backup plan template by its ID.
func lookupBuiltinBackupPlanTemplate(id string) (builtinBackupPlanTemplate, bool) {
	for _, t := range builtinBackupPlanTemplates() {
		if t.ID == id {
			return t, true
		}
	}

	return builtinBackupPlanTemplate{}, false
}

// dispatchPlanTemplateCatalogOps handles the backup-plan-template family of
// operations (export/import/list/resolve) plus plan version listing.
func (h *Handler) dispatchPlanTemplateCatalogOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opListBackupPlanVersions:
		versions, err := h.Backend.ListBackupPlanVersions(route.resource)
		if err != nil {
			return true, c.JSON(http.StatusOK, map[string]any{"BackupPlanVersionsList": []any{}})
		}
		items := make([]map[string]any, 0, len(versions))
		for _, v := range versions {
			items = append(items, map[string]any{
				"BackupPlanId":    v.BackupPlanID,
				keyBackupPlanName: v.BackupPlanName,
				keyVersionID:      v.VersionID,
				keyCreationDate:   epochSeconds(v.CreationTime),
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"BackupPlanVersionsList": items})
	case opExportBackupPlanTemplate:
		tmpl, err := h.Backend.ExportBackupPlanTemplate(route.resource)
		if err != nil {
			tmpl = "{}"
		}

		return true, c.JSON(http.StatusOK, map[string]any{"BackupPlanTemplateJson": tmpl})
	case opGetBackupPlanFromJSON:
		var reqBody struct {
			BackupPlanTemplateJSON string `json:"BackupPlanTemplateJson"`
		}
		if err := json.Unmarshal(body, &reqBody); err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}

		var doc backupPlanBodyDoc
		if err := json.Unmarshal([]byte(reqBody.BackupPlanTemplateJSON), &doc); err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid BackupPlanTemplateJson"),
			)
		}

		planDoc := map[string]any{
			keyBackupPlanName: doc.BackupPlanName,
			keyRules:          doc.Rules,
		}
		if len(doc.AdvancedBackupSettings) > 0 {
			planDoc["AdvancedBackupSettings"] = doc.AdvancedBackupSettings
		}

		return true, c.JSON(http.StatusOK, map[string]any{"BackupPlan": planDoc})
	case opGetBackupPlanFromTemplate:
		tmpl, ok := lookupBuiltinBackupPlanTemplate(route.resource)
		if !ok {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("ResourceNotFoundException", "Backup plan template with ID "+route.resource+" not found"),
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			"BackupPlanDocument": map[string]any{
				keyBackupPlanName: tmpl.Name,
				keyRules:          rulesToJSON(tmpl.Rules),
			},
		})
	case opListBackupPlanTemplates:
		templates := builtinBackupPlanTemplates()
		items := make([]map[string]any, 0, len(templates))
		for _, t := range templates {
			items = append(items, map[string]any{
				"BackupPlanTemplateId":   t.ID,
				"BackupPlanTemplateName": t.Name,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"BackupPlanTemplatesList": items})
	}

	return false, nil
}

// --- Job handlers ---
