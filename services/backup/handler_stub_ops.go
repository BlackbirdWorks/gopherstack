package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// dispatchStubOps handles stub operations that return minimal valid responses.
func (h *Handler) dispatchStubOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	if ok, err := h.dispatchStubSettingsAndJobs(c, route, body); ok {
		return true, err
	}

	if ok, err := h.dispatchStubReportsAndHolds(c, route, body); ok {
		return true, err
	}

	return h.dispatchStubPlanTemplatesAndTiering(c, route, body)
}

// dispatchStubSettingsAndJobs handles settings, protected resources, and restore/restore-job stubs.
func (h *Handler) dispatchStubSettingsAndJobs(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	if ok, err := h.dispatchStubSettingsOps(c, route, body); ok {
		return true, err
	}

	return h.dispatchStubRestoreOps(c, route, body)
}

func (h *Handler) dispatchStubSettingsOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opDescribeGlobalSettings:
		settings, lastUpdate := h.Backend.DescribeGlobalSettings()

		return true, c.JSON(http.StatusOK, map[string]any{
			"GlobalSettings": settings,
			"LastUpdateTime": epochSeconds(lastUpdate),
		})
	case opUpdateGlobalSettings:
		var reqGSBody struct {
			GlobalSettings map[string]string `json:"GlobalSettings"`
		}
		if err := json.Unmarshal(body, &reqGSBody); err == nil &&
			reqGSBody.GlobalSettings != nil {
			h.Backend.UpdateGlobalSettings(reqGSBody.GlobalSettings)
		}

		return true, c.NoContent(http.StatusOK)
	case opDescribeRegionSettings:
		rs := h.Backend.DescribeRegionSettings()

		return true, c.JSON(http.StatusOK, map[string]any{
			"ResourceTypeManagementPreference": rs.ResourceTypeManagementPreference,
			"ResourceTypeOptInPreference":      rs.ResourceTypeOptInPreference,
		})
	case opUpdateRegionSettings:
		var reqRSBody struct {
			ResourceTypeManagementPreference map[string]bool `json:"ResourceTypeManagementPreference"`
			ResourceTypeOptInPreference      map[string]bool `json:"ResourceTypeOptInPreference"`
		}
		if err := json.Unmarshal(body, &reqRSBody); err == nil {
			h.Backend.UpdateRegionSettings(
				reqRSBody.ResourceTypeManagementPreference,
				reqRSBody.ResourceTypeOptInPreference,
			)
		}

		return true, c.NoContent(http.StatusOK)
	case opGetSupportedResourceTypes:

		return true, c.JSON(http.StatusOK, map[string]any{
			"ResourceTypes": []string{
				"EBS", "EC2", "RDS", "S3", "DynamoDB", "EFS", "FSx",
				"Aurora", "DocumentDB", "Neptune", "Redshift", "Timestream",
			},
		})
	case opDescribeProtectedResource:
		pr, err := h.Backend.DescribeProtectedResource(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusNotFound,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			keyResourceArn:   pr.ResourceArn,
			keyResourceType:  pr.ResourceType,
			"LastBackupTime": epochSeconds(pr.LastBackupTime),
		})
	case opListProtectedResources:
		prs := h.Backend.ListProtectedResources()
		items := make([]map[string]any, 0, len(prs))
		for _, pr := range prs {
			items = append(items, map[string]any{
				keyResourceArn:  pr.ResourceArn,
				keyResourceType: pr.ResourceType,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"Results": items})
	case opListProtectedResourcesByBackupVault:
		prs := h.Backend.ListProtectedResourcesByBackupVault(route.resource)
		items := make([]map[string]any, 0, len(prs))
		for _, pr := range prs {
			items = append(items, map[string]any{
				keyResourceArn:  pr.ResourceArn,
				keyResourceType: pr.ResourceType,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"Results": items})
	}

	return false, nil
}

func (h *Handler) dispatchStubRestoreOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opDescribeRestoreJob:
		job, err := h.Backend.DescribeRestoreJob(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusNotFound,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			keyRestoreJobID:    job.RestoreJobID,
			keyStatus:          job.Status,
			"RecoveryPointArn": job.RecoveryPointArn,
			"IamRoleArn":       job.IAMRoleArn,
			"PercentDone":      job.PercentDone,
		})
	case opListRestoreJobs:
		jobs := h.Backend.ListRestoreJobs()
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(
				items,
				map[string]any{keyRestoreJobID: j.RestoreJobID, keyStatus: j.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{"RestoreJobs": items})
	case opListRestoreJobsByProtectedResource:
		jobs := h.Backend.ListRestoreJobsByProtectedResource(route.resource)
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(
				items,
				map[string]any{keyRestoreJobID: j.RestoreJobID, keyStatus: j.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{"RestoreJobs": items})
	case opListRestoreJobSummaries:
		jobs := h.Backend.ListRestoreJobs()

		return true, c.JSON(http.StatusOK, map[string]any{
			"RestoreJobSummaries": []map[string]any{
				{"Count": len(jobs), "Region": h.Backend.Region()},
			},
		})
	case opGetRestoreJobMetadata:
		job, err := h.Backend.DescribeRestoreJob(route.resource)
		metadata := map[string]string{}
		if err == nil && job.Metadata != nil {
			metadata = job.Metadata
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			keyRestoreJobID: route.resource, "Metadata": metadata,
		})
	case opGetRestoreTestingInferredMetadata:

		return true, c.JSON(http.StatusOK, map[string]any{"InferredMetadata": map[string]string{}})
	case opStartRestoreJob:
		var reqBody struct {
			Metadata         map[string]string `json:"Metadata"`
			RecoveryPointArn string            `json:"RecoveryPointArn"`
			IamRoleArn       string            `json:"IamRoleArn"`
			ResourceType     string            `json:"ResourceType"`
		}
		_ = json.Unmarshal(body, &reqBody)
		if reqBody.RecoveryPointArn == "" {
			reqBody.RecoveryPointArn = route.resource
		}
		job := h.Backend.StartRestoreJob(
			reqBody.RecoveryPointArn,
			reqBody.IamRoleArn,
			reqBody.ResourceType,
			reqBody.Metadata,
		)

		return true, c.JSON(http.StatusOK, map[string]any{keyRestoreJobID: job.RestoreJobID})
	}

	return false, nil
}

// dispatchStubReportsAndHolds handles report, scan job, and legal hold stub responses.
func (h *Handler) dispatchStubReportsAndHolds(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	if ok, err := h.dispatchStubReportOps(c, route, body); ok {
		return true, err
	}

	return h.dispatchStubLegalHoldOps(c, route, body)
}

func (h *Handler) dispatchStubReportOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opPutRestoreValidationResult:
		var reqBody struct {
			RestoreJobID     string `json:"RestoreJobId"`
			ValidationStatus string `json:"ValidationStatus"`
		}
		if err := json.Unmarshal(body, &reqBody); err == nil {
			h.Backend.PutRestoreValidationResult(reqBody.RestoreJobID, reqBody.ValidationStatus)
		}

		return true, c.NoContent(http.StatusNoContent)
	case opDescribeReportJob:
		job, err := h.Backend.DescribeReportJob(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusNotFound,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			"ReportJob": map[string]any{keyReportJobID: job.ReportJobID, keyStatus: job.Status},
		})
	case opListReportJobs:
		jobs := h.Backend.ListReportJobs("")
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(
				items,
				map[string]any{keyReportJobID: j.ReportJobID, keyStatus: j.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{"ReportJobs": items})
	case opStartReportJob:
		job := h.Backend.StartReportJob(route.resource)

		return true, c.JSON(http.StatusOK, map[string]any{keyReportJobID: job.ReportJobID})
	case opDescribeScanJob:
		job, err := h.Backend.DescribeScanJob(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusNotFound,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(
			http.StatusOK,
			map[string]any{keyScanJobID: job.ScanJobID, keyStatus: job.Status},
		)
	case opListScanJobs:
		jobs := h.Backend.ListScanJobs()
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(items, map[string]any{keyScanJobID: j.ScanJobID, keyStatus: j.Status})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"ScanJobs": items})
	case opListScanJobSummaries:
		jobs := h.Backend.ListScanJobs()

		return true, c.JSON(http.StatusOK, map[string]any{
			"ScanJobSummaries": []map[string]any{{"Count": len(jobs)}},
		})
	case opStartScanJob:
		// StartScanJobInput carries BackupVaultName (not an ARN) in the JSON body.
		var reqBody struct {
			BackupVaultName string `json:"BackupVaultName"`
		}
		_ = json.Unmarshal(body, &reqBody)

		vaultArn := reqBody.BackupVaultName
		if v, err := h.Backend.DescribeBackupVault(reqBody.BackupVaultName); err == nil {
			vaultArn = v.BackupVaultArn
		}

		job := h.Backend.StartScanJob(vaultArn)

		return true, c.JSON(http.StatusOK, map[string]any{
			keyScanJobID:    job.ScanJobID,
			keyCreationDate: epochSeconds(job.CreationTime),
		})
	}

	return false, nil
}

func (h *Handler) dispatchStubLegalHoldOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	if ok, err := h.dispatchRecoveryPointIndexOps(c, route, body); ok {
		return true, err
	}

	switch route.operation {
	case opGetLegalHold:
		lh, err := h.Backend.GetLegalHold(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusNotFound,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			keyLegalHoldID: lh.LegalHoldID, keyTitle: lh.Title,
			keyStatus: lh.Status, "LegalHoldArn": lh.LegalHoldArn,
		})
	case opListLegalHolds:
		lhs := h.Backend.ListLegalHolds()
		items := make([]map[string]any, 0, len(lhs))
		for _, lh := range lhs {
			items = append(
				items,
				map[string]any{
					keyLegalHoldID: lh.LegalHoldID,
					keyTitle:       lh.Title,
					keyStatus:      lh.Status,
				},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{"LegalHolds": items})
	case opListRecoveryPointsByLegalHold:
		rps := h.Backend.ListRecoveryPointsByLegalHold(route.resource)
		items := make([]map[string]any, 0, len(rps))
		for _, rp := range rps {
			items = append(items, map[string]any{keyRecoveryPointArn: rp.RecoveryPointArn})
		}

		return true, c.JSON(http.StatusOK, map[string]any{keyRecoveryPoints: items})
	case opListRecoveryPointsByResource:
		rps := h.Backend.ListRecoveryPointsByResource(route.resource)
		items := make([]map[string]any, 0, len(rps))
		for _, rp := range rps {
			items = append(
				items,
				map[string]any{keyRecoveryPointArn: rp.RecoveryPointArn, keyStatus: rp.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{keyRecoveryPoints: items})
	case opListIndexedRecoveryPoints:
		rps := h.Backend.ListIndexedRecoveryPoints()
		items := make([]map[string]any, 0, len(rps))
		for _, rp := range rps {
			items = append(
				items,
				map[string]any{keyRecoveryPointArn: rp.RecoveryPointArn, keyStatus: rp.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{"IndexedRecoveryPoints": items})
	}

	return false, nil
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

// dispatchStubPlanTemplatesAndTiering handles plan template, job, and tiering stub responses.
func (h *Handler) dispatchStubPlanTemplatesAndTiering(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	if ok, err := h.dispatchStubPlanTemplateOps(c, route, body); ok {
		return true, err
	}

	return h.dispatchStubTieringOps(c, route, body)
}

func (h *Handler) dispatchStubPlanTemplateOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	if ok, err := h.dispatchPlanTemplateCatalogOps(c, route, body); ok {
		return true, err
	}

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
	case opListBackupJobSummaries:
		summaries := h.Backend.ListBackupJobSummaries()

		return true, c.JSON(http.StatusOK, map[string]any{"BackupJobSummaries": summaries})
	case opListCopyJobSummaries:
		summaries := h.Backend.ListCopyJobSummaries()

		return true, c.JSON(http.StatusOK, map[string]any{"CopyJobSummaries": summaries})
	case opStartCopyJob:
		var copyJobReq struct {
			RecoveryPointArn          string `json:"RecoveryPointArn"`
			SourceBackupVaultName     string `json:"SourceBackupVaultName"`
			DestinationBackupVaultArn string `json:"DestinationBackupVaultArn"`
			IamRoleArn                string `json:"IamRoleArn"`
		}
		_ = json.Unmarshal(body, &copyJobReq)
		job := h.Backend.StartCopyJob(
			copyJobReq.RecoveryPointArn,
			copyJobReq.SourceBackupVaultName,
			copyJobReq.DestinationBackupVaultArn,
			copyJobReq.IamRoleArn,
		)

		return true, c.JSON(http.StatusOK, map[string]any{
			keyCopyJobID:    job.CopyJobID,
			keyCreationDate: epochSeconds(job.CreationDate),
		})
	}

	return false, nil
}

// dispatchPlanTemplateCatalogOps handles the backup-plan-template family of
// operations (export/import/list/resolve), split out of
// dispatchStubPlanTemplateOps to keep cyclomatic complexity in check.
func (h *Handler) dispatchPlanTemplateCatalogOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
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
			return true, c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}

		var doc backupPlanBodyDoc
		if err := json.Unmarshal([]byte(reqBody.BackupPlanTemplateJSON), &doc); err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid BackupPlanTemplateJson"),
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
				http.StatusNotFound,
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

func (h *Handler) dispatchStubTieringOps(
	c *echo.Context,
	route backupRoute,
	_ []byte,
) (bool, error) {
	switch route.operation {
	case opStopBackupJob:
		if err := h.Backend.StopBackupJob(route.resource); err != nil {
			return true, c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
		}

		return true, c.NoContent(http.StatusNoContent)
	case opListRestoreAccessBackupVaults:
		vaults := h.Backend.ListRestoreAccessBackupVaults()
		items := make([]map[string]any, 0, len(vaults))
		for _, v := range vaults {
			items = append(items, map[string]any{
				"RestoreAccessBackupVaultName": v.RestoreAccessBackupVaultName,
				"RestoreAccessBackupVaultArn":  v.RestoreAccessBackupVaultArn,
				keyVaultState:                  v.VaultState,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"RestoreAccessBackupVaults": items})
	case opRevokeRestoreAccessBackupVault:
		_ = h.Backend.RevokeRestoreAccessBackupVault(route.resource)

		return true, c.NoContent(http.StatusNoContent)
	case opDisassociateBackupVaultMpaApprovalTeam:
		_ = h.Backend.DisassociateBackupVaultMpaApprovalTeam(route.resource)

		return true, c.NoContent(http.StatusNoContent)
	case opCreateTieringConfiguration:
		err := h.Backend.CreateTieringConfiguration(route.resource)
		if err != nil {
			account := awsmeta.Account(c.Request().Context())
			vaultArn := "arn:aws:backup:" + h.Backend.Region() + ":" + account + ":backup-vault:" + route.resource

			return true, c.JSON(http.StatusOK, map[string]any{keyBackupVaultArn: vaultArn})
		}
		tc, _ := h.Backend.GetTieringConfiguration(route.resource)

		return true, c.JSON(http.StatusOK, map[string]any{keyBackupVaultArn: tc.BackupVaultArn})
	case opDeleteTieringConfiguration:
		_ = h.Backend.DeleteTieringConfiguration(route.resource)

		return true, c.NoContent(http.StatusNoContent)
	case opGetTieringConfiguration:
		tc, err := h.Backend.GetTieringConfiguration(route.resource)
		if err != nil {
			return true, c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			"TieringConfiguration": map[string]any{
				keyBackupVaultName: tc.BackupVaultName,
				keyBackupVaultArn:  tc.BackupVaultArn,
			},
		})
	case opListTieringConfigurations:
		tcs := h.Backend.ListTieringConfigurations()
		items := make([]map[string]any, 0, len(tcs))
		for _, tc := range tcs {
			items = append(
				items,
				map[string]any{
					keyBackupVaultName: tc.BackupVaultName,
					keyBackupVaultArn:  tc.BackupVaultArn,
				},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{keyTieringConfigurations: items})
	case opUpdateTieringConfiguration:
		_ = h.Backend.UpdateTieringConfiguration(route.resource)

		return true, c.NoContent(http.StatusOK)
	}

	return false, nil
}
