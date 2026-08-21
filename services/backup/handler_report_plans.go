package backup

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

type reportDeliveryChannelJSON struct {
	S3BucketName string   `json:"S3BucketName"`
	S3KeyPrefix  string   `json:"S3KeyPrefix,omitempty"`
	Formats      []string `json:"Formats,omitempty"`
}

type reportSettingJSON struct {
	ReportTemplate     string   `json:"ReportTemplate"`
	FrameworkArns      []string `json:"FrameworkArns,omitempty"`
	Accounts           []string `json:"Accounts,omitempty"`
	OrganizationUnits  []string `json:"OrganizationUnits,omitempty"`
	Regions            []string `json:"Regions,omitempty"`
	NumberOfFrameworks int32    `json:"NumberOfFrameworks,omitempty"`
}

func reportDeliveryChannelFromJSON(in *reportDeliveryChannelJSON) *ReportDeliveryChannel {
	if in == nil {
		return nil
	}

	return &ReportDeliveryChannel{
		S3BucketName: in.S3BucketName,
		S3KeyPrefix:  in.S3KeyPrefix,
		Formats:      in.Formats,
	}
}

func reportDeliveryChannelToJSON(in *ReportDeliveryChannel) map[string]any {
	out := map[string]any{"S3BucketName": in.S3BucketName}
	setOptionalStr(out, "S3KeyPrefix", in.S3KeyPrefix)
	if len(in.Formats) > 0 {
		out["Formats"] = in.Formats
	}

	return out
}

func reportSettingFromJSON(in *reportSettingJSON) *ReportSetting {
	if in == nil {
		return nil
	}

	return &ReportSetting{
		ReportTemplate:     in.ReportTemplate,
		FrameworkArns:      in.FrameworkArns,
		Accounts:           in.Accounts,
		OrganizationUnits:  in.OrganizationUnits,
		Regions:            in.Regions,
		NumberOfFrameworks: in.NumberOfFrameworks,
	}
}

func reportSettingToJSON(in *ReportSetting) map[string]any {
	out := map[string]any{"ReportTemplate": in.ReportTemplate}
	if len(in.FrameworkArns) > 0 {
		out["FrameworkArns"] = in.FrameworkArns
	}
	if len(in.Accounts) > 0 {
		out["Accounts"] = in.Accounts
	}
	if len(in.OrganizationUnits) > 0 {
		out["OrganizationUnits"] = in.OrganizationUnits
	}
	if len(in.Regions) > 0 {
		out["Regions"] = in.Regions
	}
	if in.NumberOfFrameworks > 0 {
		out["NumberOfFrameworks"] = in.NumberOfFrameworks
	}

	return out
}

type createReportPlanBody struct {
	ReportPlanName        string                     `json:"ReportPlanName"`
	ReportPlanDescription string                     `json:"ReportPlanDescription,omitempty"`
	ReportDeliveryChannel *reportDeliveryChannelJSON `json:"ReportDeliveryChannel,omitempty"`
	ReportSetting         *reportSettingJSON         `json:"ReportSetting,omitempty"`
	IdempotencyToken      string                     `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateReportPlan(c *echo.Context, body []byte) error {
	var in createReportPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.ReportPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "ReportPlanName is required"),
		)
	}

	rp, err := h.Backend.CreateReportPlan(
		in.ReportPlanName,
		in.ReportPlanDescription,
		reportDeliveryChannelFromJSON(in.ReportDeliveryChannel),
		reportSettingFromJSON(in.ReportSetting),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportPlanArn:  rp.ReportPlanArn,
		keyReportPlanName: rp.ReportPlanName,
		keyCreationTime:   epochSeconds(rp.CreationTime),
	})
}

func (h *Handler) handleListReportPlans(c *echo.Context) error {
	plans := h.Backend.ListReportPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, rp := range plans {
		items = append(items, map[string]any{
			keyReportPlanArn:        rp.ReportPlanArn,
			keyReportPlanName:       rp.ReportPlanName,
			"ReportPlanDescription": rp.ReportPlanDescription,
			keyCreationTime:         epochSeconds(rp.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReportPlans": items,
	})
}

func (h *Handler) handleDescribeReportPlan(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "ReportPlanName is required"),
		)
	}

	rp, err := h.Backend.DescribeReportPlan(name)
	if err != nil {
		return h.handleError(c, err)
	}

	rpDoc := map[string]any{
		keyReportPlanArn:        rp.ReportPlanArn,
		keyReportPlanName:       rp.ReportPlanName,
		"ReportPlanDescription": rp.ReportPlanDescription,
		keyCreationTime:         epochSeconds(rp.CreationTime),
	}
	if rp.ReportDeliveryChannel != nil {
		rpDoc["ReportDeliveryChannel"] = reportDeliveryChannelToJSON(rp.ReportDeliveryChannel)
	}
	if rp.ReportSetting != nil {
		rpDoc["ReportSetting"] = reportSettingToJSON(rp.ReportSetting)
	}

	return c.JSON(http.StatusOK, map[string]any{"ReportPlan": rpDoc})
}

type updateReportPlanBody struct {
	ReportDeliveryChannel *reportDeliveryChannelJSON `json:"ReportDeliveryChannel,omitempty"`
	ReportSetting         *reportSettingJSON         `json:"ReportSetting,omitempty"`
	ReportPlanDescription string                     `json:"ReportPlanDescription,omitempty"`
	IdempotencyToken      string                     `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleUpdateReportPlan(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "ReportPlanName is required"),
		)
	}

	var in updateReportPlanBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	rp, err := h.Backend.UpdateReportPlan(
		name,
		in.ReportPlanDescription,
		reportDeliveryChannelFromJSON(in.ReportDeliveryChannel),
		reportSettingFromJSON(in.ReportSetting),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportPlanArn:  rp.ReportPlanArn,
		keyReportPlanName: rp.ReportPlanName,
	})
}

func (h *Handler) handleDeleteReportPlan(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "ReportPlanName is required"),
		)
	}

	if err := h.Backend.DeleteReportPlan(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// dispatchReportJobOps handles report-job and scan-job operations.
func (h *Handler) dispatchReportJobOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opDescribeReportJob:
		job, err := h.Backend.DescribeReportJob(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
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
				http.StatusBadRequest,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(http.StatusOK, scanJobToJSON(job))
	case opListScanJobs:
		jobs := h.Backend.ListScanJobs()
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(items, scanJobToJSON(j))
		}

		return true, c.JSON(http.StatusOK, map[string]any{"ScanJobs": items})
	case opListScanJobSummaries:
		jobs := h.Backend.ListScanJobs()

		return true, c.JSON(http.StatusOK, map[string]any{
			"ScanJobSummaries": []map[string]any{{"Count": len(jobs)}},
		})
	case opStartScanJob:
		return true, h.handleStartScanJob(c, body)
	case opGetPITRMalwareScanResults:

		return true, h.handleGetPITRMalwareScanResults(c)
	}

	return false, nil
}

// scanJobToJSON renders the required members of DescribeScanJobOutput /
// types.ScanJob (api_op_DescribeScanJob.go:39-146, types.go:2779-2871,
// backup@v1.59.4) that this backend tracks. Before gopherstack-r80d batch
// 11, both DescribeScanJob and ListScanJobs emitted only ScanJobId/Status,
// silently dropping the other 12+ required members below even though the
// backend already had most of them in ScanJob.
//
// CreatedBy (types.ScanJobCreator: BackupPlanArn/Id/Version + RuleId) is
// required but deliberately NOT emitted: this backend has no association
// between a scan job (or its recovery point) and an originating backup
// plan/rule to source it from, and fabricating one would violate the
// no-fabrication rule -- see PARITY.md's disclosed gap for this op.
func scanJobToJSON(job *ScanJob) map[string]any {
	resp := map[string]any{
		keyAccountID:        job.AccountID,
		keyBackupVaultArn:   job.BackupVaultArn,
		keyBackupVaultName:  job.BackupVaultName,
		keyCreationDate:     epochSeconds(job.CreationTime),
		keyIamRoleArn:       job.IamRoleArn,
		"MalwareScanner":    job.MalwareScanner,
		keyRecoveryPointArn: job.RecoveryPointArn,
		keyResourceArn:      job.ResourceArn,
		"ResourceName":      job.ResourceName,
		keyResourceType:     job.ResourceType,
		keyScanJobID:        job.ScanJobID,
		"ScanMode":          job.ScanMode,
		"ScannerRoleArn":    job.ScannerRoleArn,
		keyState:            job.Status,
	}
	if job.CompletionTime != nil {
		resp["CompletionDate"] = epochSeconds(*job.CompletionTime)
	}
	if job.ContinuousScanEndTime != nil {
		resp["ContinuousScanEndTime"] = epochSeconds(*job.ContinuousScanEndTime)
	}
	setOptionalStr(resp, "ScanBaseRecoveryPointArn", job.ScanBaseRecoveryPointArn)

	return resp
}

// handleStartScanJob serves StartScanJob: StartScanJobInput carries
// BackupVaultName (not an ARN) plus five other required members in the JSON
// body (api_op_StartScanJob.go:29-75, backup@v1.59.4).
func (h *Handler) handleStartScanJob(c *echo.Context, body []byte) error {
	var reqBody struct {
		ContinuousScanEndTime    *float64 `json:"ContinuousScanEndTime"`
		BackupVaultName          string   `json:"BackupVaultName"`
		IamRoleArn               string   `json:"IamRoleArn"`
		MalwareScanner           string   `json:"MalwareScanner"`
		RecoveryPointArn         string   `json:"RecoveryPointArn"`
		ScanMode                 string   `json:"ScanMode"`
		ScannerRoleArn           string   `json:"ScannerRoleArn"`
		IdempotencyToken         string   `json:"IdempotencyToken"`
		ScanBaseRecoveryPointArn string   `json:"ScanBaseRecoveryPointArn"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid request body"),
		)
	}

	if msg := validateStartScanJobParams(reqBody.BackupVaultName, reqBody.IamRoleArn,
		reqBody.MalwareScanner, reqBody.RecoveryPointArn, reqBody.ScanMode, reqBody.ScannerRoleArn); msg != "" {
		return c.JSON(http.StatusBadRequest, errResp("MissingParameterValueException", msg))
	}

	if reqBody.MalwareScanner != "GUARDDUTY" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "MalwareScanner must be GUARDDUTY"),
		)
	}

	if reqBody.ScanMode != "FULL_SCAN" && reqBody.ScanMode != "INCREMENTAL_SCAN" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "ScanMode must be FULL_SCAN or INCREMENTAL_SCAN"),
		)
	}

	vaultArn := reqBody.BackupVaultName
	if v, err := h.Backend.DescribeBackupVault(reqBody.BackupVaultName); err == nil {
		vaultArn = v.BackupVaultArn
	}

	var continuousScanEndTime *time.Time
	if reqBody.ContinuousScanEndTime != nil {
		t := time.Unix(int64(*reqBody.ContinuousScanEndTime), 0).UTC()
		continuousScanEndTime = &t
	}

	job := h.Backend.StartScanJob(vaultArn, StartScanJobInput{
		BackupVaultName:          reqBody.BackupVaultName,
		IamRoleArn:               reqBody.IamRoleArn,
		MalwareScanner:           reqBody.MalwareScanner,
		RecoveryPointArn:         reqBody.RecoveryPointArn,
		ScanMode:                 reqBody.ScanMode,
		ScannerRoleArn:           reqBody.ScannerRoleArn,
		IdempotencyToken:         reqBody.IdempotencyToken,
		ScanBaseRecoveryPointArn: reqBody.ScanBaseRecoveryPointArn,
		ContinuousScanEndTime:    continuousScanEndTime,
	})

	// Real AWS: responseCode 201.
	return c.JSON(http.StatusCreated, map[string]any{
		keyScanJobID:    job.ScanJobID,
		keyCreationDate: epochSeconds(job.CreationTime),
	})
}

// validateStartScanJobParams returns a non-empty MissingParameterValueException
// message naming the first missing required field, or "" if all six are
// present. All six are required on the real wire (StartScanJobInput,
// api_op_StartScanJob.go:29-75, backup@v1.59.4).
func validateStartScanJobParams(
	backupVaultName, iamRoleArn, malwareScanner, recoveryPointArn, scanMode, scannerRoleArn string,
) string {
	switch {
	case backupVaultName == "":
		return "BackupVaultName is required"
	case iamRoleArn == "":
		return "IamRoleArn is required"
	case malwareScanner == "":
		return "MalwareScanner is required"
	case recoveryPointArn == "":
		return "RecoveryPointArn is required"
	case scanMode == "":
		return "ScanMode is required"
	case scannerRoleArn == "":
		return "ScannerRoleArn is required"
	default:
		return ""
	}
}
