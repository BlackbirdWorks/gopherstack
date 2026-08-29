package backup

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

type startBackupJobBody struct {
	BackupVaultName string `json:"BackupVaultName"`
	ResourceArn     string `json:"ResourceArn"`
	IamRoleArn      string `json:"IamRoleArn"`
	ResourceType    string `json:"ResourceType"`
}

func (h *Handler) handleStartBackupJob(c *echo.Context, body []byte) error {
	var in startBackupJobBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.BackupVaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp(
				"MissingParameterValueException",
				fmt.Sprintf("%s: BackupVaultName is required", errInvalidRequest),
			),
		)
	}

	j, err := h.Backend.StartBackupJob(
		in.BackupVaultName,
		in.ResourceArn,
		in.IamRoleArn,
		in.ResourceType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupJobID:    j.BackupJobID,
		keyBackupVaultArn: j.BackupVaultArn,
		keyCreationDate:   epochSeconds(j.CreationTime),
	})
}

func (h *Handler) handleDescribeBackupJob(c *echo.Context, jobID string) error {
	j, err := h.Backend.DescribeBackupJob(jobID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyBackupJobID:     j.BackupJobID,
		keyBackupVaultName: j.BackupVaultName,
		keyBackupVaultArn:  j.BackupVaultArn,
		keyState:           j.State,
		keyCreationDate:    epochSeconds(j.CreationTime),
	}
	setOptionalStr(resp, "ResourceArn", j.ResourceArn)
	setOptionalStr(resp, "ResourceType", j.ResourceType)
	setOptionalStr(resp, "IamRoleArn", j.IAMRoleArn)
	setOptionalStr(resp, keyAccountID, j.AccountID)
	setOptionalStr(resp, "RecoveryPointArn", j.RecoveryPointArn)
	setOptionalStr(resp, "PercentDone", j.PercentDone)
	setOptionalStr(resp, "MessageCategory", j.MessageCategory)
	setOptionalStr(resp, "ParentJobId", j.ParentJobID)
	setOptionalStr(resp, "CompositeMemberIdentifier", j.CompositeMemberIdentifier)

	if j.IsParent {
		resp["IsParent"] = j.IsParent
	}

	if j.BytesTransferred > 0 {
		resp["BytesTransferred"] = j.BytesTransferred
	}

	if j.BackupSizeInBytes > 0 {
		resp["BackupSizeInBytes"] = j.BackupSizeInBytes
	}

	if j.CompletionTime != nil {
		resp["CompletionDate"] = epochSeconds(*j.CompletionTime)
	}

	if j.ExpectedCompletionDate != nil {
		resp["ExpectedCompletionDate"] = epochSeconds(*j.ExpectedCompletionDate)
	}

	if j.StartBy != nil {
		resp["StartBy"] = epochSeconds(*j.StartBy)
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupJobs(c *echo.Context) error {
	q := c.Request().URL.Query()
	f := ListBackupJobsFilter{
		VaultName:     q.Get("backupVaultName"),
		State:         q.Get("state"),
		ResourceArn:   q.Get("resourceArn"),
		ResourceType:  q.Get("resourceType"),
		AccountID:     q.Get("accountId"),
		ParentJobID:   q.Get("parentJobId"),
		CreatedAfter:  ParseTimeFilter(q.Get("createdAfter")),
		CreatedBefore: ParseTimeFilter(q.Get("createdBefore")),
		NextToken:     q.Get("nextToken"),
	}
	if mr := parseInt(q.Get("maxResults")); mr > 0 {
		f.MaxResults = mr
	}

	jobs, nextToken := h.Backend.ListBackupJobsFiltered(f)
	items := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		item := map[string]any{
			keyBackupJobID:     j.BackupJobID,
			keyBackupVaultName: j.BackupVaultName,
			keyBackupVaultArn:  j.BackupVaultArn,
			keyState:           j.State,
			keyCreationDate:    epochSeconds(j.CreationTime),
		}
		setOptionalStr(item, "ResourceArn", j.ResourceArn)
		setOptionalStr(item, "ResourceType", j.ResourceType)
		setOptionalStr(item, "IamRoleArn", j.IAMRoleArn)
		setOptionalStr(item, keyAccountID, j.AccountID)
		setOptionalStr(item, "ParentJobId", j.ParentJobID)
		setOptionalStr(item, "RecoveryPointArn", j.RecoveryPointArn)
		setOptionalStr(item, "MessageCategory", j.MessageCategory)
		if j.CompletionTime != nil {
			item["CompletionDate"] = epochSeconds(*j.CompletionTime)
		}
		if j.BackupSizeInBytes > 0 {
			item["BackupSizeInBytes"] = j.BackupSizeInBytes
		}
		if j.BytesTransferred > 0 {
			item["BytesTransferred"] = j.BytesTransferred
		}
		if j.IsParent {
			item["IsParent"] = j.IsParent
		}
		items = append(items, item)
	}

	resp := map[string]any{"BackupJobs": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// dispatchBackupJobSummaryOps handles backup-job summary and stop operations.
func (h *Handler) dispatchBackupJobSummaryOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opListBackupJobSummaries:
		summaries := h.Backend.ListBackupJobSummaries()

		return true, c.JSON(http.StatusOK, map[string]any{"BackupJobSummaries": summaries})
	case opStopBackupJob:
		if err := h.Backend.StopBackupJob(route.resource); err != nil {
			return true, c.JSON(http.StatusBadRequest, errResp("ResourceNotFoundException", err.Error()))
		}

		return true, c.NoContent(http.StatusNoContent)
	}

	return false, nil
}

// --- Tag handlers ---
