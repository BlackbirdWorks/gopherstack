package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListCopyJobs(c *echo.Context) error {
	q := c.Request().URL.Query()
	f := ListCopyJobsFilter{
		State:                     q.Get("byState"),
		ResourceArn:               q.Get("byResourceArn"),
		ResourceType:              q.Get("byResourceType"),
		SourceBackupVaultArn:      q.Get("bySourceBackupVaultArn"),
		DestinationBackupVaultArn: q.Get("byDestinationVaultArn"),
		AccountID:                 q.Get("byAccountId"),
		CreatedAfter:              ParseTimeFilter(q.Get("byCreatedAfter")),
		CreatedBefore:             ParseTimeFilter(q.Get("byCreatedBefore")),
		NextToken:                 q.Get("nextToken"),
		MaxResults:                parseInt(q.Get("maxResults")),
	}

	jobs, nextToken := h.Backend.ListCopyJobsFiltered(f)
	items := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		item := map[string]any{
			keyCopyJobID:    j.CopyJobID,
			keyState:        j.State,
			keyCreationDate: epochSeconds(j.CreationDate),
		}
		setOptionalStr(item, "ResourceArn", j.ResourceArn)
		setOptionalStr(item, "ResourceType", j.ResourceType)
		setOptionalStr(item, "SourceBackupVaultArn", j.SourceBackupVaultArn)
		setOptionalStr(item, "DestinationBackupVaultArn", j.DestinationBackupVaultArn)
		setOptionalStr(item, "IamRoleArn", j.IAMRoleArn)
		setOptionalStr(item, "AccountId", j.AccountID)
		if j.CompletionDate != nil {
			item["CompletionDate"] = epochSeconds(*j.CompletionDate)
		}
		items = append(items, item)
	}

	resp := map[string]any{"CopyJobs": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeCopyJob(c *echo.Context, copyJobID string) error {
	if copyJobID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "CopyJobId is required"),
		)
	}

	j, err := h.Backend.DescribeCopyJob(copyJobID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyCopyJobID:    j.CopyJobID,
		keyState:        j.State,
		keyCreationDate: epochSeconds(j.CreationDate),
	}
	if j.ResourceArn != "" {
		resp["ResourceArn"] = j.ResourceArn
	}
	if j.SourceBackupVaultArn != "" {
		resp["SourceBackupVaultArn"] = j.SourceBackupVaultArn
	}
	if j.DestinationBackupVaultArn != "" {
		resp["DestinationBackupVaultArn"] = j.DestinationBackupVaultArn
	}

	return c.JSON(http.StatusOK, map[string]any{
		"CopyJob": resp,
	})
}

// dispatchCopyJobExtraOps handles copy-job summary and start operations.
func (h *Handler) dispatchCopyJobExtraOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
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

// --- Restore testing read/update/delete handlers ---
