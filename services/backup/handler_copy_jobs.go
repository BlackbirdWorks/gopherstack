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
		items = append(items, copyJobToJSON(j))
	}

	resp := map[string]any{"CopyJobs": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// copyJobToJSON renders the fields of a CopyJob this backend actually
// tracks, matching (a subset of) the real types.CopyJob wire shape.
func copyJobToJSON(j *CopyJob) map[string]any {
	resp := map[string]any{
		keyCopyJobID:    j.CopyJobID,
		keyState:        j.State,
		keyCreationDate: epochSeconds(j.CreationDate),
		keyAccountID:    j.AccountID,
	}
	setOptionalStr(resp, "ResourceArn", j.ResourceArn)
	setOptionalStr(resp, "ResourceType", j.ResourceType)
	setOptionalStr(resp, "IamRoleArn", j.IAMRoleArn)
	setOptionalStr(resp, "SourceBackupVaultArn", j.SourceBackupVaultArn)
	setOptionalStr(resp, "DestinationBackupVaultArn", j.DestinationBackupVaultArn)
	setOptionalStr(resp, "DestinationRecoveryPointArn", j.DestinationRecoveryPointArn)
	if j.CompletionDate != nil {
		resp["CompletionDate"] = epochSeconds(*j.CompletionDate)
	}

	return resp
}

func (h *Handler) handleDescribeCopyJob(c *echo.Context, copyJobID string) error {
	if copyJobID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "CopyJobId is required"),
		)
	}

	j, err := h.Backend.DescribeCopyJob(copyJobID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"CopyJob": copyJobToJSON(j),
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
		if err := json.Unmarshal(body, &copyJobReq); err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
		job, err := h.Backend.StartCopyJob(
			copyJobReq.RecoveryPointArn,
			copyJobReq.SourceBackupVaultName,
			copyJobReq.DestinationBackupVaultArn,
			copyJobReq.IamRoleArn,
		)
		if err != nil {
			return true, h.handleError(c, err)
		}

		// Real StartCopyJobOutput: CopyJobId, CreationDate, IsParent only --
		// the destination recovery point ARN isn't known until the job
		// completes, surfaced later via DescribeCopyJob.
		return true, c.JSON(http.StatusOK, map[string]any{
			keyCopyJobID:    job.CopyJobID,
			keyCreationDate: epochSeconds(job.CreationDate),
			"IsParent":      false,
		})
	}

	return false, nil
}

// --- Restore testing read/update/delete handlers ---
