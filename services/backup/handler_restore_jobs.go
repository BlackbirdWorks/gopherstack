package backup

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// RestoreJobsFilterFromQuery builds a ListRestoreJobsFilter from ListRestoreJobs
// query parameters (api_op_ListRestoreJobs.go, serializers.go, backup@v1.59.4):
// accountId, resourceType, status, createdAfter, createdBefore, completeAfter,
// completeBefore, maxResults, nextToken.
func RestoreJobsFilterFromQuery(q url.Values) ListRestoreJobsFilter {
	return ListRestoreJobsFilter{
		AccountID:      q.Get("accountId"),
		ResourceType:   q.Get("resourceType"),
		Status:         q.Get("status"),
		CreatedAfter:   ParseTimeFilter(q.Get("createdAfter")),
		CreatedBefore:  ParseTimeFilter(q.Get("createdBefore")),
		CompleteAfter:  ParseTimeFilter(q.Get("completeAfter")),
		CompleteBefore: ParseTimeFilter(q.Get("completeBefore")),
		MaxResults:     parseInt(q.Get("maxResults")),
		NextToken:      q.Get("nextToken"),
	}
}

// restoreJobToJSON renders the fields of a RestoreJob this backend tracks,
// matching (a subset of) the real types.RestoreJobsListMember wire shape
// shared by DescribeRestoreJob/ListRestoreJobs.
func restoreJobToJSON(j *RestoreJob) map[string]any {
	resp := map[string]any{
		keyRestoreJobID:    j.RestoreJobID,
		keyStatus:          j.Status,
		"RecoveryPointArn": j.RecoveryPointArn,
		keyIamRoleArn:      j.IAMRoleArn,
		"PercentDone":      j.PercentDone,
		keyAccountID:       j.AccountID,
		"CreationDate":     epochSeconds(j.StartTime),
	}
	// Wire member is SourceResourceArn, not ResourceArn -- neither
	// types.RestoreJobsListMember nor DescribeRestoreJobOutput (backup@v1.59.4
	// types/types.go:2109-2196, api_op_DescribeRestoreJob.go:39-124) declares
	// "ResourceArn".
	setOptionalStr(resp, "SourceResourceArn", j.ResourceArn)
	setOptionalStr(resp, "ResourceType", j.ResourceType)
	setOptionalStr(resp, "BackupVaultArn", j.BackupVaultArn)
	setOptionalStr(resp, "CreatedResourceArn", j.CreatedResourceArn)
	setOptionalStr(resp, "ValidationStatus", j.ValidationStatus)
	setOptionalStr(resp, "ValidationStatusMessage", j.ValidationStatusMessage)
	setOptionalStr(resp, "StatusMessage", j.StatusMessage)
	if j.BackupSizeInBytes > 0 {
		resp["BackupSizeInBytes"] = j.BackupSizeInBytes
	}
	if j.CompletionDate != nil {
		resp["CompletionDate"] = epochSeconds(*j.CompletionDate)
	}

	return resp
}

func (h *Handler) handleDescribeRestoreJob(c *echo.Context, restoreJobID string) error {
	job, err := h.Backend.DescribeRestoreJob(restoreJobID)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, restoreJobToJSON(job))
}

func (h *Handler) handleGetRestoreJobMetadata(c *echo.Context, restoreJobID string) error {
	job, err := h.Backend.DescribeRestoreJob(restoreJobID)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ResourceNotFoundException", err.Error()))
	}
	metadata := job.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreJobID: restoreJobID, "Metadata": metadata,
	})
}

func (h *Handler) handleStartRestoreJob(c *echo.Context, defaultRecoveryPointArn string, body []byte) error {
	var reqBody struct {
		Metadata         map[string]string `json:"Metadata"`
		RecoveryPointArn string            `json:"RecoveryPointArn"`
		IamRoleArn       string            `json:"IamRoleArn"`
		ResourceType     string            `json:"ResourceType"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}
	if reqBody.RecoveryPointArn == "" {
		reqBody.RecoveryPointArn = defaultRecoveryPointArn
	}

	job, err := h.Backend.StartRestoreJob(
		reqBody.RecoveryPointArn,
		reqBody.IamRoleArn,
		reqBody.ResourceType,
		reqBody.Metadata,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyRestoreJobID: job.RestoreJobID})
}

func (h *Handler) handlePutRestoreValidationResult(c *echo.Context, body []byte) error {
	var reqBody struct {
		RestoreJobID            string `json:"RestoreJobId"`
		ValidationStatus        string `json:"ValidationStatus"`
		ValidationStatusMessage string `json:"ValidationStatusMessage"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if err := h.Backend.PutRestoreValidationResult(
		reqBody.RestoreJobID, reqBody.ValidationStatus, reqBody.ValidationStatusMessage,
	); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// dispatchRestoreJobOps handles restore-job operations.
func (h *Handler) dispatchRestoreJobOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opDescribeRestoreJob:

		return true, h.handleDescribeRestoreJob(c, route.resource)
	case opListRestoreJobs:
		q := c.Request().URL.Query()
		jobs, nextToken := h.Backend.ListRestoreJobsFiltered(RestoreJobsFilterFromQuery(q))
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(items, restoreJobToJSON(j))
		}

		resp := map[string]any{"RestoreJobs": items}
		if nextToken != "" {
			resp["NextToken"] = nextToken
		}

		return true, c.JSON(http.StatusOK, resp)
	case opListRestoreJobsByProtectedResource:
		jobs := h.Backend.ListRestoreJobsByProtectedResource(route.resource)
		items := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			items = append(items, restoreJobToJSON(j))
		}

		return true, c.JSON(http.StatusOK, map[string]any{"RestoreJobs": items})
	case opListRestoreJobSummaries:
		summaries := h.Backend.ListRestoreJobSummaries()

		return true, c.JSON(http.StatusOK, map[string]any{"RestoreJobSummaries": summaries})
	case opGetRestoreJobMetadata:

		return true, h.handleGetRestoreJobMetadata(c, route.resource)
	case opStartRestoreJob:

		return true, h.handleStartRestoreJob(c, route.resource, body)
	case opPutRestoreValidationResult:

		return true, h.handlePutRestoreValidationResult(c, body)
	}

	return false, nil
}
