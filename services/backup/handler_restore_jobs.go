package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchRestoreJobOps handles restore-job operations.
func (h *Handler) dispatchRestoreJobOps(
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
	case opPutRestoreValidationResult:
		var reqBody struct {
			RestoreJobID     string `json:"RestoreJobId"`
			ValidationStatus string `json:"ValidationStatus"`
		}
		if err := json.Unmarshal(body, &reqBody); err == nil {
			h.Backend.PutRestoreValidationResult(reqBody.RestoreJobID, reqBody.ValidationStatus)
		}

		return true, c.NoContent(http.StatusNoContent)
	}

	return false, nil
}
