package bedrock

import (
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// createModelImportJobInput is the parsed request body for CreateModelImportJob.
type createModelImportJobInput struct {
	JobName string `json:"jobName"`
	Tags    []Tag  `json:"tags,omitempty"`
}

func (h *Handler) handleCreateModelImportJob(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			errorResponse("InternalFailure", "internal server error"),
		)
	}

	in, parseErr := parseBody[createModelImportJobInput](body)
	if parseErr != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	job, opErr := h.Backend.CreateModelImportJob(in.JobName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, modelImportJobToOutput(job))
}

func (h *Handler) handleListModelImportJobs(c *echo.Context) error {
	jobs := h.Backend.ListModelImportJobs()
	summaries := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, modelImportJobToOutput(j))
	}

	return c.JSON(http.StatusOK, map[string]any{"modelImportJobSummaries": summaries})
}

func (h *Handler) handleGetModelImportJob(c *echo.Context, jobARN string) error {
	job, err := h.Backend.GetModelImportJob(jobARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, modelImportJobToOutput(job))
}

func modelImportJobToOutput(j *ModelImportJob) map[string]any {
	out := map[string]any{
		keyJobArn:           j.JobArn,
		keyJobName:          j.JobName,
		"importedModelArn":  j.ImportedModelArn,
		keyStatus:           j.Status,
		keyCreationTime:     j.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
	}

	if j.EndTime != nil {
		out["endTime"] = j.EndTime.Format(time.RFC3339)
	}

	if len(j.Tags) > 0 {
		out["tags"] = j.Tags
	}

	return out
}

func (h *Handler) handleGetImportedModel(c *echo.Context, modelARN string) error {
	job, err := h.Backend.GetImportedModel(modelARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyModelArn:  job.ImportedModelArn,
		keyJobName:   job.JobName,
		keyStatus:    job.Status,
		keyCreatedAt: job.CreationTime.Format(time.RFC3339),
	})
}

func (h *Handler) handleListImportedModels(c *echo.Context) error {
	models := h.Backend.ListImportedModels()
	summaries := make([]map[string]any, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, map[string]any{
			keyModelArn:  m.ImportedModelArn,
			keyJobName:   m.JobName,
			keyStatus:    m.Status,
			keyCreatedAt: m.CreationTime.Format(time.RFC3339),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"modelSummaries": summaries})
}

func (h *Handler) handleDeleteImportedModel(c *echo.Context, modelARN string) error {
	if err := h.Backend.DeleteImportedModel(modelARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
