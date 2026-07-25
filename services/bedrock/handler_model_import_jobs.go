package bedrock

import (
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// modelDataSourceWire is the wire shape of the real ModelDataSource union,
// whose sole member today is "s3DataSource" (types.ModelDataSourceMemberS3DataSource).
type modelDataSourceWire struct {
	S3DataSource *s3DataSourceWire `json:"s3DataSource,omitempty"`
}

type s3DataSourceWire struct {
	S3Uri string `json:"s3Uri"`
}

// createModelImportJobInput is the parsed request body for CreateModelImportJob.
type createModelImportJobInput struct {
	ModelDataSource   *modelDataSourceWire `json:"modelDataSource"`
	JobName           string               `json:"jobName"`
	ImportedModelName string               `json:"importedModelName"`
	RoleArn           string               `json:"roleArn"`
	Tags              []Tag                `json:"jobTags,omitempty"`
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

	var s3Uri string
	if in.ModelDataSource != nil && in.ModelDataSource.S3DataSource != nil {
		s3Uri = in.ModelDataSource.S3DataSource.S3Uri
	}

	job, opErr := h.Backend.CreateModelImportJob(in.JobName, in.ImportedModelName, in.RoleArn, s3Uri, in.Tags)
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
		"importedModelName": j.ImportedModelName,
		"roleArn":           j.RoleArn,
		keyStatus:           j.Status,
		keyCreationTime:     j.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
	}

	if j.ModelDataSourceS3 != "" {
		out["modelDataSource"] = modelDataSourceWire{S3DataSource: &s3DataSourceWire{S3Uri: j.ModelDataSourceS3}}
	}

	if j.EndTime != nil {
		out["endTime"] = j.EndTime.Format(time.RFC3339)
	}

	if len(j.Tags) > 0 {
		out["tags"] = j.Tags
	}

	return out
}

// importedModelToWire builds the real GetImportedModelOutput / ImportedModelSummary
// shape: modelArn, modelName, jobArn, jobName, creationTime, and (when known)
// modelDataSource. gopherstack previously invented a "status" field (not
// present on either real shape -- ImportedModel has no lifecycle status of its
// own, only the ModelImportJob that produced it does) and used "createdAt"
// instead of the real "creationTime" key.
func importedModelToWire(j *ModelImportJob) map[string]any {
	out := map[string]any{
		keyModelArn:     j.ImportedModelArn,
		"modelName":     j.ImportedModelName,
		"jobArn":        j.JobArn,
		"jobName":       j.JobName,
		keyCreationTime: j.CreationTime.Format(time.RFC3339),
	}

	if j.ModelDataSourceS3 != "" {
		out["modelDataSource"] = modelDataSourceWire{S3DataSource: &s3DataSourceWire{S3Uri: j.ModelDataSourceS3}}
	}

	return out
}

func (h *Handler) handleGetImportedModel(c *echo.Context, modelARN string) error {
	job, err := h.Backend.GetImportedModel(modelARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, importedModelToWire(job))
}

func (h *Handler) handleListImportedModels(c *echo.Context) error {
	q := c.Request().URL.Query()

	var creationTimeAfter, creationTimeBefore *time.Time
	if v := q.Get("creationTimeAfter"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			creationTimeAfter = &t
		}
	}
	if v := q.Get("creationTimeBefore"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			creationTimeBefore = &t
		}
	}

	models, nextToken := h.Backend.ListImportedModels(
		q.Get("nameContains"), creationTimeAfter, creationTimeBefore, q.Get("nextToken"),
	)

	summaries := make([]map[string]any, 0, len(models))
	for _, m := range models {
		summaries = append(summaries, importedModelToWire(m))
	}

	resp := map[string]any{"modelSummaries": summaries}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteImportedModel(c *echo.Context, modelARN string) error {
	if err := h.Backend.DeleteImportedModel(modelARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
