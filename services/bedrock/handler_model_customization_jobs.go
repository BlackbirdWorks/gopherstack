package bedrock

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

func extractCustomizationJobOperation(path, method string) (string, bool) {
	isSubPath := strings.HasPrefix(path, modelCustomizationJobsPrefix+"/")
	isStop := isSubPath && strings.HasSuffix(path, "/stop")

	switch {
	case path == modelCustomizationJobsPrefix && method == http.MethodPost:
		return "CreateModelCustomizationJob", true
	case path == modelCustomizationJobsPrefix && method == http.MethodGet:
		return "ListModelCustomizationJobs", true
	case isSubPath && method == http.MethodGet && !isStop:
		return "GetModelCustomizationJob", true
	case isStop && method == http.MethodPost:
		return "StopModelCustomizationJob", true
	default:
		return "", false
	}
}

func (h *Handler) routeCustomizationJob(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	isSubPath := strings.HasPrefix(path, modelCustomizationJobsPrefix+"/")
	isStop := isSubPath && strings.HasSuffix(path, "/stop")

	switch {
	case path == modelCustomizationJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelCustomizationJob(c, body)
	case path == modelCustomizationJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelCustomizationJobs(c)
	case isSubPath && method == http.MethodGet && !isStop:
		id := decodePath(strings.TrimPrefix(path, modelCustomizationJobsPrefix+"/"))

		return true, h.handleGetModelCustomizationJob(c, id)
	case isStop && method == http.MethodPost:
		rest := strings.TrimPrefix(path, modelCustomizationJobsPrefix+"/")
		id := decodePath(strings.TrimSuffix(rest, "/stop"))

		return true, h.handleStopModelCustomizationJob(c, id)
	default:
		return false, nil
	}
}

type createModelCustomizationJobInput struct {
	JobName             string `json:"jobName"`
	BaseModelIdentifier string `json:"baseModelIdentifier"`
	CustomizationType   string `json:"customizationType,omitempty"`
	Tags                []Tag  `json:"tags,omitempty"`
}

type createModelCustomizationJobOutput struct {
	JobArn string `json:"jobArn"`
}

func (h *Handler) handleCreateModelCustomizationJob(c *echo.Context, body []byte) error {
	in, err := parseBody[createModelCustomizationJobInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	job, opErr := h.Backend.CreateModelCustomizationJob(
		in.JobName, in.BaseModelIdentifier, in.CustomizationType, in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createModelCustomizationJobOutput{JobArn: job.JobArn})
}

type modelCustomizationJobOutput struct {
	CreationTime      string `json:"creationTime"`
	LastModifiedTime  string `json:"lastModifiedTime"`
	JobArn            string `json:"jobArn"`
	JobName           string `json:"jobName"`
	BaseModelArn      string `json:"baseModelArn"`
	OutputModelArn    string `json:"outputModelArn"`
	Status            string `json:"status"`
	CustomizationType string `json:"customizationType,omitempty"`
	Tags              []Tag  `json:"tags,omitempty"`
}

func customizationJobToOutput(j *ModelCustomizationJob) modelCustomizationJobOutput {
	return modelCustomizationJobOutput{
		JobArn:            j.JobArn,
		JobName:           j.JobName,
		BaseModelArn:      j.BaseModelArn,
		OutputModelArn:    j.OutputModelArn,
		Status:            j.Status,
		CustomizationType: j.CustomizationType,
		CreationTime:      j.CreationTime.Format(time.RFC3339),
		LastModifiedTime:  j.LastModifiedTime.Format(time.RFC3339),
		Tags:              j.Tags,
	}
}

func (h *Handler) handleGetModelCustomizationJob(c *echo.Context, id string) error {
	job, err := h.Backend.GetModelCustomizationJob(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, customizationJobToOutput(job))
}

type listModelCustomizationJobsOutput struct {
	NextToken                      string                        `json:"nextToken,omitempty"`
	ModelCustomizationJobSummaries []modelCustomizationJobOutput `json:"modelCustomizationJobSummaries"`
}

func (h *Handler) handleListModelCustomizationJobs(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	jobs, outToken := h.Backend.ListModelCustomizationJobs(nextToken)
	summaries := make([]modelCustomizationJobOutput, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, customizationJobToOutput(j))
	}

	return c.JSON(http.StatusOK, listModelCustomizationJobsOutput{
		ModelCustomizationJobSummaries: summaries,
		NextToken:                      outToken,
	})
}

func (h *Handler) handleStopModelCustomizationJob(c *echo.Context, id string) error {
	if err := h.Backend.StopModelCustomizationJob(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
