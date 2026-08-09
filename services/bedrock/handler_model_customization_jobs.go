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
	CustomModelName     string `json:"customModelName"`
	BaseModelIdentifier string `json:"baseModelIdentifier"`
	CustomizationType   string `json:"customizationType,omitempty"`
	// JobTags, not Tags: real CreateModelCustomizationJobInput carries the
	// job's own tags as JobTags (wire key "jobTags"), separate from
	// CustomModelTags (wire key "customModelTags") on the resulting output
	// model, which gopherstack does not track as an independently taggable
	// resource (bedrock@v1.66.4 serializers.go:
	// awsRestjson1_serializeOpDocumentCreateModelCustomizationJobInput).
	Tags []Tag `json:"jobTags,omitempty"`
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
		in.JobName, in.CustomModelName, in.BaseModelIdentifier, in.CustomizationType, in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createModelCustomizationJobOutput{JobArn: job.JobArn})
}

// modelCustomizationJobOutput is GetModelCustomizationJob's response shape
// (bedrock@v1.66.4 GetModelCustomizationJobResponse via botocore
// service-2.json: outputModelArn/outputModelName, not customModelArn/Name --
// see modelCustomizationJobSummaryOutput for the distinct ListModelCustomizationJobs
// shape).
type modelCustomizationJobOutput struct {
	CreationTime      string `json:"creationTime"`
	LastModifiedTime  string `json:"lastModifiedTime"`
	JobArn            string `json:"jobArn"`
	JobName           string `json:"jobName"`
	BaseModelArn      string `json:"baseModelArn"`
	OutputModelArn    string `json:"outputModelArn"`
	OutputModelName   string `json:"outputModelName"`
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
		OutputModelName:   j.CustomModelName,
		Status:            j.Status,
		CustomizationType: j.CustomizationType,
		CreationTime:      j.CreationTime.Format(time.RFC3339),
		LastModifiedTime:  j.LastModifiedTime.Format(time.RFC3339),
		Tags:              j.Tags,
	}
}

// modelCustomizationJobSummaryOutput is ListModelCustomizationJobs' per-item
// shape (bedrock@v1.66.4 ModelCustomizationJobSummary via botocore
// service-2.json): customModelArn/customModelName, distinct from Get's
// outputModelArn/outputModelName.
type modelCustomizationJobSummaryOutput struct {
	CreationTime      string `json:"creationTime"`
	LastModifiedTime  string `json:"lastModifiedTime"`
	JobArn            string `json:"jobArn"`
	JobName           string `json:"jobName"`
	BaseModelArn      string `json:"baseModelArn"`
	CustomModelArn    string `json:"customModelArn,omitempty"`
	CustomModelName   string `json:"customModelName,omitempty"`
	Status            string `json:"status"`
	CustomizationType string `json:"customizationType,omitempty"`
}

func customizationJobToSummaryOutput(j *ModelCustomizationJob) modelCustomizationJobSummaryOutput {
	return modelCustomizationJobSummaryOutput{
		JobArn:            j.JobArn,
		JobName:           j.JobName,
		BaseModelArn:      j.BaseModelArn,
		CustomModelArn:    j.OutputModelArn,
		CustomModelName:   j.CustomModelName,
		Status:            j.Status,
		CustomizationType: j.CustomizationType,
		CreationTime:      j.CreationTime.Format(time.RFC3339),
		LastModifiedTime:  j.LastModifiedTime.Format(time.RFC3339),
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
	NextToken                      string                               `json:"nextToken,omitempty"`
	ModelCustomizationJobSummaries []modelCustomizationJobSummaryOutput `json:"modelCustomizationJobSummaries"`
}

// parseListModelCustomizationJobsQuery builds the backend filter/sort/pagination
// input from the real ListModelCustomizationJobs query-string bindings
// (aws-sdk-go-v2 serializers.go:6989-7027): statusEquals, nameContains,
// creationTimeAfter/Before, sortBy, sortOrder, nextToken.
func parseListModelCustomizationJobsQuery(c *echo.Context) *ListModelCustomizationJobsInput {
	q := c.Request().URL.Query()

	in := &ListModelCustomizationJobsInput{
		StatusEquals: q.Get("statusEquals"),
		NameContains: q.Get("nameContains"),
		SortBy:       q.Get("sortBy"),
		SortOrder:    q.Get("sortOrder"),
		NextToken:    q.Get("nextToken"),
	}

	if v := q.Get("creationTimeAfter"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			in.CreationTimeAfter = &t
		}
	}

	if v := q.Get("creationTimeBefore"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			in.CreationTimeBefore = &t
		}
	}

	return in
}

func (h *Handler) handleListModelCustomizationJobs(c *echo.Context) error {
	jobs, outToken := h.Backend.ListModelCustomizationJobs(parseListModelCustomizationJobsQuery(c))
	summaries := make([]modelCustomizationJobSummaryOutput, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, customizationJobToSummaryOutput(j))
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
