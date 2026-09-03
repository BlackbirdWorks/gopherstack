package bedrock

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// extractModelCopyImportOperation mirrors routeStubCopyImportOps's dispatch
// order exactly, so ExtractOperation agrees with the real dispatch contract
// for the ModelCopyJob/ModelImportJob families -- previously absent from
// ExtractOperation's extractor list entirely (found by gopherstack-n1mb's
// route table; Handler() itself already dispatched these correctly).
func extractModelCopyImportOperation(path, method string) (string, bool) {
	switch {
	case path == modelCopyJobsPrefix && method == http.MethodPost:
		return "CreateModelCopyJob", true
	case path == modelCopyJobsPrefix && method == http.MethodGet:
		return "ListModelCopyJobs", true
	case strings.HasPrefix(path, modelCopyJobsPrefix+"/") && method == http.MethodGet:
		return "GetModelCopyJob", true
	case path == modelImportJobsPrefix && method == http.MethodPost:
		return "CreateModelImportJob", true
	case path == modelImportJobsPrefix && method == http.MethodGet:
		return "ListModelImportJobs", true
	case strings.HasPrefix(path, modelImportJobsPrefix+"/") && method == http.MethodGet:
		return "GetModelImportJob", true
	}

	return "", false
}

// routeStubCopyImportOps handles model copy and import job operations backed by real state.
func (h *Handler) routeStubCopyImportOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == modelCopyJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelCopyJob(c)
	case path == modelCopyJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelCopyJobs(c)
	case strings.HasPrefix(path, modelCopyJobsPrefix+"/") && method == http.MethodGet:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, modelCopyJobsPrefix+"/"))

		return true, h.handleGetModelCopyJob(c, jobARN)
	case path == modelImportJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelImportJob(c)
	case path == modelImportJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelImportJobs(c)
	case strings.HasPrefix(path, modelImportJobsPrefix+"/") && method == http.MethodGet:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, modelImportJobsPrefix+"/"))

		return true, h.handleGetModelImportJob(c, jobARN)
	}

	return false, nil
}

// createModelCopyJobInput is the parsed request body for CreateModelCopyJob.
type createModelCopyJobInput struct {
	SourceModelArn  string `json:"sourceModelArn"`
	TargetModelName string `json:"targetModelName"`
	// TargetModelTags, not Tags: real CreateModelCopyJobInput carries the field
	// as TargetModelTags, wire key "targetModelTags" (bedrock@v1.66.4
	// serializers.go: awsRestjson1_serializeOpDocumentCreateModelCopyJobInput).
	Tags []Tag `json:"targetModelTags,omitempty"`
}

func (h *Handler) handleCreateModelCopyJob(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			errorResponse("InternalServerException", "internal server error"),
		)
	}

	in, parseErr := parseBody[createModelCopyJobInput](body)
	if parseErr != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	if in.TargetModelName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "targetModelName is required"),
		)
	}

	job, opErr := h.Backend.CreateModelCopyJob(in.SourceModelArn, in.TargetModelName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, modelCopyJobToOutput(job))
}

func parseListModelCopyJobsQuery(c *echo.Context) *ListModelCopyJobsInput {
	q := c.Request().URL.Query()

	maxResults, _ := strconv.ParseInt(q.Get("maxResults"), 10, 32)

	in := &ListModelCopyJobsInput{
		StatusEquals:            q.Get("statusEquals"),
		SourceAccountEquals:     q.Get("sourceAccountEquals"),
		SourceModelArnEquals:    q.Get("sourceModelArnEquals"),
		TargetModelNameContains: q.Get("outputModelNameContains"),
		SortBy:                  q.Get("sortBy"),
		SortOrder:               q.Get("sortOrder"),
		NextToken:               q.Get("nextToken"),
		MaxResults:              int32(maxResults),
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

func (h *Handler) handleListModelCopyJobs(c *echo.Context) error {
	jobs, nextToken := h.Backend.ListModelCopyJobs(parseListModelCopyJobsQuery(c))
	summaries := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, modelCopyJobToOutput(j))
	}

	resp := map[string]any{"modelCopyJobSummaries": summaries}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetModelCopyJob(c *echo.Context, jobARN string) error {
	job, err := h.Backend.GetModelCopyJob(jobARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, modelCopyJobToOutput(job))
}

// accountIDFromARN extracts the account segment (index 4 of arnFieldCount)
// from a well-formed ARN this backend built itself via pkgs/arn.Build --
// not a fabricated value, just re-deriving what's already embedded.
func accountIDFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", arnFieldCount)
	if len(parts) != arnFieldCount {
		return ""
	}

	return parts[4]
}

func modelCopyJobToOutput(j *ModelCopyJob) map[string]any {
	out := map[string]any{
		keyJobArn:           j.JobArn,
		"sourceModelArn":    j.SourceModelArn,
		"sourceAccountId":   accountIDFromARN(j.SourceModelArn),
		"targetModelArn":    j.TargetModelArn,
		keyStatus:           j.Status,
		keyCreationTime:     j.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
	}

	if j.FailureMessage != "" {
		out["failureMessage"] = j.FailureMessage
	}

	if len(j.Tags) > 0 {
		out["tags"] = j.Tags
	}

	return out
}
