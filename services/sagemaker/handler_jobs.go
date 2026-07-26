package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// aiAndGenericJobOpsSupported aggregates the operation lists for the four
// families added by the SDK bump that introduced CreateJob et al.
// (AIBenchmarkJob, AIRecommendationJob, AIWorkloadConfig, and the generic
// Job/JobSchemaVersion family below) into a single list, so
// GetSupportedOperations only needs one variable/append pair for all of
// them.
func aiAndGenericJobOpsSupported() []string {
	ops := make(
		[]string,
		0,
		len(aiBenchmarkJobOpsSupported())+len(aiRecommendationJobOpsSupported())+
			len(aiWorkloadConfigOpsSupported())+len(jobOpsSupported()),
	)
	ops = append(ops, aiBenchmarkJobOpsSupported()...)
	ops = append(ops, aiRecommendationJobOpsSupported()...)
	ops = append(ops, aiWorkloadConfigOpsSupported()...)
	ops = append(ops, jobOpsSupported()...)

	return ops
}

// jobOpsSupported returns the operations handled by dispatchJobOps.
func jobOpsSupported() []string {
	return []string{
		"CreateJob",
		"DescribeJob",
		"DeleteJob",
		"StopJob",
		"ListJobs",
		"DescribeJobSchemaVersion",
		"ListJobSchemaVersions",
	}
}

// dispatchJobOps handles the generic Job / JobSchemaVersion operation family.
func (h *Handler) dispatchJobOps(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "CreateJob":
		r, err := h.handleCreateJob(ctx, body)

		return r, true, err
	case "DescribeJob":
		r, err := h.handleDescribeJob(ctx, body)

		return r, true, err
	case "DeleteJob":
		return nil, true, h.handleDeleteJob(ctx, body)
	case "StopJob":
		return nil, true, h.handleStopJob(ctx, body)
	case "ListJobs":
		r, err := h.handleListJobs(ctx, body)

		return r, true, err
	case "DescribeJobSchemaVersion":
		r, err := h.handleDescribeJobSchemaVersion(ctx, body)

		return r, true, err
	case "ListJobSchemaVersions":
		r, err := h.handleListJobSchemaVersions(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleCreateJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobCategory            string      `json:"JobCategory"`
		JobConfigDocument      string      `json:"JobConfigDocument"`
		JobConfigSchemaVersion string      `json:"JobConfigSchemaVersion"`
		JobName                string      `json:"JobName"`
		RoleArn                string      `json:"RoleArn"`
		Tags                   []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	j, err := h.Backend.CreateJob(ctx, CreateJobOptions{
		JobCategory:            req.JobCategory,
		JobConfigDocument:      req.JobConfigDocument,
		JobConfigSchemaVersion: req.JobConfigSchemaVersion,
		JobName:                req.JobName,
		RoleArn:                req.RoleArn,
		Tags:                   fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyJobArn: j.JobArn})
}

func (h *Handler) handleDescribeJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobCategory string `json:"JobCategory"`
		JobName     string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" || req.JobName == "" {
		return nil, fmt.Errorf("%w: JobCategory and JobName are required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeJob(ctx, req.JobCategory, req.JobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(j)
}

func (h *Handler) handleDeleteJob(ctx context.Context, body []byte) error {
	var req struct {
		JobCategory string `json:"JobCategory"`
		JobName     string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" || req.JobName == "" {
		return fmt.Errorf("%w: JobCategory and JobName are required", errInvalidRequest)
	}

	return h.Backend.DeleteJob(ctx, req.JobCategory, req.JobName)
}

func (h *Handler) handleStopJob(ctx context.Context, body []byte) error {
	var req struct {
		JobCategory string `json:"JobCategory"`
		JobName     string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" || req.JobName == "" {
		return fmt.Errorf("%w: JobCategory and JobName are required", errInvalidRequest)
	}

	return h.Backend.StopJob(ctx, req.JobCategory, req.JobName)
}

func (h *Handler) handleListJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
		CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
		LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
		LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
		JobCategory            string   `json:"JobCategory"`
		NameContains           string   `json:"NameContains"`
		StatusEquals           string   `json:"StatusEquals"`
		NextToken              string   `json:"NextToken"`
		SortBy                 string   `json:"SortBy"`
		SortOrder              string   `json:"SortOrder"`
		MaxResults             int32    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" {
		return nil, fmt.Errorf("%w: JobCategory is required", errInvalidRequest)
	}

	jobs, nextToken := h.Backend.ListJobs(ctx, ListJobsParams{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		JobCategory:            req.JobCategory,
		NameContains:           req.NameContains,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		NextToken:              req.NextToken,
		MaxResults:             req.MaxResults,
	})

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		item := map[string]any{
			keyJobArn:            j.JobArn,
			"JobName":            j.JobName,
			"JobCategory":        j.JobCategory,
			"JobStatus":          j.JobStatus,
			"JobSecondaryStatus": j.SecondaryStatus,
			keyCreationTime:      epochSeconds(j.CreationTime),
			keyLastModifiedTime:  epochSeconds(j.LastModifiedTime),
		}

		if j.EndTime != nil {
			item["EndTime"] = epochSeconds(*j.EndTime)
		}

		items = append(items, item)
	}

	return listResp("JobSummaries", items, nextToken)
}

func (h *Handler) handleDescribeJobSchemaVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobCategory            string `json:"JobCategory"`
		JobConfigSchemaVersion string `json:"JobConfigSchemaVersion"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" {
		return nil, fmt.Errorf("%w: JobCategory is required", errInvalidRequest)
	}

	info, err := h.Backend.DescribeJobSchemaVersion(ctx, req.JobCategory, req.JobConfigSchemaVersion)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"JobCategory":            req.JobCategory,
		"JobConfigSchema":        info.JobConfigSchema,
		"JobConfigSchemaVersion": info.JobConfigSchemaVersion,
	})
}

func (h *Handler) handleListJobSchemaVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobCategory string `json:"JobCategory"`
		NextToken   string `json:"NextToken"`
		MaxResults  int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" {
		return nil, fmt.Errorf("%w: JobCategory is required", errInvalidRequest)
	}

	versions, nextToken, err := h.Backend.ListJobSchemaVersions(ctx, req.JobCategory, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, map[string]any{"JobConfigSchemaVersion": v})
	}

	return listResp("JobConfigSchemas", items, nextToken)
}
