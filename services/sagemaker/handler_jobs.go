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

// createJobInput mirrors CreateJobInput (api_op_CreateJob.go:29-63):
// JobCategory/JobConfigDocument/JobConfigSchemaVersion/JobName/RoleArn are
// all required; Tags is the sole optional member.
type createJobInput struct {
	JobCategory            string      `json:"JobCategory"`
	JobConfigDocument      string      `json:"JobConfigDocument"`
	JobConfigSchemaVersion string      `json:"JobConfigSchemaVersion"`
	JobName                string      `json:"JobName"`
	RoleArn                string      `json:"RoleArn"`
	Tags                   []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createJobInput

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

// describeJobInput mirrors DescribeJobInput (api_op_DescribeJob.go:27-38):
// both members required.
type describeJobInput struct {
	JobCategory string `json:"JobCategory"`
	JobName     string `json:"JobName"`
}

func (h *Handler) handleDescribeJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeJobInput

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

// deleteJobInput mirrors DeleteJobInput (api_op_DeleteJob.go:27-38): both
// members required.
type deleteJobInput struct {
	JobCategory string `json:"JobCategory"`
	JobName     string `json:"JobName"`
}

func (h *Handler) handleDeleteJob(ctx context.Context, body []byte) error {
	var req deleteJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" || req.JobName == "" {
		return fmt.Errorf("%w: JobCategory and JobName are required", errInvalidRequest)
	}

	return h.Backend.DeleteJob(ctx, req.JobCategory, req.JobName)
}

// stopJobInput mirrors StopJobInput (api_op_StopJob.go:27-38): both members
// required.
type stopJobInput struct {
	JobCategory string `json:"JobCategory"`
	JobName     string `json:"JobName"`
}

func (h *Handler) handleStopJob(ctx context.Context, body []byte) error {
	var req stopJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobCategory == "" || req.JobName == "" {
		return fmt.Errorf("%w: JobCategory and JobName are required", errInvalidRequest)
	}

	return h.Backend.StopJob(ctx, req.JobCategory, req.JobName)
}

// listJobsInput mirrors ListJobsInput (api_op_ListJobs.go:29-70): JobCategory
// is the sole required member. The four time filters are awsjson1.1
// epoch-second numbers on the wire (confirmed by this campaign's
// repo-spanning time-decode bug, parity-16) — decoded as *float64, never
// *time.Time.
type listJobsInput struct {
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

func (h *Handler) handleListJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listJobsInput

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

// describeJobSchemaVersionInput mirrors DescribeJobSchemaVersionInput
// (api_op_DescribeJobSchemaVersion.go:27-38): JobCategory required,
// JobConfigSchemaVersion optional ("If not specified, the latest version is
// returned").
type describeJobSchemaVersionInput struct {
	JobCategory            string `json:"JobCategory"`
	JobConfigSchemaVersion string `json:"JobConfigSchemaVersion"`
}

func (h *Handler) handleDescribeJobSchemaVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req describeJobSchemaVersionInput

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

// listJobSchemaVersionsInput mirrors ListJobSchemaVersionsInput
// (api_op_ListJobSchemaVersions.go:27-40): JobCategory required, MaxResults/
// NextToken optional.
type listJobSchemaVersionsInput struct {
	JobCategory string `json:"JobCategory"`
	NextToken   string `json:"NextToken"`
	MaxResults  int32  `json:"MaxResults"`
}

func (h *Handler) handleListJobSchemaVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req listJobSchemaVersionsInput

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
