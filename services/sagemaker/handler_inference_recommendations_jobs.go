package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob handlers
// ---------------------------------------------------------------------------

// createInferenceRecommendationsJobInput mirrors
// CreateInferenceRecommendationsJobInput (api_op_CreateInferenceRecommendationsJob.go:1-58).
type createInferenceRecommendationsJobInput struct {
	JobName            string          `json:"JobName"`
	JobType            string          `json:"JobType,omitempty"`
	JobDescription     string          `json:"JobDescription,omitempty"`
	RoleArn            string          `json:"RoleArn,omitempty"`
	InputConfig        json.RawMessage `json:"InputConfig,omitempty"`
	OutputConfig       json.RawMessage `json:"OutputConfig,omitempty"`
	StoppingConditions json.RawMessage `json:"StoppingConditions,omitempty"`
	Tags               []tagObject     `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateInferenceRecommendationsJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createInferenceRecommendationsJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.CreateInferenceRecommendationsJob(ctx, CreateInferenceRecommendationsJobOptions{
		JobName:            req.JobName,
		JobType:            req.JobType,
		JobDescription:     req.JobDescription,
		RoleArn:            req.RoleArn,
		InputConfig:        req.InputConfig,
		OutputConfig:       req.OutputConfig,
		StoppingConditions: req.StoppingConditions,
		Tags:               fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyJobArn: j.JobArn})
}

// describeInferenceRecommendationsJobInput mirrors
// DescribeInferenceRecommendationsJobInput (api_op_DescribeInferenceRecommendationsJob.go:29-34).
type describeInferenceRecommendationsJobInput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleDescribeInferenceRecommendationsJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeInferenceRecommendationsJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeInferenceRecommendationsJob(ctx, req.JobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"JobName":                  j.JobName,
		keyJobArn:                  j.JobArn,
		keyStatus:                  j.Status,
		"JobType":                  j.JobType,
		"InferenceRecommendations": []any{},
		keyCreationTime:            epochSeconds(j.CreationTime),
		keyLastModifiedTime:        epochSeconds(j.LastModifiedTime),
	}

	if j.JobDescription != "" {
		resp["JobDescription"] = j.JobDescription
	}

	if j.RoleArn != "" {
		resp[keyRoleArn] = j.RoleArn
	}

	if len(j.InputConfig) > 0 {
		resp["InputConfig"] = j.InputConfig
	}

	if len(j.StoppingConditions) > 0 {
		resp["StoppingConditions"] = j.StoppingConditions
	}

	return json.Marshal(resp)
}

// stopInferenceRecommendationsJobInput mirrors
// StopInferenceRecommendationsJobInput (api_op_StopInferenceRecommendationsJob.go:27-32).
type stopInferenceRecommendationsJobInput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleStopInferenceRecommendationsJob(ctx context.Context, body []byte) error {
	var req stopInferenceRecommendationsJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceRecommendationsJob(ctx, req.JobName)
}

type inferenceRecommendationsJobSummary struct {
	JobName          string  `json:"JobName"`
	JobArn           string  `json:"JobArn"`
	Status           string  `json:"Status"`
	JobType          string  `json:"JobType,omitempty"`
	JobDescription   string  `json:"JobDescription,omitempty"`
	RoleArn          string  `json:"RoleArn,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

// listInferenceRecommendationsJobsInput mirrors
// ListInferenceRecommendationsJobsInput (api_op_ListInferenceRecommendationsJobs.go:30-63).
type listInferenceRecommendationsJobsInput struct {
	CreationTimeAfter            *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore           *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter        *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore       *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken                    string   `json:"NextToken"`
	NameContains                 string   `json:"NameContains"`
	ModelNameEquals              string   `json:"ModelNameEquals"`
	ModelPackageVersionArnEquals string   `json:"ModelPackageVersionArnEquals"`
	StatusEquals                 string   `json:"StatusEquals"`
	SortBy                       string   `json:"SortBy"`
	SortOrder                    string   `json:"SortOrder"`
	MaxResults                   int32    `json:"MaxResults"`
}

func (h *Handler) handleListInferenceRecommendationsJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listInferenceRecommendationsJobsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListInferenceRecommendationsJobs(
		ctx,
		req.NextToken,
		ListInferenceRecommendationsJobsFilter{
			NameContains:                 req.NameContains,
			ModelNameEquals:              req.ModelNameEquals,
			ModelPackageVersionArnEquals: req.ModelPackageVersionArnEquals,
			StatusEquals:                 req.StatusEquals,
			SortBy:                       req.SortBy,
			SortOrder:                    req.SortOrder,
			MaxResults:                   req.MaxResults,
			CreationTimeAfter:            timeFromEpochSecondsPtr(req.CreationTimeAfter),
			CreationTimeBefore:           timeFromEpochSecondsPtr(req.CreationTimeBefore),
			LastModifiedTimeAfter:        timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
			LastModifiedTimeBefore:       timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
		},
	)

	summaries := make([]inferenceRecommendationsJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, inferenceRecommendationsJobSummary{
			JobName:          j.JobName,
			JobArn:           j.JobArn,
			Status:           j.Status,
			JobType:          j.JobType,
			JobDescription:   j.JobDescription,
			RoleArn:          j.RoleArn,
			CreationTime:     epochSeconds(j.CreationTime),
			LastModifiedTime: epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"InferenceRecommendationsJobs": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

// listInferenceRecommendationsJobStepsInput mirrors
// ListInferenceRecommendationsJobStepsInput (api_op_ListInferenceRecommendationsJobSteps.go:27-49).
// MaxResults/Status/StepType are decoded for wire-shape fidelity but are
// disclosed no-ops: this backend never populates any job's Steps at all (no
// recommender subtask simulation exists), so there is nothing for them to
// filter or page over.
type listInferenceRecommendationsJobStepsInput struct {
	JobName    string `json:"JobName"`
	NextToken  string `json:"NextToken"`
	Status     string `json:"Status,omitempty"`
	StepType   string `json:"StepType,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListInferenceRecommendationsJobSteps(ctx context.Context, body []byte) ([]byte, error) {
	var req listInferenceRecommendationsJobStepsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	if _, err := h.Backend.DescribeInferenceRecommendationsJob(ctx, req.JobName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Steps": []any{}})
}
