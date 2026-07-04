package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// labelingOpsSupported returns the labeling-job / workteam-subscription
// operations implemented in this file.
func labelingOpsSupported() []string {
	return []string{
		"CreateLabelingJob",
		"DescribeLabelingJob",
		"StopLabelingJob",
		"ListLabelingJobs",
		"ListLabelingJobsForWorkteam",
		"ListSubscribedWorkteams",
		"DescribeSubscribedWorkteam",
	}
}

// dispatchHubAndLabelingOps combines the hub/hub-content and labeling-job/
// workteam-subscription dispatchers into a single call site so dispatchNewOps
// only grows by one branch per grouped family instead of one per family.
func (h *Handler) dispatchHubAndLabelingOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchHubOps(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchLabelingOps(ctx, op, body)
}

// dispatchLabelingOps dispatches labeling-job and workteam-subscription operations.
func (h *Handler) dispatchLabelingOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateLabelingJob":
		r, err := h.handleCreateLabelingJob(ctx, body)

		return r, true, err
	case "DescribeLabelingJob":
		r, err := h.handleDescribeLabelingJob(ctx, body)

		return r, true, err
	case "StopLabelingJob":
		r, err := h.handleStopLabelingJob(ctx, body)

		return r, true, err
	case "ListLabelingJobs":
		r, err := h.handleListLabelingJobs(ctx, body)

		return r, true, err
	case "ListLabelingJobsForWorkteam":
		r, err := h.handleListLabelingJobsForWorkteam(ctx, body)

		return r, true, err
	case "ListSubscribedWorkteams":
		r, err := h.handleListSubscribedWorkteams(ctx, body)

		return r, true, err
	case "DescribeSubscribedWorkteam":
		return nil, true, h.handleDescribeSubscribedWorkteam(ctx, body)
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// CreateLabelingJob
// ---------------------------------------------------------------------------

type createLabelingJobRequest struct {
	StoppingConditions          *LabelingJobStoppingConditions `json:"StoppingConditions"`
	LabelingJobAlgorithmsConfig *LabelingJobAlgorithmsConfig   `json:"LabelingJobAlgorithmsConfig"`
	InputConfig                 *LabelingJobInputConfig        `json:"InputConfig"`
	OutputConfig                *LabelingJobOutputConfig       `json:"OutputConfig"`
	HumanTaskConfig             *HumanTaskConfig               `json:"HumanTaskConfig"`
	LabelingJobName             string                         `json:"LabelingJobName"`
	LabelAttributeName          string                         `json:"LabelAttributeName"`
	RoleArn                     string                         `json:"RoleArn"`
	LabelCategoryConfigS3Uri    string                         `json:"LabelCategoryConfigS3Uri"`
	Tags                        []tagObject                    `json:"Tags"`
}

func (h *Handler) handleCreateLabelingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createLabelingJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LabelingJobName == "" {
		return nil, fmt.Errorf("%w: LabelingJobName is required", errInvalidRequest)
	}

	if req.InputConfig == nil {
		return nil, fmt.Errorf("%w: InputConfig is required", errInvalidRequest)
	}

	if req.OutputConfig == nil {
		return nil, fmt.Errorf("%w: OutputConfig is required", errInvalidRequest)
	}

	if req.HumanTaskConfig == nil {
		return nil, fmt.Errorf("%w: HumanTaskConfig is required", errInvalidRequest)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateLabelingJob(ctx, CreateLabelingJobOptions{
		LabelingJobName:             req.LabelingJobName,
		LabelAttributeName:          req.LabelAttributeName,
		RoleArn:                     req.RoleArn,
		LabelCategoryConfigS3Uri:    req.LabelCategoryConfigS3Uri,
		InputConfig:                 *req.InputConfig,
		OutputConfig:                *req.OutputConfig,
		HumanTaskConfig:             *req.HumanTaskConfig,
		StoppingConditions:          req.StoppingConditions,
		LabelingJobAlgorithmsConfig: req.LabelingJobAlgorithmsConfig,
		Tags:                        fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyLabelingJobArn: result.LabelingJobArn})
}

// ---------------------------------------------------------------------------
// DescribeLabelingJob
// ---------------------------------------------------------------------------

// labelingJobResponseMap builds the AWS wire representation of a LabelingJob,
// converting timestamps to epoch seconds as required by the aws-json-1.1 protocol.
func labelingJobResponseMap(j *LabelingJob) map[string]any {
	resp := map[string]any{
		"LabelingJobName":   j.LabelingJobName,
		keyLabelingJobArn:   j.LabelingJobArn,
		"LabelingJobStatus": j.LabelingJobStatus,
		keyCreationTime:     epochSeconds(j.CreationTime),
		keyLastModifiedTime: epochSeconds(j.LastModifiedTime),
		"JobReferenceCode":  j.JobReferenceCode,
		keyRoleArn:          j.RoleArn,
		"LabelCounters":     j.LabelCounters,
		"InputConfig":       j.InputConfig,
		"OutputConfig":      j.OutputConfig,
		"HumanTaskConfig":   j.HumanTaskConfig,
	}

	if j.LabelAttributeName != "" {
		resp["LabelAttributeName"] = j.LabelAttributeName
	}

	if j.LabelCategoryConfigS3Uri != "" {
		resp["LabelCategoryConfigS3Uri"] = j.LabelCategoryConfigS3Uri
	}

	if j.FailureReason != "" {
		resp["FailureReason"] = j.FailureReason
	}

	if j.LabelingJobOutput != nil {
		resp["LabelingJobOutput"] = j.LabelingJobOutput
	}

	if j.StoppingConditions != nil {
		resp["StoppingConditions"] = j.StoppingConditions
	}

	if j.LabelingJobAlgorithmsConfig != nil {
		resp["LabelingJobAlgorithmsConfig"] = j.LabelingJobAlgorithmsConfig
	}

	return resp
}

func (h *Handler) handleDescribeLabelingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		LabelingJobName string `json:"LabelingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LabelingJobName == "" {
		return nil, fmt.Errorf("%w: LabelingJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeLabelingJob(ctx, req.LabelingJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(labelingJobResponseMap(result))
}

// ---------------------------------------------------------------------------
// StopLabelingJob
// ---------------------------------------------------------------------------

func (h *Handler) handleStopLabelingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		LabelingJobName string `json:"LabelingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LabelingJobName == "" {
		return nil, fmt.Errorf("%w: LabelingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopLabelingJob(ctx, req.LabelingJobName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ListLabelingJobs
// ---------------------------------------------------------------------------

type labelingJobSummary struct {
	LabelingJobName                  string        `json:"LabelingJobName"`
	LabelingJobArn                   string        `json:"LabelingJobArn"`
	LabelingJobStatus                string        `json:"LabelingJobStatus"`
	WorkteamArn                      string        `json:"WorkteamArn"`
	AnnotationConsolidationLambdaArn string        `json:"AnnotationConsolidationLambdaArn,omitempty"`
	FailureReason                    string        `json:"FailureReason,omitempty"`
	PreHumanTaskLambdaArn            string        `json:"PreHumanTaskLambdaArn,omitempty"`
	CreationTime                     float64       `json:"CreationTime"`
	LastModifiedTime                 float64       `json:"LastModifiedTime"`
	LabelCounters                    LabelCounters `json:"LabelCounters"`
}

func (h *Handler) handleListLabelingJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		NameContains string `json:"NameContains"`
		StatusEquals string `json:"StatusEquals"`
		MaxResults   int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, next := h.Backend.ListLabelingJobs(ctx, req.NextToken, ListLabelingJobsFilter{
		NameContains: req.NameContains,
		StatusEquals: req.StatusEquals,
		MaxResults:   req.MaxResults,
	})

	summaries := make([]labelingJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, labelingJobSummary{
			LabelingJobName:                  j.LabelingJobName,
			LabelingJobArn:                   j.LabelingJobArn,
			LabelingJobStatus:                j.LabelingJobStatus,
			WorkteamArn:                      j.HumanTaskConfig.WorkteamArn,
			CreationTime:                     epochSeconds(j.CreationTime),
			LastModifiedTime:                 epochSeconds(j.LastModifiedTime),
			LabelCounters:                    j.LabelCounters,
			AnnotationConsolidationLambdaArn: j.HumanTaskConfig.AnnotationConsolidationLambdaArn,
			FailureReason:                    j.FailureReason,
			PreHumanTaskLambdaArn:            j.HumanTaskConfig.PreHumanTaskLambdaArn,
		})
	}

	resp := map[string]any{"LabelingJobSummaryList": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// ListLabelingJobsForWorkteam
// ---------------------------------------------------------------------------

type labelingJobForWorkteamSummary struct {
	JobReferenceCode                  string                   `json:"JobReferenceCode"`
	WorkRequesterAccountID            string                   `json:"WorkRequesterAccountId"`
	LabelingJobName                   string                   `json:"LabelingJobName,omitempty"`
	CreationTime                      float64                  `json:"CreationTime"`
	LabelCounters                     LabelCountersForWorkteam `json:"LabelCounters"`
	NumberOfHumanWorkersPerDataObject int32                    `json:"NumberOfHumanWorkersPerDataObject,omitempty"`
}

func (h *Handler) handleListLabelingJobsForWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamArn string `json:"WorkteamArn"`
		NextToken   string `json:"NextToken"`
		MaxResults  int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamArn == "" {
		return nil, fmt.Errorf("%w: WorkteamArn is required", errInvalidRequest)
	}

	jobs, next := h.Backend.ListLabelingJobsForWorkteam(ctx, req.WorkteamArn, req.NextToken, req.MaxResults)

	summaries := make([]labelingJobForWorkteamSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, labelingJobForWorkteamSummary{
			JobReferenceCode:                  j.JobReferenceCode,
			WorkRequesterAccountID:            h.Backend.accountID,
			LabelingJobName:                   j.LabelingJobName,
			CreationTime:                      epochSeconds(j.CreationTime),
			NumberOfHumanWorkersPerDataObject: j.HumanTaskConfig.NumberOfHumanWorkersPerDataObject,
			LabelCounters: LabelCountersForWorkteam{
				HumanLabeled: j.LabelCounters.HumanLabeled,
				PendingHuman: j.LabelCounters.Unlabeled,
				Total:        j.LabelCounters.TotalLabeled + j.LabelCounters.Unlabeled,
			},
		})
	}

	resp := map[string]any{"LabelingJobSummaryList": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// ListSubscribedWorkteams / DescribeSubscribedWorkteam
//
// Amazon Web Services Marketplace vendor work teams are not modeled by this
// emulator — there is no CreateSubscribedWorkteam API, subscriptions are
// created out-of-band via the Marketplace console. ListSubscribedWorkteams
// therefore always returns an empty list, and DescribeSubscribedWorkteam
// always reports the requested workteam as not found.
// ---------------------------------------------------------------------------

func (h *Handler) handleListSubscribedWorkteams(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		NameContains string `json:"NameContains"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]any{"SubscribedWorkteams": []any{}})
}

func (h *Handler) handleDescribeSubscribedWorkteam(_ context.Context, body []byte) error {
	var req struct {
		WorkteamArn string `json:"WorkteamArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamArn == "" {
		return fmt.Errorf("%w: WorkteamArn is required", errInvalidRequest)
	}

	return fmt.Errorf(
		"%w: subscribed workteam %q not found", ErrSubscribedWorkteamNotFound, req.WorkteamArn,
	)
}
