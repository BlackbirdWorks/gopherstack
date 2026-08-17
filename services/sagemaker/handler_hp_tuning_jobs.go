package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// HyperParameterTuningJob handlers
// ---------------------------------------------------------------------------

type hpResourceLimitsRequest struct {
	MaxParallelTrainingJobs int32 `json:"MaxParallelTrainingJobs"`
	MaxNumberOfTrainingJobs int32 `json:"MaxNumberOfTrainingJobs,omitempty"`
	MaxRuntimeInSeconds     int32 `json:"MaxRuntimeInSeconds,omitempty"`
}

type createHPTuningJobRequest struct {
	Tags                          []tagObject `json:"Tags"`
	HyperParameterTuningJobName   string      `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobConfig struct {
		Strategy       string                  `json:"Strategy"`
		ResourceLimits hpResourceLimitsRequest `json:"ResourceLimits"`
	} `json:"HyperParameterTuningJobConfig"`
}

func (h *Handler) handleCreateHyperParameterTuningJob(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req createHPTuningJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)
	limits := HPResourceLimits{
		MaxParallelTrainingJobs: req.HyperParameterTuningJobConfig.ResourceLimits.MaxParallelTrainingJobs,
		MaxNumberOfTrainingJobs: req.HyperParameterTuningJobConfig.ResourceLimits.MaxNumberOfTrainingJobs,
		MaxRuntimeInSeconds:     req.HyperParameterTuningJobConfig.ResourceLimits.MaxRuntimeInSeconds,
	}

	j, err := h.Backend.CreateHyperParameterTuningJob(
		ctx,
		req.HyperParameterTuningJobName,
		req.HyperParameterTuningJobConfig.Strategy,
		limits,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created HP tuning job",
		"name",
		j.HyperParameterTuningJobName,
		"arn",
		j.HyperParameterTuningJobArn,
	)

	return json.Marshal(
		map[string]string{"HyperParameterTuningJobArn": j.HyperParameterTuningJobArn},
	)
}

func (h *Handler) handleDescribeHyperParameterTuningJob(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeHyperParameterTuningJob(ctx, req.HyperParameterTuningJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"HyperParameterTuningJobName":   j.HyperParameterTuningJobName,
		"HyperParameterTuningJobArn":    j.HyperParameterTuningJobArn,
		"HyperParameterTuningJobStatus": j.HyperParameterTuningJobStatus,
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy":       j.Strategy,
			"ResourceLimits": j.ResourceLimits,
		},
		"ObjectiveStatusCounters":   j.ObjectiveStatusCounters,
		"TrainingJobStatusCounters": j.TrainingJobStatusCounters,
		keyCreationTime:             epochSeconds(j.CreationTime),
		keyLastModifiedTime:         epochSeconds(j.LastModifiedTime),
	})
}

type hpTuningJobSummary struct {
	HyperParameterTuningJobName   string                      `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobArn    string                      `json:"HyperParameterTuningJobArn"`
	HyperParameterTuningJobStatus string                      `json:"HyperParameterTuningJobStatus"`
	Strategy                      string                      `json:"Strategy,omitempty"`
	ObjectiveStatusCounters       HPObjectiveStatusCounters   `json:"ObjectiveStatusCounters"`
	TrainingJobStatusCounters     HPTrainingJobStatusCounters `json:"TrainingJobStatusCounters"`
	ResourceLimits                HPResourceLimits            `json:"ResourceLimits"`
	CreationTime                  float64                     `json:"CreationTime"`
	LastModifiedTime              float64                     `json:"LastModifiedTime"`
}

func (h *Handler) handleListHyperParameterTuningJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListHyperParameterTuningJobs(ctx, req.NextToken)
	summaries := make([]hpTuningJobSummary, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, hpTuningJobSummary{
			HyperParameterTuningJobName:   j.HyperParameterTuningJobName,
			HyperParameterTuningJobArn:    j.HyperParameterTuningJobArn,
			HyperParameterTuningJobStatus: j.HyperParameterTuningJobStatus,
			Strategy:                      j.Strategy,
			ResourceLimits:                j.ResourceLimits,
			ObjectiveStatusCounters:       j.ObjectiveStatusCounters,
			TrainingJobStatusCounters:     j.TrainingJobStatusCounters,
			CreationTime:                  epochSeconds(j.CreationTime),
			LastModifiedTime:              epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"HyperParameterTuningJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleStopHyperParameterTuningJob(ctx context.Context, body []byte) error {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopHyperParameterTuningJob(ctx, req.HyperParameterTuningJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: stopped HP tuning job",
		"name",
		req.HyperParameterTuningJobName,
	)

	return nil
}

func (h *Handler) handleDeleteHyperParameterTuningJob(ctx context.Context, body []byte) error {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHyperParameterTuningJob(ctx, req.HyperParameterTuningJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: deleted HP tuning job",
		"name",
		req.HyperParameterTuningJobName,
	)

	return nil
}

type hyperParameterTrainingJobSummary struct {
	TrainingStartTime    *float64          `json:"TrainingStartTime,omitempty"`
	TrainingEndTime      *float64          `json:"TrainingEndTime,omitempty"`
	TunedHyperParameters map[string]string `json:"TunedHyperParameters,omitempty"`
	TrainingJobName      string            `json:"TrainingJobName"`
	TrainingJobArn       string            `json:"TrainingJobArn"`
	TuningJobName        string            `json:"TuningJobName,omitempty"`
	TrainingJobStatus    string            `json:"TrainingJobStatus"`
	FailureReason        string            `json:"FailureReason,omitempty"`
	CreationTime         float64           `json:"CreationTime"`
}

func (h *Handler) handleListTrainingJobsForHyperParameterTuningJob(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
		NextToken                   string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	jobs, nextToken, err := h.Backend.ListTrainingJobsForHyperParameterTuningJob(
		ctx,
		req.HyperParameterTuningJobName,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	summaries := make([]hyperParameterTrainingJobSummary, 0, len(jobs))
	for _, tj := range jobs {
		summaries = append(summaries, hyperParameterTrainingJobSummary{
			TrainingJobName:      tj.TrainingJobName,
			TrainingJobArn:       tj.TrainingJobArn,
			TuningJobName:        req.HyperParameterTuningJobName,
			CreationTime:         epochSeconds(tj.CreationTime),
			TrainingStartTime:    epochSecondsPtr(tj.TrainingStartTime),
			TrainingEndTime:      epochSecondsPtr(tj.TrainingEndTime),
			TrainingJobStatus:    tj.TrainingJobStatus,
			TunedHyperParameters: tj.HyperParameters,
			FailureReason:        tj.FailureReason,
		})
	}

	resp := map[string]any{"TrainingJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
