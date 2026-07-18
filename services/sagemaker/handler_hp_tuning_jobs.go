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

type createHPTuningJobRequest struct {
	HyperParameterTuningJobConfig struct {
		Strategy string `json:"Strategy"`
	} `json:"HyperParameterTuningJobConfig"`
	HyperParameterTuningJobName string      `json:"HyperParameterTuningJobName"`
	Tags                        []tagObject `json:"Tags"`
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

	j, err := h.Backend.CreateHyperParameterTuningJob(
		ctx,
		req.HyperParameterTuningJobName,
		req.HyperParameterTuningJobConfig.Strategy,
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
		"Strategy":                      j.Strategy,
		keyCreationTime:                 epochSeconds(j.CreationTime),
		keyLastModifiedTime:             epochSeconds(j.LastModifiedTime),
	})
}

type hpTuningJobSummary struct {
	HyperParameterTuningJobName   string  `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobArn    string  `json:"HyperParameterTuningJobArn"`
	HyperParameterTuningJobStatus string  `json:"HyperParameterTuningJobStatus"`
	Strategy                      string  `json:"Strategy,omitempty"`
	CreationTime                  float64 `json:"CreationTime"`
	LastModifiedTime              float64 `json:"LastModifiedTime"`
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

func (h *Handler) handleListTrainingJobsForHyperParameterTuningJob(ctx context.Context, body []byte) ([]byte, error) {
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

	jobs, _, err := h.Backend.ListTrainingJobsForHyperParameterTuningJob(
		ctx,
		req.HyperParameterTuningJobName,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	summaries := make([]any, 0, len(jobs))

	return json.Marshal(map[string]any{"TrainingJobSummaries": summaries})
}
