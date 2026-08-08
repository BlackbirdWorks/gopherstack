package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// AutoMLJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string       `json:"Tags"`
		OutputDataConfig   *AutoMLOutputDataConfig `json:"OutputDataConfig"`
		AutoMLJobObjective *AutoMLJobObjective     `json:"AutoMLJobObjective"`
		AutoMLJobName      string                  `json:"AutoMLJobName"`
		RoleArn            string                  `json:"RoleArn"`
		InputDataConfig    []AutoMLChannel         `json:"InputDataConfig"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAutoMLJob(ctx, req.AutoMLJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	if req.OutputDataConfig != nil || req.AutoMLJobObjective != nil || req.InputDataConfig != nil {
		if extErr := h.Backend.SetAutoMLJobExtras(
			ctx, req.AutoMLJobName, req.OutputDataConfig, req.AutoMLJobObjective, req.InputDataConfig,
		); extErr != nil {
			return nil, extErr
		}
	}

	return json.Marshal(map[string]any{"AutoMLJobArn": result.AutoMLJobArn})
}

func (h *Handler) handleDescribeAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAutoMLJob(ctx, req.AutoMLJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopAutoMLJob(ctx context.Context, body []byte) error {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	return h.Backend.StopAutoMLJob(ctx, req.AutoMLJobName)
}

func (h *Handler) handleListAutoMLJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListAutoMLJobs(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summaries = append(summaries, map[string]any{
			"AutoMLJobName":            j.AutoMLJobName,
			"AutoMLJobArn":             j.AutoMLJobArn,
			"AutoMLJobStatus":          j.AutoMLJobStatus,
			"AutoMLJobSecondaryStatus": j.AutoMLJobSecondaryStatus,
			keyCreationTime:            epochSeconds(j.CreationTime),
			keyLastModifiedTime:        epochSeconds(j.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"AutoMLJobSummaries": summaries,
		keyNextToken:         next,
	})
}
