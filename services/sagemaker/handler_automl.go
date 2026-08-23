package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// AutoMLJob handlers
// ---------------------------------------------------------------------------

type createAutoMLJobRequest struct {
	Tags               []tagObject             `json:"Tags"`
	OutputDataConfig   *AutoMLOutputDataConfig `json:"OutputDataConfig"`
	AutoMLJobObjective *AutoMLJobObjective     `json:"AutoMLJobObjective"`
	ModelDeployConfig  *ModelDeployConfig      `json:"ModelDeployConfig,omitempty"`
	AutoMLJobName      string                  `json:"AutoMLJobName"`
	RoleArn            string                  `json:"RoleArn"`
	InputDataConfig    []AutoMLChannel         `json:"InputDataConfig"`
}

func (h *Handler) handleCreateAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createAutoMLJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if len(req.InputDataConfig) == 0 {
		return nil, fmt.Errorf("%w: InputDataConfig is required", errInvalidRequest)
	}

	if req.OutputDataConfig == nil {
		return nil, fmt.Errorf("%w: OutputDataConfig is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAutoMLJob(ctx, req.AutoMLJobName, req.RoleArn, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	if extErr := h.Backend.SetAutoMLJobExtras(
		ctx,
		req.AutoMLJobName,
		req.OutputDataConfig,
		req.AutoMLJobObjective,
		req.InputDataConfig,
		req.ModelDeployConfig,
	); extErr != nil {
		return nil, extErr
	}

	return json.Marshal(map[string]any{keyAutoMLJobArn: result.AutoMLJobArn})
}

type describeAutoMLJobRequest struct {
	AutoMLJobName string `json:"AutoMLJobName"`
}

func (h *Handler) handleDescribeAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeAutoMLJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeAutoMLJob(ctx, req.AutoMLJobName)
	if err != nil {
		return nil, err
	}

	inputDataConfig := j.InputDataConfig
	if inputDataConfig == nil {
		inputDataConfig = []AutoMLChannel{}
	}

	resp := map[string]any{
		keyAutoMLJobArn:             j.AutoMLJobArn,
		keyAutoMLJobName:            j.AutoMLJobName,
		keyAutoMLJobStatus:          j.AutoMLJobStatus,
		keyAutoMLJobSecondaryStatus: j.AutoMLJobSecondaryStatus,
		keyRoleArn:                  j.RoleArn,
		keyCreationTime:             epochSeconds(j.CreationTime),
		keyLastModifiedTime:         epochSeconds(j.LastModifiedTime),
		"InputDataConfig":           inputDataConfig,
	}

	if j.OutputDataConfig != nil {
		resp["OutputDataConfig"] = j.OutputDataConfig
	}

	if j.AutoMLJobObjective != nil {
		resp["AutoMLJobObjective"] = j.AutoMLJobObjective
	}

	if j.ModelDeployConfig != nil {
		resp["ModelDeployConfig"] = j.ModelDeployConfig
	}

	return json.Marshal(resp)
}

type stopAutoMLJobRequest struct {
	AutoMLJobName string `json:"AutoMLJobName"`
}

func (h *Handler) handleStopAutoMLJob(ctx context.Context, body []byte) error {
	var req stopAutoMLJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	return h.Backend.StopAutoMLJob(ctx, req.AutoMLJobName)
}

type listAutoMLJobsRequest struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	NameContains           string   `json:"NameContains,omitempty"`
	StatusEquals           string   `json:"StatusEquals,omitempty"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	MaxResults             int32    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListAutoMLJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listAutoMLJobsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListAutoMLJobs(ctx, req.NextToken, ListAutoMLJobsFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summaries = append(summaries, map[string]any{
			keyAutoMLJobName:            j.AutoMLJobName,
			keyAutoMLJobArn:             j.AutoMLJobArn,
			keyAutoMLJobStatus:          j.AutoMLJobStatus,
			keyAutoMLJobSecondaryStatus: j.AutoMLJobSecondaryStatus,
			keyCreationTime:             epochSeconds(j.CreationTime),
			keyLastModifiedTime:         epochSeconds(j.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"AutoMLJobSummaries": summaries,
		keyNextToken:         next,
	})
}
