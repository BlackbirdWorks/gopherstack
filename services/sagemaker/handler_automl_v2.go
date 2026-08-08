package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// AutoMLJob V2 handlers — CreateAutoMLJobV2/DescribeAutoMLJobV2 have a
// materially different required-field wire shape from V1 (AutoMLJobInputDataConfig
// vs InputDataConfig, required AutoMLProblemTypeConfig union vs optional
// ProblemType/AutoMLJobConfig) and so cannot honestly share handleCreateAutoMLJob/
// handleDescribeAutoMLJob. Both versions store into the same b.autoMLJobs table
// (AWS job names are unique across V1/V2), but each op parses/emits its own
// accurate subset of AutoMLJob's fields.
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAutoMLJobV2(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                     map[string]string       `json:"Tags"`
		AutoMLProblemTypeConfig  json.RawMessage         `json:"AutoMLProblemTypeConfig"`
		OutputDataConfig         *AutoMLOutputDataConfig `json:"OutputDataConfig"`
		AutoMLJobObjective       *AutoMLJobObjective     `json:"AutoMLJobObjective"`
		AutoMLComputeConfig      *AutoMLComputeConfig    `json:"AutoMLComputeConfig"`
		DataSplitConfig          *AutoMLDataSplitConfig  `json:"DataSplitConfig"`
		SecurityConfig           *AutoMLSecurityConfig   `json:"SecurityConfig"`
		ModelDeployConfig        *ModelDeployConfig      `json:"ModelDeployConfig"`
		AutoMLJobName            string                  `json:"AutoMLJobName"`
		RoleArn                  string                  `json:"RoleArn"`
		AutoMLJobInputDataConfig []AutoMLJobChannel      `json:"AutoMLJobInputDataConfig"`
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

	extErr := h.Backend.SetAutoMLJobV2Extras(ctx, req.AutoMLJobName, AutoMLJobV2Extras{
		AutoMLJobInputDataConfig: req.AutoMLJobInputDataConfig,
		AutoMLProblemTypeConfig:  req.AutoMLProblemTypeConfig,
		OutputDataConfig:         req.OutputDataConfig,
		AutoMLJobObjective:       req.AutoMLJobObjective,
		AutoMLComputeConfig:      req.AutoMLComputeConfig,
		DataSplitConfig:          req.DataSplitConfig,
		SecurityConfig:           req.SecurityConfig,
		ModelDeployConfig:        req.ModelDeployConfig,
	})
	if extErr != nil {
		return nil, extErr
	}

	return json.Marshal(map[string]any{keyAutoMLJobArn: result.AutoMLJobArn})
}

func (h *Handler) handleDescribeAutoMLJobV2(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

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

	inputDataConfig := j.AutoMLJobInputDataConfig
	if inputDataConfig == nil {
		inputDataConfig = []AutoMLJobChannel{}
	}

	resp := map[string]any{
		keyAutoMLJobArn:             j.AutoMLJobArn,
		keyAutoMLJobName:            j.AutoMLJobName,
		keyAutoMLJobStatus:          j.AutoMLJobStatus,
		keyAutoMLJobSecondaryStatus: j.AutoMLJobSecondaryStatus,
		keyRoleArn:                  j.RoleArn,
		keyCreationTime:             epochSeconds(j.CreationTime),
		keyLastModifiedTime:         epochSeconds(j.LastModifiedTime),
		"AutoMLJobInputDataConfig":  inputDataConfig,
	}

	if j.OutputDataConfig != nil {
		resp["OutputDataConfig"] = j.OutputDataConfig
	}

	if j.AutoMLProblemTypeConfig != nil {
		resp["AutoMLProblemTypeConfig"] = j.AutoMLProblemTypeConfig

		if name := automlProblemTypeConfigName(j.AutoMLProblemTypeConfig); name != "" {
			resp["AutoMLProblemTypeConfigName"] = name
		}
	}

	if j.AutoMLJobObjective != nil {
		resp["AutoMLJobObjective"] = j.AutoMLJobObjective
	}

	if j.AutoMLComputeConfig != nil {
		resp["AutoMLComputeConfig"] = j.AutoMLComputeConfig
	}

	if j.DataSplitConfig != nil {
		resp["DataSplitConfig"] = j.DataSplitConfig
	}

	if j.SecurityConfig != nil {
		resp["SecurityConfig"] = j.SecurityConfig
	}

	if j.ModelDeployConfig != nil {
		resp["ModelDeployConfig"] = j.ModelDeployConfig
	}

	return json.Marshal(resp)
}
