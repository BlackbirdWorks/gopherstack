package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// InferenceExperiment handlers
// ---------------------------------------------------------------------------

// createInferenceExperimentInput mirrors CreateInferenceExperimentInput
// (api_op_CreateInferenceExperiment.go:29-90): EndpointName/ModelVariants/
// Name/RoleArn/ShadowModeConfig/Type are all required; DataStorageConfig/
// Description/KmsKey/Schedule/Tags are optional. Description is accepted on
// Create for wire-shape fidelity but not currently persisted separately —
// UpdateInferenceExperiment is the only path that sets it, matching this
// handler's pre-existing behavior of only ever setting Description via
// Update.
type createInferenceExperimentInput struct {
	DataStorageConfig *InferenceExperimentDataStorageConfig `json:"DataStorageConfig"`
	Schedule          *InferenceExperimentSchedule          `json:"Schedule"`
	ShadowModeConfig  *ShadowModeConfig                     `json:"ShadowModeConfig"`
	Name              string                                `json:"Name"`
	Type              string                                `json:"Type"`
	RoleArn           string                                `json:"RoleArn"`
	EndpointName      string                                `json:"EndpointName"`
	KmsKey            string                                `json:"KmsKey"`
	ModelVariants     []ModelVariantConfig                  `json:"ModelVariants"`
	Tags              []tagObject                           `json:"Tags"`
}

func (h *Handler) handleCreateInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req createInferenceExperimentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateInferenceExperiment(ctx, CreateInferenceExperimentOptions{
		Name:              req.Name,
		Type:              req.Type,
		RoleArn:           req.RoleArn,
		EndpointName:      req.EndpointName,
		ModelVariants:     req.ModelVariants,
		ShadowModeConfig:  req.ShadowModeConfig,
		DataStorageConfig: req.DataStorageConfig,
		Schedule:          req.Schedule,
		KmsKey:            req.KmsKey,
		Tags:              fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

// describeInferenceExperimentInput mirrors DescribeInferenceExperimentInput
// (api_op_DescribeInferenceExperiment.go:27-33): its sole member is required.
type describeInferenceExperimentInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDescribeInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req describeInferenceExperimentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// stopInferenceExperimentInput mirrors StopInferenceExperimentInput
// (api_op_StopInferenceExperiment.go:27-56): ModelVariantActions/Name are
// required; DesiredModelVariants/DesiredState/Reason are optional.
type stopInferenceExperimentInput struct {
	DesiredState         string               `json:"DesiredState"`
	Reason               string               `json:"Reason"`
	Name                 string               `json:"Name"`
	ModelVariantActions  map[string]string    `json:"ModelVariantActions"`
	DesiredModelVariants []ModelVariantConfig `json:"DesiredModelVariants"`
}

// handleStopInferenceExperiment previously discarded
// StopInferenceExperimentOutput.InferenceExperimentArn entirely (returned no
// body at all) even though it is a required response member
// (api_op_StopInferenceExperiment.go:59-65).
func (h *Handler) handleStopInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req stopInferenceExperimentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.StopInferenceExperiment(ctx, req.Name, StopInferenceExperimentOptions{
		ModelVariantActions:  req.ModelVariantActions,
		DesiredModelVariants: req.DesiredModelVariants,
		DesiredState:         req.DesiredState,
		Reason:               req.Reason,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

// deleteInferenceExperimentInput mirrors DeleteInferenceExperimentInput
// (api_op_DeleteInferenceExperiment.go:27-33): its sole member is required.
type deleteInferenceExperimentInput struct {
	Name string `json:"Name"`
}

// handleDeleteInferenceExperiment previously discarded
// DeleteInferenceExperimentOutput.InferenceExperimentArn entirely (returned
// no body at all) even though it is a required response member
// (api_op_DeleteInferenceExperiment.go:36-42).
func (h *Handler) handleDeleteInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteInferenceExperimentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	arn, err := h.Backend.DeleteInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: arn})
}

// startInferenceExperimentInput mirrors StartInferenceExperimentInput
// (api_op_StartInferenceExperiment.go:27-33): its sole member is required.
type startInferenceExperimentInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req startInferenceExperimentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.StartInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

// updateInferenceExperimentInput mirrors UpdateInferenceExperimentInput
// (api_op_UpdateInferenceExperiment.go:28-63): Name is required; every other
// member is optional.
type updateInferenceExperimentInput struct {
	DataStorageConfig *InferenceExperimentDataStorageConfig `json:"DataStorageConfig"`
	Schedule          *InferenceExperimentSchedule          `json:"Schedule"`
	ShadowModeConfig  *ShadowModeConfig                     `json:"ShadowModeConfig"`
	Name              string                                `json:"Name"`
	Description       string                                `json:"Description,omitempty"`
	ModelVariants     []ModelVariantConfig                  `json:"ModelVariants"`
}

func (h *Handler) handleUpdateInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req updateInferenceExperimentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateInferenceExperiment(ctx, req.Name, UpdateInferenceExperimentOptions{
		Description:       req.Description,
		DataStorageConfig: req.DataStorageConfig,
		Schedule:          req.Schedule,
		ShadowModeConfig:  req.ShadowModeConfig,
		ModelVariants:     req.ModelVariants,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

// listInferenceExperimentsInput mirrors ListInferenceExperimentsInput
// (api_op_ListInferenceExperiments.go:29-59): every member is optional. The
// four time filters are awsjson1.1 epoch-second numbers on the wire
// (confirmed by this campaign's repo-spanning time-decode bug, parity-16)
// — decoded as *float64, never *time.Time.
type listInferenceExperimentsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	StatusEquals           string   `json:"StatusEquals"`
	Type                   string   `json:"Type"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListInferenceExperiments(ctx context.Context, body []byte) ([]byte, error) {
	var req listInferenceExperimentsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListInferenceExperiments(ctx, ListInferenceExperimentsParams{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		StatusEquals:           req.StatusEquals,
		Type:                   req.Type,
		NextToken:              req.NextToken,
		MaxResults:             req.MaxResults,
	})

	items := make([]map[string]any, 0, len(exps))
	for _, e := range exps {
		items = append(items, map[string]any{
			keyGenericName:      e.Name,
			"Arn":               e.Arn,
			keyStatus:           e.Status,
			"Type":              e.Type,
			keyCreationTime:     epochSeconds(e.CreationTime),
			keyLastModifiedTime: epochSeconds(e.LastModifiedTime),
		})
	}

	return listResp("InferenceExperiments", items, nextToken)
}
