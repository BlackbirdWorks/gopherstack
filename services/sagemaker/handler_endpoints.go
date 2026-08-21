package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Expanded CreateEndpoint handler (uses FSM)
// ---------------------------------------------------------------------------

// createEndpointInput mirrors CreateEndpointInput (api_op_CreateEndpoint.go:1-49).
type createEndpointInput struct {
	DeploymentConfig   json.RawMessage `json:"DeploymentConfig"`
	EndpointName       string          `json:"EndpointName"`
	EndpointConfigName string          `json:"EndpointConfigName"`
	Tags               []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateEndpointFSM(ctx context.Context, body []byte) ([]byte, error) {
	var req createEndpointInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}
	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	ep, err := h.Backend.CreateEndpointFSM(ctx, CreateEndpointOptions{
		Name:               req.EndpointName,
		EndpointConfigName: req.EndpointConfigName,
		Tags:               fromTagObjects(req.Tags),
		DeploymentConfig:   req.DeploymentConfig,
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created endpoint (FSM)",
		"name",
		ep.EndpointName,
		"arn",
		ep.EndpointArn,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// updateEndpointInput mirrors UpdateEndpointInput (api_op_UpdateEndpoint.go:1-56).
type updateEndpointInput struct {
	DeploymentConfig                 json.RawMessage `json:"DeploymentConfig"`
	EndpointConfigName               string          `json:"EndpointConfigName"`
	EndpointName                     string          `json:"EndpointName"`
	ExcludeRetainedVariantProperties []string        `json:"ExcludeRetainedVariantProperties"`
	RetainAllVariantProperties       bool            `json:"RetainAllVariantProperties"`
	RetainDeploymentConfig           bool            `json:"RetainDeploymentConfig"`
}

// handleUpdateEndpointFSM replaces the basic UpdateEndpoint handler with one
// that properly drives Updating → InService via the lifecycle simulator.
func (h *Handler) handleUpdateEndpointFSM(ctx context.Context, body []byte) ([]byte, error) {
	var req updateEndpointInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpointFSM(ctx, req.EndpointName, UpdateEndpointOptions{
		EndpointConfigName:               req.EndpointConfigName,
		DeploymentConfig:                 req.DeploymentConfig,
		ExcludeRetainedVariantProperties: req.ExcludeRetainedVariantProperties,
		RetainAllVariantProperties:       req.RetainAllVariantProperties,
		RetainDeploymentConfig:           req.RetainDeploymentConfig,
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated endpoint (FSM)", "name", ep.EndpointName)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// UpdateEndpointWeightsAndCapacities (proper implementation, gap #10)
// ---------------------------------------------------------------------------

// updateEndpointWeightsAndCapacitiesInput mirrors UpdateEndpointWeightsAndCapacitiesInput
// (api_op_UpdateEndpointWeightsAndCapacities.go:24-38).
type updateEndpointWeightsAndCapacitiesInput struct {
	EndpointName                string                     `json:"EndpointName"`
	DesiredWeightsAndCapacities []DesiredWeightAndCapacity `json:"DesiredWeightsAndCapacities"`
}

func (h *Handler) handleUpdateEndpointWeightsAndCapacitiesFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req updateEndpointWeightsAndCapacitiesInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpointWeightsAndCapacitiesFull(
		ctx,
		req.EndpointName,
		req.DesiredWeightsAndCapacities,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: updated endpoint weights and capacities",
		"name",
		req.EndpointName,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// Endpoint handlers
// ---------------------------------------------------------------------------

// describeEndpointInput mirrors DescribeEndpointInput (api_op_DescribeEndpoint.go:29-37).
type describeEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

// dataCaptureConfigSummary builds the DataCaptureConfigSummary wire shape
// (types/types.go:6685-6715) from this backend's stored DataCaptureConfig
// (models.go:128-134). CaptureStatus mirrors EnableCapture — this backend
// never fails or pauses capture independently.
func dataCaptureConfigSummary(dcc *DataCaptureConfig) map[string]any {
	captureStatus := "Stopped"
	if dcc.EnableCapture {
		captureStatus = "Started"
	}

	return map[string]any{
		"CaptureStatus":             captureStatus,
		"CurrentSamplingPercentage": dcc.InitialSamplingPercentage,
		"DestinationS3Uri":          dcc.DestinationS3Uri,
		"EnableCapture":             dcc.EnableCapture,
		"KmsKeyId":                  dcc.KmsKeyID,
	}
}

// endpointResponseMap builds the DescribeEndpointOutput wire shape
// (api_op_DescribeEndpoint.go:39-97). ExplainerConfig/MetricsConfig have no
// counterpart on this service's EndpointConfig type, and
// PendingDeploymentSummary is not simulated — all three are disclosed no-ops
// rather than modeled.
func endpointResponseMap(ep *Endpoint) map[string]any {
	resp := map[string]any{
		keyEndpointNameField: ep.EndpointName,
		keyEndpointArn:       ep.EndpointArn,
		"EndpointConfigName": ep.EndpointConfigName,
		"EndpointStatus":     ep.EndpointStatus,
		keyCreationTime:      epochSeconds(ep.CreationTime),
		keyLastModifiedTime:  epochSeconds(ep.LastModifiedTime),
	}
	if ep.FailureReason != "" {
		resp["FailureReason"] = ep.FailureReason
	}
	if len(ep.ProductionVariants) > 0 {
		resp["ProductionVariants"] = ep.ProductionVariants
	}
	if len(ep.ShadowProductionVariants) > 0 {
		resp["ShadowProductionVariants"] = ep.ShadowProductionVariants
	}
	if ep.AsyncInferenceConfig != nil {
		resp["AsyncInferenceConfig"] = ep.AsyncInferenceConfig
	}
	if ep.DataCaptureConfig != nil {
		resp["DataCaptureConfig"] = dataCaptureConfigSummary(ep.DataCaptureConfig)
	}
	if ep.DeploymentConfig != nil {
		resp["LastDeploymentConfig"] = ep.DeploymentConfig
	}

	return resp
}

func (h *Handler) handleDescribeEndpoint(ctx context.Context, body []byte) ([]byte, error) {
	var req describeEndpointInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.DescribeEndpoint(ctx, req.EndpointName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(endpointResponseMap(ep))
}

type endpointSummary struct {
	EndpointName     string  `json:"EndpointName"`
	EndpointArn      string  `json:"EndpointArn"`
	EndpointStatus   string  `json:"EndpointStatus"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

// listEndpointsInput mirrors ListEndpointsInput (api_op_ListEndpoints.go:30-64).
type listEndpointsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	StatusEquals           string   `json:"StatusEquals"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListEndpoints(ctx context.Context, body []byte) ([]byte, error) {
	var req listEndpointsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	eps, nextToken := h.Backend.ListEndpoints(ctx, ListEndpointsFilter{
		NameContains:           req.NameContains,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		NextToken:              req.NextToken,
		MaxResults:             req.MaxResults,
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
	})
	summaries := make([]endpointSummary, 0, len(eps))

	for _, ep := range eps {
		summaries = append(summaries, endpointSummary{
			EndpointName:     ep.EndpointName,
			EndpointArn:      ep.EndpointArn,
			EndpointStatus:   ep.EndpointStatus,
			CreationTime:     epochSeconds(ep.CreationTime),
			LastModifiedTime: epochSeconds(ep.LastModifiedTime),
		})
	}

	resp := map[string]any{"Endpoints": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteEndpointInput mirrors DeleteEndpointInput (api_op_DeleteEndpoint.go:29-37).
type deleteEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

func (h *Handler) handleDeleteEndpoint(ctx context.Context, body []byte) error {
	var req deleteEndpointInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteEndpoint(ctx, req.EndpointName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted endpoint", "name", req.EndpointName)

	return nil
}
