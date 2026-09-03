package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createEndpointConfigRequest is the request body for CreateEndpointConfig.
type createEndpointConfigRequest struct {
	DataCaptureConfig        *DataCaptureConfig    `json:"DataCaptureConfig,omitempty"`
	AsyncInferenceConfig     *AsyncInferenceConfig `json:"AsyncInferenceConfig,omitempty"`
	VpcConfig                *VpcConfig            `json:"VpcConfig,omitempty"`
	ExplainerConfig          json.RawMessage       `json:"ExplainerConfig,omitempty"`
	MetricsConfig            json.RawMessage       `json:"MetricsConfig,omitempty"`
	EndpointConfigName       string                `json:"EndpointConfigName"`
	ExecutionRoleArn         string                `json:"ExecutionRoleArn,omitempty"`
	KmsKeyID                 string                `json:"KmsKeyId,omitempty"`
	Tags                     []tagObject           `json:"Tags"`
	ProductionVariants       []ProductionVariant   `json:"ProductionVariants"`
	ShadowProductionVariants []ProductionVariant   `json:"ShadowProductionVariants,omitempty"`
	EnableNetworkIsolation   bool                  `json:"EnableNetworkIsolation,omitempty"`
}

// endpointConfigSummary is a summary of an endpoint config for list responses.
type endpointConfigSummary struct {
	EndpointConfigArn  string  `json:"EndpointConfigArn"`
	EndpointConfigName string  `json:"EndpointConfigName"`
	CreationTime       float64 `json:"CreationTime"`
}

// describeEndpointConfigResponse is the response body for DescribeEndpointConfig.
type describeEndpointConfigResponse struct {
	DataCaptureConfig        *DataCaptureConfig    `json:"DataCaptureConfig,omitempty"`
	AsyncInferenceConfig     *AsyncInferenceConfig `json:"AsyncInferenceConfig,omitempty"`
	VpcConfig                *VpcConfig            `json:"VpcConfig,omitempty"`
	ExplainerConfig          json.RawMessage       `json:"ExplainerConfig,omitempty"`
	MetricsConfig            json.RawMessage       `json:"MetricsConfig,omitempty"`
	EndpointConfigArn        string                `json:"EndpointConfigArn"`
	EndpointConfigName       string                `json:"EndpointConfigName"`
	ExecutionRoleArn         string                `json:"ExecutionRoleArn,omitempty"`
	KmsKeyID                 string                `json:"KmsKeyId,omitempty"`
	ProductionVariants       []ProductionVariant   `json:"ProductionVariants"`
	ShadowProductionVariants []ProductionVariant   `json:"ShadowProductionVariants,omitempty"`
	CreationTime             float64               `json:"CreationTime"`
	EnableNetworkIsolation   bool                  `json:"EnableNetworkIsolation,omitempty"`
}

func (h *Handler) handleCreateEndpointConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req createEndpointConfigRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	if len(req.ProductionVariants) == 0 {
		return nil, fmt.Errorf(
			"%w: At least one ProductionVariant must be specified", errInvalidRequest,
		)
	}

	tags := fromTagObjects(req.Tags)

	ec, err := h.Backend.CreateEndpointConfig(ctx, req.EndpointConfigName, req.ProductionVariants, tags)
	if err != nil {
		return nil, err
	}

	hasExtras := req.DataCaptureConfig != nil || req.AsyncInferenceConfig != nil ||
		req.VpcConfig != nil || req.ExecutionRoleArn != "" || req.KmsKeyID != "" ||
		len(req.ShadowProductionVariants) > 0 || req.EnableNetworkIsolation ||
		len(req.ExplainerConfig) > 0 || len(req.MetricsConfig) > 0

	if hasExtras {
		if extErr := h.Backend.SetEndpointConfigExtras(
			ctx,
			req.EndpointConfigName,
			req.DataCaptureConfig,
			req.AsyncInferenceConfig,
			req.VpcConfig,
			req.ExecutionRoleArn,
			req.KmsKeyID,
			req.ShadowProductionVariants,
			req.EnableNetworkIsolation,
			req.ExplainerConfig,
			req.MetricsConfig,
		); extErr != nil {
			return nil, extErr
		}
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created endpoint config",
		"name",
		ec.EndpointConfigName,
		"arn",
		ec.EndpointConfigARN,
	)

	return json.Marshal(map[string]string{"EndpointConfigArn": ec.EndpointConfigARN})
}

// describeEndpointConfigRequest is the request body for DescribeEndpointConfig.
type describeEndpointConfigRequest struct {
	EndpointConfigName string `json:"EndpointConfigName"`
}

func (h *Handler) handleDescribeEndpointConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req describeEndpointConfigRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	ec, err := h.Backend.DescribeEndpointConfig(ctx, req.EndpointConfigName)
	if err != nil {
		return nil, err
	}

	resp := describeEndpointConfigResponse{
		EndpointConfigName:       ec.EndpointConfigName,
		EndpointConfigArn:        ec.EndpointConfigARN,
		ProductionVariants:       ec.ProductionVariants,
		CreationTime:             epochSeconds(ec.CreationTime),
		DataCaptureConfig:        ec.DataCaptureConfig,
		AsyncInferenceConfig:     ec.AsyncInferenceConfig,
		VpcConfig:                ec.VpcConfig,
		ExplainerConfig:          ec.ExplainerConfig,
		MetricsConfig:            ec.MetricsConfig,
		ExecutionRoleArn:         ec.ExecutionRoleArn,
		KmsKeyID:                 ec.KmsKeyID,
		ShadowProductionVariants: ec.ShadowProductionVariants,
		EnableNetworkIsolation:   ec.EnableNetworkIsolation,
	}

	if len(resp.ProductionVariants) == 0 {
		resp.ProductionVariants = nil
	}

	if len(resp.ShadowProductionVariants) == 0 {
		resp.ShadowProductionVariants = nil
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListEndpointConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req nameTimeListRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := req.toFilter()
	// ListEndpointConfigsInput.CreationTimeAfter's own doc: "a creation time
	// greater than or equal to the specified time" -- inclusive, unlike this
	// family's shared default.
	filter.AfterInclusive = true

	configs, nextToken := h.Backend.ListEndpointConfigs(ctx, req.NextToken, filter)
	summaries := make([]endpointConfigSummary, 0, len(configs))

	for _, ec := range configs {
		summaries = append(summaries, endpointConfigSummary{
			EndpointConfigName: ec.EndpointConfigName,
			EndpointConfigArn:  ec.EndpointConfigARN,
			CreationTime:       epochSeconds(ec.CreationTime),
		})
	}

	resp := map[string]any{"EndpointConfigs": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteEndpointConfigRequest is the request body for DeleteEndpointConfig.
type deleteEndpointConfigRequest struct {
	EndpointConfigName string `json:"EndpointConfigName"`
}

func (h *Handler) handleDeleteEndpointConfig(ctx context.Context, body []byte) error {
	var req deleteEndpointConfigRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteEndpointConfig(ctx, req.EndpointConfigName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted endpoint config", "name", req.EndpointConfigName)

	return nil
}
