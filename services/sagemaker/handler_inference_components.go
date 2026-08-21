package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// InferenceComponent handlers
// ---------------------------------------------------------------------------

// createInferenceComponentInput mirrors CreateInferenceComponentInput
// (api_op_CreateInferenceComponent.go:68-116).
type createInferenceComponentInput struct {
	RuntimeConfig          *InferenceComponentRuntimeConfigInput `json:"RuntimeConfig"`
	InferenceComponentName string                                `json:"InferenceComponentName"`
	EndpointName           string                                `json:"EndpointName"`
	VariantName            string                                `json:"VariantName"`
	Specification          *InferenceComponentSpecification      `json:"Specification"`
	Tags                   []tagObject                           `json:"Tags"`
	Specifications         []InferenceComponentSpecification     `json:"Specifications"`
}

func (h *Handler) handleCreateInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req createInferenceComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.InferenceComponentName == "" {
		return nil, fmt.Errorf("%w: InferenceComponentName is required", errInvalidRequest)
	}

	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	var copyCount int32
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	c, err := h.Backend.CreateInferenceComponent(ctx, CreateInferenceComponentOptions{
		InferenceComponentName: req.InferenceComponentName,
		EndpointName:           req.EndpointName,
		VariantName:            req.VariantName,
		CopyCount:              copyCount,
		Specification:          req.Specification,
		Specifications:         req.Specifications,
		Tags:                   fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

// inferenceComponentResponseMap builds the DescribeInferenceComponentOutput
// wire shape (api_op_DescribeInferenceComponent.go:39-97). Tags is
// deliberately absent — DescribeInferenceComponentOutput has no such member;
// the previous version of this handler marshaled the internal storage struct
// directly and leaked it onto the wire.
func inferenceComponentResponseMap(c *InferenceComponent) map[string]any {
	resp := map[string]any{
		keyCreationTime:          epochSeconds(c.CreationTime),
		keyEndpointArn:           c.EndpointArn,
		keyEndpointNameField:     c.EndpointName,
		keyInferenceComponentArn: c.InferenceComponentArn,
		"InferenceComponentName": c.InferenceComponentName,
		keyLastModifiedTime:      epochSeconds(c.LastModifiedTime),
	}

	if c.VariantName != "" {
		resp["VariantName"] = c.VariantName
	}

	if c.InferenceComponentStatus != "" {
		resp["InferenceComponentStatus"] = c.InferenceComponentStatus
	}

	if c.FailureReason != "" {
		resp["FailureReason"] = c.FailureReason
	}

	if c.CopyCount > 0 {
		resp["RuntimeConfig"] = map[string]any{
			"CurrentCopyCount": c.CurrentCopyCount,
			"DesiredCopyCount": c.CopyCount,
		}
	}

	if c.Specification != nil {
		resp["Specification"] = inferenceComponentSpecificationSummary(c.Specification)
	}

	if len(c.Specifications) > 0 {
		summaries := make([]map[string]any, len(c.Specifications))
		for i := range c.Specifications {
			summaries[i] = inferenceComponentSpecificationSummary(&c.Specifications[i])
		}

		resp["Specifications"] = summaries
	}

	if c.DeploymentConfig != nil {
		resp["LastDeploymentConfig"] = c.DeploymentConfig
	}

	return resp
}

// describeInferenceComponentInput mirrors DescribeInferenceComponentInput
// (api_op_DescribeInferenceComponent.go:29-37).
type describeInferenceComponentInput struct {
	InferenceComponentName string `json:"InferenceComponentName"`
}

func (h *Handler) handleDescribeInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req describeInferenceComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(inferenceComponentResponseMap(c))
}

// listInferenceComponentsInput mirrors ListInferenceComponentsInput
// (api_op_ListInferenceComponents.go:30-71).
type listInferenceComponentsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	EndpointNameEquals     string   `json:"EndpointNameEquals"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	StatusEquals           string   `json:"StatusEquals"`
	VariantNameEquals      string   `json:"VariantNameEquals"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListInferenceComponents(ctx context.Context, body []byte) ([]byte, error) {
	var req listInferenceComponentsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	components, next := h.Backend.ListInferenceComponents(ctx, ListInferenceComponentsFilter{
		EndpointNameEquals:     req.EndpointNameEquals,
		NameContains:           req.NameContains,
		StatusEquals:           req.StatusEquals,
		VariantNameEquals:      req.VariantNameEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		NextToken:              req.NextToken,
		MaxResults:             req.MaxResults,
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
	})

	items := make([]map[string]any, 0, len(components))
	for _, c := range components {
		item := map[string]any{
			"InferenceComponentName": c.InferenceComponentName,
			keyInferenceComponentArn: c.InferenceComponentArn,
			keyEndpointArn:           c.EndpointArn,
			keyEndpointNameField:     c.EndpointName,
			"VariantName":            c.VariantName,
			keyCreationTime:          epochSeconds(c.CreationTime),
			keyLastModifiedTime:      epochSeconds(c.LastModifiedTime),
		}

		if c.InferenceComponentStatus != "" {
			item["InferenceComponentStatus"] = c.InferenceComponentStatus
		}

		items = append(items, item)
	}

	return listResp("InferenceComponents", items, next)
}

// updateInferenceComponentInput mirrors UpdateInferenceComponentInput
// (api_op_UpdateInferenceComponent.go:24-53). The real op has no VariantName
// member — a production variant is fixed at Create time.
type updateInferenceComponentInput struct {
	DeploymentConfig       json.RawMessage                       `json:"DeploymentConfig"`
	RuntimeConfig          *InferenceComponentRuntimeConfigInput `json:"RuntimeConfig"`
	InferenceComponentName string                                `json:"InferenceComponentName"`
	Specification          *InferenceComponentSpecification      `json:"Specification"`
	Specifications         []InferenceComponentSpecification     `json:"Specifications"`
}

func (h *Handler) handleUpdateInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req updateInferenceComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.InferenceComponentName == "" {
		return nil, fmt.Errorf("%w: InferenceComponentName is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateInferenceComponent(ctx, req.InferenceComponentName, UpdateInferenceComponentOptions{
		RuntimeConfig:    req.RuntimeConfig,
		Specification:    req.Specification,
		Specifications:   req.Specifications,
		DeploymentConfig: req.DeploymentConfig,
	}); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

// updateInferenceComponentRuntimeConfigInput mirrors
// UpdateInferenceComponentRuntimeConfigInput (api_op_UpdateInferenceComponentRuntimeConfig.go:24-38).
type updateInferenceComponentRuntimeConfigInput struct {
	DesiredRuntimeConfig   *InferenceComponentRuntimeConfigInput `json:"DesiredRuntimeConfig"`
	InferenceComponentName string                                `json:"InferenceComponentName"`
}

func (h *Handler) handleUpdateInferenceComponentRuntimeConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req updateInferenceComponentRuntimeConfigInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.InferenceComponentName == "" {
		return nil, fmt.Errorf("%w: InferenceComponentName is required", errInvalidRequest)
	}

	if req.DesiredRuntimeConfig == nil {
		return nil, fmt.Errorf("%w: DesiredRuntimeConfig is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateInferenceComponentRuntimeConfig(
		ctx, req.InferenceComponentName, req.DesiredRuntimeConfig.CopyCount,
	); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

// deleteInferenceComponentInput mirrors DeleteInferenceComponentInput
// (api_op_DeleteInferenceComponent.go:29-37).
type deleteInferenceComponentInput struct {
	InferenceComponentName string `json:"InferenceComponentName"`
}

func (h *Handler) handleDeleteInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteInferenceComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteInferenceComponent(ctx, req.InferenceComponentName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
