package awsconfig

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for resource ops.
const (
	opBatchGetAggregateResourceConfig      = "BatchGetAggregateResourceConfig"
	opBatchGetResourceConfig               = "BatchGetResourceConfig"
	opDeleteResourceConfig                 = "DeleteResourceConfig"
	opGetAggregateDiscoveredResourceCounts = "GetAggregateDiscoveredResourceCounts"
	opGetAggregateResourceConfig           = "GetAggregateResourceConfig"
	opGetDiscoveredResourceCounts          = "GetDiscoveredResourceCounts"
	opGetResourceConfigHistory             = "GetResourceConfigHistory"
	opGetResourceEvaluationSummary         = "GetResourceEvaluationSummary"
	opListAggregateDiscoveredResources     = "ListAggregateDiscoveredResources"
	opListDiscoveredResources              = "ListDiscoveredResources"
	opListResourceEvaluations              = "ListResourceEvaluations"
	opPutResourceConfig                    = "PutResourceConfig"
	opSelectAggregateResourceConfig        = "SelectAggregateResourceConfig"
	opSelectResourceConfig                 = "SelectResourceConfig"
	opStartResourceEvaluation              = "StartResourceEvaluation"
)

// resourceSupportedOps returns the operation names this family handles.
func resourceSupportedOps() []string {
	return []string{
		opBatchGetAggregateResourceConfig,
		opBatchGetResourceConfig,
		opDeleteResourceConfig,
		opPutResourceConfig,
		opGetResourceConfigHistory,
		opGetDiscoveredResourceCounts,
		opGetAggregateDiscoveredResourceCounts,
		opGetAggregateResourceConfig,
		opListDiscoveredResources,
		opListAggregateDiscoveredResources,
		opSelectResourceConfig,
		opSelectAggregateResourceConfig,
		opGetResourceEvaluationSummary,
		opListResourceEvaluations,
		opStartResourceEvaluation,
	}
}

// BatchGetAggregateResourceConfig request/response types and handler.
type batchGetAggregateResourceConfigInput struct {
	ConfigurationAggregatorName string                        `json:"ConfigurationAggregatorName"`
	ResourceIdentifiers         []AggregateResourceIdentifier `json:"ResourceIdentifiers"`
}

type batchGetAggregateResourceConfigOutput struct {
	BaseConfigurationItems         []BaseConfigurationItem       `json:"BaseConfigurationItems"`
	UnprocessedResourceIdentifiers []AggregateResourceIdentifier `json:"UnprocessedResourceIdentifiers"`
}

func (h *Handler) handleBatchGetAggregateResourceConfig(
	_ context.Context,
	in *batchGetAggregateResourceConfigInput,
) (*batchGetAggregateResourceConfigOutput, error) {
	items, unprocessed := h.Backend.BatchGetAggregateResourceConfig(
		in.ConfigurationAggregatorName,
		in.ResourceIdentifiers,
	)

	return &batchGetAggregateResourceConfigOutput{
		BaseConfigurationItems:         items,
		UnprocessedResourceIdentifiers: unprocessed,
	}, nil
}

// BatchGetResourceConfig request/response types and handler.
type batchGetResourceConfigInput struct {
	ResourceKeys []ResourceKey `json:"ResourceKeys"`
}

type batchGetResourceConfigOutput struct {
	BaseConfigurationItems  []BaseConfigurationItem `json:"BaseConfigurationItems"`
	UnprocessedResourceKeys []ResourceKey           `json:"UnprocessedResourceKeys"`
}

func (h *Handler) handleBatchGetResourceConfig(
	_ context.Context,
	in *batchGetResourceConfigInput,
) (*batchGetResourceConfigOutput, error) {
	items, unprocessed := h.Backend.BatchGetResourceConfig(in.ResourceKeys)

	return &batchGetResourceConfigOutput{
		BaseConfigurationItems:  items,
		UnprocessedResourceKeys: unprocessed,
	}, nil
}

// DeleteResourceConfig request/response types and handler.
type deleteResourceConfigInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

func (h *Handler) handleDeleteResourceConfig(
	_ context.Context, in *deleteResourceConfigInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteResourceConfig(in.ResourceType, in.ResourceID)
}

// PutResourceConfig request/response types and handler.
type putResourceConfigInput struct {
	ResourceType  string `json:"ResourceType"`
	ResourceID    string `json:"ResourceId"`
	Configuration string `json:"Configuration"`
}

func (h *Handler) handlePutResourceConfig(
	_ context.Context, in *putResourceConfigInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutResourceConfig(in.ResourceType, in.ResourceID, in.Configuration)
}

// GetResourceConfigHistory request/response types and handler.
type getResourceConfigHistoryInput struct {
	ResourceType string `json:"resourceType"`
	ResourceID   string `json:"resourceId"`
	NextToken    string `json:"nextToken,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}
type getResourceConfigHistoryOutput struct {
	NextToken          string               `json:"nextToken,omitempty"`
	ConfigurationItems []ResourceConfigItem `json:"configurationItems"`
}

func (h *Handler) handleGetResourceConfigHistory(
	_ context.Context, in *getResourceConfigHistoryInput,
) (*getResourceConfigHistoryOutput, error) {
	if err := page.ValidateToken(in.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	items, next := h.Backend.GetResourceConfigHistoryPage(in.ResourceType, in.ResourceID, in.Limit, in.NextToken)

	return &getResourceConfigHistoryOutput{ConfigurationItems: items, NextToken: next}, nil
}

// GetDiscoveredResourceCounts request/response types and handler.
type getDiscoveredResourceCountsOutput struct {
	TotalDiscoveredResources int64 `json:"TotalDiscoveredResources"`
}

func (h *Handler) handleGetDiscoveredResourceCounts(
	_ context.Context, _ *emptyInput,
) (*getDiscoveredResourceCountsOutput, error) {
	return &getDiscoveredResourceCountsOutput{
		TotalDiscoveredResources: h.Backend.GetDiscoveredResourceCounts(),
	}, nil
}

// GetAggregateDiscoveredResourceCounts request/response types and handler.
type getAggregateDiscoveredResourceCountsOutput struct {
	TotalDiscoveredResources int32 `json:"TotalDiscoveredResources"`
}

func (h *Handler) handleGetAggregateDiscoveredResourceCounts(
	_ context.Context, _ *emptyInput,
) (*getAggregateDiscoveredResourceCountsOutput, error) {
	return &getAggregateDiscoveredResourceCountsOutput{
		TotalDiscoveredResources: h.Backend.GetAggregateDiscoveredResourceCounts(),
	}, nil
}

// GetAggregateResourceConfig request/response types and handler.
type getAggregateResourceConfigOutput struct {
	ConfigurationItem *BaseConfigurationItem `json:"ConfigurationItem"`
}

func (h *Handler) handleGetAggregateResourceConfig(
	_ context.Context, _ *emptyInput,
) (*getAggregateResourceConfigOutput, error) {
	return &getAggregateResourceConfigOutput{
		ConfigurationItem: h.Backend.GetAggregateResourceConfig(),
	}, nil
}

// ListDiscoveredResources request/response types and handler.
type listDiscoveredResourcesInput struct {
	ResourceType string `json:"resourceType"`
}
type listDiscoveredResourcesOutput struct {
	ResourceIdentifiers []ResourceConfigItem `json:"ResourceIdentifiers"`
}

func (h *Handler) handleListDiscoveredResources(
	_ context.Context, in *listDiscoveredResourcesInput,
) (*listDiscoveredResourcesOutput, error) {
	return &listDiscoveredResourcesOutput{
		ResourceIdentifiers: h.Backend.ListDiscoveredResources(in.ResourceType),
	}, nil
}

// ListAggregateDiscoveredResources request/response types and handler.
type listAggregateDiscoveredResourcesOutput struct {
	ResourceIdentifiers []any `json:"ResourceIdentifiers"`
}

func (h *Handler) handleListAggregateDiscoveredResources(
	_ context.Context, _ *emptyInput,
) (*listAggregateDiscoveredResourcesOutput, error) {
	return &listAggregateDiscoveredResourcesOutput{
		ResourceIdentifiers: h.Backend.ListAggregateDiscoveredResources(),
	}, nil
}

// SelectResourceConfig request/response types and handler.
type selectResourceConfigInput struct {
	Expression string `json:"Expression"`
}

type selectResourceConfigOutput struct {
	Results []string `json:"Results"`
}

func (h *Handler) handleSelectResourceConfig(
	_ context.Context, in *selectResourceConfigInput,
) (*selectResourceConfigOutput, error) {
	return &selectResourceConfigOutput{
		Results: h.Backend.SelectResourceConfig(in.Expression),
	}, nil
}

// SelectAggregateResourceConfig request/response types and handler.
type selectAggregateResourceConfigInput struct {
	Expression string `json:"Expression"`
}

type selectAggregateResourceConfigOutput struct {
	Results []string `json:"Results"`
}

func (h *Handler) handleSelectAggregateResourceConfig(
	_ context.Context, in *selectAggregateResourceConfigInput,
) (*selectAggregateResourceConfigOutput, error) {
	return &selectAggregateResourceConfigOutput{
		Results: h.Backend.SelectAggregateResourceConfig(in.Expression),
	}, nil
}

// GetResourceEvaluationSummary request/response types and handler.
type getResourceEvaluationSummaryInput struct {
	ResourceEvaluationID string `json:"ResourceEvaluationId"`
}
type resourceEvaluationStatus struct {
	Status string `json:"Status"`
}
type resourceEvaluationDetails struct {
	ResourceID   string `json:"ResourceId,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}
type getResourceEvaluationSummaryOutput struct {
	EvaluationStatus         *resourceEvaluationStatus  `json:"EvaluationStatus,omitempty"`
	ResourceDetails          *resourceEvaluationDetails `json:"ResourceDetails,omitempty"`
	ResourceEvaluationID     string                     `json:"ResourceEvaluationId,omitempty"`
	EvaluationMode           string                     `json:"EvaluationMode,omitempty"`
	Compliance               string                     `json:"Compliance,omitempty"`
	EvaluationStartTimestamp float64                    `json:"EvaluationStartTimestamp,omitempty"`
}

func (h *Handler) handleGetResourceEvaluationSummary(
	_ context.Context, in *getResourceEvaluationSummaryInput,
) (*getResourceEvaluationSummaryOutput, error) {
	re := h.Backend.GetResourceEvaluationSummaryByID(in.ResourceEvaluationID)
	if re == nil {
		return nil, fmt.Errorf("%w: %s", ErrResourceNotFound, in.ResourceEvaluationID)
	}

	return &getResourceEvaluationSummaryOutput{
		ResourceEvaluationID:     re.ResourceEvaluationID,
		EvaluationMode:           re.EvaluationMode,
		EvaluationStatus:         &resourceEvaluationStatus{Status: re.Status},
		EvaluationStartTimestamp: re.StartTime,
		Compliance:               re.Compliance,
		ResourceDetails: &resourceEvaluationDetails{
			ResourceID:   re.ResourceID,
			ResourceType: re.ResourceType,
		},
	}, nil
}

// ListResourceEvaluations request/response types and handler.
type resourceEvaluationSummary struct {
	ResourceEvaluationID     string  `json:"ResourceEvaluationId"`
	EvaluationMode           string  `json:"EvaluationMode"`
	EvaluationStartTimestamp float64 `json:"EvaluationStartTimestamp"`
}
type listResourceEvaluationsOutput struct {
	ResourceEvaluations []resourceEvaluationSummary `json:"ResourceEvaluations"`
}

func (h *Handler) handleListResourceEvaluations(
	_ context.Context, _ *emptyInput,
) (*listResourceEvaluationsOutput, error) {
	evals := h.Backend.ListResourceEvaluationSummaries()

	out := make([]resourceEvaluationSummary, 0, len(evals))
	for _, e := range evals {
		out = append(out, resourceEvaluationSummary{
			ResourceEvaluationID:     e.ResourceEvaluationID,
			EvaluationMode:           e.EvaluationMode,
			EvaluationStartTimestamp: e.StartTime,
		})
	}

	return &listResourceEvaluationsOutput{ResourceEvaluations: out}, nil
}

// StartResourceEvaluation request/response types and handler.
type startResourceEvaluationDetails struct {
	ResourceID            string `json:"ResourceId"`
	ResourceType          string `json:"ResourceType"`
	ResourceConfiguration string `json:"ResourceConfiguration"`
}
type startResourceEvaluationInput struct {
	ResourceDetails startResourceEvaluationDetails `json:"ResourceDetails"`
	EvaluationMode  string                         `json:"EvaluationMode"`
}
type startResourceEvaluationOutput struct {
	ResourceEvaluationID string `json:"ResourceEvaluationId"`
}

func (h *Handler) handleStartResourceEvaluation(
	_ context.Context, in *startResourceEvaluationInput,
) (*startResourceEvaluationOutput, error) {
	id := h.Backend.StartResourceEvaluation(
		in.ResourceDetails.ResourceType,
		in.ResourceDetails.ResourceID,
		in.EvaluationMode,
		in.ResourceDetails.ResourceConfiguration,
	)

	return &startResourceEvaluationOutput{ResourceEvaluationID: id}, nil
}

// buildResourceDispatch returns dispatch entries for resource ops.
func (h *Handler) buildResourceDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opBatchGetAggregateResourceConfig:      service.WrapOp(h.handleBatchGetAggregateResourceConfig),
		opBatchGetResourceConfig:               service.WrapOp(h.handleBatchGetResourceConfig),
		opDeleteResourceConfig:                 service.WrapOp(h.handleDeleteResourceConfig),
		opPutResourceConfig:                    service.WrapOp(h.handlePutResourceConfig),
		opGetResourceConfigHistory:             service.WrapOp(h.handleGetResourceConfigHistory),
		opGetDiscoveredResourceCounts:          service.WrapOp(h.handleGetDiscoveredResourceCounts),
		opGetAggregateDiscoveredResourceCounts: service.WrapOp(h.handleGetAggregateDiscoveredResourceCounts),
		opGetAggregateResourceConfig:           service.WrapOp(h.handleGetAggregateResourceConfig),
		opListDiscoveredResources:              service.WrapOp(h.handleListDiscoveredResources),
		opListAggregateDiscoveredResources:     service.WrapOp(h.handleListAggregateDiscoveredResources),
		opSelectResourceConfig:                 service.WrapOp(h.handleSelectResourceConfig),
		opSelectAggregateResourceConfig:        service.WrapOp(h.handleSelectAggregateResourceConfig),
		opGetResourceEvaluationSummary:         service.WrapOp(h.handleGetResourceEvaluationSummary),
		opListResourceEvaluations:              service.WrapOp(h.handleListResourceEvaluations),
		opStartResourceEvaluation:              service.WrapOp(h.handleStartResourceEvaluation),
	}
}
