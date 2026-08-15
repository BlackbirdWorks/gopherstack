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
	items, unprocessed, err := h.Backend.BatchGetAggregateResourceConfig(
		in.ConfigurationAggregatorName,
		in.ResourceIdentifiers,
	)
	if err != nil {
		return nil, err
	}

	return &batchGetAggregateResourceConfigOutput{
		BaseConfigurationItems:         items,
		UnprocessedResourceIdentifiers: unprocessed,
	}, nil
}

// BatchGetResourceConfig request/response types and handler. Sibling trap:
// BatchGetAggregateResourceConfig genuinely uses PascalCase
// ("ResourceIdentifiers"/"BaseConfigurationItems"/
// "UnprocessedResourceIdentifiers" -- confirmed at deserializers.go's
// awsAwsjson11_deserializeOpDocumentBatchGetAggregateResourceConfigOutput),
// but this plain (non-aggregate) sibling is lowerCamelCase on both sides
// ("resourceKeys" request; "baseConfigurationItems"/"unprocessedResourceKeys"
// response -- confirmed at serializers.go's
// awsAwsjson11_serializeOpDocumentBatchGetResourceConfigInput and
// deserializers.go's awsAwsjson11_deserializeOpDocumentBatchGetResourceConfigOutput).
// Reusing the aggregate op's casing here meant a real client's request never
// carried its ResourceKeys (always parsed as empty) and its response was
// always an empty BaseConfigurationItems regardless.
type batchGetResourceConfigInput struct {
	ResourceKeys []ResourceKey `json:"resourceKeys"`
}

type batchGetResourceConfigOutput struct {
	BaseConfigurationItems  []BaseConfigurationItem `json:"baseConfigurationItems"`
	UnprocessedResourceKeys []ResourceKey           `json:"unprocessedResourceKeys"`
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

// PutResourceConfig request/response types and handler. SchemaVersionId is a
// required member (aws-sdk-go-v2/service/configservice's
// PutResourceConfigInput) that real AWS uses to validate Configuration
// against the CloudFormation-registered schema for ResourceType -- a check
// this emulator cannot perform. It carries no output (PutResourceConfigOutput
// has no fields), so it is accepted and required but not stored.
type putResourceConfigInput struct {
	ResourceType    string `json:"ResourceType"`
	ResourceID      string `json:"ResourceId"`
	Configuration   string `json:"Configuration"`
	SchemaVersionID string `json:"SchemaVersionId"`
}

func (h *Handler) handlePutResourceConfig(
	_ context.Context, in *putResourceConfigInput,
) (*emptyOutput, error) {
	if in.SchemaVersionID == "" {
		return nil, fmt.Errorf("%w: SchemaVersionId is required", ErrValidation)
	}

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

// GetDiscoveredResourceCounts request/response types and handler. Real
// GetDiscoveredResourceCountsOutput is lowerCamelCase
// ("totalDiscoveredResources"/"resourceCounts"/"nextToken" -- confirmed at
// deserializers.go's
// awsAwsjson11_deserializeOpDocumentGetDiscoveredResourceCountsOutput),
// unlike most of this service's DescribeXxx wrappers -- TotalDiscoveredResources
// was always 0 for a real client regardless of how many resources this
// backend had discovered. The real, required ResourceCounts (per-type
// breakdown) member is not modeled: this backend's resourceConfigsByType
// index has no method to enumerate its group keys with counts, so adding it
// needs new pkgs/store surface, not a wire-key rename -- left as a disclosed
// gap rather than fabricated.
type getDiscoveredResourceCountsOutput struct {
	TotalDiscoveredResources int64 `json:"totalDiscoveredResources"`
}

func (h *Handler) handleGetDiscoveredResourceCounts(
	_ context.Context, _ *emptyInput,
) (*getDiscoveredResourceCountsOutput, error) {
	return &getDiscoveredResourceCountsOutput{
		TotalDiscoveredResources: h.Backend.GetDiscoveredResourceCounts(),
	}, nil
}

// GetAggregateDiscoveredResourceCounts request/response types and handler.
// Real GetAggregateDiscoveredResourceCountsOutput also echoes the request's
// GroupByKey and, only when GroupByKey was provided, a GroupedResourceCounts
// breakdown ("If GroupByKey is not provided, the result will be empty" per
// api_op_GetAggregateDiscoveredResourceCounts.go) -- GroupByKey is not read
// from the request at all here, and GroupedResourceCounts is not modeled;
// this backend has no per-group (account/region) resource-count breakdown
// surface to source it from without new tracking, so it is disclosed as a
// gap rather than fabricated. TotalDiscoveredResources ("This member is
// required") is unaffected by that gap and already correctly cased/emitted.
type getAggregateDiscoveredResourceCountsInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
	GroupByKey                  string `json:"GroupByKey,omitempty"`
}
type getAggregateDiscoveredResourceCountsOutput struct {
	GroupByKey               string `json:"GroupByKey,omitempty"`
	TotalDiscoveredResources int32  `json:"TotalDiscoveredResources"`
}

func (h *Handler) handleGetAggregateDiscoveredResourceCounts(
	_ context.Context, in *getAggregateDiscoveredResourceCountsInput,
) (*getAggregateDiscoveredResourceCountsOutput, error) {
	return &getAggregateDiscoveredResourceCountsOutput{
		GroupByKey:               in.GroupByKey,
		TotalDiscoveredResources: h.Backend.GetAggregateDiscoveredResourceCounts(),
	}, nil
}

// GetAggregateResourceConfig request/response types and handler.
type getAggregateResourceConfigInput struct {
	ConfigurationAggregatorName string                      `json:"ConfigurationAggregatorName"`
	ResourceIdentifier          AggregateResourceIdentifier `json:"ResourceIdentifier"`
}
type getAggregateResourceConfigOutput struct {
	ConfigurationItem *BaseConfigurationItem `json:"ConfigurationItem"`
}

func (h *Handler) handleGetAggregateResourceConfig(
	_ context.Context, in *getAggregateResourceConfigInput,
) (*getAggregateResourceConfigOutput, error) {
	item, err := h.Backend.GetAggregateResourceConfig(in.ConfigurationAggregatorName, in.ResourceIdentifier)
	if err != nil {
		return nil, err
	}

	return &getAggregateResourceConfigOutput{ConfigurationItem: item}, nil
}

// ListDiscoveredResources request/response types and handler. Real
// ListDiscoveredResourcesOutput wraps its list under "resourceIdentifiers"
// (lowercase; confirmed against configservice's
// awsAwsjson11_deserializeOpDocumentListDiscoveredResourcesOutput, unlike
// this service's DescribeXxx ops which are PascalCase) -- ResourceType,
// ResourceID were previously emitted under "ResourceIdentifiers", a key
// that does not exist on the real shape at all, so a real client's
// ResourceIdentifiers was always empty. ResourceConfigItem's per-item
// resourceType/resourceId are already correct for the real
// types.ResourceIdentifier shape used here; Configuration and
// ConfigurationItemCaptureTime are extra fields this op's real response
// doesn't have (harmless -- a real client ignores unknown keys), and
// ResourceName/ResourceDeletionTime (real, optional members) go unpopulated
// because this backend never tracks a discovered resource's display name or
// deletion time.
type listDiscoveredResourcesInput struct {
	ResourceType string `json:"resourceType"`
}
type listDiscoveredResourcesOutput struct {
	ResourceIdentifiers []ResourceConfigItem `json:"resourceIdentifiers"`
}

func (h *Handler) handleListDiscoveredResources(
	_ context.Context, in *listDiscoveredResourcesInput,
) (*listDiscoveredResourcesOutput, error) {
	return &listDiscoveredResourcesOutput{
		ResourceIdentifiers: h.Backend.ListDiscoveredResources(in.ResourceType),
	}, nil
}

// ListAggregateDiscoveredResources request/response types and handler.
type listAggregateDiscoveredResourcesFiltersBody struct {
	AccountID    string `json:"AccountId,omitempty"`
	Region       string `json:"Region,omitempty"`
	ResourceID   string `json:"ResourceId,omitempty"`
	ResourceName string `json:"ResourceName,omitempty"`
}
type listAggregateDiscoveredResourcesInput struct {
	Filters                     *listAggregateDiscoveredResourcesFiltersBody `json:"Filters,omitempty"`
	ConfigurationAggregatorName string                                       `json:"ConfigurationAggregatorName"`
	ResourceType                string                                       `json:"ResourceType"`
}
type listAggregateDiscoveredResourcesOutput struct {
	ResourceIdentifiers []AggregateResourceIdentifier `json:"ResourceIdentifiers"`
}

func (h *Handler) handleListAggregateDiscoveredResources(
	_ context.Context, in *listAggregateDiscoveredResourcesInput,
) (*listAggregateDiscoveredResourcesOutput, error) {
	var accountFilter, regionFilter, resourceIDFilter string
	if in.Filters != nil {
		accountFilter = in.Filters.AccountID
		regionFilter = in.Filters.Region
		resourceIDFilter = in.Filters.ResourceID
	}

	identifiers, err := h.Backend.ListAggregateDiscoveredResources(
		in.ConfigurationAggregatorName, in.ResourceType, accountFilter, regionFilter, resourceIDFilter,
	)
	if err != nil {
		return nil, err
	}

	return &listAggregateDiscoveredResourcesOutput{ResourceIdentifiers: identifiers}, nil
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
