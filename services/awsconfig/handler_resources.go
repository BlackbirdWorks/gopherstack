package awsconfig

import (
	"context"
	"fmt"
	"slices"

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

// PutResourceConfig: SchemaVersionId is required but real schema validation is not modelled.
type putResourceConfigInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	ResourceType    string            `json:"ResourceType"`
	ResourceID      string            `json:"ResourceId"`
	ResourceName    string            `json:"ResourceName,omitempty"`
	Configuration   string            `json:"Configuration"`
	SchemaVersionID string            `json:"SchemaVersionId"`
}

func (h *Handler) handlePutResourceConfig(
	_ context.Context, in *putResourceConfigInput,
) (*emptyOutput, error) {
	if in.SchemaVersionID == "" {
		return nil, fmt.Errorf("%w: SchemaVersionId is required", ErrValidation)
	}

	return &emptyOutput{}, h.Backend.PutResourceConfigNamed(
		in.ResourceType, in.ResourceID, in.Configuration, in.ResourceName, in.Tags,
	)
}

// GetResourceConfigHistory request/response types and handler.
type getResourceConfigHistoryInput struct {
	ResourceType       string  `json:"resourceType"`
	ResourceID         string  `json:"resourceId"`
	NextToken          string  `json:"nextToken,omitempty"`
	ChronologicalOrder string  `json:"chronologicalOrder,omitempty"`
	Limit              int     `json:"limit,omitempty"`
	EarlierTime        float64 `json:"earlierTime,omitempty"`
	LaterTime          float64 `json:"laterTime,omitempty"`
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

	items, next := h.Backend.GetResourceConfigHistoryPage(
		in.ResourceType, in.ResourceID, in.Limit, in.NextToken,
		in.ChronologicalOrder, in.EarlierTime, in.LaterTime,
	)

	return &getResourceConfigHistoryOutput{ConfigurationItems: items, NextToken: next}, nil
}

// GetDiscoveredResourceCounts is lowerCamelCase on the wire
// (deserializers.go: awsAwsjson11_deserializeOpDocumentGetDiscoveredResourceCountsOutput).
type getDiscoveredResourceCountsInput struct {
	NextToken     string   `json:"nextToken,omitempty"`
	ResourceTypes []string `json:"resourceTypes,omitempty"`
	Limit         int32    `json:"limit,omitempty"`
}
type getDiscoveredResourceCountsOutput struct {
	NextToken                string              `json:"nextToken,omitempty"`
	ResourceCounts           []ResourceTypeCount `json:"resourceCounts"`
	TotalDiscoveredResources int64               `json:"totalDiscoveredResources"`
}

// getDiscoveredResourceCountsPageDefault is the documented default page size.
const getDiscoveredResourceCountsPageDefault = 100

func (h *Handler) handleGetDiscoveredResourceCounts(
	_ context.Context, in *getDiscoveredResourceCountsInput,
) (*getDiscoveredResourceCountsOutput, error) {
	counts, total := h.Backend.DiscoveredResourceTypeCounts(in.ResourceTypes)

	p, err := paginate(counts, in.NextToken, in.Limit, getDiscoveredResourceCountsPageDefault)
	if err != nil {
		return nil, err
	}

	return &getDiscoveredResourceCountsOutput{
		ResourceCounts:           p.Data,
		NextToken:                p.Next,
		TotalDiscoveredResources: total,
	}, nil
}

// GetAggregateDiscoveredResourceCounts groups counts by GroupByKey; the group
// list is empty when GroupByKey is omitted, per the SDK output docs.
type getAggregateDiscoveredResourceCountsInput struct {
	Filters                     *ResourceCountFilters `json:"Filters,omitempty"`
	ConfigurationAggregatorName string                `json:"ConfigurationAggregatorName"`
	GroupByKey                  string                `json:"GroupByKey,omitempty"`
	NextToken                   string                `json:"NextToken,omitempty"`
	Limit                       int32                 `json:"Limit,omitempty"`
}
type getAggregateDiscoveredResourceCountsOutput struct {
	GroupByKey               string                 `json:"GroupByKey,omitempty"`
	NextToken                string                 `json:"NextToken,omitempty"`
	GroupedResourceCounts    []GroupedResourceCount `json:"GroupedResourceCounts,omitempty"`
	TotalDiscoveredResources int64                  `json:"TotalDiscoveredResources"`
}

// getAggregateDiscoveredResourceCountsPageDefault is the documented default (and maximum) page size.
const getAggregateDiscoveredResourceCountsPageDefault = 1000

func (h *Handler) handleGetAggregateDiscoveredResourceCounts(
	_ context.Context, in *getAggregateDiscoveredResourceCountsInput,
) (*getAggregateDiscoveredResourceCountsOutput, error) {
	var f ResourceCountFilters
	if in.Filters != nil {
		f = *in.Filters
	}

	groups, total, err := h.Backend.AggregateResourceCounts(in.ConfigurationAggregatorName, in.GroupByKey, f)
	if err != nil {
		return nil, err
	}

	p, err := paginate(groups, in.NextToken, in.Limit, getAggregateDiscoveredResourceCountsPageDefault)
	if err != nil {
		return nil, err
	}

	return &getAggregateDiscoveredResourceCountsOutput{
		GroupByKey:               in.GroupByKey,
		GroupedResourceCounts:    p.Data,
		NextToken:                p.Next,
		TotalDiscoveredResources: total,
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
	ResourceType            string   `json:"resourceType"`
	NextToken               string   `json:"nextToken,omitempty"`
	ResourceName            string   `json:"resourceName,omitempty"`
	ResourceIDs             []string `json:"resourceIds,omitempty"`
	Limit                   int32    `json:"limit,omitempty"`
	IncludeDeletedResources bool     `json:"includeDeletedResources,omitempty"`
}
type listDiscoveredResourcesOutput struct {
	NextToken           string               `json:"nextToken,omitempty"`
	ResourceIdentifiers []ResourceConfigItem `json:"resourceIdentifiers"`
}

// listDiscoveredResourcesPageDefault is the documented default page size
// (api_op_ListDiscoveredResources.go: "The default is 100.").
const listDiscoveredResourcesPageDefault = 100

func (h *Handler) handleListDiscoveredResources(
	_ context.Context, in *listDiscoveredResourcesInput,
) (*listDiscoveredResourcesOutput, error) {
	all := h.Backend.ListDiscoveredResourcesIncludingDeleted(in.ResourceType, in.IncludeDeletedResources)
	if len(in.ResourceIDs) > 0 {
		all = slices.DeleteFunc(
			all,
			func(it ResourceConfigItem) bool { return !slices.Contains(in.ResourceIDs, it.ResourceID) },
		)
	}

	if in.ResourceName != "" {
		all = slices.DeleteFunc(all, func(it ResourceConfigItem) bool { return it.ResourceName != in.ResourceName })
	}

	p, err := paginate(all, in.NextToken, in.Limit, listDiscoveredResourcesPageDefault)
	if err != nil {
		return nil, err
	}

	return &listDiscoveredResourcesOutput{ResourceIdentifiers: p.Data, NextToken: p.Next}, nil
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
	NextToken                   string                                       `json:"NextToken,omitempty"`
	ResourceType                string                                       `json:"ResourceType"`
	Limit                       int32                                        `json:"Limit,omitempty"`
}
type listAggregateDiscoveredResourcesOutput struct {
	NextToken           string                        `json:"NextToken,omitempty"`
	ResourceIdentifiers []AggregateResourceIdentifier `json:"ResourceIdentifiers"`
}

// listAggregateDiscoveredResourcesPageDefault: real docs cap Limit at 100 but
// state no separate default; 100 doubles as both (api_op_
// ListAggregateDiscoveredResources.go: "You cannot specify a number greater
// than 100.").
const listAggregateDiscoveredResourcesPageDefault = 100

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

	p, err := paginate(identifiers, in.NextToken, in.Limit, listAggregateDiscoveredResourcesPageDefault)
	if err != nil {
		return nil, err
	}

	return &listAggregateDiscoveredResourcesOutput{ResourceIdentifiers: p.Data, NextToken: p.Next}, nil
}

// SelectResourceConfig request/response types and handler.
type selectResourceConfigInput struct {
	Expression string `json:"Expression"`
	NextToken  string `json:"NextToken,omitempty"`
	Limit      int32  `json:"Limit,omitempty"`
}

type selectResourceConfigOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	Results   []string `json:"Results"`
}

func (h *Handler) handleSelectResourceConfig(
	_ context.Context, in *selectResourceConfigInput,
) (*selectResourceConfigOutput, error) {
	results, err := h.Backend.SelectResourceConfig(in.Expression)
	if err != nil {
		return nil, err
	}

	p, err := paginate(results, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &selectResourceConfigOutput{Results: p.Data, NextToken: p.Next}, nil
}

// SelectAggregateResourceConfig request/response types and handler.
type selectAggregateResourceConfigInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
	Expression                  string `json:"Expression"`
	NextToken                   string `json:"NextToken,omitempty"`
	MaxResults                  int32  `json:"MaxResults,omitempty"`
	Limit                       int32  `json:"Limit,omitempty"`
}

type selectAggregateResourceConfigOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	Results   []string `json:"Results"`
}

func (h *Handler) handleSelectAggregateResourceConfig(
	_ context.Context, in *selectAggregateResourceConfigInput,
) (*selectAggregateResourceConfigOutput, error) {
	if err := h.Backend.RequireAggregator(in.ConfigurationAggregatorName); err != nil {
		return nil, err
	}

	results, err := h.Backend.SelectAggregateResourceConfig(in.Expression)
	if err != nil {
		return nil, err
	}

	size := in.MaxResults
	if size == 0 {
		size = in.Limit
	}

	p, err := paginate(results, in.NextToken, size, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &selectAggregateResourceConfigOutput{Results: p.Data, NextToken: p.Next}, nil
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
	EvaluationStatus  *resourceEvaluationStatus  `json:"EvaluationStatus,omitempty"`
	ResourceDetails   *resourceEvaluationDetails `json:"ResourceDetails,omitempty"`
	EvaluationContext *struct {
		EvaluationContextIdentifier string `json:"EvaluationContextIdentifier"`
	} `json:"EvaluationContext,omitempty"`
	ResourceEvaluationID     string  `json:"ResourceEvaluationId,omitempty"`
	EvaluationMode           string  `json:"EvaluationMode,omitempty"`
	Compliance               string  `json:"Compliance,omitempty"`
	EvaluationStartTimestamp float64 `json:"EvaluationStartTimestamp,omitempty"`
}

func (h *Handler) handleGetResourceEvaluationSummary(
	_ context.Context, in *getResourceEvaluationSummaryInput,
) (*getResourceEvaluationSummaryOutput, error) {
	re := h.Backend.GetResourceEvaluationSummaryByID(in.ResourceEvaluationID)
	if re == nil {
		return nil, fmt.Errorf("%w: %s", ErrResourceNotFound, in.ResourceEvaluationID)
	}

	out := &getResourceEvaluationSummaryOutput{
		ResourceEvaluationID:     re.ResourceEvaluationID,
		EvaluationMode:           re.EvaluationMode,
		EvaluationStatus:         &resourceEvaluationStatus{Status: re.Status},
		EvaluationStartTimestamp: re.StartTime,
		Compliance:               re.Compliance,
		ResourceDetails: &resourceEvaluationDetails{
			ResourceID:   re.ResourceID,
			ResourceType: re.ResourceType,
		},
	}

	if re.ContextIdentifier != "" {
		out.EvaluationContext = &struct {
			EvaluationContextIdentifier string `json:"EvaluationContextIdentifier"`
		}{EvaluationContextIdentifier: re.ContextIdentifier}
	}

	return out, nil
}

// ListResourceEvaluations request/response types and handler.
type resourceEvaluationSummary struct {
	ResourceEvaluationID     string  `json:"ResourceEvaluationId"`
	EvaluationMode           string  `json:"EvaluationMode"`
	EvaluationStartTimestamp float64 `json:"EvaluationStartTimestamp"`
}
type listResourceEvaluationsInput struct {
	Filters *struct {
		TimeWindow *struct {
			StartTime float64 `json:"StartTime"`
			EndTime   float64 `json:"EndTime"`
		} `json:"TimeWindow,omitempty"`
		EvaluationMode              string `json:"EvaluationMode,omitempty"`
		EvaluationContextIdentifier string `json:"EvaluationContextIdentifier,omitempty"`
	} `json:"Filters,omitempty"`
	NextToken string `json:"NextToken,omitempty"`
	Limit     int32  `json:"Limit,omitempty"`
}
type listResourceEvaluationsOutput struct {
	NextToken           string                      `json:"NextToken,omitempty"`
	ResourceEvaluations []resourceEvaluationSummary `json:"ResourceEvaluations"`
}

// listResourceEvaluationsPageDefault is the documented default page size
// (api_op_ListResourceEvaluations.go: "The default is 10. You cannot specify
// a number greater than 100.").
const listResourceEvaluationsPageDefault = 10

func (h *Handler) handleListResourceEvaluations(
	_ context.Context, in *listResourceEvaluationsInput,
) (*listResourceEvaluationsOutput, error) {
	evals := h.Backend.ListResourceEvaluationSummaries()

	out := make([]resourceEvaluationSummary, 0, len(evals))
	for _, e := range evals {
		if f := in.Filters; f != nil {
			if f.EvaluationMode != "" && e.EvaluationMode != f.EvaluationMode {
				continue
			}

			if f.EvaluationContextIdentifier != "" && e.ContextIdentifier != f.EvaluationContextIdentifier {
				continue
			}

			if w := f.TimeWindow; w != nil &&
				(e.StartTime < w.StartTime || (w.EndTime != 0 && e.StartTime > w.EndTime)) {
				continue
			}
		}

		out = append(out, resourceEvaluationSummary{
			ResourceEvaluationID:     e.ResourceEvaluationID,
			EvaluationMode:           e.EvaluationMode,
			EvaluationStartTimestamp: e.StartTime,
		})
	}

	p, err := paginate(out, in.NextToken, in.Limit, listResourceEvaluationsPageDefault)
	if err != nil {
		return nil, err
	}

	return &listResourceEvaluationsOutput{ResourceEvaluations: p.Data, NextToken: p.Next}, nil
}

// maxResourceEvaluationTimeout is StartResourceEvaluationInput.EvaluationTimeout's documented ceiling.
const maxResourceEvaluationTimeout = 3600

type startResourceEvaluationDetails struct {
	ResourceID            string `json:"ResourceId"`
	ResourceType          string `json:"ResourceType"`
	ResourceConfiguration string `json:"ResourceConfiguration"`
}
type startResourceEvaluationInput struct {
	EvaluationContext *struct {
		EvaluationContextIdentifier string `json:"EvaluationContextIdentifier"`
	} `json:"EvaluationContext,omitempty"`
	ResourceDetails   startResourceEvaluationDetails `json:"ResourceDetails"`
	EvaluationMode    string                         `json:"EvaluationMode"`
	ClientToken       string                         `json:"ClientToken,omitempty"`
	EvaluationTimeout int32                          `json:"EvaluationTimeout,omitempty"`
}
type startResourceEvaluationOutput struct {
	ResourceEvaluationID string `json:"ResourceEvaluationId"`
}

func (h *Handler) handleStartResourceEvaluation(
	_ context.Context, in *startResourceEvaluationInput,
) (*startResourceEvaluationOutput, error) {
	if in.EvaluationTimeout < 0 || in.EvaluationTimeout > maxResourceEvaluationTimeout {
		return nil, fmt.Errorf(
			"%w: EvaluationTimeout must be between 0 and %d", ErrInvalidParameterValue, maxResourceEvaluationTimeout,
		)
	}

	contextID := ""
	if in.EvaluationContext != nil {
		contextID = in.EvaluationContext.EvaluationContextIdentifier
	}

	id, err := h.Backend.StartResourceEvaluationIdempotent(
		in.ResourceDetails.ResourceType,
		in.ResourceDetails.ResourceID,
		in.EvaluationMode,
		in.ResourceDetails.ResourceConfiguration,
		in.ClientToken,
		contextID,
	)
	if err != nil {
		return nil, err
	}

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
