package ce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ceTargetPrefix = "AWSInsightsIndexService."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Cost Explorer (Ce) operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Cost Explorer handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "Ce" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCostCategoryDefinition",
		"DeleteCostCategoryDefinition",
		"DescribeCostCategoryDefinition",
		"ListCostCategoryDefinitions",
		"UpdateCostCategoryDefinition",
		"CreateAnomalyMonitor",
		"DeleteAnomalyMonitor",
		"GetAnomalyMonitors",
		"UpdateAnomalyMonitor",
		"CreateAnomalySubscription",
		"DeleteAnomalySubscription",
		"GetAnomalySubscriptions",
		"UpdateAnomalySubscription",
		"GetCostAndUsage",
		"GetCostForecast",
		"GetUsageForecast",
		"GetDimensionValues",
		"GetTags",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"GetAnomalies",
		"GetApproximateUsageRecords",
		"GetCommitmentPurchaseAnalysis",
		"GetCostAndUsageComparisons",
		"GetCostAndUsageWithResources",
		"GetCostCategories",
		"GetCostComparisonDrivers",
		"GetReservationCoverage",
		"GetReservationPurchaseRecommendation",
		"GetReservationUtilization",
		"GetRightsizingRecommendation",
		"GetSavingsPlanPurchaseRecommendationDetails",
		"GetSavingsPlansCoverage",
		"GetSavingsPlansPurchaseRecommendation",
		"GetSavingsPlansUtilization",
		"GetSavingsPlansUtilizationDetails",
		"ListCommitmentPurchaseAnalyses",
		"ListCostAllocationTagBackfillHistory",
		"ListCostAllocationTags",
		"ListCostCategoryResourceAssociations",
		"ListSavingsPlansPurchaseRecommendationGeneration",
		"ProvideAnomalyFeedback",
		"StartCommitmentPurchaseAnalysis",
		"StartCostAllocationTagBackfill",
		"StartSavingsPlansPurchaseRecommendationGeneration",
		"UpdateCostAllocationTagsStatus",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ce" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Cost Explorer requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), ceTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Cost Explorer action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, ceTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for Ce).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for Cost Explorer requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Ce", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

//nolint:funlen // large dispatch table for CE operations
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCostCategoryDefinition": service.WrapOp(
			h.handleCreateCostCategoryDefinition,
		),
		"DeleteCostCategoryDefinition": service.WrapOp(
			h.handleDeleteCostCategoryDefinition,
		),
		"DescribeCostCategoryDefinition": service.WrapOp(
			h.handleDescribeCostCategoryDefinition,
		),
		"ListCostCategoryDefinitions": service.WrapOp(
			h.handleListCostCategoryDefinitions,
		),
		"UpdateCostCategoryDefinition": service.WrapOp(
			h.handleUpdateCostCategoryDefinition,
		),
		"CreateAnomalyMonitor": service.WrapOp(
			h.handleCreateAnomalyMonitor,
		),
		"DeleteAnomalyMonitor": service.WrapOp(
			h.handleDeleteAnomalyMonitor,
		),
		"GetAnomalyMonitors": service.WrapOp(
			h.handleGetAnomalyMonitors,
		),
		"UpdateAnomalyMonitor": service.WrapOp(
			h.handleUpdateAnomalyMonitor,
		),
		"CreateAnomalySubscription": service.WrapOp(
			h.handleCreateAnomalySubscription,
		),
		"DeleteAnomalySubscription": service.WrapOp(
			h.handleDeleteAnomalySubscription,
		),
		"GetAnomalySubscriptions": service.WrapOp(
			h.handleGetAnomalySubscriptions,
		),
		"UpdateAnomalySubscription": service.WrapOp(
			h.handleUpdateAnomalySubscription,
		),
		"GetCostAndUsage": service.WrapOp(
			h.handleGetCostAndUsage,
		),
		"GetCostForecast": service.WrapOp(
			h.handleGetCostForecast,
		),
		"GetUsageForecast": service.WrapOp(
			h.handleGetUsageForecast,
		),
		"GetDimensionValues": service.WrapOp(
			h.handleGetDimensionValues,
		),
		"GetTags": service.WrapOp(h.handleGetTags),
		"ListTagsForResource": service.WrapOp(
			h.handleListTagsForResource,
		),
		"TagResource":   service.WrapOp(h.handleTagResource),
		"UntagResource": service.WrapOp(h.handleUntagResource),
		"GetAnomalies":  service.WrapOp(h.handleGetAnomalies),
		"GetApproximateUsageRecords": service.WrapOp(
			h.handleGetApproximateUsageRecords,
		),
		"GetCommitmentPurchaseAnalysis": service.WrapOp(
			h.handleGetCommitmentPurchaseAnalysis,
		),
		"GetCostAndUsageComparisons": service.WrapOp(
			h.handleGetCostAndUsageComparisons,
		),
		"GetCostAndUsageWithResources": service.WrapOp(
			h.handleGetCostAndUsageWithResources,
		),
		"GetCostCategories": service.WrapOp(
			h.handleGetCostCategories,
		),
		"GetCostComparisonDrivers": service.WrapOp(
			h.handleGetCostComparisonDrivers,
		),
		"GetReservationCoverage": service.WrapOp(
			h.handleGetReservationCoverage,
		),
		"GetReservationPurchaseRecommendation": service.WrapOp(
			h.handleGetReservationPurchaseRecommendation,
		),
		"GetReservationUtilization": service.WrapOp(
			h.handleGetReservationUtilization,
		),
		"GetRightsizingRecommendation": service.WrapOp(
			h.handleGetRightsizingRecommendation,
		),
		"GetSavingsPlanPurchaseRecommendationDetails": service.WrapOp(
			h.handleGetSavingsPlanPurchaseRecommendationDetails,
		),
		"GetSavingsPlansCoverage": service.WrapOp(
			h.handleGetSavingsPlansCoverage,
		),
		"GetSavingsPlansPurchaseRecommendation": service.WrapOp(
			h.handleGetSavingsPlansPurchaseRecommendation,
		),
		"GetSavingsPlansUtilization": service.WrapOp(
			h.handleGetSavingsPlansUtilization,
		),
		"GetSavingsPlansUtilizationDetails": service.WrapOp(
			h.handleGetSavingsPlansUtilizationDetails,
		),
		"ListCommitmentPurchaseAnalyses": service.WrapOp(
			h.handleListCommitmentPurchaseAnalyses,
		),
		"ListCostAllocationTagBackfillHistory": service.WrapOp(
			h.handleListCostAllocationTagBackfillHistory,
		),
		"ListCostAllocationTags": service.WrapOp(
			h.handleListCostAllocationTags,
		),
		"ListCostCategoryResourceAssociations": service.WrapOp(
			h.handleListCostCategoryResourceAssociations,
		),
		"ListSavingsPlansPurchaseRecommendationGeneration": service.WrapOp(
			h.handleListSavingsPlansPurchaseRecommendationGeneration,
		),
		"ProvideAnomalyFeedback": service.WrapOp(
			h.handleProvideAnomalyFeedback,
		),
		"StartCommitmentPurchaseAnalysis": service.WrapOp(
			h.handleStartCommitmentPurchaseAnalysis,
		),
		"StartCostAllocationTagBackfill": service.WrapOp(
			h.handleStartCostAllocationTagBackfill,
		),
		"StartSavingsPlansPurchaseRecommendationGeneration": service.WrapOp(
			h.handleStartSavingsPlansPurchaseRecommendationGeneration,
		),
		"UpdateCostAllocationTagsStatus": service.WrapOp(
			h.handleUpdateCostAllocationTagsStatus,
		),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ServiceQuotaExceededException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidParameterException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// --- Cost Category operations ---

// resourceTag represents a single AWS CE resource tag (Key+Value pair).
// The Cost Explorer API serializes tags as a JSON array of {Key, Value} objects.
type resourceTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// resourceTagsToMap converts an array of resourceTag to map[string]string for backend storage.
func resourceTagsToMap(tags []resourceTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))

	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToResourceTags converts a map[string]string to an array of resourceTag for API responses.
// Tags are sorted by Key for deterministic output.
func mapToResourceTags(m map[string]string) []resourceTag {
	tags := make([]resourceTag, 0, len(m))

	for k, v := range m {
		tags = append(tags, resourceTag{Key: k, Value: v})
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags
}

type createCostCategoryDefinitionInput struct {
	Name             string             `json:"Name"`
	RuleVersion      string             `json:"RuleVersion"`
	DefaultValue     string             `json:"DefaultValue"`
	EffectiveStart   string             `json:"EffectiveStart"`
	Rules            []costCategoryRule `json:"Rules"`
	SplitChargeRules []splitChargeRule  `json:"SplitChargeRules"`
	ResourceTags     []resourceTag      `json:"ResourceTags"`
}

type costCategoryRule struct {
	Value string `json:"Value"`
}

type splitChargeRule struct {
	Source  string   `json:"Source"`
	Method  string   `json:"Method"`
	Targets []string `json:"Targets"`
}

type createCostCategoryDefinitionOutput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveStart  string `json:"EffectiveStart"`
}

func (h *Handler) handleCreateCostCategoryDefinition(
	_ context.Context,
	in *createCostCategoryDefinitionInput,
) (*createCostCategoryDefinitionOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	rules := make([]CostCategoryRule, 0, len(in.Rules))
	for _, r := range in.Rules {
		rules = append(rules, CostCategoryRule(r))
	}

	cat, err := h.Backend.CreateCostCategoryDefinition(
		in.Name, in.RuleVersion, in.DefaultValue,
		rules, resourceTagsToMap(in.ResourceTags),
	)
	if err != nil {
		return nil, err
	}

	return &createCostCategoryDefinitionOutput{
		CostCategoryArn: cat.ARN,
		EffectiveStart:  cat.EffectiveStart,
	}, nil
}

type deleteCostCategoryDefinitionInput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
}

type deleteCostCategoryDefinitionOutput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveEnd    string `json:"EffectiveEnd"`
}

func (h *Handler) handleDeleteCostCategoryDefinition(
	_ context.Context,
	in *deleteCostCategoryDefinitionInput,
) (*deleteCostCategoryDefinitionOutput, error) {
	if in.CostCategoryArn == "" {
		return nil, fmt.Errorf("%w: CostCategoryArn is required", errInvalidRequest)
	}

	cat, err := h.Backend.DeleteCostCategoryDefinition(in.CostCategoryArn)
	if err != nil {
		return nil, err
	}

	return &deleteCostCategoryDefinitionOutput{
		CostCategoryArn: cat.ARN,
		EffectiveEnd:    effectiveStart(),
	}, nil
}

type describeCostCategoryDefinitionInput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveOn     string `json:"EffectiveOn"`
}

type costCategorySummary struct {
	CostCategoryArn string             `json:"CostCategoryArn"`
	Name            string             `json:"Name"`
	RuleVersion     string             `json:"RuleVersion"`
	DefaultValue    string             `json:"DefaultValue"`
	EffectiveStart  string             `json:"EffectiveStart"`
	Rules           []costCategoryRule `json:"Rules"`
}

type describeCostCategoryDefinitionOutput struct {
	CostCategory costCategorySummary `json:"CostCategory"`
}

func (h *Handler) handleDescribeCostCategoryDefinition(
	_ context.Context,
	in *describeCostCategoryDefinitionInput,
) (*describeCostCategoryDefinitionOutput, error) {
	if in.CostCategoryArn == "" {
		return nil, fmt.Errorf("%w: CostCategoryArn is required", errInvalidRequest)
	}

	cat, err := h.Backend.DescribeCostCategoryDefinition(in.CostCategoryArn)
	if err != nil {
		return nil, err
	}

	rules := make([]costCategoryRule, len(cat.Rules))
	for i, r := range cat.Rules {
		rules[i] = costCategoryRule(r)
	}

	return &describeCostCategoryDefinitionOutput{
		CostCategory: costCategorySummary{
			CostCategoryArn: cat.ARN,
			Name:            cat.Name,
			RuleVersion:     cat.RuleVersion,
			DefaultValue:    cat.DefaultValue,
			EffectiveStart:  cat.EffectiveStart,
			Rules:           rules,
		},
	}, nil
}

type listCostCategoryDefinitionsInput struct {
	NextToken   string `json:"NextToken"`
	EffectiveOn string `json:"EffectiveOn"`
	MaxResults  int    `json:"MaxResults"`
}

type costCategoryReference struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	Name            string `json:"Name"`
	EffectiveStart  string `json:"EffectiveStart"`
}

type listCostCategoryDefinitionsOutput struct {
	NextPageToken          string                  `json:"NextPageToken,omitempty"`
	CostCategoryReferences []costCategoryReference `json:"CostCategoryReferences"`
}

func (h *Handler) handleListCostCategoryDefinitions(
	_ context.Context,
	_ *listCostCategoryDefinitionsInput,
) (*listCostCategoryDefinitionsOutput, error) {
	cats := h.Backend.ListCostCategoryDefinitions()
	refs := make([]costCategoryReference, 0, len(cats))
	for _, cat := range cats {
		refs = append(refs, costCategoryReference{
			CostCategoryArn: cat.ARN,
			Name:            cat.Name,
			EffectiveStart:  cat.EffectiveStart,
		})
	}

	return &listCostCategoryDefinitionsOutput{CostCategoryReferences: refs}, nil
}

type updateCostCategoryDefinitionInput struct {
	CostCategoryArn  string             `json:"CostCategoryArn"`
	RuleVersion      string             `json:"RuleVersion"`
	DefaultValue     string             `json:"DefaultValue"`
	Rules            []costCategoryRule `json:"Rules"`
	SplitChargeRules []splitChargeRule  `json:"SplitChargeRules"`
}

type updateCostCategoryDefinitionOutput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveStart  string `json:"EffectiveStart"`
}

func (h *Handler) handleUpdateCostCategoryDefinition(
	_ context.Context,
	in *updateCostCategoryDefinitionInput,
) (*updateCostCategoryDefinitionOutput, error) {
	if in.CostCategoryArn == "" {
		return nil, fmt.Errorf("%w: CostCategoryArn is required", errInvalidRequest)
	}

	rules := make([]CostCategoryRule, 0, len(in.Rules))
	for _, r := range in.Rules {
		rules = append(rules, CostCategoryRule(r))
	}

	splitChargeRules := make([]SplitChargeRule, 0, len(in.SplitChargeRules))
	for _, r := range in.SplitChargeRules {
		splitChargeRules = append(splitChargeRules, SplitChargeRule(r))
	}

	cat, err := h.Backend.UpdateCostCategoryDefinition(
		in.CostCategoryArn, in.RuleVersion, in.DefaultValue,
		rules, splitChargeRules,
	)
	if err != nil {
		return nil, err
	}

	return &updateCostCategoryDefinitionOutput{
		CostCategoryArn: cat.ARN,
		EffectiveStart:  cat.EffectiveStart,
	}, nil
}

// --- Anomaly Monitor operations ---

type anomalyMonitorInput struct {
	MonitorName      string `json:"MonitorName"`
	MonitorType      string `json:"MonitorType"`
	MonitorDimension string `json:"MonitorDimension"`
}

type createAnomalyMonitorInput struct {
	AnomalyMonitor anomalyMonitorInput `json:"AnomalyMonitor"`
	ResourceTags   []resourceTag       `json:"ResourceTags"`
}

type createAnomalyMonitorOutput struct {
	MonitorArn string `json:"MonitorArn"`
}

func (h *Handler) handleCreateAnomalyMonitor(
	_ context.Context,
	in *createAnomalyMonitorInput,
) (*createAnomalyMonitorOutput, error) {
	if in.AnomalyMonitor.MonitorName == "" {
		return nil, fmt.Errorf("%w: MonitorName is required", errInvalidRequest)
	}

	mon, err := h.Backend.CreateAnomalyMonitor(
		in.AnomalyMonitor.MonitorName,
		in.AnomalyMonitor.MonitorType,
		in.AnomalyMonitor.MonitorDimension,
		resourceTagsToMap(in.ResourceTags),
	)
	if err != nil {
		return nil, err
	}

	return &createAnomalyMonitorOutput{MonitorArn: mon.MonitorARN}, nil
}

type deleteAnomalyMonitorInput struct {
	MonitorArn string `json:"MonitorArn"`
}

type deleteAnomalyMonitorOutput struct{}

func (h *Handler) handleDeleteAnomalyMonitor(
	_ context.Context,
	in *deleteAnomalyMonitorInput,
) (*deleteAnomalyMonitorOutput, error) {
	if in.MonitorArn == "" {
		return nil, fmt.Errorf("%w: MonitorArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAnomalyMonitor(in.MonitorArn); err != nil {
		return nil, err
	}

	return &deleteAnomalyMonitorOutput{}, nil
}

type getAnomalyMonitorsInput struct {
	NextPageToken  string   `json:"NextPageToken"`
	MonitorArnList []string `json:"MonitorArnList"`
	MaxResults     int      `json:"MaxResults"`
}

type anomalyMonitorSummary struct {
	MonitorArn       string `json:"MonitorArn"`
	MonitorName      string `json:"MonitorName"`
	MonitorType      string `json:"MonitorType"`
	MonitorDimension string `json:"MonitorDimension,omitempty"`
}

type getAnomalyMonitorsOutput struct {
	NextPageToken   string                  `json:"NextPageToken,omitempty"`
	AnomalyMonitors []anomalyMonitorSummary `json:"AnomalyMonitors"`
}

func (h *Handler) handleGetAnomalyMonitors(
	_ context.Context,
	in *getAnomalyMonitorsInput,
) (*getAnomalyMonitorsOutput, error) {
	monitors := h.Backend.GetAnomalyMonitors(in.MonitorArnList)
	items := make([]anomalyMonitorSummary, 0, len(monitors))
	for _, mon := range monitors {
		items = append(items, anomalyMonitorSummary{
			MonitorArn:       mon.MonitorARN,
			MonitorName:      mon.MonitorName,
			MonitorType:      mon.MonitorType,
			MonitorDimension: mon.MonitorDimension,
		})
	}

	return &getAnomalyMonitorsOutput{AnomalyMonitors: items}, nil
}

type updateAnomalyMonitorInput struct {
	MonitorArn  string `json:"MonitorArn"`
	MonitorName string `json:"MonitorName"`
}

type updateAnomalyMonitorOutput struct {
	MonitorArn string `json:"MonitorArn"`
}

func (h *Handler) handleUpdateAnomalyMonitor(
	_ context.Context,
	in *updateAnomalyMonitorInput,
) (*updateAnomalyMonitorOutput, error) {
	if in.MonitorArn == "" {
		return nil, fmt.Errorf("%w: MonitorArn is required", errInvalidRequest)
	}

	if in.MonitorName == "" {
		return nil, fmt.Errorf("%w: MonitorName is required", errInvalidRequest)
	}

	mon, err := h.Backend.UpdateAnomalyMonitor(in.MonitorArn, in.MonitorName)
	if err != nil {
		return nil, err
	}

	return &updateAnomalyMonitorOutput{MonitorArn: mon.MonitorARN}, nil
}

// --- Anomaly Subscription operations ---

type subscriberInput struct {
	Address string `json:"Address"`
	Type    string `json:"Type"`
	Status  string `json:"Status"`
}

type anomalySubscriptionInput struct {
	SubscriptionName string            `json:"SubscriptionName"`
	Frequency        string            `json:"Frequency"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Threshold        float64           `json:"Threshold"`
}

type createAnomalySubscriptionInput struct {
	ResourceTags        []resourceTag            `json:"ResourceTags"`
	AnomalySubscription anomalySubscriptionInput `json:"AnomalySubscription"`
}

type createAnomalySubscriptionOutput struct {
	SubscriptionArn string `json:"SubscriptionArn"`
}

func (h *Handler) handleCreateAnomalySubscription(
	_ context.Context,
	in *createAnomalySubscriptionInput,
) (*createAnomalySubscriptionOutput, error) {
	if in.AnomalySubscription.SubscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", errInvalidRequest)
	}

	subs := make([]Subscriber, 0, len(in.AnomalySubscription.Subscribers))
	for _, s := range in.AnomalySubscription.Subscribers {
		subs = append(subs, Subscriber(s))
	}

	sub, err := h.Backend.CreateAnomalySubscription(
		in.AnomalySubscription.SubscriptionName,
		in.AnomalySubscription.Frequency,
		in.AnomalySubscription.MonitorArnList,
		subs,
		in.AnomalySubscription.Threshold,
		resourceTagsToMap(in.ResourceTags),
	)
	if err != nil {
		return nil, err
	}

	return &createAnomalySubscriptionOutput{SubscriptionArn: sub.SubscriptionARN}, nil
}

type deleteAnomalySubscriptionInput struct {
	SubscriptionArn string `json:"SubscriptionArn"`
}

type deleteAnomalySubscriptionOutput struct{}

func (h *Handler) handleDeleteAnomalySubscription(
	_ context.Context,
	in *deleteAnomalySubscriptionInput,
) (*deleteAnomalySubscriptionOutput, error) {
	if in.SubscriptionArn == "" {
		return nil, fmt.Errorf("%w: SubscriptionArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAnomalySubscription(in.SubscriptionArn); err != nil {
		return nil, err
	}

	return &deleteAnomalySubscriptionOutput{}, nil
}

type getAnomalySubscriptionsInput struct {
	MonitorArn          string   `json:"MonitorArn"`
	NextPageToken       string   `json:"NextPageToken"`
	SubscriptionArnList []string `json:"SubscriptionArnList"`
	MaxResults          int      `json:"MaxResults"`
}

type anomalySubscriptionSummary struct {
	SubscriptionArn  string            `json:"SubscriptionArn"`
	SubscriptionName string            `json:"SubscriptionName"`
	Frequency        string            `json:"Frequency"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Threshold        float64           `json:"Threshold,omitempty"`
}

type getAnomalySubscriptionsOutput struct {
	NextPageToken        string                       `json:"NextPageToken,omitempty"`
	AnomalySubscriptions []anomalySubscriptionSummary `json:"AnomalySubscriptions"`
}

func (h *Handler) handleGetAnomalySubscriptions(
	_ context.Context,
	in *getAnomalySubscriptionsInput,
) (*getAnomalySubscriptionsOutput, error) {
	subs := h.Backend.GetAnomalySubscriptions(in.SubscriptionArnList, in.MonitorArn)
	items := make([]anomalySubscriptionSummary, 0, len(subs))
	for _, sub := range subs {
		subscribers := make([]subscriberInput, 0, len(sub.Subscribers))
		for _, s := range sub.Subscribers {
			subscribers = append(subscribers, subscriberInput(s))
		}

		items = append(items, anomalySubscriptionSummary{
			SubscriptionArn:  sub.SubscriptionARN,
			SubscriptionName: sub.SubscriptionName,
			MonitorArnList:   sub.MonitorARNList,
			Frequency:        sub.Frequency,
			Threshold:        sub.Threshold,
			Subscribers:      subscribers,
		})
	}

	return &getAnomalySubscriptionsOutput{AnomalySubscriptions: items}, nil
}

type updateAnomalySubscriptionInput struct {
	SubscriptionArn  string            `json:"SubscriptionArn"`
	Frequency        string            `json:"Frequency"`
	SubscriptionName string            `json:"SubscriptionName"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Threshold        float64           `json:"Threshold"`
}

type updateAnomalySubscriptionOutput struct {
	SubscriptionArn string `json:"SubscriptionArn"`
}

func (h *Handler) handleUpdateAnomalySubscription(
	_ context.Context,
	in *updateAnomalySubscriptionInput,
) (*updateAnomalySubscriptionOutput, error) {
	if in.SubscriptionArn == "" {
		return nil, fmt.Errorf("%w: SubscriptionArn is required", errInvalidRequest)
	}

	subs := make([]Subscriber, 0, len(in.Subscribers))
	for _, s := range in.Subscribers {
		subs = append(subs, Subscriber(s))
	}

	sub, err := h.Backend.UpdateAnomalySubscription(
		in.SubscriptionArn, in.Frequency, in.SubscriptionName,
		in.MonitorArnList, subs, in.Threshold,
	)
	if err != nil {
		return nil, err
	}

	return &updateAnomalySubscriptionOutput{SubscriptionArn: sub.SubscriptionARN}, nil
}

// --- Cost & Usage queries ---

type groupBySpec struct {
	Type string `json:"Type"`
	Key  string `json:"Key"`
}

type getCostAndUsageInput struct {
	Filter      any               `json:"Filter"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metrics     []string          `json:"Metrics"`
	GroupBy     []groupBySpec     `json:"GroupBy"`
	NextPageToken string          `json:"NextPageToken"`
}

type getCostAndUsageOutput struct {
	NextPageToken            string        `json:"NextPageToken,omitempty"`
	ResultsByTime            []ResultByTime `json:"ResultsByTime"`
	DimensionValueAttributes []any         `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsage(
	_ context.Context,
	in *getCostAndUsageInput,
) (*getCostAndUsageOutput, error) {
	start := ""
	end := ""

	if in.TimePeriod != nil {
		start = in.TimePeriod["Start"]
		end = in.TimePeriod["End"]
	}

	if start == "" {
		start = "2024-01-01"
	}

	if end == "" {
		end = "2024-02-01"
	}

	granularity := in.Granularity
	if granularity == "" {
		granularity = "DAILY"
	}

	groupBy := make([]GroupBySpec, 0, len(in.GroupBy))
	for _, g := range in.GroupBy {
		groupBy = append(groupBy, GroupBySpec{Type: g.Type, Key: g.Key})
	}

	results := h.Backend.GetCostAndUsage(start, end, granularity, in.Metrics, groupBy)

	return &getCostAndUsageOutput{
		ResultsByTime:            results,
		DimensionValueAttributes: []any{},
	}, nil
}

type dimensionValue struct {
	Value      string            `json:"Value"`
	Attributes map[string]string `json:"Attributes,omitempty"`
}

type getDimensionValuesInput struct {
	TimePeriod   map[string]string `json:"TimePeriod"`
	Dimension    string            `json:"Dimension"`
	SearchString string            `json:"SearchString"`
	Context      string            `json:"Context"`
	NextPageToken string           `json:"NextPageToken"`
	MaxResults   int               `json:"MaxResults"`
}

type getDimensionValuesOutput struct {
	NextPageToken   string           `json:"NextPageToken,omitempty"`
	DimensionValues []dimensionValue `json:"DimensionValues"`
	ReturnSize      int              `json:"ReturnSize"`
	TotalSize       int              `json:"TotalSize"`
}

func (h *Handler) handleGetDimensionValues(
	_ context.Context,
	in *getDimensionValuesInput,
) (*getDimensionValuesOutput, error) {
	vals := h.Backend.GetDimensionValues(in.Dimension)

	if in.SearchString != "" {
		filtered := vals[:0]
		search := strings.ToLower(in.SearchString)

		for _, v := range vals {
			if strings.Contains(strings.ToLower(v), search) {
				filtered = append(filtered, v)
			}
		}

		vals = filtered
	}

	items := make([]dimensionValue, 0, len(vals))
	for _, v := range vals {
		items = append(items, dimensionValue{Value: v})
	}

	return &getDimensionValuesOutput{
		DimensionValues: items,
		ReturnSize:      len(items),
		TotalSize:       len(items),
	}, nil
}

type getTagsInput struct {
	TimePeriod   map[string]string `json:"TimePeriod"`
	TagKey       string            `json:"TagKey"`
	SearchString string            `json:"SearchString"`
	Filter       any               `json:"Filter"`
	NextPageToken string           `json:"NextPageToken"`
	MaxResults   int               `json:"MaxResults"`
}

type getTagsOutput struct {
	NextPageToken string   `json:"NextPageToken,omitempty"`
	Tags          []string `json:"Tags"`
	ReturnSize    int      `json:"ReturnSize"`
	TotalSize     int      `json:"TotalSize"`
}

func (h *Handler) handleGetTags(
	_ context.Context,
	in *getTagsInput,
) (*getTagsOutput, error) {
	var tags []string

	if in.TagKey != "" {
		tags = h.Backend.GetTagValues(in.TagKey)
	} else {
		tags = h.Backend.GetTagKeys()
	}

	if in.SearchString != "" {
		filtered := tags[:0]
		search := strings.ToLower(in.SearchString)

		for _, t := range tags {
			if strings.Contains(strings.ToLower(t), search) {
				filtered = append(filtered, t)
			}
		}

		tags = filtered
	}

	if tags == nil {
		tags = []string{}
	}

	return &getTagsOutput{
		Tags:       tags,
		ReturnSize: len(tags),
		TotalSize:  len(tags),
	}, nil
}

// --- Tagging operations ---

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	ResourceTags []resourceTag `json:"ResourceTags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	t, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{ResourceTags: mapToResourceTags(t)}, nil
}

type tagResourceInput struct {
	ResourceArn  string        `json:"ResourceArn"`
	ResourceTags []resourceTag `json:"ResourceTags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, resourceTagsToMap(in.ResourceTags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn     string   `json:"ResourceArn"`
	ResourceTagKeys []string `json:"ResourceTagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.ResourceTagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- Forecast stubs ---

type getCostForecastInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metric      string            `json:"Metric"`
}

type forecastResult struct {
	MeanValue                    string `json:"MeanValue"`
	PredictionIntervalLowerBound string `json:"PredictionIntervalLowerBound,omitempty"`
	PredictionIntervalUpperBound string `json:"PredictionIntervalUpperBound,omitempty"`
}

type getCostForecastOutput struct {
	Total                 *forecastResult `json:"Total,omitempty"`
	ForecastResultsByTime []any           `json:"ForecastResultsByTime"`
}

// handleGetCostForecast returns a stub forecast response.
// Real AWS returns predicted cost totals; the stub returns an empty forecast list.
func (h *Handler) handleGetCostForecast(
	_ context.Context,
	_ *getCostForecastInput,
) (*getCostForecastOutput, error) {
	return &getCostForecastOutput{
		Total:                 &forecastResult{MeanValue: "0"},
		ForecastResultsByTime: []any{},
	}, nil
}

type getUsageForecastInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metric      string            `json:"Metric"`
}

type getUsageForecastOutput struct {
	Total                 *forecastResult `json:"Total,omitempty"`
	ForecastResultsByTime []any           `json:"ForecastResultsByTime"`
}

// handleGetUsageForecast returns a stub usage forecast response.
func (h *Handler) handleGetUsageForecast(
	_ context.Context,
	_ *getUsageForecastInput,
) (*getUsageForecastOutput, error) {
	return &getUsageForecastOutput{
		Total:                 &forecastResult{MeanValue: "0"},
		ForecastResultsByTime: []any{},
	}, nil
}

// --- GetAnomalies ---

type anomalyDateInterval struct {
	StartDate string `json:"StartDate"`
	EndDate   string `json:"EndDate"`
}

type getAnomaliesInput struct {
	DateInterval  anomalyDateInterval `json:"DateInterval"`
	MonitorArn    string              `json:"MonitorArn"`
	Feedback      string              `json:"Feedback"`
	TotalImpact   map[string]any      `json:"TotalImpact"`
	NextPageToken string              `json:"NextPageToken"`
	MaxResults    int                 `json:"MaxResults"`
}

type anomalyImpact struct {
	MaxImpact              float64 `json:"MaxImpact"`
	TotalImpact            float64 `json:"TotalImpact"`
	TotalActualSpend       float64 `json:"TotalActualSpend"`
	TotalExpectedSpend     float64 `json:"TotalExpectedSpend"`
	TotalImpactPercentage  float64 `json:"TotalImpactPercentage"`
}

type anomalySummary struct {
	AnomalyID        string          `json:"AnomalyId"`
	AnomalyStartDate string          `json:"AnomalyStartDate"`
	AnomalyEndDate   string          `json:"AnomalyEndDate"`
	DimensionValue   string          `json:"DimensionValue"`
	MonitorArn       string          `json:"MonitorArn"`
	SubscriptionArn  string          `json:"SubscriptionArn,omitempty"`
	Feedback         string          `json:"Feedback,omitempty"`
	AnomalyScore     AnomalyScore    `json:"AnomalyScore"`
	Impact           anomalyImpact   `json:"Impact"`
	RootCauses       []AnomalyRootCause `json:"RootCauses,omitempty"`
}

type getAnomaliesOutput struct {
	NextPageToken string           `json:"NextPageToken,omitempty"`
	Anomalies     []anomalySummary `json:"Anomalies"`
}

func (h *Handler) handleGetAnomalies(
	_ context.Context,
	in *getAnomaliesInput,
) (*getAnomaliesOutput, error) {
	anomalies := h.Backend.GetAnomalies(in.MonitorArn, in.Feedback)
	items := make([]anomalySummary, 0, len(anomalies))

	for _, a := range anomalies {
		items = append(items, anomalySummary{
			AnomalyID:        a.AnomalyID,
			AnomalyStartDate: a.AnomalyStartDate,
			AnomalyEndDate:   a.AnomalyEndDate,
			DimensionValue:   a.DimensionValue,
			MonitorArn:       a.MonitorARN,
			SubscriptionArn:  a.SubscriptionARN,
			AnomalyScore:     a.AnomalyScore,
			Impact: anomalyImpact{
				MaxImpact:             a.TotalImpact,
				TotalImpact:           a.TotalImpact,
				TotalActualSpend:      a.TotalImpact * 1.2,
				TotalExpectedSpend:    a.TotalImpact * 0.9,
				TotalImpactPercentage: 25.0,
			},
			Feedback:   a.FeedbackType,
			RootCauses: a.RootCauses,
		})
	}

	return &getAnomaliesOutput{Anomalies: items}, nil
}

// --- GetApproximateUsageRecords stub ---

type getApproximateUsageRecordsInput struct {
	ApproximationDimension string   `json:"ApproximationDimension"`
	Granularity            string   `json:"Granularity"`
	Services               []string `json:"Services"`
}

type getApproximateUsageRecordsOutput struct {
	LookbackPeriod map[string]string `json:"LookbackPeriod,omitempty"`
	Services       map[string]string `json:"Services"`
	TotalRecords   string            `json:"TotalRecords"`
}

func (h *Handler) handleGetApproximateUsageRecords(
	_ context.Context,
	_ *getApproximateUsageRecordsInput,
) (*getApproximateUsageRecordsOutput, error) {
	return &getApproximateUsageRecordsOutput{
		Services:     map[string]string{},
		TotalRecords: "0",
	}, nil
}

// --- GetCommitmentPurchaseAnalysis stub ---

type getCommitmentPurchaseAnalysisInput struct {
	AnalysisID string `json:"AnalysisId"`
}

type getCommitmentPurchaseAnalysisOutput struct {
	EstimatedSavings any    `json:"EstimatedSavings,omitempty"`
	AnalysisID       string `json:"AnalysisId,omitempty"`
	AnalysisStatus   string `json:"AnalysisStatus,omitempty"`
}

func (h *Handler) handleGetCommitmentPurchaseAnalysis(
	_ context.Context,
	_ *getCommitmentPurchaseAnalysisInput,
) (*getCommitmentPurchaseAnalysisOutput, error) {
	return &getCommitmentPurchaseAnalysisOutput{}, nil
}

// --- GetCostAndUsageComparisons stub ---

type getCostAndUsageComparisonsInput struct {
	BaseTimePeriod       map[string]string `json:"BaseTimePeriod"`
	ComparisonTimePeriod map[string]string `json:"ComparisonTimePeriod"`
	Granularity          string            `json:"Granularity"`
	Metrics              []string          `json:"Metrics"`
}

type getCostAndUsageComparisonsOutput struct {
	NextPageToken     string `json:"NextPageToken,omitempty"`
	CostAndUsages     []any  `json:"CostAndUsages"`
	TotalCostAndUsage []any  `json:"TotalCostAndUsage"`
}

func (h *Handler) handleGetCostAndUsageComparisons(
	_ context.Context,
	_ *getCostAndUsageComparisonsInput,
) (*getCostAndUsageComparisonsOutput, error) {
	return &getCostAndUsageComparisonsOutput{
		CostAndUsages:     []any{},
		TotalCostAndUsage: []any{},
	}, nil
}

// --- GetCostAndUsageWithResources stub ---

type getCostAndUsageWithResourcesInput struct {
	Filter      any               `json:"Filter"`
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metrics     []string          `json:"Metrics"`
}

type getCostAndUsageWithResourcesOutput struct {
	NextPageToken            string `json:"NextPageToken,omitempty"`
	ResultsByTime            []any  `json:"ResultsByTime"`
	DimensionValueAttributes []any  `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsageWithResources(
	_ context.Context,
	_ *getCostAndUsageWithResourcesInput,
) (*getCostAndUsageWithResourcesOutput, error) {
	return &getCostAndUsageWithResourcesOutput{
		ResultsByTime:            []any{},
		DimensionValueAttributes: []any{},
	}, nil
}

// --- GetCostCategories ---

type getCostCategoriesInput struct {
	TimePeriod       map[string]string `json:"TimePeriod"`
	CostCategoryName string            `json:"CostCategoryName"`
	SearchString     string            `json:"SearchString"`
	NextPageToken    string            `json:"NextPageToken"`
	MaxResults       int               `json:"MaxResults"`
}

type getCostCategoriesOutput struct {
	NextPageToken      string   `json:"NextPageToken,omitempty"`
	CostCategoryValues []string `json:"CostCategoryValues"`
	ReturnSize         int      `json:"ReturnSize"`
	TotalSize          int      `json:"TotalSize"`
}

func (h *Handler) handleGetCostCategories(
	_ context.Context,
	in *getCostCategoriesInput,
) (*getCostCategoriesOutput, error) {
	values := h.Backend.GetCostCategories(in.CostCategoryName)

	return &getCostCategoriesOutput{
		CostCategoryValues: values,
		ReturnSize:         len(values),
		TotalSize:          len(values),
	}, nil
}

// --- GetCostComparisonDrivers stub ---

type getCostComparisonDriversInput struct {
	BaselineTimePeriod   map[string]string `json:"BaselineTimePeriod"`
	ComparisonTimePeriod map[string]string `json:"ComparisonTimePeriod"`
	Metric               string            `json:"Metric"`
}

type getCostComparisonDriversOutput struct {
	NextPageToken         string `json:"NextPageToken,omitempty"`
	CostComparisonDrivers []any  `json:"CostComparisonDrivers"`
}

func (h *Handler) handleGetCostComparisonDrivers(
	_ context.Context,
	_ *getCostComparisonDriversInput,
) (*getCostComparisonDriversOutput, error) {
	return &getCostComparisonDriversOutput{
		CostComparisonDrivers: []any{},
	}, nil
}

// --- GetReservationCoverage stub ---

type getReservationCoverageInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
}

type getReservationCoverageOutput struct {
	Total           any    `json:"Total,omitempty"`
	NextPageToken   string `json:"NextPageToken,omitempty"`
	CoveragesByTime []any  `json:"CoveragesByTime"`
}

func (h *Handler) handleGetReservationCoverage(
	_ context.Context,
	_ *getReservationCoverageInput,
) (*getReservationCoverageOutput, error) {
	return &getReservationCoverageOutput{
		CoveragesByTime: []any{},
	}, nil
}

// --- GetReservationPurchaseRecommendation stub ---

type getReservationPurchaseRecommendationInput struct {
	Service              string `json:"Service"`
	AccountScope         string `json:"AccountScope"`
	LookbackPeriodInDays string `json:"LookbackPeriodInDays"`
	TermInYears          string `json:"TermInYears"`
	PaymentOption        string `json:"PaymentOption"`
	NextPageToken        string `json:"NextPageToken"`
}

type getReservationPurchaseRecommendationOutput struct {
	NextPageToken   string `json:"NextPageToken,omitempty"`
	Metadata        any    `json:"Metadata,omitempty"`
	Recommendations []any  `json:"Recommendations"`
}

func (h *Handler) handleGetReservationPurchaseRecommendation(
	_ context.Context,
	_ *getReservationPurchaseRecommendationInput,
) (*getReservationPurchaseRecommendationOutput, error) {
	return &getReservationPurchaseRecommendationOutput{
		Recommendations: []any{},
	}, nil
}

// --- GetReservationUtilization stub ---

type getReservationUtilizationInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
}

type getReservationUtilizationOutput struct {
	Total              any    `json:"Total,omitempty"`
	NextPageToken      string `json:"NextPageToken,omitempty"`
	UtilizationsByTime []any  `json:"UtilizationsByTime"`
}

func (h *Handler) handleGetReservationUtilization(
	_ context.Context,
	_ *getReservationUtilizationInput,
) (*getReservationUtilizationOutput, error) {
	return &getReservationUtilizationOutput{
		UtilizationsByTime: []any{},
	}, nil
}

// --- GetRightsizingRecommendation stub ---

type getRightsizingRecommendationInput struct {
	Service       string `json:"Service"`
	NextPageToken string `json:"NextPageToken"`
	PageSize      int    `json:"PageSize"`
}

type getRightsizingRecommendationOutput struct {
	Summary                    any    `json:"Summary,omitempty"`
	Metadata                   any    `json:"Metadata,omitempty"`
	NextPageToken              string `json:"NextPageToken,omitempty"`
	RightsizingRecommendations []any  `json:"RightsizingRecommendations"`
}

func (h *Handler) handleGetRightsizingRecommendation(
	_ context.Context,
	_ *getRightsizingRecommendationInput,
) (*getRightsizingRecommendationOutput, error) {
	return &getRightsizingRecommendationOutput{
		RightsizingRecommendations: []any{},
	}, nil
}

// --- GetSavingsPlanPurchaseRecommendationDetails stub ---

type getSavingsPlanPurchaseRecommendationDetailsInput struct {
	RecommendationDetailID string `json:"RecommendationDetailId"`
}

type getSavingsPlanPurchaseRecommendationDetailsOutput struct {
	RecommendationDetail   any    `json:"RecommendationDetail,omitempty"`
	RecommendationDetailID string `json:"RecommendationDetailId,omitempty"`
}

func (h *Handler) handleGetSavingsPlanPurchaseRecommendationDetails(
	_ context.Context,
	_ *getSavingsPlanPurchaseRecommendationDetailsInput,
) (*getSavingsPlanPurchaseRecommendationDetailsOutput, error) {
	return &getSavingsPlanPurchaseRecommendationDetailsOutput{}, nil
}

// --- GetSavingsPlansCoverage stub ---

type getSavingsPlansCoverageInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	NextToken   string            `json:"NextToken"`
	MaxResults  int               `json:"MaxResults"`
}

type getSavingsPlansCoverageOutput struct {
	NextToken             string `json:"NextToken,omitempty"`
	SavingsPlansCoverages []any  `json:"SavingsPlansCoverages"`
}

func (h *Handler) handleGetSavingsPlansCoverage(
	_ context.Context,
	_ *getSavingsPlansCoverageInput,
) (*getSavingsPlansCoverageOutput, error) {
	return &getSavingsPlansCoverageOutput{
		SavingsPlansCoverages: []any{},
	}, nil
}

// --- GetSavingsPlansPurchaseRecommendation stub ---

type getSavingsPlansPurchaseRecommendationInput struct {
	SavingsPlansType     string `json:"SavingsPlansType"`
	TermInYears          string `json:"TermInYears"`
	PaymentOption        string `json:"PaymentOption"`
	LookbackPeriodInDays string `json:"LookbackPeriodInDays"`
	NextPageToken        string `json:"NextPageToken"`
	PageSize             int    `json:"PageSize"`
}

type getSavingsPlansPurchaseRecommendationOutput struct {
	SavingsPlansPurchaseRecommendation any    `json:"SavingsPlansPurchaseRecommendation,omitempty"`
	Metadata                           any    `json:"Metadata,omitempty"`
	NextPageToken                      string `json:"NextPageToken,omitempty"`
}

func (h *Handler) handleGetSavingsPlansPurchaseRecommendation(
	_ context.Context,
	_ *getSavingsPlansPurchaseRecommendationInput,
) (*getSavingsPlansPurchaseRecommendationOutput, error) {
	return &getSavingsPlansPurchaseRecommendationOutput{}, nil
}

// --- GetSavingsPlansUtilization stub ---

type getSavingsPlansUtilizationInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
}

type getSavingsPlansUtilizationOutput struct {
	Total                          any   `json:"Total,omitempty"`
	SavingsPlansUtilizationsByTime []any `json:"SavingsPlansUtilizationsByTime"`
}

func (h *Handler) handleGetSavingsPlansUtilization(
	_ context.Context,
	_ *getSavingsPlansUtilizationInput,
) (*getSavingsPlansUtilizationOutput, error) {
	return &getSavingsPlansUtilizationOutput{
		SavingsPlansUtilizationsByTime: []any{},
	}, nil
}

// --- GetSavingsPlansUtilizationDetails stub ---

type getSavingsPlansUtilizationDetailsInput struct {
	TimePeriod map[string]string `json:"TimePeriod"`
	NextToken  string            `json:"NextToken"`
	MaxResults int               `json:"MaxResults"`
}

type getSavingsPlansUtilizationDetailsOutput struct {
	NextToken                      string            `json:"NextToken,omitempty"`
	Total                          any               `json:"Total,omitempty"`
	TimePeriod                     map[string]string `json:"TimePeriod,omitempty"`
	SavingsPlansUtilizationDetails []any             `json:"SavingsPlansUtilizationDetails"`
}

func (h *Handler) handleGetSavingsPlansUtilizationDetails(
	_ context.Context,
	_ *getSavingsPlansUtilizationDetailsInput,
) (*getSavingsPlansUtilizationDetailsOutput, error) {
	return &getSavingsPlansUtilizationDetailsOutput{
		SavingsPlansUtilizationDetails: []any{},
	}, nil
}

// --- ListCommitmentPurchaseAnalyses stub ---

type listCommitmentPurchaseAnalysesInput struct {
	NextPageToken string `json:"NextPageToken"`
	PageSize      int    `json:"PageSize"`
}

type listCommitmentPurchaseAnalysesOutput struct {
	NextPageToken       string `json:"NextPageToken,omitempty"`
	AnalysisSummaryList []any  `json:"AnalysisSummaryList"`
}

func (h *Handler) handleListCommitmentPurchaseAnalyses(
	_ context.Context,
	_ *listCommitmentPurchaseAnalysesInput,
) (*listCommitmentPurchaseAnalysesOutput, error) {
	return &listCommitmentPurchaseAnalysesOutput{
		AnalysisSummaryList: []any{},
	}, nil
}

// --- ListCostAllocationTagBackfillHistory stub ---

type listCostAllocationTagBackfillHistoryInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listCostAllocationTagBackfillHistoryOutput struct {
	NextToken        string `json:"NextToken,omitempty"`
	BackfillRequests []any  `json:"BackfillRequests"`
}

func (h *Handler) handleListCostAllocationTagBackfillHistory(
	_ context.Context,
	_ *listCostAllocationTagBackfillHistoryInput,
) (*listCostAllocationTagBackfillHistoryOutput, error) {
	return &listCostAllocationTagBackfillHistoryOutput{
		BackfillRequests: []any{},
	}, nil
}

// --- ListCostAllocationTags stub ---

type listCostAllocationTagsInput struct {
	Status     string   `json:"Status"`
	Type       string   `json:"Type"`
	NextToken  string   `json:"NextToken"`
	TagKeys    []string `json:"TagKeys"`
	MaxResults int      `json:"MaxResults"`
}

type listCostAllocationTagsOutput struct {
	NextToken          string `json:"NextToken,omitempty"`
	CostAllocationTags []any  `json:"CostAllocationTags"`
}

func (h *Handler) handleListCostAllocationTags(
	_ context.Context,
	_ *listCostAllocationTagsInput,
) (*listCostAllocationTagsOutput, error) {
	return &listCostAllocationTagsOutput{
		CostAllocationTags: []any{},
	}, nil
}

// --- ListCostCategoryResourceAssociations stub ---

type listCostCategoryResourceAssociationsInput struct {
	CostCategoryArn   string `json:"CostCategoryArn"`
	NextToken         string `json:"NextToken"`
	ResourceTagFilter []any  `json:"ResourceTagFilter"`
}

type listCostCategoryResourceAssociationsOutput struct {
	CostCategoryReference any    `json:"CostCategoryReference,omitempty"`
	NextToken             string `json:"NextToken,omitempty"`
	ResourceTagsCount     int    `json:"ResourceTagsCount"`
}

func (h *Handler) handleListCostCategoryResourceAssociations(
	_ context.Context,
	_ *listCostCategoryResourceAssociationsInput,
) (*listCostCategoryResourceAssociationsOutput, error) {
	return &listCostCategoryResourceAssociationsOutput{
		ResourceTagsCount: 0,
	}, nil
}

// --- ListSavingsPlansPurchaseRecommendationGeneration stub ---

type listSavingsPlansPurchaseRecommendationGenerationInput struct {
	GenerationStatus string `json:"GenerationStatus"`
	NextPageToken    string `json:"NextPageToken"`
	PageSize         int    `json:"PageSize"`
}

type listSavingsPlansPurchaseRecommendationGenerationOutput struct {
	NextPageToken         string `json:"NextPageToken,omitempty"`
	GenerationSummaryList []any  `json:"GenerationSummaryList"`
}

func (h *Handler) handleListSavingsPlansPurchaseRecommendationGeneration(
	_ context.Context,
	_ *listSavingsPlansPurchaseRecommendationGenerationInput,
) (*listSavingsPlansPurchaseRecommendationGenerationOutput, error) {
	return &listSavingsPlansPurchaseRecommendationGenerationOutput{
		GenerationSummaryList: []any{},
	}, nil
}

// --- ProvideAnomalyFeedback stub ---

type provideAnomalyFeedbackInput struct {
	AnomalyID string `json:"AnomalyId"`
	Feedback  string `json:"Feedback"`
}

type provideAnomalyFeedbackOutput struct {
	AnomalyID string `json:"AnomalyId"`
}

func (h *Handler) handleProvideAnomalyFeedback(
	_ context.Context,
	in *provideAnomalyFeedbackInput,
) (*provideAnomalyFeedbackOutput, error) {
	return &provideAnomalyFeedbackOutput{
		AnomalyID: in.AnomalyID,
	}, nil
}

// --- StartCommitmentPurchaseAnalysis stub ---

type startCommitmentPurchaseAnalysisInput struct {
	CommitmentPurchaseAnalysisConfiguration any `json:"CommitmentPurchaseAnalysisConfiguration"`
}

type startCommitmentPurchaseAnalysisOutput struct {
	AnalysisID              string `json:"AnalysisId,omitempty"`
	AnalysisStartedTime     string `json:"AnalysisStartedTime,omitempty"`
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
}

func (h *Handler) handleStartCommitmentPurchaseAnalysis(
	_ context.Context,
	_ *startCommitmentPurchaseAnalysisInput,
) (*startCommitmentPurchaseAnalysisOutput, error) {
	return &startCommitmentPurchaseAnalysisOutput{}, nil
}

// --- StartCostAllocationTagBackfill stub ---

type startCostAllocationTagBackfillInput struct {
	BackfillFrom string `json:"BackfillFrom"`
}

type startCostAllocationTagBackfillOutput struct {
	BackfillRequest any `json:"BackfillRequest,omitempty"`
}

func (h *Handler) handleStartCostAllocationTagBackfill(
	_ context.Context,
	_ *startCostAllocationTagBackfillInput,
) (*startCostAllocationTagBackfillOutput, error) {
	return &startCostAllocationTagBackfillOutput{}, nil
}

// --- StartSavingsPlansPurchaseRecommendationGeneration stub ---

type startSavingsPlansPurchaseRecommendationGenerationInput struct{}

type startSavingsPlansPurchaseRecommendationGenerationOutput struct {
	GenerationID            string `json:"GenerationId,omitempty"`
	GenerationStartedTime   string `json:"GenerationStartedTime,omitempty"`
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
}

func (h *Handler) handleStartSavingsPlansPurchaseRecommendationGeneration(
	_ context.Context,
	_ *startSavingsPlansPurchaseRecommendationGenerationInput,
) (*startSavingsPlansPurchaseRecommendationGenerationOutput, error) {
	return &startSavingsPlansPurchaseRecommendationGenerationOutput{}, nil
}

// --- UpdateCostAllocationTagsStatus stub ---

type updateCostAllocationTagsStatusInput struct {
	CostAllocationTagsStatus []any `json:"CostAllocationTagsStatus"`
}

type updateCostAllocationTagsStatusOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateCostAllocationTagsStatus(
	_ context.Context,
	_ *updateCostAllocationTagsStatusInput,
) (*updateCostAllocationTagsStatusOutput, error) {
	return &updateCostAllocationTagsStatusOutput{
		Errors: []any{},
	}, nil
}
