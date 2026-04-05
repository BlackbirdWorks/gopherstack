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

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCostCategoryDefinition":         service.WrapOp(h.handleCreateCostCategoryDefinition),
		"DeleteCostCategoryDefinition":         service.WrapOp(h.handleDeleteCostCategoryDefinition),
		"DescribeCostCategoryDefinition":       service.WrapOp(h.handleDescribeCostCategoryDefinition),
		"ListCostCategoryDefinitions":          service.WrapOp(h.handleListCostCategoryDefinitions),
		"UpdateCostCategoryDefinition":         service.WrapOp(h.handleUpdateCostCategoryDefinition),
		"CreateAnomalyMonitor":                 service.WrapOp(h.handleCreateAnomalyMonitor),
		"DeleteAnomalyMonitor":                 service.WrapOp(h.handleDeleteAnomalyMonitor),
		"GetAnomalyMonitors":                   service.WrapOp(h.handleGetAnomalyMonitors),
		"UpdateAnomalyMonitor":                 service.WrapOp(h.handleUpdateAnomalyMonitor),
		"CreateAnomalySubscription":            service.WrapOp(h.handleCreateAnomalySubscription),
		"DeleteAnomalySubscription":            service.WrapOp(h.handleDeleteAnomalySubscription),
		"GetAnomalySubscriptions":              service.WrapOp(h.handleGetAnomalySubscriptions),
		"UpdateAnomalySubscription":            service.WrapOp(h.handleUpdateAnomalySubscription),
		"GetCostAndUsage":                      service.WrapOp(h.handleGetCostAndUsage),
		"GetCostForecast":                      service.WrapOp(h.handleGetCostForecast),
		"GetUsageForecast":                     service.WrapOp(h.handleGetUsageForecast),
		"GetDimensionValues":                   service.WrapOp(h.handleGetDimensionValues),
		"GetTags":                              service.WrapOp(h.handleGetTags),
		"ListTagsForResource":                  service.WrapOp(h.handleListTagsForResource),
		"TagResource":                          service.WrapOp(h.handleTagResource),
		"UntagResource":                        service.WrapOp(h.handleUntagResource),
		"GetAnomalies":                         service.WrapOp(h.handleGetAnomalies),
		"GetApproximateUsageRecords":           service.WrapOp(h.handleGetApproximateUsageRecords),
		"GetCommitmentPurchaseAnalysis":        service.WrapOp(h.handleGetCommitmentPurchaseAnalysis),
		"GetCostAndUsageComparisons":           service.WrapOp(h.handleGetCostAndUsageComparisons),
		"GetCostAndUsageWithResources":         service.WrapOp(h.handleGetCostAndUsageWithResources),
		"GetCostCategories":                    service.WrapOp(h.handleGetCostCategories),
		"GetCostComparisonDrivers":             service.WrapOp(h.handleGetCostComparisonDrivers),
		"GetReservationCoverage":               service.WrapOp(h.handleGetReservationCoverage),
		"GetReservationPurchaseRecommendation": service.WrapOp(h.handleGetReservationPurchaseRecommendation),
		"GetReservationUtilization":            service.WrapOp(h.handleGetReservationUtilization),
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

// --- Cost & Usage query stubs ---

type getCostAndUsageInput struct {
	TimePeriod  map[string]string `json:"TimePeriod"`
	Granularity string            `json:"Granularity"`
	Metrics     []string          `json:"Metrics"`
}

type getCostAndUsageOutput struct {
	ResultsByTime            []any `json:"ResultsByTime"`
	DimensionValueAttributes []any `json:"DimensionValueAttributes"`
}

func (h *Handler) handleGetCostAndUsage(
	_ context.Context,
	_ *getCostAndUsageInput,
) (*getCostAndUsageOutput, error) {
	return &getCostAndUsageOutput{
		ResultsByTime:            []any{},
		DimensionValueAttributes: []any{},
	}, nil
}

type getDimensionValuesInput struct {
	TimePeriod   map[string]string `json:"TimePeriod"`
	Dimension    string            `json:"Dimension"`
	SearchString string            `json:"SearchString"`
}

type getDimensionValuesOutput struct {
	NextPageToken   string `json:"NextPageToken,omitempty"`
	DimensionValues []any  `json:"DimensionValues"`
	ReturnSize      int    `json:"ReturnSize"`
	TotalSize       int    `json:"TotalSize"`
}

func (h *Handler) handleGetDimensionValues(
	_ context.Context,
	_ *getDimensionValuesInput,
) (*getDimensionValuesOutput, error) {
	return &getDimensionValuesOutput{
		DimensionValues: []any{},
		ReturnSize:      0,
		TotalSize:       0,
	}, nil
}

type getTagsInput struct {
	TimePeriod   map[string]string `json:"TimePeriod"`
	TagKey       string            `json:"TagKey"`
	SearchString string            `json:"SearchString"`
}

type getTagsOutput struct {
	NextPageToken string   `json:"NextPageToken,omitempty"`
	Tags          []string `json:"Tags"`
	ReturnSize    int      `json:"ReturnSize"`
	TotalSize     int      `json:"TotalSize"`
}

func (h *Handler) handleGetTags(
	_ context.Context,
	_ *getTagsInput,
) (*getTagsOutput, error) {
	return &getTagsOutput{
		Tags:       []string{},
		ReturnSize: 0,
		TotalSize:  0,
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

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
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

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
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

type anomalySummary struct {
	AnomalyID        string  `json:"AnomalyId"`
	AnomalyStartDate string  `json:"AnomalyStartDate"`
	AnomalyEndDate   string  `json:"AnomalyEndDate"`
	DimensionValue   string  `json:"DimensionValue"`
	MonitorArn       string  `json:"MonitorArn"`
	SubscriptionArn  string  `json:"SubscriptionArn,omitempty"`
	Feedback         string  `json:"Feedback,omitempty"`
	AnomalyScore     float64 `json:"AnomalyScore"`
	Impact           float64 `json:"Impact"`
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
			Impact:           a.TotalImpact,
			Feedback:         a.FeedbackType,
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
