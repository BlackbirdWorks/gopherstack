// Package ce provides an in-memory implementation of the AWS Cost Explorer (Ce) service.
package ce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
}

// NewHandler creates a new Cost Explorer handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		"GetDimensionValues",
		"GetTags",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCostCategoryDefinition":  service.WrapOp(h.handleCreateCostCategoryDefinition),
		"DeleteCostCategoryDefinition":  service.WrapOp(h.handleDeleteCostCategoryDefinition),
		"DescribeCostCategoryDefinition": service.WrapOp(h.handleDescribeCostCategoryDefinition),
		"ListCostCategoryDefinitions":   service.WrapOp(h.handleListCostCategoryDefinitions),
		"UpdateCostCategoryDefinition":  service.WrapOp(h.handleUpdateCostCategoryDefinition),
		"CreateAnomalyMonitor":          service.WrapOp(h.handleCreateAnomalyMonitor),
		"DeleteAnomalyMonitor":          service.WrapOp(h.handleDeleteAnomalyMonitor),
		"GetAnomalyMonitors":            service.WrapOp(h.handleGetAnomalyMonitors),
		"UpdateAnomalyMonitor":          service.WrapOp(h.handleUpdateAnomalyMonitor),
		"CreateAnomalySubscription":     service.WrapOp(h.handleCreateAnomalySubscription),
		"DeleteAnomalySubscription":     service.WrapOp(h.handleDeleteAnomalySubscription),
		"GetAnomalySubscriptions":       service.WrapOp(h.handleGetAnomalySubscriptions),
		"UpdateAnomalySubscription":     service.WrapOp(h.handleUpdateAnomalySubscription),
		"GetCostAndUsage":               service.WrapOp(h.handleGetCostAndUsage),
		"GetDimensionValues":            service.WrapOp(h.handleGetDimensionValues),
		"GetTags":                       service.WrapOp(h.handleGetTags),
		"ListTagsForResource":           service.WrapOp(h.handleListTagsForResource),
		"TagResource":                   service.WrapOp(h.handleTagResource),
		"UntagResource":                 service.WrapOp(h.handleUntagResource),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// --- Cost Category operations ---

type createCostCategoryDefinitionInput struct {
	ResourceTags     map[string]string  `json:"ResourceTags"`
	Name             string             `json:"Name"`
	RuleVersion      string             `json:"RuleVersion"`
	DefaultValue     string             `json:"DefaultValue"`
	EffectiveStart   string             `json:"EffectiveStart"`
	Rules            []costCategoryRule `json:"Rules"`
	SplitChargeRules []splitChargeRule  `json:"SplitChargeRules"`
}

type costCategoryRule struct {
	Value string `json:"Value"`
}

type splitChargeRule struct {
	Source  string   `json:"Source"`
	Targets []string `json:"Targets"`
	Method  string   `json:"Method"`
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
		rules = append(rules, CostCategoryRule{Value: r.Value})
	}

	cat, err := h.Backend.CreateCostCategoryDefinition(
		in.Name, in.RuleVersion, in.DefaultValue,
		rules, in.ResourceTags,
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

	rules := make([]costCategoryRule, 0, len(cat.Rules))
	for _, r := range cat.Rules {
		rules = append(rules, costCategoryRule{Value: r.Value})
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
	MaxResults   int    `json:"MaxResults"`
	NextToken    string `json:"NextToken"`
	EffectiveOn  string `json:"EffectiveOn"`
}

type costCategoryReference struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	Name            string `json:"Name"`
	EffectiveStart  string `json:"EffectiveStart"`
}

type listCostCategoryDefinitionsOutput struct {
	CostCategoryReferences []costCategoryReference `json:"CostCategoryReferences"`
	NextPageToken          string                  `json:"NextPageToken,omitempty"`
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
		rules = append(rules, CostCategoryRule{Value: r.Value})
	}

	splitChargeRules := make([]SplitChargeRule, 0, len(in.SplitChargeRules))
	for _, r := range in.SplitChargeRules {
		splitChargeRules = append(splitChargeRules, SplitChargeRule{
			Source:  r.Source,
			Targets: r.Targets,
			Method:  r.Method,
		})
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
	ResourceTags   map[string]string   `json:"ResourceTags"`
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
		in.ResourceTags,
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
	MonitorArnList []string `json:"MonitorArnList"`
	NextPageToken  string   `json:"NextPageToken"`
	MaxResults     int      `json:"MaxResults"`
}

type anomalyMonitorSummary struct {
	MonitorArn       string `json:"MonitorArn"`
	MonitorName      string `json:"MonitorName"`
	MonitorType      string `json:"MonitorType"`
	MonitorDimension string `json:"MonitorDimension,omitempty"`
}

type getAnomalyMonitorsOutput struct {
	AnomalyMonitors []anomalyMonitorSummary `json:"AnomalyMonitors"`
	NextPageToken   string                  `json:"NextPageToken,omitempty"`
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
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Frequency        string            `json:"Frequency"`
	Threshold        float64           `json:"Threshold"`
}

type createAnomalySubscriptionInput struct {
	AnomalySubscription anomalySubscriptionInput `json:"AnomalySubscription"`
	ResourceTags        map[string]string        `json:"ResourceTags"`
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
		subs = append(subs, Subscriber{
			Address: s.Address,
			Type:    s.Type,
			Status:  s.Status,
		})
	}

	sub, err := h.Backend.CreateAnomalySubscription(
		in.AnomalySubscription.SubscriptionName,
		in.AnomalySubscription.Frequency,
		in.AnomalySubscription.MonitorArnList,
		subs,
		in.AnomalySubscription.Threshold,
		in.ResourceTags,
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
	SubscriptionArnList []string `json:"SubscriptionArnList"`
	MonitorArn          string   `json:"MonitorArn"`
	NextPageToken       string   `json:"NextPageToken"`
	MaxResults          int      `json:"MaxResults"`
}

type anomalySubscriptionSummary struct {
	SubscriptionArn  string            `json:"SubscriptionArn"`
	SubscriptionName string            `json:"SubscriptionName"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Frequency        string            `json:"Frequency"`
	Threshold        float64           `json:"Threshold,omitempty"`
	Subscribers      []subscriberInput `json:"Subscribers"`
}

type getAnomalySubscriptionsOutput struct {
	AnomalySubscriptions []anomalySubscriptionSummary `json:"AnomalySubscriptions"`
	NextPageToken        string                       `json:"NextPageToken,omitempty"`
}

func (h *Handler) handleGetAnomalySubscriptions(
	_ context.Context,
	in *getAnomalySubscriptionsInput,
) (*getAnomalySubscriptionsOutput, error) {
	subs := h.Backend.GetAnomalySubscriptions(in.SubscriptionArnList)
	items := make([]anomalySubscriptionSummary, 0, len(subs))
	for _, sub := range subs {
		subscribers := make([]subscriberInput, 0, len(sub.Subscribers))
		for _, s := range sub.Subscribers {
			subscribers = append(subscribers, subscriberInput{
				Address: s.Address,
				Type:    s.Type,
				Status:  s.Status,
			})
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
		subs = append(subs, Subscriber{
			Address: s.Address,
			Type:    s.Type,
			Status:  s.Status,
		})
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
	TimePeriod map[string]string `json:"TimePeriod"`
	Granularity string           `json:"Granularity"`
	Metrics    []string          `json:"Metrics"`
}

type getCostAndUsageOutput struct {
	ResultsByTime          []any `json:"ResultsByTime"`
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
	TimePeriod  map[string]string `json:"TimePeriod"`
	Dimension   string            `json:"Dimension"`
	SearchString string           `json:"SearchString"`
}

type getDimensionValuesOutput struct {
	DimensionValues  []any  `json:"DimensionValues"`
	ReturnSize       int    `json:"ReturnSize"`
	TotalSize        int    `json:"TotalSize"`
	NextPageToken    string `json:"NextPageToken,omitempty"`
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
	TimePeriod    map[string]string `json:"TimePeriod"`
	TagKey        string            `json:"TagKey"`
	SearchString  string            `json:"SearchString"`
}

type getTagsOutput struct {
	Tags          []string `json:"Tags"`
	ReturnSize    int      `json:"ReturnSize"`
	TotalSize     int      `json:"TotalSize"`
	NextPageToken string   `json:"NextPageToken,omitempty"`
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
	ResourceTags map[string]string `json:"ResourceTags"`
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

	return &listTagsForResourceOutput{ResourceTags: t}, nil
}

type tagResourceInput struct {
	ResourceTags map[string]string `json:"ResourceTags"`
	ResourceArn  string            `json:"ResourceArn"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, in.ResourceTags); err != nil {
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
