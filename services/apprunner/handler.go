package apprunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	apprunnerTargetPrefix = "AppRunner."
	matchPriority         = service.PriorityHeaderExact
	contentType           = "application/x-amz-json-1.0"

	keyType    = "__type"
	keyMessage = "message"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler handles App Runner HTTP requests.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppRunner" }

// Reset resets the backend and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.ops = h.buildOps()
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateCustomDomain",
		"CreateAutoScalingConfiguration",
		"CreateConnection",
		"CreateObservabilityConfiguration",
		"CreateService",
		"CreateVpcConnector",
		"CreateVpcIngressConnection",
		"DeleteAutoScalingConfiguration",
		"DeleteConnection",
		"DeleteObservabilityConfiguration",
		"DeleteService",
		"DeleteVpcConnector",
		"DeleteVpcIngressConnection",
		"DescribeAutoScalingConfiguration",
		"DescribeCustomDomains",
		"DescribeObservabilityConfiguration",
		"DescribeService",
		"DescribeVpcConnector",
		"DescribeVpcIngressConnection",
		"DisassociateCustomDomain",
		"ListAutoScalingConfigurations",
		"ListConnections",
		"ListObservabilityConfigurations",
		"ListOperations",
		"ListServices",
		"ListServicesForAutoScalingConfiguration",
		"ListTagsForResource",
		"ListVpcConnectors",
		"ListVpcIngressConnections",
		"PauseService",
		"ResumeService",
		"StartDeployment",
		"TagResource",
		"UntagResource",
		"UpdateDefaultAutoScalingConfiguration",
		"UpdateService",
		"UpdateVpcIngressConnection",
	}
}

// RouteMatcher returns a function that matches App Runner API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), apprunnerTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, apprunnerTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function for App Runner requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"AppRunner", contentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateCustomDomain":                   service.WrapOp(h.handleAssociateCustomDomain),
		"CreateAutoScalingConfiguration":          service.WrapOp(h.handleCreateAutoScalingConfiguration),
		"CreateConnection":                        service.WrapOp(h.handleCreateConnection),
		"CreateObservabilityConfiguration":        service.WrapOp(h.handleCreateObservabilityConfiguration),
		"CreateService":                           service.WrapOp(h.handleCreateService),
		"CreateVpcConnector":                      service.WrapOp(h.handleCreateVpcConnector),
		"CreateVpcIngressConnection":              service.WrapOp(h.handleCreateVpcIngressConnection),
		"DeleteAutoScalingConfiguration":          service.WrapOp(h.handleDeleteAutoScalingConfiguration),
		"DeleteConnection":                        service.WrapOp(h.handleDeleteConnection),
		"DeleteObservabilityConfiguration":        service.WrapOp(h.handleDeleteObservabilityConfiguration),
		"DeleteService":                           service.WrapOp(h.handleDeleteService),
		"DeleteVpcConnector":                      service.WrapOp(h.handleDeleteVpcConnector),
		"DeleteVpcIngressConnection":              service.WrapOp(h.handleDeleteVpcIngressConnection),
		"DescribeAutoScalingConfiguration":        service.WrapOp(h.handleDescribeAutoScalingConfiguration),
		"DescribeCustomDomains":                   service.WrapOp(h.handleDescribeCustomDomains),
		"DescribeObservabilityConfiguration":      service.WrapOp(h.handleDescribeObservabilityConfiguration),
		"DescribeService":                         service.WrapOp(h.handleDescribeService),
		"DescribeVpcConnector":                    service.WrapOp(h.handleDescribeVpcConnector),
		"DescribeVpcIngressConnection":            service.WrapOp(h.handleDescribeVpcIngressConnection),
		"DisassociateCustomDomain":                service.WrapOp(h.handleDisassociateCustomDomain),
		"ListAutoScalingConfigurations":           service.WrapOp(h.handleListAutoScalingConfigurations),
		"ListConnections":                         service.WrapOp(h.handleListConnections),
		"ListObservabilityConfigurations":         service.WrapOp(h.handleListObservabilityConfigurations),
		"ListOperations":                          service.WrapOp(h.handleListOperations),
		"ListServices":                            service.WrapOp(h.handleListServices),
		"ListServicesForAutoScalingConfiguration": service.WrapOp(h.handleListServicesForAutoScalingConfiguration),
		"ListTagsForResource":                     service.WrapOp(h.handleListTagsForResource),
		"ListVpcConnectors":                       service.WrapOp(h.handleListVpcConnectors),
		"ListVpcIngressConnections":               service.WrapOp(h.handleListVpcIngressConnections),
		"PauseService":                            service.WrapOp(h.handlePauseService),
		"ResumeService":                           service.WrapOp(h.handleResumeService),
		"StartDeployment":                         service.WrapOp(h.handleStartDeployment),
		"TagResource":                             service.WrapOp(h.handleTagResource),
		"UntagResource":                           service.WrapOp(h.handleUntagResource),
		"UpdateDefaultAutoScalingConfiguration":   service.WrapOp(h.handleUpdateDefaultAutoScalingConfiguration),
		"UpdateService":                           service.WrapOp(h.handleUpdateService),
		"UpdateVpcIngressConnection":              service.WrapOp(h.handleUpdateVpcIngressConnection),
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
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "InvalidParameterException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "ServiceQuotaExceededException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "InvalidStateException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "InvalidRequestException",
			keyMessage: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyType:    "InternalServiceError",
			keyMessage: err.Error(),
		})
	}
}

// --- Service operations ---

type imageRepositoryInput struct {
	ImageIdentifier     string `json:"ImageIdentifier"`
	ImageRepositoryType string `json:"ImageRepositoryType"`
}

type sourceConfigurationInput struct {
	ImageRepository *imageRepositoryInput `json:"ImageRepository,omitempty"`
}

type instanceConfigurationInput struct {
	CPU    string `json:"Cpu,omitempty"`
	Memory string `json:"Memory,omitempty"`
}

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type createServiceInput struct {
	ServiceName           string                      `json:"ServiceName"`
	SourceConfiguration   *sourceConfigurationInput   `json:"SourceConfiguration,omitempty"`
	InstanceConfiguration *instanceConfigurationInput `json:"InstanceConfiguration,omitempty"`
	Tags                  []tagInput                  `json:"Tags"`
}

type serviceOutput struct {
	ServiceArn  string `json:"ServiceArn"`
	ServiceID   string `json:"ServiceId"`
	ServiceName string `json:"ServiceName"`
	ServiceURL  string `json:"ServiceUrl"`
	Status      string `json:"Status"`
	CreatedAt   int64  `json:"CreatedAt"`
	UpdatedAt   int64  `json:"UpdatedAt"`
}

type createServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func toServiceOutput(svc *Service) serviceOutput {
	return serviceOutput{
		ServiceArn:  svc.ServiceArn,
		ServiceID:   svc.ServiceID,
		ServiceName: svc.ServiceName,
		ServiceURL:  svc.ServiceURL,
		Status:      svc.Status,
		CreatedAt:   svc.CreatedAt.Unix(),
		UpdatedAt:   svc.UpdatedAt.Unix(),
	}
}

func (h *Handler) handleCreateService(
	_ context.Context,
	in *createServiceInput,
) (*createServiceOutput, error) {
	if in.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	var cpu, memory, imageURI string

	if in.InstanceConfiguration != nil {
		cpu = in.InstanceConfiguration.CPU
		memory = in.InstanceConfiguration.Memory
	}

	if in.SourceConfiguration != nil && in.SourceConfiguration.ImageRepository != nil {
		imageURI = in.SourceConfiguration.ImageRepository.ImageIdentifier
	}

	tags := tagsFromInput(in.Tags)

	svc, err := h.Backend.CreateService(in.ServiceName, cpu, memory, imageURI, tags)
	if err != nil {
		return nil, err
	}

	return &createServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type describeServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type describeServiceOutput struct {
	Service serviceOutput `json:"Service"`
}

func (h *Handler) handleDescribeService(
	_ context.Context,
	in *describeServiceInput,
) (*describeServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.DescribeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &describeServiceOutput{Service: toServiceOutput(svc)}, nil
}

type updateServiceInput struct {
	SourceConfiguration   *sourceConfigurationInput   `json:"SourceConfiguration,omitempty"`
	InstanceConfiguration *instanceConfigurationInput `json:"InstanceConfiguration,omitempty"`
	ServiceArn            string                      `json:"ServiceArn"`
}

type updateServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleUpdateService(
	_ context.Context,
	in *updateServiceInput,
) (*updateServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	var cpu, memory, imageURI string

	if in.InstanceConfiguration != nil {
		cpu = in.InstanceConfiguration.CPU
		memory = in.InstanceConfiguration.Memory
	}

	if in.SourceConfiguration != nil && in.SourceConfiguration.ImageRepository != nil {
		imageURI = in.SourceConfiguration.ImageRepository.ImageIdentifier
	}

	svc, err := h.Backend.UpdateService(in.ServiceArn, cpu, memory, imageURI)
	if err != nil {
		return nil, err
	}

	return &updateServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type deleteServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type deleteServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleDeleteService(
	_ context.Context,
	in *deleteServiceInput,
) (*deleteServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.DeleteService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &deleteServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type listServicesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type serviceSummaryOutput struct {
	ServiceArn  string `json:"ServiceArn"`
	ServiceID   string `json:"ServiceId"`
	ServiceName string `json:"ServiceName"`
	ServiceURL  string `json:"ServiceUrl"`
	Status      string `json:"Status"`
	CreatedAt   int64  `json:"CreatedAt"`
}

type listServicesOutput struct {
	NextToken          string                 `json:"NextToken,omitempty"`
	ServiceSummaryList []serviceSummaryOutput `json:"ServiceSummaryList"`
}

func (h *Handler) handleListServices(
	_ context.Context,
	in *listServicesInput,
) (*listServicesOutput, error) {
	services, nextToken, err := h.Backend.ListServices(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]serviceSummaryOutput, 0, len(services))
	for _, s := range services {
		out = append(out, serviceSummaryOutput{
			ServiceArn:  s.ServiceArn,
			ServiceID:   s.ServiceID,
			ServiceName: s.ServiceName,
			ServiceURL:  s.ServiceURL,
			Status:      s.Status,
			CreatedAt:   s.CreatedAt.Unix(),
		})
	}

	return &listServicesOutput{ServiceSummaryList: out, NextToken: nextToken}, nil
}

type pauseServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type pauseServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handlePauseService(
	_ context.Context,
	in *pauseServiceInput,
) (*pauseServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.PauseService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &pauseServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type resumeServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type resumeServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleResumeService(
	_ context.Context,
	in *resumeServiceInput,
) (*resumeServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.ResumeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &resumeServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type startDeploymentInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type startDeploymentOutput struct {
	OperationID string `json:"OperationId"`
}

func (h *Handler) handleStartDeployment(
	_ context.Context,
	in *startDeploymentInput,
) (*startDeploymentOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	opID, err := h.Backend.StartDeployment(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &startDeploymentOutput{OperationID: opID}, nil
}

type listOperationsInput struct {
	ServiceArn string `json:"ServiceArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type operationSummaryOutput struct {
	ID        string `json:"Id"`
	Type      string `json:"Type"`
	Status    string `json:"Status"`
	TargetArn string `json:"TargetArn"`
	StartedAt int64  `json:"StartedAt"`
	EndedAt   int64  `json:"EndedAt"`
}

type listOperationsOutput struct {
	NextToken            string                   `json:"NextToken,omitempty"`
	OperationSummaryList []operationSummaryOutput `json:"OperationSummaryList"`
}

func (h *Handler) handleListOperations(
	_ context.Context,
	in *listOperationsInput,
) (*listOperationsOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	ops, nextToken, err := h.Backend.ListOperations(in.ServiceArn, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]operationSummaryOutput, 0, len(ops))
	for _, op := range ops {
		out = append(out, operationSummaryOutput{
			ID:        op.ID,
			Type:      op.Type,
			Status:    op.Status,
			TargetArn: op.TargetArn,
			StartedAt: op.StartedAt.Unix(),
			EndedAt:   op.EndedAt.Unix(),
		})
	}

	return &listOperationsOutput{OperationSummaryList: out, NextToken: nextToken}, nil
}

// --- Tag operations ---

type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagInput `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	if err := h.Backend.TagResource(in.ResourceArn, tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagInput `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	// Sort keys for stable output.
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]tagInput, 0, len(keys))
	for _, k := range keys {
		out = append(out, tagInput{Key: k, Value: tags[k]})
	}

	return &listTagsForResourceOutput{Tags: out}, nil
}

// tagsFromInput converts a slice of tagInput to a map.
func tagsFromInput(inputs []tagInput) map[string]string {
	if len(inputs) == 0 {
		return nil
	}

	m := make(map[string]string, len(inputs))
	for _, t := range inputs {
		m[t.Key] = t.Value
	}

	return m
}

// --- AutoScalingConfiguration handler types and methods ---

type createAutoScalingConfigurationInput struct {
	AutoScalingConfigurationName string     `json:"AutoScalingConfigurationName"`
	Tags                         []tagInput `json:"Tags"`
	MaxConcurrency               int32      `json:"MaxConcurrency"`
	MaxSize                      int32      `json:"MaxSize"`
	MinSize                      int32      `json:"MinSize"`
}

type autoScalingConfigurationOutput struct {
	AutoScalingConfigurationArn      string `json:"AutoScalingConfigurationArn"`
	AutoScalingConfigurationName     string `json:"AutoScalingConfigurationName"`
	Status                           string `json:"Status"`
	AutoScalingConfigurationRevision int32  `json:"AutoScalingConfigurationRevision"`
	MaxConcurrency                   int32  `json:"MaxConcurrency"`
	MaxSize                          int32  `json:"MaxSize"`
	MinSize                          int32  `json:"MinSize"`
	IsDefault                        bool   `json:"IsDefault"`
	HasAssociatedService             bool   `json:"HasAssociatedService"`
	CreatedAt                        int64  `json:"CreatedAt"`
}

type createAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func toAutoScalingConfigurationOutput(cfg *AutoScalingConfiguration) autoScalingConfigurationOutput {
	return autoScalingConfigurationOutput{
		AutoScalingConfigurationArn:      cfg.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     cfg.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: cfg.AutoScalingConfigurationRevision,
		Status:                           cfg.Status,
		MaxConcurrency:                   cfg.MaxConcurrency,
		MaxSize:                          cfg.MaxSize,
		MinSize:                          cfg.MinSize,
		IsDefault:                        cfg.IsDefault,
		HasAssociatedService:             cfg.HasAssociatedService,
		CreatedAt:                        cfg.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateAutoScalingConfiguration(
	_ context.Context,
	in *createAutoScalingConfigurationInput,
) (*createAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationName == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationName is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	cfg, err := h.Backend.CreateAutoScalingConfiguration(
		in.AutoScalingConfigurationName,
		in.MaxConcurrency, in.MaxSize, in.MinSize,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type describeAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
}

type describeAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func (h *Handler) handleDescribeAutoScalingConfiguration(
	_ context.Context,
	in *describeAutoScalingConfigurationInput,
) (*describeAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DescribeAutoScalingConfiguration(in.AutoScalingConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &describeAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type deleteAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
}

type deleteAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func (h *Handler) handleDeleteAutoScalingConfiguration(
	_ context.Context,
	in *deleteAutoScalingConfigurationInput,
) (*deleteAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DeleteAutoScalingConfiguration(in.AutoScalingConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &deleteAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type autoScalingConfigurationSummaryOutput struct {
	AutoScalingConfigurationArn      string `json:"AutoScalingConfigurationArn"`
	AutoScalingConfigurationName     string `json:"AutoScalingConfigurationName"`
	Status                           string `json:"Status"`
	AutoScalingConfigurationRevision int32  `json:"AutoScalingConfigurationRevision"`
	IsDefault                        bool   `json:"IsDefault"`
	CreatedAt                        int64  `json:"CreatedAt"`
}

type listAutoScalingConfigurationsInput struct {
	AutoScalingConfigurationName string `json:"AutoScalingConfigurationName"`
	NextToken                    string `json:"NextToken"`
	MaxResults                   int32  `json:"MaxResults"`
	LatestOnly                   bool   `json:"LatestOnly"`
}

type listAutoScalingConfigurationsOutput struct {
	AutoScalingConfigurationSummaryList []autoScalingConfigurationSummaryOutput `json:"AutoScalingConfigurationSummaryList"`
	NextToken                           string                                  `json:"NextToken,omitempty"`
}

func (h *Handler) handleListAutoScalingConfigurations(
	_ context.Context,
	in *listAutoScalingConfigurationsInput,
) (*listAutoScalingConfigurationsOutput, error) {
	cfgs, nextToken, err := h.Backend.ListAutoScalingConfigurations(
		in.AutoScalingConfigurationName,
		in.LatestOnly,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]autoScalingConfigurationSummaryOutput, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, autoScalingConfigurationSummaryOutput{
			AutoScalingConfigurationArn:      c.AutoScalingConfigurationArn,
			AutoScalingConfigurationName:     c.AutoScalingConfigurationName,
			AutoScalingConfigurationRevision: c.AutoScalingConfigurationRevision,
			Status:                           c.Status,
			IsDefault:                        c.IsDefault,
			CreatedAt:                        c.CreatedAt.Unix(),
		})
	}

	return &listAutoScalingConfigurationsOutput{
		AutoScalingConfigurationSummaryList: out,
		NextToken:                           nextToken,
	}, nil
}

type updateDefaultAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
}

type updateDefaultAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func (h *Handler) handleUpdateDefaultAutoScalingConfiguration(
	_ context.Context,
	in *updateDefaultAutoScalingConfigurationInput,
) (*updateDefaultAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.UpdateDefaultAutoScalingConfiguration(in.AutoScalingConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &updateDefaultAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type listServicesForAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
	NextToken                   string `json:"NextToken"`
	MaxResults                  int32  `json:"MaxResults"`
}

type listServicesForAutoScalingConfigurationOutput struct {
	ServiceArnList []string `json:"ServiceArnList"`
	NextToken      string   `json:"NextToken,omitempty"`
}

func (h *Handler) handleListServicesForAutoScalingConfiguration(
	_ context.Context,
	in *listServicesForAutoScalingConfigurationInput,
) (*listServicesForAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	arns, nextToken, err := h.Backend.ListServicesForAutoScalingConfiguration(
		in.AutoScalingConfigurationArn,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return &listServicesForAutoScalingConfigurationOutput{
		ServiceArnList: arns,
		NextToken:      nextToken,
	}, nil
}

// --- Connection handler types and methods ---

type createConnectionInput struct {
	ConnectionName string     `json:"ConnectionName"`
	ProviderType   string     `json:"ProviderType"`
	Tags           []tagInput `json:"Tags"`
}

type connectionOutput struct {
	ConnectionArn  string `json:"ConnectionArn"`
	ConnectionName string `json:"ConnectionName"`
	ProviderType   string `json:"ProviderType"`
	Status         string `json:"Status"`
	CreatedAt      int64  `json:"CreatedAt"`
}

type createConnectionOutput struct {
	Connection connectionOutput `json:"Connection"`
}

func toConnectionOutput(c *Connection) connectionOutput {
	return connectionOutput{
		ConnectionArn:  c.ConnectionArn,
		ConnectionName: c.ConnectionName,
		ProviderType:   c.ProviderType,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errInvalidRequest)
	}

	if in.ProviderType == "" {
		return nil, fmt.Errorf("%w: ProviderType is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	conn, err := h.Backend.CreateConnection(in.ConnectionName, in.ProviderType, tags)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{Connection: toConnectionOutput(conn)}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type deleteConnectionOutput struct {
	Connection connectionOutput `json:"Connection"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	conn, err := h.Backend.DeleteConnection(in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{Connection: toConnectionOutput(conn)}, nil
}

type listConnectionsInput struct {
	ConnectionName string `json:"ConnectionName"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type connectionSummaryOutput struct {
	ConnectionArn  string `json:"ConnectionArn"`
	ConnectionName string `json:"ConnectionName"`
	ProviderType   string `json:"ProviderType"`
	Status         string `json:"Status"`
	CreatedAt      int64  `json:"CreatedAt"`
}

type listConnectionsOutput struct {
	ConnectionSummaryList []connectionSummaryOutput `json:"ConnectionSummaryList"`
	NextToken             string                    `json:"NextToken,omitempty"`
}

func (h *Handler) handleListConnections(
	_ context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	conns, nextToken, err := h.Backend.ListConnections(in.ConnectionName, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]connectionSummaryOutput, 0, len(conns))
	for _, c := range conns {
		out = append(out, connectionSummaryOutput{
			ConnectionArn:  c.ConnectionArn,
			ConnectionName: c.ConnectionName,
			ProviderType:   c.ProviderType,
			Status:         c.Status,
			CreatedAt:      c.CreatedAt.Unix(),
		})
	}

	return &listConnectionsOutput{ConnectionSummaryList: out, NextToken: nextToken}, nil
}

// --- ObservabilityConfiguration handler types and methods ---

type traceConfigurationInput struct {
	Vendor string `json:"Vendor"`
}

type createObservabilityConfigurationInput struct {
	ObservabilityConfigurationName string                   `json:"ObservabilityConfigurationName"`
	Tags                           []tagInput               `json:"Tags"`
	TraceConfiguration             *traceConfigurationInput `json:"TraceConfiguration"`
}

type observabilityConfigurationOutput struct {
	ObservabilityConfigurationArn      string `json:"ObservabilityConfigurationArn"`
	ObservabilityConfigurationName     string `json:"ObservabilityConfigurationName"`
	Status                             string `json:"Status"`
	ObservabilityConfigurationRevision int32  `json:"ObservabilityConfigurationRevision"`
	Latest                             bool   `json:"Latest"`
	CreatedAt                          int64  `json:"CreatedAt"`
}

type createObservabilityConfigurationOutput struct {
	ObservabilityConfiguration observabilityConfigurationOutput `json:"ObservabilityConfiguration"`
}

func toObservabilityConfigurationOutput(cfg *ObservabilityConfiguration) observabilityConfigurationOutput {
	return observabilityConfigurationOutput{
		ObservabilityConfigurationArn:      cfg.ObservabilityConfigurationArn,
		ObservabilityConfigurationName:     cfg.ObservabilityConfigurationName,
		ObservabilityConfigurationRevision: cfg.ObservabilityConfigurationRevision,
		Status:                             cfg.Status,
		Latest:                             cfg.Latest,
		CreatedAt:                          cfg.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateObservabilityConfiguration(
	_ context.Context,
	in *createObservabilityConfigurationInput,
) (*createObservabilityConfigurationOutput, error) {
	if in.ObservabilityConfigurationName == "" {
		return nil, fmt.Errorf("%w: ObservabilityConfigurationName is required", errInvalidRequest)
	}

	vendor := ""
	if in.TraceConfiguration != nil {
		vendor = in.TraceConfiguration.Vendor
	}

	tags := tagsFromInput(in.Tags)
	cfg, err := h.Backend.CreateObservabilityConfiguration(in.ObservabilityConfigurationName, vendor, tags)
	if err != nil {
		return nil, err
	}

	return &createObservabilityConfigurationOutput{
		ObservabilityConfiguration: toObservabilityConfigurationOutput(cfg),
	}, nil
}

type describeObservabilityConfigurationInput struct {
	ObservabilityConfigurationArn string `json:"ObservabilityConfigurationArn"`
}

type describeObservabilityConfigurationOutput struct {
	ObservabilityConfiguration observabilityConfigurationOutput `json:"ObservabilityConfiguration"`
}

func (h *Handler) handleDescribeObservabilityConfiguration(
	_ context.Context,
	in *describeObservabilityConfigurationInput,
) (*describeObservabilityConfigurationOutput, error) {
	if in.ObservabilityConfigurationArn == "" {
		return nil, fmt.Errorf("%w: ObservabilityConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DescribeObservabilityConfiguration(in.ObservabilityConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &describeObservabilityConfigurationOutput{
		ObservabilityConfiguration: toObservabilityConfigurationOutput(cfg),
	}, nil
}

type deleteObservabilityConfigurationInput struct {
	ObservabilityConfigurationArn string `json:"ObservabilityConfigurationArn"`
}

type deleteObservabilityConfigurationOutput struct {
	ObservabilityConfiguration observabilityConfigurationOutput `json:"ObservabilityConfiguration"`
}

func (h *Handler) handleDeleteObservabilityConfiguration(
	_ context.Context,
	in *deleteObservabilityConfigurationInput,
) (*deleteObservabilityConfigurationOutput, error) {
	if in.ObservabilityConfigurationArn == "" {
		return nil, fmt.Errorf("%w: ObservabilityConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DeleteObservabilityConfiguration(in.ObservabilityConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &deleteObservabilityConfigurationOutput{
		ObservabilityConfiguration: toObservabilityConfigurationOutput(cfg),
	}, nil
}

type listObservabilityConfigurationsInput struct {
	ObservabilityConfigurationName string `json:"ObservabilityConfigurationName"`
	NextToken                      string `json:"NextToken"`
	MaxResults                     int32  `json:"MaxResults"`
	LatestOnly                     bool   `json:"LatestOnly"`
}

type observabilityConfigurationSummaryOutput struct {
	ObservabilityConfigurationArn      string `json:"ObservabilityConfigurationArn"`
	ObservabilityConfigurationName     string `json:"ObservabilityConfigurationName"`
	Status                             string `json:"Status"`
	ObservabilityConfigurationRevision int32  `json:"ObservabilityConfigurationRevision"`
	Latest                             bool   `json:"Latest"`
	CreatedAt                          int64  `json:"CreatedAt"`
}

type listObservabilityConfigurationsOutput struct {
	ObservabilityConfigurationSummaryList []observabilityConfigurationSummaryOutput `json:"ObservabilityConfigurationSummaryList"`
	NextToken                             string                                    `json:"NextToken,omitempty"`
}

func (h *Handler) handleListObservabilityConfigurations(
	_ context.Context,
	in *listObservabilityConfigurationsInput,
) (*listObservabilityConfigurationsOutput, error) {
	cfgs, nextToken, err := h.Backend.ListObservabilityConfigurations(
		in.ObservabilityConfigurationName,
		in.LatestOnly,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]observabilityConfigurationSummaryOutput, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, observabilityConfigurationSummaryOutput{
			ObservabilityConfigurationArn:      c.ObservabilityConfigurationArn,
			ObservabilityConfigurationName:     c.ObservabilityConfigurationName,
			ObservabilityConfigurationRevision: c.ObservabilityConfigurationRevision,
			Status:                             c.Status,
			Latest:                             c.Latest,
			CreatedAt:                          c.CreatedAt.Unix(),
		})
	}

	return &listObservabilityConfigurationsOutput{
		ObservabilityConfigurationSummaryList: out,
		NextToken:                             nextToken,
	}, nil
}

// --- VpcConnector handler types and methods ---

type createVpcConnectorInput struct {
	VpcConnectorName string     `json:"VpcConnectorName"`
	Subnets          []string   `json:"Subnets"`
	SecurityGroups   []string   `json:"SecurityGroups"`
	Tags             []tagInput `json:"Tags"`
}

type vpcConnectorOutput struct {
	VpcConnectorArn      string   `json:"VpcConnectorArn"`
	VpcConnectorName     string   `json:"VpcConnectorName"`
	Status               string   `json:"Status"`
	VpcConnectorRevision int32    `json:"VpcConnectorRevision"`
	Subnets              []string `json:"Subnets"`
	SecurityGroups       []string `json:"SecurityGroups"`
	CreatedAt            int64    `json:"CreatedAt"`
}

type createVpcConnectorOutput struct {
	VpcConnector vpcConnectorOutput `json:"VpcConnector"`
}

func toVpcConnectorOutput(vc *VpcConnector) vpcConnectorOutput {
	sg := vc.SecurityGroups
	if sg == nil {
		sg = []string{}
	}
	sn := vc.Subnets
	if sn == nil {
		sn = []string{}
	}

	return vpcConnectorOutput{
		VpcConnectorArn:      vc.VpcConnectorArn,
		VpcConnectorName:     vc.VpcConnectorName,
		VpcConnectorRevision: vc.VpcConnectorRevision,
		Status:               vc.Status,
		Subnets:              sn,
		SecurityGroups:       sg,
		CreatedAt:            vc.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateVpcConnector(
	_ context.Context,
	in *createVpcConnectorInput,
) (*createVpcConnectorOutput, error) {
	if in.VpcConnectorName == "" {
		return nil, fmt.Errorf("%w: VpcConnectorName is required", errInvalidRequest)
	}

	if len(in.Subnets) == 0 {
		return nil, fmt.Errorf("%w: Subnets is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	vc, err := h.Backend.CreateVpcConnector(in.VpcConnectorName, in.Subnets, in.SecurityGroups, tags)
	if err != nil {
		return nil, err
	}

	return &createVpcConnectorOutput{VpcConnector: toVpcConnectorOutput(vc)}, nil
}

type describeVpcConnectorInput struct {
	VpcConnectorArn string `json:"VpcConnectorArn"`
}

type describeVpcConnectorOutput struct {
	VpcConnector vpcConnectorOutput `json:"VpcConnector"`
}

func (h *Handler) handleDescribeVpcConnector(
	_ context.Context,
	in *describeVpcConnectorInput,
) (*describeVpcConnectorOutput, error) {
	if in.VpcConnectorArn == "" {
		return nil, fmt.Errorf("%w: VpcConnectorArn is required", errInvalidRequest)
	}

	vc, err := h.Backend.DescribeVpcConnector(in.VpcConnectorArn)
	if err != nil {
		return nil, err
	}

	return &describeVpcConnectorOutput{VpcConnector: toVpcConnectorOutput(vc)}, nil
}

type deleteVpcConnectorInput struct {
	VpcConnectorArn string `json:"VpcConnectorArn"`
}

type deleteVpcConnectorOutput struct {
	VpcConnector vpcConnectorOutput `json:"VpcConnector"`
}

func (h *Handler) handleDeleteVpcConnector(
	_ context.Context,
	in *deleteVpcConnectorInput,
) (*deleteVpcConnectorOutput, error) {
	if in.VpcConnectorArn == "" {
		return nil, fmt.Errorf("%w: VpcConnectorArn is required", errInvalidRequest)
	}

	vc, err := h.Backend.DeleteVpcConnector(in.VpcConnectorArn)
	if err != nil {
		return nil, err
	}

	return &deleteVpcConnectorOutput{VpcConnector: toVpcConnectorOutput(vc)}, nil
}

type listVpcConnectorsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listVpcConnectorsOutput struct {
	VpcConnectors []vpcConnectorOutput `json:"VpcConnectors"`
	NextToken     string               `json:"NextToken,omitempty"`
}

func (h *Handler) handleListVpcConnectors(
	_ context.Context,
	in *listVpcConnectorsInput,
) (*listVpcConnectorsOutput, error) {
	vcs, nextToken, err := h.Backend.ListVpcConnectors(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]vpcConnectorOutput, 0, len(vcs))
	for _, vc := range vcs {
		out = append(out, toVpcConnectorOutput(vc))
	}

	return &listVpcConnectorsOutput{VpcConnectors: out, NextToken: nextToken}, nil
}

// --- VpcIngressConnection handler types and methods ---

type ingressVpcConfigurationInput struct {
	VpcId         string `json:"VpcId"`
	VpcEndpointId string `json:"VpcEndpointId"`
}

type createVpcIngressConnectionInput struct {
	VpcIngressConnectionName string                        `json:"VpcIngressConnectionName"`
	ServiceArn               string                        `json:"ServiceArn"`
	IngressVpcConfiguration  *ingressVpcConfigurationInput `json:"IngressVpcConfiguration"`
	Tags                     []tagInput                    `json:"Tags"`
}

type ingressVpcConfigurationOutput struct {
	VpcId         string `json:"VpcId"`
	VpcEndpointId string `json:"VpcEndpointId"`
}

type vpcIngressConnectionOutput struct {
	VpcIngressConnectionArn  string                        `json:"VpcIngressConnectionArn"`
	VpcIngressConnectionName string                        `json:"VpcIngressConnectionName"`
	ServiceArn               string                        `json:"ServiceArn"`
	AccountId                string                        `json:"AccountId"`
	DomainName               string                        `json:"DomainName"`
	Status                   string                        `json:"Status"`
	IngressVpcConfiguration  ingressVpcConfigurationOutput `json:"IngressVpcConfiguration"`
	CreatedAt                int64                         `json:"CreatedAt"`
}

type createVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func toVpcIngressConnectionOutput(v *VpcIngressConnection) vpcIngressConnectionOutput {
	return vpcIngressConnectionOutput{
		VpcIngressConnectionArn:  v.VpcIngressConnectionArn,
		VpcIngressConnectionName: v.VpcIngressConnectionName,
		ServiceArn:               v.ServiceArn,
		AccountId:                v.AccountID,
		DomainName:               v.DomainName,
		Status:                   v.Status,
		IngressVpcConfiguration: ingressVpcConfigurationOutput{
			VpcId:         v.VpcID,
			VpcEndpointId: v.VpcEndpointID,
		},
		CreatedAt: v.CreatedAt.Unix(),
	}
}

func (h *Handler) handleCreateVpcIngressConnection(
	_ context.Context,
	in *createVpcIngressConnectionInput,
) (*createVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionName == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionName is required", errInvalidRequest)
	}

	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	var vpcID, vpcEndpointID string
	if in.IngressVpcConfiguration != nil {
		vpcID = in.IngressVpcConfiguration.VpcId
		vpcEndpointID = in.IngressVpcConfiguration.VpcEndpointId
	}

	tags := tagsFromInput(in.Tags)
	vic, err := h.Backend.CreateVpcIngressConnection(
		in.VpcIngressConnectionName, in.ServiceArn, vpcID, vpcEndpointID, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

type describeVpcIngressConnectionInput struct {
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
}

type describeVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func (h *Handler) handleDescribeVpcIngressConnection(
	_ context.Context,
	in *describeVpcIngressConnectionInput,
) (*describeVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionArn == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionArn is required", errInvalidRequest)
	}

	vic, err := h.Backend.DescribeVpcIngressConnection(in.VpcIngressConnectionArn)
	if err != nil {
		return nil, err
	}

	return &describeVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

type deleteVpcIngressConnectionInput struct {
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
}

type deleteVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func (h *Handler) handleDeleteVpcIngressConnection(
	_ context.Context,
	in *deleteVpcIngressConnectionInput,
) (*deleteVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionArn == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionArn is required", errInvalidRequest)
	}

	vic, err := h.Backend.DeleteVpcIngressConnection(in.VpcIngressConnectionArn)
	if err != nil {
		return nil, err
	}

	return &deleteVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

type listVpcIngressConnectionsFilterInput struct {
	ServiceArn              string `json:"ServiceArn"`
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
}

type listVpcIngressConnectionsInput struct {
	Filter     *listVpcIngressConnectionsFilterInput `json:"Filter"`
	NextToken  string                                `json:"NextToken"`
	MaxResults int32                                 `json:"MaxResults"`
}

type vpcIngressConnectionSummaryOutput struct {
	VpcIngressConnectionArn string `json:"VpcIngressConnectionArn"`
	ServiceArn              string `json:"ServiceArn"`
}

type listVpcIngressConnectionsOutput struct {
	VpcIngressConnectionSummaryList []vpcIngressConnectionSummaryOutput `json:"VpcIngressConnectionSummaryList"`
	NextToken                       string                              `json:"NextToken,omitempty"`
}

func (h *Handler) handleListVpcIngressConnections(
	_ context.Context,
	in *listVpcIngressConnectionsInput,
) (*listVpcIngressConnectionsOutput, error) {
	var serviceArnFilter, connArnFilter string
	if in.Filter != nil {
		serviceArnFilter = in.Filter.ServiceArn
		connArnFilter = in.Filter.VpcIngressConnectionArn
	}

	vics, nextToken, err := h.Backend.ListVpcIngressConnections(
		serviceArnFilter, connArnFilter, in.MaxResults, in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]vpcIngressConnectionSummaryOutput, 0, len(vics))
	for _, v := range vics {
		out = append(out, vpcIngressConnectionSummaryOutput{
			VpcIngressConnectionArn: v.VpcIngressConnectionArn,
			ServiceArn:              v.ServiceArn,
		})
	}

	return &listVpcIngressConnectionsOutput{
		VpcIngressConnectionSummaryList: out,
		NextToken:                       nextToken,
	}, nil
}

type updateVpcIngressConnectionInput struct {
	VpcIngressConnectionArn string                        `json:"VpcIngressConnectionArn"`
	IngressVpcConfiguration *ingressVpcConfigurationInput `json:"IngressVpcConfiguration"`
}

type updateVpcIngressConnectionOutput struct {
	VpcIngressConnection vpcIngressConnectionOutput `json:"VpcIngressConnection"`
}

func (h *Handler) handleUpdateVpcIngressConnection(
	_ context.Context,
	in *updateVpcIngressConnectionInput,
) (*updateVpcIngressConnectionOutput, error) {
	if in.VpcIngressConnectionArn == "" {
		return nil, fmt.Errorf("%w: VpcIngressConnectionArn is required", errInvalidRequest)
	}

	var vpcID, vpcEndpointID string
	if in.IngressVpcConfiguration != nil {
		vpcID = in.IngressVpcConfiguration.VpcId
		vpcEndpointID = in.IngressVpcConfiguration.VpcEndpointId
	}

	vic, err := h.Backend.UpdateVpcIngressConnection(in.VpcIngressConnectionArn, vpcID, vpcEndpointID)
	if err != nil {
		return nil, err
	}

	return &updateVpcIngressConnectionOutput{VpcIngressConnection: toVpcIngressConnectionOutput(vic)}, nil
}

// --- CustomDomain handler types and methods ---

type associateCustomDomainInput struct {
	ServiceArn         string `json:"ServiceArn"`
	DomainName         string `json:"DomainName"`
	EnableWWWSubdomain *bool  `json:"EnableWWWSubdomain"`
}

type customDomainOutput struct {
	DomainName         string `json:"DomainName"`
	Status             string `json:"Status"`
	EnableWWWSubdomain bool   `json:"EnableWWWSubdomain"`
}

type associateCustomDomainOutput struct {
	CustomDomain customDomainOutput `json:"CustomDomain"`
	DNSTarget    string             `json:"DNSTarget"`
	ServiceArn   string             `json:"ServiceArn"`
}

func toCustomDomainOutput(cd *CustomDomain) customDomainOutput {
	return customDomainOutput{
		DomainName:         cd.DomainName,
		Status:             cd.Status,
		EnableWWWSubdomain: cd.EnableWWWSubdomain,
	}
}

func (h *Handler) handleAssociateCustomDomain(
	_ context.Context,
	in *associateCustomDomainInput,
) (*associateCustomDomainOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	if in.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}

	enableWWW := true
	if in.EnableWWWSubdomain != nil {
		enableWWW = *in.EnableWWWSubdomain
	}

	cd, err := h.Backend.AssociateCustomDomain(in.ServiceArn, in.DomainName, enableWWW)
	if err != nil {
		return nil, err
	}

	return &associateCustomDomainOutput{
		CustomDomain: toCustomDomainOutput(cd),
		DNSTarget:    in.DomainName,
		ServiceArn:   in.ServiceArn,
	}, nil
}

type disassociateCustomDomainInput struct {
	ServiceArn string `json:"ServiceArn"`
	DomainName string `json:"DomainName"`
}

type disassociateCustomDomainOutput struct {
	CustomDomain customDomainOutput `json:"CustomDomain"`
	DNSTarget    string             `json:"DNSTarget"`
	ServiceArn   string             `json:"ServiceArn"`
}

func (h *Handler) handleDisassociateCustomDomain(
	_ context.Context,
	in *disassociateCustomDomainInput,
) (*disassociateCustomDomainOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	if in.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}

	cd, err := h.Backend.DisassociateCustomDomain(in.ServiceArn, in.DomainName)
	if err != nil {
		return nil, err
	}

	return &disassociateCustomDomainOutput{
		CustomDomain: toCustomDomainOutput(cd),
		DNSTarget:    in.DomainName,
		ServiceArn:   in.ServiceArn,
	}, nil
}

type describeCustomDomainsInput struct {
	ServiceArn string `json:"ServiceArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type describeCustomDomainsOutput struct {
	CustomDomains []customDomainOutput `json:"CustomDomains"`
	DNSTarget     string               `json:"DNSTarget"`
	ServiceArn    string               `json:"ServiceArn"`
	VpcDNSTargets []any                `json:"VpcDNSTargets"`
	NextToken     string               `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeCustomDomains(
	_ context.Context,
	in *describeCustomDomainsInput,
) (*describeCustomDomainsOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	domains, nextToken, dnsTarget, err := h.Backend.DescribeCustomDomains(
		in.ServiceArn, in.MaxResults, in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]customDomainOutput, 0, len(domains))
	for _, d := range domains {
		out = append(out, toCustomDomainOutput(d))
	}

	return &describeCustomDomainsOutput{
		CustomDomains: out,
		DNSTarget:     dnsTarget,
		ServiceArn:    in.ServiceArn,
		VpcDNSTargets: []any{},
		NextToken:     nextToken,
	}, nil
}
