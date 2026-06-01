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
		"CreateService",
		"DescribeService",
		"UpdateService",
		"DeleteService",
		"ListServices",
		"PauseService",
		"ResumeService",
		"StartDeployment",
		"ListOperations",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
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
		"CreateService":       service.WrapOp(h.handleCreateService),
		"DescribeService":     service.WrapOp(h.handleDescribeService),
		"UpdateService":       service.WrapOp(h.handleUpdateService),
		"DeleteService":       service.WrapOp(h.handleDeleteService),
		"ListServices":        service.WrapOp(h.handleListServices),
		"PauseService":        service.WrapOp(h.handlePauseService),
		"ResumeService":       service.WrapOp(h.handleResumeService),
		"StartDeployment":     service.WrapOp(h.handleStartDeployment),
		"ListOperations":      service.WrapOp(h.handleListOperations),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
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
