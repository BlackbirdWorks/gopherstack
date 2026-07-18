package apprunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
			keyType:    resourceNotFoundType,
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    invalidRequestType,
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    invalidStateType,
			keyMessage: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    invalidRequestType,
			keyMessage: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyType:    internalServiceErrorType,
			keyMessage: err.Error(),
		})
	}
}

// tagInput is the wire shape of a single tag; shared by every operation's
// input/output that carries tags (Create* Tags fields, TagResource,
// ListTagsForResource).
type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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
