package lambdaweb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Handler is the HTTP handler for the Lambda Web Functions control plane and endpoint data plane.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a handler backed by backend.
func NewHandler(backend StorageBackend) *Handler { return &Handler{Backend: backend} }

// Name returns the service name.
func (h *Handler) Name() string { return "LambdaWeb" }

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the control-plane operations.
func (h *Handler) GetSupportedOperations() []string { return operations() }

// ChaosServiceName returns the service name used for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "lambda-web" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher claims the versioned control-plane path and endpoint data-plane hosts.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(effectivePath(c.Request()), apiPrefix) || IsEndpointHost(c.Request().Host)
	}
}

// MatchPriority returns the routing priority; endpoint hosts must win over path-based services.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if IsEndpointHost(c.Request().Host) {
		return opInvokeEndpoint
	}

	return parseRequest(c.Request().Method, effectivePath(c.Request())).op
}

// ExtractResource returns the resource identifier for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	t := parseRequest(c.Request().Method, effectivePath(c.Request()))

	switch {
	case t.resource != "":
		return t.resource
	case t.child != "":
		return t.function + "/" + t.child
	}

	return t.function
}

// IAMAction returns the IAM action for a request; ApplicationManaged endpoints are not IAM-gated.
func (h *Handler) IAMAction(r *http.Request) string {
	if IsEndpointHost(r.Host) {
		if h.Backend.EndpointAuthType(r.Host) == authIam {
			return "lambda:" + opInvokeEndpoint
		}

		return ""
	}

	if op := parseRequest(r.Method, effectivePath(r)).op; op != "" {
		return "lambda:" + op
	}

	return ""
}

func (h *Handler) region(c *echo.Context) string {
	return httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if h.Backend.ServeEndpoint(c.Response(), c.Request()) {
			return nil
		}

		ctx := c.Request().Context()
		log := logger.Load(ctx)

		t := parseRequest(c.Request().Method, effectivePath(c.Request()))
		if t.op == "" {
			return h.writeError(c, http.StatusBadRequest, "ValidationException", "unknown operation", "", "")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "lambdaweb: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalServerException",
				"failed to read body",
				"",
				"",
			)
		}

		log.DebugContext(ctx, "lambdaweb request", "op", t.op)

		return h.dispatch(ctx, c, t, body)
	}
}

func (h *Handler) dispatch(ctx context.Context, c *echo.Context, t target, body []byte) error {
	region := h.region(c)

	if ok, err := h.dispatchFunctionOps(ctx, c, region, t, body); ok {
		return err
	}

	if ok, err := h.dispatchRevisionOps(ctx, c, region, t, body); ok {
		return err
	}

	if ok, err := h.dispatchEndpointOps(c, region, t, body); ok {
		return err
	}

	if ok, err := h.dispatchMiscOps(c, region, t, body); ok {
		return err
	}

	return h.writeError(c, http.StatusBadRequest, "ValidationException", "unknown operation: "+t.op, "", "")
}

// decode unmarshals body into v; an empty body leaves v untouched.
func decode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	return json.Unmarshal(body, v)
}

func (h *Handler) writeInvalidBody(c *echo.Context) error {
	return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body", "", "")
}

func (h *Handler) writeError(c *echo.Context, status int, errType, msg, resourceID, resourceType string) error {
	c.Response().Header().Set("X-Amzn-Errortype", errType)

	return c.JSON(status, errorResponse{Message: msg, ResourceID: resourceID, ResourceType: resourceType})
}

func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	id, rt := "", ""
	if ae, ok := errors.AsType[*apiError](err); ok {
		id, rt = ae.resourceID, ae.resourceType
	}

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error(), id, rt)
	case errors.Is(err, awserr.ErrConflict):
		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error(), id, rt)
	case errors.Is(err, ErrQuotaExceeded):
		return h.writeError(c, http.StatusPaymentRequired, "ServiceQuotaExceededException", err.Error(), id, rt)
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error(), "", "")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalServerException", err.Error(), "", "")
	}
}

// Shutdown stops every execution environment.
func (h *Handler) Shutdown(_ context.Context) {
	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}
