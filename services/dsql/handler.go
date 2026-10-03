package dsql

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const dsqlMatchPriority = service.PriorityPathVersioned

// Handler is the HTTP handler for the Aurora DSQL REST-JSON control-plane API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Aurora DSQL handler backed by backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "DSQL" }

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateCluster,
		opGetCluster,
		opListClusters,
		opUpdateCluster,
		opDeleteCluster,
		opGetClusterPolicy,
		opPutClusterPolicy,
		opDeleteClusterPolicy,
		opGetVpcEndpointServiceName,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		opCreateStream,
		opGetStream,
		opDeleteStream,
		opListStreams,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return dsqlServiceName }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Aurora DSQL REST API requests.
//
// /tags/{arn} is SigV4-scoped rather than claimed unconditionally: it is a
// generic-tagging path many restjson1 services reuse verbatim (see
// .claude/memories/route-matcher-prefix-collision.md), so an unscoped claim
// here would swallow another service's own TagResource/UntagResource/
// ListTagsForResource requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := effectivePath(c.Request())

		if path == pathClusterRoot || path == pathClusterRoot+"/" {
			return true
		}

		if isDSQLClusterPolicyOrResourcePath(path) {
			return true
		}

		if strings.HasPrefix(path, pathClustersPrefix) && strings.HasSuffix(path, vpcEndpointServiceNameSuffix) {
			return true
		}

		if strings.HasPrefix(path, pathStreamPrefix) {
			return true
		}

		if strings.HasPrefix(path, pathTagsPrefix) {
			svc := httputils.ExtractServiceFromRequest(c.Request())

			return svc == "" || svc == dsqlServiceName
		}

		return false
	}
}

// isDSQLClusterPolicyOrResourcePath reports whether path is a DSQL cluster
// resource path (/cluster/{id} or /cluster/{id}/policy). It is a named
// helper (rather than an inline strings.HasPrefix) because Inspector2's own
// RouteMatcher over-claims the same "/cluster/" prefix for its unrelated
// "/cluster/get" operation (gopherstack-7r6bz); Inspector2 guards its claim
// against this path family via its own ambiguousRouteMatchPrefixes map.
func isDSQLClusterPolicyOrResourcePath(path string) bool {
	return strings.HasPrefix(path, pathClusterPrefix)
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return dsqlMatchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseDSQLPath(c.Request().Method, effectivePath(c.Request()))

	return op
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseDSQLPath(c.Request().Method, effectivePath(c.Request()))

	return resource
}

// effectivePath returns the raw (percent-encoded) path if available, otherwise the decoded path.
func effectivePath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}

	return r.URL.Path
}

// contextWithRegion returns the request context with the resolved AWS region attached.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

type regionContextKey struct{}

func regionFromContext(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// Handler returns the Echo handler function for Aurora DSQL requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := h.contextWithRegion(c)
		log := logger.Load(ctx)

		method := c.Request().Method
		path := effectivePath(c.Request())

		op, resource := parseDSQLPath(method, path)
		if op == "" {
			return h.writeError(
				c, http.StatusBadRequest, "ValidationException", "unknown operation", validationReasonOther,
			)
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "dsql: failed to read request body", "error", err)

			return h.writeError(
				c, http.StatusInternalServerError, "InternalServerException", "failed to read request body", "",
			)
		}

		log.DebugContext(ctx, "dsql request", "op", op, "resource", resource)

		return h.dispatch(ctx, c, op, resource, body)
	}
}

// dispatch routes a parsed operation to the appropriate handler.
func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op, resource string, body []byte) error {
	if ok, err := h.dispatchClusterOps(ctx, c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchPolicyOps(c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchTagOps(c, op, resource, body); ok {
		return err
	}

	if ok, err := h.dispatchStreamOps(c, op, resource, body); ok {
		return err
	}

	return h.writeError(
		c, http.StatusBadRequest, "ValidationException", "unknown operation: "+op, validationReasonOther,
	)
}

// writeError writes a DSQL restJson1 error response.
func (h *Handler) writeError(c *echo.Context, status int, errType, message, reason string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message, Reason: reason})
}

// writeInvalidBody writes the common ValidationException response for an
// unparseable request body.
func (h *Handler) writeInvalidBody(c *echo.Context) error {
	return h.writeError(
		c, http.StatusBadRequest, "ValidationException", "invalid request body", validationReasonFieldError,
	)
}

// writeBackendError maps a backend error to the matching AWS error response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrDeletionProtected):
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error(), validationReasonLockedOut)
	case errors.Is(err, ErrClusterQuotaExceeded), errors.Is(err, ErrStreamQuotaExceeded):
		return h.writeError(c, http.StatusPaymentRequired, "ServiceQuotaExceededException", err.Error(), "")
	case errors.Is(err, ErrPolicyVersionMismatch):
		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error(), "")
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error(), "")
	case errors.Is(err, awserr.ErrConflict):
		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error(), "")
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error(), validationReasonFieldError)
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalServerException", err.Error(), "")
	}
}
