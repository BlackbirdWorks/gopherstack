package dax

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	daxService         = "dax"
	daxTargetPrefix    = "AmazonDAXV3."
	daxMatchPriority   = service.PriorityHeaderExact
	clusterResponseKey = "Cluster"
	parameterGroupKey  = "ParameterGroup"
	tagsResponseKey    = "Tags"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the Amazon DAX API. It also owns the optional
// DAX binary-protocol data-plane listener (see dataplane_server.go).
type Handler struct {
	Backend   StorageBackend
	dataPlane *dataPlane
}

// NewHandler creates a new DAX handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears handler state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "DAX" }

// GetSupportedOperations returns the list of supported DAX operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"DescribeClusters",
		"UpdateCluster",
		"DeleteCluster",
		"IncreaseReplicationFactor",
		"DecreaseReplicationFactor",
		"RebootNode",
		"TagResource",
		"UntagResource",
		"ListTags",
		"CreateParameterGroup",
		"DescribeParameterGroups",
		"UpdateParameterGroup",
		"DeleteParameterGroup",
		"DescribeParameters",
		"DescribeDefaultParameters",
		"ResetParameterGroup",
		"CreateSubnetGroup",
		"DescribeSubnetGroups",
		"UpdateSubnetGroup",
		"DeleteSubnetGroup",
		"DescribeEvents",
	}
}

// RouteMatcher returns a function that matches DAX API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), daxTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return daxMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, daxTargetPrefix)
}

// ExtractResource extracts the resource from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for DAX requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "dax: failed to read request body", "error", err)

			return c.JSON(
				http.StatusBadRequest,
				daxError("SerializationException", "failed to read request body"),
			)
		}

		operation := h.ExtractOperation(c)
		if operation == "" {
			return c.JSON(
				http.StatusBadRequest,
				daxError("InvalidAction", "missing X-Amz-Target header"),
			)
		}

		resp, handlerErr := h.dispatch(ctx, operation, body)
		if handlerErr != nil {
			status, errBody := h.mapError(handlerErr)

			return c.JSON(status, errBody)
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// dispatch routes the DAX operation to the appropriate handler function.
//
//nolint:cyclop // routing switch covers all DAX operations
func (h *Handler) dispatch(
	ctx context.Context,
	operation string,
	body []byte,
) (any, error) {
	_ = ctx // reserved for future logging/tracing use

	switch operation {
	case "CreateCluster":
		return h.handleCreateCluster(body)
	case "DescribeClusters":
		return h.handleDescribeClusters(body)
	case "UpdateCluster":
		return h.handleUpdateCluster(body)
	case "DeleteCluster":
		return h.handleDeleteCluster(body)
	case "IncreaseReplicationFactor":
		return h.handleIncreaseReplicationFactor(body)
	case "DecreaseReplicationFactor":
		return h.handleDecreaseReplicationFactor(body)
	case "RebootNode":
		return h.handleRebootNode(body)
	case "TagResource":
		return h.handleTagResource(body)
	case "UntagResource":
		return h.handleUntagResource(body)
	case "ListTags":
		return h.handleListTags(body)
	case "CreateParameterGroup":
		return h.handleCreateParameterGroup(body)
	case "DescribeParameterGroups":
		return h.handleDescribeParameterGroups(body)
	case "UpdateParameterGroup":
		return h.handleUpdateParameterGroup(body)
	case "DeleteParameterGroup":
		return h.handleDeleteParameterGroup(body)
	case "DescribeParameters":
		return h.handleDescribeParameters(body)
	case "DescribeDefaultParameters":
		return h.handleDescribeDefaultParameters(body)
	case "ResetParameterGroup":
		return h.handleResetParameterGroup(body)
	case "CreateSubnetGroup":
		return h.handleCreateSubnetGroup(body)
	case "DescribeSubnetGroups":
		return h.handleDescribeSubnetGroups(body)
	case "UpdateSubnetGroup":
		return h.handleUpdateSubnetGroup(body)
	case "DeleteSubnetGroup":
		return h.handleDeleteSubnetGroup(body)
	case "DescribeEvents":
		return h.handleDescribeEvents(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, operation)
	}
}

// tagItem is the wire shape for a single tag; shared by every family whose
// request/response carries a Tags list (clusters, tags).
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// mapError maps a backend error to an HTTP status code and error body.
// Specific sentinel errors take priority over their parent error categories.
//
//nolint:cyclop // exhaustive error mapping requires many cases
func (h *Handler) mapError(err error) (int, map[string]any) {
	// Tag quota exceeded — specific case before the generic invalid-parameter fallback.
	if errors.Is(err, ErrTagQuotaExceeded) {
		return http.StatusBadRequest, daxError("TagQuotaPerResourceExceeded", err.Error())
	}

	// Specific not-found variants.
	switch {
	case errors.Is(err, ErrClusterNotFound):
		return http.StatusBadRequest, daxError("ClusterNotFoundFault", err.Error())
	case errors.Is(err, ErrParameterGroupNotFound):
		return http.StatusBadRequest, daxError("ParameterGroupNotFoundFault", err.Error())
	case errors.Is(err, ErrSubnetGroupNotFound):
		return http.StatusBadRequest, daxError("SubnetGroupNotFoundFault", err.Error())
	case errors.Is(err, ErrTagNotFound):
		return http.StatusBadRequest, daxError("TagNotFoundFault", err.Error())
	case errors.Is(err, ErrNodeNotFound):
		return http.StatusBadRequest, daxError("NodeNotFoundFault", err.Error())

	// Specific conflict variants.
	case errors.Is(err, ErrClusterAlreadyExists):
		return http.StatusBadRequest, daxError("ClusterAlreadyExistsFault", err.Error())
	case errors.Is(err, ErrParameterGroupAlreadyExists):
		return http.StatusBadRequest, daxError("ParameterGroupAlreadyExistsFault", err.Error())
	case errors.Is(err, ErrSubnetGroupAlreadyExists):
		return http.StatusBadRequest, daxError("SubnetGroupAlreadyExistsFault", err.Error())
	case errors.Is(err, ErrSubnetGroupInUse):
		return http.StatusBadRequest, daxError("SubnetGroupInUseFault", err.Error())
	case errors.Is(err, ErrParameterGroupInUse):
		return http.StatusBadRequest, daxError("ParameterGroupInUseFault", err.Error())
	case errors.Is(err, ErrInvalidClusterState):
		return http.StatusBadRequest, daxError("InvalidClusterStateFault", err.Error())

	// Specific invalid parameter variants.
	case errors.Is(err, ErrInvalidARN):
		return http.StatusBadRequest, daxError("InvalidARNFault", err.Error())
	case errors.Is(err, ErrInvalidParameterValue):
		return http.StatusBadRequest, daxError("InvalidParameterValueException", err.Error())
	case errors.Is(err, ErrInvalidParameterCombination):
		return http.StatusBadRequest, daxError("InvalidParameterCombinationException", err.Error())

	// Generic fallbacks.
	case errors.Is(err, awserr.ErrNotFound):
		return http.StatusBadRequest, daxError("ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrConflict):
		return http.StatusBadRequest, daxError("InvalidClusterStateFault", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return http.StatusBadRequest, daxError("InvalidParameterValueException", err.Error())
	case errors.Is(err, errUnknownAction):
		return http.StatusBadRequest, daxError("InvalidAction", err.Error())
	case errors.Is(err, errInvalidRequest):
		return http.StatusBadRequest, daxError("SerializationException", err.Error())
	default:
		return http.StatusInternalServerError, daxError("InternalFailure", err.Error())
	}
}

// daxError builds a standard DAX JSON error body.
func daxError(code, message string) map[string]any {
	return map[string]any{
		"__type":  code,
		"message": message,
	}
}

// Snapshot and Restore are delegated to the backend.

// Snapshot returns the backend state as JSON bytes.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore restores backend state from JSON bytes.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
