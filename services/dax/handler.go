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
	DataPlane *DataPlane
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
		// "ResetParameterGroup" is deliberately NOT advertised — it is not a real DAX
		// SDK operation (verified against botocore's dax service-2.json: no such
		// action exists; the full real op list is Create/Delete/Update/Describe for
		// Cluster/ParameterGroup/SubnetGroup plus IncreaseReplicationFactor,
		// DecreaseReplicationFactor, RebootNode, TagResource, UntagResource,
		// ListTags, DescribeParameters, DescribeDefaultParameters, DescribeEvents —
		// no reset action for parameter groups at all). DAX dispatches purely by
		// X-Amz-Target header value via the daxOperations table, so a real client
		// can never send this target and this route is unreachable by real traffic
		// either way; it stays wired below as internal test scaffolding only (see
		// gopherstack-vhw2 category A, same resolution as EMR's ListTagsForResource
		// and CloudFront's GetFunctionAssociations/SetFunctionAssociations).
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

// daxOpHandler is the signature every per-operation handler method conforms to,
// expressed as a method expression so it can live in a lookup table.
type daxOpHandler func(*Handler, []byte) (any, error)

// daxOperations maps each DAX API operation name to its handler method. Using a
// table instead of a switch keeps dispatch a flat O(1) lookup with no branching
// complexity, and is the single place new operations must be registered.
var daxOperations = map[string]daxOpHandler{ //nolint:gochecknoglobals // package-level lookup table
	"CreateCluster":             (*Handler).handleCreateCluster,
	"DescribeClusters":          (*Handler).handleDescribeClusters,
	"UpdateCluster":             (*Handler).handleUpdateCluster,
	"DeleteCluster":             (*Handler).handleDeleteCluster,
	"IncreaseReplicationFactor": (*Handler).handleIncreaseReplicationFactor,
	"DecreaseReplicationFactor": (*Handler).handleDecreaseReplicationFactor,
	"RebootNode":                (*Handler).handleRebootNode,
	"TagResource":               (*Handler).handleTagResource,
	"UntagResource":             (*Handler).handleUntagResource,
	"ListTags":                  (*Handler).handleListTags,
	"CreateParameterGroup":      (*Handler).handleCreateParameterGroup,
	"DescribeParameterGroups":   (*Handler).handleDescribeParameterGroups,
	"UpdateParameterGroup":      (*Handler).handleUpdateParameterGroup,
	"DeleteParameterGroup":      (*Handler).handleDeleteParameterGroup,
	"DescribeParameters":        (*Handler).handleDescribeParameters,
	"DescribeDefaultParameters": (*Handler).handleDescribeDefaultParameters,
	"ResetParameterGroup":       (*Handler).handleResetParameterGroup,
	"CreateSubnetGroup":         (*Handler).handleCreateSubnetGroup,
	"DescribeSubnetGroups":      (*Handler).handleDescribeSubnetGroups,
	"UpdateSubnetGroup":         (*Handler).handleUpdateSubnetGroup,
	"DeleteSubnetGroup":         (*Handler).handleDeleteSubnetGroup,
	"DescribeEvents":            (*Handler).handleDescribeEvents,
}

// dispatch routes the DAX operation to the appropriate handler function.
func (h *Handler) dispatch(
	ctx context.Context,
	operation string,
	body []byte,
) (any, error) {
	_ = ctx // reserved for future logging/tracing use

	fn, ok := daxOperations[operation]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, operation)
	}

	return fn(h, body)
}

// tagItem is the wire shape for a single tag; shared by every family whose
// request/response carries a Tags list (clusters, tags).
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// errCodeMapping associates a sentinel error with the AWS fault code to emit for it.
// Every mapped status is 400; only the __type code varies (see mapError doc).
type errCodeMapping struct {
	target error
	code   string
}

// daxErrCodeMappings lists sentinel errors in priority order: specific
// not-found/conflict/invalid-parameter variants first, then their generic
// parent categories (awserr.ErrNotFound/ErrConflict/ErrInvalidParameter) as
// fallbacks. errors.Is is checked against each entry in order, so a specific
// sentinel always wins over the broader category it wraps.
var daxErrCodeMappings = []errCodeMapping{ //nolint:gochecknoglobals // package-level lookup table
	// Tag quota exceeded — specific case before the generic invalid-parameter fallback.
	{ErrTagQuotaExceeded, "TagQuotaPerResourceExceeded"},

	// Specific not-found variants.
	{ErrClusterNotFound, "ClusterNotFoundFault"},
	{ErrParameterGroupNotFound, "ParameterGroupNotFoundFault"},
	{ErrSubnetGroupNotFound, "SubnetGroupNotFoundFault"},
	{ErrTagNotFound, "TagNotFoundFault"},
	{ErrNodeNotFound, "NodeNotFoundFault"},

	// Specific conflict variants.
	{ErrClusterAlreadyExists, "ClusterAlreadyExistsFault"},
	{ErrParameterGroupAlreadyExists, "ParameterGroupAlreadyExistsFault"},
	{ErrSubnetGroupAlreadyExists, "SubnetGroupAlreadyExistsFault"},
	{ErrSubnetGroupInUse, "SubnetGroupInUseFault"},
	{ErrParameterGroupInUse, "ParameterGroupInUseFault"},
	{ErrInvalidClusterState, "InvalidClusterStateFault"},

	// Specific invalid parameter variants.
	{ErrInvalidARN, "InvalidARNFault"},
	{ErrInvalidParameterValue, "InvalidParameterValueException"},
	{ErrInvalidParameterCombination, "InvalidParameterCombinationException"},

	// Generic fallbacks.
	{awserr.ErrNotFound, "ResourceNotFoundException"},
	{awserr.ErrConflict, "InvalidClusterStateFault"},
	{awserr.ErrInvalidParameter, "InvalidParameterValueException"},
	{errUnknownAction, "InvalidAction"},
	{errInvalidRequest, "SerializationException"},
}

// mapError maps a backend error to an HTTP status code and error body.
// Specific sentinel errors take priority over their parent error categories
// (see daxErrCodeMappings ordering). Every DAX fault in this table is a 400;
// unmapped errors fall back to a 500 InternalFailure.
func (h *Handler) mapError(err error) (int, map[string]any) {
	for _, m := range daxErrCodeMappings {
		if errors.Is(err, m.target) {
			return http.StatusBadRequest, daxError(m.code, err.Error())
		}
	}

	return http.StatusInternalServerError, daxError("InternalFailure", err.Error())
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
