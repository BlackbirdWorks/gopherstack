package workspaces

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityHeaderExact
	targetPrefix  = "WorkspacesService."
	contentType   = "application/x-amz-json-1.1"
)

// Handler handles WorkSpaces HTTP requests.
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
func (h *Handler) Name() string { return "WorkSpaces" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}

	return ops
}

// RouteMatcher returns a matcher that accepts WorkSpaces target header requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), contentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, newUnknownOpError(action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errBody(errResourceNotFound, err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, err.Error()))
	default:
		return c.JSON(
			http.StatusInternalServerError,
			errBody("InternalServerException", err.Error()),
		)
	}
}

// buildOps assembles the full operation table from each op-family's ops map.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	base := h.buildWorkspacesOps()

	maps.Copy(base, h.buildTagsOps())
	maps.Copy(base, h.buildBundlesOps())
	maps.Copy(base, h.buildDirectoriesOps())
	maps.Copy(base, h.buildWorkspacePropertiesOps())
	maps.Copy(base, h.buildImagesOps())
	maps.Copy(base, h.buildIPGroupsOps())
	maps.Copy(base, h.buildConnectionAliasesOps())
	maps.Copy(base, h.buildPoolsOps())
	maps.Copy(base, h.buildAccountOps())
	maps.Copy(base, h.buildConnectClientAddInsOps())
	maps.Copy(base, h.buildClientBrandingOps())
	maps.Copy(base, h.buildAccountLinksOps())
	maps.Copy(base, h.buildApplicationAssociationsOps())
	maps.Copy(base, h.buildSnapshotsOps())

	return base
}

// tagItem represents a key/value tag pair in AWS API requests.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// emptyOutput is the JSON shape for operations with no response body.
type emptyOutput struct{}

func tagsToMap(tags []tagItem) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func errBody(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

type unknownOpError struct{ op string }

func (e *unknownOpError) Error() string { return "unknown operation: " + e.op }

func newUnknownOpError(op string) error { return &unknownOpError{op: op} }
