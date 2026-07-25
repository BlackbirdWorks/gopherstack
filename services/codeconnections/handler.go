package codeconnections

import (
	"context"
	"encoding/json"
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
	codeconnectionsMatchPriority = service.PriorityHeaderExact
	ccTargetPrefix               = "CodeConnections_20231201."
	ccContentType                = "application/x-amz-json-1.0"
	ccDefaultPageSize            = 100
)

var errUnknownAction = awserr.New("UnknownOperationException", awserr.ErrNotFound)

// Handler is the Echo HTTP handler for AWS CodeConnections operations (JSON 1.0 protocol).
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new CodeConnections handler with a pre-built dispatch table.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnection":              service.WrapOp(h.handleCreateConnection),
		"GetConnection":                 service.WrapOp(h.handleGetConnection),
		"ListConnections":               service.WrapOp(h.handleListConnections),
		"DeleteConnection":              service.WrapOp(h.handleDeleteConnection),
		"TagResource":                   service.WrapOp(h.handleTagResource),
		"UntagResource":                 service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":           service.WrapOp(h.handleListTagsForResource),
		"CreateHost":                    service.WrapOp(h.handleCreateHost),
		"GetHost":                       service.WrapOp(h.handleGetHost),
		"DeleteHost":                    service.WrapOp(h.handleDeleteHost),
		"CreateRepositoryLink":          service.WrapOp(h.handleCreateRepositoryLink),
		"GetRepositoryLink":             service.WrapOp(h.handleGetRepositoryLink),
		"DeleteRepositoryLink":          service.WrapOp(h.handleDeleteRepositoryLink),
		"CreateSyncConfiguration":       service.WrapOp(h.handleCreateSyncConfiguration),
		"DeleteSyncConfiguration":       service.WrapOp(h.handleDeleteSyncConfiguration),
		"GetRepositorySyncStatus":       service.WrapOp(h.handleGetRepositorySyncStatus),
		"GetResourceSyncStatus":         service.WrapOp(h.handleGetResourceSyncStatus),
		"GetSyncBlockerSummary":         service.WrapOp(h.handleGetSyncBlockerSummary),
		"GetSyncConfiguration":          service.WrapOp(h.handleGetSyncConfiguration),
		"ListHosts":                     service.WrapOp(h.handleListHosts),
		"ListRepositoryLinks":           service.WrapOp(h.handleListRepositoryLinks),
		"ListRepositorySyncDefinitions": service.WrapOp(h.handleListRepositorySyncDefinitions),
		"ListSyncConfigurations":        service.WrapOp(h.handleListSyncConfigurations),
		"UpdateHost":                    service.WrapOp(h.handleUpdateHost),
		"UpdateRepositoryLink":          service.WrapOp(h.handleUpdateRepositoryLink),
		"UpdateSyncBlocker":             service.WrapOp(h.handleUpdateSyncBlocker),
		"UpdateSyncConfiguration":       service.WrapOp(h.handleUpdateSyncConfiguration),
	}
}

// Reset clears the backend state (test helper).
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// This delegation is required: cli.go's setupPersistence type-asserts the
// service.Registerable value returned by Provider.Init (the Handler, not
// InMemoryBackend) against a Snapshot/Restore interface -- since Handler
// itself never exposed either method before this fix, InMemoryBackend.
// Snapshot/Restore existed but were never reachable through the registered
// service, so CodeConnections state was silently never persisted.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeConnections" }

// GetSupportedOperations returns the list of supported CodeConnections operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateConnection",
		"GetConnection",
		"ListConnections",
		"DeleteConnection",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"CreateHost",
		"GetHost",
		"DeleteHost",
		"CreateRepositoryLink",
		"GetRepositoryLink",
		"DeleteRepositoryLink",
		"CreateSyncConfiguration",
		"DeleteSyncConfiguration",
		"GetRepositorySyncStatus",
		"GetResourceSyncStatus",
		"GetSyncBlockerSummary",
		"GetSyncConfiguration",
		"ListHosts",
		"ListRepositoryLinks",
		"ListRepositorySyncDefinitions",
		"ListSyncConfigurations",
		"UpdateHost",
		"UpdateRepositoryLink",
		"UpdateSyncBlocker",
		"UpdateSyncConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codeconnections" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeConnections instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeConnections JSON 1.0 requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), ccTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return codeconnectionsMatchPriority }

// ExtractOperation extracts the CodeConnections operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, ccTargetPrefix)
}

// ExtractResource extracts the primary resource identifier from the JSON request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ConnectionArn    string `json:"ConnectionArn"`
		ResourceArn      string `json:"ResourceArn"`
		HostArn          string `json:"HostArn"`
		RepositoryLinkID string `json:"RepositoryLinkId"`
		ResourceName     string `json:"ResourceName"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ConnectionArn != "":
		return req.ConnectionArn
	case req.ResourceArn != "":
		return req.ResourceArn
	case req.HostArn != "":
		return req.HostArn
	case req.RepositoryLinkID != "":
		return req.RepositoryLinkID
	case req.ResourceName != "":
		return req.ResourceName
	default:
		return ""
	}
}

// Handler returns the Echo handler function for CodeConnections requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeConnections", ccContentType,
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleEchoError,
		)
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

func (h *Handler) handleEchoError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, statusCode := resolveErrorType(err)

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", ccContentType)

	return c.JSONBlob(statusCode, payload)
}

func resolveErrorType(err error) (string, int) {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrSyncBlockerNotFound):
		return "SyncBlockerDoesNotExistException", http.StatusBadRequest
	case errors.Is(err, ErrResourceInUse):
		return "ResourceUnavailableException", http.StatusBadRequest
	case errors.Is(err, ErrSyncConfigStillExists):
		return "SyncConfigurationStillExistsException", http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		return "ResourceAlreadyExistsException", http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		return "InvalidInputException", http.StatusBadRequest
	case errors.Is(err, errUnknownAction):
		return "UnknownOperationException", http.StatusBadRequest
	default:
		return "InternalFailure", http.StatusInternalServerError
	}
}

// tag is the JSON representation of a CodeConnections tag (array format).
type tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToSortedArray converts a tag map to a sorted array for deterministic output.
func tagsToSortedArray(m map[string]string) []tag {
	keys := sortedTagKeys(m)
	out := make([]tag, 0, len(keys))

	for _, k := range keys {
		out = append(out, tag{Key: k, Value: m[k]})
	}

	return out
}

func tagsFromArray(tags []tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// emptyOutput is the shared wire shape for void-result operations.
type emptyOutput struct{}
