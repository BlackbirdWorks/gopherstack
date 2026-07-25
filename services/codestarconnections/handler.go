package codestarconnections

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
	codestarTargetPrefix = "CodeStar_connections_20191201."
	// errTypeInvalidInput is the wire error type used for every malformed-
	// input case that maps to the real InvalidInputException (see
	// ErrValidation's doc comment in errors.go for why the real error
	// catalog has no ValidationException).
	errTypeInvalidInput = "InvalidInputException"
)

var (
	errUnknownAction  = awserr.New("UnknownOperationException", awserr.ErrNotFound)
	errInvalidRequest = errors.New("invalid request")
)

const defaultCSCMaxResults = 100

// Handler is the Echo HTTP handler for CodeStar Connections operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new CodeStar Connections handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears the backend state (test helper).
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Without this delegation, cli.go's setupPersistence type-asserts the
// service.Registerable value returned by Provider.Init (this Handler, not
// InMemoryBackend) against a Snapshot/Restore interface -- since Handler
// itself never exposed either method, InMemoryBackend.Snapshot/Restore
// (persistence.go) were dead code and this service was never actually
// persisted, despite implementing the Persistable contract.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnection":              service.WrapOp(h.handleCreateConnection),
		"GetConnection":                 service.WrapOp(h.handleGetConnection),
		"ListConnections":               service.WrapOp(h.handleListConnections),
		"DeleteConnection":              service.WrapOp(h.handleDeleteConnection),
		"CreateHost":                    service.WrapOp(h.handleCreateHost),
		"GetHost":                       service.WrapOp(h.handleGetHost),
		"ListHosts":                     service.WrapOp(h.handleListHosts),
		"DeleteHost":                    service.WrapOp(h.handleDeleteHost),
		"UpdateHost":                    service.WrapOp(h.handleUpdateHost),
		"ListTagsForResource":           service.WrapOp(h.handleListTagsForResource),
		"TagResource":                   service.WrapOp(h.handleTagResource),
		"UntagResource":                 service.WrapOp(h.handleUntagResource),
		"CreateRepositoryLink":          service.WrapOp(h.handleCreateRepositoryLink),
		"GetRepositoryLink":             service.WrapOp(h.handleGetRepositoryLink),
		"DeleteRepositoryLink":          service.WrapOp(h.handleDeleteRepositoryLink),
		"ListRepositoryLinks":           service.WrapOp(h.handleListRepositoryLinks),
		"CreateSyncConfiguration":       service.WrapOp(h.handleCreateSyncConfiguration),
		"GetSyncConfiguration":          service.WrapOp(h.handleGetSyncConfiguration),
		"DeleteSyncConfiguration":       service.WrapOp(h.handleDeleteSyncConfiguration),
		"GetRepositorySyncStatus":       service.WrapOp(h.handleGetRepositorySyncStatus),
		"GetResourceSyncStatus":         service.WrapOp(h.handleGetResourceSyncStatus),
		"GetSyncBlockerSummary":         service.WrapOp(h.handleGetSyncBlockerSummary),
		"ListRepositorySyncDefinitions": service.WrapOp(h.handleListRepositorySyncDefinitions),
		"ListSyncConfigurations":        service.WrapOp(h.handleListSyncConfigurations),
		"UpdateRepositoryLink":          service.WrapOp(h.handleUpdateRepositoryLink),
		"UpdateSyncBlocker":             service.WrapOp(h.handleUpdateSyncBlocker),
		"UpdateSyncConfiguration":       service.WrapOp(h.handleUpdateSyncConfiguration),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeStarConnections" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateConnection",
		"GetConnection",
		"ListConnections",
		"DeleteConnection",
		"CreateHost",
		"GetHost",
		"ListHosts",
		"DeleteHost",
		"UpdateHost",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"CreateRepositoryLink",
		"GetRepositoryLink",
		"DeleteRepositoryLink",
		"ListRepositoryLinks",
		"CreateSyncConfiguration",
		"GetSyncConfiguration",
		"DeleteSyncConfiguration",
		"GetRepositorySyncStatus",
		"GetResourceSyncStatus",
		"GetSyncBlockerSummary",
		"ListRepositorySyncDefinitions",
		"ListSyncConfigurations",
		"UpdateRepositoryLink",
		"UpdateSyncBlocker",
		"UpdateSyncConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codestar-connections" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodeStar Connections requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codestarTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeStar Connections action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codestarTargetPrefix)
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

// Handler returns the Echo handler function for CodeStar Connections requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeStarConnections", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
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

const codestarContentType = "application/x-amz-json-1.0"

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	var errType string
	var statusCode int

	switch {
	case errors.Is(err, ErrNotFound):
		errType, statusCode = "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrSyncBlockerNotFound):
		errType, statusCode = "SyncBlockerDoesNotExistException", http.StatusBadRequest
	case errors.Is(err, ErrResourceInUse):
		errType, statusCode = "ResourceUnavailableException", http.StatusBadRequest
	case errors.Is(err, ErrSyncConfigStillExists):
		errType, statusCode = "SyncConfigurationStillExistsException", http.StatusBadRequest
	case errors.Is(err, ErrResourceAlreadyExists):
		errType, statusCode = "ResourceAlreadyExistsException", http.StatusBadRequest
	case errors.Is(err, ErrTagLimitExceeded):
		errType, statusCode = "LimitExceededException", http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrValidation),
		errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		errType, statusCode = errTypeInvalidInput, http.StatusBadRequest
	default:
		errType, statusCode = "InternalFailure", http.StatusInternalServerError
	}

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", codestarContentType)

	return c.JSONBlob(statusCode, payload)
}

// tagEntry is a key-value pair used in the API tag array format.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToSortedArray converts a tag map to a sorted array for deterministic output.
// Returns an empty (non-nil) slice when tags is empty or nil.
func tagsToSortedArray(tags map[string]string) []tagEntry {
	if len(tags) == 0 {
		return []tagEntry{}
	}

	keys := sortedTagKeys(tags)
	result := make([]tagEntry, len(keys))

	for i, k := range keys {
		result[i] = tagEntry{Key: k, Value: tags[k]}
	}

	return result
}

func tagsFromArray(entries []tagEntry) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}
