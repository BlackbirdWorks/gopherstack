package grafana

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const grafanaService = "grafana"

var errUnknownPath = errors.New("unknown path")

// Handler is the HTTP handler for the Amazon Managed Grafana API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Grafana handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Grafana" }

// GetSupportedOperations returns every operation this handler routes -- all
// 25 operations on the aws-sdk-go-v2/service/grafana client, per
// sdk_completeness_test.go's empty exception list.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateLicense",
		"CreateWorkspace",
		"CreateWorkspaceApiKey",
		"CreateWorkspaceServiceAccount",
		"CreateWorkspaceServiceAccountToken",
		"DeleteWorkspace",
		"DeleteWorkspaceApiKey",
		"DeleteWorkspaceServiceAccount",
		"DeleteWorkspaceServiceAccountToken",
		"DescribeWorkspace",
		"DescribeWorkspaceAuthentication",
		"DescribeWorkspaceConfiguration",
		"DisassociateLicense",
		"ListPermissions",
		"ListTagsForResource",
		"ListVersions",
		"ListWorkspaceServiceAccounts",
		"ListWorkspaceServiceAccountTokens",
		"ListWorkspaces",
		"TagResource",
		"UntagResource",
		"UpdatePermissions",
		"UpdateWorkspace",
		"UpdateWorkspaceAuthentication",
		"UpdateWorkspaceConfiguration",
	}
}

// Reset clears all stored state in the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return grafanaService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Grafana API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/workspaces") ||
			path == "/versions" ||
			strings.HasPrefix(path, "/tags/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.routeRequest(c.Request())

	return op
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := rawPathSegments(c.Request())
	if len(segs) == 0 {
		return ""
	}

	if segs[0] == segTags && len(segs) > 1 {
		return segs[1]
	}

	if segs[0] == segWorkspaces && len(segs) > 1 {
		return segs[1]
	}

	return ""
}

type dispatchFunc func(ctx context.Context, r *http.Request, body []byte) ([]byte, error)

// Handler returns the Echo handler function for Grafana requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "grafana: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op, dispatchFn := h.routeRequest(c.Request())
		if dispatchFn == nil {
			return h.handleError(c, fmt.Errorf("%w: %s %s", errUnknownPath, c.Request().Method, c.Request().URL.Path))
		}

		result, dispErr := dispatchFn(ctx, c.Request(), body)
		if dispErr != nil {
			log.ErrorContext(ctx, "grafana: operation failed", "op", op, "error", dispErr)

			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

// routeRequest maps HTTP method + path to operation name and dispatch function.
func (h *Handler) routeRequest(r *http.Request) (string, dispatchFunc) {
	segs := rawPathSegments(r)
	method := r.Method

	if len(segs) == 0 {
		return "", nil
	}

	switch segs[0] {
	case segWorkspaces:
		return h.routeWorkspaces(segs, method)
	case "versions":
		if method == http.MethodGet {
			return "ListVersions", h.handleListVersions
		}
	case segTags:
		return h.routeTags(segs, method)
	}

	return "", nil
}

// routeWorkspaces routes every /workspaces... path.
func (h *Handler) routeWorkspaces(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		return h.routeWorkspacesRoot(method)
	case 2: //nolint:mnd // /workspaces/{workspaceId}
		return h.routeWorkspaceByID(method)
	case 3: //nolint:mnd // /workspaces/{workspaceId}/{subResource}
		return h.routeWorkspaceSubResource(segs[2], method)
	case 4: //nolint:mnd // /workspaces/{workspaceId}/{subResource}/{id}
		return h.routeWorkspaceSubResourceItem(segs[2], method)
	case 5: //nolint:mnd // /workspaces/{workspaceId}/serviceaccounts/{id}/tokens
		if segs[2] == segServiceAccounts && segs[4] == "tokens" {
			return h.routeServiceAccountTokens(method)
		}
	case 6: //nolint:mnd // /workspaces/{workspaceId}/serviceaccounts/{id}/tokens/{tokenId}
		if segs[2] == segServiceAccounts && segs[4] == "tokens" && method == http.MethodDelete {
			return "DeleteWorkspaceServiceAccountToken", h.handleDeleteWorkspaceServiceAccountToken
		}
	}

	return "", nil
}

func (h *Handler) routeWorkspacesRoot(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodPost:
		return "CreateWorkspace", h.handleCreateWorkspace
	case http.MethodGet:
		return "ListWorkspaces", h.handleListWorkspaces
	}

	return "", nil
}

func (h *Handler) routeWorkspaceByID(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodGet:
		return "DescribeWorkspace", h.handleDescribeWorkspace
	case http.MethodPut:
		return "UpdateWorkspace", h.handleUpdateWorkspace
	case http.MethodDelete:
		return "DeleteWorkspace", h.handleDeleteWorkspace
	}

	return "", nil
}

func (h *Handler) routeWorkspaceSubResource(sub, method string) (string, dispatchFunc) {
	switch sub {
	case segAuthentication:
		switch method {
		case http.MethodGet:
			return "DescribeWorkspaceAuthentication", h.handleDescribeWorkspaceAuthentication
		case http.MethodPost:
			return "UpdateWorkspaceAuthentication", h.handleUpdateWorkspaceAuthentication
		}
	case "configuration":
		switch method {
		case http.MethodGet:
			return "DescribeWorkspaceConfiguration", h.handleDescribeWorkspaceConfiguration
		case http.MethodPut:
			return "UpdateWorkspaceConfiguration", h.handleUpdateWorkspaceConfiguration
		}
	case "permissions":
		switch method {
		case http.MethodGet:
			return "ListPermissions", h.handleListPermissions
		case http.MethodPatch:
			return "UpdatePermissions", h.handleUpdatePermissions
		}
	case "apikeys":
		if method == http.MethodPost {
			return "CreateWorkspaceApiKey", h.handleCreateWorkspaceAPIKey
		}
	case segServiceAccounts:
		switch method {
		case http.MethodPost:
			return "CreateWorkspaceServiceAccount", h.handleCreateWorkspaceServiceAccount
		case http.MethodGet:
			return "ListWorkspaceServiceAccounts", h.handleListWorkspaceServiceAccounts
		}
	}

	return "", nil
}

func (h *Handler) routeWorkspaceSubResourceItem(sub, method string) (string, dispatchFunc) {
	switch sub {
	case "licenses":
		switch method {
		case http.MethodPost:
			return "AssociateLicense", h.handleAssociateLicense
		case http.MethodDelete:
			return "DisassociateLicense", h.handleDisassociateLicense
		}
	case "apikeys":
		if method == http.MethodDelete {
			return "DeleteWorkspaceApiKey", h.handleDeleteWorkspaceAPIKey
		}
	case segServiceAccounts:
		if method == http.MethodDelete {
			return "DeleteWorkspaceServiceAccount", h.handleDeleteWorkspaceServiceAccount
		}
	}

	return "", nil
}

func (h *Handler) routeServiceAccountTokens(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodPost:
		return "CreateWorkspaceServiceAccountToken", h.handleCreateWorkspaceServiceAccountToken
	case http.MethodGet:
		return "ListWorkspaceServiceAccountTokens", h.handleListWorkspaceServiceAccountTokens
	}

	return "", nil
}

// routeTags routes /tags/{resourceArn}.
func (h *Handler) routeTags(segs []string, method string) (string, dispatchFunc) {
	if len(segs) < 2 { //nolint:mnd // need at least "tags" + resourceArn
		return "", nil
	}

	switch method {
	case http.MethodGet:
		return "ListTagsForResource", h.handleListTagsForResource
	case http.MethodPost:
		return "TagResource", h.handleTagResource
	case http.MethodDelete:
		return "UntagResource", h.handleUntagResource
	}

	return "", nil
}

// handleError renders err as one of Grafana's restjson1 error envelopes:
// {"message": "..."} with an X-Amzn-Errortype header, matching
// services/s3tables's handleError (same restjson1 protocol) plus
// resourceId/resourceType when the error carries them (see errors.go's
// apiError).
func (h *Handler) handleError(c *echo.Context, err error) error {
	status := http.StatusInternalServerError
	errType := "InternalServerException"

	var apiErr *apiError

	body := map[string]any{"message": err.Error()}

	switch {
	case errors.As(err, &apiErr) && errors.Is(err, errNotFoundSentinel):
		status, errType = http.StatusNotFound, "ResourceNotFoundException"
		addResourceFields(body, apiErr)
	case errors.As(err, &apiErr) && errors.Is(err, errConflictSentinel):
		status, errType = http.StatusConflict, "ConflictException"
		addResourceFields(body, apiErr)
	case errors.Is(err, errValidationSentinel):
		status, errType = http.StatusBadRequest, "ValidationException"
	case errors.Is(err, errUnknownPath):
		status, errType = http.StatusNotFound, "ResourceNotFoundException"
	}

	payload, _ := json.Marshal(body)

	c.Response().Header().Set("X-Amzn-Errortype", errType)

	return c.JSONBlob(status, payload)
}

func addResourceFields(body map[string]any, apiErr *apiError) {
	if apiErr == nil {
		return
	}

	if apiErr.resourceID != "" {
		body["resourceId"] = apiErr.resourceID
	}

	if apiErr.resourceType != "" {
		body["resourceType"] = apiErr.resourceType
	}
}

// rawPathSegments splits the raw (or decoded) URL path into non-empty
// segments, URL-decoding each segment individually so that encoded slashes
// in path params (e.g. the ARN in /tags/{resourceArn}) are preserved as a
// single segment. Identical in approach to services/s3tables's helper of
// the same name (see that package's handler.go and PARITY.md's note on
// this being the house fix for the ARN-in-path route-matching trap).
func rawPathSegments(r *http.Request) []string {
	rawPath := r.URL.RawPath
	if rawPath == "" {
		rawPath = r.URL.Path
	}

	rawPath = strings.TrimPrefix(rawPath, "/")
	parts := strings.Split(rawPath, "/")

	segments := make([]string, 0, len(parts))

	for _, p := range parts {
		if p == "" {
			continue
		}

		decoded, err := url.PathUnescape(p)
		if err != nil {
			decoded = p
		}

		segments = append(segments, decoded)
	}

	return segments
}

// queryMaxResults parses the "maxResults" query parameter as a positive
// integer, returning 0 (the "unspecified" sentinel page.New treats as
// defaultLimit) when the parameter is absent, empty, or not a valid
// non-negative integer. Every List operation in this service names its page
// size parameter "maxResults", so this is the one call this helper needs to
// make.
func queryMaxResults(q url.Values) int {
	raw := q.Get("maxResults")
	if raw == "" {
		return 0
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}

	return n
}
