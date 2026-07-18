package iotdataplane

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	iotDPMatchPriority = 88
	// maxPublishBodyBytes limits the size of MQTT publish request bodies.
	maxPublishBodyBytes = 128 * 1024
	// maxShadowBodyBytes limits the size of shadow document request bodies.
	maxShadowBodyBytes = 8 * 1024
	// retainedMessagePath is the URL path prefix for retained message operations.
	retainedMessagePath = "/retainedMessage"
	// retainedMessagePathSlash is the prefix used to match individual topic paths.
	retainedMessagePathSlash = retainedMessagePath + "/"
	// listThingsWithShadowsPath is the admin path for listing things that have shadows.
	listThingsWithShadowsPath = "/api/things/shadow/ListThingsWithShadows"
	// listNamedShadowsPrefix is the prefix for ListNamedShadowsForThing.
	listNamedShadowsPrefix = "/api/things/shadow/ListNamedShadowsForThing/"
	// adminConnectionsPath is the Gopherstack-only admin path for managing
	// connections via RegisterConnection/ListConnections, which have no real
	// AWS iotdataplane equivalent. Prefixed with /_admin/ to distinguish
	// from the AWS API namespace.
	adminConnectionsPath = "/_admin/connections"
	// adminConnectionsPathSlash is the prefix for individual connection operations.
	adminConnectionsPathSlash = adminConnectionsPath + "/"
	// connectionsPath is the real AWS wire path for DeleteConnection (DELETE
	// /connections/{clientId}; see aws-sdk-go-v2/service/iotdataplane's
	// serializer for DeleteConnectionInput). Unlike RegisterConnection/
	// ListConnections, DeleteConnection IS a real published AWS operation, so
	// it must remain reachable at its real wire path in addition to the
	// gopherstack admin alias below.
	connectionsPath = "/connections"
	// connectionsPathSlash is the prefix for individual connections at the real path.
	connectionsPathSlash = connectionsPath + "/"
	// defaultPageSize is the default number of items returned per page (AWS default).
	defaultPageSize = 25
	// maxPageSize is the maximum number of items returned per page (AWS cap).
	maxPageSize = 100

	keyError   = "error"
	keyMessage = "message"
	// errMethodNotAllowed is the AWS iotdataplane wire error code for a
	// combination of HTTP verb and URI that isn't supported (see
	// aws-sdk-go-v2/service/iotdataplane/types.MethodNotAllowedException).
	errMethodNotAllowed = "MethodNotAllowedException"
	opUnknown           = "Unknown"
	opDeleteConnection  = "DeleteConnection"

	// shadowPathParts is the number of parts when splitting a shadow URL on "/shadow".
	shadowPathParts = 2
)

// Handler is the Echo HTTP handler for IoT Data Plane operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new IoT Data Plane Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all handler state by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTDataPlane" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opDeleteConnection,
		"DeleteThingShadow",
		"GetRetainedMessage",
		"GetThingShadow",
		"ListConnections",
		"ListNamedShadowsForThing",
		"ListRetainedMessages",
		"ListThingsWithShadows",
		"Publish",
		"RegisterConnection",
		"UpdateThingShadow",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "iotdata" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT Data Plane instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching IoT Data Plane requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/topics/") ||
			isShadowPath(path) ||
			strings.HasPrefix(path, listNamedShadowsPrefix) ||
			path == listThingsWithShadowsPath ||
			path == adminConnectionsPath ||
			strings.HasPrefix(path, adminConnectionsPathSlash) ||
			isDeleteConnectionPath(path, c.Request().Method) ||
			path == retainedMessagePath ||
			strings.HasPrefix(path, retainedMessagePathSlash)
	}
}

// isShadowPath returns true for paths of the form:
//   - /things/{thingName}/shadow                  (classic shadow)
//   - /things/{thingName}/shadow?name=...          (named shadow via query)
//   - /things/{thingName}/shadow/name/{shadowName} (named shadow via path)
func isShadowPath(path string) bool {
	after, ok := strings.CutPrefix(path, "/things/")
	if !ok {
		return false
	}

	// Accept /things/{name}/shadow/name/{shadowName} (path-style named shadow).
	if before, _, found := strings.Cut(after, "/shadow/name/"); found {
		// thingName must be non-empty.
		return len(before) > 0
	}

	// Must end with exactly /shadow or /shadow?... (no other trailing segments).
	parts := strings.SplitN(after, "/shadow", shadowPathParts)
	if len(parts) != shadowPathParts {
		return false
	}

	tail := parts[1]

	return tail == "" || strings.HasPrefix(tail, "?")
}

// MatchPriority returns the routing priority for the IoT Data Plane handler.
func (h *Handler) MatchPriority() int { return iotDPMatchPriority }

// isDeleteConnectionPath reports whether path/method address the real AWS
// DeleteConnection wire path: DELETE /connections/{clientId}. Only DELETE is
// real here -- GET/POST on the bare /connections prefix have no AWS
// equivalent (those live under adminConnectionsPath instead).
func isDeleteConnectionPath(path, method string) bool {
	return method == http.MethodDelete && strings.HasPrefix(path, connectionsPathSlash)
}

// extractConnectionClientID returns the clientId path segment for either the
// gopherstack admin connections path or the real AWS DeleteConnection path.
func extractConnectionClientID(path string) string {
	if after, ok := strings.CutPrefix(path, adminConnectionsPathSlash); ok {
		return after
	}

	return strings.TrimPrefix(path, connectionsPathSlash)
}

// extractConnectionOperation returns the operation name for /_admin/connections paths.
func extractConnectionOperation(path, method string) string {
	if path == adminConnectionsPath {
		if method == http.MethodGet {
			return "ListConnections"
		}

		return opUnknown
	}

	switch method {
	case http.MethodDelete:
		return opDeleteConnection
	case http.MethodPost:
		return "RegisterConnection"
	}

	return opUnknown
}

// extractShadowOperation returns the operation name for /things/{name}/shadow paths.
func extractShadowOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetThingShadow"
	case http.MethodPost:
		return "UpdateThingShadow"
	case http.MethodDelete:
		return "DeleteThingShadow"
	}

	return opUnknown
}

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method
	switch {
	case strings.HasPrefix(path, "/topics/"):
		return "Publish"
	case strings.HasPrefix(path, listNamedShadowsPrefix):
		return "ListNamedShadowsForThing"
	case path == listThingsWithShadowsPath:
		return "ListThingsWithShadows"
	case path == adminConnectionsPath || strings.HasPrefix(path, adminConnectionsPathSlash):
		return extractConnectionOperation(path, method)
	case isDeleteConnectionPath(path, method):
		return opDeleteConnection
	case path == retainedMessagePath && method == http.MethodGet:
		return "ListRetainedMessages"
	case strings.HasPrefix(path, retainedMessagePathSlash) && method == http.MethodGet:
		return "GetRetainedMessage"
	case isShadowPath(path):
		return extractShadowOperation(method)
	}

	return opUnknown
}

// ExtractResource extracts the topic or thing name from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	if after, ok := strings.CutPrefix(path, "/topics/"); ok {
		return after
	}

	if after, ok := strings.CutPrefix(path, listNamedShadowsPrefix); ok {
		return after
	}

	if path == listThingsWithShadowsPath {
		return ""
	}

	if path == adminConnectionsPath {
		return ""
	}

	if after, ok := strings.CutPrefix(path, adminConnectionsPathSlash); ok {
		return after
	}

	if after, ok := strings.CutPrefix(path, connectionsPathSlash); ok {
		return after
	}

	if path == retainedMessagePath {
		return ""
	}

	if after, ok := strings.CutPrefix(path, retainedMessagePathSlash); ok {
		return after
	}

	thingName, _ := parseShadowPath(path)

	return thingName
}

// parseShadowPath extracts thingName and shadowName from shadow URL paths.
// Supports both /things/{name}/shadow?name=... and /things/{name}/shadow/name/{shadowName}.
// shadowName is empty for the classic (unnamed) shadow.
func parseShadowPath(path string) (string, string) {
	trimmed := strings.TrimPrefix(path, "/things/")

	// Path-style named shadow: /things/{name}/shadow/name/{shadowName}
	if before, after, ok := strings.Cut(trimmed, "/shadow/name/"); ok {
		return before, after
	}

	// Classic or query-param named shadow: /things/{name}/shadow
	parts := strings.SplitN(trimmed, "/shadow", shadowPathParts)

	return parts[0], ""
}

// handleError maps backend errors to appropriate HTTP status codes.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrShadowNotFound), errors.Is(err, ErrRetainedMessageNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{
			keyError:   "ResourceNotFoundException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrVersionConflict):
		return c.JSON(http.StatusConflict, map[string]any{
			"code":     http.StatusConflict,
			keyError:   "ConflictException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrRequestTooLarge):
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{
			keyError:   "RequestEntityTooLargeException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyError:   "InvalidRequestException",
			keyMessage: err.Error(),
		})
	case errors.Is(err, ErrConnectionExists):
		return c.JSON(http.StatusConflict, map[string]string{
			keyError:   "ResourceAlreadyExistsException",
			keyMessage: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}
}

// Handler returns the Echo handler function for IoT Data Plane operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		path := c.Request().URL.Path
		switch {
		case strings.HasPrefix(path, "/topics/"):
			return h.handlePublish(c)
		case strings.HasPrefix(path, listNamedShadowsPrefix):
			return h.handleListNamedShadows(c)
		case path == listThingsWithShadowsPath:
			return h.handleListThingsWithShadows(c)
		case path == adminConnectionsPath:
			return h.handleConnections(c)
		case strings.HasPrefix(path, adminConnectionsPathSlash):
			return h.handleConnectionByID(c)
		case isDeleteConnectionPath(path, c.Request().Method):
			return h.handleDeleteConnection(c)
		case path == retainedMessagePath:
			return h.handleListRetainedMessages(c)
		case strings.HasPrefix(path, retainedMessagePathSlash):
			return h.handleGetRetainedMessage(c)
		case isShadowPath(path):
			return h.handleShadow(c)
		default:
			return c.JSON(http.StatusNotFound, map[string]string{keyError: "not found"})
		}
	}
}

// parsePageSize extracts the pagination page size from query params.
// pageSize is the primary parameter (AWS convention); maxResults is accepted as an alias.
// Returns the effective page size clamped to [1, maxPageSize], or defaultPageSize when absent.
func parsePageSize(q interface{ Get(string) string }) int {
	// pageSize takes precedence over maxResults (AWS convention).
	raw := q.Get("pageSize")
	if raw == "" {
		raw = q.Get("maxResults")
	}

	if raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			if v > maxPageSize {
				return maxPageSize
			}

			return v
		}
	}

	return defaultPageSize
}

// findCursorIndex returns the start index for the given nextToken cursor in items.
// nextToken holds the first item of the next page (set to items[end] at write time),
// so the cursor item IS the first item of the new page — we start AT it (not after it).
// Returns 0 if the cursor is not found or is empty.
func findCursorIndex(items []string, cursor string) int {
	if cursor == "" {
		return 0
	}

	for i, item := range items {
		if item == cursor {
			return i
		}
	}

	return 0
}
