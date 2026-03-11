package codeconnections

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codeconnectionsMatchPriority = service.PriorityPathVersioned

	pathConnections = "/connections"
	pathTagsCC      = "/tags/arn:aws:codeconnections:"
	pathTagsCS      = "/tags/arn:aws:codestar-connections:"
)

// Handler is the Echo HTTP handler for AWS CodeConnections operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeConnections handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codeconnections" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeConnections instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeConnections REST requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == pathConnections ||
			strings.HasPrefix(path, pathConnections+"/") ||
			strings.HasPrefix(path, pathTagsCC) ||
			strings.HasPrefix(path, pathTagsCS)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return codeconnectionsMatchPriority }

// ccRoute holds the parsed information from a CodeConnections REST request path.
type ccRoute struct {
	resource  string
	operation string
}

// parseRoute maps HTTP method + path to an operation name and resource identifier.
func parseRoute(method, rawPath string) ccRoute {
	path, _ := url.PathUnescape(rawPath)

	switch {
	case strings.HasPrefix(path, pathTagsCC):
		return parseTagRoute(method, strings.TrimPrefix(path, "/tags/"))
	case strings.HasPrefix(path, pathTagsCS):
		return parseTagRoute(method, strings.TrimPrefix(path, "/tags/"))
	case strings.HasPrefix(path, pathConnections):
		return parseConnectionRoute(method, strings.TrimPrefix(path, pathConnections))
	}

	return ccRoute{operation: "Unknown"}
}

func parseConnectionRoute(method, suffix string) ccRoute {
	// Connection ARNs may contain "/" so we cannot use it to detect nesting.
	// The API only has two levels: collection (/connections) and item (/connections/{arn}).
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		switch method {
		case http.MethodPost:
			return ccRoute{operation: "CreateConnection"}
		case http.MethodGet:
			return ccRoute{operation: "ListConnections"}
		}
	} else {
		switch method {
		case http.MethodGet:
			return ccRoute{operation: "GetConnection", resource: id}
		case http.MethodDelete:
			return ccRoute{operation: "DeleteConnection", resource: id}
		}
	}

	return ccRoute{operation: "Unknown"}
}

func parseTagRoute(method, resourceArn string) ccRoute {
	switch method {
	case http.MethodPost:
		return ccRoute{operation: "TagResource", resource: resourceArn}
	case http.MethodDelete:
		return ccRoute{operation: "UntagResource", resource: resourceArn}
	case http.MethodGet:
		return ccRoute{operation: "ListTagsForResource", resource: resourceArn}
	}

	return ccRoute{operation: "Unknown"}
}

// ExtractOperation extracts the CodeConnections operation name from the REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := parseRoute(c.Request().Method, c.Request().URL.Path)

	return r.operation
}

// ExtractResource extracts the primary resource identifier from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := parseRoute(c.Request().Method, c.Request().URL.Path)

	return r.resource
}

// Handler returns the Echo handler function for CodeConnections requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		route := parseRoute(c.Request().Method, c.Request().URL.Path)

		log.Debug("codeconnections request", "operation", route.operation, "resource", route.resource)

		var body []byte
		if c.Request().Body != nil {
			decoder := json.NewDecoder(c.Request().Body)
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err == nil {
				body = raw
			}
		}

		return h.dispatch(c, route, body)
	}
}

func (h *Handler) dispatch(c *echo.Context, route ccRoute, body []byte) error {
	switch route.operation {
	case "CreateConnection":
		return h.handleCreateConnection(c, body)
	case "GetConnection":
		return h.handleGetConnection(c, route.resource)
	case "ListConnections":
		return h.handleListConnections(c)
	case "DeleteConnection":
		return h.handleDeleteConnection(c, route.resource)
	case "TagResource":
		return h.handleTagResource(c, route.resource, body)
	case "UntagResource":
		return h.handleUntagResource(c, route.resource)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, route.resource)
	default:
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusBadRequest, errResp("ResourceNotFoundException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// tag is the JSON representation of a CodeConnections tag (array format).
type tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsToArray(m map[string]string) []tag {
	out := make([]tag, 0, len(m))
	for k, v := range m {
		out = append(out, tag{Key: k, Value: v})
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

// --- Connection handlers ---

type createConnectionBody struct {
	ConnectionName string `json:"ConnectionName"`
	ProviderType   string `json:"ProviderType"`
	Tags           []tag  `json:"Tags"`
}

func (h *Handler) handleCreateConnection(c *echo.Context, body []byte) error {
	var in createConnectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.ConnectionName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "ConnectionName is required"))
	}

	tags := tagsFromArray(in.Tags)

	conn, err := h.Backend.CreateConnection(in.ConnectionName, in.ProviderType, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ConnectionArn": conn.ConnectionArn,
	})
}

func (h *Handler) handleGetConnection(c *echo.Context, connectionArn string) error {
	conn, err := h.Backend.GetConnection(connectionArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Connection": map[string]any{
			"ConnectionName":   conn.ConnectionName,
			"ConnectionArn":    conn.ConnectionArn,
			"ProviderType":     conn.ProviderType,
			"ConnectionStatus": conn.Status,
			"OwnerAccountId":   conn.OwnerAccountID,
		},
	})
}

func (h *Handler) handleListConnections(c *echo.Context) error {
	providerTypeFilter := c.Request().URL.Query().Get("providerType")
	conns := h.Backend.ListConnections(providerTypeFilter)

	items := make([]map[string]any, 0, len(conns))
	for _, conn := range conns {
		items = append(items, map[string]any{
			"ConnectionName":   conn.ConnectionName,
			"ConnectionArn":    conn.ConnectionArn,
			"ProviderType":     conn.ProviderType,
			"ConnectionStatus": conn.Status,
			"OwnerAccountId":   conn.OwnerAccountID,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Connections": items,
	})
}

func (h *Handler) handleDeleteConnection(c *echo.Context, connectionArn string) error {
	if err := h.Backend.DeleteConnection(connectionArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Tag handlers ---

type tagResourceBody struct {
	Tags []tag `json:"Tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	tags := tagsFromArray(in.Tags)

	if err := h.Backend.TagResource(resourceArn, tags); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, tagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Tags": tagsToArray(tags),
	})
}
