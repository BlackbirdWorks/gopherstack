package iotdataplane

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	iotDPMatchPriority = 88
	// maxPublishBodyBytes limits the size of MQTT publish request bodies.
	maxPublishBodyBytes = 128 * 1024
	// maxShadowBodyBytes limits the size of shadow document request bodies.
	maxShadowBodyBytes = 8 * 1024
	// retainedMessagePath is the URL path prefix for retained message operations.
	retainedMessagePath = "/retainedMessage"
)

// Handler is the Echo HTTP handler for IoT Data Plane operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new IoT Data Plane Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTDataPlane" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"DeleteConnection",
		"DeleteThingShadow",
		"GetRetainedMessage",
		"GetThingShadow",
		"ListNamedShadowsForThing",
		"ListRetainedMessages",
		"Publish",
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
			strings.HasPrefix(path, "/things/") ||
			strings.HasPrefix(path, "/api/things/shadow/ListNamedShadowsForThing/") ||
			strings.HasPrefix(path, "/connections/") ||
			path == retainedMessagePath ||
			strings.HasPrefix(path, retainedMessagePath+"/")
	}
}

// MatchPriority returns the routing priority for the IoT Data Plane handler.
func (h *Handler) MatchPriority() int { return iotDPMatchPriority }

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method
	switch {
	case strings.HasPrefix(path, "/topics/"):
		return "Publish"
	case strings.HasPrefix(path, "/api/things/shadow/ListNamedShadowsForThing/"):
		return "ListNamedShadowsForThing"
	case strings.HasPrefix(path, "/connections/") && method == http.MethodDelete:
		return "DeleteConnection"
	case path == retainedMessagePath && method == http.MethodGet:
		return "ListRetainedMessages"
	case strings.HasPrefix(path, retainedMessagePath+"/") && method == http.MethodGet:
		return "GetRetainedMessage"
	case strings.HasPrefix(path, "/things/") && strings.HasSuffix(path, "/shadow"):
		switch method {
		case http.MethodGet:
			return "GetThingShadow"
		case http.MethodPost:
			return "UpdateThingShadow"
		case http.MethodDelete:
			return "DeleteThingShadow"
		}
	}

	return "Unknown"
}

// ExtractResource extracts the topic or thing name from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	if after, ok := strings.CutPrefix(path, "/topics/"); ok {
		return after
	}

	if after, ok := strings.CutPrefix(path, "/api/things/shadow/ListNamedShadowsForThing/"); ok {
		return after
	}

	if after, ok := strings.CutPrefix(path, "/connections/"); ok {
		return after
	}

	if path == retainedMessagePath {
		return ""
	}

	if after, ok := strings.CutPrefix(path, retainedMessagePath+"/"); ok {
		return after
	}

	// /things/{thingName}/shadow
	thingName := parseShadowPath(path)

	return thingName
}

// parseShadowPath extracts thingName from a /things/{thingName}/shadow path.
func parseShadowPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/things/")
	const shadowPathParts = 2
	parts := strings.SplitN(trimmed, "/shadow", shadowPathParts)

	return parts[0]
}

// Handler returns the Echo handler function for IoT Data Plane operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		path := c.Request().URL.Path
		switch {
		case strings.HasPrefix(path, "/topics/"):
			return h.handlePublish(c)
		case strings.HasPrefix(path, "/api/things/shadow/ListNamedShadowsForThing/"):
			return h.handleListNamedShadows(c)
		case strings.HasPrefix(path, "/connections/"):
			return h.handleDeleteConnection(c)
		case path == retainedMessagePath:
			return h.handleListRetainedMessages(c)
		case strings.HasPrefix(path, retainedMessagePath+"/"):
			return h.handleGetRetainedMessage(c)
		case strings.HasPrefix(path, "/things/") && strings.HasSuffix(path, "/shadow"):
			return h.handleShadow(c)
		default:
			return c.JSON(http.StatusNotFound, map[string]string{"error": "not found"})
		}
	}
}

// handlePublish processes POST /topics/{topic} requests.
func (h *Handler) handlePublish(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, "/topics/")
	if topic == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "topic is required"})
	}

	// Limit the request body size to prevent excessive memory usage.
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxPublishBodyBytes)

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
	}

	payload := body

	// If the body is a JSON object with a "payload" string key, unwrap it.
	// Using a fixed struct avoids the map allocation that a map[string]json.RawMessage would incur.
	var wrapper struct {
		Payload *json.RawMessage `json:"payload"`
	}

	if jsonErr := json.Unmarshal(body, &wrapper); jsonErr == nil && wrapper.Payload != nil {
		var payloadStr string
		if unmarshalErr := json.Unmarshal(*wrapper.Payload, &payloadStr); unmarshalErr == nil {
			payload = []byte(payloadStr)
		}
	}

	if publishErr := h.Backend.Publish(topic, payload); publishErr != nil {
		log.Error("iot data plane publish failed", "topic", topic, "error", publishErr)

		return c.JSON(http.StatusInternalServerError, map[string]string{"error": publishErr.Error()})
	}

	// Store as retained message when the caller sets ?retain=true.
	if strings.ToLower(c.Request().URL.Query().Get("retain")) == "true" {
		if storeErr := h.Backend.StoreRetainedMessage(topic, payload, 0); storeErr != nil {
			log.Warn("failed to store retained message", "topic", topic, "error", storeErr)
		}
	}

	return c.JSON(http.StatusOK, map[string]string{"topic": topic})
}

// handleDeleteConnection processes DELETE /connections/{clientId} requests.
func (h *Handler) handleDeleteConnection(c *echo.Context) error {
	if c.Request().Method != http.MethodDelete {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}

	clientID := strings.TrimPrefix(c.Request().URL.Path, "/connections/")
	if clientID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "clientId is required"})
	}

	if err := h.Backend.DeleteConnection(clientID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// handleGetRetainedMessage processes GET /retainedMessage/{topic} requests.
func (h *Handler) handleGetRetainedMessage(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, retainedMessagePath+"/")
	if topic == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "topic is required"})
	}

	msg, err := h.Backend.GetRetainedMessage(topic)
	if err != nil {
		if errors.Is(err, ErrShadowNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error":   "ResourceNotFoundException",
				"message": err.Error(),
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	resp := map[string]any{
		"topic":            msg.Topic,
		"payload":          msg.Payload,
		"qos":              msg.Qos,
		"lastModifiedTime": msg.LastModifiedTime,
	}

	return c.JSON(http.StatusOK, resp)
}

// handleListRetainedMessages processes GET /retainedMessage requests.
func (h *Handler) handleListRetainedMessages(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}

	msgs, err := h.Backend.ListRetainedMessages()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	summaries := make([]map[string]any, 0, len(msgs))
	for _, msg := range msgs {
		summaries = append(summaries, map[string]any{
			"topic":            msg.Topic,
			"payloadSize":      int64(len(msg.Payload)),
			"qos":              msg.Qos,
			"lastModifiedTime": msg.LastModifiedTime,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"retainedTopics": summaries,
	})
}

// handleShadow dispatches GET/POST/DELETE /things/{thingName}/shadow requests.
func (h *Handler) handleShadow(c *echo.Context) error {
	thingName := parseShadowPath(c.Request().URL.Path)
	if thingName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "thingName is required"})
	}

	// Named shadow support via ?name= query parameter.
	shadowName := c.Request().URL.Query().Get("name")

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetThingShadow(c, thingName, shadowName)
	case http.MethodPost:
		return h.handleUpdateThingShadow(c, thingName, shadowName)
	case http.MethodDelete:
		return h.handleDeleteThingShadow(c, thingName, shadowName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// handleGetThingShadow processes GET /things/{thingName}/shadow.
func (h *Handler) handleGetThingShadow(c *echo.Context, thingName, shadowName string) error {
	doc, err := h.Backend.GetThingShadow(thingName, shadowName)
	if err != nil {
		if errors.Is(err, ErrShadowNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error":   "ResourceNotFoundException",
				"message": err.Error(),
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.Blob(http.StatusOK, "application/json", doc)
}

// handleUpdateThingShadow processes POST /things/{thingName}/shadow.
func (h *Handler) handleUpdateThingShadow(c *echo.Context, thingName, shadowName string) error {
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxShadowBodyBytes)

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
	}

	updated, updateErr := h.Backend.UpdateThingShadow(thingName, shadowName, body)
	if updateErr != nil {
		if errors.Is(updateErr, ErrVersionConflict) {
			return c.JSON(http.StatusConflict, map[string]string{
				"error":   "VersionConflictException",
				"message": updateErr.Error(),
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"error": updateErr.Error()})
	}

	return c.Blob(http.StatusOK, "application/json", updated)
}

// handleDeleteThingShadow processes DELETE /things/{thingName}/shadow.
func (h *Handler) handleDeleteThingShadow(c *echo.Context, thingName, shadowName string) error {
	if err := h.Backend.DeleteThingShadow(thingName, shadowName); err != nil {
		if errors.Is(err, ErrShadowNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error":   "ResourceNotFoundException",
				"message": err.Error(),
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// handleListNamedShadows processes GET /api/things/shadow/ListNamedShadowsForThing/{thingName}.
func (h *Handler) handleListNamedShadows(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}

	thingName := strings.TrimPrefix(c.Request().URL.Path, "/api/things/shadow/ListNamedShadowsForThing/")
	if thingName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "thingName is required"})
	}

	names, err := h.Backend.ListNamedShadowsForThing(thingName)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	sort.Strings(names)

	resp := map[string]any{
		"results": names,
	}

	return c.JSON(http.StatusOK, resp)
}
