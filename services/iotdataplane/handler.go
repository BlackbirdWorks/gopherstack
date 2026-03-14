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
		"Publish",
		"GetThingShadow",
		"UpdateThingShadow",
		"DeleteThingShadow",
		"ListNamedShadowsForThing",
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
			strings.HasPrefix(path, "/api/things/shadow/ListNamedShadowsForThing/")
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

	// If the body is a JSON object with a "payload" key, unwrap it.
	var wrapper map[string]json.RawMessage
	if jsonErr := json.Unmarshal(body, &wrapper); jsonErr == nil {
		if rawPayload, ok := wrapper["payload"]; ok {
			var payloadStr string
			if unmarshalErr := json.Unmarshal(rawPayload, &payloadStr); unmarshalErr == nil {
				payload = []byte(payloadStr)
			}
		}
	}

	if publishErr := h.Backend.Publish(topic, payload); publishErr != nil {
		log.Error("iot data plane publish failed", "topic", topic, "error", publishErr)

		return c.JSON(http.StatusInternalServerError, map[string]string{"error": publishErr.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"topic": topic})
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

	if updateErr := h.Backend.UpdateThingShadow(thingName, shadowName, body); updateErr != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": updateErr.Error()})
	}

	return c.Blob(http.StatusOK, "application/json", body)
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

	return c.JSON(http.StatusOK, map[string]any{
		"results":   names,
		"nextToken": nil,
	})
}
