package iotdataplane

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	iotDPMatchPriority = 88
	unknownOperation   = "Unknown"
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
	return []string{"Publish"}
}

// RouteMatcher returns a function matching IoT Data Plane requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, "/topics/")
	}
}

// MatchPriority returns the routing priority for the IoT Data Plane handler.
func (h *Handler) MatchPriority() int { return iotDPMatchPriority }

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(_ *echo.Context) string { return "Publish" }

// ExtractResource extracts the topic name from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().URL.Path, "/topics/")
}

// Handler returns the Echo handler function for IoT Data Plane operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())

		if c.Request().Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}

		topic := strings.TrimPrefix(c.Request().URL.Path, "/topics/")
		if topic == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "topic is required"})
		}

		// Accept the raw body as the MQTT payload; also accept a JSON wrapper.
		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}

		payload := body

		// If the body is a JSON object with a "payload" key, unwrap it.
		var wrapper PublishInput
		if jsonErr := json.Unmarshal(body, &wrapper); jsonErr == nil && wrapper.Payload != "" {
			payload = []byte(wrapper.Payload)
		}

		if publishErr := h.Backend.Publish(topic, payload); publishErr != nil {
			log.Error("iot data plane publish failed", "topic", topic, "error", publishErr)

			return c.JSON(http.StatusInternalServerError, map[string]string{"error": publishErr.Error()})
		}

		return c.JSON(http.StatusOK, map[string]string{"topic": topic})
	}
}
