package iotdataplane

import (
	"encoding/json"
	"io"
	"net/http"
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

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "iotdata" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT Data Plane instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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

		// Limit the request body size to prevent excessive memory usage.
		c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxPublishBodyBytes)

		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		}

		payload := body

		// If the body is a JSON object with a "payload" key, unwrap it.
		// Use map[string]json.RawMessage to detect the key regardless of the value.
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
}
