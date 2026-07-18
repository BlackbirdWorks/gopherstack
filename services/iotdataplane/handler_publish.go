package iotdataplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/labstack/echo/v5"
)

// parsePublishQoS extracts and validates the qos query parameter.
func parsePublishQoS(qosStr string) (int32, error) {
	if qosStr == "" {
		return 0, nil
	}

	qosVal, err := strconv.ParseInt(qosStr, 10, 32)
	if err != nil || qosVal < 0 || qosVal > 1 {
		return 0, fmt.Errorf("%w: qos must be 0 or 1", ErrValidation)
	}

	return int32(qosVal), nil
}

// unwrapPublishPayload unwraps a JSON `{"payload":"..."}` envelope when the
// content type is JSON (or absent). Binary payloads (application/octet-stream)
// are returned unchanged to avoid double-decoding.
func unwrapPublishPayload(body []byte, contentType string) []byte {
	// Skip JSON unwrap for explicit binary content types.
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if ct == "application/octet-stream" {
		return body
	}

	var wrapper struct {
		Payload *json.RawMessage `json:"payload"`
	}

	if jsonErr := json.Unmarshal(body, &wrapper); jsonErr == nil && wrapper.Payload != nil {
		var payloadStr string
		if unmarshalErr := json.Unmarshal(*wrapper.Payload, &payloadStr); unmarshalErr == nil {
			return []byte(payloadStr)
		}
	}

	return body
}

// parseRetainFlag parses the ?retain= query parameter. Accepts "true", "1" (case-insensitive).
func parseRetainFlag(retainStr string) bool {
	v := strings.ToLower(retainStr)

	return v == "true" || v == "1"
}

// handlePublish processes POST /topics/{topic} requests.
func (h *Handler) handlePublish(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, "/topics/")
	if topic == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "topic is required"})
	}

	if err := validateTopic(topic); err != nil {
		return h.handleError(c, err)
	}

	qos, qosErr := parsePublishQoS(c.Request().URL.Query().Get("qos"))
	if qosErr != nil {
		return h.handleError(c, qosErr)
	}

	retain := parseRetainFlag(c.Request().URL.Query().Get("retain"))
	contentType := c.Request().Header.Get("Content-Type")

	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxPublishBodyBytes)

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{keyError: "request body too large"})
	}

	payload := unwrapPublishPayload(body, contentType)

	if publishErr := h.Backend.Publish(topic, payload, qos, retain); publishErr != nil {
		if errors.Is(publishErr, ErrNoBroker) {
			log.Warn("iot data plane: no broker configured, message dropped", "topic", topic)
		} else {
			log.Error("iot data plane publish failed", "topic", topic, "error", publishErr)

			return c.JSON(http.StatusInternalServerError, map[string]string{keyError: publishErr.Error()})
		}
	}

	return h.handleRetain(c, topic, payload, qos, retain)
}

// handleRetain stores a retained message when ?retain=true/1 is set.
func (h *Handler) handleRetain(c *echo.Context, topic string, payload []byte, qos int32, retain bool) error {
	log := logger.Load(c.Request().Context())

	if retain {
		if storeErr := h.Backend.StoreRetainedMessage(topic, payload, qos); storeErr != nil {
			if errors.Is(storeErr, ErrValidation) {
				return h.handleError(c, storeErr)
			}

			log.Warn("failed to store retained message", "topic", topic, "error", storeErr)
		}
	}

	return c.JSON(http.StatusOK, map[string]string{})
}
