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

// MQTT5 Publish header names, per the real SDK's serializer
// (awsRestjson1_serializeOpHttpBindingsPublishInput in
// aws-sdk-go-v2/service/iotdataplane/serializers.go). contentType,
// messageExpiry, responseTopic, and qos/retain are query params instead --
// see parseMQTT5PublishParams.
const (
	headerMQTTCorrelationData        = "X-Amz-Mqtt5-Correlation-Data"
	headerMQTTPayloadFormatIndicator = "X-Amz-Mqtt5-Payload-Format-Indicator"
	headerMQTTUserProperties         = "X-Amz-Mqtt5-User-Properties"
)

// mqtt5PublishParams holds the PublishInput fields beyond topic/qos/payload/retain.
// All are optional. Of these, only UserProperties currently has an AWS-visible
// effect in gopherstack: it's persisted onto the retained message (mirrors
// GetRetainedMessageOutput.UserProperties) when retain=true. The others aren't
// forwarded to live MQTT subscribers -- see the Publish/broker-wiring gap noted
// in PARITY.md; there is no other AWS-modeled response surface that echoes them.
type mqtt5PublishParams struct {
	ContentType            string
	CorrelationData        string
	PayloadFormatIndicator string
	ResponseTopic          string
	UserProperties         []byte
	MessageExpiry          int64
}

// parseMQTT5PublishParams parses and validates the optional MQTT5 Publish
// fields from the request's query string and headers.
func parseMQTT5PublishParams(c *echo.Context) (mqtt5PublishParams, error) {
	q := c.Request().URL.Query()
	reqHeader := c.Request().Header

	messageExpiry, err := parseMessageExpiry(q.Get("messageExpiry"))
	if err != nil {
		return mqtt5PublishParams{}, err
	}

	responseTopic := q.Get("responseTopic")
	if topicErr := validateResponseTopic(responseTopic); topicErr != nil {
		return mqtt5PublishParams{}, topicErr
	}

	payloadFormatIndicator := reqHeader.Get(headerMQTTPayloadFormatIndicator)
	if pfiErr := validatePayloadFormatIndicator(payloadFormatIndicator); pfiErr != nil {
		return mqtt5PublishParams{}, pfiErr
	}

	userProperties, err := decodeUserProperties(reqHeader.Get(headerMQTTUserProperties))
	if err != nil {
		return mqtt5PublishParams{}, err
	}

	return mqtt5PublishParams{
		ContentType:            q.Get("contentType"),
		CorrelationData:        reqHeader.Get(headerMQTTCorrelationData),
		MessageExpiry:          messageExpiry,
		PayloadFormatIndicator: payloadFormatIndicator,
		ResponseTopic:          responseTopic,
		UserProperties:         userProperties,
	}, nil
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

	mqtt5, mqtt5Err := parseMQTT5PublishParams(c)
	if mqtt5Err != nil {
		return h.handleError(c, mqtt5Err)
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

	return h.handleRetain(c, topic, payload, qos, retain, mqtt5.UserProperties)
}

// handleRetain stores a retained message when ?retain=true/1 is set.
// userProperties is the decoded MQTT5 user properties blob from the Publish
// request (see mqtt5PublishParams), persisted alongside the retained message.
func (h *Handler) handleRetain(
	c *echo.Context, topic string, payload []byte, qos int32, retain bool, userProperties []byte,
) error {
	log := logger.Load(c.Request().Context())

	if retain {
		if storeErr := h.Backend.StoreRetainedMessage(topic, payload, qos, userProperties); storeErr != nil {
			if errors.Is(storeErr, ErrValidation) {
				return h.handleError(c, storeErr)
			}

			log.Warn("failed to store retained message", "topic", topic, "error", storeErr)
		}
	}

	return c.JSON(http.StatusOK, map[string]string{})
}
