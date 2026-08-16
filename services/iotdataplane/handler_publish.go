package iotdataplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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

// mqtt5PublishParams holds the PublishInput/SendDirectMessageInput fields
// beyond topic/qos/payload/retain -- both ops share the same wire locations
// (see handleSendDirectMessage). All are optional. UserProperties is
// persisted onto the retained message (mirrors
// GetRetainedMessageOutput.UserProperties) when Publish's retain=true; every
// field now also reaches the broker for live-subscriber delivery via
// toMQTT5Properties (see PublishWithProperties/SendToClientWithProperties).
type mqtt5PublishParams struct {
	ContentType            string
	PayloadFormatIndicator string
	ResponseTopic          string
	CorrelationData        []byte
	UserProperties         []byte
	UserPropertiesParsed   []MQTT5UserProperty
	MessageExpiry          int64
}

// toMQTT5Properties converts the parsed/validated MQTT5 Publish fields into
// the MQTTPublisher-facing shape the broker forwards as real MQTT5 packet
// properties.
func (p mqtt5PublishParams) toMQTT5Properties() MQTT5Properties {
	return MQTT5Properties{
		ContentType:            p.ContentType,
		ResponseTopic:          p.ResponseTopic,
		PayloadFormatIndicator: p.PayloadFormatIndicator,
		CorrelationData:        p.CorrelationData,
		UserProperties:         p.UserPropertiesParsed,
		MessageExpiry:          p.MessageExpiry,
	}
}

// parseMQTT5PublishParams parses and validates the optional MQTT5 fields
// shared by Publish and SendDirectMessage from the request's query string
// and headers (identical header/query names for both ops -- confirmed via
// awsRestjson1_serializeOpHttpBindingsSendDirectMessageInput alongside
// Publish's own serializer, aws-sdk-go-v2/service/iotdataplane@v1.35.4).
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

	correlationData, err := decodeCorrelationData(reqHeader.Get(headerMQTTCorrelationData))
	if err != nil {
		return mqtt5PublishParams{}, err
	}

	userProperties, err := decodeUserProperties(reqHeader.Get(headerMQTTUserProperties))
	if err != nil {
		return mqtt5PublishParams{}, err
	}

	userPropertiesParsed, err := parseUserProperties(userProperties)
	if err != nil {
		return mqtt5PublishParams{}, err
	}

	return mqtt5PublishParams{
		ContentType:            q.Get("contentType"),
		CorrelationData:        correlationData,
		MessageExpiry:          messageExpiry,
		PayloadFormatIndicator: payloadFormatIndicator,
		ResponseTopic:          responseTopic,
		UserProperties:         userProperties,
		UserPropertiesParsed:   userPropertiesParsed,
	}, nil
}

// handlePublish processes POST /topics/{topic} requests.
func (h *Handler) handlePublish(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	if c.Request().Method != http.MethodPost {
		return methodNotAllowedResponse(c)
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, "/topics/")
	if topic == "" {
		return invalidRequestResponse(c, "topic is required")
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
		// No X-Amzn-Errortype header here: Publish models only
		// InternalFailureException, InvalidRequestException,
		// MethodNotAllowedException, ThrottlingException and
		// UnauthorizedException (iotdataplane@v1.35.4 deserializers.go) --
		// unlike SendDirectMessage/UpdateThingShadow, it does not model
		// RequestEntityTooLargeException, so there's no verified type to emit.
		return c.JSON(
			http.StatusRequestEntityTooLarge,
			map[string]string{keyError: "request body too large"},
		)
	}

	payload := unwrapPublishPayload(body, contentType)

	if publishErr := h.Backend.Publish(topic, payload, qos, retain, mqtt5.toMQTT5Properties()); publishErr != nil {
		if errors.Is(publishErr, ErrNoBroker) {
			log.Warn("iot data plane: no broker configured, message dropped", "topic", topic)
		} else {
			log.Error("iot data plane publish failed", "topic", topic, "error", publishErr)

			return h.handleError(c, publishErr)
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
