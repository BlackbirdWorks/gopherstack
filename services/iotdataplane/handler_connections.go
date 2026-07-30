package iotdataplane

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// handleConnections dispatches GET /_admin/connections requests.
func (h *Handler) handleConnections(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	return h.handleListConnections(c)
}

// handleConnectionByID dispatches requests for /_admin/connections/{clientId}.
func (h *Handler) handleConnectionByID(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodDelete:
		return h.handleDeleteConnection(c)
	case http.MethodPost:
		return h.handleRegisterConnection(c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}
}

// handleListConnections processes GET /_admin/connections requests.
func (h *Handler) handleListConnections(c *echo.Context) error {
	conns := h.Backend.ListConnections()

	type connResp struct {
		ClientID    string `json:"clientId"`
		SourceIP    string `json:"sourceIp,omitempty"`
		ConnectedAt int64  `json:"connectedAt"`
	}

	out := make([]connResp, 0, len(conns))
	for _, conn := range conns {
		out = append(out, connResp{
			ClientID:    conn.ClientID,
			SourceIP:    conn.SourceIP,
			ConnectedAt: conn.ConnectedAt.Unix(),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"connections": out})
}

// handleRegisterConnection processes POST /_admin/connections/{clientId} requests.
func (h *Handler) handleRegisterConnection(c *echo.Context) error {
	clientID := strings.TrimPrefix(c.Request().URL.Path, adminConnectionsPathSlash)
	if clientID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "clientId is required"})
	}

	sourceIP := c.RealIP()

	if err := h.Backend.RegisterConnection(clientID, sourceIP); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]string{"clientId": clientID})
}

// handleDeleteConnection processes DELETE requests for both the gopherstack
// admin path (/_admin/connections/{clientId}) and the real AWS wire path
// (/connections/{clientId}).
func (h *Handler) handleDeleteConnection(c *echo.Context) error {
	clientID := extractConnectionClientID(c.Request().URL.Path)
	if clientID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "clientId is required"})
	}

	if err := h.Backend.DeleteConnection(clientID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// handleConnectionsWire dispatches requests matched by connectionsWireOperation
// -- the real AWS wire paths rooted at /connections/{clientId}: GetConnection,
// DeleteConnection, ListSubscriptions, SendDirectMessage.
func (h *Handler) handleConnectionsWire(c *echo.Context) error {
	switch connectionsWireOperation(c.Request().URL.Path, c.Request().Method) {
	case opGetConnection:
		return h.handleGetConnection(c)
	case opDeleteConnection:
		return h.handleDeleteConnection(c)
	case opListSubscriptions:
		return h.handleListSubscriptions(c)
	case opSendDirectMessage:
		return h.handleSendDirectMessage(c)
	default:
		// Unreachable in practice: Handler() only routes here when
		// connectionsWireOperation already returned a non-"" op.
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}
}

// getConnectionResponse mirrors GetConnectionOutput's JSON shape (see
// aws-sdk-go-v2/service/iotdataplane/deserializers.go's
// awsRestjson1_deserializeOpDocumentGetConnectionOutput field list:
// cleanSession, clientId, connected, connectedSince, disconnectReason,
// disconnectedSince, keepAliveDuration, sessionExpiry, sourceIp, sourcePort,
// targetIp, targetPort, thingName, vpcEndpointId). gopherstack's connections
// table (populated only via the gopherstack-only RegisterConnection admin
// extension -- see admin-only-extensions family in PARITY.md) genuinely
// tracks only clientId/sourceIp/connectedAt; every other field above has no
// real backing data here and is therefore omitted from this response type
// entirely rather than fabricated with a plausible-looking zero value. A
// real SDK client decodes an absent JSON key exactly the same as an explicit
// zero value for these optional fields, so this is wire-compatible, not a
// shortcut.
type getConnectionResponse struct {
	ClientID       string `json:"clientId"`
	SourceIP       string `json:"sourceIp,omitempty"`
	ConnectedSince int64  `json:"connectedSince,omitempty"`
	Connected      bool   `json:"connected"`
}

// handleGetConnection processes GET /connections/{clientId} requests.
func (h *Handler) handleGetConnection(c *echo.Context) error {
	clientID := extractConnectionClientID(c.Request().URL.Path)
	if clientID == "" {
		return h.handleError(c, fmt.Errorf("%w: clientId is required", ErrValidation))
	}

	conn, err := h.Backend.GetConnection(clientID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := getConnectionResponse{
		ClientID:       conn.ClientID,
		Connected:      true,
		ConnectedSince: conn.ConnectedAt.UnixMilli(),
	}

	// includeSocketInformation defaults to false per GetConnectionInput; only
	// echo the (genuinely tracked) sourceIp when the caller opted in, mirroring
	// the real API's documented gating.
	if parseRetainFlag(c.Request().URL.Query().Get("includeSocketInformation")) {
		resp.SourceIP = conn.SourceIP
	}

	return c.JSON(http.StatusOK, resp)
}

// subscriptionSummaryResponse mirrors types.SubscriptionSummary (topicFilter, qos).
type subscriptionSummaryResponse struct {
	TopicFilter string `json:"topicFilter"`
	Qos         int32  `json:"qos"`
}

// listSubscriptionsResponse mirrors ListSubscriptionsOutput's JSON shape.
// Subscriptions reflects the client's live MQTT subscriptions read straight
// off the mochi-mqtt broker's per-client session state (see
// InMemoryBackend.ListSubscriptions) when a broker is wired and knows about
// the client; otherwise it is an honestly empty list rather than a
// fabricated one -- see PARITY.md gaps for when that occurs.
type listSubscriptionsResponse struct {
	NextToken     string                        `json:"nextToken,omitempty"`
	Subscriptions []subscriptionSummaryResponse `json:"subscriptions"`
}

// handleListSubscriptions processes GET /connections/{clientId}/subscriptions requests.
func (h *Handler) handleListSubscriptions(c *echo.Context) error {
	clientID := extractConnectionClientID(c.Request().URL.Path)
	if clientID == "" {
		return h.handleError(c, fmt.Errorf("%w: clientId is required", ErrValidation))
	}

	subs, err := h.Backend.ListSubscriptions(clientID)
	if err != nil {
		return h.handleError(c, err)
	}

	out := make([]subscriptionSummaryResponse, 0, len(subs))
	for _, s := range subs {
		out = append(out, subscriptionSummaryResponse{TopicFilter: s.TopicFilter, Qos: int32(s.QoS)})
	}

	return c.JSON(http.StatusOK, listSubscriptionsResponse{Subscriptions: out})
}

// sendDirectMessageResponse mirrors SendDirectMessageOutput's JSON shape
// (message, traceId).
type sendDirectMessageResponse struct {
	Message string `json:"message"`
	TraceID string `json:"traceId"`
}

// validateSendDirectMessageParams validates the topic and optional MQTT5
// query/header fields shared with Publish (see parseMQTT5PublishParams).
// confirmation/timeout aren't validated here -- see handleSendDirectMessage.
func validateSendDirectMessageParams(c *echo.Context, topic string) error {
	if err := validateTopic(topic); err != nil {
		return err
	}

	if err := validateResponseTopic(c.Request().URL.Query().Get("responseTopic")); err != nil {
		return err
	}

	if err := validatePayloadFormatIndicator(c.Request().Header.Get(headerMQTTPayloadFormatIndicator)); err != nil {
		return err
	}

	if _, err := decodeUserProperties(c.Request().Header.Get(headerMQTTUserProperties)); err != nil {
		return err
	}

	return nil
}

// handleSendDirectMessage processes POST /connections/{clientId}/messages
// requests. It validates the target clientId and topic, then delivers via
// InMemoryBackend.SendDirectMessage -- which now addresses the target
// client's live broker connection directly when the broker has one, falling
// back to a topic broadcast only when it doesn't (see SendDirectMessage's doc
// comment and PARITY.md gaps for exactly when that fallback applies). Of the
// optional MQTT5 fields real SendDirectMessageInput carries (contentType/
// correlationData/payloadFormatIndicator/responseTopic/userProperties), only
// the always-present topic/payload actually reach the broker -- the rest are
// parsed and validated (so malformed values are still rejected) but not
// forwarded, mirroring Publish's identical, already-documented gap.
// confirmation/timeout (real AWS: wait for a QoS-1 PUBACK, 504 on timeout)
// select QoS 0-vs-1 on the outgoing publish but never block or time out,
// since neither MQTTPublisher.Publish nor SendToClient wait for an ack.
func (h *Handler) handleSendDirectMessage(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	clientID := extractConnectionClientID(c.Request().URL.Path)
	if clientID == "" {
		return h.handleError(c, fmt.Errorf("%w: clientId is required", ErrValidation))
	}

	q := c.Request().URL.Query()

	topic := q.Get("topic")
	if topic == "" {
		return h.handleError(c, fmt.Errorf("%w: topic is required", ErrValidation))
	}

	if err := validateSendDirectMessageParams(c, topic); err != nil {
		return h.handleError(c, err)
	}

	confirmation := parseRetainFlag(q.Get("confirmation"))

	var qos int32
	if confirmation {
		qos = 1
	}

	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxPublishBodyBytes)

	payload, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return h.handleError(c,
			fmt.Errorf("%w: message payload exceeds %d bytes", ErrRequestTooLarge, maxPublishBodyBytes))
	}

	if sendErr := h.Backend.SendDirectMessage(clientID, topic, payload, qos); sendErr != nil {
		switch {
		case errors.Is(sendErr, ErrNoBroker):
			log.Warn("iot data plane: no broker configured, direct message dropped",
				"clientId", clientID, "topic", topic)
		case errors.Is(sendErr, ErrConnectionNotFound), errors.Is(sendErr, ErrValidation):
			return h.handleError(c, sendErr)
		default:
			log.Error("iot data plane send direct message failed",
				"clientId", clientID, "topic", topic, "error", sendErr)

			return c.JSON(http.StatusInternalServerError, map[string]string{keyError: sendErr.Error()})
		}
	}

	return c.JSON(http.StatusOK, sendDirectMessageResponse{
		Message: "OK",
		TraceID: uuid.NewString(),
	})
}
