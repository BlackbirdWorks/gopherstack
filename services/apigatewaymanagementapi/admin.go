package apigatewaymanagementapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type simulateRequest struct {
	ConnectionID string `json:"connectionId"`
	SourceIP     string `json:"sourceIp"`
	UserAgent    string `json:"userAgent"`
}

type broadcastRequest struct {
	Data string `json:"data"`
}

type pruneRequest struct {
	IdleSeconds int `json:"idleSeconds"`
}

type listResponse struct {
	Connections []Connection `json:"connections"`
}

type messagesResponse struct {
	Messages []messageDTO `json:"messages"`
}

// messageDTO mirrors PostedMessage but presents the payload as a UTF-8 string so
// the UI can render it without an extra base64 decode.
type messageDTO struct {
	ReceivedAt   time.Time `json:"receivedAt"`
	ConnectionID string    `json:"connectionId"`
	Data         string    `json:"data"`
	Bytes        int       `json:"bytes"`
}

type timelineResponse struct {
	Events []LifecycleEvent `json:"events"`
}

type broadcastResponse struct {
	Delivered int `json:"delivered"`
}

type pruneResponse struct {
	Pruned []string `json:"pruned"`
}

func (h *Handler) handleAdmin(c *echo.Context, sub string) error {
	method := c.Request().Method

	switch {
	case sub == adminConnections && method == http.MethodGet:
		return h.adminListConnections(c)
	case sub == adminConnections && method == http.MethodPost:
		return h.adminSimulateConnection(c)
	case sub == adminBroadcastEndpoint && method == http.MethodPost:
		return h.adminBroadcast(c)
	case sub == adminStatsEndpoint && method == http.MethodGet:
		return c.JSON(http.StatusOK, h.Backend.Stats())
	case sub == adminPruneEndpoint && method == http.MethodPost:
		return h.adminPrune(c)
	}

	if rest, ok := strings.CutPrefix(sub, adminConnectionsPrefix); ok {
		return h.adminConnectionSubresource(c, rest)
	}

	return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: adminUnknownEndpointMsg})
}

func (h *Handler) adminListConnections(c *echo.Context) error {
	q := c.QueryParam("q")
	conns := h.Backend.FilterConnections(q)

	return c.JSON(http.StatusOK, listResponse{Connections: conns})
}

func (h *Handler) adminSimulateConnection(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	var req simulateRequest
	if err := decodeJSON(c.Request().Body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: adminInvalidJSONMsg})
	}

	if req.SourceIP == "" {
		req.SourceIP = "127.0.0.1"
	}

	if req.UserAgent == "" {
		req.UserAgent = "Gopherstack UI"
	}

	conn, err := h.Backend.CreateConnection(req.ConnectionID, req.SourceIP, req.UserAgent, nil)
	if err != nil {
		log.Warn("apigwmgmt admin: simulate failed", "error", err)

		if errors.Is(err, ErrConnectionExists) {
			return c.JSON(http.StatusConflict, map[string]string{keyMessageField: err.Error()})
		}

		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	}

	return c.JSON(http.StatusCreated, conn)
}

func (h *Handler) adminBroadcast(c *echo.Context) error {
	var req broadcastRequest
	if err := decodeJSON(c.Request().Body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: adminInvalidJSONMsg})
	}

	delivered, err := h.Backend.Broadcast([]byte(req.Data))
	if err != nil {
		if errors.Is(err, ErrPayloadTooLarge) {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{keyMessageField: err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}

	return c.JSON(http.StatusOK, broadcastResponse{Delivered: delivered})
}

func (h *Handler) adminPrune(c *echo.Context) error {
	var req pruneRequest
	if err := decodeJSON(c.Request().Body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: adminInvalidJSONMsg})
	}

	pruned := h.Backend.PruneIdle(time.Duration(req.IdleSeconds) * time.Second)

	return c.JSON(http.StatusOK, pruneResponse{Pruned: pruned})
}

func (h *Handler) adminConnectionSubresource(c *echo.Context, rest string) error {
	method := c.Request().Method

	idx := strings.LastIndex(rest, "/")
	if idx <= 0 {
		return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: adminUnknownEndpointMsg})
	}

	connectionID := rest[:idx]
	suffix := rest[idx+1:]

	switch {
	case suffix == adminMessagesSuffix && method == http.MethodGet:
		return h.adminGetMessages(c, connectionID)
	case suffix == adminMessagesSuffix && method == http.MethodDelete:
		return h.adminClearMessages(c, connectionID)
	case suffix == adminTimelineSuffix && method == http.MethodGet:
		return h.adminGetTimeline(c, connectionID)
	case suffix == adminPingSuffix && method == http.MethodPost:
		return h.adminPingConnection(c, connectionID)
	}

	return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: adminUnknownEndpointMsg})
}

func (h *Handler) adminGetMessages(c *echo.Context, connectionID string) error {
	if !h.connectionExists(connectionID) {
		return c.JSON(
			http.StatusNotFound,
			map[string]string{keyMessageField: adminConnNotFoundMsg, keyConnectionID: connectionID},
		)
	}

	msgs := h.Backend.GetMessages(connectionID)
	out := make([]messageDTO, len(msgs))

	for i, m := range msgs {
		out[i] = messageDTO{
			ReceivedAt:   m.ReceivedAt,
			ConnectionID: m.ConnectionID,
			Data:         string(m.Data),
			Bytes:        len(m.Data),
		}
	}

	return c.JSON(http.StatusOK, messagesResponse{Messages: out})
}

func (h *Handler) adminClearMessages(c *echo.Context, connectionID string) error {
	if err := h.Backend.ClearMessages(connectionID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]string{keyMessageField: adminConnNotFoundMsg, keyConnectionID: connectionID},
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) adminGetTimeline(c *echo.Context, connectionID string) error {
	if !h.connectionExists(connectionID) {
		return c.JSON(
			http.StatusNotFound,
			map[string]string{keyMessageField: adminConnNotFoundMsg, keyConnectionID: connectionID},
		)
	}

	events := h.Backend.GetTimeline(connectionID)

	return c.JSON(http.StatusOK, timelineResponse{Events: events})
}

func (h *Handler) adminPingConnection(c *echo.Context, connectionID string) error {
	if err := h.Backend.PingConnection(connectionID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]string{keyMessageField: adminConnNotFoundMsg, keyConnectionID: connectionID},
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) connectionExists(connectionID string) bool {
	_, err := h.Backend.GetConnection(connectionID)

	return err == nil
}

func decodeJSON(r io.Reader, dst any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	return nil
}
