package iotdataplane

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
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
