package apigatewaymanagementapi

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyConnectionID = "connectionId"
)

const (
	keyMessageField  = "message"
	keyTypeField     = "__type"
	errGoneException = "GoneException"
	// amznErrorTypeHeader carries the modeled error type in the AWS rest-json
	// protocol; the SDK reads the exception type from this header.
	amznErrorTypeHeader = "X-Amzn-Errortype"
)

const (
	apigwMgmtMatchPriority = 87
	connectionsPathPrefix  = "/@connections/"
	adminPathPrefix        = "/_gopherstack/apigwmgmt/"

	adminConnections        = "connections"
	adminConnectionsPrefix  = "connections/"
	adminBroadcastEndpoint  = "broadcast"
	adminStatsEndpoint      = "stats"
	adminPruneEndpoint      = "prune"
	adminMessagesSuffix     = "messages"
	adminTimelineSuffix     = "timeline"
	adminPingSuffix         = "ping"
	adminUnknownEndpointMsg = "unknown admin endpoint"
	adminInvalidJSONMsg     = "invalid JSON body"
	adminConnNotFoundMsg    = "connection not found"

	opUnknown = "Unknown"
)

// Handler is the Echo HTTP handler for API Gateway Management API operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new API Gateway Management API Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// writeGoneException emits a GoneException (HTTP 410) in the AWS rest-json
// shape: the modeled type travels in both the X-Amzn-Errortype header and the
// body's __type field, with a human-readable message (not the type) in
// "message". The SDK resolves the exception from these, not from the message.
func writeGoneException(c *echo.Context, connectionID string) error {
	c.Response().Header().Set(amznErrorTypeHeader, errGoneException)

	return c.JSON(http.StatusGone, map[string]string{
		keyTypeField:    errGoneException,
		keyMessageField: "the connection is no longer available",
		keyConnectionID: connectionID,
	})
}

// Name returns the service name.
func (h *Handler) Name() string { return "APIGatewayManagementAPI" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{"PostToConnection", "GetConnection", "DeleteConnection"}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "apigatewaymanagementapi" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching API Gateway Management API requests.
// In addition to the AWS-shaped /@connections/* prefix it also claims the
// /_gopherstack/apigwmgmt/* prefix, which exposes diagnostic endpoints used by
// the gopherstack UI (list, broadcast, timeline, stats).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, connectionsPathPrefix) ||
			strings.HasPrefix(path, adminPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return apigwMgmtMatchPriority }

// ExtractOperation returns the operation name based on path / HTTP method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path

	if sub, ok := strings.CutPrefix(path, adminPathPrefix); ok {
		return "Admin:" + adminAction(c.Request().Method, sub)
	}

	switch c.Request().Method {
	case http.MethodPost:
		return "PostToConnection"
	case http.MethodGet:
		return "GetConnection"
	case http.MethodDelete:
		return "DeleteConnection"
	default:
		return opUnknown
	}
}

// ExtractResource extracts the connection ID from the URL path. For admin paths
// it returns the trailing segment after the admin prefix.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	if sub, ok := strings.CutPrefix(path, adminPathPrefix); ok {
		return sub
	}

	return strings.TrimPrefix(path, connectionsPathPrefix)
}

// Handler returns the Echo handler function for API Gateway Management API operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		path := c.Request().URL.Path

		if sub, ok := strings.CutPrefix(path, adminPathPrefix); ok {
			return h.handleAdmin(c, sub)
		}

		log := logger.Load(c.Request().Context())

		if !strings.HasPrefix(path, connectionsPathPrefix) {
			return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: "not found"})
		}

		connectionID := strings.TrimPrefix(path, connectionsPathPrefix)
		if connectionID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: "connectionId is required"})
		}

		switch c.Request().Method {
		case http.MethodPost:
			return h.handlePostToConnection(c, connectionID)
		case http.MethodGet:
			return h.handleGetConnection(c, connectionID)
		case http.MethodDelete:
			return h.handleDeleteConnection(c, connectionID)
		default:
			log.Warn("api gateway management api: unsupported method", "method", c.Request().Method)

			return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyMessageField: "method not allowed"})
		}
	}
}

func (h *Handler) handlePostToConnection(c *echo.Context, connectionID string) error {
	log := logger.Load(c.Request().Context())

	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxPayloadBytes)

	body, readErr := io.ReadAll(c.Request().Body)
	if readErr != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(readErr, &maxBytesErr) {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{
				keyMessageField: "payload too large: exceeds maximum allowed size",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: "failed to read request body"})
	}

	if err := h.Backend.PostToConnection(connectionID, body); err != nil {
		log.Error("api gateway management api: post to connection failed", keyConnectionID, connectionID, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return writeGoneException(c, connectionID)
		}

		if errors.Is(err, ErrPayloadTooLarge) {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{keyMessageField: err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetConnection(c *echo.Context, connectionID string) error {
	log := logger.Load(c.Request().Context())

	conn, err := h.Backend.GetConnection(connectionID)
	if err != nil {
		log.Error("api gateway management api: get connection failed", keyConnectionID, connectionID, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return writeGoneException(c, connectionID)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}

	return c.JSON(http.StatusOK, conn)
}

func (h *Handler) handleDeleteConnection(c *echo.Context, connectionID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteConnection(connectionID); err != nil {
		log.Error("api gateway management api: delete connection failed", keyConnectionID, connectionID, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return writeGoneException(c, connectionID)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func adminAction(method, sub string) string {
	if action := adminTopLevelAction(method, sub); action != "" {
		return action
	}

	if rest, ok := strings.CutPrefix(sub, adminConnectionsPrefix); ok {
		return adminConnSubresourceAction(method, rest)
	}

	return opUnknown
}

func adminTopLevelAction(method, sub string) string {
	switch sub {
	case adminConnections:
		if method == http.MethodGet {
			return "ListConnections"
		}

		if method == http.MethodPost {
			return "SimulateConnection"
		}
	case adminBroadcastEndpoint:
		if method == http.MethodPost {
			return "Broadcast"
		}
	case adminStatsEndpoint:
		if method == http.MethodGet {
			return "Stats"
		}
	case adminPruneEndpoint:
		if method == http.MethodPost {
			return "PruneIdle"
		}
	}

	return ""
}

func adminConnSubresourceAction(method, rest string) string {
	switch {
	case strings.HasSuffix(rest, "/"+adminMessagesSuffix) && method == http.MethodGet:
		return "GetMessages"
	case strings.HasSuffix(rest, "/"+adminMessagesSuffix) && method == http.MethodDelete:
		return "ClearMessages"
	case strings.HasSuffix(rest, "/"+adminTimelineSuffix) && method == http.MethodGet:
		return "GetTimeline"
	case strings.HasSuffix(rest, "/"+adminPingSuffix) && method == http.MethodPost:
		return "PingConnection"
	}

	return opUnknown
}
