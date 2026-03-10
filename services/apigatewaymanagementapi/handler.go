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
	apigwMgmtMatchPriority = 87
	connectionsPathPrefix  = "/@connections/"
)

// Handler is the Echo HTTP handler for API Gateway Management API operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new API Gateway Management API Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
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
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, connectionsPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return apigwMgmtMatchPriority }

// ExtractOperation returns the operation name based on HTTP method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	switch c.Request().Method {
	case http.MethodPost:
		return "PostToConnection"
	case http.MethodGet:
		return "GetConnection"
	case http.MethodDelete:
		return "DeleteConnection"
	default:
		return "Unknown"
	}
}

// ExtractResource extracts the connection ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().URL.Path, connectionsPathPrefix)
}

// Handler returns the Echo handler function for API Gateway Management API operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())

		path := c.Request().URL.Path
		if !strings.HasPrefix(path, connectionsPathPrefix) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": "not found"})
		}

		connectionID := strings.TrimPrefix(path, connectionsPathPrefix)
		if connectionID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "connectionId is required"})
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

			return c.JSON(http.StatusMethodNotAllowed, map[string]string{"message": "method not allowed"})
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
				"message": "payload too large: exceeds maximum allowed size",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read request body"})
	}

	if err := h.Backend.PostToConnection(connectionID, body); err != nil {
		log.Error("api gateway management api: post to connection failed", "connectionId", connectionID, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return c.JSON(http.StatusGone, map[string]string{"message": "GoneException", "connectionId": connectionID})
		}

		if errors.Is(err, ErrPayloadTooLarge) {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetConnection(c *echo.Context, connectionID string) error {
	log := logger.Load(c.Request().Context())

	conn, err := h.Backend.GetConnection(connectionID)
	if err != nil {
		log.Error("api gateway management api: get connection failed", "connectionId", connectionID, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return c.JSON(http.StatusGone, map[string]string{"message": "GoneException", "connectionId": connectionID})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, conn)
}

func (h *Handler) handleDeleteConnection(c *echo.Context, connectionID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteConnection(connectionID); err != nil {
		log.Error("api gateway management api: delete connection failed", "connectionId", connectionID, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return c.JSON(http.StatusGone, map[string]string{"message": "GoneException", "connectionId": connectionID})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}
