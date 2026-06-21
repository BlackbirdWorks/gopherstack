package apigatewayv2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for the local emulator
	},
}

// handleStageProxyEcho routes /v2proxy/{apiId}/{stageName}/{path} requests to the proxy handler.
func (h *Handler) handleStageProxyEcho(c *echo.Context) error {
	rest := strings.TrimPrefix(c.Request().URL.Path, "/v2proxy/")
	const minProxyPathParts = 2
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < minProxyPathParts {
		return c.String(http.StatusNotFound, "invalid proxy path")
	}

	apiID := parts[0]
	stageName := parts[1]

	resourcePath := "/"
	if len(parts) == 3 && parts[2] != "" {
		resourcePath = "/" + parts[2]
	}

	return h.handleProxy(c, apiID, stageName, resourcePath)
}

// handleUserRequestEcho handles data-plane invocations at the standard AWS endpoint format:
// /restapis/{apiId}/{stageName}/_user_request_/{resourcePath...}.
func (h *Handler) handleUserRequestEcho(c *echo.Context) error {
	segs := strings.Split(strings.TrimPrefix(c.Request().URL.Path, "/"), "/")
	const (
		idxAPIID     = 1
		idxStageName = 2
		idxPathStart = 4
	)

	apiID := segs[idxAPIID]
	stageName := segs[idxStageName]

	resourcePath := "/"
	if len(segs) > idxPathStart && segs[idxPathStart] != "" {
		resourcePath = "/" + strings.Join(segs[idxPathStart:], "/")
	}

	return h.handleProxy(c, apiID, stageName, resourcePath)
}

// handleProxy performs the actual WebSocket or HTTP API routing.
func (h *Handler) handleProxy(c *echo.Context, apiID, stageName, resourcePath string) error {
	// 1. Get the API
	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		return c.String(http.StatusNotFound, "API not found")
	}

	// 2. Determine protocol
	protocol := api.ProtocolType
	if protocol == "WEBSOCKET" {
		return h.handleWebSocketProxy(c, apiID, stageName, resourcePath)
	} else if protocol == "HTTP" {
		return h.handleHTTPProxy(c, apiID, stageName, resourcePath)
	}

	return c.String(http.StatusNotImplemented, "unsupported protocol type: "+string(protocol))
}

func (h *Handler) handleHTTPProxy(c *echo.Context, apiID, stageName, resourcePath string) error {
	return c.String(http.StatusNotImplemented, "HTTP API proxy not fully implemented yet")
}

func (h *Handler) handleWebSocketProxy(c *echo.Context, apiID, stageName, resourcePath string) error {
	log := logger.Load(c.Request().Context())

	// A WebSocket proxy requires a lambda invoker to handle $connect, $disconnect, etc.
	if h.lambdaInvoker == nil {
		return c.String(http.StatusInternalServerError, "Lambda invoker not configured")
	}

	// For WebSockets, the initial request must be an HTTP Upgrade.
	if !websocket.IsWebSocketUpgrade(c.Request()) {
		return c.String(http.StatusBadRequest, "request is not a WebSocket upgrade")
	}

	connectionID := uuid.New().String()

	// Route the $connect event
	err := h.invokeWSRoute(c, apiID, "$connect", connectionID, []byte{})
	if err != nil {
		log.Error("apigatewayv2: $connect route failed", "error", err)
		return c.String(http.StatusForbidden, "Forbidden")
	}

	// Upgrade the connection
	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Error("apigatewayv2: websocket upgrade failed", "error", err)
		return err
	}

	downstream := make(chan []byte, 10)
	if h.managementAPI != nil {
		_, err = h.managementAPI.CreateConnection(connectionID, c.RealIP(), c.Request().UserAgent(), downstream)
		if err != nil {
			log.Error("apigatewayv2: failed to register connection", "error", err)
			conn.Close()
			return err
		}
	}

	go func() {
		for msg := range downstream {
			err := conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Info("apigatewayv2: websocket write error", "error", err)
				break
			}
		}
		conn.Close()
	}()

	// Read loop
	for {
		msgType, msgBody, err := conn.ReadMessage()
		if err != nil {
			log.Info("apigatewayv2: websocket closed", "error", err)
			break
		}

		if msgType == websocket.TextMessage || msgType == websocket.BinaryMessage {
			// Find route based on RouteSelectionExpression.
			routeKey := "$default"

			var payload map[string]any
			if json.Unmarshal(msgBody, &payload) == nil {
				if action, ok := payload["action"].(string); ok && action != "" {
					routeKey = action
				}
			}

			// Invoke Lambda
			_ = h.invokeWSRoute(c, apiID, routeKey, connectionID, msgBody)
		}
	}

	if h.managementAPI != nil {
		_ = h.managementAPI.DeleteConnection(connectionID)
	}

	// Route the $disconnect event
	_ = h.invokeWSRoute(c, apiID, "$disconnect", connectionID, []byte{})

	return nil
}

// invokeWSRoute invokes the backend integration for a specific route.
func (h *Handler) invokeWSRoute(c *echo.Context, apiID, routeKey, connectionID string, body []byte) error {
	// 1. Find the Route
	routes, err := h.Backend.GetRoutes(apiID)
	if err != nil {
		return err
	}

	var targetRoute *Route
	var defaultRoute *Route

	for i, r := range routes {
		if r.RouteKey == routeKey {
			targetRoute = &routes[i]
		}
		if r.RouteKey == "$default" {
			defaultRoute = &routes[i]
		}
	}

	if targetRoute == nil {
		targetRoute = defaultRoute
	}

	if targetRoute == nil {
		return fmt.Errorf("route %s not found and no $default route", routeKey)
	}

	if targetRoute.Target == "" {
		// No integration target
		return nil
	}

	// 2. Find the Integration
	// Target format is "integrations/{integrationId}"
	integrationID := strings.TrimPrefix(targetRoute.Target, "integrations/")
	integration, err := h.Backend.GetIntegration(apiID, integrationID)
	if err != nil {
		return err
	}

	// 3. Extract Lambda ARN
	uri := integration.IntegrationURI
	if uri == "" || !strings.Contains(uri, "arn:aws:lambda") {
		return fmt.Errorf("integration URI is not a valid Lambda ARN: %s", uri)
	}

	// Parse out function name
	// arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-func/invocations
	parts := strings.Split(uri, "function:")
	if len(parts) < 2 {
		return fmt.Errorf("invalid lambda integration URI")
	}
	funcNamePart := strings.Split(parts[1], "/")
	funcName := funcNamePart[0]

	// 4. Build API Gateway Proxy Event
	// TODO: Pass proper WebSocket request context
	payload := []byte(fmt.Sprintf(`{
		"requestContext": {
			"routeKey": "%s",
			"connectionId": "%s",
			"eventType": "MESSAGE"
		},
		"body": %q
	}`, routeKey, connectionID, string(body)))

	// 5. Invoke Lambda
	_, _, err = h.lambdaInvoker.InvokeFunction(c.Request().Context(), funcName, "RequestResponse", payload)
	if err != nil {
		return fmt.Errorf("lambda invocation failed: %w", err)
	}

	return nil
}
