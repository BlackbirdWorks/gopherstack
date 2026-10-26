package apigatewayv2

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

var (
	ErrUnsupportedType  = errors.New("unsupported integration type")
	ErrInvalidLambdaArn = errors.New("invalid lambda arn")
)

const (
	protocolTypeWebSocket = "WEBSOCKET"
	routeKeyDefault       = "$default"
)

// handleStageProxyEcho routes /v2proxy/{apiId}/{stageName}/{path} requests to the proxy handler.
func (h *Handler) handleStageProxyEcho(c *echo.Context) error {
	rest := strings.TrimPrefix(c.Request().URL.Path, "/v2proxy/")
	const minProxyPathParts = 2
	const splitCount = 3

	parts := strings.SplitN(rest, "/", splitCount)
	if len(parts) < minProxyPathParts {
		return c.String(http.StatusNotFound, "invalid proxy path")
	}

	apiID := parts[0]
	stageName := parts[1]

	resourcePath := "/"
	if len(parts) == splitCount && parts[2] != "" {
		resourcePath = "/" + parts[2]
	}

	return h.handleProxy(c, apiID, stageName, resourcePath)
}

// hasAPI reports whether h or a peer region owns the API.
func (h *Handler) hasAPI(apiID string) bool {
	_, err := h.invokeOwner(apiID).Backend.GetAPI(apiID)

	return err == nil
}

// handleExecuteAPIEcho serves {apiId}.execute-api.<host>/[{stage}/]{path} and
// /_aws/execute-api/{apiId}/[{stage}/]{path}; the stage is optional when it names no stage.
func (h *Handler) handleExecuteAPIEcho(c *echo.Context, apiID, rest string) error {
	stageName, resourcePath := routeKeyDefault, rest
	if resourcePath == "" {
		resourcePath = "/"
	}

	first, tail, _ := strings.Cut(strings.TrimPrefix(rest, "/"), "/")
	if first != "" {
		if _, err := h.invokeOwner(apiID).Backend.GetStage(apiID, first); err == nil {
			stageName, resourcePath = first, "/"+tail
		}
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
	if owner := h.invokeOwner(apiID); owner != h {
		return owner.handleProxy(c, apiID, stageName, resourcePath)
	}

	c.SetRequest(h.invokeRequest(c.Request()))

	// 1. Get the API
	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		return c.String(http.StatusNotFound, "API not found")
	}

	// A request naming a stage that was never created has no deployed
	// snapshot to route against. Previously this handler never checked
	// stageName against the backend at all here, so any string in the URL's
	// stage-name slot -- including one that was never created via
	// CreateStage -- still routed through to the live integration.
	if _, stageErr := h.Backend.GetStage(apiID, stageName); stageErr != nil {
		return writeErr(c, http.StatusNotFound, msgNotFound)
	}

	// 2. Determine protocol
	protocol := api.ProtocolType
	switch protocol {
	case protocolTypeWebSocket:
		return h.handleWebSocketProxy(c, apiID, stageName)
	case protocolTypeHTTP:
		start := time.Now()
		err = h.handleHTTPAPIProxy(c, apiID, stageName, resourcePath)
		h.emitHTTPMetrics(c, apiID, stageName, start, err)

		return err
	}

	return c.String(http.StatusNotImplemented, "unsupported protocol type: "+protocol)
}
