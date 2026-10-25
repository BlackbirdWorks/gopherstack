package apigatewayv2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	routeKeyConnect    = "$connect"
	routeKeyDisconnect = "$disconnect"

	wsEventConnect    = "CONNECT"
	wsEventMessage    = "MESSAGE"
	wsEventDisconnect = "DISCONNECT"

	wsRequestTimeLayout  = "02/Jan/2006:15:04:05 -0700"
	wsDownstreamChanSize = 10
	wsNoStatusReceived   = "Client-side close frame status not set"
	wsGoingAway          = "Server closed the connection"
	wsForbiddenMessage   = "Forbidden"
)

// wsBodyVarRE matches $request.body.<path> and ${request.body.<path>} in a
// WebSocket selection expression.
var wsBodyVarRE = regexp.MustCompile(`\$\{request\.body\.([^}]+)\}|\$request\.body\.([A-Za-z0-9_.\-]+)`)

type wsSession struct {
	connectedAt time.Time
	h           *Handler
	authorizer  map[string]any
	downstream  chan []byte
	apiID       string
	stage       string
	connID      string
	domain      string
	sourceIP    string
	userAgent   string
	header      http.Header
	query       map[string][]string
}

func (s *wsSession) requestContext(routeKey, eventType string) map[string]any {
	now := time.Now().UTC()
	reqID := uuid.NewString()

	rc := map[string]any{
		"routeKey":          routeKey,
		"eventType":         eventType,
		"messageDirection":  "IN",
		"extendedRequestId": reqID,
		"requestId":         reqID,
		"requestTime":       now.Format(wsRequestTimeLayout),
		"requestTimeEpoch":  now.UnixMilli(),
		"connectedAt":       s.connectedAt.UnixMilli(),
		"stage":             s.stage,
		"domainName":        s.domain,
		"connectionId":      s.connID,
		"apiId":             s.apiID,
		"identity":          map[string]any{"sourceIp": s.sourceIP, "userAgent": s.userAgent},
	}

	if s.authorizer != nil {
		rc["authorizer"] = s.authorizer
	}

	return rc
}

func singleValues(m map[string][]string) map[string]string {
	out := make(map[string]string, len(m))

	for k, vs := range m {
		if len(vs) > 0 {
			out[k] = vs[len(vs)-1]
		}
	}

	return out
}

func (s *wsSession) connectEvent() map[string]any {
	ev := map[string]any{
		"headers":           singleValues(s.header),
		"multiValueHeaders": s.header,
		"requestContext":    s.requestContext(routeKeyConnect, wsEventConnect),
		"isBase64Encoded":   false,
	}

	if len(s.query) > 0 {
		ev["queryStringParameters"] = singleValues(s.query)
		ev["multiValueQueryStringParameters"] = s.query
	}

	return ev
}

func (s *wsSession) messageEvent(routeKey string, body []byte, binary bool) map[string]any {
	rc := s.requestContext(routeKey, wsEventMessage)
	rc["messageId"] = uuid.NewString()

	ev := map[string]any{"requestContext": rc, "isBase64Encoded": binary}
	if binary {
		ev["body"] = base64.StdEncoding.EncodeToString(body)
	} else {
		ev["body"] = string(body)
	}

	return ev
}

func (s *wsSession) disconnectEvent(code int, reason string) map[string]any {
	rc := s.requestContext(routeKeyDisconnect, wsEventDisconnect)
	rc["disconnectStatusCode"] = code
	rc["disconnectReason"] = reason

	return map[string]any{
		"headers":           map[string]string{"Host": s.domain},
		"multiValueHeaders": map[string][]string{"Host": {s.domain}},
		"requestContext":    rc,
		"isBase64Encoded":   false,
	}
}

// send delivers data to the client through the Management API connection
// (which owns the downstream channel) or, with none configured, directly.
func (s *wsSession) send(data []byte) {
	if s.h.managementAPI != nil {
		_ = s.h.managementAPI.PostToConnection(s.connID, data)

		return
	}

	select {
	case s.downstream <- data:
	default:
	}
}

func (s *wsSession) sendForbidden() {
	b, _ := json.Marshal(map[string]string{
		"message":      wsForbiddenMessage,
		"connectionId": s.connID,
		"requestId":    uuid.NewString(),
	})
	s.send(b)
}

// wsRoute resolves routeKey against the stage's deployed (or live) routes.
func (h *Handler) wsRoute(apiID, stage, routeKey string) (*Route, *Deployment, error) {
	routes, deployment, err := h.resolveHTTPAPIRoutes(apiID, h.lookupStage(apiID, stage))
	if err != nil {
		return nil, nil, err
	}

	if route := findWSRoute(routes, routeKey); route != nil {
		return route, deployment, nil
	}

	return nil, deployment, fmt.Errorf("%w: %s", ErrRouteNotFound, routeKey)
}

func findWSRoute(routes []Route, key string) *Route {
	for i := range routes {
		if routes[i].RouteKey == key {
			return &routes[i]
		}
	}

	return nil
}

// invoke runs the route's integration with event and returns the Lambda
// payload. Routes without a target, and MOCK integrations, invoke nothing.
func (s *wsSession) invoke(
	ctx context.Context,
	deployment *Deployment,
	route *Route,
	event map[string]any,
) ([]byte, error) {
	if route == nil || route.Target == "" {
		return nil, nil
	}

	integrationID := strings.TrimPrefix(route.Target, "integrations/")

	integration, err := s.h.resolveHTTPAPIIntegration(s.apiID, integrationID, deployment)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, integrationID)
	}

	if integration.IntegrationType == integrationTypeMock {
		return nil, nil
	}

	if integration.IntegrationType != IntegrationTypeAWSProxy {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, integration.IntegrationType)
	}

	if !strings.Contains(integration.IntegrationURI, "function:") {
		return nil, fmt.Errorf("%w: %s", ErrInvalidLambdaArn, integration.IntegrationURI)
	}

	payload, marshalErr := json.Marshal(event)
	if marshalErr != nil {
		return nil, fmt.Errorf("failed to marshal WebSocket event: %w", marshalErr)
	}

	out, _, invokeErr := s.h.lambdaInvoker.InvokeFunction(
		ctx, extractHTTPAPILambdaARN(integration.IntegrationURI), "RequestResponse", payload)
	if invokeErr != nil {
		return nil, fmt.Errorf("lambda invocation failed: %w: %w", ErrIntegrationInvoke, invokeErr)
	}

	return out, nil
}

func (h *Handler) handleWebSocketProxy(c *echo.Context, apiID, stageName string) error {
	wm := h.wsMetrics(apiID, stageName)
	log := logger.Load(c.Request().Context())

	if h.lambdaInvoker == nil {
		return c.String(http.StatusInternalServerError, "Lambda invoker not configured")
	}

	if !websocket.IsWebSocketUpgrade(c.Request()) {
		return c.String(http.StatusBadRequest, "request is not a WebSocket upgrade")
	}

	req := c.Request()
	s := &wsSession{
		h:           h,
		apiID:       apiID,
		stage:       stageName,
		connID:      uuid.NewString(),
		domain:      req.Host,
		sourceIP:    realIP(req),
		userAgent:   req.UserAgent(),
		connectedAt: time.Now().UTC(),
		header:      req.Header.Clone(),
		query:       req.URL.Query(),
		downstream:  make(chan []byte, wsDownstreamChanSize),
	}

	wm.forRoute(routeKeyConnect).connect()

	if rejected, err := h.wsAdmit(c, s); rejected {
		wm.clientError()

		return err
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}

	conn, upgradeErr := upgrader.Upgrade(c.Response(), req, nil)
	if upgradeErr != nil {
		log.Error("apigatewayv2: websocket upgrade failed", "error", upgradeErr)

		return upgradeErr
	}

	if h.managementAPI != nil {
		_, regErr := h.managementAPI.CreateConnection(s.connID, s.sourceIP, s.userAgent, s.downstream)
		if regErr != nil {
			log.Error("apigatewayv2: failed to register connection", "error", regErr)
			_ = conn.Close()

			return regErr
		}
	}

	go func() {
		for msg := range s.downstream {
			if writeErr := conn.WriteMessage(websocket.TextMessage, msg); writeErr != nil {
				log.Info("apigatewayv2: websocket write error", "error", writeErr)

				break
			}
		}

		_ = conn.Close()
	}()

	code, reason := h.wsReadLoop(c, conn, s, wm)

	if h.managementAPI != nil {
		_ = h.managementAPI.DeleteConnection(s.connID)
	} else {
		close(s.downstream)
	}

	routes, deployment, _ := h.resolveHTTPAPIRoutes(apiID, h.lookupStage(apiID, stageName))
	if route := findWSRoute(routes, routeKeyDisconnect); route != nil {
		_, _ = s.invoke(req.Context(), deployment, route, s.disconnectEvent(code, reason))
	}

	return nil
}

// wsAdmit runs the $connect route's authorizer and integration before the
// upgrade. It reports whether the connection was rejected, having written the
// rejection response.
func (h *Handler) wsAdmit(c *echo.Context, s *wsSession) (bool, error) {
	log := logger.Load(c.Request().Context())

	routes, deployment, err := h.resolveHTTPAPIRoutes(s.apiID, h.lookupStage(s.apiID, s.stage))
	if err != nil {
		return true, writeErr(c, http.StatusInternalServerError, "Internal server error")
	}

	route := findWSRoute(routes, routeKeyConnect)
	if route == nil {
		return false, nil
	}

	if authErr := h.wsAuthorize(c, s, route); authErr != nil {
		return true, writeRouteControlRejection(c, authErr)
	}

	out, invokeErr := s.invoke(c.Request().Context(), deployment, route, s.connectEvent())
	if invokeErr != nil {
		log.Error("apigatewayv2: $connect route failed", "error", invokeErr)

		return true, writeErr(c, http.StatusInternalServerError, "Internal server error")
	}

	if status := proxyResponseStatus(out); status != 0 && status != http.StatusOK {
		return true, writeErr(c, status, http.StatusText(status))
	}

	return false, nil
}

// proxyResponseStatus returns the statusCode of a Lambda proxy response, or 0
// when the payload carries none.
func proxyResponseStatus(out []byte) int {
	var resp struct {
		StatusCode *int `json:"statusCode"`
	}
	if json.Unmarshal(out, &resp) != nil || resp.StatusCode == nil {
		return 0
	}

	return *resp.StatusCode
}

func (h *Handler) wsAuthorize(c *echo.Context, s *wsSession, route *Route) error {
	switch route.AuthorizationType {
	case authorizationTypeAWSIAM:
		return h.enforceIAMAuth(c)
	case authorizationTypeCustom:
		if route.AuthorizerID == "" {
			return nil
		}

		auth, err := h.Backend.GetAuthorizer(s.apiID, route.AuthorizerID)
		if err != nil {
			return errRouteUnauthorized
		}

		return h.enforceWSRequestAuthorizer(c, s, auth)
	default:
		return nil
	}
}

// resolveWSIdentitySource resolves route.request.header.X and
// route.request.querystring.X identity sources.
func resolveWSIdentitySource(r *http.Request, src string) string {
	if name, ok := strings.CutPrefix(src, "route.request.header."); ok {
		return r.Header.Get(name)
	}

	if name, ok := strings.CutPrefix(src, "route.request.querystring."); ok {
		return r.URL.Query().Get(name)
	}

	return resolveIdentitySource(r, src)
}

func (h *Handler) enforceWSRequestAuthorizer(c *echo.Context, s *wsSession, auth *Authorizer) error {
	log := logger.Load(c.Request().Context())
	req := c.Request()

	values := make([]string, 0, len(auth.IdentitySource))

	for _, src := range auth.IdentitySource {
		v := resolveWSIdentitySource(req, src)
		if v == "" {
			log.Info("apigatewayv2: WebSocket authorizer missing identity source", "source", src)

			return errRouteUnauthorized
		}

		values = append(values, v)
	}

	methodArn := buildWSRouteArn(regionFromCtx(req.Context()), s.apiID, s.stage)
	cacheKey := auth.AuthorizerID + "\n" + strings.Join(values, "\n")
	cacheable := len(values) > 0

	if d, ok := h.authCache.getDecision(cacheKey); ok && cacheable {
		s.authorizer = d.ctx

		return finishAuthDecision(d.allow)
	}

	payload, err := json.Marshal(map[string]any{
		"type":                            authorizerTypeRequest,
		"methodArn":                       methodArn,
		"headers":                         singleValues(s.header),
		"multiValueHeaders":               s.header,
		"queryStringParameters":           singleValues(s.query),
		"multiValueQueryStringParameters": s.query,
		"stageVariables":                  map[string]string{},
		"requestContext":                  s.requestContext(routeKeyConnect, wsEventConnect),
	})
	if err != nil {
		return errRouteAuthConfigInvalid
	}

	arn := extractHTTPAPILambdaARN(auth.AuthorizerURI)

	out, _, invokeErr := h.lambdaInvoker.InvokeFunction(req.Context(), arn, "RequestResponse", payload)
	if invokeErr != nil {
		log.Error("apigatewayv2: WebSocket authorizer invocation failed", "arn", arn, "error", invokeErr)

		return errRouteAuthConfigInvalid
	}

	var policy iamPolicyResponse
	allow, explicit := false, false

	if json.Unmarshal(out, &policy) == nil {
		allow, explicit = evaluateIAMPolicy(policy.PolicyDocument.Statement, methodArn)
	}

	var authzCtx map[string]any
	if allow {
		authzCtx = map[string]any{"principalId": policy.PrincipalID}
		for k, v := range policy.Context {
			authzCtx[k] = v
		}
	}

	if cacheable {
		h.authCache.putDecision(cacheKey, authDecision{allow: allow, ctx: authzCtx},
			time.Duration(auth.AuthorizerResultTTLInSeconds)*time.Second)
	}

	switch {
	case allow:
		s.authorizer = authzCtx

		return nil
	case explicit:
		return errRouteExplicitDeny
	default:
		return errRouteForbidden
	}
}

func buildWSRouteArn(region, apiID, stage string) string {
	return "arn:aws:execute-api:" + region + ":" + config.DefaultAccountID + ":" + apiID + "/" + stage + "/" + routeKeyConnect
}

// evalRouteSelection evaluates a WebSocket route selection expression against
// a JSON message body; "" means the expression produced no key.
func evalRouteSelection(expr string, body []byte) string {
	const escaped = "\x00"

	expr = strings.ReplaceAll(expr, `\$`, escaped)

	var doc any
	_ = json.Unmarshal(body, &doc)

	failed := false
	out := wsBodyVarRE.ReplaceAllStringFunc(expr, func(m string) string {
		sub := wsBodyVarRE.FindStringSubmatch(m)
		path := sub[1]
		if path == "" {
			path = sub[2]
		}

		v, ok := jsonPathString(doc, path)
		if !ok {
			failed = true
		}

		return v
	})

	if failed || strings.Contains(out, "$") {
		return ""
	}

	return strings.ReplaceAll(out, escaped, "$")
}

func jsonPathString(doc any, path string) (string, bool) {
	cur := doc

	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}

		if cur, ok = m[key]; !ok {
			return "", false
		}
	}

	switch v := cur.(type) {
	case string:
		return v, v != ""
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(v), true
	default:
		return "", false
	}
}

func (h *Handler) wsReadLoop(c *echo.Context, conn *websocket.Conn, s *wsSession, wm wsMetrics) (int, string) {
	log := logger.Load(c.Request().Context())

	for {
		msgType, msgBody, readErr := conn.ReadMessage()
		if readErr != nil {
			log.Info("apigatewayv2: websocket closed", "error", readErr)

			return disconnectInfo(readErr)
		}

		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}

		if h.managementAPI != nil {
			_ = h.managementAPI.TouchConnection(s.connID)
		}

		h.wsDispatch(c, s, wm, msgBody, msgType == websocket.BinaryMessage)
	}
}

func disconnectInfo(readErr error) (int, string) {
	if ce, ok := errors.AsType[*websocket.CloseError](readErr); ok {
		if ce.Code == websocket.CloseNoStatusReceived {
			return ce.Code, wsNoStatusReceived
		}

		return ce.Code, ce.Text
	}

	return websocket.CloseGoingAway, wsGoingAway
}

func (h *Handler) wsDispatch(c *echo.Context, s *wsSession, wm wsMetrics, body []byte, binary bool) {
	ctx := c.Request().Context()

	api, err := h.Backend.GetAPI(s.apiID)
	if err != nil {
		return
	}

	key := evalRouteSelection(api.RouteSelectionExpression, body)

	route, deployment, err := h.wsRoute(s.apiID, s.stage, key)
	if key == "" || err != nil {
		route, deployment, err = h.wsRoute(s.apiID, s.stage, routeKeyDefault)
	}

	if err != nil {
		s.sendForbidden()

		return
	}

	rm := wm.forRoute(route.RouteKey)
	rm.message()

	start := time.Now()
	out, invokeErr := s.invoke(ctx, deployment, route, s.messageEvent(route.RouteKey, body, binary))
	rm.routed(invokeErr, start)

	if invokeErr == nil && len(out) > 0 && h.hasDefaultRouteResponse(s.apiID, route) {
		s.send(out)
	}
}

// hasDefaultRouteResponse reports whether the route enables two-way
// communication: a RouteResponseSelectionExpression plus a $default route response.
func (h *Handler) hasDefaultRouteResponse(apiID string, route *Route) bool {
	if route.RouteResponseSelectionExpression == "" {
		return false
	}

	rrs, err := h.Backend.GetRouteResponses(apiID, route.RouteID)
	if err != nil {
		return false
	}

	for i := range rrs {
		if rrs[i].RouteResponseKey == routeKeyDefault {
			return true
		}
	}

	return false
}
