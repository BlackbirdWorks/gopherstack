package apigatewayv2

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	payloadFormatV2          = "2.0"
	payloadFormatV1          = "1.0"
	integrationTypeHTTPProxy = "HTTP_PROXY"
	integrationTypeHTTPType  = "HTTP"

	maxHTTPAPIBodyBytes = 10 * 1024 * 1024 // 10 MiB

	httpClientTimeout = 30 * time.Second
	maxXFFParts       = 2
)

var (
	errNoTokenFound            = errors.New("no token found in identity source")
	errInvalidJWTClaimsType    = errors.New("invalid JWT claims type")
	errJWTExpired              = errors.New("JWT expired")
	errJWTMissingIss           = errors.New("JWT missing iss claim")
	errJWTIssuerMismatch       = errors.New("JWT issuer mismatch")
	errJWTAudienceMismatch     = errors.New("JWT audience mismatch")
	errUnexpectedSigningMethod = errors.New("unexpected JWT signing method")
	errNoJWKSProvider          = errors.New("no JWKS provider configured")
)

// httpAPIEvent is the Lambda payload for HTTP API (format version 2.0).
type httpAPIEvent struct {
	QueryStringParameters map[string]string     `json:"queryStringParameters,omitempty"`
	Headers               map[string]string     `json:"headers,omitempty"`
	PathParameters        map[string]string     `json:"pathParameters,omitempty"`
	StageVariables        map[string]string     `json:"stageVariables,omitempty"`
	Version               string                `json:"version"`
	RouteKey              string                `json:"routeKey"`
	RawPath               string                `json:"rawPath"`
	RawQueryString        string                `json:"rawQueryString"`
	Body                  string                `json:"body,omitempty"`
	RequestContext        httpAPIRequestContext `json:"requestContext"`
	Cookies               []string              `json:"cookies,omitempty"`
	IsBase64Encoded       bool                  `json:"isBase64Encoded"`
}

type httpAPIRequestContext struct {
	Authorizer   map[string]any     `json:"authorizer,omitempty"`
	HTTP         httpAPIHTTPContext `json:"http"`
	AccountID    string             `json:"accountId"`
	APIID        string             `json:"apiId"`
	DomainName   string             `json:"domainName"`
	DomainPrefix string             `json:"domainPrefix"`
	RequestID    string             `json:"requestId"`
	RouteKey     string             `json:"routeKey"`
	Stage        string             `json:"stage"`
	Time         string             `json:"time"`
	TimeEpoch    int64              `json:"timeEpoch"`
}

type httpAPIHTTPContext struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Protocol  string `json:"protocol"`
	SourceIP  string `json:"sourceIp"`
	UserAgent string `json:"userAgent"`
}

// httpAPILambdaResponse is the response returned by a Lambda function for format 2.0.
type httpAPILambdaResponse struct {
	Headers         map[string]string   `json:"headers,omitempty"`
	MultiValueHdrs  map[string][]string `json:"multiValueHeaders,omitempty"`
	Body            string              `json:"body,omitempty"`
	Cookies         []string            `json:"cookies,omitempty"`
	StatusCode      int                 `json:"statusCode"`
	IsBase64Encoded bool                `json:"isBase64Encoded,omitempty"`
}

// handleHTTPAPIProxy implements the HTTP API data plane for API Gateway v2.
// It performs route matching, optional JWT authorization, and integration dispatch.
func (h *Handler) handleHTTPAPIProxy(c *echo.Context, apiID, stageName, resourcePath string) error {
	req := c.Request()

	// Fetch the stage (best-effort — $default stage may not exist) for stage
	// variables and, below, its pinned deployment snapshot.
	stage := h.lookupStage(apiID, stageName)

	var stageVars map[string]string
	if stage != nil {
		stageVars = stage.StageVariables
	}

	// Fetch API for CORS configuration.
	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		return writeJSONMessage(c, http.StatusNotFound, httpAPIMessage("Not Found"))
	}

	// CORS preflight — respond immediately before route matching.
	if req.Method == http.MethodOptions && api.CorsConfiguration != nil {
		writeCORSPreflight(c.Response(), c.Request(), api.CorsConfiguration)

		return nil
	}

	if api.CorsConfiguration != nil {
		writeCORSResponseHeaders(c.Response(), c.Request(), api.CorsConfiguration)
	}

	// Route matching against the stage's pinned deployment snapshot when one
	// exists (gopherstack-cfr1); an undeployed stage, or one whose pinned
	// deployment was since deleted, falls back to the API's live routes.
	routes, deployment, err := h.resolveHTTPAPIRoutes(apiID, stage)
	if err != nil {
		return writeJSONMessage(c, http.StatusInternalServerError, msgInternalError)
	}

	matchedRoute, pathParams := matchHTTPAPIRoute(routes, req.Method, resourcePath)
	if matchedRoute == nil {
		return writeJSONMessage(c, http.StatusNotFound, httpAPIMessage("Not Found"))
	}

	c.Set(routeKeyCtxKey, matchedRoute.RouteKey)

	// Throttle (RouteSettings/DefaultRouteSettings) then authorization (NONE /
	// JWT / CUSTOM / AWS_IAM) enforcement for the matched route.
	if ctrlErr := h.applyRouteControls(c, apiID, stageName, resourcePath, matchedRoute); ctrlErr != nil {
		return writeRouteControlRejection(c, ctrlErr)
	}

	// Resolve integration.
	if matchedRoute.Target == "" {
		return c.String(http.StatusInternalServerError, "Route has no integration target")
	}

	integrationID := strings.TrimPrefix(matchedRoute.Target, "integrations/")

	integration, err := h.resolveHTTPAPIIntegration(apiID, integrationID, deployment)
	if err != nil {
		log := logger.Load(req.Context())
		log.Error("apigatewayv2: integration not found", "id", integrationID)

		return c.String(http.StatusInternalServerError, "Integration not found")
	}

	// Dispatch to integration.
	switch integration.IntegrationType {
	case IntegrationTypeAWSProxy:
		defer h.recordIntegrationLatency(c, time.Now())

		return h.invokeHTTPAPILambda(
			c, apiID, stageName, matchedRoute.RouteKey,
			resourcePath, pathParams, stageVars, integration,
		)
	case integrationTypeHTTPProxy, integrationTypeHTTPType:
		defer h.recordIntegrationLatency(c, time.Now())

		return h.forwardHTTPAPIHTTPIntegration(c, integration, stageVars, pathParams)
	default:
		return c.String(http.StatusInternalServerError, "Unsupported integration type: "+integration.IntegrationType)
	}
}

// lookupStage returns apiID's stageName Stage, or nil if it doesn't exist.
func (h *Handler) lookupStage(apiID, stageName string) *Stage {
	stage, err := h.Backend.GetStage(apiID, stageName)
	if err != nil {
		return nil
	}

	return stage
}

// resolveHTTPAPIRoutes returns the routes to match a request against, and the
// deployment they came from (nil when serving live state). A stage pinned to
// a deployment (stage.DeploymentID != "") serves that deployment's frozen
// route snapshot; a stage with no deployment yet, or whose pinned deployment
// was since deleted, falls back to the API's live routes so it still serves
// traffic instead of 500ing (gopherstack-cfr1).
func (h *Handler) resolveHTTPAPIRoutes(apiID string, stage *Stage) ([]Route, *Deployment, error) {
	if stage != nil && stage.DeploymentID != "" {
		if dep, err := h.Backend.GetDeployment(apiID, stage.DeploymentID); err == nil && dep != nil {
			return dep.Routes, dep, nil
		}
	}

	routes, err := h.Backend.GetRoutes(apiID)

	return routes, nil, err
}

// resolveHTTPAPIIntegration looks up integrationID within deployment's frozen
// snapshot when deployment is non-nil, otherwise reads the API's live
// integrations. Mirrors resolveHTTPAPIRoutes' snapshot-vs-live split so a
// route resolved from a deployment snapshot always resolves its integration
// from that same snapshot, never a live one (gopherstack-cfr1).
func (h *Handler) resolveHTTPAPIIntegration(apiID, integrationID string, deployment *Deployment) (*Integration, error) {
	if deployment == nil {
		return h.Backend.GetIntegration(apiID, integrationID)
	}

	for i := range deployment.Integrations {
		if deployment.Integrations[i].IntegrationID == integrationID {
			cp := deployment.Integrations[i]

			return &cp, nil
		}
	}

	return nil, ErrIntegrationNotFound
}

// applyRouteControls runs the throttle and authorizer checks for the matched
// route, in that order: throttle first, mirroring apigateway v1's
// stage-throttle-before-authorizer precedence (proxy.go's
// applyMethodControls) -- it's not client-specific, so it applies to traffic
// regardless of whether the request is later authorized. Returns a non-nil,
// unwritten error when the request is denied; the caller maps and writes it
// exactly once via writeRouteControlRejection.
func (h *Handler) applyRouteControls(
	c *echo.Context,
	apiID, stageName, resourcePath string,
	route *Route,
) error {
	if throttleErr := h.enforceRouteThrottle(c, apiID, stageName, route.RouteKey); throttleErr != nil {
		return throttleErr
	}

	return h.enforceRouteAuth(c, apiID, stageName, resourcePath, route)
}

// enforceRouteThrottle applies the stage's RouteSettings/DefaultRouteSettings
// throttling for routeKey. Returns nil (request allowed) when unthrottled, or
// ErrThrottled (unwritten) when the limit is exceeded -- the caller maps and
// writes the AWS-accurate 429 response exactly once. An unexpected
// enforcement error deliberately fails open (returns nil): that's a separate
// design decision from this rejection-writing bug, not part of it.
func (h *Handler) enforceRouteThrottle(c *echo.Context, apiID, stageName, routeKey string) error {
	log := logger.Load(c.Request().Context())

	err := h.Backend.EnforceRouteThrottle(apiID, stageName, routeKey)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrThrottled):
		log.Info("apigatewayv2: route throttle exceeded", "apiId", apiID, "stage", stageName, "routeKey", routeKey)

		return err
	default:
		log.Warn("apigatewayv2: route-throttle enforcement error", "error", err)

		return nil
	}
}

// enforceRouteAuth enforces the matched route's authorization type on the data
// plane. It dispatches to the JWT, REQUEST (Lambda), or AWS_IAM authorizer as
// appropriate and returns a non-nil, unwritten error when authorization
// fails; the caller maps and writes it exactly once. NONE routes pass through.
func (h *Handler) enforceRouteAuth(
	c *echo.Context,
	apiID, stageName, resourcePath string,
	route *Route,
) error {
	log := logger.Load(c.Request().Context())

	switch route.AuthorizationType {
	case authorizationTypeAWSIAM:
		return h.enforceIAMAuth(c)

	case authorizerTypeJWT:
		if route.AuthorizerID == "" {
			return nil
		}

		authorizer, authErr := h.Backend.GetAuthorizer(apiID, route.AuthorizerID)
		if authErr != nil {
			log.Warn("apigatewayv2: authorizer not found", "id", route.AuthorizerID)

			return errRouteUnauthorized
		}

		claims, jwtErr := h.validateJWT(c.Request(), authorizer)
		if jwtErr != nil {
			log.Info("apigatewayv2: JWT validation failed", "error", jwtErr)

			return errRouteUnauthorized
		}

		if !routeScopesSatisfied(claims, route.AuthorizationScopes) {
			return errRouteForbidden
		}

		setAuthorizerContext(c, jwtAuthorizerContext(claims))

		return nil

	case authorizationTypeCustom:
		if route.AuthorizerID == "" {
			return nil
		}

		authorizer, authErr := h.Backend.GetAuthorizer(apiID, route.AuthorizerID)
		if authErr != nil {
			log.Warn("apigatewayv2: authorizer not found", "id", route.AuthorizerID)

			return errRouteUnauthorized
		}

		return h.enforceRequestAuthorizer(c, apiID, stageName, route, authorizer, resourcePath)

	default: // NONE or unset
		return nil
	}
}

// buildV1Payload constructs a payload format 1.0 Lambda event and marshals it.
func buildV1Payload(
	req *http.Request,
	apiID, stageName, routeKey, resourcePath string,
	headers, qsp, pathParams, stageVars map[string]string,
	body string, isBase64 bool, requestID string, authorizer map[string]any,
) []byte {
	now := time.Now().UTC()
	resource := routeResource(routeKey)

	event := httpAPIV1Event{
		Version:                         payloadFormatV1,
		HTTPMethod:                      req.Method,
		Path:                            resourcePath,
		Resource:                        resource,
		Headers:                         headers,
		MultiValueHeaders:               multiValueHeaders(req.Header),
		QueryStringParameters:           qsp,
		MultiValueQueryStringParameters: req.URL.Query(),
		PathParameters:                  pathParams,
		StageVariables:                  stageVars,
		Body:                            body,
		IsBase64Encoded:                 isBase64,
		RequestContext: httpAPIV1RequestContext{
			Authorizer:       authorizer,
			HTTPMethod:       req.Method,
			ResourcePath:     resource,
			Path:             resourcePath,
			Stage:            stageName,
			APIId:            apiID,
			AccountID:        config.DefaultAccountID,
			DomainName:       apiID + ".execute-api." + regionFromCtx(req.Context()) + ".amazonaws.com",
			DomainPrefix:     apiID,
			Protocol:         req.Proto,
			RequestID:        requestID,
			RequestTime:      now.Format("02/Jan/2006:15:04:05 +0000"),
			RequestTimeEpoch: now.UnixMilli(),
			Identity:         httpAPIV1Identity{SourceIP: realIP(req), UserAgent: req.UserAgent()},
		},
	}

	p, _ := json.Marshal(event)

	return p
}

// routeResource returns the 1.0 event's resource: the route key without its method.
func routeResource(routeKey string) string {
	if _, path, ok := strings.Cut(routeKey, " "); ok {
		return path
	}

	return "/" + routeKey
}

// multiValueHeaders returns the lowercase-keyed multi-value header map of format 1.0.
func multiValueHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		out[strings.ToLower(k)] = vs
	}

	return out
}

// eventPath prefixes resourcePath with the stage name for named stages, as the
// default execute-api endpoint does in payload format 2.0.
func eventPath(stageName, resourcePath string) string {
	if stageName == "" || stageName == routeKeyDefault {
		return resourcePath
	}

	return "/" + stageName + resourcePath
}

// buildV2Payload constructs a payload format 2.0 Lambda event and marshals it.
func buildV2Payload(
	req *http.Request,
	apiID, stageName, routeKey, resourcePath string,
	headers, qsp, pathParams, stageVars map[string]string,
	cookies []string,
	body string, isBase64 bool,
	requestID string, now time.Time, authorizer map[string]any,
) []byte {
	rawPath := eventPath(stageName, resourcePath)

	event := httpAPIEvent{
		Version:               payloadFormatV2,
		RouteKey:              routeKey,
		RawPath:               rawPath,
		RawQueryString:        req.URL.RawQuery,
		Headers:               headers,
		QueryStringParameters: qsp,
		PathParameters:        pathParams,
		StageVariables:        stageVars,
		Body:                  body,
		IsBase64Encoded:       isBase64,
		Cookies:               cookies,
		RequestContext: httpAPIRequestContext{
			Authorizer:   authorizer,
			AccountID:    config.DefaultAccountID,
			APIID:        apiID,
			DomainName:   apiID + ".execute-api." + regionFromCtx(req.Context()) + ".amazonaws.com",
			DomainPrefix: apiID,
			RouteKey:     routeKey,
			Stage:        stageName,
			RequestID:    requestID,
			Time:         now.Format("02/Jan/2006:15:04:05 +0000"),
			TimeEpoch:    now.UnixMilli(),
			HTTP: httpAPIHTTPContext{
				Method:    req.Method,
				Path:      rawPath,
				Protocol:  req.Proto,
				SourceIP:  realIP(req),
				UserAgent: req.UserAgent(),
			},
		},
	}

	p, _ := json.Marshal(event)

	return p
}

// invokeHTTPAPILambda builds the Lambda payload and invokes the function, then writes the response.
func (h *Handler) invokeHTTPAPILambda(
	c *echo.Context,
	apiID, stageName, routeKey, resourcePath string,
	pathParams map[string]string,
	stageVars map[string]string,
	integration *Integration,
) error {
	if h.lambdaInvoker == nil {
		return c.String(http.StatusServiceUnavailable, "Lambda invoker not configured")
	}

	req := c.Request()

	// Read body.
	body, isBase64, err := readHTTPAPIBody(req)
	if err != nil {
		return c.String(http.StatusBadRequest, "Failed to read request body")
	}

	// Build headers map (lowercased, multi-value joined with comma per format 2.0).
	headers := buildHTTPAPIHeaders(req)

	// Build query string parameters.
	qsp := make(map[string]string, len(req.URL.Query()))
	for k, vs := range req.URL.Query() {
		qsp[k] = strings.Join(vs, ",")
	}

	// Extract cookies.
	cookies := make([]string, 0, len(req.Cookies()))
	for _, ck := range req.Cookies() {
		cookies = append(cookies, ck.String())
	}

	now := time.Now().UTC()
	requestID := uuid.New().String()

	// Choose payload format version.
	pfv := integration.PayloadFormatVersion
	if pfv == "" {
		pfv = payloadFormatV2
	}

	lambdaArn := extractHTTPAPILambdaARN(integration.IntegrationURI)

	var payload []byte

	switch pfv {
	case payloadFormatV1:
		payload = buildV1Payload(req, apiID, stageName, routeKey, resourcePath,
			headers, qsp, pathParams, stageVars, body, isBase64, requestID, authorizerContext(c))
	default: // 2.0
		payload = buildV2Payload(req, apiID, stageName, routeKey, resourcePath,
			headers, qsp, pathParams, stageVars, cookies, body, isBase64, requestID, now, authorizerContext(c))
	}

	respBytes, statusCode, invokeErr := h.lambdaInvoker.InvokeFunction(
		req.Context(),
		lambdaArn,
		"RequestResponse",
		payload,
	)
	if invokeErr != nil {
		logger.Load(req.Context()).Error("apigatewayv2: Lambda invocation failed",
			"arn", lambdaArn, "error", invokeErr)

		return c.String(http.StatusBadGateway, "Integration invocation failed")
	}

	// Lambda may return a function error via status code (200 = ok, 200 with X-Amz-Function-Error = error).
	_ = statusCode

	return writeHTTPAPILambdaResponse(c, respBytes, pfv)
}

// forwardHTTPAPIHTTPIntegration forwards a request to an HTTP backend integration.
func (h *Handler) forwardHTTPAPIHTTPIntegration(
	c *echo.Context,
	integration *Integration,
	stageVars, pathParams map[string]string,
) error {
	if integration.IntegrationURI == "" {
		return c.String(http.StatusInternalServerError, "Integration URI is empty")
	}

	// Substitute stage variables in URI.
	targetURI := substitutePathParams(substituteStageVars(integration.IntegrationURI, stageVars), pathParams)

	method := c.Request().Method
	if integration.IntegrationMethod != "" && !strings.EqualFold(integration.IntegrationMethod, httpMethodAny) {
		method = integration.IntegrationMethod
	}

	upstreamReq, err := http.NewRequestWithContext(c.Request().Context(), method, targetURI, c.Request().Body)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to build upstream request")
	}

	// Copy incoming headers.
	for k, vs := range c.Request().Header {
		for _, v := range vs {
			upstreamReq.Header.Add(k, v)
		}
	}

	// Merge query parameters.
	upstreamReq.URL.RawQuery = c.Request().URL.RawQuery

	client := h.getHTTPClient()

	resp, doErr := client.Do(upstreamReq)
	if doErr != nil {
		return c.String(http.StatusBadGateway, "Upstream request failed")
	}

	defer resp.Body.Close()

	// Copy response headers.
	for k, vs := range resp.Header {
		for _, v := range vs {
			c.Response().Header().Add(k, v)
		}
	}

	c.Response().WriteHeader(resp.StatusCode)
	_, _ = io.Copy(c.Response(), resp.Body)

	return nil
}

// matchHTTPAPIRoute finds the best matching route for the given HTTP method and path.
// Priority: literal > path-param > greedy > $default.
func matchHTTPAPIRoute(routes []Route, method, path string) (*Route, map[string]string) {
	type candidate struct {
		route  *Route
		params map[string]string
		score  int
	}

	var best *candidate
	var defaultRoute *Route

	for i := range routes {
		r := &routes[i]

		if r.RouteKey == routeKeyDefault {
			defaultRoute = r

			continue
		}

		// Parse "METHOD /path" route key.
		const maxParts = 2

		parts := strings.SplitN(r.RouteKey, " ", maxParts)
		if len(parts) != maxParts {
			continue
		}

		routeMethod, routePath := parts[0], parts[1]

		// "ANY" matches all HTTP methods.
		if routeMethod != httpMethodAny && routeMethod != method {
			continue
		}

		params, score, ok := scoreHTTPRoutePath(routePath, path)
		if !ok {
			continue
		}

		if best == nil || score > best.score {
			best = &candidate{route: r, params: params, score: score}
		}
	}

	if best != nil {
		return best.route, best.params
	}

	if defaultRoute != nil {
		return defaultRoute, map[string]string{}
	}

	return nil, nil
}

// scoreHTTPRoutePath matches routePath against requestPath and returns extracted
// path params, a specificity score (higher = more literal segments), and whether the match succeeded.
func scoreHTTPRoutePath(routePath, requestPath string) (map[string]string, int, bool) {
	routeSegs := splitHTTPPathSegs(routePath)
	reqSegs := splitHTTPPathSegs(requestPath)

	// Determine whether the route ends with a greedy param.
	hasGreedy := len(routeSegs) > 0 && strings.HasSuffix(routeSegs[len(routeSegs)-1], "+}")

	// Segment count check.
	if !hasGreedy && len(routeSegs) != len(reqSegs) {
		return nil, 0, false
	}

	if hasGreedy && len(reqSegs) < len(routeSegs)-1 {
		return nil, 0, false
	}

	params := map[string]string{}
	score := 0

	for i, seg := range routeSegs {
		if strings.HasSuffix(seg, "+}") {
			// Greedy: capture remaining segments.
			name := seg[1 : len(seg)-2]
			params[name] = strings.Join(reqSegs[i:], "/")

			break
		}

		if i >= len(reqSegs) {
			return nil, 0, false
		}

		switch {
		case strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}"):
			// Named path parameter.
			params[seg[1:len(seg)-1]] = reqSegs[i]
		case seg == reqSegs[i]:
			score++
		default:
			return nil, 0, false
		}
	}

	return params, score, true
}

// splitHTTPPathSegs splits a URL path into non-empty segments.
func splitHTTPPathSegs(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return []string{}
	}

	return strings.Split(trimmed, "/")
}

// validateJWT validates the JWT token extracted from the request using
// the authorizer's identity source and JwtConfiguration (issuer + audience).
// Signature is verified against the JWKS provider when available; tokens from
// unknown issuers are always rejected.
func (h *Handler) validateJWT(r *http.Request, auth *Authorizer) (jwt.MapClaims, error) {
	tokenStr := extractJWTToken(r, auth.IdentitySource)
	if tokenStr == "" {
		return nil, errNoTokenFound
	}

	// Strip "Bearer " prefix (case-insensitive).
	tokenStr = strings.TrimPrefix(tokenStr, "Bearer ")
	tokenStr = strings.TrimPrefix(tokenStr, "bearer ")

	cfg := auth.JwtConfiguration

	keyfunc := func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errUnexpectedSigningMethod
		}

		kid, _ := t.Header["kid"].(string)

		// Use the configured issuer for key lookup; iss claim is validated below.
		issuer := ""
		if cfg != nil {
			issuer = cfg.Issuer
		}

		if h.jwksProvider == nil {
			return nil, errNoJWKSProvider
		}

		if mc, ok := t.Claims.(jwt.MapClaims); ok {
			if tokenIss, _ := mc["iss"].(string); tokenIss != "" && jwtIssuerMatches(tokenIss, issuer) {
				issuer = tokenIss
			}
		}

		return h.jwksProvider.GetJWTPublicKey(issuer, kid)
	}

	token, parseErr := jwt.Parse(tokenStr, keyfunc, jwt.WithExpirationRequired())
	if parseErr != nil {
		return nil, fmt.Errorf("invalid JWT: %w", parseErr)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, errInvalidJWTClaimsType
	}

	if err := validateJWTClaims(claims, cfg); err != nil {
		return nil, err
	}

	return claims, nil
}

// validateJWTClaims checks exp, iss, and aud claims against the JwtConfiguration.
func validateJWTClaims(claims jwt.MapClaims, cfg *JwtConfiguration) error {
	// Validate expiry.
	if expVal, hasExp := claims["exp"]; hasExp {
		if expF, isFloat := expVal.(float64); isFloat {
			if time.Now().Unix() > int64(expF) {
				return errJWTExpired
			}
		}
	}

	if cfg == nil {
		return nil
	}

	// Validate issuer if configured.
	if cfg.Issuer != "" {
		if issVal, hasIss := claims["iss"]; hasIss {
			if issStr, isStr := issVal.(string); !isStr || !jwtIssuerMatches(issStr, cfg.Issuer) {
				return fmt.Errorf("%w: got %v, want %s", errJWTIssuerMismatch, issVal, cfg.Issuer)
			}
		} else {
			return errJWTMissingIss
		}
	}

	// Validate audience if configured.
	if len(cfg.Audience) > 0 {
		if !jwtAudienceValid(claims, cfg.Audience) {
			return errJWTAudienceMismatch
		}
	}

	return nil
}

// jwtIssuerMatches reports whether a token's iss names the configured issuer. A
// Cognito issuer written in its real-AWS form (https://cognito-idp.<region>.amazonaws.com/<pool>)
// also matches the emulator-hosted issuer of the same pool.
func jwtIssuerMatches(tokenIss, cfgIss string) bool {
	if tokenIss == cfgIss {
		return true
	}

	tokenPool, tokenOK := cognitoPoolOfIssuer(tokenIss)
	cfgPool, cfgOK := cognitoPoolOfIssuer(cfgIss)

	return tokenOK && cfgOK && tokenPool == cfgPool
}

// cognitoPoolOfIssuer returns the "<region>_<id>" pool id when iss is shaped like
// a Cognito issuer URL (a single path segment of that form).
func cognitoPoolOfIssuer(iss string) (string, bool) {
	u, err := url.Parse(iss)
	if err != nil {
		return "", false
	}

	pool := strings.Trim(u.Path, "/")
	region, id, found := strings.Cut(pool, "_")

	return pool, found && region != "" && id != "" && !strings.Contains(pool, "/")
}

// extractJWTToken extracts the raw token string from the request using the identity
// source expression (e.g. "$request.header.Authorization").
func extractJWTToken(r *http.Request, identitySource []string) string {
	for _, src := range identitySource {
		if name, ok := strings.CutPrefix(src, "$request.header."); ok {
			if v := r.Header.Get(name); v != "" {
				return v
			}
		} else if qname, qok := strings.CutPrefix(src, "$request.querystring."); qok {
			if v := r.URL.Query().Get(qname); v != "" {
				return v
			}
		}
	}

	// Default to Authorization header.
	return r.Header.Get("Authorization")
}

// jwtAudienceValid returns true if the token aud claim contains at least one of the
// configured audiences.
func jwtAudienceValid(claims jwt.MapClaims, wantAud []string) bool {
	audVal, hasAud := claims["aud"]
	if !hasAud {
		audVal, hasAud = claims["client_id"]
	}

	if !hasAud {
		return false
	}

	var tokenAuds []string

	switch v := audVal.(type) {
	case string:
		tokenAuds = []string{v}
	case []any:
		for _, a := range v {
			if s, ok := a.(string); ok {
				tokenAuds = append(tokenAuds, s)
			}
		}
	}

	for _, want := range wantAud {
		if slices.Contains(tokenAuds, want) {
			return true
		}
	}

	return false
}

// writeCORSPreflight writes a 204 response with CORS headers for a preflight request.
func writeCORSPreflight(w http.ResponseWriter, r *http.Request, cors *CorsConfiguration) {
	if allowedCORSOrigin(r, cors) != "" {
		writeCORSResponseHeaders(w, r, cors)

		if len(cors.AllowMethods) > 0 {
			w.Header().Set("Access-Control-Allow-Methods", strings.Join(cors.AllowMethods, ","))
		}

		if len(cors.AllowHeaders) > 0 {
			w.Header().Set("Access-Control-Allow-Headers", strings.Join(cors.AllowHeaders, ","))
		}

		if cors.MaxAge > 0 {
			w.Header().Set("Access-Control-Max-Age", strconv.FormatInt(int64(cors.MaxAge), 10))
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// allowedCORSOrigin returns the Access-Control-Allow-Origin value for r, or "" when
// the request has no Origin or the origin is not allowed.
func allowedCORSOrigin(r *http.Request, cors *CorsConfiguration) string {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return ""
	}

	for _, o := range cors.AllowOrigins {
		if o == "*" {
			if cors.AllowCredentials {
				return origin
			}

			return "*"
		}

		if o == origin {
			return origin
		}
	}

	return ""
}

// writeCORSResponseHeaders sets the headers API Gateway adds to actual (non-preflight)
// responses for an allowed Origin.
func writeCORSResponseHeaders(w http.ResponseWriter, r *http.Request, cors *CorsConfiguration) {
	allowed := allowedCORSOrigin(r, cors)
	if allowed == "" {
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", allowed)

	if len(cors.ExposeHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(cors.ExposeHeaders, ","))
	}

	if cors.AllowCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}

// readHTTPAPIBody reads the request body, returning it as a string (or base64 for binary).
func readHTTPAPIBody(r *http.Request) (string, bool, error) {
	if r.Body == nil {
		return "", false, nil
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPAPIBodyBytes))
	if err != nil {
		return "", false, err
	}

	if utf8.Valid(raw) {
		return string(raw), false, nil
	}

	return base64.StdEncoding.EncodeToString(raw), true, nil
}

// buildHTTPAPIHeaders builds a lowercase header map for the Lambda event.
// Multi-value headers are joined with comma per format 2.0 spec.
func buildHTTPAPIHeaders(r *http.Request) map[string]string {
	headers := make(map[string]string, len(r.Header))

	for k, vs := range r.Header {
		headers[strings.ToLower(k)] = strings.Join(vs, ",")
	}

	return headers
}

// extractHTTPAPILambdaARN extracts the Lambda ARN or function name from an integration URI.
// Supports:
//   - "arn:aws:lambda:region:account:function:name[/invocations]"
//   - "arn:aws:apigateway:region:lambda:path/.../functions/{arn}/invocations"
func extractHTTPAPILambdaARN(uri string) string {
	const invocations = "/invocations"

	if before, _, ok := strings.CutLast(uri, invocations); ok {
		arn := before

		const functionsPrefix = "/functions/"
		if fi := strings.LastIndex(arn, functionsPrefix); fi != -1 {
			arn = arn[fi+len(functionsPrefix):]
		}

		return extractHTTPAPILambdaARN(arn)
	}

	const functionSegment = ":function:"

	if _, after, ok := strings.CutLast(uri, functionSegment); ok {
		return after
	}

	// Assume it's already a function name.
	return uri
}

// substitutePathParams replaces {name} placeholders in an HTTP integration URI with the
// matched route's path parameters (a greedy {proxy+} param is referenced as {proxy}).
func substitutePathParams(uri string, pathParams map[string]string) string {
	for name, val := range pathParams {
		segs := strings.Split(val, "/")
		for i, seg := range segs {
			segs[i] = url.PathEscape(seg)
		}

		uri = strings.ReplaceAll(uri, "{"+name+"}", strings.Join(segs, "/"))
	}

	return uri
}

// substituteStageVars replaces ${stageVariables.X} placeholders in s with values from stageVars.
func substituteStageVars(s string, stageVars map[string]string) string {
	const prefix = "${stageVariables."

	for {
		start := strings.Index(s, prefix)
		if start < 0 {
			break
		}

		end := strings.Index(s[start:], "}")
		if end < 0 {
			break
		}

		end += start
		name := s[start+len(prefix) : end]
		val := stageVars[name]
		s = s[:start] + val + s[end+1:]
	}

	return s
}

// realIP extracts the real client IP from the request, checking X-Forwarded-For first.
func realIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", maxXFFParts)

		return strings.TrimSpace(parts[0])
	}

	if ip, _, err := splitHostPort(r.RemoteAddr); err == nil {
		return ip
	}

	return r.RemoteAddr
}

// splitHostPort splits host:port, handling IPv6 addresses.
func splitHostPort(addr string) (string, string, error) {
	if !strings.Contains(addr, ":") {
		return addr, "", nil
	}

	u, err := url.Parse("tcp://" + addr)
	if err != nil {
		return "", "", err
	}

	return u.Hostname(), u.Port(), nil
}

// httpAPIV1Event is a v1-format Lambda event for format version "1.0".
type httpAPIV1Event struct {
	QueryStringParameters           map[string]string       `json:"queryStringParameters,omitempty"`
	MultiValueQueryStringParameters map[string][]string     `json:"multiValueQueryStringParameters,omitempty"`
	Headers                         map[string]string       `json:"headers,omitempty"`
	MultiValueHeaders               map[string][]string     `json:"multiValueHeaders,omitempty"`
	PathParameters                  map[string]string       `json:"pathParameters,omitempty"`
	StageVariables                  map[string]string       `json:"stageVariables,omitempty"`
	Version                         string                  `json:"version,omitempty"`
	Resource                        string                  `json:"resource"`
	Path                            string                  `json:"path"`
	HTTPMethod                      string                  `json:"httpMethod"`
	Body                            string                  `json:"body,omitempty"`
	RequestContext                  httpAPIV1RequestContext `json:"requestContext"`
	IsBase64Encoded                 bool                    `json:"isBase64Encoded"`
}

type httpAPIV1Identity struct {
	SourceIP  string `json:"sourceIp,omitempty"`
	UserAgent string `json:"userAgent,omitempty"`
}

type httpAPIV1RequestContext struct {
	Authorizer       map[string]any    `json:"authorizer,omitempty"`
	Identity         httpAPIV1Identity `json:"identity,omitzero"`
	ResourcePath     string            `json:"resourcePath"`
	HTTPMethod       string            `json:"httpMethod"`
	Stage            string            `json:"stage"`
	APIId            string            `json:"apiId"`
	AccountID        string            `json:"accountId,omitempty"`
	DomainName       string            `json:"domainName,omitempty"`
	DomainPrefix     string            `json:"domainPrefix,omitempty"`
	Path             string            `json:"path,omitempty"`
	Protocol         string            `json:"protocol,omitempty"`
	RequestID        string            `json:"requestId,omitempty"`
	RequestTime      string            `json:"requestTime,omitempty"`
	RequestTimeEpoch int64             `json:"requestTimeEpoch,omitempty"`
}
