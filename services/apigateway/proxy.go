package apigateway

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// defaultAuthorizerTTL is the default authorizer result cache TTL (AWS default: 300 s).
const defaultAuthorizerTTL = 300 * time.Second

const defaultAuthorizerCacheMaxEntries = 1024

const defaultIdentitySource = "method.request.header.Authorization"

// maxProxyRequestBodyBytes caps API Gateway proxy request bodies. AWS limits the
// Lambda synchronous invoke payload to 6 MiB; bodies larger than that cannot be
// forwarded anyway, so cap reads to prevent unbounded io.ReadAll memory usage.
const maxProxyRequestBodyBytes = 6 * 1024 * 1024 // 6 MiB

// LambdaInvoker can invoke a Lambda function by name/ARN.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// LambdaProxyEvent is the API Gateway Lambda proxy event format.
// https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
type LambdaProxyEvent struct {
	QueryStringParameters map[string]string   `json:"queryStringParameters,omitempty"`
	Headers               map[string]string   `json:"headers,omitempty"`
	MultiValueHeaders     map[string][]string `json:"multiValueHeaders,omitempty"`
	PathParameters        map[string]string   `json:"pathParameters,omitempty"`
	MultiValueQueryString map[string][]string `json:"multiValueQueryStringParameters,omitempty"`
	StageVariables        map[string]string   `json:"stageVariables,omitempty"`
	RequestContext        LambdaProxyContext  `json:"requestContext"`
	Resource              string              `json:"resource"`
	Path                  string              `json:"path"`
	HTTPMethod            string              `json:"httpMethod"`
	Body                  string              `json:"body,omitempty"`
	IsBase64Encoded       bool                `json:"isBase64Encoded"`
}

// LambdaProxyContext provides context for the Lambda proxy event.
type LambdaProxyContext struct {
	Authorizer   map[string]any `json:"authorizer,omitempty"`
	ResourcePath string         `json:"resourcePath"`
	HTTPMethod   string         `json:"httpMethod"`
	Stage        string         `json:"stage"`
	APIId        string         `json:"apiId"`
	RequestID    string         `json:"requestId,omitempty"`
}

type ctxKey int

const ctxKeyClaims ctxKey = 1

// LambdaProxyResponse is the response format from a Lambda proxy function.
type LambdaProxyResponse struct {
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	StatusCode      int               `json:"statusCode"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
}

// BuildProxyEvent converts an incoming HTTP request to a Lambda proxy event.
// pathParameters are the path variable values extracted by the routing engine (may be nil).
func BuildProxyEvent(
	r *http.Request,
	apiID, stageName, resource, path string,
	pathParameters map[string]string,
) (*LambdaProxyEvent, error) {
	// Read body.
	var bodyStr string
	var isBase64 bool

	if r.Body != nil {
		bodyBytes, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxProxyRequestBodyBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}

		if utf8.Valid(bodyBytes) {
			bodyStr = string(bodyBytes)
		} else {
			bodyStr = base64.StdEncoding.EncodeToString(bodyBytes)
			isBase64 = true
		}
	}

	// Build headers map.
	headers := make(map[string]string)
	multiValueHeaders := make(map[string][]string)

	for k, vs := range r.Header {
		lower := strings.ToLower(k)
		headers[lower] = vs[len(vs)-1] // last value
		multiValueHeaders[lower] = vs
	}

	// Build query parameters.
	qsp := make(map[string]string)
	mqsp := make(map[string][]string)

	for k, vs := range r.URL.Query() {
		qsp[k] = vs[len(vs)-1]
		mqsp[k] = vs
	}

	var authorizer map[string]any
	if claims, ok := r.Context().Value(ctxKeyClaims).(jwt.MapClaims); ok {
		authorizer = map[string]any{
			"claims": claims,
		}
	}

	return &LambdaProxyEvent{
		HTTPMethod:            r.Method,
		Path:                  path,
		Resource:              resource,
		Headers:               headers,
		MultiValueHeaders:     multiValueHeaders,
		QueryStringParameters: qsp,
		MultiValueQueryString: mqsp,
		PathParameters:        pathParameters,
		Body:                  bodyStr,
		IsBase64Encoded:       isBase64,
		RequestContext: LambdaProxyContext{
			ResourcePath: resource,
			HTTPMethod:   r.Method,
			Stage:        stageName,
			APIId:        apiID,
			Authorizer:   authorizer,
		},
	}, nil
}

// authorizerCacheEntry holds a cached authorizer result.
type authorizerCacheEntry struct {
	expiresAt time.Time
	key       string
	allowed   bool
}

// authorizerCache caches Lambda authorizer results keyed by authorizerID + cacheKey.
type authorizerCache struct {
	entries    map[string]*list.Element
	order      *list.List
	maxEntries int
	mu         sync.Mutex
}

func newAuthorizerCache() *authorizerCache {
	return newAuthorizerCacheWithMaxEntries(defaultAuthorizerCacheMaxEntries)
}

func newAuthorizerCacheWithMaxEntries(maxEntries int) *authorizerCache {
	if maxEntries <= 0 {
		maxEntries = 1
	}

	return &authorizerCache{
		entries:    make(map[string]*list.Element),
		order:      list.New(),
		maxEntries: maxEntries,
	}
}

// get returns (allowed, found) for the given cache key.
func (c *authorizerCache) get(key string) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return false, false
	}

	e, ok := elem.Value.(authorizerCacheEntry)
	if !ok {
		c.removeElement(elem)

		return false, false
	}

	if time.Now().After(e.expiresAt) {
		c.removeElement(elem)

		return false, false
	}

	c.order.MoveToFront(elem)

	return e.allowed, true
}

// set stores the result for the given cache key with a TTL.
func (c *authorizerCache) set(key string, allowed bool, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		entry, valueOk := elem.Value.(authorizerCacheEntry)
		if !valueOk {
			c.removeElement(elem)
		} else {
			entry.allowed = allowed
			entry.expiresAt = time.Now().Add(ttl)
			elem.Value = entry
			c.order.MoveToFront(elem)

			return
		}
	}

	elem := c.order.PushFront(authorizerCacheEntry{
		key:       key,
		allowed:   allowed,
		expiresAt: time.Now().Add(ttl),
	})
	c.entries[key] = elem

	for len(c.entries) > c.maxEntries {
		c.removeElement(c.order.Back())
	}
}

// flush removes all entries from the cache (used by FlushStageAuthorizersCache).
func (c *authorizerCache) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*list.Element)
	c.order.Init()
}

func (c *authorizerCache) removeElement(elem *list.Element) {
	if elem == nil {
		return
	}

	entry, ok := elem.Value.(authorizerCacheEntry)
	if !ok {
		c.order.Remove(elem)

		return
	}

	delete(c.entries, entry.key)
	c.order.Remove(elem)
}

// handleProxyRequest handles a single HTTP request for a Lambda proxy integration.
func (h *Handler) handleProxyRequest(apiID, stageName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Find the resource and integration.
		resources, _, err := h.Backend.GetResources(apiID, "", 0)
		if err != nil {
			logger.Load(ctx).ErrorContext(ctx, "APIGateway proxy: failed to get resources", "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)

			return
		}

		// Match request path to resource path, extracting any path parameters.
		resource, pathParams := findMatchingResource(resources, r.URL.Path, stageName)
		if resource == nil {
			http.NotFound(w, r)

			return
		}

		// Handle CORS preflight OPTIONS request.
		if r.Method == http.MethodOptions {
			if resource.CorsConfiguration != nil {
				h.writeCORSPreflight(w, r, resource.CorsConfiguration)

				return
			}
		}

		// Attach CORS headers to the response when configured.
		if resource.CorsConfiguration != nil {
			h.addCORSHeaders(w, r, resource.CorsConfiguration)
		}

		// Apply method-level access controls (authorizer + request validator).
		if denied := h.applyMethodControls(ctx, w, r, apiID, stageName, resource.ID); denied {
			return
		}

		// Get the integration.
		integration, err := h.Backend.GetIntegration(apiID, resource.ID, r.Method)
		if err != nil {
			// Fall back to any method.
			integration, err = h.Backend.GetIntegration(apiID, resource.ID, "ANY")
			if err != nil {
				http.NotFound(w, r)

				return
			}
		}

		h.dispatchIntegration(ctx, w, r, apiID, stageName, resource, integration, pathParams)
	}
}

// applyMethodControls runs the authorizer and request validator for the matched method.
// Returns true if the request was denied and the response has already been written.
func (h *Handler) applyMethodControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName, resourceID string,
) bool {
	method, methodErr := h.Backend.GetMethod(apiID, resourceID, r.Method)
	if methodErr != nil {
		method, methodErr = h.Backend.GetMethod(apiID, resourceID, "ANY")
	}

	if methodErr != nil || method == nil {
		return false
	}

	if method.APIKeyRequired {
		if h.enforceAPIKey(ctx, w, r, apiID) {
			return true
		}
	}

	if method.AuthorizerID != "" {
		if h.runAuthorizer(ctx, w, r, apiID, stageName, method.AuthorizerID) {
			return true
		}
	}

	if method.RequestValidatorID != "" {
		if h.runRequestValidator(ctx, w, r, apiID, method.RequestValidatorID) {
			return true
		}
	}

	return false
}

// enforceAPIKey validates the x-api-key header against enabled API keys.
// Returns true if the request was denied (response already written).
func (h *Handler) enforceAPIKey(ctx context.Context, w http.ResponseWriter, r *http.Request, apiID string) bool {
	keyValue := r.Header.Get("X-Api-Key")
	if keyValue == "" {
		logger.Load(ctx).InfoContext(ctx, "APIGateway proxy: missing x-api-key", "apiId", apiID)
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	apiKey, err := h.Backend.GetAPIKeyByValue(keyValue)
	if err != nil || apiKey == nil {
		logger.Load(ctx).InfoContext(ctx, "APIGateway proxy: invalid x-api-key", "apiId", apiID)
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	if !apiKey.Enabled {
		logger.Load(ctx).InfoContext(ctx, "APIGateway proxy: disabled API key", "apiId", apiID, "keyId", apiKey.ID)
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	return false
}

// dispatchIntegration routes the request to the appropriate integration handler.
func (h *Handler) dispatchIntegration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName string,
	resource *Resource,
	integration *Integration,
	pathParams map[string]string,
) {
	// Interpolate stage variables (${stageVariables.X}) in integration URI/templates.
	stageVars := h.stageVars(apiID, stageName)
	if len(stageVars) > 0 {
		resolved := *integration
		resolved.URI = interpolateStageVars(integration.URI, stageVars)
		if len(integration.RequestTemplates) > 0 {
			resolved.RequestTemplates = make(map[string]string, len(integration.RequestTemplates))
			for k, v := range integration.RequestTemplates {
				resolved.RequestTemplates[k] = interpolateStageVars(v, stageVars)
			}
		}
		integration = &resolved
	}

	switch integration.Type {
	case "AWS_PROXY":
		h.handleAWSProxy(ctx, w, r, apiID, stageName, resource, integration, pathParams)
	case "AWS":
		h.handleAWSIntegration(ctx, w, r, apiID, stageName, resource, stageVars, integration)
	case "HTTP", "HTTP_PROXY":
		h.handleHTTPProxy(ctx, w, r, integration)
	case IntegrationTypeMock:
		h.handleMockIntegration(w, integration)
	default:
		http.Error(w, "Unsupported or unknown integration type for stage URL", http.StatusNotImplemented)
	}
}

// stageVars fetches stage variables for the given API/stage (returns nil on error).
func (h *Handler) stageVars(apiID, stageName string) map[string]string {
	stage, err := h.Backend.GetStage(apiID, stageName)
	if err != nil || stage == nil {
		return nil
	}

	return stage.Variables
}

// interpolateStageVars substitutes ${stageVariables.X} placeholders in s with values from vars.
func interpolateStageVars(s string, vars map[string]string) string {
	if len(vars) == 0 || !strings.Contains(s, "${stageVariables.") {
		return s
	}

	for k, v := range vars {
		s = strings.ReplaceAll(s, "${stageVariables."+k+"}", v)
	}

	return s
}

// AuthorizerEvent is the event payload sent to a Lambda authorizer function.
type AuthorizerEvent struct {
	Headers               map[string]string  `json:"headers,omitempty"`
	QueryStringParameters map[string]string  `json:"queryStringParameters,omitempty"`
	StageVariables        map[string]string  `json:"stageVariables,omitempty"`
	RequestContext        LambdaProxyContext `json:"requestContext"`
	Type                  string             `json:"type"`
	AuthorizationToken    string             `json:"authorizationToken,omitempty"`
	MethodArn             string             `json:"methodArn"`
	Resource              string             `json:"resource,omitempty"`
	Path                  string             `json:"path,omitempty"`
	HTTPMethod            string             `json:"httpMethod,omitempty"`
}

// runAuthorizer invokes the Lambda authorizer and returns true if the request
// should be denied (i.e., the response was written with a 4xx status).
func (h *Handler) runAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName, authorizerID string,
) bool {
	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return true
	}

	auth, err := h.Backend.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer not found", "authorizerId", authorizerID)
		http.Error(w, "Authorizer configuration error", http.StatusInternalServerError)

		return true
	}

	// Determine TTL for cache (authorizer-level setting, default 300 s).
	ttl := defaultAuthorizerTTL
	if auth.AuthorizerResultTTLInSeconds > 0 {
		ttl = time.Duration(auth.AuthorizerResultTTLInSeconds) * time.Second
	} else if auth.AuthorizerResultTTLInSeconds < 0 {
		ttl = 0 // caching disabled
	}

	// Build cache key: for TOKEN type use the token, for REQUEST type use the full path.
	cacheKey := h.authorizerCacheKey(r, auth, authorizerID)
	if ttl > 0 {
		if allowed, found := h.authCache.get(cacheKey); found {
			if !allowed {
				http.Error(w, "Forbidden", http.StatusForbidden)

				return true
			}

			return false
		}
	}

	if auth.Type == AuthTypeCognitoUserPool {
		return h.runCognitoAuthorizer(ctx, w, r, auth, cacheKey, ttl)
	}

	// Build the authorizer event based on type.
	event := h.buildAuthorizerEvent(ctx, r, auth, apiID, stageName)

	payload, _ := json.Marshal(event)

	funcName := ExtractLambdaFunctionName(auth.AuthorizerURI)
	respBytes, _, invokeErr := h.lambda.InvokeFunction(ctx, funcName, "RequestResponse", payload)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer invocation failed",
			"authorizerId", authorizerID, "error", invokeErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	// Parse the authorizer response (IAM policy document).
	var authResp AuthorizerResponse
	if parseErr := json.Unmarshal(respBytes, &authResp); parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: failed to parse authorizer response", "error", parseErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	// Evaluate the policy document to determine allow/deny.
	allowed := isAuthorizerAllowed(&authResp)
	h.authCache.set(cacheKey, allowed, ttl)

	if !allowed {
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	return false
}

// runCognitoAuthorizer verifies a JWT token for COGNITO_USER_POOLS authorizer type.
func (h *Handler) runCognitoAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	auth *Authorizer,
	cacheKey string,
	ttl time.Duration,
) bool {
	tokenSource := auth.IdentitySource
	if tokenSource == "" {
		tokenSource = defaultIdentitySource
	}

	var tokenStr string

	if headerName, found := strings.CutPrefix(tokenSource, "method.request.header."); found {
		tokenStr = r.Header.Get(headerName)
	} else {
		tokenStr = r.Header.Get("Authorization")
	}

	if tokenStr == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	token, _, parseErr := new(jwt.Parser).ParseUnverified(tokenStr, jwt.MapClaims{})
	if parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: cognito authorizer invalid token", "error", parseErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	*r = *r.WithContext(context.WithValue(r.Context(), ctxKeyClaims, claims))

	if ttl > 0 {
		h.authCache.set(cacheKey, true, ttl)
	}

	return false
}

// authorizerCacheKey builds the cache key for an authorizer invocation.
// TOKEN: authorizerID + ":" + extracted token (using identityValidationExpression if set)
// REQUEST: authorizerID + ":" + method + " " + path (per-request granularity).
func (h *Handler) authorizerCacheKey(r *http.Request, auth *Authorizer, authorizerID string) string {
	if auth.Type != "TOKEN" {
		return authorizerID + ":" + r.Method + " " + r.URL.Path
	}

	token := extractTokenFromIdentitySource(r, auth.IdentitySource)
	token = h.applyIdentityValidation(auth.IdentityValidationExpression, token)

	return authorizerID + ":" + token
}

// applyIdentityValidation normalizes a token using the authorizer's identityValidationExpression.
// Returns the first capture group if present, the full match otherwise, or the original token unchanged.
func (h *Handler) applyIdentityValidation(expr, token string) string {
	if expr == "" {
		return token
	}

	re := h.cachedRegexp(expr)
	if re == nil {
		return token
	}

	m := re.FindStringSubmatch(token)
	if len(m) > 1 {
		return m[1]
	}

	if len(m) == 1 {
		return m[0]
	}

	return token
}

// runRequestValidator enforces request validation rules when a requestValidatorId
// is configured on the method. Returns true if validation failed and the response
// has been written.
func (h *Handler) runRequestValidator(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, validatorID string,
) bool {
	rv, err := h.Backend.GetRequestValidator(apiID, validatorID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: request validator not found",
			"validatorId", validatorID)

		return false // fail open when validator config is missing
	}

	if rv.ValidateRequestBody && r.Body != nil {
		bodyBytes, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyRequestBodyBytes))
		if readErr != nil {
			http.Error(w, "Bad Request: failed to read body", http.StatusBadRequest)

			return true
		}

		// Replace body so downstream handlers can still read it.
		r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))

		if len(bodyBytes) > 0 && !json.Valid(bodyBytes) {
			http.Error(w, "Bad Request: request body must be valid JSON", http.StatusBadRequest)

			return true
		}
	}

	return false
}

// buildAuthorizerEvent constructs the event payload for the Lambda authorizer.
func (h *Handler) buildAuthorizerEvent(
	ctx context.Context, r *http.Request, auth *Authorizer, apiID, stageName string,
) AuthorizerEvent {
	headers := make(map[string]string)
	for k, vs := range r.Header {
		if len(vs) > 0 {
			headers[strings.ToLower(k)] = vs[0]
		}
	}

	qsp := make(map[string]string)
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			qsp[k] = vs[0]
		}
	}

	// Strip internal proxy prefixes so the resource path matches the API definition path.
	resourcePath := r.URL.Path
	prefixes := []string{
		fmt.Sprintf("/restapis/%s/%s/_user_request_", apiID, stageName),
		fmt.Sprintf("/restapis/%s/%s", apiID, stageName),
		fmt.Sprintf("/proxy/%s/%s", apiID, stageName),
		"/" + stageName,
	}

	for _, prefix := range prefixes {
		if after, ok := strings.CutPrefix(resourcePath, prefix); ok {
			resourcePath = after
			if resourcePath == "" {
				resourcePath = "/"
			} else if !strings.HasPrefix(resourcePath, "/") {
				resourcePath = "/" + resourcePath
			}

			break
		}
	}

	region := awsmeta.Region(ctx)
	if region == "" {
		region = config.DefaultRegion
	}

	methodArn := arn.Build("execute-api", region, awsmeta.Account(ctx),
		fmt.Sprintf("%s/%s/%s%s", apiID, stageName, r.Method, resourcePath))

	event := AuthorizerEvent{
		Type:                  auth.Type,
		MethodArn:             methodArn,
		Path:                  resourcePath,
		HTTPMethod:            r.Method,
		Resource:              resourcePath,
		Headers:               headers,
		QueryStringParameters: qsp,
		RequestContext: LambdaProxyContext{
			Stage: stageName,
			APIId: apiID,
		},
	}

	// For TOKEN type: extract token from identity source header.
	if auth.Type == "TOKEN" {
		token := extractTokenFromIdentitySource(r, auth.IdentitySource)
		event.AuthorizationToken = token
		// TOKEN authorizers only need type, token, and methodArn.
		event.Headers = nil
		event.QueryStringParameters = nil
	}

	return event
}

// extractTokenFromIdentitySource extracts the token value from the request
// based on the authorizer's identitySource (e.g. "method.request.header.Authorization").
func extractTokenFromIdentitySource(r *http.Request, identitySource string) string {
	const headerPrefix = "method.request.header."
	if strings.HasPrefix(identitySource, headerPrefix) {
		headerName := identitySource[len(headerPrefix):]

		return r.Header.Get(headerName)
	}

	// Default: try Authorization header.

	return r.Header.Get("Authorization")
}

// isAuthorizerAllowed evaluates the IAM policy document returned by a Lambda authorizer.
// Returns true if at least one Allow statement exists and no explicit Deny overrides it.
func isAuthorizerAllowed(authResp *AuthorizerResponse) bool {
	if authResp.PolicyDocument == nil {
		return false
	}

	allow := false

	for _, stmt := range authResp.PolicyDocument.Statement {
		effect := strings.ToUpper(stmt.Effect)
		if effect == "DENY" {
			return false
		}
		if effect == "ALLOW" {
			allow = true
		}
	}

	return allow
}

// handleAWSProxy handles an AWS_PROXY Lambda integration — the full event is forwarded as-is.
func (h *Handler) handleAWSProxy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName string,
	resource *Resource,
	integration *Integration,
	pathParams map[string]string,
) {
	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return
	}

	event, buildErr := BuildProxyEvent(r, apiID, stageName, resource.Path, r.URL.Path, pathParams)
	if buildErr != nil {
		logger.Load(ctx).ErrorContext(ctx, "APIGateway proxy: failed to build event", "error", buildErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	payload, _ := json.Marshal(event)

	respBytes, _, invokeErr := h.lambda.InvokeFunction(
		ctx,
		ExtractLambdaFunctionName(integration.URI),
		"RequestResponse",
		payload,
	)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: Lambda invocation failed",
			"uri", integration.URI, "error", invokeErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Parse Lambda response.
	var lambdaResp LambdaProxyResponse
	if parseErr := json.Unmarshal(respBytes, &lambdaResp); parseErr != nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes) //nolint:gosec // local emulation: response passthrough is intentional

		return
	}

	for k, v := range lambdaResp.Headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")

	statusCode := lambdaResp.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	var bodyBytes []byte
	if lambdaResp.IsBase64Encoded {
		decoded, decErr := base64.StdEncoding.DecodeString(lambdaResp.Body)
		if decErr == nil {
			bodyBytes = decoded
		} else {
			bodyBytes = []byte(lambdaResp.Body)
		}
	} else {
		bodyBytes = []byte(lambdaResp.Body)
	}

	bodyBytes = maybeCompressResponse(w, r, bodyBytes, h.minCompressSize(apiID))
	w.WriteHeader(statusCode)
	_, _ = w.Write(bodyBytes)
}

// minCompressSize returns the MinimumCompressionSize for the given API (0 = disabled).
func (h *Handler) minCompressSize(apiID string) int {
	api, err := h.Backend.GetRestAPI(apiID)
	if err != nil || api == nil {
		return 0
	}

	return api.MinimumCompressionSize
}

// handleAWSIntegration handles an AWS (non-proxy) Lambda integration using VTL templates.
func (h *Handler) handleAWSIntegration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName string,
	resource *Resource,
	stageVars map[string]string,
	integration *Integration,
) {
	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return
	}

	// Read the raw request body.
	rawBody, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyRequestBodyBytes))
	if readErr != nil {
		var maxErr *http.MaxBytesError
		if errors.As(readErr, &maxErr) {
			http.Error(w, "Request entity too large", http.StatusRequestEntityTooLarge)

			return
		}

		logger.Load(ctx).ErrorContext(ctx, "APIGateway AWS integration: failed to read body", "error", readErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	resourcePath := "/"
	if resource != nil && resource.Path != "" {
		resourcePath = resource.Path
	}

	vtlCtx := VTLContext{
		Body:           string(rawBody),
		RequestID:      r.Header.Get("X-Amzn-Requestid"),
		HTTPMethod:     r.Method,
		ResourcePath:   resourcePath,
		Path:           r.URL.Path,
		Stage:          stageName,
		APIID:          apiID,
		SourceIP:       realClientIP(r),
		UserAgent:      r.Header.Get("User-Agent"),
		StageVariables: stageVars,
	}

	// Apply request mapping template (content-type "application/json" is standard).
	payload := rawBody
	if tpl, ok := integration.RequestTemplates[contentTypeJSON]; ok && tpl != "" {
		rendered := RenderTemplate(tpl, vtlCtx)
		payload = []byte(rendered)
	}

	// Invoke Lambda.
	respBytes, _, invokeErr := h.lambda.InvokeFunction(
		ctx,
		ExtractLambdaFunctionName(integration.URI),
		"RequestResponse",
		payload,
	)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway AWS integration: Lambda invocation failed",
			"uri", integration.URI, "error", invokeErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Apply response mapping template using status-code pattern matching.
	responseBody, statusCode := h.applyResponseTemplate(respBytes, integration, vtlCtx.RequestID)

	responseBody = maybeCompressResponse(w, r, responseBody, h.minCompressSize(apiID))
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	_, _ = w.Write(responseBody) //nolint:gosec // local emulation: response passthrough is intentional
}

// applyResponseTemplate selects the best-matching integration response by status code pattern
// (using regex selectionPattern), applies VTL response template and contentHandling conversion,
// and returns the rendered body and HTTP status code. Falls back to the raw response bytes and 200 if no match.
func (h *Handler) applyResponseTemplate(respBytes []byte, integration *Integration, requestID string) ([]byte, int) {
	if integration.IntegrationResponses == nil {
		return respBytes, http.StatusOK
	}

	// Try to find a matching integration response by selectionPattern (regex) against respBytes.
	// If no pattern matches, fall back to the "default" or "200" entry.
	ir := h.matchIntegrationResponse(integration.IntegrationResponses, string(respBytes))
	if ir == nil {
		return respBytes, http.StatusOK
	}

	statusCode := http.StatusOK
	if sc := parseStatusCode(ir.StatusCode); sc > 0 {
		statusCode = sc
	}

	body := respBytes
	tpl, ok := ir.ResponseTemplates[contentTypeJSON]

	if ok && tpl != "" {
		respVTLCtx := VTLContext{
			Body:      string(respBytes),
			RequestID: requestID,
		}
		body = []byte(RenderTemplate(tpl, respVTLCtx))
	}

	body = applyContentHandling(body, ir.ContentHandling)

	return body, statusCode
}

// applyContentHandling converts body bytes according to the AWS contentHandling setting.
// CONVERT_TO_BINARY: base64-decode the body (UTF-8 string → binary bytes).
// CONVERT_TO_TEXT:   base64-encode the body (binary bytes → base64 string).
// Empty string: pass through unchanged.
func applyContentHandling(body []byte, handling string) []byte {
	switch handling {
	case "CONVERT_TO_BINARY":
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(body)))
		if err != nil {
			return body
		}

		return decoded
	case "CONVERT_TO_TEXT":
		encoded := base64.StdEncoding.EncodeToString(body)

		return []byte(encoded)
	default:

		return body
	}
}

// matchIntegrationResponse finds the best-matching IntegrationResponse entry for the given body.
// Priority:
//  1. An entry whose selectionPattern regex matches the body (first match wins).
//  2. The "default" entry (empty selectionPattern treated as catch-all).
//  3. The "200" entry if it has no selectionPattern.
func (h *Handler) matchIntegrationResponse(
	responses map[string]*IntegrationResponse,
	body string,
) *IntegrationResponse {
	var defaultEntry *IntegrationResponse

	for _, ir := range responses {
		if ir == nil {
			continue
		}

		pat := ir.SelectionPattern
		if pat == "" {
			// Treat entries without a selection pattern as the default/catch-all.
			defaultEntry = ir

			continue
		}

		re := h.cachedRegexp(pat)
		if re == nil {
			continue
		}

		if re.MatchString(body) {
			return ir
		}
	}

	if defaultEntry != nil {
		return defaultEntry
	}

	// No pattern and no default: return nil.

	return nil
}

// cachedRegexp returns a compiled regexp for the pattern, using the handler's cache.
// Returns nil if the pattern does not compile.
func (h *Handler) cachedRegexp(pattern string) *regexp.Regexp {
	if v, ok := h.selRegexpCache.Load(pattern); ok {
		re, _ := v.(*regexp.Regexp)

		return re
	}

	re, compileErr := regexp.Compile(pattern)
	if compileErr != nil {
		h.selRegexpCache.Store(pattern, (*regexp.Regexp)(nil))

		return nil
	}

	h.selRegexpCache.Store(pattern, re)

	return re
}

// writeCORSPreflight writes an HTTP 200 response with CORS preflight headers.
func (h *Handler) writeCORSPreflight(w http.ResponseWriter, r *http.Request, cors *CorsConfiguration) {
	h.addCORSHeaders(w, r, cors)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// addCORSHeaders adds Access-Control-* headers to the response based on the CorsConfiguration.
func (h *Handler) addCORSHeaders(w http.ResponseWriter, r *http.Request, cors *CorsConfiguration) {
	origin := r.Header.Get("Origin")
	allowed := corsOriginAllowed(origin, cors.AllowOrigins)
	if allowed != "" {
		w.Header().Set("Access-Control-Allow-Origin", allowed)
	}

	if len(cors.AllowMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(cors.AllowMethods, ", "))
	}

	if len(cors.AllowHeaders) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(cors.AllowHeaders, ", "))
	}

	if len(cors.ExposeHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(cors.ExposeHeaders, ", "))
	}

	if cors.MaxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(cors.MaxAge))
	}
}

// maybeCompressResponse gzip-compresses body if the request accepts gzip encoding and body
// length meets the minimumCompressionSize threshold (0 means no compression).
// Sets Content-Encoding: gzip on the response header when compression is applied.
func maybeCompressResponse(
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	minCompressSize int,
) []byte {
	if minCompressSize <= 0 || len(body) < minCompressSize {
		return body
	}

	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		return body
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(body); err != nil {
		return body
	}

	if err := gz.Close(); err != nil {
		return body
	}

	w.Header().Set("Content-Encoding", "gzip")

	return buf.Bytes()
}

// corsOriginAllowed returns the effective Access-Control-Allow-Origin value for the given origin.
// Returns "*" if origins includes "*", the exact origin if it matches, or empty if not allowed.
func corsOriginAllowed(origin string, allowOrigins []string) string {
	for _, allowed := range allowOrigins {
		if allowed == "*" {
			return "*"
		}

		if allowed == origin {
			return origin
		}
	}

	return ""
}

// handleHTTPProxy forwards the request to the target URI specified in the integration.
// Both HTTP and HTTP_PROXY integration types are handled identically: the request
// is forwarded as-is and the upstream response is returned directly to the caller.
func (h *Handler) handleHTTPProxy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	integration *Integration,
) {
	//nolint:gosec // local emulation: integration URI is test-configured
	targetReq, err := http.NewRequestWithContext(
		ctx,
		r.Method,
		integration.URI,
		r.Body,
	)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway HTTP proxy: bad integration URI",
			"uri", integration.URI, "error", err)
		http.Error(w, "Bad integration URI", http.StatusBadGateway)

		return
	}

	// Merge query parameters from the integration URI with the incoming request's query string.
	// This preserves any required query params baked into the integration URI.
	mergedQuery := targetReq.URL.Query()
	for key, values := range r.URL.Query() {
		for _, value := range values {
			mergedQuery.Add(key, value)
		}
	}
	targetReq.URL.RawQuery = mergedQuery.Encode()
	for k, vs := range r.Header {
		for _, v := range vs {
			targetReq.Header.Add(k, v)
		}
	}

	// Apply integration RequestParameters mappings (e.g. forward a method header as an integration header).
	if len(integration.RequestParameters) > 0 {
		applyIntegrationRequestParams(r, targetReq, integration.RequestParameters)
	}

	client := h.getHTTPClient()

	//nolint:gosec // local emulation: integration URI is test-configured
	resp, doErr := client.Do(targetReq)
	if doErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway HTTP proxy: upstream request failed",
			"uri", integration.URI, "error", doErr)
		http.Error(w, "Upstream request failed", http.StatusBadGateway)

		return
	}

	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleMockIntegration returns a static response configured on the integration.
// It evaluates the first integrationResponse entry keyed by its status code.
// If no integrationResponses are configured, it defaults to HTTP 200 with an empty body.
func (h *Handler) handleMockIntegration(w http.ResponseWriter, integration *Integration) {
	statusCode, body, ir := mockResponseWithIR(integration)

	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Apply ResponseParameters from IntegrationResponse as response headers.
	if ir != nil {
		applyIntegrationResponseParams(w, ir.ResponseParameters)
	}

	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(body)) //nolint:gosec // local emulation: mock integration body is test-configured
}

// mockResponseWithIR resolves the status code, body, and integration response for a MOCK integration.
func mockResponseWithIR(integration *Integration) (int, string, *IntegrationResponse) {
	statusCode := http.StatusOK

	ir := mockIntegrationResponse(integration)
	if ir == nil {
		return statusCode, "", nil
	}

	if sc := parseStatusCode(ir.StatusCode); sc > 0 {
		statusCode = sc
	}

	body := ""
	if ir.ResponseTemplates != nil {
		body = ir.ResponseTemplates["application/json"]
	}

	return statusCode, body, ir
}

// applyIntegrationResponseParams applies integration response parameter mappings as HTTP response headers.
// Parameters of the form "method.response.header.{name}: {value}" set response headers.
// The value can be "integration.response.header.{name}" (reads from integration headers, not yet
// available for MOCK integrations so treated as static) or a static string.
func applyIntegrationResponseParams(w http.ResponseWriter, params map[string]string) {
	const methodRespPrefix = "method.response.header."

	for dest, src := range params {
		if !strings.HasPrefix(dest, methodRespPrefix) {
			continue
		}

		headerName := dest[len(methodRespPrefix):]
		if headerName == "" {
			continue
		}

		// Resolve static values (or simplified integration header echo).
		value := resolveResponseParamSource(src)
		if value != "" {
			w.Header().Set(headerName, value)
		}
	}
}

// resolveResponseParamSource resolves an integration response parameter value.
// Static strings are returned as-is. integration.response.header.{name} references
// are returned as the raw name (for MOCK integrations there is no actual response to read from).
func resolveResponseParamSource(src string) string {
	const integRespPrefix = "integration.response.header."
	if strings.HasPrefix(src, integRespPrefix) {
		return src[len(integRespPrefix):]
	}

	return src
}

// mockIntegrationResponse returns the "200" integration response, if configured.
func mockIntegrationResponse(integration *Integration) *IntegrationResponse {
	if integration.IntegrationResponses == nil {
		return nil
	}

	ir, ok := integration.IntegrationResponses["200"]
	if !ok || ir == nil {
		return nil
	}

	return ir
}

// parseStatusCode converts a status-code string to an int; returns 0 on error.
func parseStatusCode(s string) int {
	const (
		minHTTP = 100
		maxHTTP = 599
		decBase = 10
	)

	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*decBase + int(c-'0')
	}

	if n < minHTTP || n > maxHTTP {
		return 0
	}

	return n
}

// findMatchingResource finds a resource whose path pattern matches the request path.
// It supports exact path segments, single-segment path variables ({param}), and
// greedy path variables ({proxy+} or {param+}). The most-specific match wins.
// Returns the matched resource and extracted path parameters, or nil if no match.
// Stage name prefix is stripped from the request path before matching.
func findMatchingResource(resources []Resource, requestPath, stageName string) (*Resource, map[string]string) {
	// Strip stage prefix: /{stageName}/... -> /...
	stripped := requestPath
	prefix := "/" + stageName
	if strings.HasPrefix(requestPath, prefix) {
		stripped = requestPath[len(prefix):]
	}

	if stripped == "" {
		stripped = "/"
	}

	trie := newResourcePathTrie()
	for _, resource := range resources {
		trie.insert(resource)
	}

	return trie.match(stripped)
}

type resourcePathTrie struct {
	root *resourcePathTrieNode
}

type resourcePathTrieNode struct {
	literalChildren map[string]*resourcePathTrieNode
	paramChild      *resourcePathTrieNode
	greedyChild     *resourcePathTrieNode
	resource        *Resource
	paramName       string
	greedyParamName string
}

func newResourcePathTrie() *resourcePathTrie {
	return &resourcePathTrie{
		root: &resourcePathTrieNode{
			literalChildren: make(map[string]*resourcePathTrieNode),
		},
	}
}

func (t *resourcePathTrie) insert(resource Resource) {
	segs := splitPathSegs(resource.Path)
	node := t.root

	for i, seg := range segs {
		isLast := i == len(segs)-1

		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "+}") {
			if !isLast {
				return
			}

			if node.greedyChild == nil {
				node.greedyChild = &resourcePathTrieNode{
					literalChildren: make(map[string]*resourcePathTrieNode),
					greedyParamName: seg[1 : len(seg)-2],
				}
			}

			node = node.greedyChild

			continue
		}

		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			if node.paramChild == nil {
				node.paramChild = &resourcePathTrieNode{
					literalChildren: make(map[string]*resourcePathTrieNode),
					paramName:       seg[1 : len(seg)-1],
				}
			}

			node = node.paramChild

			continue
		}

		child, ok := node.literalChildren[seg]
		if !ok {
			child = &resourcePathTrieNode{
				literalChildren: make(map[string]*resourcePathTrieNode),
			}
			node.literalChildren[seg] = child
		}

		node = child
	}

	resourceCopy := resource
	node.resource = &resourceCopy
}

func (t *resourcePathTrie) match(path string) (*Resource, map[string]string) {
	return t.root.match(splitPathSegs(path), 0, map[string]string{})
}

func (n *resourcePathTrieNode) match(
	urlSegs []string,
	index int,
	params map[string]string,
) (*Resource, map[string]string) {
	if index == len(urlSegs) {
		if n.resource == nil {
			return nil, nil
		}

		return n.resource, params
	}

	seg := urlSegs[index]

	if child, ok := n.literalChildren[seg]; ok {
		if res, matchedParams := child.match(urlSegs, index+1, params); res != nil {
			return res, matchedParams
		}
	}

	if n.paramChild != nil {
		nextParams := clonePathParams(params)
		nextParams[n.paramChild.paramName] = seg

		if res, matchedParams := n.paramChild.match(urlSegs, index+1, nextParams); res != nil {
			return res, matchedParams
		}
	}

	if n.greedyChild != nil && n.greedyChild.resource != nil {
		nextParams := clonePathParams(params)
		nextParams[n.greedyChild.greedyParamName] = "/" + strings.Join(urlSegs[index:], "/")

		return n.greedyChild.resource, nextParams
	}

	return nil, nil
}

func clonePathParams(params map[string]string) map[string]string {
	clone := make(map[string]string, len(params)+1)
	maps.Copy(clone, params)

	return clone
}

// splitPathSegs splits a URL path into non-empty segments, ignoring leading and trailing slashes.
func splitPathSegs(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return []string{}
	}

	return strings.Split(trimmed, "/")
}

// ExtractLambdaFunctionName extracts a Lambda function name (or short ARN) from either:
//   - A plain function name: "my-function"
//   - A Lambda ARN: "arn:aws:lambda:region:account:function:my-function"
//   - An API Gateway invoke URI containing
//     "arn:aws:apigateway:region:lambda:path/.../functions/{lambdaArn}/invocations"
//
// Returns the input unchanged if it does not match any known pattern.
func ExtractLambdaFunctionName(uri string) string {
	// API Gateway integration URI: extract the Lambda ARN embedded in the path.
	// Format: arn:aws:apigateway:...:lambda:path/2015-03-31/functions/{lambdaArn}/invocations
	const invocations = "/invocations"
	if idx := strings.LastIndex(uri, invocations); idx != -1 {
		// Everything before "/invocations" is the Lambda ARN.
		lambdaARN := uri[:idx]
		// The Lambda ARN may itself be within a path like ".../functions/{arn}"
		const functionsPrefix = "/functions/"
		if fi := strings.LastIndex(lambdaARN, functionsPrefix); fi != -1 {
			lambdaARN = lambdaARN[fi+len(functionsPrefix):]
		}

		return ExtractLambdaFunctionName(lambdaARN)
	}

	// Lambda ARN: "arn:aws:lambda:{region}:{account}:function:{name}" (with optional qualifier).
	// Extract the name (and optional qualifier) after ":function:".
	// Use ":function:" (with leading colon) to avoid matching "function:" inside a function name.
	const functionSegment = ":function:"
	if fi := strings.LastIndex(uri, functionSegment); fi != -1 {
		return uri[fi+len(functionSegment):]
	}

	// Plain name or already-resolved value — return as-is.

	return uri
}

// applyIntegrationRequestParams applies integration request parameter mappings to the outgoing
// HTTP request. Mappings are of the form:
//
//	integration.request.{type}.{name}: method.request.{type}.{name}
//
// where type is header, querystring, or path. Only the header and querystring destination types
// are applied here (path substitution is handled in the URI template). Static string values
// (not starting with "method.request.") are also supported.
func applyIntegrationRequestParams(incoming *http.Request, outgoing *http.Request, params map[string]string) {
	outQuery := outgoing.URL.Query()

	for dest, src := range params {
		value := resolveRequestParamSource(incoming, src)
		if value == "" {
			continue
		}

		// Parse destination: "integration.request.{type}.{name}"
		const integPrefix = "integration.request."
		if !strings.HasPrefix(dest, integPrefix) {
			continue
		}

		rest := dest[len(integPrefix):]
		paramType, paramName, ok := strings.Cut(rest, ".")
		if !ok {
			continue
		}

		switch paramType {
		case paramLocationHeader:
			outgoing.Header.Set(paramName, value)
		case paramLocationQuery:
			outQuery.Set(paramName, value)
		}
	}

	outgoing.URL.RawQuery = outQuery.Encode()
}

// realClientIP extracts the client IP from X-Forwarded-For or RemoteAddr.
func realClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		host, _, _ := strings.Cut(xff, ",")

		return strings.TrimSpace(host)
	}

	host := r.RemoteAddr
	if idx := strings.LastIndexByte(host, ':'); idx >= 0 {
		host = host[:idx]
	}

	return strings.Trim(host, "[]")
}

// resolveRequestParamSource resolves a parameter source expression against the incoming request.
// Supported formats:
//   - method.request.header.{name}
//   - method.request.querystring.{name}
//   - method.request.path.{name}   (returns the raw path segment from the URL)
//   - Any other string is treated as a static value.
func resolveRequestParamSource(r *http.Request, src string) string {
	const methodPrefix = "method.request."
	if !strings.HasPrefix(src, methodPrefix) {
		return src
	}

	rest := src[len(methodPrefix):]
	srcType, srcName, ok := strings.Cut(rest, ".")
	if !ok {
		return ""
	}

	switch srcType {
	case paramLocationHeader:

		return r.Header.Get(srcName)
	case paramLocationQuery:

		return r.URL.Query().Get(srcName)
	case paramLocationPath:
		// Return the named path segment from the raw URL path.
		// This is a best-effort approximation: the actual value depends on route matching.
		segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		for _, seg := range segments {
			// Caller must use exact segment value — path parameter names don't map here.
			if seg == srcName {
				return seg
			}
		}

		return ""
	}

	return ""
}
