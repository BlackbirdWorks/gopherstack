package apigateway

import (
	"container/list"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// defaultAuthorizerTTL is the default authorizer result cache TTL (AWS default: 300 s).
const defaultAuthorizerTTL = 300 * time.Second

const defaultAuthorizerCacheMaxEntries = 1024

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
	ResourcePath string `json:"resourcePath"`
	HTTPMethod   string `json:"httpMethod"`
	Stage        string `json:"stage"`
	APIId        string `json:"apiId"`
	RequestID    string `json:"requestId,omitempty"`
}

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
		bodyBytes, err := io.ReadAll(r.Body)
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
	switch integration.Type {
	case "AWS_PROXY":
		h.handleAWSProxy(ctx, w, r, apiID, stageName, resource, integration, pathParams)
	case "AWS":
		h.handleAWSIntegration(ctx, w, r, integration)
	case "HTTP", "HTTP_PROXY":
		h.handleHTTPProxy(ctx, w, r, integration)
	case "MOCK":
		h.handleMockIntegration(w, integration)
	default:
		http.Error(w, "Unsupported or unknown integration type for stage URL", http.StatusNotImplemented)
	}
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

	// Build the authorizer event based on type.
	event := h.buildAuthorizerEvent(r, auth, apiID, stageName)

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

// authorizerCacheKey builds the cache key for an authorizer invocation.
// TOKEN: authorizerID + ":" + token value (per-token granularity)
// REQUEST: authorizerID + ":" + method + " " + path (per-request granularity).
func (h *Handler) authorizerCacheKey(r *http.Request, auth *Authorizer, authorizerID string) string {
	if auth.Type == "TOKEN" {
		token := extractTokenFromIdentitySource(r, auth.IdentitySource)

		return authorizerID + ":" + token
	}

	return authorizerID + ":" + r.Method + " " + r.URL.Path
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
		bodyBytes, readErr := io.ReadAll(r.Body)
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
func (h *Handler) buildAuthorizerEvent(r *http.Request, auth *Authorizer, apiID, stageName string) AuthorizerEvent {
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

	methodArn := fmt.Sprintf("arn:aws:execute-api:us-east-1:000000000000:%s/%s/%s%s",
		apiID, stageName, r.Method, resourcePath)

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

	w.WriteHeader(statusCode)

	body := lambdaResp.Body
	if lambdaResp.IsBase64Encoded {
		decoded, decErr := base64.StdEncoding.DecodeString(body)
		if decErr == nil {
			_, _ = w.Write(decoded)
		} else {
			_, _ = w.Write([]byte(body))
		}
	} else {
		_, _ = w.Write([]byte(body))
	}
}

// handleAWSIntegration handles an AWS (non-proxy) Lambda integration using VTL templates.
func (h *Handler) handleAWSIntegration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	integration *Integration,
) {
	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return
	}

	// Read the raw request body.
	rawBody, readErr := io.ReadAll(r.Body)
	if readErr != nil {
		logger.Load(ctx).ErrorContext(ctx, "APIGateway AWS integration: failed to read body", "error", readErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	vtlCtx := VTLContext{
		Body:      string(rawBody),
		RequestID: r.Header.Get("X-Amzn-Requestid"),
	}

	// Apply request mapping template (content-type "application/json" is standard).
	payload := rawBody
	if tpl, ok := integration.RequestTemplates["application/json"]; ok && tpl != "" {
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
	responseBody, statusCode := applyResponseTemplate(respBytes, integration, vtlCtx.RequestID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	_, _ = w.Write(responseBody) //nolint:gosec // local emulation: response passthrough is intentional
}

// applyResponseTemplate selects the best-matching integration response by status code pattern
// (using regex selectionPattern), applies VTL response template, and returns the rendered
// body and HTTP status code. Falls back to the raw response bytes and 200 if no match.
func applyResponseTemplate(respBytes []byte, integration *Integration, requestID string) ([]byte, int) {
	if integration.IntegrationResponses == nil {
		return respBytes, http.StatusOK
	}

	// Try to find a matching integration response by selectionPattern (regex) against respBytes.
	// If no pattern matches, fall back to the "default" or "200" entry.
	ir := matchIntegrationResponse(integration.IntegrationResponses, string(respBytes))
	if ir == nil {
		return respBytes, http.StatusOK
	}

	statusCode := http.StatusOK
	if sc := parseStatusCode(ir.StatusCode); sc > 0 {
		statusCode = sc
	}

	tpl, ok := ir.ResponseTemplates["application/json"]
	if !ok || tpl == "" {
		return respBytes, statusCode
	}

	respVTLCtx := VTLContext{
		Body:      string(respBytes),
		RequestID: requestID,
	}

	return []byte(RenderTemplate(tpl, respVTLCtx)), statusCode
}

// matchIntegrationResponse finds the best-matching IntegrationResponse entry for the given body.
// Priority:
//  1. An entry whose selectionPattern regex matches the body (first match wins).
//  2. The "default" entry (empty selectionPattern treated as catch-all).
//  3. The "200" entry if it has no selectionPattern.
func matchIntegrationResponse(
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

		re, err := regexp.Compile(pat)
		if err != nil {
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
	statusCode, body := mockResponse(integration)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(body)) //nolint:gosec // local emulation: mock integration body is test-configured
}

// mockResponse resolves the status code and body for a MOCK integration.
func mockResponse(integration *Integration) (int, string) {
	statusCode := http.StatusOK

	ir := mockIntegrationResponse(integration)
	if ir == nil {
		return statusCode, ""
	}

	if sc := parseStatusCode(ir.StatusCode); sc > 0 {
		statusCode = sc
	}

	body := ""
	if ir.ResponseTemplates != nil {
		body = ir.ResponseTemplates["application/json"]
	}

	return statusCode, body
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
