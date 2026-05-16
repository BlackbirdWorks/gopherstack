package apigateway

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// defaultAuthorizerTTL is the default authorizer result cache TTL (AWS default: 300 s).
const defaultAuthorizerTTL = 300 * time.Second

const defaultAuthorizerCacheMaxEntries = 1024

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
	context   map[string]any
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

// get returns (allowed, context, found) for the given cache key.
func (c *authorizerCache) get(key string) (bool, map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return false, nil, false
	}

	e, ok := elem.Value.(authorizerCacheEntry)
	if !ok {
		c.removeElement(elem)

		return false, nil, false
	}

	if time.Now().After(e.expiresAt) {
		c.removeElement(elem)

		return false, nil, false
	}

	c.order.MoveToFront(elem)

	return e.allowed, e.context, true
}

// set stores the result for the given cache key with a TTL.
func (c *authorizerCache) set(key string, allowed bool, context map[string]any, ttl time.Duration) {
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
			entry.context = context
			entry.expiresAt = time.Now().Add(ttl)
			elem.Value = entry
			c.order.MoveToFront(elem)

			return
		}
	}

	elem := c.order.PushFront(authorizerCacheEntry{
		key:       key,
		allowed:   allowed,
		context:   context,
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
		start := time.Now()
		requestID := generateRequestID()

		// Wrap the writer to capture status/size for access logging.
		alw := newAccessLogWriter(w)

		defer func() {
			h.emitAccessLog(apiID, stageName, r, alw, requestID, time.Since(start))
		}()

		// Evaluate REST API resource policy before dispatching.
		if denied := h.checkResourcePolicy(apiID, r, alw); denied {
			return
		}

		resource, pathParams, ok := h.resolveResource(ctx, apiID, stageName, r, alw)
		if !ok {
			return
		}

		// Apply method-level access controls (authorizer + API key + request validator).
		if denied := h.applyMethodControls(ctx, alw, r, apiID, stageName, resource.ID); denied {
			return
		}

		integration, ok := h.resolveIntegration(apiID, resource, r, alw)
		if !ok {
			return
		}

		h.dispatchIntegration(ctx, alw, r, apiID, stageName, resource, integration, pathParams)
	}
}

// checkResourcePolicy evaluates the REST API resource policy. Returns true (denied) when
// the policy rejects the request and the response has already been written.
func (h *Handler) checkResourcePolicy(apiID string, r *http.Request, w http.ResponseWriter) bool {
	api, err := h.Backend.GetRestAPI(apiID)
	if err != nil || api == nil || api.Policy == "" {
		return false
	}

	if !evaluateResourcePolicy(api.Policy, r) {
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	return false
}

// resolveResource finds the matching resource for the request path and handles CORS.
// Returns (resource, pathParams, true) on success; (nil, nil, false) when the response was written.
func (h *Handler) resolveResource(
	ctx context.Context,
	apiID, stageName string,
	r *http.Request,
	w http.ResponseWriter,
) (*Resource, map[string]string, bool) {
	resources, _, err := h.Backend.GetResources(apiID, "", 0)
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "APIGateway proxy: failed to get resources", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return nil, nil, false
	}

	resource, pathParams := findMatchingResource(resources, r.URL.Path, stageName)
	if resource == nil {
		http.NotFound(w, r)

		return nil, nil, false
	}

	if r.Method == http.MethodOptions && resource.CorsConfiguration != nil {
		h.writeCORSPreflight(w, r, resource.CorsConfiguration)

		return nil, nil, false
	}

	if resource.CorsConfiguration != nil {
		h.addCORSHeaders(w, r, resource.CorsConfiguration)
	}

	return resource, pathParams, true
}

// resolveIntegration looks up the integration for a resource+method, falling back to ANY.
// Returns (integration, true) on success; (nil, false) when the response was written.
func (h *Handler) resolveIntegration(
	apiID string,
	resource *Resource,
	r *http.Request,
	w http.ResponseWriter,
) (*Integration, bool) {
	integration, err := h.Backend.GetIntegration(apiID, resource.ID, r.Method)
	if err != nil {
		integration, err = h.Backend.GetIntegration(apiID, resource.ID, "ANY")
		if err != nil {
			http.NotFound(w, r)

			return nil, false
		}
	}

	return integration, true
}

// applyMethodControls runs the authorizer, request validator, and API key check for the matched method.
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

	var authContext map[string]any
	if method.AuthorizerID != "" {
		denied, ac := h.runAuthorizer(ctx, w, r, apiID, stageName, method.AuthorizerID)
		if denied {
			return true
		}
		authContext = ac
	}

	if method.APIKeyRequired {
		if h.runAPIKeyCheck(ctx, w, r, apiID, authContext) {
			return true
		}
	}

	if method.RequestValidatorID != "" {
		if h.runRequestValidator(ctx, w, r, apiID, method.RequestValidatorID, method) {
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
		h.handleAWSIntegration(ctx, w, r, apiID, integration)
	case "HTTP", "HTTP_PROXY":
		h.handleHTTPProxy(ctx, w, r, integration)
	case "MOCK":
		h.handleMockIntegration(w, integration)
	default:
		http.Error(w, "Unsupported or unknown integration type for stage URL", http.StatusNotImplemented)
	}
}

// stageVars fetches stage variables for the given API/stage (returns nil on error).
// Canary StageVariableOverrides are applied when this request is routed to the canary.
func (h *Handler) stageVars(apiID, stageName string) map[string]string {
	stage, err := h.Backend.GetStage(apiID, stageName)
	if err != nil || stage == nil {
		return nil
	}

	if stage.CanarySettings == nil || stage.CanarySettings.PercentTraffic <= 0 {
		return stage.Variables
	}

	if !shouldRouteToCanary(stage.CanarySettings.PercentTraffic) {
		return stage.Variables
	}

	// Limitation: only stage variable overrides are applied; CanarySettings.DeploymentID is not
	// used to select a different deployment. Full multi-deployment canary routing is not implemented.
	overrides := stage.CanarySettings.StageVariableOverrides
	if len(overrides) == 0 {
		return stage.Variables
	}

	merged := make(map[string]string, len(stage.Variables)+len(overrides))
	maps.Copy(merged, stage.Variables)
	maps.Copy(merged, overrides)

	return merged
}

// shouldRouteToCanary returns true with probability percentTraffic/100.
// Used to implement weighted canary traffic splitting.
func shouldRouteToCanary(percentTraffic float64) bool {
	if percentTraffic >= canaryMaxPercent {
		return true
	}

	if percentTraffic <= 0 {
		return false
	}

	const uint64Max = 1 << 64 // maximum value of uint64 + 1 (as float64)
	var b [8]byte
	_, _ = rand.Read(b[:])
	sample := float64(binary.BigEndian.Uint64(b[:])) / uint64Max

	return sample*canaryMaxPercent < percentTraffic
}

const canaryMaxPercent = 100.0

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

// runAuthorizer invokes the Lambda authorizer and returns (denied bool, authContext map[string]any).
// When denied=true the response has already been written with a 4xx status.
// authContext carries the Lambda authorizer context (e.g. usageIdentifierKey for AUTHORIZER apiKeySource).
func (h *Handler) runAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName, authorizerID string,
) (bool, map[string]any) {
	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return true, nil
	}

	auth, err := h.Backend.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer not found", "authorizerId", authorizerID)
		http.Error(w, "Authorizer configuration error", http.StatusInternalServerError)

		return true, nil
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
		if allowed, cachedContext, found := h.authCache.get(cacheKey); found {
			if !allowed {
				http.Error(w, "Forbidden", http.StatusForbidden)

				return true, nil
			}

			return false, cachedContext
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

		return true, nil
	}

	// Parse the authorizer response (IAM policy document).
	var authResp AuthorizerResponse
	if parseErr := json.Unmarshal(respBytes, &authResp); parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: failed to parse authorizer response", "error", parseErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true, nil
	}

	// Evaluate the policy document to determine allow/deny.
	allowed := isAuthorizerAllowed(&authResp)
	h.authCache.set(cacheKey, allowed, authResp.Context, ttl)

	if !allowed {
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true, nil
	}

	return false, authResp.Context
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
	method *Method,
) bool {
	rv, err := h.Backend.GetRequestValidator(apiID, validatorID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: request validator not found",
			"validatorId", validatorID)

		return false // fail open when validator config is missing
	}

	if rv.ValidateRequestBody && r.Body != nil {
		return h.validateRequestBody(ctx, w, r, apiID, method)
	}

	return false
}

// validateRequestBody reads the request body, enforces JSON validity, and delegates model validation.
func (h *Handler) validateRequestBody(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID string,
	method *Method,
) bool {
	bodyBytes, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyRequestBodyBytes))
	if readErr != nil {
		http.Error(w, "Bad Request: failed to read body", http.StatusBadRequest)

		return true
	}

	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	if len(bodyBytes) > 0 && !json.Valid(bodyBytes) {
		http.Error(w, "Bad Request: request body must be valid JSON", http.StatusBadRequest)

		return true
	}

	if len(bodyBytes) > 0 {
		return h.runModelValidation(ctx, w, r, apiID, method, bodyBytes)
	}

	return false
}

// runModelValidation resolves the request model by Content-Type and validates the body.
// Returns true if validation failed and the response has already been written.
func (h *Handler) runModelValidation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID string,
	method *Method,
	bodyBytes []byte,
) bool {
	if method == nil || len(method.RequestModels) == 0 {
		return false
	}

	ct := normaliseContentType(r.Header.Get("Content-Type"))
	modelName, ok := method.RequestModels[ct]
	if !ok {
		modelName, ok = method.RequestModels[contentTypeJSON]
	}

	if !ok || modelName == "" {
		return false
	}

	return h.validateBodyAgainstModel(ctx, w, apiID, modelName, bodyBytes)
}

// normaliseContentType strips parameters (e.g. "; charset=utf-8") from a Content-Type value.
func normaliseContentType(ct string) string {
	if ct == "" {
		return contentTypeJSON
	}

	if before, _, found := strings.Cut(ct, ";"); found {
		return strings.TrimSpace(before)
	}

	return ct
}

// validateBodyAgainstModel validates the request body JSON against the "required" fields
// declared in the named model's JSON Schema. Returns true if validation failed.
// Implements a subset of JSON Schema draft 4 "required" array enforcement.
func (h *Handler) validateBodyAgainstModel(
	ctx context.Context,
	w http.ResponseWriter,
	apiID, modelName string,
	bodyBytes []byte,
) bool {
	model, err := h.Backend.GetModel(apiID, modelName)
	if err != nil || model == nil || model.Schema == "" {
		return false // no schema → skip validation
	}

	required, parseErr := extractRequiredFields(model.Schema)
	if parseErr != nil || len(required) == 0 {
		return false
	}

	// Parse body as object.
	var bodyObj map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(bodyBytes, &bodyObj); unmarshalErr != nil {
		return false // not an object — already checked JSON validity above
	}

	for _, field := range required {
		if _, present := bodyObj[field]; !present {
			msg := fmt.Sprintf("Bad Request: body missing required field %q (model %q)", field, modelName)
			http.Error(w, msg, http.StatusBadRequest)
			logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: model validation failed",
				"field", field, "model", modelName)

			return true
		}
	}

	return false
}

// extractRequiredFields parses the "required" array from a JSON Schema string.
// Returns the list of required field names, or nil if absent.
func extractRequiredFields(schema string) ([]string, error) {
	var s struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal([]byte(schema), &s); err != nil {
		return nil, err
	}

	return s.Required, nil
}

// runAPIKeyCheck verifies the API key when method.APIKeyRequired is true.
// Returns true if the request should be denied (response already written).
// authContext is the Lambda authorizer response context; used when apiKeySource == "AUTHORIZER".
func (h *Handler) runAPIKeyCheck(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID string,
	authContext map[string]any,
) bool {
	api, err := h.Backend.GetRestAPI(apiID)
	if err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	keyValue := resolveAPIKeyValue(r, api, authContext)

	if keyValue == "" {
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	apiKeys, listErr := h.Backend.GetAPIKeys()
	if listErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: failed to list API keys", "error", listErr)
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	for _, k := range apiKeys {
		if k.Enabled && k.Value == keyValue {
			return false
		}
	}

	http.Error(w, "Forbidden", http.StatusForbidden)

	return true
}

const (
	apiKeySourceHeader     = "HEADER"
	apiKeySourceAuthorizer = "AUTHORIZER"
)

// resolveAPIKeyValue extracts the API key value from the request based on the configured source.
// HEADER source: uses the x-api-key request header.
// AUTHORIZER source: uses usageIdentifierKey from the Lambda authorizer context.
func resolveAPIKeyValue(r *http.Request, api *RestAPI, authContext map[string]any) string {
	source := apiKeySourceHeader
	if api != nil && api.APIKeySource == apiKeySourceAuthorizer {
		source = apiKeySourceAuthorizer
	}

	if source == apiKeySourceAuthorizer {
		if authContext != nil {
			if v, ok := authContext["usageIdentifierKey"].(string); ok {
				return v
			}
		}

		return ""
	}

	return r.Header.Get("X-Api-Key")
}

// resourcePolicyDoc is a simplified IAM policy document attached to a REST API.
type resourcePolicyDoc struct {
	Statement []resourcePolicyStatement `json:"Statement"`
}

// resourcePolicyStatement is a single statement in a resource policy.
type resourcePolicyStatement struct {
	Condition map[string]map[string]any `json:"Condition,omitempty"`
	Effect    string                    `json:"Effect"`
	Action    json.RawMessage           `json:"Action"`
	Resource  json.RawMessage           `json:"Resource"`
	Principal json.RawMessage           `json:"Principal"`
}

// evaluateResourcePolicy parses the IAM policy attached to a REST API and evaluates
// whether the request is permitted. Returns true (allow) or false (deny).
// An empty or missing policy allows all requests; an unparseable policy denies (AWS semantics).
func evaluateResourcePolicy(policy string, r *http.Request) bool {
	if policy == "" {
		return true
	}

	var doc resourcePolicyDoc
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return false // malformed policy: deny per AWS semantics
	}

	sourceIP := extractClientIP(r)
	allow := false

	for _, stmt := range doc.Statement {
		if !policyStatementMatchesRequest(stmt, sourceIP) {
			continue
		}

		switch strings.ToUpper(stmt.Effect) {
		case "DENY":
			return false
		case "ALLOW":
			allow = true
		}
	}

	return allow
}

// policyStatementMatchesRequest checks whether a policy statement applies to the request.
// Evaluates action "execute-api:Invoke" and optional IpAddress condition.
func policyStatementMatchesRequest(stmt resourcePolicyStatement, sourceIP string) bool {
	if !policyActionMatchesInvoke(stmt.Action) {
		return false
	}

	if len(stmt.Condition) == 0 {
		return true
	}

	if ipCond, ok := stmt.Condition["IpAddress"]; ok {
		raw, hasKey := ipCond["aws:SourceIp"]
		if hasKey && !clientIPMatchesCondition(sourceIP, raw) {
			return false
		}
	}

	return true
}

// policyActionMatchesInvoke checks whether the statement's Action covers "execute-api:Invoke".
func policyActionMatchesInvoke(action json.RawMessage) bool {
	if len(action) == 0 {
		return false
	}

	var single string
	if err := json.Unmarshal(action, &single); err == nil {
		return single == "*" || strings.EqualFold(single, "execute-api:Invoke")
	}

	var multi []string
	if err := json.Unmarshal(action, &multi); err != nil {
		return false
	}

	for _, a := range multi {
		if a == "*" || strings.EqualFold(a, "execute-api:Invoke") {
			return true
		}
	}

	return false
}

// clientIPMatchesCondition checks whether sourceIP matches the IpAddress condition value.
// The condition value may be a single CIDR string or a list of CIDR strings.
func clientIPMatchesCondition(sourceIP string, raw any) bool {
	switch v := raw.(type) {
	case string:
		return ipMatchesCIDROrExact(sourceIP, v)
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && ipMatchesCIDROrExact(sourceIP, s) {
				return true
			}
		}
	}

	return false
}

// ipMatchesCIDROrExact checks whether sourceIP equals or is contained within the CIDR.
// Supports both exact IP addresses and CIDR notation (e.g., "10.0.0.0/8").
func ipMatchesCIDROrExact(sourceIP, cidr string) bool {
	if sourceIP == cidr {
		return true
	}

	ip := net.ParseIP(sourceIP)
	if ip == nil {
		return false
	}

	if strings.ContainsRune(cidr, '/') {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return false
		}

		return network.Contains(ip)
	}

	return ip.Equal(net.ParseIP(cidr))
}

// extractClientIP returns the best-effort client IP from the request.
func extractClientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if first, _, found := strings.Cut(fwd, ","); found {
			return strings.TrimSpace(first)
		}

		return strings.TrimSpace(fwd)
	}

	if r.RemoteAddr != "" {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			return host
		}

		return r.RemoteAddr
	}

	return ""
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
		_, _ = w.Write(respBytes)

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
	apiID string,
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

	vtlCtx := VTLContext{
		Body:      string(rawBody),
		RequestID: r.Header.Get("X-Amzn-Requestid"),
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

	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(body))
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
