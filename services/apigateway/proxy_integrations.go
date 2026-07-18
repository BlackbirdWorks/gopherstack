package apigateway

import (
	"container/list"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

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

// cachedRegexp returns a compiled regexp for the pattern, using the handler's bounded
// LRU cache. Returns nil (also cached) if the pattern does not compile.
func (h *Handler) cachedRegexp(pattern string) *regexp.Regexp {
	if re, ok := h.selRegexpCache.get(pattern); ok {
		return re
	}

	re, compileErr := regexp.Compile(pattern)
	if compileErr != nil {
		h.selRegexpCache.put(pattern, nil)

		return nil
	}

	h.selRegexpCache.put(pattern, re)

	return re
}

// defaultRegexpCacheMaxEntries bounds the compiled selection-pattern regexp cache.
const defaultRegexpCacheMaxEntries = 1024

// regexpCache is a mutex-guarded LRU of compiled regexps keyed by user-supplied
// selection patterns. It caps its size so that unbounded distinct patterns cannot leak
// memory. A cached nil value records a pattern that failed to compile.
type regexpCache struct {
	entries    map[string]*list.Element
	order      *list.List
	mu         sync.Mutex
	maxEntries int
}

type regexpCacheEntry struct {
	re  *regexp.Regexp
	key string
}

func newRegexpCache(maxEntries int) *regexpCache {
	if maxEntries <= 0 {
		maxEntries = defaultRegexpCacheMaxEntries
	}

	return &regexpCache{
		entries:    make(map[string]*list.Element),
		order:      list.New(),
		maxEntries: maxEntries,
	}
}

func (c *regexpCache) get(pattern string) (*regexp.Regexp, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[pattern]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	entry, _ := elem.Value.(regexpCacheEntry)

	return entry.re, true
}

func (c *regexpCache) put(pattern string, re *regexp.Regexp) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[pattern]; ok {
		elem.Value = regexpCacheEntry{re: re, key: pattern}
		c.order.MoveToFront(elem)

		return
	}

	elem := c.order.PushFront(regexpCacheEntry{re: re, key: pattern})
	c.entries[pattern] = elem

	for c.order.Len() > c.maxEntries {
		back := c.order.Back()
		if back == nil {
			break
		}
		entry, _ := back.Value.(regexpCacheEntry)
		delete(c.entries, entry.key)
		c.order.Remove(back)
	}
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
