package apigateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

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
		_, _ = w.Write(respBytes) //nolint:gosec // G705: Lambda response bytes

		return
	}

	for k, v := range lambdaResp.Headers {
		w.Header().Set(k, v)
	}

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

	// Apply response mapping template for status code "200" if present.
	responseBody := applyResponseTemplate(respBytes, integration, vtlCtx.RequestID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody) //nolint:gosec // G705: Lambda response bytes
}

// applyResponseTemplate applies the response VTL template (status "200") if configured.
func applyResponseTemplate(respBytes []byte, integration *Integration, requestID string) []byte {
	if integration.IntegrationResponses == nil {
		return respBytes
	}

	ir, ok := integration.IntegrationResponses["200"]
	if !ok || ir == nil {
		return respBytes
	}

	tpl, ok := ir.ResponseTemplates["application/json"]
	if !ok || tpl == "" {
		return respBytes
	}

	respVTLCtx := VTLContext{
		Body:      string(respBytes),
		RequestID: requestID,
	}

	return []byte(RenderTemplate(tpl, respVTLCtx))
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

	//nolint:gosec // G107: integration URI is configured via the API definition, not raw user input
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
// greedy path variables ({proxy+} or {param+}).  The most-specific match wins.
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

	// Sort resources by specificity so the most specific match wins.
	sorted := make([]Resource, len(resources))
	copy(sorted, resources)
	sort.Slice(sorted, func(i, j int) bool {
		return resourceSpecificity(sorted[i].Path) > resourceSpecificity(sorted[j].Path)
	})

	for i := range sorted {
		params, ok := matchResourcePath(sorted[i].Path, stripped)
		if ok {
			return &sorted[i], params
		}
	}

	return nil, nil
}

// resourceSpecificity returns a score for the given resource path pattern.
// Higher scores indicate more specific patterns.
// Each exact literal segment contributes 2 points and each non-greedy path variable
// contributes 1 point.  A greedy variable ({proxy+}) contributes 0.
// Path length (in segments) is used as a secondary factor to prefer longer matches.
func resourceSpecificity(pattern string) int {
	const segLengthFactor = 10 // multiply path length to prioritise longer paths

	segs := splitPathSegs(pattern)
	score := len(segs) * segLengthFactor

	for _, seg := range segs {
		switch {
		case strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "+}"):
			// greedy variable — no extra points
		case strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}"):
			score++ // parameterized variable
		default:
			score += 2 // exact literal
		}
	}

	return score
}

// splitPathSegs splits a URL path into non-empty segments, ignoring leading and trailing slashes.
func splitPathSegs(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return []string{}
	}

	return strings.Split(trimmed, "/")
}

// matchResourcePath tries to match urlPath against a resource path pattern.
// Returns extracted path parameters and true on a successful match.
func matchResourcePath(pattern, urlPath string) (map[string]string, bool) {
	patternSegs := splitPathSegs(pattern)
	urlSegs := splitPathSegs(urlPath)

	// Root resource matches only the root path.
	if len(patternSegs) == 0 {
		if len(urlSegs) == 0 {
			return map[string]string{}, true
		}

		return nil, false
	}

	params := make(map[string]string)

	for i, seg := range patternSegs {
		// Greedy variable {param+} must be the last pattern segment.
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "+}") {
			// If the greedy segment is not the last, the pattern is malformed — treat as non-matching.
			if i != len(patternSegs)-1 {
				return nil, false
			}

			if i >= len(urlSegs) {
				return nil, false
			}

			paramName := seg[1 : len(seg)-2] // strip '{' and '+}'
			params[paramName] = "/" + strings.Join(urlSegs[i:], "/")

			return params, true
		}

		if i >= len(urlSegs) {
			return nil, false
		}

		urlSeg := urlSegs[i]

		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			paramName := seg[1 : len(seg)-1] // strip '{' and '}'
			params[paramName] = urlSeg
		} else if seg != urlSeg {
			return nil, false
		}
	}

	// All pattern segments consumed — reject if URL has additional segments.
	if len(urlSegs) != len(patternSegs) {
		return nil, false
	}

	return params, true
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
