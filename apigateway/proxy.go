package apigateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
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
func BuildProxyEvent(r *http.Request, apiID, stageName, resource, path string) (*LambdaProxyEvent, error) {
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
//
//nolint:gocognit // proxy request handling requires multiple decision points
func (h *Handler) handleProxyRequest(apiID, stageName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log := h.Logger

		if h.lambda == nil {
			http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

			return
		}

		// Find the resource and integration.
		resources, _, err := h.Backend.GetResources(apiID, "", 0)
		if err != nil {
			log.ErrorContext(ctx, "APIGateway proxy: failed to get resources", "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)

			return
		}

		// Match request path to resource path.
		resource := findMatchingResource(resources, r.URL.Path, stageName)
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

		if integration.Type != "AWS_PROXY" {
			http.Error(w, "Non-proxy integrations not supported on stage URL", http.StatusNotImplemented)

			return
		}

		// Build Lambda proxy event.
		event, buildErr := BuildProxyEvent(r, apiID, stageName, resource.Path, r.URL.Path)
		if buildErr != nil {
			log.ErrorContext(ctx, "APIGateway proxy: failed to build event", "error", buildErr)
			http.Error(w, "Internal server error", http.StatusInternalServerError)

			return
		}

		payload, _ := json.Marshal(event)

		// Invoke Lambda.
		respBytes, _, invokeErr := h.lambda.InvokeFunction(ctx, integration.URI, "RequestResponse", payload)
		if invokeErr != nil {
			log.WarnContext(ctx, "APIGateway proxy: Lambda invocation failed",
				"uri", integration.URI, "error", invokeErr)
			http.Error(w, "Internal server error", http.StatusInternalServerError)

			return
		}

		// Parse Lambda response.
		var lambdaResp LambdaProxyResponse
		if parseErr := json.Unmarshal(respBytes, &lambdaResp); parseErr != nil {
			// If not a proxy response format, return body as-is.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)

			return
		}

		// Write response.
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
}

// findMatchingResource finds a resource whose path matches the request path.
// Stage name prefix is stripped from the request path before matching.
func findMatchingResource(resources []Resource, requestPath, stageName string) *Resource {
	// Strip stage prefix: /{stageName}/... -> /...
	stripped := requestPath
	prefix := "/" + stageName
	if strings.HasPrefix(requestPath, prefix) {
		stripped = requestPath[len(prefix):]
	}

	if stripped == "" {
		stripped = "/"
	}

	for i := range resources {
		if resources[i].Path == stripped {
			return &resources[i]
		}
	}

	// Try root resource.
	for i := range resources {
		if resources[i].Path == "/" {
			return &resources[i]
		}
	}

	return nil
}
