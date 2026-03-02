package apigateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var errUnknownOperation = errors.New("UnknownOperationException")

type createRestAPIInput struct {
	Tags        *tags.Tags `json:"tags"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
}

type deleteRestAPIInput struct {
	RestAPIID string `json:"restApiId"`
}

type getRestAPIInput struct {
	RestAPIID string `json:"restApiId"`
}

type getRestApisInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type getResourcesInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
}

type getResourceInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type createResourceInput struct {
	RestAPIID string `json:"restApiId"`
	ParentID  string `json:"parentId"`
	PathPart  string `json:"pathPart"`
}

type deleteResourceInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type putMethodInput struct {
	RestAPIID         string `json:"restApiId"`
	ResourceID        string `json:"resourceId"`
	HTTPMethod        string `json:"httpMethod"`
	AuthorizationType string `json:"authorizationType"`
	APIKeyRequired    bool   `json:"apiKeyRequired"`
}

type getMethodInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type deleteMethodInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type putIntegrationInput struct {
	PutIntegrationInput

	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type getIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type deleteIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type createDeploymentInput struct {
	RestAPIID   string `json:"restApiId"`
	StageName   string `json:"stageName"`
	Description string `json:"description"`
}

type getDeploymentInput struct {
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type getDeploymentsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteDeploymentInput struct {
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type getStagesInput struct {
	RestAPIID string `json:"restApiId"`
}

type getStageInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type deleteStageInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

// Handler is the Echo HTTP service handler for API Gateway operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
	lambda  LambdaInvoker
}

// NewHandler creates a new API Gateway handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// SetLambdaInvoker configures the Lambda invoker for AWS_PROXY integrations.
func (h *Handler) SetLambdaInvoker(lambda LambdaInvoker) {
	h.lambda = lambda
}

// Name returns the service name.
func (h *Handler) Name() string { return "APIGateway" }

// GetSupportedOperations returns all mocked API Gateway operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateRestApi",
		"DeleteRestApi",
		"GetRestApi",
		"GetRestApis",
		"GetResources",
		"GetResource",
		"CreateResource",
		"DeleteResource",
		"PutMethod",
		"GetMethod",
		"DeleteMethod",
		"PutIntegration",
		"GetIntegration",
		"DeleteIntegration",
		"CreateDeployment",
		"GetDeployment",
		"GetDeployments",
		"DeleteDeployment",
		"GetStages",
		"GetStage",
		"DeleteStage",
	}
}

// RouteMatcher returns a matcher for API Gateway requests.
// Matches both X-Amz-Target (JSON protocol) and REST API paths (/restapis/...).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), "APIGateway.") {
			return true
		}

		return strings.HasPrefix(c.Request().URL.Path, "/restapis")
	}
}

// MatchPriority returns the routing priority for the API Gateway handler.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header or REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	op, _, _ := parseAPIGWRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"restApiId", "name"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for API Gateway requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
			return c.JSON(http.StatusOK, h.GetSupportedOperations())
		}

		// Handle proxy invocations for deployed API stages.
		// Path format: /proxy/{apiId}/{stageName}/{resourcePath}
		if strings.HasPrefix(c.Request().URL.Path, "/proxy/") {
			return h.handleStageProxyEcho(c)
		}

		// REST API paths: /restapis/...
		if strings.HasPrefix(c.Request().URL.Path, "/restapis") &&
			!strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), "APIGateway.") {
			return h.handleRESTAPI(c)
		}

		if c.Request().Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
		}

		target := c.Request().Header.Get("X-Amz-Target")
		if target == "" {
			return c.String(http.StatusBadRequest, "Missing X-Amz-Target")
		}

		parts := strings.Split(target, ".")
		const targetParts = 2
		if len(parts) != targetParts {
			return c.String(http.StatusBadRequest, "Invalid X-Amz-Target")
		}
		action := parts[1]

		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		log.DebugContext(ctx, "APIGateway request", "action", action)

		statusCode, response, reqErr := h.dispatch(ctx, action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")
		if statusCode == http.StatusNoContent {
			return c.NoContent(http.StatusNoContent)
		}

		return c.JSONBlob(statusCode, response)
	}
}

// handleRESTAPI handles REST-style API Gateway calls (e.g. from the AWS SDK v2).
// It parses the URL path, extracts path parameters, merges them with the request
// body, and dispatches to the existing typed action handlers.
func (h *Handler) handleRESTAPI(c *echo.Context) error {
	ctx := c.Request().Context()

	action, pathParams, ok := parseAPIGWRESTPath(c.Request().Method, c.Request().URL.Path)
	if !ok {
		return c.String(http.StatusNotFound, "not found")
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	// Merge path parameters into the JSON body so existing handlers can read them.
	for k, v := range pathParams {
		body = injectJSONFieldAPIGW(body, k, v)
	}

	statusCode, response, reqErr := h.dispatch(ctx, action, body)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	c.Response().Header().Set("Content-Type", "application/json")
	if statusCode == http.StatusNoContent {
		return c.NoContent(http.StatusNoContent)
	}

	return c.JSONBlob(statusCode, response)
}

// injectJSONFieldAPIGW merges a key/value string pair into a JSON object body.
func injectJSONFieldAPIGW(body []byte, key, value string) []byte {
	var m map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &m); err != nil {
			m = make(map[string]json.RawMessage)
		}
	} else {
		m = make(map[string]json.RawMessage)
	}

	quoted, _ := json.Marshal(value)
	m[key] = json.RawMessage(quoted)

	result, _ := json.Marshal(m)

	return result
}

// parseAPIGWRESTPath maps an HTTP method + URL path to an API Gateway operation name
// and extracts path parameters. Returns ("Unknown", nil, false) when no pattern matches.
//
//nolint:cyclop,gocognit // path routing table is inherently a multi-branch switch
func parseAPIGWRESTPath(method, path string) (op string, params map[string]string, ok bool) {
	// Strip leading "/" and split into path segments.
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	n := len(segs)

	// All API Gateway REST paths start with "restapis".
	if n == 0 || segs[0] != "restapis" {
		return "Unknown", nil, false
	}

	switch {
	// POST /restapis → CreateRestApi
	case n == 1 && method == http.MethodPost:
		return "CreateRestApi", nil, true
	// GET /restapis → GetRestApis
	case n == 1 && method == http.MethodGet:
		return "GetRestApis", nil, true
	// GET /restapis/{id} → GetRestApi
	case n == 2 && method == http.MethodGet: //nolint:mnd
		return "GetRestApi", map[string]string{"restApiId": segs[1]}, true
	// DELETE /restapis/{id} → DeleteRestApi
	case n == 2 && method == http.MethodDelete: //nolint:mnd
		return "DeleteRestApi", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/resources → GetResources
	case n == 3 && segs[2] == "resources" && method == http.MethodGet: //nolint:mnd
		return "GetResources", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/resources/{resId} → GetResource
	case n == 4 && segs[2] == "resources" && method == http.MethodGet: //nolint:mnd
		return "GetResource", map[string]string{"restApiId": segs[1], "resourceId": segs[3]}, true
	// POST /restapis/{id}/resources/{parentId} → CreateResource
	case n == 4 && segs[2] == "resources" && method == http.MethodPost: //nolint:mnd
		return "CreateResource", map[string]string{"restApiId": segs[1], "parentId": segs[3]}, true
	// DELETE /restapis/{id}/resources/{resId} → DeleteResource
	case n == 4 && segs[2] == "resources" && method == http.MethodDelete: //nolint:mnd
		return "DeleteResource", map[string]string{"restApiId": segs[1], "resourceId": segs[3]}, true
	// /restapis/{id}/resources/{resId}/methods/{httpMethod}[/integration]
	case n >= 5 && segs[2] == "resources" && segs[4] == "methods": //nolint:mnd
		return parseAPIGWMethodPath(method, segs)
	// POST /restapis/{id}/deployments → CreateDeployment
	case n == 3 && segs[2] == "deployments" && method == http.MethodPost: //nolint:mnd
		return "CreateDeployment", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/deployments → GetDeployments
	case n == 3 && segs[2] == "deployments" && method == http.MethodGet: //nolint:mnd
		return "GetDeployments", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/deployments/{deplId} → GetDeployment
	case n == 4 && segs[2] == "deployments" && method == http.MethodGet: //nolint:mnd
		return "GetDeployment", map[string]string{"restApiId": segs[1], "deploymentId": segs[3]}, true
	// DELETE /restapis/{id}/deployments/{deplId} → DeleteDeployment
	case n == 4 && segs[2] == "deployments" && method == http.MethodDelete: //nolint:mnd
		return "DeleteDeployment", map[string]string{"restApiId": segs[1], "deploymentId": segs[3]}, true
	// GET /restapis/{id}/stages → GetStages
	case n == 3 && segs[2] == "stages" && method == http.MethodGet: //nolint:mnd
		return "GetStages", map[string]string{"restApiId": segs[1]}, true
	// GET /restapis/{id}/stages/{stageName} → GetStage
	case n == 4 && segs[2] == "stages" && method == http.MethodGet: //nolint:mnd
		return "GetStage", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	// DELETE /restapis/{id}/stages/{stageName} → DeleteStage
	case n == 4 && segs[2] == "stages" && method == http.MethodDelete: //nolint:mnd
		return "DeleteStage", map[string]string{"restApiId": segs[1], "stageName": segs[3]}, true
	}

	return "Unknown", nil, false
}

// parseAPIGWMethodPath handles paths under /restapis/{id}/resources/{resId}/methods/{httpMethod}.
func parseAPIGWMethodPath(method string, segs []string) (op string, params map[string]string, ok bool) {
	// segs: [restapis, {id}, resources, {resId}, methods, {httpMethod}, ...]
	const (
		idxID         = 1
		idxResourceID = 3
		idxMethodSeg  = 4
		idxHTTPMethod = 5
		idxIntegSeg   = 6
	)

	if len(segs) <= idxHTTPMethod {
		return "Unknown", nil, false
	}

	apiID := segs[idxID]
	resID := segs[idxResourceID]
	httpMethod := segs[idxHTTPMethod]
	baseParams := map[string]string{
		"restApiId":  apiID,
		"resourceId": resID,
		"httpMethod": httpMethod,
	}

	// /restapis/{id}/resources/{resId}/methods/{httpMethod}/integration
	if len(segs) > idxIntegSeg && segs[idxIntegSeg] == "integration" {
		switch method {
		case http.MethodPut:
			return "PutIntegration", baseParams, true
		case http.MethodGet:
			return "GetIntegration", baseParams, true
		case http.MethodDelete:
			return "DeleteIntegration", baseParams, true
		}

		return "Unknown", nil, false
	}

	// /restapis/{id}/resources/{resId}/methods/{httpMethod}
	switch method {
	case http.MethodPut:
		return "PutMethod", baseParams, true
	case http.MethodGet:
		return "GetMethod", baseParams, true
	case http.MethodDelete:
		return "DeleteMethod", baseParams, true
	}

	return "Unknown", nil, false
}

type actionFn func([]byte) (int, any, error)

// handleStageProxyEcho routes /proxy/{apiId}/{stageName}/{path} requests to the Lambda proxy handler.
func (h *Handler) handleStageProxyEcho(c *echo.Context) error {
	// Strip the /proxy/ prefix to get /{apiId}/{stageName}/{path}
	rest := strings.TrimPrefix(c.Request().URL.Path, "/proxy/")
	const minProxyPathParts = 2
	parts := strings.SplitN(rest, "/", 3) //nolint:mnd // 3-part split: apiId, stage, path
	if len(parts) < minProxyPathParts {
		return c.String(http.StatusNotFound, "invalid proxy path")
	}

	apiID := parts[0]
	stageName := parts[1]

	// Rewrite the URL so handleProxyRequest sees "/{stageName}/{resourcePath}".
	resourcePath := "/"
	if len(parts) == 3 && parts[2] != "" {
		resourcePath = "/" + parts[2]
	}

	r := c.Request().Clone(c.Request().Context())
	r.URL.Path = "/" + stageName + resourcePath

	fn := h.handleProxyRequest(apiID, stageName)
	fn(c.Response(), r)

	return nil
}

func (h *Handler) restAPIActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateRestApi": func(b []byte) (int, any, error) {
			var input createRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			api, err := h.Backend.CreateRestAPI(input.Name, input.Description, input.Tags)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, api, nil
		},
		"DeleteRestApi": func(b []byte) (int, any, error) {
			var input deleteRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteRestAPI(input.RestAPIID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetRestApi": func(b []byte) (int, any, error) {
			var input getRestAPIInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			api, err := h.Backend.GetRestAPI(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, api, nil
		},
		"GetRestApis": func(b []byte) (int, any, error) {
			var input getRestApisInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			apis, position, err := h.Backend.GetRestAPIs(input.Limit, input.Position)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": apis, "position": position}, nil
		},
	}
}

func (h *Handler) resourceActions() map[string]actionFn {
	return map[string]actionFn{
		"GetResources": func(b []byte) (int, any, error) {
			var input getResourcesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			resources, position, err := h.Backend.GetResources(input.RestAPIID, input.Position, input.Limit)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": resources, "position": position}, nil
		},
		"GetResource": func(b []byte) (int, any, error) {
			var input getResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			r, err := h.Backend.GetResource(input.RestAPIID, input.ResourceID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, r, nil
		},
		"CreateResource": func(b []byte) (int, any, error) {
			var input createResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			r, err := h.Backend.CreateResource(input.RestAPIID, input.ParentID, input.PathPart)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, r, nil
		},
		"DeleteResource": func(b []byte) (int, any, error) {
			var input deleteResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteResource(input.RestAPIID, input.ResourceID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) methodActions() map[string]actionFn {
	return map[string]actionFn{
		"PutMethod": func(b []byte) (int, any, error) {
			var input putMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			m, err := h.Backend.PutMethod(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.AuthorizationType,
				input.APIKeyRequired,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, m, nil
		},
		"GetMethod": func(b []byte) (int, any, error) {
			var input getMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			m, err := h.Backend.GetMethod(input.RestAPIID, input.ResourceID, input.HTTPMethod)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, m, nil
		},
		"DeleteMethod": func(b []byte) (int, any, error) {
			var input deleteMethodInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteMethod(input.RestAPIID, input.ResourceID, input.HTTPMethod); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) integrationActions() map[string]actionFn {
	return map[string]actionFn{
		"PutIntegration": func(b []byte) (int, any, error) {
			var input putIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			integ, err := h.Backend.PutIntegration(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.PutIntegrationInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, integ, nil
		},
		"GetIntegration": func(b []byte) (int, any, error) {
			var input getIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			integ, err := h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, integ, nil
		},
		"DeleteIntegration": func(b []byte) (int, any, error) {
			var input deleteIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) deploymentActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateDeployment": func(b []byte) (int, any, error) {
			var input createDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depl, err := h.Backend.CreateDeployment(input.RestAPIID, input.StageName, input.Description)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, depl, nil
		},
		"GetDeployment": func(b []byte) (int, any, error) {
			var input getDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depl, err := h.Backend.GetDeployment(input.RestAPIID, input.DeploymentID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, depl, nil
		},
		"GetDeployments": func(b []byte) (int, any, error) {
			var input getDeploymentsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depls, err := h.Backend.GetDeployments(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": depls}, nil
		},
		"DeleteDeployment": func(b []byte) (int, any, error) {
			var input deleteDeploymentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteDeployment(input.RestAPIID, input.DeploymentID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		"GetStages": func(b []byte) (int, any, error) {
			var input getStagesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			stages, err := h.Backend.GetStages(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": stages}, nil
		},
		"GetStage": func(b []byte) (int, any, error) {
			var input getStageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			stage, err := h.Backend.GetStage(input.RestAPIID, input.StageName)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, stage, nil
		},
		"DeleteStage": func(b []byte) (int, any, error) {
			var input deleteStageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteStage(input.RestAPIID, input.StageName); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.restAPIActions())
	maps.Copy(table, h.resourceActions())
	maps.Copy(table, h.methodActions())
	maps.Copy(table, h.integrationActions())
	maps.Copy(table, h.deploymentActions())

	return table
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) (int, []byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return 0, nil, fmt.Errorf("%w:%s", errUnknownOperation, action)
	}

	statusCode, response, err := fn(body)
	if err != nil {
		return 0, nil, err
	}

	encoded, err := json.Marshal(response)
	if err != nil {
		return 0, nil, err
	}

	return statusCode, encoded, nil
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errType string
	var statusCode int

	switch {
	case errors.Is(reqErr, ErrRestAPINotFound),
		errors.Is(reqErr, ErrResourceNotFound),
		errors.Is(reqErr, ErrMethodNotFound):
		errType = "NotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrAlreadyExists):
		errType = "ConflictException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrInvalidParameter):
		errType = "BadRequestException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, errUnknownOperation):
		errType = "UnknownOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "APIGateway internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "APIGateway request error", "error", reqErr, "action", action)
	}

	errResp := ErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}
