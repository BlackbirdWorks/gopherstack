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
)

var errUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP service handler for API Gateway operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
}

// NewHandler creates a new API Gateway handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
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
		"GetDeployments",
		"GetStages",
		"GetStage",
		"DeleteStage",
	}
}

// RouteMatcher returns a matcher for API Gateway requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "APIGateway.")
	}
}

const apiGatewayMatchPriority = 100

// MatchPriority returns the routing priority for the API Gateway handler.
func (h *Handler) MatchPriority() int { return apiGatewayMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
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

type actionFn func([]byte) (int, any, error)

func (h *Handler) restAPIActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateRestApi": func(b []byte) (int, any, error) {
			var input struct {
				Tags        map[string]string `json:"tags"`
				Name        string            `json:"name"`
				Description string            `json:"description"`
			}
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
			var input struct {
				RestAPIID string `json:"restApiId"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteRestAPI(input.RestAPIID); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, map[string]any{}, nil
		},
		"GetRestApi": func(b []byte) (int, any, error) {
			var input struct {
				RestAPIID string `json:"restApiId"`
			}
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
			var input struct {
				Position string `json:"position"`
				Limit    int    `json:"limit"`
			}
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
			var input struct {
				RestAPIID string `json:"restApiId"`
				Position  string `json:"position"`
				Limit     int    `json:"limit"`
			}
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
			var input struct {
				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
			}
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
			var input struct {
				RestAPIID string `json:"restApiId"`
				ParentID  string `json:"parentId"`
				PathPart  string `json:"pathPart"`
			}
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
			var input struct {
				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
			}
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
			var input struct {
				RestAPIID         string `json:"restApiId"`
				ResourceID        string `json:"resourceId"`
				HTTPMethod        string `json:"httpMethod"`
				AuthorizationType string `json:"authorizationType"`
				APIKeyRequired    bool   `json:"apiKeyRequired"`
			}
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
			var input struct {
				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
				HTTPMethod string `json:"httpMethod"`
			}
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
			var input struct {
				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
				HTTPMethod string `json:"httpMethod"`
			}
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
			var input struct {
				PutIntegrationInput

				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
				HTTPMethod string `json:"httpMethod"`
			}
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
			var input struct {
				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
				HTTPMethod string `json:"httpMethod"`
			}
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
			var input struct {
				RestAPIID  string `json:"restApiId"`
				ResourceID string `json:"resourceId"`
				HTTPMethod string `json:"httpMethod"`
			}
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
			var input struct {
				RestAPIID   string `json:"restApiId"`
				StageName   string `json:"stageName"`
				Description string `json:"description"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depl, err := h.Backend.CreateDeployment(input.RestAPIID, input.StageName, input.Description)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, depl, nil
		},
		"GetDeployments": func(b []byte) (int, any, error) {
			var input struct {
				RestAPIID string `json:"restApiId"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			depls, err := h.Backend.GetDeployments(input.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"item": depls}, nil
		},
		"GetStages": func(b []byte) (int, any, error) {
			var input struct {
				RestAPIID string `json:"restApiId"`
			}
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
			var input struct {
				RestAPIID string `json:"restApiId"`
				StageName string `json:"stageName"`
			}
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
			var input struct {
				RestAPIID string `json:"restApiId"`
				StageName string `json:"stageName"`
			}
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
