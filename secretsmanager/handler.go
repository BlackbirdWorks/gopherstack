package secretsmanager

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

// ErrUnknownOperation is returned when an unsupported operation is requested.
var ErrUnknownOperation = errors.New("UnknownOperationException")

type getResourcePolicyInput struct {
	SecretID string `json:"SecretId"`
}

// LambdaInvoker can invoke a Lambda function synchronously.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// Handler is the Echo HTTP handler for Secrets Manager operations.
type Handler struct {
	Backend       StorageBackend
	lambdaInvoker LambdaInvoker
	Logger        *slog.Logger
	DefaultRegion string
}

// NewHandler creates a new Secrets Manager handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
	}
}

// SetLambdaInvoker sets the Lambda invoker used for RotateSecret with a rotation Lambda ARN.
func (h *Handler) SetLambdaInvoker(invoker LambdaInvoker) {
	h.lambdaInvoker = invoker
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SecretsManager"
}

// GetSupportedOperations returns the list of supported Secrets Manager operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateSecret",
		"GetSecretValue",
		"PutSecretValue",
		"DeleteSecret",
		"ListSecrets",
		"DescribeSecret",
		"UpdateSecret",
		"RestoreSecret",
		"TagResource",
		"UntagResource",
		"RotateSecret",
	}
}

// RouteMatcher returns a function that matches Secrets Manager requests by X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "secretsmanager")
	}
}

// MatchPriority returns the routing priority for the Secrets Manager handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderPartial
}

// ExtractOperation extracts the Secrets Manager operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")

	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource returns the secret ID from the request body when present.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if secretID, ok := data["SecretId"].(string); ok {
		return secretID
	}

	if name, ok := data["Name"].(string); ok {
		return name
	}

	return ""
}

// Handler returns the Echo handler function for Secrets Manager operations.
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

		log.DebugContext(ctx, "SecretsManager request", "action", action)

		response, reqErr := h.dispatch(ctx, c.Request(), action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSONBlob(http.StatusOK, response)
	}
}

type smActionFn func(ctx context.Context, region string, body []byte) (any, error)

func (h *Handler) smDispatchTable() map[string]smActionFn {
	table := make(map[string]smActionFn)
	maps.Copy(table, h.smCRUDActions())
	maps.Copy(table, h.smTagActions())
	maps.Copy(table, h.smPolicyActions())

	return table
}

func (h *Handler) smCRUDActions() map[string]smActionFn {
	return map[string]smActionFn{
		"CreateSecret": func(_ context.Context, region string, b []byte) (any, error) {
			var input CreateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			input.Region = region

			return h.Backend.CreateSecret(&input)
		},
		"GetSecretValue": func(_ context.Context, _ string, b []byte) (any, error) {
			var input GetSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetSecretValue(&input)
		},
		"PutSecretValue": func(_ context.Context, _ string, b []byte) (any, error) {
			var input PutSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutSecretValue(&input)
		},
		"DeleteSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input DeleteSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteSecret(&input)
		},
		"ListSecrets": func(_ context.Context, _ string, b []byte) (any, error) {
			var input ListSecretsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListSecrets(&input)
		},
		"DescribeSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input DescribeSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSecret(&input)
		},
		"UpdateSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input UpdateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateSecret(&input)
		},
		"RestoreSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input RestoreSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RestoreSecret(&input)
		},
		"RotateSecret": func(ctx context.Context, region string, b []byte) (any, error) {
			var input RotateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.rotateSecret(ctx, region, &input)
		},
	}
}

func (h *Handler) smTagActions() map[string]smActionFn {
	return map[string]smActionFn{
		"TagResource": func(_ context.Context, _ string, b []byte) (any, error) {
			var input TagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.TagResource(&input)
		},
		"UntagResource": func(_ context.Context, _ string, b []byte) (any, error) {
			var input UntagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UntagResource(&input)
		},
	}
}

func (h *Handler) smPolicyActions() map[string]smActionFn {
	return map[string]smActionFn{
		"GetResourcePolicy": func(_ context.Context, _ string, b []byte) (any, error) {
			var input getResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return map[string]string{"ARN": input.SecretID, "Name": input.SecretID}, nil
		},
		"PutResourcePolicy": func(_ context.Context, _ string, _ []byte) (any, error) {
			return struct{}{}, nil
		},
		"DeleteResourcePolicy": func(_ context.Context, _ string, _ []byte) (any, error) {
			return struct{}{}, nil
		},
	}
}

// dispatch routes the operation to the appropriate backend method.
func (h *Handler) dispatch(ctx context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	region := httputil.ExtractRegionFromRequest(r, h.DefaultRegion)

	fn, ok := h.smDispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	response, err := fn(ctx, region, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a structured error response for a Secrets Manager operation failure.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errorType string

	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrSecretNotFound), errors.Is(reqErr, ErrVersionNotFound):
		errorType = "ResourceNotFoundException"
	case errors.Is(reqErr, ErrSecretAlreadyExists):
		errorType = "ResourceExistsException"
	case errors.Is(reqErr, ErrSecretDeleted):
		errorType = "InvalidRequestException"
	case errors.Is(reqErr, ErrUnknownOperation):
		errorType = "UnknownOperationException"
	default:
		errorType = "InternalServiceError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "SecretsManager internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "SecretsManager request error", "error", reqErr, "action", action)
	}

	payload, _ := json.Marshal(ErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	})

	return c.JSONBlob(statusCode, payload)
}

// rotationSteps is the ordered sequence of rotation steps sent to a rotation Lambda.
//
//nolint:gochecknoglobals // intentional package-level constant slice
var rotationSteps = []string{"createSecret", "setSecret", "testSecret", "finishSecret"}

// extractFunctionNameFromARN extracts the bare function name from a Lambda ARN.
// Handles both unqualified ARNs (…:function:my-fn) and qualified ARNs (…:function:my-fn:alias).
func extractFunctionNameFromARN(arn string) string {
	const functionSegment = ":function:"

	if _, after, found := strings.Cut(arn, functionSegment); found {
		// Strip trailing qualifier (alias or version) if present.
		name, _, _ := strings.Cut(after, ":")

		return name
	}

	// Fallback: take the last colon-separated segment.
	if idx := strings.LastIndex(arn, ":"); idx >= 0 {
		return arn[idx+1:]
	}

	return arn
}

// rotateSecret performs RotateSecret, optionally invoking a rotation Lambda for each step.
func (h *Handler) rotateSecret(ctx context.Context, _ string, input *RotateSecretInput) (*RotateSecretOutput, error) {
	out, err := h.Backend.RotateSecret(input)
	if err != nil {
		return nil, err
	}

	if input.RotationLambdaARN == "" || h.lambdaInvoker == nil {
		return out, nil
	}

	functionName := extractFunctionNameFromARN(input.RotationLambdaARN)

	token := input.ClientRequestToken
	if token == "" {
		token = out.VersionID
	}

	for _, step := range rotationSteps {
		event, marshalErr := json.Marshal(map[string]string{
			"SecretId":           input.SecretID,
			"ClientRequestToken": token,
			"Step":               step,
		})
		if marshalErr != nil {
			return nil, fmt.Errorf("rotation event marshal: %w", marshalErr)
		}

		if _, _, invokeErr := h.lambdaInvoker.InvokeFunction(
			ctx,
			functionName,
			"RequestResponse",
			event,
		); invokeErr != nil {
			return nil, fmt.Errorf("rotation Lambda step %q failed: %w", step, invokeErr)
		}
	}

	return out, nil
}
