package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrUnknownOperation is returned when the requested KMS operation is not supported.
var ErrUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for KMS operations.
type Handler struct {
	// Backend is the underlying KMS storage backend.
	Backend StorageBackend
	// Logger is the structured logger for this handler.
	Logger *slog.Logger
}

// NewHandler creates a new KMS handler with the given storage backend and logger.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
	}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "KMS"
}

// GetSupportedOperations returns the list of supported KMS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateKey",
		"DescribeKey",
		"ListKeys",
		"Encrypt",
		"Decrypt",
		"GenerateDataKey",
		"ReEncrypt",
		"CreateAlias",
		"DeleteAlias",
		"ListAliases",
		"EnableKeyRotation",
		"DisableKeyRotation",
		"GetKeyRotationStatus",
	}
}

// RouteMatcher returns a matcher that identifies KMS requests by the X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "TrentService")
	}
}

// kmsMatchPriority is the routing priority for the KMS handler.
const kmsMatchPriority = 95

// MatchPriority returns the routing priority for the KMS handler.
func (h *Handler) MatchPriority() int {
	return kmsMatchPriority
}

// ExtractOperation extracts the KMS operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")

	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource returns the key ID from the request body when present.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if keyID, ok := data["KeyId"].(string); ok {
		return keyID
	}

	return ""
}

// Handler returns the Echo handler function for KMS operations.
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

		log.DebugContext(ctx, "KMS request", "action", action)

		response, reqErr := h.dispatch(ctx, action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSONBlob(http.StatusOK, response)
	}
}

// dispatch routes the KMS operation to the appropriate backend method.
//
//nolint:cyclop,gocognit,funlen // Dispatch switch is intentionally comprehensive.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var response any
	var err error

	switch action {
	case "CreateKey":
		var input CreateKeyInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.CreateKey(&input)

	case "DescribeKey":
		var input DescribeKeyInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.DescribeKey(&input)

	case "ListKeys":
		var input ListKeysInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.ListKeys(&input)

	case "Encrypt":
		var input EncryptInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.Encrypt(&input)

	case "Decrypt":
		var input DecryptInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.Decrypt(&input)

	case "GenerateDataKey":
		var input GenerateDataKeyInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.GenerateDataKey(&input)

	case "ReEncrypt":
		var input ReEncryptInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.ReEncrypt(&input)

	case "CreateAlias":
		var input CreateAliasInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		err = h.Backend.CreateAlias(&input)
		response = struct{}{}

	case "DeleteAlias":
		var input DeleteAliasInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		err = h.Backend.DeleteAlias(&input)
		response = struct{}{}

	case "ListAliases":
		var input ListAliasesInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.ListAliases(&input)

	case "EnableKeyRotation":
		var input EnableKeyRotationInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		err = h.Backend.EnableKeyRotation(&input)
		response = struct{}{}

	case "DisableKeyRotation":
		var input DisableKeyRotationInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		err = h.Backend.DisableKeyRotation(&input)
		response = struct{}{}

	case "GetKeyRotationStatus":
		var input GetKeyRotationStatusInput
		if uErr := json.Unmarshal(body, &input); uErr != nil {
			return nil, uErr
		}
		response, err = h.Backend.GetKeyRotationStatus(&input)

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a structured error response for a KMS operation failure.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errorType string

	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrKeyNotFound), errors.Is(reqErr, ErrAliasNotFound):
		errorType = "NotFoundException"
	case errors.Is(reqErr, ErrKeyDisabled):
		errorType = "DisabledException"
	case errors.Is(reqErr, ErrAliasAlreadyExists):
		errorType = "AlreadyExistsException"
	case errors.Is(reqErr, ErrInvalidCiphertext), errors.Is(reqErr, ErrCiphertextTooShort):
		errorType = "InvalidCiphertextException"
	case errors.Is(reqErr, ErrUnknownOperation):
		errorType = "UnknownOperationException"
	default:
		errorType = "InternalServiceError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "KMS internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "KMS request error", "error", reqErr, "action", action)
	}

	payload, _ := json.Marshal(ErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	})

	return c.JSONBlob(statusCode, payload)
}
