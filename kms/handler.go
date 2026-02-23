package kms

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

// ErrUnknownOperation is returned when the requested KMS operation is not supported.
var ErrUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for KMS operations.
type Handler struct {
	// Backend is the underlying KMS storage backend.
	Backend StorageBackend
	// Logger is the structured logger for this handler.
	Logger *slog.Logger
	// DefaultRegion is the fallback region used when region cannot be extracted from the request.
	DefaultRegion string
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
		"CancelKeyDeletion",
		"CreateKey",
		"DescribeKey",
		"DisableKey",
		"DisableKeyRotation",
		"Decrypt",
		"EnableKey",
		"EnableKeyRotation",
		"Encrypt",
		"GenerateDataKey",
		"GetKeyRotationStatus",
		"ListAliases",
		"ListKeys",
		"ReEncrypt",
		"ScheduleKeyDeletion",
		"CreateAlias",
		"DeleteAlias",
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

		response, reqErr := h.dispatch(ctx, c.Request(), action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSONBlob(http.StatusOK, response)
	}
}

type kmsActionFn func(region string, body []byte) (any, error)

func (h *Handler) kmsKeyDispatchTable() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateKey": func(region string, b []byte) (any, error) {
			var input CreateKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			input.Region = region

			return h.Backend.CreateKey(&input)
		},
		"DescribeKey": func(_ string, b []byte) (any, error) {
			var input DescribeKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeKey(&input)
		},
		"ListKeys": func(_ string, b []byte) (any, error) {
			var input ListKeysInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeys(&input)
		},
		"Encrypt": func(_ string, b []byte) (any, error) {
			var input EncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Encrypt(&input)
		},
		"Decrypt": func(_ string, b []byte) (any, error) {
			var input DecryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Decrypt(&input)
		},
		"DisableKey": func(_ string, b []byte) (any, error) {
			var input DisableKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisableKey(&input)
		},
		"EnableKey": func(_ string, b []byte) (any, error) {
			var input EnableKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.EnableKey(&input)
		},
		"ScheduleKeyDeletion": func(_ string, b []byte) (any, error) {
			var input ScheduleKeyDeletionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ScheduleKeyDeletion(&input)
		},
		"CancelKeyDeletion": func(_ string, b []byte) (any, error) {
			var input CancelKeyDeletionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CancelKeyDeletion(&input)
		},
	}
}

func (h *Handler) kmsMiscDispatchTable() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"GenerateDataKey": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKey(&input)
		},
		"ReEncrypt": func(_ string, b []byte) (any, error) {
			var input ReEncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReEncrypt(&input)
		},
		"CreateAlias": func(_ string, b []byte) (any, error) {
			var input CreateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateAlias(&input)
		},
		"DeleteAlias": func(_ string, b []byte) (any, error) {
			var input DeleteAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteAlias(&input)
		},
		"ListAliases": func(_ string, b []byte) (any, error) {
			var input ListAliasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAliases(&input)
		},
		"EnableKeyRotation": func(_ string, b []byte) (any, error) {
			var input EnableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.EnableKeyRotation(&input)
		},
		"DisableKeyRotation": func(_ string, b []byte) (any, error) {
			var input DisableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisableKeyRotation(&input)
		},
		"GetKeyRotationStatus": func(_ string, b []byte) (any, error) {
			var input GetKeyRotationStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyRotationStatus(&input)
		},
	}
}

// dispatch routes the KMS operation to the appropriate backend method.
func (h *Handler) dispatch(_ context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	region := httputil.ExtractRegionFromRequest(r, h.DefaultRegion)

	table := h.kmsKeyDispatchTable()
	maps.Copy(table, h.kmsMiscDispatchTable())

	fn, ok := table[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	response, err := fn(region, body)
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
