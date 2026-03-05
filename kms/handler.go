package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ErrUnknownOperation is returned when the requested KMS operation is not supported.
var ErrUnknownOperation = errors.New("UnknownOperationException")

type listResourceTagsInput struct {
	KeyID string `json:"KeyId"` //nolint:tagliatelle // AWS API uses KeyId
}

type kmsTagEntry struct {
	TagKey   string `json:"TagKey"`
	TagValue string `json:"TagValue"`
}

type tagResourceInput struct {
	KeyID string        `json:"KeyId"` //nolint:tagliatelle // AWS API uses KeyId
	Tags  []kmsTagEntry `json:"Tags"`
}

type listResourceTagsOutput struct {
	Tags      []kmsTagEntry `json:"Tags"`
	Truncated bool          `json:"Truncated"`
}

type untagResourceInput struct {
	KeyID   string   `json:"KeyId"` //nolint:tagliatelle // AWS API uses KeyId
	TagKeys []string `json:"TagKeys"`
}

// Handler is the Echo HTTP handler for KMS operations.
type Handler struct {
	Backend       StorageBackend
	actions       map[string]kmsActionFn
	tags          map[string]*tags.Tags
	tagsMu        *lockmetrics.RWMutex
	DefaultRegion string
}

// NewHandler creates a new KMS handler with the given storage backend and logger.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("kms.tags"),
	}
	h.actions = h.buildDispatchTable()

	return h
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("kms." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
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
		"GenerateDataKeyWithoutPlaintext",
		"GetKeyRotationStatus",
		"ListAliases",
		"ListKeys",
		"ReEncrypt",
		"ScheduleKeyDeletion",
		"CreateAlias",
		"DeleteAlias",
		"CreateGrant",
		"ListGrants",
		"RevokeGrant",
		"RetireGrant",
		"ListRetirableGrants",
		"PutKeyPolicy",
		"GetKeyPolicy",
		"ListResourceTags",
		"TagResource",
		"UntagResource",
	}
}

// RouteMatcher returns a matcher that identifies KMS requests by the X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "TrentService")
	}
}

// MatchPriority returns the routing priority for the KMS handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderPartial
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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"KMS", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, c.Request(), action, body)
			},
			h.handleError,
		)
	}
}

type kmsActionFn func(region string, body []byte) (any, error)

// buildDispatchTable merges key lifecycle, crypto, alias/rotation, and tag actions into a single lookup map.
func (h *Handler) buildDispatchTable() map[string]kmsActionFn {
	table := h.buildKeyLifecycleActions()
	maps.Copy(table, h.buildCryptoActions())
	maps.Copy(table, h.buildAliasRotationActions())
	maps.Copy(table, h.buildGrantPolicyActions())
	maps.Copy(table, h.buildTagActions())

	return table
}

// buildKeyLifecycleActions returns dispatch entries for key creation, description, listing and deletion.
func (h *Handler) buildKeyLifecycleActions() map[string]kmsActionFn {
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

// buildCryptoActions returns dispatch entries for encrypt, decrypt, and data-key operations.
func (h *Handler) buildCryptoActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
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
		"GenerateDataKey": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKey(&input)
		},
		"GenerateDataKeyWithoutPlaintext": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyWithoutPlaintext(&input)
		},
		"ReEncrypt": func(_ string, b []byte) (any, error) {
			var input ReEncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReEncrypt(&input)
		},
	}
}

// buildAliasRotationActions returns dispatch entries for alias management and key rotation.
func (h *Handler) buildAliasRotationActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
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

// buildGrantPolicyActions returns dispatch entries for grant and key policy operations.
func (h *Handler) buildGrantPolicyActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateGrant": func(_ string, b []byte) (any, error) {
			var input CreateGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateGrant(&input)
		},
		"ListGrants": func(_ string, b []byte) (any, error) {
			var input ListGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListGrants(&input)
		},
		"RevokeGrant": func(_ string, b []byte) (any, error) {
			var input RevokeGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RevokeGrant(&input)
		},
		"RetireGrant": func(_ string, b []byte) (any, error) {
			var input RetireGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RetireGrant(&input)
		},
		"ListRetirableGrants": func(_ string, b []byte) (any, error) {
			var input ListRetirableGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListRetirableGrants(&input)
		},
		"PutKeyPolicy": func(_ string, b []byte) (any, error) {
			var input PutKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.PutKeyPolicy(&input)
		},
		"GetKeyPolicy": func(_ string, b []byte) (any, error) {
			var input GetKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyPolicy(&input)
		},
	}
}

// buildTagActions returns dispatch entries for KMS resource tag operations.
func (h *Handler) buildTagActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"ListResourceTags": func(_ string, b []byte) (any, error) {
			var input listResourceTagsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			tags := h.getTags(input.KeyID)
			tagList := make([]kmsTagEntry, 0, len(tags))
			for k, v := range tags {
				tagList = append(tagList, kmsTagEntry{TagKey: k, TagValue: v})
			}

			return &listResourceTagsOutput{Tags: tagList, Truncated: false}, nil
		},
		"TagResource": func(_ string, b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.TagKey] = t.TagValue
			}
			h.setTags(input.KeyID, kv)

			return struct{}{}, nil
		},
		"UntagResource": func(_ string, b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.KeyID, input.TagKeys)

			return struct{}{}, nil
		},
	}
}

// dispatch routes the KMS operation to the appropriate backend method.
func (h *Handler) dispatch(_ context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	region := httputil.ExtractRegionFromRequest(r, h.DefaultRegion)

	fn, ok := h.actions[action]
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
	case errors.Is(reqErr, ErrKeyNotFound), errors.Is(reqErr, ErrAliasNotFound), errors.Is(reqErr, ErrGrantNotFound):
		errorType = "NotFoundException"
	case errors.Is(reqErr, ErrKeyDisabled):
		errorType = "DisabledException"
	case errors.Is(reqErr, ErrKeyInvalidState):
		errorType = "KMSInvalidStateException"
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
