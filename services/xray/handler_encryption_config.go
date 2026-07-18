package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

type putEncryptionConfigInput struct {
	KeyID string `json:"KeyId,omitempty"`
	Type  string `json:"Type"`
}

func (h *Handler) handleGetEncryptionConfig(c *echo.Context) error {
	cfg := h.Backend.GetEncryptionConfig()

	return c.JSON(http.StatusOK, map[string]any{
		keyEncryptionConfig: cfg,
	})
}

// handleGetEncryptionConfigBody serves GetEncryptionConfig via the table-driven
// dispatch path. The AWS SDK sends GetEncryptionConfig as POST /EncryptionConfig
// (PutEncryptionConfig uses the distinct POST /PutEncryptionConfig path).
func (h *Handler) handleGetEncryptionConfigBody(_ context.Context, _ []byte) ([]byte, error) {
	cfg := h.Backend.GetEncryptionConfig()

	return json.Marshal(map[string]any{
		keyEncryptionConfig: cfg,
	})
}

func (h *Handler) handlePutEncryptionConfig(_ context.Context, body []byte) ([]byte, error) {
	var in putEncryptionConfigInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.Type == "" {
		in.Type = encTypeNone
	}

	if in.Type != encTypeNone && in.Type != encTypeKMS {
		return nil, fmt.Errorf("%w: Type must be %q or %q, got %q",
			errInvalidRequest, encTypeNone, encTypeKMS, in.Type)
	}

	if in.Type == encTypeKMS && in.KeyID == "" {
		return nil, fmt.Errorf("%w: KeyId is required when Type is %q", errInvalidRequest, encTypeKMS)
	}

	if in.Type == encTypeNone && in.KeyID != "" {
		return nil, fmt.Errorf("%w: KeyId must not be set when Type is %q", errInvalidRequest, encTypeNone)
	}

	cfg, err := h.Backend.PutEncryptionConfig(in.Type, in.KeyID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyEncryptionConfig: cfg,
	})
}
