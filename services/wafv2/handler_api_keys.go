package wafv2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createAPIKeyRequest is the request body for CreateAPIKey.
type createAPIKeyRequest struct {
	Scope        string   `json:"Scope"`
	TokenDomains []string `json:"TokenDomains"`
}

func (h *Handler) handleCreateAPIKey(ctx context.Context, body []byte) ([]byte, error) {
	var req createAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if len(req.TokenDomains) < 1 || len(req.TokenDomains) > maxAPIKeyTokenDomains {
		return nil, fmt.Errorf(
			"%w: TokenDomains must have 1 to %d entries",
			errInvalidRequest,
			maxAPIKeyTokenDomains,
		)
	}

	a, err := h.Backend.CreateAPIKey(ctx, req.Scope, req.TokenDomains)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created API key", "scope", a.Scope)

	// Emit a base64-encoded key value as AWS does.
	encodedKey := base64.StdEncoding.EncodeToString([]byte(a.APIKeyValue))

	return json.Marshal(map[string]any{
		"APIKey": encodedKey,
	})
}

// deleteAPIKeyRequest is the request body for DeleteAPIKey.
type deleteAPIKeyRequest struct {
	Scope  string `json:"Scope"`
	APIKey string `json:"APIKey"`
}

func (h *Handler) handleDeleteAPIKey(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.APIKey == "" {
		return nil, fmt.Errorf("%w: APIKey is required", errInvalidRequest)
	}

	// API keys may be passed as base64-encoded values; try decoding first.
	lookupKey := req.APIKey
	if decoded, err := base64.StdEncoding.DecodeString(req.APIKey); err == nil {
		lookupKey = string(decoded)
	}

	if err := h.Backend.DeleteAPIKey(ctx, req.Scope, lookupKey); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted API key", "scope", req.Scope)

	return nil, nil
}

// listAPIKeysRequest is the request body for ListAPIKeys.
type listAPIKeysRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

func (h *Handler) handleListAPIKeys(ctx context.Context, body []byte) ([]byte, error) {
	var req listAPIKeysRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	keys := h.Backend.ListAPIKeys(ctx, req.Scope)

	// Apply pagination.
	page, nextMarker := paginateByName(
		keys,
		func(k *APIKey) string { return k.APIKeyValue },
		req.NextMarker,
		req.Limit,
	)

	items := make([]map[string]any, 0, len(page))

	for _, k := range page {
		items = append(items, map[string]any{
			"APIKey":       base64.StdEncoding.EncodeToString([]byte(k.APIKeyValue)),
			keyScope:       k.Scope,
			"TokenDomains": k.TokenDomains,
		})
	}

	resp := map[string]any{"APIKeys": items}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
}

// getDecryptedAPIKeyRequest is the request body for GetDecryptedAPIKey.
type getDecryptedAPIKeyRequest struct {
	Scope  string `json:"Scope"`
	APIKey string `json:"APIKey"`
}

func (h *Handler) handleGetDecryptedAPIKey(ctx context.Context, body []byte) ([]byte, error) {
	var req getDecryptedAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.APIKey == "" {
		return nil, fmt.Errorf("%w: APIKey is required", errInvalidRequest)
	}

	// API keys may be passed as base64-encoded values; try decoding first.
	lookupKey := req.APIKey
	if decoded, err := base64.StdEncoding.DecodeString(req.APIKey); err == nil {
		lookupKey = string(decoded)
	}

	a, err := h.Backend.GetDecryptedAPIKey(ctx, req.Scope, lookupKey)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"TokenDomains": a.TokenDomains,
		keyScope:       a.Scope,
	})
}

// apiKeyDispatchOps returns the API-key-family operation dispatch entries. Each entry is a
// bound method value -- handleCreateAPIKey et al. already match the dispatchFn signature,
// so no wrapper closure is needed.
func (h *Handler) apiKeyDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"CreateAPIKey":       h.handleCreateAPIKey,
		"DeleteAPIKey":       h.handleDeleteAPIKey,
		"ListAPIKeys":        h.handleListAPIKeys,
		"GetDecryptedAPIKey": h.handleGetDecryptedAPIKey,
	}
}
