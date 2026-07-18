package apigateway

import (
	"encoding/json"
	"net/http"
	"net/url"
)

type getAPIKeyInput struct {
	APIKeyID     string `json:"apiKeyId"`
	IncludeValue string `json:"includeValue"`
}

type getAPIKeysPageInput struct {
	Position     string `json:"position"`
	IncludeValue string `json:"includeValue"`
	Limit        int    `json:"limit"`
}

type deleteAPIKeyInput struct {
	APIKeyID string `json:"apiKeyId"`
}

type updateAPIKeyInput struct {
	UpdateAPIKeyInput
	APIKeyID string `json:"apiKeyId"`
}

// parseAPIGWAPIKeysPath handles /apikeys/... paths.
func parseAPIGWAPIKeysPath(method string, segs []string, n int, query url.Values) (string, map[string]string, bool) {
	switch {
	// GET /apikeys → GetApiKeys
	case n == 1 && method == http.MethodGet:
		return opGetAPIKeys, nil, true
	// POST /apikeys?mode=import → ImportApiKeys. POST /apikeys (no mode) → CreateAPIKey.
	case n == 1 && method == http.MethodPost && query.Get("mode") == modeImport:
		return opImportAPIKeys, nil, true
	case n == 1 && method == http.MethodPost:
		return opCreateAPIKey, nil, true
	// GET /apikeys/{id} → GetApiKey
	case n == pathDepth2 && method == http.MethodGet:
		return opGetAPIKey, map[string]string{keyAPIKeyID: segs[1]}, true
	// DELETE /apikeys/{id} → DeleteApiKey
	case n == pathDepth2 && method == http.MethodDelete:
		return opDeleteAPIKey, map[string]string{keyAPIKeyID: segs[1]}, true
	// PATCH /apikeys/{id} → UpdateApiKey
	case n == pathDepth2 && method == http.MethodPatch:
		return opUpdateAPIKey, map[string]string{keyAPIKeyID: segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

func (h *Handler) getAPIKeyAction(b []byte) (int, any, error) {
	var input getAPIKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	key, err := h.Backend.GetAPIKey(input.APIKeyID)
	if err != nil {
		return 0, nil, err
	}
	if input.IncludeValue != litTrue {
		key.Value = ""
	}

	return http.StatusOK, key, nil
}

func (h *Handler) getAPIKeysAction(b []byte) (int, any, error) {
	var input getAPIKeysPageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	keys, position, err := h.fetchAPIKeys(input)
	if err != nil {
		return 0, nil, err
	}
	if input.IncludeValue != litTrue {
		for i := range keys {
			keys[i].Value = ""
		}
	}
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: keys, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: keys}, nil
}

func (h *Handler) fetchAPIKeys(input getAPIKeysPageInput) ([]APIKey, string, error) {
	if input.Limit == 0 && input.Position == "" {
		keys, err := h.Backend.GetAPIKeys()

		return keys, "", err
	}

	return h.Backend.GetAPIKeysPage(input.Limit, input.Position)
}

func (h *Handler) deleteAPIKeyAction(b []byte) (int, any, error) {
	var input deleteAPIKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteAPIKey(input.APIKeyID); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

// apiKeyActions returns the action map for API key CRUD operations.
func (h *Handler) apiKeyActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateAPIKey: func(b []byte) (int, any, error) {
			var input CreateAPIKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			key, err := h.Backend.CreateAPIKey(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, key, nil
		},
		opGetAPIKey:    h.getAPIKeyAction,
		opGetAPIKeys:   h.getAPIKeysAction,
		opDeleteAPIKey: h.deleteAPIKeyAction,
		opUpdateAPIKey: func(b []byte) (int, any, error) {
			var input updateAPIKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			key, err := h.Backend.UpdateAPIKey(input.APIKeyID, input.UpdateAPIKeyInput)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, key, nil
		},
	}
}
