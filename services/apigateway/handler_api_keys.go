package apigateway

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

type getAPIKeyInput struct {
	APIKeyID     string `json:"apiKeyId"`
	IncludeValue string `json:"includeValue"`
}

type getAPIKeysPageInput struct {
	Position     string `json:"position"`
	CustomerID   string `json:"customerId"`
	NameQuery    string `json:"name"`
	IncludeValue string `json:"includeValues"`
	Limit        int    `json:"limit"`
}

type deleteAPIKeyInput struct {
	APIKeyID string `json:"apiKeyId"`
}

type updateAPIKeyInput struct {
	APIKeyID string `json:"apiKeyId"`
	UpdateAPIKeyInput
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
	if input.CustomerID == "" && input.NameQuery == "" {
		if input.Limit == 0 && input.Position == "" {
			keys, err := h.Backend.GetAPIKeys()

			return keys, "", err
		}

		return h.Backend.GetAPIKeysPage(input.Limit, input.Position)
	}

	keys, err := h.Backend.GetAPIKeys()
	if err != nil {
		return nil, "", err
	}
	keys = filterAPIKeys(keys, input.CustomerID, input.NameQuery)
	if input.Limit == 0 && input.Position == "" {
		return keys, "", nil
	}
	page, position := paginatePageByKey(keys, input.Limit, input.Position, func(k APIKey) string { return k.ID })

	return page, position, nil
}

// filterAPIKeys applies GetApiKeys' customerId (exact match) and nameQuery
// (substring match) filters. Real key: customerId, name.Query in
// apigateway@v1.42.4/serializers.go:4102,4114.
func filterAPIKeys(keys []APIKey, customerID, nameQuery string) []APIKey {
	out := make([]APIKey, 0, len(keys))
	for _, k := range keys {
		if customerID != "" && k.CustomerID != customerID {
			continue
		}
		if nameQuery != "" && !strings.Contains(k.Name, nameQuery) {
			continue
		}
		out = append(out, k)
	}

	return out
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
