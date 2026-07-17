package wafv2_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAndDeleteAPIKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		deleteBody func(apiKey string) map[string]any
		name       string
		wantCreate int
		wantDelete int
	}{
		{
			name:       "success",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(apiKey string) map[string]any {
				return map[string]any{"Scope": "REGIONAL", "APIKey": apiKey}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusOK,
		},
		{
			name:       "create_missing_scope",
			createBody: map[string]any{"TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any { return nil },
			wantCreate: http.StatusBadRequest,
		},
		{
			name:       "delete_missing_scope",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any {
				return map[string]any{"APIKey": "somekey"}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusBadRequest,
		},
		{
			name:       "delete_missing_key",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any {
				return map[string]any{"Scope": "REGIONAL"}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusBadRequest,
		},
		{
			name:       "delete_not_found",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any {
				return map[string]any{"Scope": "REGIONAL", "APIKey": "nonexistent-key"}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRec := doWafv2Request(t, h, "CreateAPIKey", tt.createBody)
			assert.Equal(t, tt.wantCreate, createRec.Code)

			if tt.wantCreate != http.StatusOK || tt.deleteBody == nil {
				return
			}

			var createResult map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResult))
			apiKey, _ := createResult["APIKey"].(string)

			deleteRec := doWafv2Request(t, h, "DeleteAPIKey", tt.deleteBody(apiKey))
			assert.Equal(t, tt.wantDelete, deleteRec.Code)
		})
	}
}

func TestAPIKeyBase64Encoding(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{"example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	apiKey, ok := resp["APIKey"].(string)
	require.True(t, ok)
	require.NotEmpty(t, apiKey)

	// The returned key should be base64-encoded.
	decoded, err := base64.StdEncoding.DecodeString(apiKey)
	require.NoError(t, err, "APIKey should be base64-encoded")
	assert.NotEmpty(t, string(decoded))

	// Validate TokenDomains limit (1–5).
	recTooMany := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{"a.com", "b.com", "c.com", "d.com", "e.com", "f.com"},
	})
	assert.Equal(t, http.StatusBadRequest, recTooMany.Code)

	// Validate TokenDomains minimum.
	recNone := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{},
	})
	assert.Equal(t, http.StatusBadRequest, recNone.Code)
}

// ---- Gap 24: Error header x-amzn-errortype ---------------------------------

func TestHandler_ListAPIKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		setupScope string
		setupCount int
		wantCount  int
		wantStatus int
	}{
		{
			name:       "empty",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "list_all",
			setupScope: "REGIONAL",
			setupCount: 2,
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "filter_scope_match",
			setupScope: "REGIONAL",
			setupCount: 1,
			scope:      "REGIONAL",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter_scope_no_match",
			setupScope: "REGIONAL",
			setupCount: 1,
			scope:      "CLOUDFRONT",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.setupCount {
				rec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
					"Scope":        tt.setupScope,
					"TokenDomains": []string{"example.com"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doWafv2Request(t, h, "ListAPIKeys", map[string]any{"Scope": tt.scope})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, ok := resp["APIKeys"].([]any)
			require.True(t, ok)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

func TestHandler_GetDecryptedAPIKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		apiKey     string
		createKey  bool
		wantStatus int
	}{
		{
			name:       "found",
			scope:      "REGIONAL",
			createKey:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_scope",
			apiKey:     "somekey",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_apikey",
			scope:      "REGIONAL",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			scope:      "REGIONAL",
			apiKey:     "nonexistent-key",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			apiKey := tt.apiKey
			if tt.createKey {
				rec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
					"Scope":        "REGIONAL",
					"TokenDomains": []string{"example.com"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				apiKey, _ = resp["APIKey"].(string)
				require.NotEmpty(t, apiKey)
			}

			body := map[string]any{}
			if tt.scope != "" {
				body["Scope"] = tt.scope
			}
			if apiKey != "" {
				body["APIKey"] = apiKey
			}

			rec := doWafv2Request(t, h, "GetDecryptedAPIKey", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Scope")
				assert.Contains(t, resp, "TokenDomains")
			}
		})
	}
}
