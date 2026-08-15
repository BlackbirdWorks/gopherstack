package wafv2_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
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

// TestHandler_ListAPIKeys drives the real aws-sdk-go-v2 client rather than
// asserting a raw response map: ListAPIKeysOutput wraps items under
// "APIKeySummaries" (deserializers.go's
// awsAwsjson11_deserializeOpDocumentListAPIKeysOutput), not "APIKeys" -- a
// raw-body assertion on resp["APIKeys"] passed cleanly against the old,
// wrong wrapper key because the handler and the test agreed on it. Only a
// typed client's APIKeySummaries field can prove the key is right.
func TestHandler_ListAPIKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      types.Scope
		setupScope types.Scope
		setupCount int
		wantCount  int
	}{
		{
			name:      "empty",
			scope:     types.ScopeRegional,
			wantCount: 0,
		},
		{
			name:       "list_regional",
			setupScope: types.ScopeRegional,
			setupCount: 2,
			scope:      types.ScopeRegional,
			wantCount:  2,
		},
		{
			name:       "filter_scope_match",
			setupScope: types.ScopeRegional,
			setupCount: 1,
			scope:      types.ScopeRegional,
			wantCount:  1,
		},
		{
			name:       "filter_scope_no_match",
			setupScope: types.ScopeRegional,
			setupCount: 1,
			scope:      types.ScopeCloudfront,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestWAFV2Client(t, h)

			for range tt.setupCount {
				_, err := client.CreateAPIKey(t.Context(), &wafv2sdk.CreateAPIKeyInput{
					Scope:        tt.setupScope,
					TokenDomains: []string{"example.com"},
				})
				require.NoError(t, err)
			}

			out, err := client.ListAPIKeys(t.Context(), &wafv2sdk.ListAPIKeysInput{Scope: tt.scope})
			require.NoError(t, err)
			require.Len(t, out.APIKeySummaries, tt.wantCount)

			for _, summary := range out.APIKeySummaries {
				assert.NotNil(t, summary.CreationTimestamp)
				assert.False(t, summary.CreationTimestamp.IsZero())
			}
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

// TestHandler_GetDecryptedAPIKey_CreationTimestamp drives the real
// aws-sdk-go-v2 client: GetDecryptedAPIKeyOutput.CreationTimestamp is a real,
// always-populated member (deserializers.go's smithytime.ParseEpochSeconds
// case) that the handler previously never emitted at all.
func TestHandler_GetDecryptedAPIKey_CreationTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	created, err := client.CreateAPIKey(t.Context(), &wafv2sdk.CreateAPIKeyInput{
		Scope:        types.ScopeRegional,
		TokenDomains: []string{"example.com"},
	})
	require.NoError(t, err)

	out, err := client.GetDecryptedAPIKey(t.Context(), &wafv2sdk.GetDecryptedAPIKeyInput{
		Scope:  types.ScopeRegional,
		APIKey: created.APIKey,
	})
	require.NoError(t, err)
	require.NotNil(t, out.CreationTimestamp)
	assert.False(t, out.CreationTimestamp.IsZero())
	assert.Equal(t, []string{"example.com"}, out.TokenDomains)
}
