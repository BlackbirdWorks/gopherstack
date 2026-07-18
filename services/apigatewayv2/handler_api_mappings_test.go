package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateApiMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		setup        func(h *apigatewayv2.Handler) (apiID, stageName, domainName string)
		extraBody    map[string]any
		name         string
		domainName   string
		wantStatus   int
		wantAPIIDSet bool
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/stages", apiID),
					map[string]any{"stageName": "prod"},
				)
				require.Equal(t, http.StatusCreated, rr.Code)
				rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "example.com"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return apiID, "prod", "example.com"
			},
			wantStatus:   http.StatusCreated,
			wantAPIIDSet: true,
		},
		{
			name: "with_mapping_key",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/stages", apiID),
					map[string]any{"stageName": "dev"},
				)
				require.Equal(t, http.StatusCreated, rr.Code)
				rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "example.com"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return apiID, "dev", "example.com"
			},
			extraBody:  map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "domain_not_found",
			domainName: "missing.com",
			body:       map[string]any{"apiId": "abc123", "stage": "prod"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			domainName: "example.com",
			body:       "not-json",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "example.com"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return "", "", "example.com"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var apiID, stageName, domainName string
			domainName = tt.domainName

			if tt.setup != nil {
				apiID, stageName, domainName = tt.setup(h)
			}

			path := fmt.Sprintf("/v2/domainnames/%s/apimappings", domainName)

			var rr *httptest.ResponseRecorder

			if tt.body != nil {
				if s, ok := tt.body.(string); ok {
					rr = doRequestRaw(t, h, path, s)
				} else {
					rr = doRequest(t, h, http.MethodPost, path, tt.body)
				}
			} else {
				body := map[string]any{"apiId": apiID, "stage": stageName}
				maps.Copy(body, tt.extraBody)
				rr = doRequest(t, h, http.MethodPost, path, body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantAPIIDSet {
				var mapping apigatewayv2.APIMapping
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &mapping))
				assert.Equal(t, apiID, mapping.APIID)
				assert.NotEmpty(t, mapping.APIMappingID)
			}
		})
	}
}

func TestHandler_GetAPIMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		domainName  string
		wantStatus  int
		mappingsCnt int
	}{
		{
			name:        "found_empty",
			domainName:  "example.com",
			wantStatus:  http.StatusOK,
			mappingsCnt: 0,
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent.com",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			// Create a stage for the api mapping
			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodGet, "/v2/domainnames/"+tt.domainName+"/apimappings", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.APIMapping `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.mappingsCnt)
			}
		})
	}
}

func TestHandler_GetAPIMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useBadDN   bool
		useBadID   bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "bad_domain",
			useBadDN:   true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_mapping_id",
			useBadID:   true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, "/v2/domainnames/example.com/apimappings", map[string]any{
				"apiId": apiID,
				"stage": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var mapping apigatewayv2.APIMapping
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &mapping))

			domainName := "example.com"
			mappingID := mapping.APIMappingID

			if tt.useBadDN {
				domainName = "bad.com"
			}

			if tt.useBadID {
				mappingID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/domainnames/%s/apimappings/%s", domainName, mappingID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteAPIMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useBadDN   bool
		useBadID   bool
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "bad_domain",
			useBadDN:   true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_mapping_id",
			useBadID:   true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, "/v2/domainnames/example.com/apimappings", map[string]any{
				"apiId": apiID,
				"stage": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var mapping apigatewayv2.APIMapping
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &mapping))

			domainName := "example.com"
			mappingID := mapping.APIMappingID

			if tt.useBadDN {
				domainName = "bad.com"
			}

			if tt.useBadID {
				mappingID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/domainnames/%s/apimappings/%s", domainName, mappingID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateAPIMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (domainName, mappingID string)
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success_update_key",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				createStage(t, h, apiID, "prod")
				createDomainName(t, h, "api.example.com")
				rr := doRequest(t, h, http.MethodPost, "/v2/domainnames/api.example.com/apimappings", map[string]any{
					"apiId": apiID,
					"stage": "prod",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var m apigatewayv2.APIMapping
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &m))

				return "api.example.com", m.APIMappingID
			},
			body:       map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusOK,
		},
		{
			name: "domain_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent.com", "mapping123"
			},
			body:       map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "mapping_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				createDomainName(t, h, "api.example.com")

				return "api.example.com", "nonexistent"
			},
			body:       map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			domainName, mappingID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/domainnames/%s/apimappings/%s", domainName, mappingID), tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestCreateAPIMapping_DuplicateKeyConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainName(t, h, "api.example.com")

	apiID := createAPI(t, h, "mapping-api")
	createStage(t, h, apiID, "prod")

	mappingPath := "/v2/domainnames/api.example.com/apimappings"

	// First mapping with key "v1" succeeds.
	rr := doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId":         apiID,
		"stage":         "prod",
		"apiMappingKey": "v1",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	// Duplicate key on same domain → 409 ConflictException.
	rr = doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId":         apiID,
		"stage":         "prod",
		"apiMappingKey": "v1",
	})
	assert.Equal(t, http.StatusConflict, rr.Code)
	assert.Equal(t, "ConflictException", rr.Header().Get(errTypeHeaderKey))

	// A different key still succeeds.
	rr = doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId":         apiID,
		"stage":         "prod",
		"apiMappingKey": "v2",
	})
	assert.Equal(t, http.StatusCreated, rr.Code)
}

func TestCreateAPIMapping_DuplicateDefaultKeyConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainName(t, h, "default.example.com")

	apiID := createAPI(t, h, "default-mapping-api")
	createStage(t, h, apiID, "beta")

	mappingPath := "/v2/domainnames/default.example.com/apimappings"

	// Empty (default) mapping key.
	rr := doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId": apiID,
		"stage": "beta",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	// Second default mapping is a conflict.
	rr = doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId": apiID,
		"stage": "beta",
	})
	assert.Equal(t, http.StatusConflict, rr.Code)
}
