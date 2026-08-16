package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestHandler_CreateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantDomain string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"domainName": "example.com"},
			wantStatus: http.StatusCreated,
			wantDomain: "example.com",
		},
		{
			name:       "with_tags",
			body:       map[string]any{"domainName": "tagged.com", "tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusCreated,
			wantDomain: "tagged.com",
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/domainnames", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantDomain != "" {
				var dn apigatewayv2.DomainName
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))
				assert.Equal(t, tt.wantDomain, dn.DomainNameValue)
			}
		})
	}
}

func TestHandler_DuplicateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate_domain_name_returns_409",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetDomainNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainCnt  int
		wantStatus int
	}{
		{
			name:       "empty_list",
			domainCnt:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			domainCnt:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.domainCnt {
				rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
					"domainName": fmt.Sprintf("domain%d.example.com", i),
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/domainnames", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.DomainName `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.domainCnt)
			}
		})
	}
}

func TestHandler_GetDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		wantStatus int
	}{
		{
			name:       "found",
			domain:     "example.com",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			domain:     "nonexistent.com",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodGet, "/v2/domainnames/"+tt.domain, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var dn apigatewayv2.DomainName
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))
				assert.Equal(t, "example.com", dn.DomainNameValue)
			}
		})
	}
}

func TestHandler_DeleteDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		wantStatus int
	}{
		{
			name:       "success",
			domain:     "example.com",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			domain:     "nonexistent.com",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodDelete, "/v2/domainnames/"+tt.domain, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				createDomainName(t, h, "example.com")

				return "example.com"
			},
			body:       map[string]any{"tags": map[string]string{"env": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent.com"
			},
			body:       map[string]any{"tags": map[string]string{}},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			domainName := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				"/v2/domainnames/"+domainName, tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_CreateDomainName_WithConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantConfig bool
	}{
		{
			name: "with_configurations",
			body: map[string]any{
				"domainName": "api.example.com",
				"domainNameConfigurations": []map[string]any{
					{"certificateArn": "arn:aws:acm:us-east-1:123:certificate/abc", "endpointType": "REGIONAL"},
				},
			},
			wantStatus: http.StatusCreated,
			wantConfig: true,
		},
		{
			name: "without_configurations",
			body: map[string]any{
				"domainName": "plain.example.com",
			},
			wantStatus: http.StatusCreated,
			wantConfig: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantConfig {
				var dn apigatewayv2.DomainName
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))
				require.Len(t, dn.DomainNameConfigurations, 1)
				assert.Equal(t, "AVAILABLE", dn.DomainNameConfigurations[0].DomainNameStatus)
				assert.Equal(t, "REGIONAL", dn.DomainNameConfigurations[0].EndpointType)
			}
		})
	}
}

// TestDomainNameConfigurationsNullBug verifies that DomainName responses always
// include "domainNameConfigurations" as [] when no configurations are provided.
// AWS always returns domainNameConfigurations:[] even when empty.
func TestDomainNameConfigurationsNullBug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantConfigsEmpty bool
	}{
		{
			name:             "no_configs_returns_empty_array",
			body:             map[string]any{"domainName": "api.no-configs.example.com"},
			wantConfigsEmpty: true,
		},
		{
			name: "with_configs_returns_them",
			body: map[string]any{
				"domainName": "api.with-configs.example.com",
				"domainNameConfigurations": []map[string]any{
					{
						"certificateArn": "arn:aws:acm:us-east-1:123:certificate/abc",
						"endpointType":   "REGIONAL",
					},
				},
			},
			wantConfigsEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", tt.body)
			require.Equal(t, http.StatusCreated, rr.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))

			_, hasKey := raw["domainNameConfigurations"]
			assert.True(t, hasKey, "domainNameConfigurations key must always be present in DomainName response")

			var dn apigatewayv2.DomainName
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))

			if tt.wantConfigsEmpty {
				assert.Empty(t, dn.DomainNameConfigurations,
					"domainNameConfigurations should be empty array, not absent")
			} else {
				assert.NotEmpty(t, dn.DomainNameConfigurations)
			}
		})
	}
}
