package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_CreateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantDomainName string
		wantStatus     int
	}{
		{
			name: "creates_domain_name_successfully",
			body: map[string]any{
				"domainName":     "api.example.com",
				"certificateArn": "arn:aws:acm:us-east-1:000:certificate/abc",
				"description":    "my domain",
			},
			wantStatus:     http.StatusCreated,
			wantDomainName: "api.example.com",
		},
		{
			name:       "missing_domain_name_returns_400",
			body:       map[string]any{"certificateArn": "arn:aws:acm:us-east-1:000:certificate/abc"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_certificate_arn_returns_400",
			body:       map[string]any{"domainName": "api.example.com"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()

			rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDomainName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				cfg, ok := resp["domainNameConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantDomainName, cfg["domainName"])
			}
		})
	}
}

func TestHandler_AssociateApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) (string, string)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "associates_api_successfully",
			setup: func(b *appsync.InMemoryBackend) (string, string) {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				dn, _ := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)

				return dn.DomainName, api.APIID
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "returns_404_for_missing_domain",
			setup: func(_ *appsync.InMemoryBackend) (string, string) {
				return "missing.example.com", "someapiid"
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_api_id_returns_400",
			setup: func(b *appsync.InMemoryBackend) (string, string) {
				dn, _ := b.CreateDomainName("api2.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)

				return dn.DomainName, ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			domainName, apiID := tt.setup(b)

			var body map[string]any
			if apiID != "" {
				body = map[string]any{"apiId": apiID}
			} else {
				body = map[string]any{}
			}

			rec := doRequest(t, h, http.MethodPost, "/v1/domainnames/"+domainName+"/apiassociation", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateDomainName_InvalidDomain(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	body := map[string]any{
		"domainName":     "notadomain",
		"certificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc",
	}
	rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DomainNameCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create.
	createBody := map[string]any{"domainName": "api.example.com", "certificateArn": certARN}
	rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	// List.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/domainnames", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	dns := listResp["domainNameConfigs"].([]any)
	assert.Len(t, dns, 1)

	// Get.
	rec3 := doRequest(t, h, http.MethodGet, "/v1/domainnames/api.example.com", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	// Delete.
	rec4 := doRequest(t, h, http.MethodDelete, "/v1/domainnames/api.example.com", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// After delete, get returns 404.
	rec5 := doRequest(t, h, http.MethodGet, "/v1/domainnames/api.example.com", nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_GetApiAssociation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create domain name.
	createBody := map[string]any{"domainName": "api.example.com", "certificateArn": certARN}
	doRequest(t, h, http.MethodPost, "/v1/domainnames", createBody)

	// Get association (no API associated yet).
	rec := doRequest(t, h, http.MethodGet, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DisassociateAPI(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create domain name.
	doRequest(t, h, http.MethodPost, "/v1/domainnames", map[string]any{
		"domainName": "api.example.com", "certificateArn": certARN,
	})

	// Create and associate a GraphQL API.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name": "TestAPI", "authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&apiResp))
	apiID := apiResp["graphqlApi"].(map[string]any)["apiId"].(string)

	doRequest(t, h, http.MethodPost, "/v1/domainnames/api.example.com/apiassociation",
		map[string]any{"apiId": apiID})

	// Disassociate.
	rec3 := doRequest(t, h, http.MethodDelete, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusNoContent, rec3.Code)

	// Second disassociate returns 404.
	rec4 := doRequest(t, h, http.MethodDelete, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}

func TestHandler_UpdateDomainName(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	doRequest(t, h, http.MethodPost, "/v1/domainnames", map[string]any{
		"domainName": "api.example.com", "certificateArn": certARN,
	})

	rec := doRequest(t, h, http.MethodPut, "/v1/domainnames/api.example.com",
		map[string]any{"description": "updated description"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	dn := resp["domainNameConfig"].(map[string]any)
	assert.Equal(t, "updated description", dn["description"])
}

func TestHandler_DisassociateAPI_DomainNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodDelete, "/v1/domainnames/missing.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DomainName_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// CONNECT on domain names collection → 405.
	rec := doRequest(t, h, http.MethodConnect, "/v1/domainnames", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// CONNECT on domain name item → 405.
	rec2 := doRequest(t, h, http.MethodConnect, "/v1/domainnames/api.example.com", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec2.Code)

	// CONNECT on apiassociation → 405.
	rec3 := doRequest(t, h, http.MethodConnect, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec3.Code)
}

func TestHandler_AssociateAPI_InvalidAPIID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	_, err := b.CreateDomainName("assoc.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/v1/domainnames/assoc.example.com/ApiAssociation", map[string]any{
		"apiId": "nonexistent-api",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
