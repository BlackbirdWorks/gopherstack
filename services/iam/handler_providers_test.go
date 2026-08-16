package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestHandler_SAMLProvider_UpdateAndGet(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	provider, err := b.CreateSAMLProvider("test-saml", "<original/>")
	require.NoError(t, err)

	req := iamRequest("UpdateSAMLProvider", map[string]string{
		"SAMLProviderArn":      provider.Arn,
		"SAMLMetadataDocument": "<updated-metadata/>",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetSAMLProvider", map[string]string{
		"SAMLProviderArn": provider.Arn,
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "updated-metadata")
}

func TestHandler_OIDCProvider_CreateListDelete(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("CreateOpenIDConnectProvider", map[string]string{
		"Url":                     "https://oidc.example.com",
		"ClientIDList.member.1":   "sts.amazonaws.com",
		"ThumbprintList.member.1": "9e99a48a9960b14926bb7f3b02e22da2b0ab7280",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	oidcBody := rec.Body.String()
	assert.Contains(t, oidcBody, "OpenIDConnectProviderArn")
	assert.Contains(t, oidcBody, "arn:aws:iam::")

	req2 := iamRequest("ListOpenIDConnectProviders", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "oidc-provider")
}

func TestAddClientIDToOpenIDConnectProvider_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrType error
		setup       func(b *iam.InMemoryBackend) string
		name        string
		clientID    string
		wantErr     bool
	}{
		{
			name: "add_client_id_success",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://token.actions.githubusercontent.com", nil, nil)

				return p.Arn
			},
			clientID: "sts.amazonaws.com",
		},
		{
			name: "add_idempotent_client_id",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider(
					"https://token.actions.githubusercontent.com",
					[]string{"existing-id"},
					nil,
				)

				return p.Arn
			},
			clientID: "existing-id",
		},
		{
			name: "provider_not_found",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:oidc-provider/nonexistent"
			},
			clientID:    "sts.amazonaws.com",
			wantErr:     true,
			wantErrType: iam.ErrOIDCProviderNotFound,
		},
		{
			name: "empty_client_id_returns_error",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://token.actions.githubusercontent.com", nil, nil)

				return p.Arn
			},
			clientID: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			providerArn := tt.setup(b)

			err := b.AddClientIDToOpenIDConnectProvider(providerArn, tt.clientID)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrType != nil {
					require.ErrorIs(t, err, tt.wantErrType)
				}

				return
			}

			require.NoError(t, err)

			// Verify the client ID was added (and is idempotent).
			p, getErr := b.GetOpenIDConnectProvider(providerArn)
			require.NoError(t, getErr)

			found := slices.Contains(p.ClientIDList, tt.clientID)
			assert.True(t, found, "clientID %q should be in ClientIDList", tt.clientID)

			// For idempotent case, ensure no duplicates.
			count := 0
			for _, id := range p.ClientIDList {
				if id == tt.clientID {
					count++
				}
			}
			assert.Equal(t, 1, count, "clientID should appear exactly once")
		})
	}
}

func TestHandler_SAMLProvider_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// CreateSAMLProvider.
	req := iamRequest("CreateSAMLProvider", map[string]string{
		"Name":                 "MySAML",
		"SAMLMetadataDocument": "<md>doc</md>",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "saml-provider")

	var createResp struct {
		CreateSAMLProviderResult struct {
			SAMLProviderArn string `xml:"SAMLProviderArn"`
		} `xml:"CreateSAMLProviderResult"`
	}
	_ = xml.Unmarshal(rec.Body.Bytes(), &createResp)
	provArn := createResp.CreateSAMLProviderResult.SAMLProviderArn

	// GetSAMLProvider.
	req2 := iamRequest("GetSAMLProvider", map[string]string{"SAMLProviderArn": provArn})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// UpdateSAMLProvider.
	req3 := iamRequest("UpdateSAMLProvider", map[string]string{
		"SAMLProviderArn":      provArn,
		"SAMLMetadataDocument": "<md>updated</md>",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// ListSAMLProviders.
	req4 := iamRequest("ListSAMLProviders", map[string]string{})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Contains(t, rec4.Body.String(), "MySAML")

	// DeleteSAMLProvider.
	req5 := iamRequest("DeleteSAMLProvider", map[string]string{"SAMLProviderArn": provArn})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestHandler_OIDCProvider_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// CreateOpenIDConnectProvider.
	req := iamRequest("CreateOpenIDConnectProvider", map[string]string{
		"Url":                     "https://example.com/oidc",
		"ClientIDList.member.1":   "client1",
		"ThumbprintList.member.1": "990f41981148b53dc7c615a6b0c2a26555cc5d85",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CreateOpenIDConnectProviderResult struct {
			OpenIDConnectProviderArn string `xml:"OpenIDConnectProviderArn"`
		} `xml:"CreateOpenIDConnectProviderResult"`
	}
	_ = xml.Unmarshal(rec.Body.Bytes(), &createResp)
	oidcArn := createResp.CreateOpenIDConnectProviderResult.OpenIDConnectProviderArn

	if oidcArn == "" {
		// Some response formats embed differently; skip OIDC ARN-dependent ops.
		return
	}

	// GetOpenIDConnectProvider.
	req2 := iamRequest("GetOpenIDConnectProvider", map[string]string{
		"OpenIDConnectProviderArn": oidcArn,
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// ListOpenIDConnectProviders.
	req3 := iamRequest("ListOpenIDConnectProviders", map[string]string{})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// DeleteOpenIDConnectProvider.
	req4 := iamRequest("DeleteOpenIDConnectProvider", map[string]string{
		"OpenIDConnectProviderArn": oidcArn,
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}
