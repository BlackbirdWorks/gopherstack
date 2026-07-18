package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestInMemoryBackend_OIDCProvider(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetOIDCProvider", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateOpenIDConnectProvider(
			"https://token.actions.githubusercontent.com",
			[]string{"sts.amazonaws.com"},
			[]string{"6938fd4d98bab03faadb97b34396831e3780aea1"},
		)
		require.NoError(t, err)
		assert.Contains(t, p.Arn, "oidc-provider/token.actions.githubusercontent.com")
		assert.Equal(t, "https://token.actions.githubusercontent.com", p.URL)
		assert.Equal(t, []string{"sts.amazonaws.com"}, p.ClientIDList)
		assert.Equal(t, []string{"6938fd4d98bab03faadb97b34396831e3780aea1"}, p.ThumbprintList)

		got, err := b.GetOpenIDConnectProvider(p.Arn)
		require.NoError(t, err)
		assert.Equal(t, p.Arn, got.Arn)
	})

	t.Run("CreateOIDCProvider_BareHostname", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateOpenIDConnectProvider("token.actions.githubusercontent.com", nil, nil)
		require.NoError(t, err)
		assert.Contains(t, p.Arn, "oidc-provider/token.actions.githubusercontent.com")
	})

	t.Run("CreateOIDCProvider_BareHostnameWithPath", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		// Bare hostname with a trailing path should strip the path.
		p, err := b.CreateOpenIDConnectProvider("example.com/v1/oidc", nil, nil)
		require.NoError(t, err)
		assert.Contains(t, p.Arn, "oidc-provider/example.com")
		assert.NotContains(t, p.Arn, "/v1/oidc")
	})

	t.Run("CreateOIDCProvider_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateOpenIDConnectProvider("https://example.com", nil, nil)
		require.NoError(t, err)
		_, err = b.CreateOpenIDConnectProvider("https://example.com", nil, nil)
		require.ErrorIs(t, err, iam.ErrOIDCProviderAlreadyExists)
	})

	t.Run("GetOIDCProvider_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetOpenIDConnectProvider("arn:aws:iam::000000000000:oidc-provider/ghost")
		require.ErrorIs(t, err, iam.ErrOIDCProviderNotFound)
	})

	t.Run("UpdateOIDCProviderThumbprint", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateOpenIDConnectProvider(
			"https://example.com",
			nil,
			[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
		)
		require.NoError(t, err)

		err = b.UpdateOpenIDConnectProviderThumbprint(p.Arn, []string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"})
		require.NoError(t, err)

		got, err := b.GetOpenIDConnectProvider(p.Arn)
		require.NoError(t, err)
		assert.Equal(t, []string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"}, got.ThumbprintList)
	})

	t.Run("UpdateOIDCProviderThumbprint_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.UpdateOpenIDConnectProviderThumbprint(
			"arn:aws:iam::000000000000:oidc-provider/ghost",
			[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
		)
		require.ErrorIs(t, err, iam.ErrOIDCProviderNotFound)
	})

	t.Run("DeleteOIDCProvider", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateOpenIDConnectProvider("https://example.com", nil, nil)
		require.NoError(t, err)

		err = b.DeleteOpenIDConnectProvider(p.Arn)
		require.NoError(t, err)

		_, err = b.GetOpenIDConnectProvider(p.Arn)
		require.ErrorIs(t, err, iam.ErrOIDCProviderNotFound)
	})

	t.Run("DeleteOIDCProvider_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteOpenIDConnectProvider("arn:aws:iam::000000000000:oidc-provider/ghost")
		require.ErrorIs(t, err, iam.ErrOIDCProviderNotFound)
	})

	t.Run("ListOIDCProviders", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateOpenIDConnectProvider("https://z.example.com", nil, nil)
		_, _ = b.CreateOpenIDConnectProvider("https://a.example.com", nil, nil)

		providers, err := b.ListOpenIDConnectProviders()
		require.NoError(t, err)
		require.Len(t, providers, 2)
		// Sorted by ARN: a before z.
		assert.Contains(t, providers[0].Arn, "a.example.com")
		assert.Contains(t, providers[1].Arn, "z.example.com")
	})

	t.Run("ListOIDCProviders_Empty", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		providers, err := b.ListOpenIDConnectProviders()
		require.NoError(t, err)
		assert.Empty(t, providers)
	})
}

// ---- Login Profile backend tests ----

func TestIAMHandler_OIDCProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		setup  func(*iam.InMemoryBackend) string
		check  func(*testing.T, *httptest.ResponseRecorder, *iam.InMemoryBackend)
		name   string
		action string
	}{
		{
			name:   "CreateOpenIDConnectProvider",
			action: "CreateOpenIDConnectProvider",
			params: map[string]string{
				"Url":                     "https://token.actions.githubusercontent.com",
				"ClientIDList.member.1":   "sts.amazonaws.com",
				"ThumbprintList.member.1": "6938fd4d98bab03faadb97b34396831e3780aea1",
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.CreateOpenIDConnectProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp.CreateOpenIDConnectProviderResult.OpenIDConnectProviderArn, "oidc-provider")
			},
		},
		{
			name:   "GetOpenIDConnectProvider",
			action: "GetOpenIDConnectProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider(
					"https://example.com", []string{"client-1"}, []string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
				)

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.GetOpenIDConnectProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "https://example.com", resp.GetOpenIDConnectProviderResult.URL)
				assert.Equal(t, []string{"client-1"}, resp.GetOpenIDConnectProviderResult.ClientIDList)
				assert.Equal(
					t,
					[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
					resp.GetOpenIDConnectProviderResult.ThumbprintList,
				)
			},
		},
		{
			name:   "UpdateOpenIDConnectProviderThumbprint",
			action: "UpdateOpenIDConnectProviderThumbprint",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider(
					"https://example.com",
					nil,
					[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
				)

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, b *iam.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.UpdateOpenIDConnectProviderThumbprintResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.ResponseMetadata.RequestID)

				// Verify thumbprint was actually updated in the backend.
				providers, err := b.ListOpenIDConnectProviders()
				require.NoError(t, err)
				require.Len(t, providers, 1)
				assert.Equal(t, []string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"}, providers[0].ThumbprintList)
			},
		},
		{
			name:   "DeleteOpenIDConnectProvider",
			action: "DeleteOpenIDConnectProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://example.com", nil, nil)

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.DeleteOpenIDConnectProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			},
		},
		{
			name:   "ListOpenIDConnectProviders",
			action: "ListOpenIDConnectProviders",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateOpenIDConnectProvider("https://example.com", nil, nil)

				return ""
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.ListOpenIDConnectProvidersResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.ListOpenIDConnectProvidersResult.OpenIDConnectProviderList, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)

			var providerArn string
			if tt.setup != nil {
				providerArn = tt.setup(b)
			}

			params := tt.params
			if params == nil {
				params = map[string]string{}
			}

			if providerArn != "" {
				params["OpenIDConnectProviderArn"] = providerArn
				params["ThumbprintList.member.1"] = "9e99a48a9960b14926bb7f3b02e22da2b0ab7280"
			}

			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			tt.check(t, rec, b)
		})
	}
}

// ---- OIDC Provider handler error tests ----

func TestIAMHandler_OIDCProvider_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		params     map[string]string
		setup      func(*iam.InMemoryBackend)
		wantCode   string
		wantStatus int
	}{
		{
			name:       "CreateOpenIDConnectProvider_AlreadyExists",
			action:     "CreateOpenIDConnectProvider",
			params:     map[string]string{"Url": "https://example.com"},
			setup:      func(b *iam.InMemoryBackend) { _, _ = b.CreateOpenIDConnectProvider("https://example.com", nil, nil) },
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "GetOpenIDConnectProvider_NotFound",
			action:     "GetOpenIDConnectProvider",
			params:     map[string]string{"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "UpdateOpenIDConnectProviderThumbprint_NotFound",
			action: "UpdateOpenIDConnectProviderThumbprint",
			params: map[string]string{
				"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/ghost",
				"ThumbprintList.member.1":  "990f41981148b53dc7c615a6b0c2a26555cc5d85",
			},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteOpenIDConnectProvider_NotFound",
			action:     "DeleteOpenIDConnectProvider",
			params:     map[string]string{"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// ---- Login Profile handler tests ----

func TestHandler_AccessAdvisorAndSTSPreferences(t *testing.T) {
	t.Parallel()

	t.Run("GetServiceLastAccessedDetails", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("GetServiceLastAccessedDetails", map[string]string{"JobId": "test-job-id"})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.GetServiceLastAccessedDetailsResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "COMPLETED", resp.GetServiceLastAccessedDetailsResult.JobStatus)
		assert.NotEmpty(t, resp.GetServiceLastAccessedDetailsResult.JobCreationDate)
		assert.NotEmpty(t, resp.GetServiceLastAccessedDetailsResult.JobCompletionDate)
		assert.False(t, resp.GetServiceLastAccessedDetailsResult.IsTruncated)
	})

	t.Run("SetSecurityTokenServicePreferences", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("SetSecurityTokenServicePreferences", map[string]string{
			"GlobalEndpointTokenVersion": "v2Token",
		})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.SetSecurityTokenServicePreferencesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
	})
}

// ---- GetSupportedOperations test ----

func TestRemoveClientIDFromOpenIDConnectProvider(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	oidc, err := b.CreateOpenIDConnectProvider(
		"https://example.com",
		[]string{"client-id-1"},
		[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
	)
	require.NoError(t, err)

	rec := callIAM(t, h, "RemoveClientIDFromOpenIDConnectProvider", map[string]string{
		"OpenIDConnectProviderArn": oidc.Arn,
		"ClientID":                 "client-id-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOIDCProvider_URLAndThumbprints(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	thumbprints := []string{
		"9e99a48a9960b14926bb7f3b02e22da2b0ab7280",
		"da7ade79a85e56b9b51aa2be5d0e50e7e0a8c5d3",
	}

	provider, err := b.CreateOpenIDConnectProvider(
		"https://token.example.com",
		[]string{"sts.amazonaws.com"},
		thumbprints,
	)
	require.NoError(t, err)

	got, err := b.GetOpenIDConnectProvider(provider.Arn)
	require.NoError(t, err)

	assert.Equal(t, "https://token.example.com", got.URL)
	assert.Equal(t, thumbprints, got.ThumbprintList)
	assert.Equal(t, []string{"sts.amazonaws.com"}, got.ClientIDList)
}

func TestOIDCProvider_ARNFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateOpenIDConnectProvider(
		"https://oidc.example.com",
		nil,
		[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
	)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(provider.Arn, "arn:aws:iam::"),
		"OIDC provider ARN must start with arn:aws:iam::")
	assert.Contains(t, provider.Arn, ":oidc-provider/",
		"OIDC provider ARN must contain :oidc-provider/")
}

func TestOIDCProvider_ListIncludesCreated(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, _ = b.CreateOpenIDConnectProvider(
		"https://oidc-a.example.com",
		nil,
		[]string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"},
	)
	_, _ = b.CreateOpenIDConnectProvider(
		"https://oidc-b.example.com",
		nil,
		[]string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"},
	)

	providers, err := b.ListOpenIDConnectProviders()
	require.NoError(t, err)
	assert.Len(t, providers, 2)
}

func TestOIDCProvider_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	clientIDs := []string{"client1"}
	thumbprints := []string{"990f41981148b53dc7c615a6b0c2a26555cc5d85"}

	p, err := b.CreateOpenIDConnectProvider("https://example.com", clientIDs, thumbprints)
	require.NoError(t, err)
	assert.Contains(t, p.Arn, "oidc-provider")

	got, err := b.GetOpenIDConnectProvider(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, clientIDs, got.ClientIDList)

	require.NoError(t, b.AddClientIDToOpenIDConnectProvider(p.Arn, "client2"))
	got2, _ := b.GetOpenIDConnectProvider(p.Arn)
	assert.Contains(t, got2.ClientIDList, "client2")

	require.NoError(t, b.RemoveClientIDFromOpenIDConnectProvider(p.Arn, "client1"))
	got3, _ := b.GetOpenIDConnectProvider(p.Arn)
	assert.NotContains(t, got3.ClientIDList, "client1")

	all, err := b.ListOpenIDConnectProviders()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	require.NoError(t, b.DeleteOpenIDConnectProvider(p.Arn))

	_, err = b.GetOpenIDConnectProvider(p.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrOIDCProviderNotFound)
}
