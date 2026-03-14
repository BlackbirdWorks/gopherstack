package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- SAML Provider backend tests ----

func TestInMemoryBackend_SAMLProvider(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetSAMLProvider", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateSAMLProvider("MySAML", "<saml-metadata/>")
		require.NoError(t, err)
		assert.Contains(t, p.Arn, "saml-provider/MySAML")
		assert.Equal(t, "<saml-metadata/>", p.SAMLMetadataDocument)

		got, err := b.GetSAMLProvider(p.Arn)
		require.NoError(t, err)
		assert.Equal(t, p.Arn, got.Arn)
	})

	t.Run("CreateSAMLProvider_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateSAMLProvider("MySAML", "<doc/>")
		require.NoError(t, err)
		_, err = b.CreateSAMLProvider("MySAML", "<doc/>")
		require.ErrorIs(t, err, iam.ErrSAMLProviderAlreadyExists)
	})

	t.Run("GetSAMLProvider_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetSAMLProvider("arn:aws:iam::000000000000:saml-provider/ghost")
		require.ErrorIs(t, err, iam.ErrSAMLProviderNotFound)
	})

	t.Run("UpdateSAMLProvider", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateSAMLProvider("MySAML", "<old/>")
		require.NoError(t, err)

		updated, err := b.UpdateSAMLProvider(p.Arn, "<new/>")
		require.NoError(t, err)
		assert.Equal(t, "<new/>", updated.SAMLMetadataDocument)

		got, err := b.GetSAMLProvider(p.Arn)
		require.NoError(t, err)
		assert.Equal(t, "<new/>", got.SAMLMetadataDocument)
	})

	t.Run("UpdateSAMLProvider_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.UpdateSAMLProvider("arn:aws:iam::000000000000:saml-provider/ghost", "<doc/>")
		require.ErrorIs(t, err, iam.ErrSAMLProviderNotFound)
	})

	t.Run("DeleteSAMLProvider", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		p, err := b.CreateSAMLProvider("MySAML", "<doc/>")
		require.NoError(t, err)

		err = b.DeleteSAMLProvider(p.Arn)
		require.NoError(t, err)

		_, err = b.GetSAMLProvider(p.Arn)
		require.ErrorIs(t, err, iam.ErrSAMLProviderNotFound)
	})

	t.Run("DeleteSAMLProvider_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteSAMLProvider("arn:aws:iam::000000000000:saml-provider/ghost")
		require.ErrorIs(t, err, iam.ErrSAMLProviderNotFound)
	})

	t.Run("ListSAMLProviders", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateSAMLProvider("ZProvider", "<z/>")
		_, _ = b.CreateSAMLProvider("AProvider", "<a/>")

		providers, err := b.ListSAMLProviders()
		require.NoError(t, err)
		require.Len(t, providers, 2)
		// Should be sorted by ARN, so A before Z.
		assert.Contains(t, providers[0].Arn, "AProvider")
		assert.Contains(t, providers[1].Arn, "ZProvider")
	})

	t.Run("ListSAMLProviders_Empty", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		providers, err := b.ListSAMLProviders()
		require.NoError(t, err)
		assert.Empty(t, providers)
	})
}

// ---- OIDC Provider backend tests ----

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
		p, err := b.CreateOpenIDConnectProvider("https://example.com", nil, []string{"old-thumb"})
		require.NoError(t, err)

		err = b.UpdateOpenIDConnectProviderThumbprint(p.Arn, []string{"new-thumb"})
		require.NoError(t, err)

		got, err := b.GetOpenIDConnectProvider(p.Arn)
		require.NoError(t, err)
		assert.Equal(t, []string{"new-thumb"}, got.ThumbprintList)
	})

	t.Run("UpdateOIDCProviderThumbprint_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.UpdateOpenIDConnectProviderThumbprint(
			"arn:aws:iam::000000000000:oidc-provider/ghost",
			[]string{"thumb"},
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

func TestInMemoryBackend_LoginProfile(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetLoginProfile", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		lp, err := b.CreateLoginProfile("alice", "Password123!", false)
		require.NoError(t, err)
		assert.Equal(t, "alice", lp.UserName)
		assert.False(t, lp.PasswordResetRequired)

		got, err := b.GetLoginProfile("alice")
		require.NoError(t, err)
		assert.Equal(t, "alice", got.UserName)
	})

	t.Run("CreateLoginProfile_UserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateLoginProfile("nobody", "Password123!", false)
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("CreateLoginProfile_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		_, err := b.CreateLoginProfile("alice", "Pass1", false)
		require.NoError(t, err)
		_, err = b.CreateLoginProfile("alice", "Pass2", false)
		require.ErrorIs(t, err, iam.ErrLoginProfileAlreadyExists)
	})

	t.Run("GetLoginProfile_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetLoginProfile("nobody")
		require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
	})

	t.Run("UpdateLoginProfile", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		_, err := b.CreateLoginProfile("alice", "Pass1", false)
		require.NoError(t, err)

		err = b.UpdateLoginProfile("alice", "NewPass", true)
		require.NoError(t, err)

		got, err := b.GetLoginProfile("alice")
		require.NoError(t, err)
		assert.True(t, got.PasswordResetRequired)
	})

	t.Run("UpdateLoginProfile_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.UpdateLoginProfile("nobody", "Pass", false)
		require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
	})

	t.Run("DeleteLoginProfile", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		_, err := b.CreateLoginProfile("alice", "Pass1", false)
		require.NoError(t, err)

		err = b.DeleteLoginProfile("alice")
		require.NoError(t, err)

		_, err = b.GetLoginProfile("alice")
		require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
	})

	t.Run("DeleteLoginProfile_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteLoginProfile("nobody")
		require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
	})
}

// ---- SAML Provider handler tests ----

func TestIAMHandler_SAMLProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		setup  func(*iam.InMemoryBackend) string
		check  func(*testing.T, *httptest.ResponseRecorder)
		name   string
		action string
	}{
		{
			name:   "CreateSAMLProvider",
			action: "CreateSAMLProvider",
			params: map[string]string{
				"Name":                 "MySAML",
				"SAMLMetadataDocument": "<EntityDescriptor/>",
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.CreateSAMLProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp.CreateSAMLProviderResult.SAMLProviderArn, "saml-provider/MySAML")
			},
		},
		{
			name:   "UpdateSAMLProvider",
			action: "UpdateSAMLProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateSAMLProvider("MySAML", "<old/>")

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.UpdateSAMLProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp.UpdateSAMLProviderResult.SAMLProviderArn, "saml-provider/MySAML")
			},
		},
		{
			name:   "DeleteSAMLProvider",
			action: "DeleteSAMLProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateSAMLProvider("MySAML", "<doc/>")

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.DeleteSAMLProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			},
		},
		{
			name:   "GetSAMLProvider",
			action: "GetSAMLProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateSAMLProvider("MySAML", "<meta/>")

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.GetSAMLProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "<meta/>", resp.GetSAMLProviderResult.SAMLMetadataDocument)
				assert.NotEmpty(t, resp.GetSAMLProviderResult.CreateDate)
			},
		},
		{
			name:   "ListSAMLProviders",
			action: "ListSAMLProviders",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateSAMLProvider("Provider1", "<doc/>")

				return ""
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.ListSAMLProvidersResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.ListSAMLProvidersResult.SAMLProviderList, 1)
				assert.Contains(t, resp.ListSAMLProvidersResult.SAMLProviderList[0].Arn, "Provider1")
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
				// Inject the ARN into the appropriate param.
				params["SAMLProviderArn"] = providerArn
			}

			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			tt.check(t, rec)
		})
	}
}

// ---- SAML Provider handler error tests ----

func TestIAMHandler_SAMLProvider_Errors(t *testing.T) {
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
			name:       "CreateSAMLProvider_AlreadyExists",
			action:     "CreateSAMLProvider",
			params:     map[string]string{"Name": "MySAML", "SAMLMetadataDocument": "<doc/>"},
			setup:      func(b *iam.InMemoryBackend) { _, _ = b.CreateSAMLProvider("MySAML", "<doc/>") },
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetSAMLProvider_NotFound",
			action:     "GetSAMLProvider",
			params:     map[string]string{"SAMLProviderArn": "arn:aws:iam::000000000000:saml-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "UpdateSAMLProvider_NotFound",
			action:     "UpdateSAMLProvider",
			params:     map[string]string{"SAMLProviderArn": "arn:aws:iam::000000000000:saml-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteSAMLProvider_NotFound",
			action:     "DeleteSAMLProvider",
			params:     map[string]string{"SAMLProviderArn": "arn:aws:iam::000000000000:saml-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
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

// ---- OIDC Provider handler tests ----

func TestIAMHandler_OIDCProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		setup  func(*iam.InMemoryBackend) string
		check  func(*testing.T, *httptest.ResponseRecorder)
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
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
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
					"https://example.com", []string{"client-1"}, []string{"thumb-1"},
				)

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.GetOpenIDConnectProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "https://example.com", resp.GetOpenIDConnectProviderResult.URL)
				assert.Equal(t, []string{"client-1"}, resp.GetOpenIDConnectProviderResult.ClientIDList)
				assert.Equal(t, []string{"thumb-1"}, resp.GetOpenIDConnectProviderResult.ThumbprintList)
			},
		},
		{
			name:   "UpdateOpenIDConnectProviderThumbprint",
			action: "UpdateOpenIDConnectProviderThumbprint",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://example.com", nil, []string{"old"})

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.UpdateOpenIDConnectProviderThumbprintResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			},
		},
		{
			name:   "DeleteOpenIDConnectProvider",
			action: "DeleteOpenIDConnectProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://example.com", nil, nil)

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
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
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
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
				params["ThumbprintList.member.1"] = "new-thumb"
			}

			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			tt.check(t, rec)
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
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetOpenIDConnectProvider_NotFound",
			action:     "GetOpenIDConnectProvider",
			params:     map[string]string{"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "UpdateOpenIDConnectProviderThumbprint_NotFound",
			action: "UpdateOpenIDConnectProviderThumbprint",
			params: map[string]string{
				"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/ghost",
				"ThumbprintList.member.1":  "thumb",
			},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteOpenIDConnectProvider_NotFound",
			action:     "DeleteOpenIDConnectProvider",
			params:     map[string]string{"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
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

func TestIAMHandler_LoginProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		setup  func(*iam.InMemoryBackend)
		check  func(*testing.T, *httptest.ResponseRecorder)
		name   string
		action string
	}{
		{
			name:   "CreateLoginProfile",
			action: "CreateLoginProfile",
			params: map[string]string{
				"UserName": "alice",
				"Password": "Password123!",
			},
			setup: func(b *iam.InMemoryBackend) { _, _ = b.CreateUser("alice", "/", "") },
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.CreateLoginProfileResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "alice", resp.CreateLoginProfileResult.LoginProfile.UserName)
				assert.NotEmpty(t, resp.CreateLoginProfileResult.LoginProfile.CreateDate)
			},
		},
		{
			name:   "CreateLoginProfile_PasswordResetRequired",
			action: "CreateLoginProfile",
			params: map[string]string{
				"UserName":              "bob",
				"Password":              "TempPass1!",
				"PasswordResetRequired": "true",
			},
			setup: func(b *iam.InMemoryBackend) { _, _ = b.CreateUser("bob", "/", "") },
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.CreateLoginProfileResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.True(t, resp.CreateLoginProfileResult.LoginProfile.PasswordResetRequired)
			},
		},
		{
			name:   "GetLoginProfile",
			action: "GetLoginProfile",
			params: map[string]string{"UserName": "alice"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateLoginProfile("alice", "Pass", false)
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.GetLoginProfileResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "alice", resp.GetLoginProfileResult.LoginProfile.UserName)
			},
		},
		{
			name:   "UpdateLoginProfile",
			action: "UpdateLoginProfile",
			params: map[string]string{"UserName": "alice", "Password": "NewPass"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateLoginProfile("alice", "OldPass", false)
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.UpdateLoginProfileResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			},
		},
		{
			name:   "DeleteLoginProfile",
			action: "DeleteLoginProfile",
			params: map[string]string{"UserName": "alice"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateLoginProfile("alice", "Pass", false)
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.DeleteLoginProfileResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			},
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
			tt.check(t, rec)
		})
	}
}

// ---- Login Profile handler error tests ----

func TestIAMHandler_LoginProfile_Errors(t *testing.T) {
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
			name:       "CreateLoginProfile_UserNotFound",
			action:     "CreateLoginProfile",
			params:     map[string]string{"UserName": "nobody", "Password": "Pass"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "CreateLoginProfile_AlreadyExists",
			action: "CreateLoginProfile",
			params: map[string]string{"UserName": "alice", "Password": "Pass"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateLoginProfile("alice", "Pass", false)
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetLoginProfile_NotFound",
			action:     "GetLoginProfile",
			params:     map[string]string{"UserName": "nobody"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "UpdateLoginProfile_NotFound",
			action:     "UpdateLoginProfile",
			params:     map[string]string{"UserName": "nobody", "Password": "Pass"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteLoginProfile_NotFound",
			action:     "DeleteLoginProfile",
			params:     map[string]string{"UserName": "nobody"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusBadRequest,
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

// ---- Miscellaneous handler tests ----

func TestIAMHandler_Misc(t *testing.T) {
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

// ---- GetSupportedOperations coverage test ----

func TestGetSupportedOperations_IncludesNewOps(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	ops := h.GetSupportedOperations()

	expectedNewOps := []string{
		"CreateSAMLProvider", "UpdateSAMLProvider", "DeleteSAMLProvider",
		"GetSAMLProvider", "ListSAMLProviders",
		"CreateOpenIDConnectProvider", "UpdateOpenIDConnectProviderThumbprint",
		"DeleteOpenIDConnectProvider", "GetOpenIDConnectProvider", "ListOpenIDConnectProviders",
		"CreateLoginProfile", "UpdateLoginProfile", "DeleteLoginProfile", "GetLoginProfile",
		"GetServiceLastAccessedDetails", "SetSecurityTokenServicePreferences",
	}

	for _, op := range expectedNewOps {
		assert.Contains(t, ops, op, "operation %q should be in GetSupportedOperations", op)
	}
}
