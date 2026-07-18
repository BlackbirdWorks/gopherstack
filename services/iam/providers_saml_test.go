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
		_, err := b.CreateLoginProfile("alice", "Password1!", false)
		require.NoError(t, err)
		_, err = b.CreateLoginProfile("alice", "Password2!", false)
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
		_, err := b.CreateLoginProfile("alice", "Password1!", false)
		require.NoError(t, err)

		err = b.UpdateLoginProfile("alice", "NewPassword1!", true)
		require.NoError(t, err)

		got, err := b.GetLoginProfile("alice")
		require.NoError(t, err)
		assert.True(t, got.PasswordResetRequired)
	})

	t.Run("UpdateLoginProfile_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.UpdateLoginProfile("nobody", "Password1!", false)
		require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
	})

	t.Run("DeleteLoginProfile", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		_, err := b.CreateLoginProfile("alice", "Password1!", false)
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
		check  func(*testing.T, *httptest.ResponseRecorder, *iam.InMemoryBackend)
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
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
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
			params: map[string]string{"SAMLMetadataDocument": "<updated/>"},
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateSAMLProvider("MySAML", "<old/>")

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, b *iam.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)
				var resp iam.UpdateSAMLProviderResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp.UpdateSAMLProviderResult.SAMLProviderArn, "saml-provider/MySAML")

				// Verify the metadata was actually updated in the backend.
				got, err := b.GetSAMLProvider(resp.UpdateSAMLProviderResult.SAMLProviderArn)
				require.NoError(t, err)
				assert.Equal(t, "<updated/>", got.SAMLMetadataDocument)
			},
		},
		{
			name:   "DeleteSAMLProvider",
			action: "DeleteSAMLProvider",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateSAMLProvider("MySAML", "<doc/>")

				return p.Arn
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
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
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
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
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ *iam.InMemoryBackend) {
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
			tt.check(t, rec, b)
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
			wantStatus: http.StatusConflict,
		},
		{
			name:       "GetSAMLProvider_NotFound",
			action:     "GetSAMLProvider",
			params:     map[string]string{"SAMLProviderArn": "arn:aws:iam::000000000000:saml-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateSAMLProvider_NotFound",
			action:     "UpdateSAMLProvider",
			params:     map[string]string{"SAMLProviderArn": "arn:aws:iam::000000000000:saml-provider/ghost"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteSAMLProvider_NotFound",
			action:     "DeleteSAMLProvider",
			params:     map[string]string{"SAMLProviderArn": "arn:aws:iam::000000000000:saml-provider/ghost"},
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

// ---- OIDC Provider handler tests ----

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
				_, _ = b.CreateLoginProfile("alice", "Password1", false)
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
			params: map[string]string{"UserName": "alice", "Password": "NewPassword1"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateLoginProfile("alice", "OldPassword1", false)
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
				_, _ = b.CreateLoginProfile("alice", "Password1", false)
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
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "CreateLoginProfile_AlreadyExists",
			action: "CreateLoginProfile",
			params: map[string]string{"UserName": "alice", "Password": "Pass"},
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateLoginProfile("alice", "Password1", false)
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "GetLoginProfile_NotFound",
			action:     "GetLoginProfile",
			params:     map[string]string{"UserName": "nobody"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateLoginProfile_NotFound",
			action:     "UpdateLoginProfile",
			params:     map[string]string{"UserName": "nobody", "Password": "Pass"},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteLoginProfile_NotFound",
			action:     "DeleteLoginProfile",
			params:     map[string]string{"UserName": "nobody"},
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

// ---- Miscellaneous handler tests ----

func TestGetSupportedOperations_IncludesProviderAndProfileOps(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	ops := h.GetSupportedOperations()

	expectedOps := []string{
		"CreateSAMLProvider", "UpdateSAMLProvider", "DeleteSAMLProvider",
		"GetSAMLProvider", "ListSAMLProviders",
		"CreateOpenIDConnectProvider", "UpdateOpenIDConnectProviderThumbprint",
		"DeleteOpenIDConnectProvider", "GetOpenIDConnectProvider", "ListOpenIDConnectProviders",
		"CreateLoginProfile", "UpdateLoginProfile", "DeleteLoginProfile", "GetLoginProfile",
		"GetServiceLastAccessedDetails", "SetSecurityTokenServicePreferences",
	}

	for _, op := range expectedOps {
		assert.Contains(t, ops, op, "operation %q should be in GetSupportedOperations", op)
	}
}

// TestRemoveClientIDFromOpenIDConnectProvider covers RemoveClientIDFromOpenIDConnectProvider.

func TestDeleteUser_CleansLoginProfile(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("lp-user", "/", "")
	require.NoError(t, err)

	_, err = b.CreateLoginProfile("lp-user", "S3cur3P@ss!", false)
	require.NoError(t, err)

	require.NoError(t, b.DeleteUser("lp-user"))

	_, err = b.GetLoginProfile("lp-user")
	require.ErrorIs(t, err, iam.ErrLoginProfileNotFound)
}

func TestCreateLoginProfile_RejectsEmptyPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{
			name:     "empty_password_returns_error",
			password: "",
			wantErr:  true,
		},
		{
			name:     "non_empty_password_succeeds",
			password: "S3cur3P@ss!",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateUser("pass-user-"+tt.name, "/", "")

			_, err := b.CreateLoginProfile("pass-user-"+tt.name, tt.password, false)
			if tt.wantErr {
				require.ErrorIs(t, err, iam.ErrInvalidPassword)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateLoginProfile_RejectsEmptyPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{
			name:     "empty_password_returns_error",
			password: "",
			wantErr:  true,
		},
		{
			name:     "non_empty_password_succeeds",
			password: "N3wP@ss!",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateUser("upd-user-"+tt.name, "/", "")
			_, _ = b.CreateLoginProfile("upd-user-"+tt.name, "InitialPass1!", false)

			err := b.UpdateLoginProfile("upd-user-"+tt.name, tt.password, false)
			if tt.wantErr {
				require.ErrorIs(t, err, iam.ErrInvalidPassword)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSAMLProvider_MetadataRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	metadataDoc := "<EntityDescriptor entityID=\"https://idp.example.com/\"></EntityDescriptor>"
	provider, err := b.CreateSAMLProvider("my-idp", metadataDoc)
	require.NoError(t, err)

	got, err := b.GetSAMLProvider(provider.Arn)
	require.NoError(t, err)

	assert.Equal(t, metadataDoc, got.SAMLMetadataDocument,
		"metadata document must survive round-trip")
}

func TestSAMLProvider_ARNFormat(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateSAMLProvider("test-saml-provider", "<metadata/>")
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(provider.Arn, "arn:aws:iam::"),
		"SAML provider ARN must start with arn:aws:iam::")
	assert.Contains(t, provider.Arn, ":saml-provider/",
		"SAML provider ARN must contain :saml-provider/")
	assert.Contains(t, provider.Arn, "test-saml-provider",
		"SAML provider ARN must contain the provider name")
}

func TestSAMLProvider_UpdateMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateSAMLProvider("update-saml", "<original/>")
	require.NoError(t, err)

	_, updateErr := b.UpdateSAMLProvider(provider.Arn, "<updated/>")
	require.NoError(t, updateErr)

	got, err := b.GetSAMLProvider(provider.Arn)
	require.NoError(t, err)
	assert.Equal(t, "<updated/>", got.SAMLMetadataDocument)
}

func TestSAMLProvider_DeleteRemoves(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	provider, err := b.CreateSAMLProvider("del-saml", "<metadata/>")
	require.NoError(t, err)

	require.NoError(t, b.DeleteSAMLProvider(provider.Arn))

	_, err = b.GetSAMLProvider(provider.Arn)
	require.Error(t, err)
}

func TestPasswordPolicy_CreateLoginProfile_MinLength(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 10,
	}))

	_, err := b.CreateLoginProfile("alice", "short", false)
	require.Error(t, err)
	require.ErrorIs(t, err, iam.ErrInvalidPassword)

	_, err = b.CreateLoginProfile("alice", "longenough1", false)
	require.NoError(t, err)
}

func TestPasswordPolicy_UpdateLoginProfile_Enforced(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	_, err := b.CreateLoginProfile("alice", "InitialPass1!", false)
	require.NoError(t, err)

	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength:      12,
		RequireUppercaseCharacters: true,
		RequireNumbers:             true,
	}))

	// Password below minimum.
	err = b.UpdateLoginProfile("alice", "Short1A", false)
	require.Error(t, err)
	require.ErrorIs(t, err, iam.ErrInvalidPassword)

	// Password missing uppercase.
	err = b.UpdateLoginProfile("alice", "alllower123", false)
	require.Error(t, err)
	require.ErrorIs(t, err, iam.ErrInvalidPassword)

	// Valid password.
	err = b.UpdateLoginProfile("alice", "GoodPassword123", false)
	require.NoError(t, err)
}

func TestSAMLProvider_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := "<md>test</md>"

	sp, err := b.CreateSAMLProvider("MySAML", doc)
	require.NoError(t, err)
	assert.Contains(t, sp.Arn, "saml-provider/MySAML")

	got, err := b.GetSAMLProvider(sp.Arn)
	require.NoError(t, err)
	assert.Equal(t, doc, got.SAMLMetadataDocument)

	updated, err := b.UpdateSAMLProvider(sp.Arn, "<md>updated</md>")
	require.NoError(t, err)
	assert.Equal(t, "<md>updated</md>", updated.SAMLMetadataDocument)

	all, err := b.ListSAMLProviders()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	require.NoError(t, b.DeleteSAMLProvider(sp.Arn))

	_, err = b.GetSAMLProvider(sp.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrSAMLProviderNotFound)
}
