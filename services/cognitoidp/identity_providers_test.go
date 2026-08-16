package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestCreateIdentityProvider_Duplicate_ReturnsDuplicateProviderException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_level"},
		{name: "backend_level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "handler_level" {
				h := newTestHandler(t)
				poolID, _ := setupHandlerPoolAndClient(t, h, "dup-idp-handler-pool")

				body := map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "Google",
					"ProviderType": "Google",
					"ProviderDetails": map[string]string{
						"client_id":     "g-id",
						"client_secret": "g-sec",
					},
				}
				rec := doCognitoRequest(t, h, "CreateIdentityProvider", body)
				require.Equal(t, http.StatusOK, rec.Code)

				// Second create with same name — must be DuplicateProviderException.
				rec = doCognitoRequest(t, h, "CreateIdentityProvider", body)
				require.Equal(t, http.StatusBadRequest, rec.Code)

				var errResp struct {
					Code string `json:"__type,omitempty"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "DuplicateProviderException", errResp.Code)
			} else {
				b := newTestBackend()
				pool, err := b.CreateUserPool("dup-idp-backend-pool")
				require.NoError(t, err)

				_, err = b.CreateIdentityProviderFull(
					pool.ID, "Facebook", "Facebook",
					map[string]string{"client_id": "fb-id"}, nil, nil,
				)
				require.NoError(t, err)

				_, err = b.CreateIdentityProviderFull(
					pool.ID, "Facebook", "Facebook",
					map[string]string{"client_id": "fb-id2"}, nil, nil,
				)
				require.Error(t, err)
				assert.ErrorIs(t, err, cognitoidp.ErrDuplicateProvider)
			}
		})
	}
}

func TestUpdateIdentityProvider_ReplacesProviderDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_level"},
		{name: "backend_level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "handler_level" {
				h := newTestHandler(t)
				poolID, _ := setupHandlerPoolAndClient(t, h, "update-idp-handler-pool")

				rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "SAML",
					"ProviderType": "SAML",
					"ProviderDetails": map[string]string{
						"MetadataURL":           "https://old.example.com/metadata",
						"IDPSignout":            "true",
						"SLORedirectBindingURI": "https://old.example.com/slo",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				// Update with only MetadataURL — old keys must be gone.
				rec = doCognitoRequest(t, h, "UpdateIdentityProvider", map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "SAML",
					"ProviderDetails": map[string]string{
						"MetadataURL": "https://new.example.com/metadata",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doCognitoRequest(t, h, "DescribeIdentityProvider", map[string]any{
					"UserPoolId":   poolID,
					"ProviderName": "SAML",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					IdentityProvider *struct {
						ProviderDetails map[string]string `json:"ProviderDetails,omitempty"`
					} `json:"IdentityProvider"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.IdentityProvider)

				assert.Equal(t, "https://new.example.com/metadata", out.IdentityProvider.ProviderDetails["MetadataURL"])
				// Old keys must be gone after replace (not still present from merge).
				assert.Empty(
					t,
					out.IdentityProvider.ProviderDetails["IDPSignout"],
					"old key IDPSignout must be absent after replace",
				)
				assert.Empty(
					t,
					out.IdentityProvider.ProviderDetails["SLORedirectBindingURI"],
					"old key must be absent after replace",
				)
			} else {
				b := newTestBackend()
				pool, err := b.CreateUserPool("update-idp-backend-pool")
				require.NoError(t, err)

				_, err = b.CreateIdentityProviderFull(
					pool.ID, "SAML", "SAML",
					map[string]string{"MetadataURL": "https://old.test/metadata", "IDPSignout": "true"},
					nil, nil,
				)
				require.NoError(t, err)

				updated, err := b.UpdateIdentityProviderFull(
					pool.ID, "SAML",
					map[string]string{"MetadataURL": "https://new.test/metadata"},
					nil, nil,
				)
				require.NoError(t, err)

				assert.Equal(t, "https://new.test/metadata", updated.ProviderDetails["MetadataURL"])
				_, hasOldKey := updated.ProviderDetails["IDPSignout"]
				assert.False(t, hasOldKey, "IDPSignout must be absent after replace")
			}
		})
	}
}

func TestIdentityProvider_SAML_WithAttributeMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-saml-pool")

	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MySAML",
		"ProviderType": "SAML",
		"ProviderDetails": map[string]string{
			"MetadataURL": "https://idp.example.com/metadata",
		},
		"AttributeMapping": map[string]string{
			"email":      "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
			"given_name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
		},
		"IdpIdentifiers": []string{"idp.example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut struct {
		IdentityProvider *struct {
			ProviderName     string            `json:"ProviderName,omitempty"`
			ProviderType     string            `json:"ProviderType,omitempty"`
			AttributeMapping map[string]string `json:"AttributeMapping,omitempty"`
			IdpIdentifiers   []string          `json:"IdpIdentifiers,omitempty"`
			CreationDate     float64           `json:"CreationDate,omitempty"`
		} `json:"IdentityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	require.NotNil(t, createOut.IdentityProvider)
	assert.Equal(t, "MySAML", createOut.IdentityProvider.ProviderName)
	assert.Equal(t, "SAML", createOut.IdentityProvider.ProviderType)
	assert.Equal(t, "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
		createOut.IdentityProvider.AttributeMapping["email"])
	assert.Contains(t, createOut.IdentityProvider.IdpIdentifiers, "idp.example.com")
	assert.Greater(t, createOut.IdentityProvider.CreationDate, float64(0))
}

func TestIdentityProvider_OIDC_WithScopes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-oidc-pool")

	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "MyOIDC",
		"ProviderType": "OIDC",
		"ProviderDetails": map[string]string{
			"client_id":        "oidc-client-id",
			"client_secret":    "oidc-secret",
			"authorize_scopes": "openid email profile",
			"oidc_issuer":      "https://oidc.example.com",
		},
		"AttributeMapping": map[string]string{
			"email":    "email",
			"username": "sub",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		IdentityProvider *struct {
			AttributeMapping map[string]string `json:"AttributeMapping,omitempty"`
			ProviderName     string            `json:"ProviderName,omitempty"`
			ProviderType     string            `json:"ProviderType,omitempty"`
		} `json:"IdentityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.IdentityProvider)
	assert.Equal(t, "MyOIDC", out.IdentityProvider.ProviderName)
	assert.Equal(t, "email", out.IdentityProvider.AttributeMapping["email"])
	assert.Equal(t, "sub", out.IdentityProvider.AttributeMapping["username"])
}

func TestIdentityProvider_Social_Google(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-google-pool")

	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "Google",
		"ProviderType": "Google",
		"ProviderDetails": map[string]string{
			"client_id":        "google-app-id",
			"client_secret":    "google-secret",
			"authorize_scopes": "profile email openid",
		},
		"AttributeMapping": map[string]string{
			"email":       "email",
			"name":        "name",
			"given_name":  "given_name",
			"family_name": "family_name",
			"picture":     "picture",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe should return AttributeMapping.
	rec = doCognitoRequest(t, h, "DescribeIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "Google",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		IdentityProvider *struct {
			AttributeMapping map[string]string `json:"AttributeMapping,omitempty"`
			ProviderDetails  map[string]string `json:"ProviderDetails,omitempty"`
		} `json:"IdentityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.IdentityProvider)
	assert.Equal(t, "email", descOut.IdentityProvider.AttributeMapping["email"])
	assert.Equal(t, "google-app-id", descOut.IdentityProvider.ProviderDetails["client_id"])
}

func TestIdentityProvider_Update_AttributeMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-update-pool")

	// Create.
	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "Facebook",
		"ProviderType": "Facebook",
		"ProviderDetails": map[string]string{
			"client_id":        "fb-app-id",
			"client_secret":    "fb-secret",
			"authorize_scopes": "public_profile,email",
		},
		"AttributeMapping": map[string]string{"email": "email"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with new AttributeMapping.
	rec = doCognitoRequest(t, h, "UpdateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "Facebook",
		"AttributeMapping": map[string]string{
			"email":    "email",
			"username": "id",
		},
		"IdpIdentifiers": []string{"facebook.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updOut struct {
		IdentityProvider *struct {
			AttributeMapping map[string]string `json:"AttributeMapping,omitempty"`
			IdpIdentifiers   []string          `json:"IdpIdentifiers,omitempty"`
		} `json:"IdentityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updOut))
	require.NotNil(t, updOut.IdentityProvider)
	assert.Equal(t, "id", updOut.IdentityProvider.AttributeMapping["username"])
	assert.Contains(t, updOut.IdentityProvider.IdpIdentifiers, "facebook.com")
}

func TestIdentityProvider_GetByIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-byident-pool")

	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "LoginWithAmazon",
		"ProviderType": "LoginWithAmazon",
		"ProviderDetails": map[string]string{
			"client_id":        "amzn-id",
			"client_secret":    "amzn-sec",
			"authorize_scopes": "profile",
		},
		"IdpIdentifiers": []string{"amazon.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "GetIdentityProviderByIdentifier", map[string]any{
		"UserPoolId":    poolID,
		"IdpIdentifier": "amazon.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		IdentityProvider *struct {
			ProviderName string `json:"ProviderName,omitempty"`
		} `json:"IdentityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.IdentityProvider)
	assert.Equal(t, "LoginWithAmazon", out.IdentityProvider.ProviderName)
}

func TestIdentityProvider_List_WithTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-list-ts-pool")

	for _, name := range []string{"Google", "Facebook", "SignInWithApple"} {
		provType := name
		if name == "SignInWithApple" {
			provType = "SignInWithApple"
		}

		rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
			"UserPoolId":   poolID,
			"ProviderName": name,
			"ProviderType": provType,
			"ProviderDetails": map[string]string{
				"client_id":        name + "-id",
				"client_secret":    name + "-secret",
				"authorize_scopes": "email",
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCognitoRequest(t, h, "ListIdentityProviders", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Providers []struct {
			ProviderName     string  `json:"ProviderName,omitempty"`
			ProviderType     string  `json:"ProviderType,omitempty"`
			CreationDate     float64 `json:"CreationDate,omitempty"`
			LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
		} `json:"Providers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Providers, 3)

	for _, p := range out.Providers {
		assert.Greater(t, p.CreationDate, float64(0), "CreationDate should be set for %s", p.ProviderName)
		assert.Greater(t, p.LastModifiedDate, float64(0), "LastModifiedDate should be set for %s", p.ProviderName)
	}
}

func TestIdentityProvider_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("idp-backend-pool")
	require.NoError(t, err)

	idp, err := b.CreateIdentityProviderFull(
		pool.ID, "MySAML", "SAML",
		map[string]string{"MetadataURL": "https://idp.test/metadata"},
		map[string]string{"email": "urn:email"},
		[]string{"saml.example.com"},
	)
	require.NoError(t, err)
	assert.Equal(t, "urn:email", idp.AttributeMapping["email"])
	assert.Contains(t, idp.IdpIdentifiers, "saml.example.com")

	// Duplicate should fail.
	_, err = b.CreateIdentityProviderFull(pool.ID, "MySAML", "SAML", nil, nil, nil)
	require.Error(t, err)

	// Update AttributeMapping.
	updated, err := b.UpdateIdentityProviderFull(
		pool.ID, "MySAML",
		map[string]string{"MetadataURL": "https://new-idp.test/metadata"},
		map[string]string{"email": "urn:new-email", "username": "urn:username"},
		[]string{"new-saml.example.com"},
	)
	require.NoError(t, err)
	assert.Equal(t, "urn:new-email", updated.AttributeMapping["email"])
	assert.Equal(t, "urn:username", updated.AttributeMapping["username"])
}

func TestAdminLinkProviderForUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "link-provider-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "link-user")

	rec := doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
		"DestinationUser": map[string]any{
			"ProviderName":           "Cognito",
			"ProviderAttributeValue": "link-user",
		},
		"SourceUser": map[string]any{
			"ProviderName":           "Facebook",
			"ProviderAttributeName":  "Cognito_Subject",
			"ProviderAttributeValue": "fb-12345",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Unknown destination user.
	rec = doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
		"DestinationUser": map[string]any{
			"ProviderName":           "Cognito",
			"ProviderAttributeValue": "ghost",
		},
		"SourceUser": map[string]any{
			"ProviderName":           "Facebook",
			"ProviderAttributeName":  "Cognito_Subject",
			"ProviderAttributeValue": "fb-999",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing SourceUser fields.
	rec = doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
		"DestinationUser": map[string]any{
			"ProviderName":           "Cognito",
			"ProviderAttributeValue": "link-user",
		},
		"SourceUser": map[string]any{
			"ProviderName": "Facebook",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing DestinationUser/SourceUser entirely.
	rec = doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
		"UserPoolId": poolID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestIdentityProvider_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "idp-crud-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":      poolID,
		"ProviderName":    "Google",
		"ProviderType":    "Google",
		"ProviderDetails": map[string]string{"authorize_scopes": "email"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		IdentityProvider struct {
			ProviderName string `json:"ProviderName,omitempty"`
			ProviderType string `json:"ProviderType,omitempty"`
		} `json:"IdentityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Equal(t, "Google", createResp.IdentityProvider.ProviderName)
	assert.Equal(t, "Google", createResp.IdentityProvider.ProviderType)

	// Describe
	rec = doCognitoRequest(t, h, "DescribeIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "Google",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetByIdentifier
	rec = doCognitoRequest(t, h, "GetIdentityProviderByIdentifier", map[string]any{
		"UserPoolId":    poolID,
		"IdpIdentifier": "Google",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doCognitoRequest(t, h, "ListIdentityProviders", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Providers []struct {
			ProviderName string `json:"ProviderName,omitempty"`
		} `json:"Providers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Providers, 1)
	assert.Equal(t, "Google", listResp.Providers[0].ProviderName)

	// Update
	rec = doCognitoRequest(t, h, "UpdateIdentityProvider", map[string]any{
		"UserPoolId":      poolID,
		"ProviderName":    "Google",
		"ProviderDetails": map[string]string{"authorize_scopes": "email profile"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteIdentityProvider", map[string]any{
		"UserPoolId":   poolID,
		"ProviderName": "Google",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — should be empty
	rec = doCognitoRequest(t, h, "ListIdentityProviders", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Providers)
}

func TestIdentityProvider_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   "nonexistent-pool",
		"ProviderName": "Google",
		"ProviderType": "Google",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_AdminLinkProviderForUser_Links covers the HTTP handler for
// AdminLinkProviderForUser, verifying the external identity is recorded on the
// destination user.
func TestHandler_AdminLinkProviderForUser_Links(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "link-pool")
	signUpAndConfirmViaHandler(t, h, clientID, "native-user")

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
			"UserPoolId": poolID,
			"DestinationUser": map[string]any{
				"ProviderName":           "Cognito",
				"ProviderAttributeValue": "native-user",
			},
			"SourceUser": map[string]any{
				"ProviderName":           "Google",
				"ProviderAttributeName":  "Cognito_Subject",
				"ProviderAttributeValue": "google-12345",
			},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("destination_user_not_found", func(t *testing.T) {
		t.Parallel()

		rec := doCognitoRequest(t, h, "AdminLinkProviderForUser", map[string]any{
			"UserPoolId": poolID,
			"DestinationUser": map[string]any{
				"ProviderName":           "Cognito",
				"ProviderAttributeValue": "ghost",
			},
			"SourceUser": map[string]any{
				"ProviderName":           "Google",
				"ProviderAttributeValue": "google-99999",
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_AdminDisableProviderForUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolID   string
		wantCode int
	}{
		{
			name:     "success_existing_pool",
			wantCode: http.StatusOK,
		},
		{
			name:     "pool_not_found",
			poolID:   "us-east-1_NOTEXIST",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.poolID
			if poolID == "" {
				poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "provider-pool"})
				var poolResp map[string]any
				require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
				poolID = poolResp["UserPool"].(map[string]any)["Id"].(string)
			}

			rec := doCognitoRequest(t, h, "AdminDisableProviderForUser", map[string]any{
				"UserPoolId": poolID,
				"User": map[string]any{
					"ProviderName":           "Google",
					"ProviderAttributeName":  "Cognito_Subject",
					"ProviderAttributeValue": "google-123",
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
