package cognitoidp_test

// batch2_test.go covers the batch-2 AWS-accuracy improvements for Cognito IDP.
// Targets: MFA config sub-types, UICustomization timestamps, IdentityProvider
// AttributeMapping/IdpIdentifiers, UserPoolDomain CertificateArn, typed
// RiskConfiguration, stateful attribute verification, Group RoleArn + pagination,
// AdminCreateUser MessageAction, AdminSetUserPassword policy enforcement.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// ---------------------------------------------------------------------------
// MFA Configuration — full sub-type support
// ---------------------------------------------------------------------------

func TestBatch2_GetUserPoolMfaConfig_DefaultsToOFF(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-default-pool")

	rec := doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "OFF", out.MfaConfiguration)
}

func TestBatch2_SetGetMfaConfig_SmsMfaConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-sms-pool")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
		"SmsMfaConfiguration": map[string]any{
			"SmsAuthenticationMessage": "Your code is {####}",
			"SmsConfiguration": map[string]any{
				"SnsCallerArn": "arn:aws:iam::123456789:role/cognito-sms",
				"SnsRegion":    "us-east-1",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		SmsMfaConfiguration *struct {
			SmsConfiguration *struct {
				SnsCallerArn string `json:"SnsCallerArn,omitempty"`
			} `json:"SmsConfiguration"`
			SmsAuthenticationMessage string `json:"SmsAuthenticationMessage,omitempty"`
		} `json:"SmsMfaConfiguration"`
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	assert.Equal(t, "ON", setOut.MfaConfiguration)
	require.NotNil(t, setOut.SmsMfaConfiguration)
	assert.Equal(t, "Your code is {####}", setOut.SmsMfaConfiguration.SmsAuthenticationMessage)
	require.NotNil(t, setOut.SmsMfaConfiguration.SmsConfiguration)
	assert.Equal(t, "arn:aws:iam::123456789:role/cognito-sms", setOut.SmsMfaConfiguration.SmsConfiguration.SnsCallerArn)

	// Get and verify persisted.
	rec = doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		SmsMfaConfiguration *struct {
			SmsAuthenticationMessage string `json:"SmsAuthenticationMessage,omitempty"`
		} `json:"SmsMfaConfiguration"`
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, "ON", getOut.MfaConfiguration)
	require.NotNil(t, getOut.SmsMfaConfiguration)
	assert.Equal(t, "Your code is {####}", getOut.SmsMfaConfiguration.SmsAuthenticationMessage)
}

func TestBatch2_SetGetMfaConfig_SoftwareTokenMfa(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-totp-pool")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "OPTIONAL",
		"SoftwareTokenMfaConfiguration": map[string]any{
			"Enabled": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		SoftwareTokenMfaConfiguration *struct {
			Enabled bool `json:"Enabled,omitempty"`
		} `json:"SoftwareTokenMfaConfiguration"`
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	assert.Equal(t, "OPTIONAL", setOut.MfaConfiguration)
	require.NotNil(t, setOut.SoftwareTokenMfaConfiguration)
	assert.True(t, setOut.SoftwareTokenMfaConfiguration.Enabled)

	// Verify Get returns the same.
	rec = doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		SoftwareTokenMfaConfiguration *struct {
			Enabled bool `json:"Enabled,omitempty"`
		} `json:"SoftwareTokenMfaConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	require.NotNil(t, getOut.SoftwareTokenMfaConfiguration)
	assert.True(t, getOut.SoftwareTokenMfaConfiguration.Enabled)
}

func TestBatch2_SetGetMfaConfig_EmailMfa(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-email-pool")

	rec := doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "ON",
		"EmailMfaConfiguration": map[string]any{
			"Message": "Your login code is {####}",
			"Subject": "Login verification",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		EmailMfaConfiguration *struct {
			Message string `json:"Message,omitempty"`
			Subject string `json:"Subject,omitempty"`
		} `json:"EmailMfaConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	require.NotNil(t, setOut.EmailMfaConfiguration)
	assert.Equal(t, "Your login code is {####}", setOut.EmailMfaConfiguration.Message)
	assert.Equal(t, "Login verification", setOut.EmailMfaConfiguration.Subject)
}

func TestBatch2_MfaConfig_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": "invalid-pool-id",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doCognitoRequest(t, h, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":       "invalid-pool-id",
		"MfaConfiguration": "ON",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch2_MfaConfig_Backend_Full(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("mfa-full-pool")
	require.NoError(t, err)

	cfg := cognitoidp.UserPoolMfaFullConfig{
		MfaConfiguration: "ON",
		SmsMfaConfiguration: &cognitoidp.SmsMfaConfiguration{
			SmsAuthenticationMessage: "Code: {####}",
			SmsConfiguration: &cognitoidp.SmsConfiguration{
				SnsCallerArn: "arn:aws:iam::111:role/role",
				SnsRegion:    "us-west-2",
				ExternalID:   "ext-id-123",
			},
		},
		SoftwareTokenMfa: &cognitoidp.SoftwareTokenMfaConfiguration{Enabled: true},
	}

	require.NoError(t, b.SetUserPoolMfaConfigFull(pool.ID, cfg))

	got, err := b.GetUserPoolMfaConfigFull(pool.ID)
	require.NoError(t, err)
	assert.Equal(t, "ON", got.MfaConfiguration)
	require.NotNil(t, got.SmsMfaConfiguration)
	assert.Equal(t, "Code: {####}", got.SmsMfaConfiguration.SmsAuthenticationMessage)
	require.NotNil(t, got.SmsMfaConfiguration.SmsConfiguration)
	assert.Equal(t, "us-west-2", got.SmsMfaConfiguration.SmsConfiguration.SnsRegion)
	assert.Equal(t, "ext-id-123", got.SmsMfaConfiguration.SmsConfiguration.ExternalID)
	require.NotNil(t, got.SoftwareTokenMfa)
	assert.True(t, got.SoftwareTokenMfa.Enabled)
}

func TestBatch2_MfaConfig_Backend_InvalidPool(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetUserPoolMfaConfigFull("bad-pool")
	require.Error(t, err)

	err = b.SetUserPoolMfaConfigFull("bad-pool", cognitoidp.UserPoolMfaFullConfig{})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// UICustomization — extended with timestamps and ImageUrl
// ---------------------------------------------------------------------------

func TestBatch2_UICustomization_SetGet_WithImageUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "ui-custom-pool")

	rec := doCognitoRequest(t, h, "SetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"CSS":        ".banner { background: blue; }",
		"ImageData":  "https://example.com/logo.png",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		UICustomization *struct {
			UserPoolID       string  `json:"UserPoolId,omitempty"`
			ClientID         string  `json:"ClientId,omitempty"`
			CSS              string  `json:"CSS,omitempty"`
			ImageURL         string  `json:"ImageUrl,omitempty"`
			CreationDate     float64 `json:"CreationDate,omitempty"`
			LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	require.NotNil(t, setOut.UICustomization)
	assert.Equal(t, poolID, setOut.UICustomization.UserPoolID)
	assert.Equal(t, clientID, setOut.UICustomization.ClientID)
	assert.Equal(t, ".banner { background: blue; }", setOut.UICustomization.CSS)
	assert.Equal(t, "https://example.com/logo.png", setOut.UICustomization.ImageURL)
	assert.Greater(t, setOut.UICustomization.CreationDate, float64(0))
	assert.Greater(t, setOut.UICustomization.LastModifiedDate, float64(0))

	// Get and verify.
	rec = doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut struct {
		UICustomization *struct {
			CSS              string  `json:"CSS,omitempty"`
			ImageURL         string  `json:"ImageUrl,omitempty"`
			LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	require.NotNil(t, getOut.UICustomization)
	assert.Equal(t, ".banner { background: blue; }", getOut.UICustomization.CSS)
	assert.Equal(t, "https://example.com/logo.png", getOut.UICustomization.ImageURL)
	assert.Greater(t, getOut.UICustomization.LastModifiedDate, float64(0))
}

func TestBatch2_UICustomization_Get_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "ui-empty-pool")

	rec := doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UICustomization *struct {
			UserPoolID string `json:"UserPoolId,omitempty"`
			CSS        string `json:"CSS,omitempty"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.UICustomization)
	assert.Empty(t, out.UICustomization.CSS)
}

func TestBatch2_UICustomization_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": "bad-pool",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doCognitoRequest(t, h, "SetUICustomization", map[string]any{
		"UserPoolId": "bad-pool",
		"CSS":        ".foo {}",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch2_UICustomization_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("ui-backend-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "ui-client")
	require.NoError(t, err)

	ui, err := b.SetUICustomizationFull(
		pool.ID,
		client.ClientID,
		".body { color: red; }",
		"https://img.example.com/logo.png",
	)
	require.NoError(t, err)
	assert.Equal(t, ".body { color: red; }", ui.CSS)
	assert.Equal(t, "https://img.example.com/logo.png", ui.ImageURL)
	assert.False(t, ui.CreatedAt.IsZero())
	assert.False(t, ui.LastModifiedAt.IsZero())

	got, err := b.GetUICustomizationFull(pool.ID, client.ClientID)
	require.NoError(t, err)
	assert.Equal(t, ui.CSS, got.CSS)
	assert.Equal(t, ui.ImageURL, got.ImageURL)
}

// ---------------------------------------------------------------------------
// IdentityProvider — with AttributeMapping and IdpIdentifiers
// ---------------------------------------------------------------------------

func TestBatch2_IdentityProvider_SAML_WithAttributeMapping(t *testing.T) {
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

func TestBatch2_IdentityProvider_OIDC_WithScopes(t *testing.T) {
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

func TestBatch2_IdentityProvider_Social_Google(t *testing.T) {
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

func TestBatch2_IdentityProvider_Update_AttributeMapping(t *testing.T) {
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

func TestBatch2_IdentityProvider_GetByIdentifier(t *testing.T) {
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

func TestBatch2_IdentityProvider_List_WithTimestamps(t *testing.T) {
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

func TestBatch2_IdentityProvider_Backend_Direct(t *testing.T) {
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

// ---------------------------------------------------------------------------
// UserPoolDomain — with CertificateArn for custom domains
// ---------------------------------------------------------------------------

func TestBatch2_UserPoolDomain_Managed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-managed-pool")

	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp-managed",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	// Managed domains: AWS returns empty CloudFrontDomain (no CloudFront distribution).
	assert.Empty(t, out.CloudFrontDomain)
}

func TestBatch2_UserPoolDomain_Custom_WithCertArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-custom-pool")

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/abc123"

	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "auth.mycompany.com",
		"CustomDomainConfig": map[string]any{
			"CertificateArn": certArn,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.Contains(t, createOut.CloudFrontDomain, "cloudfront.net")

	// Describe should return the domain with status.
	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "auth.mycompany.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		DomainDescription *struct {
			Domain                 string `json:"Domain,omitempty"`
			UserPoolID             string `json:"UserPoolId,omitempty"`
			Status                 string `json:"Status,omitempty"`
			CloudFrontDistribution string `json:"CloudFrontDistribution,omitempty"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.DomainDescription)
	assert.Equal(t, "auth.mycompany.com", descOut.DomainDescription.Domain)
	assert.Equal(t, "ACTIVE", descOut.DomainDescription.Status)
}

func TestBatch2_UserPoolDomain_Update_WithCertArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-update-pool")

	// Create managed domain first.
	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "auth-update.mycompany.com",
		"CustomDomainConfig": map[string]any{
			"CertificateArn": "arn:aws:acm:us-east-1:123:certificate/old",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with new cert.
	rec = doCognitoRequest(t, h, "UpdateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "auth-update.mycompany.com",
		"CustomDomainConfig": map[string]any{
			"CertificateArn": "arn:aws:acm:us-east-1:123:certificate/new",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out.CloudFrontDomain, "cloudfront.net")
}

func TestBatch2_UserPoolDomain_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("domain-direct-pool")
	require.NoError(t, err)

	// Managed domain (no cert).
	d, err := b.CreateUserPoolDomainFull(pool.ID, "my-managed-domain", "")
	require.NoError(t, err)
	assert.Contains(t, d.CloudFrontDistribution, "amazoncognito.com")
	assert.Empty(t, d.CertificateArn)

	// Custom domain with cert.
	certArn := "arn:aws:acm:us-east-1:123:certificate/xyz"
	d2, err := b.CreateUserPoolDomainFull(pool.ID, "auth.example.com", certArn)
	require.NoError(t, err)
	assert.Contains(t, d2.CloudFrontDistribution, "cloudfront.net")
	assert.Equal(t, certArn, d2.CertificateArn)

	// Update cert.
	newCert := "arn:aws:acm:us-east-1:123:certificate/new"
	cfDomain, err := b.UpdateUserPoolDomainFull(pool.ID, "auth.example.com", newCert)
	require.NoError(t, err)
	assert.Contains(t, cfDomain, "cloudfront.net")

	// Delete.
	require.NoError(t, b.DeleteUserPoolDomain(pool.ID, "auth.example.com"))
	d3 := b.FindUserPoolDomain("auth.example.com")
	assert.Nil(t, d3)
}

// ---------------------------------------------------------------------------
// Typed RiskConfiguration
// ---------------------------------------------------------------------------

func TestBatch2_RiskConfiguration_CompromisedCredentials(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-cc-pool")

	rec := doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"CompromisedCredentialsRiskConfiguration": map[string]any{
			"EventFilter": []string{"SIGN_IN", "PASSWORD_CHANGE"},
			"Actions": map[string]any{
				"EventAction": "BLOCK",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct {
				Actions *struct {
					EventAction string `json:"EventAction,omitempty"`
				} `json:"Actions"`
				EventFilter []string `json:"EventFilter,omitempty"`
			} `json:"CompromisedCredentialsRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	require.NotNil(t, setOut.RiskConfiguration)
	require.NotNil(t, setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
	assert.Contains(t, setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.EventFilter, "SIGN_IN")
	require.NotNil(t, setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions)
	assert.Equal(t, "BLOCK", setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions.EventAction)

	// Describe and verify persisted.
	rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct {
				Actions *struct {
					EventAction string `json:"EventAction,omitempty"`
				} `json:"Actions"`
			} `json:"CompromisedCredentialsRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.RiskConfiguration)
	require.NotNil(t, descOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
	assert.Equal(t, "BLOCK", descOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions.EventAction)
}

func TestBatch2_RiskConfiguration_AccountTakeover(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-at-pool")

	rec := doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"AccountTakeoverRiskConfiguration": map[string]any{
			"Actions": map[string]any{
				"HighAction": map[string]any{
					"Notify":      true,
					"EventAction": "BLOCK",
				},
				"MediumAction": map[string]any{
					"Notify":      true,
					"EventAction": "MFA_IF_CONFIGURED",
				},
				"LowAction": map[string]any{
					"Notify":      false,
					"EventAction": "NO_ACTION",
				},
			},
			"NotifyConfiguration": map[string]any{
				"From":      "noreply@example.com",
				"SourceArn": "arn:aws:ses:us-east-1:123:identity/example.com",
				"BlockEmail": map[string]any{
					"Subject":  "Your account has been blocked",
					"HtmlBody": "<html>Your account was blocked.</html>",
					"TextBody": "Your account was blocked.",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		RiskConfiguration *struct {
			AccountTakeoverRiskConfiguration *struct {
				Actions *struct {
					HighAction *struct {
						EventAction string `json:"EventAction,omitempty"`
						Notify      bool   `json:"Notify,omitempty"`
					} `json:"HighAction"`
					MediumAction *struct {
						EventAction string `json:"EventAction,omitempty"`
					} `json:"MediumAction"`
					LowAction *struct {
						Notify bool `json:"Notify,omitempty"`
					} `json:"LowAction"`
				} `json:"Actions"`
				NotifyConfiguration *struct {
					BlockEmail *struct {
						Subject string `json:"Subject,omitempty"`
					} `json:"BlockEmail"`
					From string `json:"From,omitempty"`
				} `json:"NotifyConfiguration"`
			} `json:"AccountTakeoverRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.RiskConfiguration)
	require.NotNil(t, out.RiskConfiguration.AccountTakeoverRiskConfiguration)
	require.NotNil(t, out.RiskConfiguration.AccountTakeoverRiskConfiguration.Actions)

	a := out.RiskConfiguration.AccountTakeoverRiskConfiguration.Actions
	require.NotNil(t, a.HighAction)
	assert.True(t, a.HighAction.Notify)
	assert.Equal(t, "BLOCK", a.HighAction.EventAction)
	require.NotNil(t, a.MediumAction)
	assert.Equal(t, "MFA_IF_CONFIGURED", a.MediumAction.EventAction)
	require.NotNil(t, a.LowAction)
	assert.False(t, a.LowAction.Notify)

	nc := out.RiskConfiguration.AccountTakeoverRiskConfiguration.NotifyConfiguration
	require.NotNil(t, nc)
	assert.Equal(t, "noreply@example.com", nc.From)
	require.NotNil(t, nc.BlockEmail)
	assert.Equal(t, "Your account has been blocked", nc.BlockEmail.Subject)
}

func TestBatch2_RiskConfiguration_PerClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "risk-perclient-pool")

	rec := doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"CompromisedCredentialsRiskConfiguration": map[string]any{
			"EventFilter": []string{"SIGN_IN"},
			"Actions":     map[string]any{"EventAction": "NO_ACTION"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Pool-level should still be empty.
	rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var poolOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct{} `json:"CompromisedCredentialsRiskConfiguration,omitempty"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &poolOut))
	assert.Nil(t, poolOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)

	// Client-level should be set.
	rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var clientOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct {
				Actions *struct {
					EventAction string `json:"EventAction,omitempty"`
				} `json:"Actions"`
			} `json:"CompromisedCredentialsRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &clientOut))
	require.NotNil(t, clientOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
	assert.Equal(
		t,
		"NO_ACTION",
		clientOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions.EventAction,
	)
}

func TestBatch2_RiskConfiguration_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("risk-backend-pool")
	require.NoError(t, err)

	cfg := &cognitoidp.TypedRiskConfiguration{
		UserPoolID: pool.ID,
		ClientID:   "",
		CompromisedCredentialsRiskConfig: &cognitoidp.CompromisedCredentialsRiskConfig{
			EventFilter: []string{"SIGN_IN", "SIGN_UP"},
			Actions:     &cognitoidp.CompromisedCredentialsActions{EventAction: "BLOCK"},
		},
		AccountTakeoverRiskConfig: &cognitoidp.AccountTakeoverRiskConfig{
			Actions: &cognitoidp.AccountTakeoverActions{
				HighAction: &cognitoidp.AccountTakeoverActionType{Notify: true, EventAction: "BLOCK"},
			},
		},
	}

	require.NoError(t, b.SetTypedRiskConfiguration(cfg))

	got, err := b.GetTypedRiskConfiguration(pool.ID, "")
	require.NoError(t, err)
	require.NotNil(t, got.CompromisedCredentialsRiskConfig)
	assert.Equal(t, "BLOCK", got.CompromisedCredentialsRiskConfig.Actions.EventAction)
	require.NotNil(t, got.AccountTakeoverRiskConfig)
	require.NotNil(t, got.AccountTakeoverRiskConfig.Actions.HighAction)
	assert.True(t, got.AccountTakeoverRiskConfig.Actions.HighAction.Notify)

	// Invalid pool should fail.
	err = b.SetTypedRiskConfiguration(&cognitoidp.TypedRiskConfiguration{UserPoolID: "bad-pool"})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Stateful attribute verification
// ---------------------------------------------------------------------------

func TestBatch2_AttrVerification_GetAndVerify(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-verify-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-user", "Pass1234!", map[string]string{
		"email": "attr@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-user", "Pass1234!")
	require.NoError(t, err)
	accessToken := result.Tokens.AccessToken

	// Generate code.
	code, dest, medium, err := b.GetUserAttributeVerificationCode(accessToken, "email")
	require.NoError(t, err)
	assert.NotEmpty(t, code)
	assert.NotEmpty(t, dest)
	assert.Equal(t, "EMAIL", medium)
	assert.Contains(t, dest, "***")

	// Verify with code.
	require.NoError(t, b.VerifyUserAttributeWithCode(accessToken, "email", code))

	// User attribute should be verified.
	u, err := b.GetUser(accessToken)
	require.NoError(t, err)
	assert.Equal(t, "true", u.Attributes["email_verified"])
}

func TestBatch2_AttrVerification_WrongCode(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-verify-wrong-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-client2")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-user2", "Pass1234!", map[string]string{
		"email": "wrong@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-user2", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-user2", "Pass1234!")
	require.NoError(t, err)

	_, _, _, err = b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "email")
	require.NoError(t, err)

	// Wrong code.
	err = b.VerifyUserAttributeWithCode(result.Tokens.AccessToken, "email", "WRONG!")
	require.Error(t, err)
}

func TestBatch2_AttrVerification_NoCodeGenerated(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-nocode-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-nocode-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-nocode-user", "Pass1234!", map[string]string{
		"email": "nocode@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-nocode-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-nocode-user", "Pass1234!")
	require.NoError(t, err)

	// No code generated — verify should fail.
	err = b.VerifyUserAttributeWithCode(result.Tokens.AccessToken, "email", "123456")
	require.Error(t, err)
}

func TestBatch2_AttrVerification_PhoneNumber(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-phone-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "phone-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "phone-user", "Pass1234!", map[string]string{
		"phone_number": "+14155551234",
		"email":        "phone@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "phone-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "phone-user", "Pass1234!")
	require.NoError(t, err)

	code, dest, medium, err := b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "phone_number")
	require.NoError(t, err)
	assert.NotEmpty(t, code)
	assert.Equal(t, "SMS", medium)
	assert.Contains(t, dest, "+*******")
	assert.Contains(t, dest, "1234")
}

func TestBatch2_AttrVerification_InvalidAttribute(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("attr-invalid-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "attr-inv-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "attr-inv-user", "Pass1234!", map[string]string{
		"email": "inv@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "attr-inv-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "attr-inv-user", "Pass1234!")
	require.NoError(t, err)

	// "name" is not verifiable.
	_, _, _, err = b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "name")
	require.Error(t, err)
}

func TestBatch2_AttrVerification_Handler_GetAndVerify(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "attr-handler-pool")

	rec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "attr-handler-user",
		"Password": "Pass1234!",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "handler@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var signUpResp struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
		UserConfirmed       bool              `json:"UserConfirmed,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &signUpResp))

	if !signUpResp.UserConfirmed {
		confirmRec := doCognitoRequest(t, h, "ConfirmSignUp", map[string]any{
			"ClientId":         clientID,
			"Username":         "attr-handler-user",
			"ConfirmationCode": signUpResp.CodeDeliveryDetails["ConfirmationCode"],
		})
		require.Equal(t, http.StatusOK, confirmRec.Code)
	}

	accessToken := loginViaHandler(t, h, clientID, "attr-handler-user")

	// GetUserAttributeVerificationCode.
	rec = doCognitoRequest(t, h, "GetUserAttributeVerificationCode", map[string]any{
		"AccessToken":   accessToken,
		"AttributeName": "email",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var codeOut struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &codeOut))
	assert.Equal(t, "EMAIL", codeOut.CodeDeliveryDetails["DeliveryMedium"])
	assert.NotEmpty(t, codeOut.CodeDeliveryDetails["Destination"])

	// Get code from backend for test.
	code := h.Backend.GetAttrVerificationCodeForTest(poolID, "attr-handler-user", "email")
	require.NotEmpty(t, code)

	// VerifyUserAttribute with real code.
	rec = doCognitoRequest(t, h, "VerifyUserAttribute", map[string]any{
		"AccessToken":   accessToken,
		"AttributeName": "email",
		"Code":          code,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Group — RoleArn and pagination
// ---------------------------------------------------------------------------

func TestBatch2_Group_Create_WithRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-role-pool")

	roleArn := "arn:aws:iam::123456789:role/CognitoAdminRole"

	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "admins",
		"Description": "Admin users",
		"RoleArn":     roleArn,
		"Precedence":  int32(1),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group *struct {
			GroupName    string  `json:"GroupName,omitempty"`
			RoleArn      string  `json:"RoleArn,omitempty"`
			Precedence   int32   `json:"Precedence,omitempty"`
			CreationDate float64 `json:"CreationDate,omitempty"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Group)
	assert.Equal(t, "admins", out.Group.GroupName)
	assert.Equal(t, roleArn, out.Group.RoleArn)
	assert.Equal(t, int32(1), out.Group.Precedence)
	assert.Greater(t, out.Group.CreationDate, float64(0))
}

func TestBatch2_Group_Update_WithRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-upd-role-pool")

	// Create without role.
	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "editors",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with role.
	roleArn := "arn:aws:iam::123:role/EditorRole"
	rec = doCognitoRequest(t, h, "UpdateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "editors",
		"RoleArn":    roleArn,
		"Precedence": int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group *struct {
			RoleArn    string `json:"RoleArn,omitempty"`
			Precedence int32  `json:"Precedence,omitempty"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Group)
	assert.Equal(t, roleArn, out.Group.RoleArn)
	assert.Equal(t, int32(10), out.Group.Precedence)
}

func TestBatch2_Group_GetGroup_WithRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-get-role-pool")

	roleArn := "arn:aws:iam::123:role/MyRole"
	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "viewers",
		"RoleArn":    roleArn,
		"Precedence": int32(5),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "GetGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "viewers",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group *struct {
			GroupName  string `json:"GroupName,omitempty"`
			RoleArn    string `json:"RoleArn,omitempty"`
			Precedence int32  `json:"Precedence,omitempty"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Group)
	assert.Equal(t, "viewers", out.Group.GroupName)
	assert.Equal(t, roleArn, out.Group.RoleArn)
}

func TestBatch2_Group_ListGroups_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-page-pool")

	// Create 5 groups.
	for i := range 5 {
		rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
			"UserPoolId": poolID,
			"GroupName":  fmt.Sprintf("group-%02d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1 — 3 items.
	rec := doCognitoRequest(t, h, "ListGroups", map[string]any{
		"UserPoolId": poolID,
		"Limit":      3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken,omitempty"`
		Groups    []struct {
			GroupName string `json:"GroupName,omitempty"`
		} `json:"Groups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Groups, 3)
	assert.NotEmpty(t, page1.NextToken)

	// Page 2 — remaining items.
	rec = doCognitoRequest(t, h, "ListGroups", map[string]any{
		"UserPoolId": poolID,
		"Limit":      3,
		"NextToken":  page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		NextToken string `json:"NextToken,omitempty"`
		Groups    []struct {
			GroupName string `json:"GroupName,omitempty"`
		} `json:"Groups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Groups, 2)
	assert.Empty(t, page2.NextToken)
}

func TestBatch2_Group_ListUsersInGroup_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "group-users-page-pool")

	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "members",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create and add 4 users to group.
	for i := range 4 {
		username := fmt.Sprintf("member-%02d", i)
		signUpAndConfirmViaHandler(t, h, clientID, username)
		addRec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
			"UserPoolId": poolID,
			"Username":   username,
			"GroupName":  "members",
		})
		require.Equal(t, http.StatusOK, addRec.Code)
	}

	// Page 1 — 2 users.
	rec = doCognitoRequest(t, h, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "members",
		"Limit":      2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken,omitempty"`
		Users     []struct {
			Username string `json:"Username,omitempty"`
		} `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Users, 2)
	assert.NotEmpty(t, page1.NextToken)

	// Page 2.
	rec = doCognitoRequest(t, h, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "members",
		"Limit":      2,
		"NextToken":  page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		NextToken string `json:"NextToken,omitempty"`
		Users     []struct {
			Username string `json:"Username,omitempty"`
		} `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Users, 2)
	assert.Empty(t, page2.NextToken)
}

func TestBatch2_Group_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("group-backend-pool")
	require.NoError(t, err)

	roleArn := "arn:aws:iam::123:role/TestRole"
	g, err := b.CreateGroupFull(pool.ID, "testers", "Test users", roleArn, 5)
	require.NoError(t, err)
	assert.Equal(t, roleArn, g.RoleArn)
	assert.Equal(t, int32(5), g.Precedence)

	// Duplicate should fail.
	_, err = b.CreateGroupFull(pool.ID, "testers", "", "", 0)
	require.Error(t, err)

	// Update.
	newRole := "arn:aws:iam::123:role/UpdatedRole"
	updated, err := b.UpdateGroupFull(pool.ID, "testers", "Updated desc", newRole, 10)
	require.NoError(t, err)
	assert.Equal(t, newRole, updated.RoleArn)
	assert.Equal(t, "Updated desc", updated.Description)
}

func TestBatch2_Group_ListGroupsPage_Backend(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("group-page-backend-pool")
	require.NoError(t, err)

	for i := range 6 {
		_, err = b.CreateGroupFull(pool.ID, fmt.Sprintf("grp-%02d", i), "", "", int32(i))
		require.NoError(t, err)
	}

	// First page.
	page1, tok1, err := b.ListGroupsPage(pool.ID, 3, "")
	require.NoError(t, err)
	assert.Len(t, page1, 3)
	assert.NotEmpty(t, tok1)

	// Second page.
	page2, tok2, err := b.ListGroupsPage(pool.ID, 3, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 3)
	assert.Empty(t, tok2)

	// All groups — no limit.
	all, tok3, err := b.ListGroupsPage(pool.ID, 0, "")
	require.NoError(t, err)
	assert.Len(t, all, 6)
	assert.Empty(t, tok3)
}

// ---------------------------------------------------------------------------
// AdminCreateUser — MessageAction
// ---------------------------------------------------------------------------

func TestBatch2_AdminCreateUser_Suppress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-suppress-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "suppressed-user",
		"TemporaryPassword": "Temp1234!",
		"MessageAction":     "SUPPRESS",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "suppress@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		User *struct {
			Username   string `json:"Username,omitempty"`
			UserStatus string `json:"UserStatus,omitempty"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.User)
	assert.Equal(t, "suppressed-user", out.User.Username)
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", out.User.UserStatus)
}

func TestBatch2_AdminCreateUser_Resend(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-resend-pool")

	// Create user.
	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "resend-user",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Resend — should succeed and return same user.
	rec = doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":    poolID,
		"Username":      "resend-user",
		"MessageAction": "RESEND",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		User *struct {
			Username   string `json:"Username,omitempty"`
			UserStatus string `json:"UserStatus,omitempty"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.User)
	assert.Equal(t, "resend-user", out.User.Username)
}

func TestBatch2_AdminCreateUser_DeliveryMediums(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-create-delivery-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":             poolID,
		"Username":               "delivery-user",
		"TemporaryPassword":      "Temp1234!",
		"DesiredDeliveryMediums": []string{"EMAIL"},
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "delivery@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		User *struct {
			Username string `json:"Username,omitempty"`
		} `json:"User"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.User)
	assert.Equal(t, "delivery-user", out.User.Username)
}

func TestBatch2_AdminCreateUser_Backend_Full(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("admin-create-backend-pool")
	require.NoError(t, err)

	// Create with SUPPRESS.
	user, err := b.AdminCreateUserFull(
		pool.ID, "backend-user", "Temp1234!",
		map[string]string{"email": "backend@example.com"},
		"SUPPRESS", nil, false,
	)
	require.NoError(t, err)
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", user.Status)
	// SUPPRESS: custom:temporaryPassword should NOT be set.
	assert.Empty(t, user.Attributes["custom:temporaryPassword"])

	// Duplicate should fail (not RESEND).
	_, err = b.AdminCreateUserFull(pool.ID, "backend-user", "New1234!", nil, "", nil, false)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// AdminSetUserPassword — policy enforcement
// ---------------------------------------------------------------------------

func TestBatch2_AdminSetUserPassword_Permanent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-pwd-perm-pool")

	// Create user in FORCE_CHANGE_PASSWORD.
	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "pwd-user",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set permanent password.
	rec = doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "pwd-user",
		"Password":   "Perm5678!",
		"Permanent":  true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// User should now be CONFIRMED.
	rec = doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "pwd-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UserStatus string `json:"UserStatus,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "CONFIRMED", out.UserStatus)
}

func TestBatch2_AdminSetUserPassword_Temporary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "admin-pwd-temp-pool")

	rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "temp-pwd-user",
		"TemporaryPassword": "Temp1234!",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set another temporary password.
	rec = doCognitoRequest(t, h, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "temp-pwd-user",
		"Password":   "NewTemp9999!",
		"Permanent":  false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// User should remain FORCE_CHANGE_PASSWORD.
	rec = doCognitoRequest(t, h, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "temp-pwd-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UserStatus string `json:"UserStatus,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", out.UserStatus)
}

func TestBatch2_AdminSetUserPassword_PolicyEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("admin-pwd-policy-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    10,
			RequireUppercase: true,
			RequireNumbers:   true,
			RequireSymbols:   true,
		},
	})
	require.NoError(t, err)

	_, err = b.AdminCreateUser(pool.ID, "policy-user", "Temp1234!@#", nil)
	require.NoError(t, err)

	// Short password — policy violation.
	err = b.AdminSetUserPasswordFull(pool.ID, "policy-user", "short", true)
	require.Error(t, err)

	// Valid password.
	err = b.AdminSetUserPasswordFull(pool.ID, "policy-user", "LongPass1234!", true)
	require.NoError(t, err)
}

func TestBatch2_AdminSetUserPassword_Backend_UserNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("admin-pwd-notfound-pool")
	require.NoError(t, err)

	err = b.AdminSetUserPasswordFull(pool.ID, "nonexistent", "Pass1234!", true)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// EvictExpiredAttrVerificationCodes
// ---------------------------------------------------------------------------

func TestBatch2_EvictExpiredAttrVerificationCodes(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("evict-attr-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "evict-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "evict-user", "Pass1234!", map[string]string{
		"email": "evict@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "evict-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "evict-user", "Pass1234!")
	require.NoError(t, err)

	code, _, _, err := b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "email")
	require.NoError(t, err)
	assert.NotEmpty(t, code)

	// Evict — code should still be there (not expired).
	b.EvictExpiredAttrVerificationCodes()

	// Code should still work since not expired.
	storedCode := b.GetAttrVerificationCodeForTest(pool.ID, "evict-user", "email")
	assert.NotEmpty(t, storedCode)
}

// ---------------------------------------------------------------------------
// maskEmail and maskPhone helpers (via handler output)
// ---------------------------------------------------------------------------

func TestBatch2_GetUserAttributeVerifCode_MasksEmail(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("mask-email-pool")
	require.NoError(t, err)
	client, err := b.CreateUserPoolClient(pool.ID, "mask-client")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "mask-user", "Pass1234!", map[string]string{
		"email": "johndoe@example.com",
	})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "mask-user", user.ConfirmCode))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "mask-user", "Pass1234!")
	require.NoError(t, err)

	_, dest, medium, err := b.GetUserAttributeVerificationCode(result.Tokens.AccessToken, "email")
	require.NoError(t, err)
	assert.Equal(t, "EMAIL", medium)
	// Should start with "jo***" and contain the domain.
	assert.Contains(t, dest, "jo***")
	assert.Contains(t, dest, "@example.com")
}
