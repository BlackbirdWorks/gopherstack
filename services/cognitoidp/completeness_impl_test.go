package cognitoidp_test

// completeness_impl_test.go tests real stateful implementations that replaced
// the previous stubs in handler_completeness.go.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Identity Provider
// ---------------------------------------------------------------------------

func TestCompleteness_IdentityProvider_CRUD(t *testing.T) {
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
			ProviderName string `json:"ProviderName"`
			ProviderType string `json:"ProviderType"`
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
			ProviderName string `json:"ProviderName"`
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

func TestCompleteness_IdentityProvider_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateIdentityProvider", map[string]any{
		"UserPoolId":   "nonexistent-pool",
		"ProviderName": "Google",
		"ProviderType": "Google",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// User Pool Domain
// ---------------------------------------------------------------------------

func TestCompleteness_Domain_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-crud-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CloudFrontDomain string `json:"CloudFrontDomain"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	// Managed domains: AWS returns empty CloudFrontDomain in the create response.
	assert.Empty(t, createResp.CloudFrontDomain)

	// Describe
	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		DomainDescription struct {
			Domain string `json:"Domain"`
			Status string `json:"Status"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "myapp", descResp.DomainDescription.Domain)
	assert.Equal(t, "ACTIVE", descResp.DomainDescription.Status)

	// Update
	rec = doCognitoRequest(t, h, "UpdateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — returns empty description (not an error per AWS behaviour)
	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var afterDeleteResp struct {
		DomainDescription struct {
			Domain string `json:"Domain"`
			Status string `json:"Status"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterDeleteResp))
	assert.Empty(t, afterDeleteResp.DomainDescription.Domain)
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

func TestCompleteness_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc"

	// TagResource
	rec := doCognitoRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doCognitoRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, "prod", listResp.Tags["env"])
	assert.Equal(t, "platform", listResp.Tags["team"])

	// UntagResource
	rec = doCognitoRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after untag — fresh variable to avoid JSON merge artefact
	rec = doCognitoRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	var afterUntagResp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterUntagResp))
	assert.NotContains(t, afterUntagResp.Tags, "env")
	assert.Equal(t, "platform", afterUntagResp.Tags["team"])
}

// ---------------------------------------------------------------------------
// Risk Configuration
// ---------------------------------------------------------------------------

func TestCompleteness_RiskConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-cfg-pool")

	// Describe before set — pool exists, returns empty
	rec := doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set
	rec = doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set with invalid pool
	rec = doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": "bad-pool",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Log Delivery
// ---------------------------------------------------------------------------

func TestCompleteness_LogDelivery(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "log-delivery-pool")

	// Get before set — empty config
	rec := doCognitoRequest(t, h, "GetLogDeliveryConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set
	rec = doCognitoRequest(t, h, "SetLogDeliveryConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get invalid pool
	rec = doCognitoRequest(t, h, "GetLogDeliveryConfiguration", map[string]any{
		"UserPoolId": "bad-pool",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UI Customization
// ---------------------------------------------------------------------------

func TestCompleteness_UICustomization(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "ui-custom-pool")

	// Set
	rec := doCognitoRequest(t, h, "SetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"CSS":        ".logo { color: red; }",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get
	rec = doCognitoRequest(t, h, "GetUICustomization", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UICustomization struct {
			CSS string `json:"CSS"`
		} `json:"UICustomization"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, ".logo { color: red; }", resp.UICustomization.CSS)
}

// ---------------------------------------------------------------------------
// Managed Login Branding
// ---------------------------------------------------------------------------

func TestCompleteness_ManagedLoginBranding_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "mlb-crud-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateManagedLoginBranding", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		ManagedLoginBranding struct {
			ManagedLoginBrandingID string `json:"ManagedLoginBrandingId"`
		} `json:"ManagedLoginBranding"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	brandingID := createResp.ManagedLoginBranding.ManagedLoginBrandingID
	require.NotEmpty(t, brandingID)

	// Describe by ID
	rec = doCognitoRequest(t, h, "DescribeManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe by client
	rec = doCognitoRequest(t, h, "DescribeManagedLoginBrandingByClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update
	rec = doCognitoRequest(t, h, "UpdateManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — should error
	rec = doCognitoRequest(t, h, "DescribeManagedLoginBranding", map[string]any{
		"UserPoolId":             poolID,
		"ManagedLoginBrandingId": brandingID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Terms
// ---------------------------------------------------------------------------

func TestCompleteness_Terms_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "terms-crud-pool")

	// Describe before create — returns empty terms
	rec := doCognitoRequest(t, h, "DescribeTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List before create — empty
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp struct {
		Terms []any `json:"Terms"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Terms)

	// Create
	rec = doCognitoRequest(t, h, "CreateTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after create — one entry
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Terms, 1)

	// Update
	rec = doCognitoRequest(t, h, "UpdateTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — empty
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Terms)
}

func TestCompleteness_Terms_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateTerms", map[string]any{"UserPoolId": "bad-pool"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// User Import Job
// ---------------------------------------------------------------------------

func TestCompleteness_UserImportJob_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "import-job-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobName":    "my-import",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		UserImportJob struct {
			JobID   string `json:"JobId"`
			JobName string `json:"JobName"`
			Status  string `json:"Status"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	jobID := createResp.UserImportJob.JobID
	require.NotEmpty(t, jobID)
	assert.Equal(t, "my-import", createResp.UserImportJob.JobName)
	assert.Equal(t, "Created", createResp.UserImportJob.Status)

	// Describe
	rec = doCognitoRequest(t, h, "DescribeUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doCognitoRequest(t, h, "ListUserImportJobs", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp struct {
		UserImportJobs []struct {
			JobID string `json:"JobId"`
		} `json:"UserImportJobs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.UserImportJobs, 1)

	// GetCSVHeader
	rec = doCognitoRequest(t, h, "GetCSVHeader", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// Start
	rec = doCognitoRequest(t, h, "StartUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var startResp struct {
		UserImportJob struct {
			Status string `json:"Status"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Equal(t, "InProgress", startResp.UserImportJob.Status)

	// Stop
	rec = doCognitoRequest(t, h, "StopUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var stopResp struct {
		UserImportJob struct {
			Status string `json:"Status"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "Stopped", stopResp.UserImportJob.Status)

	// Start again on stopped job — error
	rec = doCognitoRequest(t, h, "StartUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// User Pool Client Secrets
// ---------------------------------------------------------------------------

func TestCompleteness_ClientSecrets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "secrets-pool")

	// List before adding — empty
	rec := doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Secrets []string `json:"Secrets"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Secrets)

	// Add a secret
	rec = doCognitoRequest(t, h, "AddUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after adding — one secret
	rec = doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Secrets, 1)

	// Delete the secret
	rec = doCognitoRequest(t, h, "DeleteUserPoolClientSecret", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — empty again
	rec = doCognitoRequest(t, h, "ListUserPoolClientSecrets", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Secrets)
}

// ---------------------------------------------------------------------------
// GetTokensFromRefreshToken
// ---------------------------------------------------------------------------

func TestCompleteness_GetTokensFromRefreshToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "refresh-token-pool")

	// Sign up and authenticate to get a refresh token.
	doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "refreshuser",
		"Password": "Pass1234!",
	})
	doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "refreshuser",
	})

	authRec := doCognitoRequest(t, h, "InitiateAuth", map[string]any{
		"ClientId":       clientID,
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{"USERNAME": "refreshuser", "PASSWORD": "Pass1234!"},
	})
	require.Equal(t, http.StatusOK, authRec.Code)

	var authResp struct {
		AuthenticationResult struct {
			RefreshToken string `json:"RefreshToken"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	refreshToken := authResp.AuthenticationResult.RefreshToken
	require.NotEmpty(t, refreshToken)

	// GetTokensFromRefreshToken with valid refresh token
	rec := doCognitoRequest(t, h, "GetTokensFromRefreshToken", map[string]any{
		"ClientId":     clientID,
		"RefreshToken": refreshToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var tokenResp struct {
		AuthenticationResult struct {
			AccessToken string `json:"AccessToken"`
		} `json:"AuthenticationResult"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tokenResp))
	assert.NotEmpty(t, tokenResp.AuthenticationResult.AccessToken)
}
