package ssoadmin_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplicationAuthMethodStructured verifies PutApplicationAuthenticationMethod stores structured body.
func TestApplicationAuthMethodStructured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		authBody       map[string]any
		name           string
		authMethodType string
		wantStatus     int
	}{
		{
			name:           "IAM auth method accepted",
			authMethodType: "IAM",
			authBody:       map[string]any{"Iam": map[string]any{}},
			wantStatus:     http.StatusOK,
		},
		{
			name:           "non-IAM auth method rejected",
			authMethodType: "SAML",
			authBody:       map[string]any{},
			wantStatus:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "auth-method-inst")
			appArn := createApplication(t, h, instanceArn, "AuthMethodApp")

			bodyBytes, err := json.Marshal(tt.authBody)
			require.NoError(t, err)

			rec := doRequest(t, h, "PutApplicationAuthenticationMethod", map[string]any{
				"ApplicationArn":           appArn,
				"AuthenticationMethodType": tt.authMethodType,
				"AuthenticationMethod":     json.RawMessage(bodyBytes),
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeleteApplicationAuthenticationMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		authMethodType string
		wantStatus     int
		useInvalidApp  bool
	}{
		{
			name:           "delete auth method from nonexistent app",
			authMethodType: "IAM",
			wantStatus:     http.StatusBadRequest,
			useInvalidApp:  true,
		},
		{
			name:           "delete nonexistent auth method from valid app",
			authMethodType: "IAM",
			wantStatus:     http.StatusBadRequest,
			useInvalidApp:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var appArn string
			if tt.useInvalidApp {
				appArn = "arn:aws:sso::123456789012:application/ssoins-bad/apl-notexist"
			} else {
				instanceArn := createInstance(t, h, "auth-app-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AuthApp",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)
				appArn = resp["ApplicationArn"].(string)
			}
			rec := doRequest(t, h, "DeleteApplicationAuthenticationMethod", map[string]any{
				"ApplicationArn":           appArn,
				"AuthenticationMethodType": tt.authMethodType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetApplicationAuthenticationMethod verifies GetApplicationAuthenticationMethod.
func TestGetApplicationAuthenticationMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authType   string
		putAuth    bool
		wantStatus int
	}{
		{
			name:       "get_existing_auth_method",
			authType:   "IAM",
			putAuth:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_nonexistent_auth_method",
			authType:   "SAML",
			putAuth:    false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-auth-inst")
			appRec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "r3-auth-app",
			})
			require.Equal(t, http.StatusOK, appRec.Code)
			appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

			if tt.putAuth {
				putRec := doRequest(t, h, "PutApplicationAuthenticationMethod", map[string]any{
					"ApplicationArn":           appArn,
					"AuthenticationMethodType": tt.authType,
					"AuthenticationMethod": map[string]any{
						"Iam": map[string]any{"ActorPolicy": map[string]any{}},
					},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			rec := doRequest(t, h, "GetApplicationAuthenticationMethod", map[string]any{
				"ApplicationArn":           appArn,
				"AuthenticationMethodType": tt.authType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				// Real GetApplicationAuthenticationMethodOutput is exactly
				// {AuthenticationMethod: <union>} -- the union's wire shape is
				// {"Iam": {...IamAuthenticationMethod fields...}}, with NO
				// sibling "AuthenticationMethodType" field alongside it.
				authMethod, ok := resp["AuthenticationMethod"].(map[string]any)
				require.True(t, ok)
				assert.NotContains(t, authMethod, "AuthenticationMethodType",
					"GetApplicationAuthenticationMethodOutput's union has no sibling AuthenticationMethodType member")
				assert.Contains(t, authMethod, "Iam")
			}
		})
	}
}
