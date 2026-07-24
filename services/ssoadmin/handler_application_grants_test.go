package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplicationGrantStructured verifies PutApplicationGrant stores structured body.
func TestApplicationGrantStructured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		grantType  string
		wantStatus int
	}{
		{
			name:       "authorization_code grant accepted",
			grantType:  "authorization_code",
			wantStatus: http.StatusOK,
		},
		{
			name:       "refresh_token grant accepted",
			grantType:  "refresh_token",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid grant type rejected",
			grantType:  "password",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "grant-inst")
			appArn := createApplication(t, h, instanceArn, "GrantApp")

			rec := doRequest(t, h, "PutApplicationGrant", map[string]any{
				"ApplicationArn": appArn,
				"GrantType":      tt.grantType,
				"Grant":          map[string]any{},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetApplicationGrant verifies GetApplicationGrant operation.
func TestGetApplicationGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		grantType  string
		putGrant   bool
		wantStatus int
	}{
		{
			name:       "get_existing_grant",
			grantType:  "authorization_code",
			putGrant:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_nonexistent_grant",
			grantType:  "implicit",
			putGrant:   false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_grant_type_param",
			grantType:  "",
			putGrant:   false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-grant-inst")
			appRec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "r3-grant-app",
			})
			require.Equal(t, http.StatusOK, appRec.Code)
			appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

			if tt.putGrant {
				putRec := doRequest(t, h, "PutApplicationGrant", map[string]any{
					"ApplicationArn": appArn,
					"GrantType":      tt.grantType,
					"Grant": map[string]any{
						"AuthorizationCode": map[string]any{"RedirectUris": []string{"https://example.com"}},
					},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			rec := doRequest(t, h, "GetApplicationGrant", map[string]any{
				"ApplicationArn": appArn,
				"GrantType":      tt.grantType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				// Real GetApplicationGrantOutput is exactly {Grant: <union>}
				// -- the union's wire shape is {"AuthorizationCode": {...}},
				// with NO sibling "GrantType" field alongside it.
				grant, ok := resp["Grant"].(map[string]any)
				require.True(t, ok)
				assert.NotContains(t, grant, "GrantType",
					"GetApplicationGrantOutput's union has no sibling GrantType member")
				assert.Contains(t, grant, "AuthorizationCode")
			}
		})
	}
}
