package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Identity Center Applications ----

func TestIdentityCenterApplicationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		appName     string
		instanceARN string
	}{
		{
			name:        "create and delete",
			appName:     "WorkMailApp",
			instanceARN: "arn:aws:sso:::instance/ssoins-000000000000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create
			rec := doOp(t, h, "CreateIdentityCenterApplication", fmt.Sprintf(
				`{"InstanceArn":%q,"Name":%q}`, tc.instanceARN, tc.appName,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := decodeJSON(t, rec)
			appARN, ok := m["ApplicationArn"].(string)
			require.True(t, ok)
			require.NotEmpty(t, appARN)

			// Delete
			rec = doOp(t, h, "DeleteIdentityCenterApplication", fmt.Sprintf(
				`{"ApplicationArn":%q}`, appARN,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete again → not found
			rec = doOp(t, h, "DeleteIdentityCenterApplication", fmt.Sprintf(
				`{"ApplicationArn":%q}`, appARN,
			))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			m = decodeJSON(t, rec)
			assert.Contains(t, m["__type"], "EntityNotFoundException")
		})
	}
}

// ---- Identity Provider Configuration ----

func TestIdentityProviderConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		authMode  string
		patStatus string
		lifetime  int
	}{
		{
			name:      "identity-provider-only",
			authMode:  "IDENTITY_PROVIDER_ONLY",
			patStatus: "ACTIVE",
			lifetime:  90,
		},
		{
			name:      "identity-provider-and-directory",
			authMode:  "IDENTITY_PROVIDER_AND_DIRECTORY",
			patStatus: "INACTIVE",
			lifetime:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "idp-org")

			// Describe before put → not found
			rec := doOp(t, h, "DescribeIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			appARN := "arn:aws:sso:::application/ssoins-abc/apl-123"
			instanceARN := "arn:aws:sso:::instance/ssoins-abc"
			body := fmt.Sprintf(
				`{"OrganizationId":%q,"AuthenticationMode":%q,"IdentityCenterConfiguration":{"ApplicationArn":%q,"InstanceArn":%q},"PersonalAccessTokenConfiguration":{"Status":%q}}`, //nolint:lll // existing issue.
				orgID,
				tc.authMode,
				appARN,
				instanceARN,
				tc.patStatus,
			)
			if tc.lifetime > 0 {
				body = fmt.Sprintf(
					`{"OrganizationId":%q,"AuthenticationMode":%q,"IdentityCenterConfiguration":{"ApplicationArn":%q,"InstanceArn":%q},"PersonalAccessTokenConfiguration":{"Status":%q,"LifetimeInDays":%d}}`, //nolint:lll // existing issue.
					orgID,
					tc.authMode,
					appARN,
					instanceARN,
					tc.patStatus,
					tc.lifetime,
				)
			}

			// Put
			rec = doOp(t, h, "PutIdentityProviderConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			// Describe
			rec = doOp(t, h, "DescribeIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m := decodeJSON(t, rec)
			assert.Equal(t, tc.authMode, m["AuthenticationMode"])
			icc, ok := m["IdentityCenterConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, appARN, icc["ApplicationArn"])
			patCfg, ok := m["PersonalAccessTokenConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tc.patStatus, patCfg["Status"])

			// Delete
			rec = doOp(t, h, "DeleteIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe after delete → not found
			rec = doOp(t, h, "DescribeIdentityProviderConfiguration", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
