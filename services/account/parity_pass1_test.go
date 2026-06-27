package account_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_EnableDisableRegion verifies that opt-in regions can be enabled and disabled.
func TestParity_EnableDisableRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		regionName string
	}{
		{name: "ap-southeast-1", regionName: "ap-southeast-1"},
		{name: "ap-northeast-1", regionName: "ap-northeast-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Initially ENABLED (opt-in).
			statusRec := doRequest(t, h, http.MethodGet, "/regions/"+tc.regionName, nil)
			require.Equal(t, http.StatusOK, statusRec.Code)

			var statusOut map[string]any
			require.NoError(t, json.NewDecoder(statusRec.Body).Decode(&statusOut))
			assert.Equal(t, "ENABLED", statusOut["RegionOptStatus"])

			// Disable.
			disableRec := doRequest(t, h, http.MethodPost, "/regions/disable", map[string]any{
				"RegionName": tc.regionName,
			})
			assert.Equal(t, http.StatusOK, disableRec.Code)

			// GetRegionOptStatus reflects DISABLED.
			afterDisableRec := doRequest(t, h, http.MethodGet, "/regions/"+tc.regionName, nil)
			require.Equal(t, http.StatusOK, afterDisableRec.Code)

			var afterDisable map[string]any
			require.NoError(t, json.NewDecoder(afterDisableRec.Body).Decode(&afterDisable))
			assert.Equal(t, "DISABLED", afterDisable["RegionOptStatus"])

			// Re-enable.
			enableRec := doRequest(t, h, http.MethodPost, "/regions/enable", map[string]any{
				"RegionName": tc.regionName,
			})
			assert.Equal(t, http.StatusOK, enableRec.Code)

			// GetRegionOptStatus reflects ENABLED again.
			afterEnableRec := doRequest(t, h, http.MethodGet, "/regions/"+tc.regionName, nil)
			require.Equal(t, http.StatusOK, afterEnableRec.Code)

			var afterEnable map[string]any
			require.NoError(t, json.NewDecoder(afterEnableRec.Body).Decode(&afterEnable))
			assert.Equal(t, "ENABLED", afterEnable["RegionOptStatus"])
		})
	}
}

// TestParity_EnableDisableRegion_DefaultRegionRejected verifies ENABLED_BY_DEFAULT regions
// cannot be enabled or disabled.
func TestParity_EnableDisableRegion_DefaultRegionRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		path   string
	}{
		{name: "disable_default", action: "disable", path: "/regions/disable"},
		{name: "enable_default", action: "enable", path: "/regions/enable"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, map[string]any{
				"RegionName": "us-east-1",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestParity_EnableDisableRegion_MissingRegionName verifies validation.
func TestParity_EnableDisableRegion_MissingRegionName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "enable_missing", path: "/regions/enable"},
		{name: "disable_missing", path: "/regions/disable"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestParity_EnableDisableRegion_NotFound verifies unknown regions return 404.
func TestParity_EnableDisableRegion_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "enable_unknown", path: "/regions/enable"},
		{name: "disable_unknown", path: "/regions/disable"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, map[string]any{
				"RegionName": "zz-invalid-1",
			})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestParity_GetRegionOptStatus verifies the opt-status endpoint returns
// the correct region name and status.
func TestParity_GetRegionOptStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		regionName     string
		wantStatus     string
		wantHTTPStatus int
	}{
		{
			name:           "enabled_by_default",
			regionName:     "us-east-1",
			wantStatus:     "ENABLED_BY_DEFAULT",
			wantHTTPStatus: http.StatusOK,
		},
		{
			name:           "opt_in_enabled",
			regionName:     "ap-southeast-1",
			wantStatus:     "ENABLED",
			wantHTTPStatus: http.StatusOK,
		},
		{
			name:           "unknown_region",
			regionName:     "zz-fake-1",
			wantHTTPStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/regions/"+tc.regionName, nil)
			require.Equal(t, tc.wantHTTPStatus, rec.Code)

			if tc.wantStatus != "" {
				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tc.regionName, out["RegionName"])
				assert.Equal(t, tc.wantStatus, out["RegionOptStatus"])
			}
		})
	}
}

// TestParity_PrimaryEmail_GetDefault verifies the initial primary email is returned.
func TestParity_PrimaryEmail_GetDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get_default_email"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/account/primaryEmail", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out["PrimaryEmail"])
		})
	}
}

// TestParity_PrimaryEmail_UpdateFlow verifies the StartPrimaryEmailUpdate /
// AcceptPrimaryEmailUpdate two-step flow.
func TestParity_PrimaryEmail_UpdateFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newEmail string
	}{
		{name: "update_email", newEmail: "new@example.com"},
		{name: "update_email_again", newEmail: "another@example.org"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Start update.
			startRec := doRequest(t, h, http.MethodPut, "/account/primaryEmail", map[string]any{
				"PrimaryEmail": tc.newEmail,
			})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut map[string]any
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
			assert.Equal(t, "PENDING", startOut["Status"])

			// Accept with the fixed sim OTP.
			acceptRec := doRequest(t, h, http.MethodPut, "/account/primaryEmail/accept", map[string]any{
				"Otp":          "123456",
				"PrimaryEmail": tc.newEmail,
			})
			require.Equal(t, http.StatusOK, acceptRec.Code)

			var acceptOut map[string]any
			require.NoError(t, json.NewDecoder(acceptRec.Body).Decode(&acceptOut))
			assert.Equal(t, "ACCEPTED", acceptOut["Status"])

			// GetPrimaryEmail reflects new address.
			getRec := doRequest(t, h, http.MethodGet, "/account/primaryEmail", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			assert.Equal(t, tc.newEmail, getOut["PrimaryEmail"])
		})
	}
}

// TestParity_PrimaryEmail_AcceptInvalidOTP verifies wrong OTP is rejected.
func TestParity_PrimaryEmail_AcceptInvalidOTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		otp        string
		wantStatus int
	}{
		{name: "wrong_otp", otp: "000000", wantStatus: http.StatusBadRequest},
		{name: "no_pending", otp: "", wantStatus: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.otp != "" {
				// Start an update so there IS a pending state.
				doRequest(t, h, http.MethodPut, "/account/primaryEmail", map[string]any{
					"PrimaryEmail": "x@example.com",
				})

				rec := doRequest(t, h, http.MethodPut, "/account/primaryEmail/accept", map[string]any{
					"Otp":          tc.otp,
					"PrimaryEmail": "x@example.com",
				})
				assert.Equal(t, tc.wantStatus, rec.Code)
			} else {
				// No pending update; any accept should fail.
				rec := doRequest(t, h, http.MethodPut, "/account/primaryEmail/accept", map[string]any{
					"Otp":          "999999",
					"PrimaryEmail": "y@example.com",
				})
				assert.Equal(t, tc.wantStatus, rec.Code)
			}
		})
	}
}

// TestParity_PrimaryEmail_StartMissingEmail verifies validation.
func TestParity_PrimaryEmail_StartMissingEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "missing_primary_email"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/account/primaryEmail", map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code, tc.name)
		})
	}
}

// TestParity_PutAccountName verifies the account name can be updated.
func TestParity_PutAccountName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accountName string
		wantStatus  int
	}{
		{name: "valid_name", accountName: "My Corp", wantStatus: http.StatusOK},
		{name: "empty_name", accountName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{}
			if tc.accountName != "" {
				body["AccountName"] = tc.accountName
			}

			rec := doRequest(t, h, http.MethodPut, "/account/accountName", body)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantStatus == http.StatusOK {
				// DescribeAccount reflects updated name.
				descRec := doRequest(t, h, http.MethodGet, "/account", nil)
				require.Equal(t, http.StatusOK, descRec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(descRec.Body).Decode(&out))
				acct := out["Account"].(map[string]any)
				assert.Equal(t, tc.accountName, acct["Name"])
			}
		})
	}
}

// TestParity_CloseAccount verifies CloseAccount returns 200.
func TestParity_CloseAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "close_account"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/account", nil)
			assert.Equal(t, http.StatusOK, rec.Code, tc.name)
		})
	}
}

// TestParity_GetAlternateContact_InvalidType verifies enum validation.
func TestParity_GetAlternateContact_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contactType string
		wantStatus  int
	}{
		{name: "invalid_type", contactType: "INVALID", wantStatus: http.StatusBadRequest},
		{name: "empty_type", contactType: "", wantStatus: http.StatusBadRequest},
		{name: "billing_valid", contactType: "BILLING", wantStatus: http.StatusNotFound},
		{name: "operations_valid", contactType: "OPERATIONS", wantStatus: http.StatusNotFound},
		{name: "security_valid", contactType: "SECURITY", wantStatus: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t,
				h,
				http.MethodGet,
				"/account/alternateContact?alternateContactType="+tc.contactType,
				nil,
			)
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

// TestParity_DeleteAlternateContact_InvalidType verifies enum validation on delete.
func TestParity_DeleteAlternateContact_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contactType string
		wantStatus  int
	}{
		{name: "invalid_type", contactType: "BOGUS", wantStatus: http.StatusBadRequest},
		{name: "empty_type", contactType: "", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t,
				h,
				http.MethodDelete,
				"/account/alternateContact?alternateContactType="+tc.contactType,
				nil,
			)
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

// TestParity_DescribeAccount_ReflectsPrimaryEmail verifies DescribeAccount returns
// the current primary email (not a hardcoded value).
func TestParity_DescribeAccount_ReflectsPrimaryEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "email_reflects_after_update"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Update email and confirm DescribeAccount shows it.
			doRequest(t, h, http.MethodPut, "/account/primaryEmail", map[string]any{
				"PrimaryEmail": "updated@corp.io",
			})
			doRequest(t, h, http.MethodPut, "/account/primaryEmail/accept", map[string]any{
				"Otp":          "123456",
				"PrimaryEmail": "updated@corp.io",
			})

			rec := doRequest(t, h, http.MethodGet, "/account", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			acct := out["Account"].(map[string]any)
			assert.Equal(t, "updated@corp.io", acct["Email"], tc.name)
		})
	}
}

// TestParity_GetSupportedOperations verifies all new ops are listed.
func TestParity_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	expectedOps := []struct {
		name string
		op   string
	}{
		{name: "EnableRegion", op: "EnableRegion"},
		{name: "DisableRegion", op: "DisableRegion"},
		{name: "GetRegionOptStatus", op: "GetRegionOptStatus"},
		{name: "GetPrimaryEmail", op: "GetPrimaryEmail"},
		{name: "StartPrimaryEmailUpdate", op: "StartPrimaryEmailUpdate"},
		{name: "AcceptPrimaryEmailUpdate", op: "AcceptPrimaryEmailUpdate"},
		{name: "PutAccountName", op: "PutAccountName"},
		{name: "CloseAccount", op: "CloseAccount"},
	}

	for _, tc := range expectedOps {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ops := h.GetSupportedOperations()
			assert.Contains(t, ops, tc.op)
		})
	}
}
