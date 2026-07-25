package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestEnableDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "enable_returns_account",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2", "ECR"},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts, ok := resp["accounts"].([]any)
				require.True(t, ok)
				require.Len(t, accounts, 1)

				acc := accounts[0].(map[string]any)
				assert.Equal(t, "ENABLED", acc["status"])
			},
		},
		{
			name: "disable_returns_account",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				// Enable only EC2, then disable it → all types disabled → DISABLED.
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})

				rec := auditDo(t, h, http.MethodPost, "/disable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts, ok := resp["accounts"].([]any)
				require.True(t, ok)
				require.Len(t, accounts, 1)

				acc := accounts[0].(map[string]any)
				assert.Equal(t, "DISABLED", acc["status"])
			},
		},
		{
			name: "enable_empty_body",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/enable", nil)
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

func TestBatchGetAccountStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "disabled_by_default",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/status/batch/get", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts, ok := resp["accounts"].([]any)
				require.True(t, ok)
				require.Len(t, accounts, 1)

				acc := accounts[0].(map[string]any)
				state := acc["state"].(map[string]any)
				assert.Equal(t, "DISABLED", state["status"])
			},
		},
		{
			name: "enabled_after_enable",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})

				rec := auditDo(t, h, http.MethodPost, "/status/batch/get", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts := resp["accounts"].([]any)
				acc := accounts[0].(map[string]any)
				state := acc["state"].(map[string]any)
				assert.Equal(t, "ENABLED", state["status"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

func TestConfigurationGetUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "get_defaults",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/configuration/get", nil)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				_, hasEC2 := resp["ec2Configuration"]
				assert.True(t, hasEC2)
				_, hasECR := resp["ecrConfiguration"]
				assert.True(t, hasECR)
			},
		},
		{
			name: "update_configuration",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/configuration/update", map[string]any{
					"ec2Configuration": map[string]any{
						"scanMode": "EC2_HYBRID",
					},
					"ecrConfiguration": map[string]any{
						"rescanDuration": "DAYS_30",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				// Verify via get
				getRec := auditDo(t, h, http.MethodPost, "/configuration/get", nil)
				var getResp map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

				ec2Cfg := getResp["ec2Configuration"].(map[string]any)
				scanModeState := ec2Cfg["scanModeState"].(map[string]any)
				assert.Equal(t, "EC2_HYBRID", scanModeState["scanMode"])

				ecrCfg := getResp["ecrConfiguration"].(map[string]any)
				rescanState := ecrCfg["rescanDurationState"].(map[string]any)
				assert.Equal(t, "DAYS_30", rescanState["rescanDuration"])
			},
		},
		{
			name: "update_empty_body",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/configuration/update", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

// TestAccountPermissions locks the real Permission wire shape
// (operation/service) -- the prior stub always returned an empty list, and
// before that a gopherstack-invented "status" field stood in for the real
// "service" field.
func TestAccountPermissions(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/accountpermissions/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	perms, ok := resp["permissions"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, perms)

	p, ok := perms[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, p["operation"])
	assert.NotEmpty(t, p["service"])
	assert.NotContains(t, p, "status", "real Permission shape has no \"status\" member")
}

// TestAccountPermissionsFilteredByService verifies the optional service
// filter narrows the returned permission matrix.
func TestAccountPermissionsFilteredByService(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/accountpermissions/list", map[string]any{
		"service": "ECR",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	perms, ok := resp["permissions"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, perms)

	for _, raw := range perms {
		p, entryOk := raw.(map[string]any)
		require.True(t, entryOk)
		assert.Equal(t, "ECR", p["service"])
	}
}

func TestEnableDisablePerResourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(t *testing.T, h *inspector2.Handler)
		wantEC2     string
		wantECR     string
		wantLambda  string
		wantOverall string
	}{
		{
			name:        "all_disabled_by_default",
			setup:       func(_ *testing.T, _ *inspector2.Handler) {},
			wantEC2:     "DISABLED",
			wantECR:     "DISABLED",
			wantLambda:  "DISABLED",
			wantOverall: "DISABLED",
		},
		{
			name: "enable_ec2_only",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})
			},
			wantEC2:     "ENABLED",
			wantECR:     "DISABLED",
			wantLambda:  "DISABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_ecr_only",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"ECR"},
				})
			},
			wantEC2:     "DISABLED",
			wantECR:     "ENABLED",
			wantLambda:  "DISABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_lambda_only",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"LAMBDA"},
				})
			},
			wantEC2:     "DISABLED",
			wantECR:     "DISABLED",
			wantLambda:  "ENABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_ec2_ecr_disable_ec2_leaves_ecr_enabled",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2", "ECR"},
				})
				auditDo(t, h, http.MethodPost, "/disable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})
			},
			wantEC2:     "DISABLED",
			wantECR:     "ENABLED",
			wantLambda:  "DISABLED",
			wantOverall: "ENABLED",
		},
		{
			name: "enable_all_then_disable_all_is_disabled",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})
				auditDo(t, h, http.MethodPost, "/disable", map[string]any{})
			},
			wantEC2:     "DISABLED",
			wantECR:     "DISABLED",
			wantLambda:  "DISABLED",
			wantOverall: "DISABLED",
		},
		{
			name: "enable_empty_body_enables_all",
			setup: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})
			},
			wantEC2:     "ENABLED",
			wantECR:     "ENABLED",
			wantLambda:  "ENABLED",
			wantOverall: "ENABLED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			tc.setup(t, h)

			rec := auditDo(t, h, http.MethodPost, "/status/batch/get", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			accounts := resp["accounts"].([]any)
			require.Len(t, accounts, 1)

			// BatchGetAccountStatus uses the AccountState wire shape: "state"
			// is itself a State object (status/errorCode/errorMessage), and
			// "resourceState" nests a State object per resource type --
			// unlike Enable/Disable's flatter Account.resourceStatus shape.
			acc := accounts[0].(map[string]any)
			state := acc["state"].(map[string]any)
			resourceState := acc["resourceState"].(map[string]any)

			assert.Equal(t, tc.wantOverall, state["status"], "overall status")
			assert.Equal(t, tc.wantEC2, resourceState["ec2"].(map[string]any)["status"], "ec2 status")
			assert.Equal(t, tc.wantECR, resourceState["ecr"].(map[string]any)["status"], "ecr status")
			assert.Equal(t, tc.wantLambda, resourceState["lambda"].(map[string]any)["status"], "lambda status")
		})
	}
}
