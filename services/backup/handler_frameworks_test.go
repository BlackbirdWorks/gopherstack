package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestFrameworkControls(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatus2    string
		wantStatus     int
		wantControlLen int
	}{
		{
			name: "framework_with_controls",
			body: `{
				"FrameworkName": "fw-with-controls",
				"FrameworkDescription": "compliance framework",
				"FrameworkControls": [
					{
						"ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_VAULT_LOCK",
						"ControlInputParameters": [
							{"ParameterName": "requiredRetentionDays", "ParameterValue": "30"}
						],
						"ControlScope": {
							"ComplianceResourceTypes": ["EBS"],
							"Tags": {"Environment": "prod"}
						}
					},
					{
						"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_PERIOD",
						"ControlInputParameters": [
							{"ParameterName": "requiredFrequencyValue", "ParameterValue": "1"}
						]
					}
				]
			}`,
			wantStatus:     http.StatusOK,
			wantControlLen: 2,
			wantStatus2:    "ACTIVE",
		},
		{
			name: "framework_no_controls",
			body: `{
				"FrameworkName": "fw-no-controls",
				"FrameworkDescription": "empty framework"
			}`,
			wantStatus:     http.StatusOK,
			wantControlLen: 0,
			wantStatus2:    "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandler(t)

			resp := doRequest(t, h, http.MethodPost, "/audit/frameworks", tt.body)
			require.Equal(t, tt.wantStatus, resp.Code)

			var createData map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &createData))
			fwName := createData["FrameworkName"].(string)

			// Describe and verify controls.
			getResp := doRequest(t, h, http.MethodGet, "/audit/frameworks/"+fwName, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var fwData map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &fwData))
			assert.Equal(t, tt.wantStatus2, fwData["FrameworkStatus"])
			assert.Equal(t, "COMPLETED", fwData["DeploymentStatus"])
			if tt.wantControlLen > 0 {
				controls, ok := fwData["FrameworkControls"].([]any)
				require.True(t, ok, "FrameworkControls should be present")
				assert.Len(t, controls, tt.wantControlLen)
				ctrl := controls[0].(map[string]any)
				assert.NotEmpty(t, ctrl["ControlName"])

				// ControlInputParameters must round-trip as an ARRAY of
				// {ParameterName, ParameterValue}, matching AWS's real wire
				// shape -- not a free-form map (a bug this pass fixed).
				params, ok := ctrl["ControlInputParameters"].([]any)
				require.True(t, ok, "ControlInputParameters should be an array")
				require.Len(t, params, 1)
				param := params[0].(map[string]any)
				assert.Equal(t, "requiredRetentionDays", param["ParameterName"])
				assert.Equal(t, "30", param["ParameterValue"])

				// ControlScope must round-trip as a STRUCT, not a free-form map.
				scope, ok := ctrl["ControlScope"].(map[string]any)
				require.True(t, ok, "ControlScope should be a struct")
				resourceTypes, ok := scope["ComplianceResourceTypes"].([]any)
				require.True(t, ok)
				assert.Equal(t, []any{"EBS"}, resourceTypes)
				tagsMap, ok := scope["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tagsMap["Environment"])
			}
		})
	}
}

// ---- ReportPlan sub-fields (item 24) ----

// TestCreateFramework exercises the audit framework creation operation.
func TestCreateFramework(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName":        "my-framework",
					"FrameworkDescription": "A test framework",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-framework", resp["FrameworkName"])
				assert.NotEmpty(t, resp["FrameworkArn"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "dup-framework",
				})
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "dup-framework",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

func TestAuditFramework_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	// Create
	rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
		"FrameworkName": "myframework",
		"FrameworkControls": []map[string]any{{
			"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	frameworkArn, ok := resp["FrameworkArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, frameworkArn)

	// Describe
	rec2 := doREST(t, h, http.MethodGet, "/audit/frameworks/myframework", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List
	rec3 := doREST(t, h, http.MethodGet, "/audit/frameworks", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	fwks, ok := listResp["Frameworks"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, fwks)

	// Update: replaces FrameworkControls, verified via a subsequent Describe
	// (UpdateFrameworkInput.FrameworkControls was previously not even
	// accepted by this backend's UpdateFramework).
	rec4 := doREST(t, h, http.MethodPut, "/audit/frameworks/myframework", map[string]any{
		"FrameworkControls": []map[string]any{{
			"ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN",
			"ControlInputParameters": []map[string]any{
				{"ParameterName": "requiredFrequencyUnit", "ParameterValue": "hours"},
			},
		}},
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	rec4b := doREST(t, h, http.MethodGet, "/audit/frameworks/myframework", nil)
	require.Equal(t, http.StatusOK, rec4b.Code)
	updated := parseResp(t, rec4b)
	controls, ok := updated["FrameworkControls"].([]any)
	require.True(t, ok)
	require.Len(t, controls, 1)
	ctrl := controls[0].(map[string]any)
	assert.Equal(t, "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN", ctrl["ControlName"])

	// Delete
	rec5 := doREST(t, h, http.MethodDelete, "/audit/frameworks/myframework", nil)
	assert.Equal(t, http.StatusOK, rec5.Code)
}
