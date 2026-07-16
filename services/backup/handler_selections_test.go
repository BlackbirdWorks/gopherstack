package backup_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestBackupSelectionResourceFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		selection map[string]any
		wantSel   func(t *testing.T, sel map[string]any)
		name      string
	}{
		{
			name: "resources_round_trip",
			selection: map[string]any{
				"SelectionName": "sel1",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"Resources": []any{
					"arn:aws:ec2:us-east-1:000000000000:instance/i-1234",
					"arn:aws:rds:us-east-1:000000000000:db:mydb",
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				resources, ok := sel["Resources"].([]any)
				require.True(t, ok, "Resources should be present")
				assert.Len(t, resources, 2)
			},
		},
		{
			name: "not_resources_round_trip",
			selection: map[string]any{
				"SelectionName": "sel2",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"NotResources": []any{
					"arn:aws:s3:::my-exclude-bucket",
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				notRes, ok := sel["NotResources"].([]any)
				require.True(t, ok, "NotResources should be present")
				assert.Len(t, notRes, 1)
			},
		},
		{
			name: "list_of_tags_round_trip",
			selection: map[string]any{
				"SelectionName": "sel3",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"ListOfTags": []any{
					map[string]any{
						"ConditionType":  "STRINGEQUALS",
						"ConditionKey":   "backup",
						"ConditionValue": "true",
					},
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				tags, ok := sel["ListOfTags"].([]any)
				require.True(t, ok, "ListOfTags should be present")
				require.Len(t, tags, 1)
				tag := tags[0].(map[string]any)
				assert.Equal(t, "STRINGEQUALS", tag["ConditionType"])
				assert.Equal(t, "backup", tag["ConditionKey"])
			},
		},
		{
			name: "conditions_round_trip",
			selection: map[string]any{
				"SelectionName": "sel4",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"Conditions": map[string]any{
					"StringEquals": []any{
						map[string]any{"Key": "aws:ResourceTag/env", "Value": "prod"},
					},
					"StringLike": []any{
						map[string]any{"Key": "aws:ResourceTag/app", "Value": "pay*"},
					},
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				conds, ok := sel["Conditions"]
				require.True(t, ok, "Conditions should be present")
				_ = conds
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			// Create plan first.
			planResp := doBatch1Request(t, h, http.MethodPut, "/backup/plans",
				`{"BackupPlan":{"BackupPlanName":"p","Rules":[{"RuleName":"r","TargetBackupVaultName":"v"}]}}`)
			require.Equal(t, http.StatusOK, planResp.Code)
			var planData map[string]any
			require.NoError(t, json.Unmarshal(planResp.Body.Bytes(), &planData))
			planID := planData["BackupPlanId"].(string)

			// Create selection.
			body, err := json.Marshal(map[string]any{"BackupSelection": tt.selection})
			require.NoError(t, err)
			selResp := doBatch1Request(t, h, http.MethodPut,
				"/backup/plans/"+planID+"/selections", string(body))
			require.Equal(t, http.StatusOK, selResp.Code)

			var selData map[string]any
			require.NoError(t, json.Unmarshal(selResp.Body.Bytes(), &selData))
			selID := selData["SelectionId"].(string)

			// Get selection and verify fields.
			getResp := doBatch1Request(t, h, http.MethodGet,
				"/backup/plans/"+planID+"/selections/"+selID, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var getData map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getData))
			selDoc := getData["BackupSelection"].(map[string]any)
			tt.wantSel(t, selDoc)
		})
	}
}

// ---- Vault name underscore (item 20) ----

// TestCreateBackupSelection exercises the backup selection creation operation.
func TestCreateBackupSelection(t *testing.T) {
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
				createPlanRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "my-plan", "Rules": []any{}},
				})
				require.Equal(t, http.StatusOK, createPlanRec.Code)
				planResp := parseResp(t, createPlanRec)
				planID, ok := planResp["BackupPlanId"].(string)
				require.True(t, ok)

				selRec := doREST(t, h, http.MethodPut, "/backup/plans/"+planID+"/selections", map[string]any{
					"BackupSelection": map[string]any{
						"SelectionName": "my-selection",
						"IamRoleArn":    "arn:aws:iam::123456789012:role/backup-role",
					},
				})
				require.Equal(t, http.StatusOK, selRec.Code)
				selResp := parseResp(t, selRec)
				assert.Equal(t, planID, selResp["BackupPlanId"])
				assert.NotEmpty(t, selResp["SelectionId"])
			},
		},
		{
			name: "plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup/plans/nonexistent/selections", map[string]any{
					"BackupSelection": map[string]any{"SelectionName": "sel"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

// TestBackupSelectionValidation covers SelectionName required check.
func TestBackupSelectionValidation(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "plan"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	planResp := parseResp(t, rec)
	planID := planResp["BackupPlanId"].(string)

	rec2 := doREST(t, h, http.MethodPut, "/backup/plans/"+planID+"/selections", map[string]any{
		"BackupSelection": map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestParity_ListBackupSelections_IamRoleArn verifies that ListBackupSelections
// includes IamRoleArn in each selection item, matching real AWS behavior.
func TestParity_ListBackupSelections_IamRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	createPlanRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "sel-iam-plan",
			"Rules": []map[string]any{
				{
					"RuleName":              "daily",
					"TargetBackupVaultName": "Default",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createPlanRec.Code)
	planID, _ := parseResp(t, createPlanRec)["BackupPlanId"].(string)
	require.NotEmpty(t, planID)

	const roleArn = "arn:aws:iam::123456789012:role/backup-role"
	createSelRec := doREST(t, h, http.MethodPut, "/backup/plans/"+planID+"/selections", map[string]any{
		"BackupSelection": map[string]any{
			"SelectionName": "my-selection",
			"IamRoleArn":    roleArn,
		},
	})
	require.Equal(t, http.StatusOK, createSelRec.Code, "create selection: %s", createSelRec.Body)

	listRec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID+"/selections", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	resp := parseResp(t, listRec)
	items, _ := resp["BackupSelectionsList"].([]any)
	require.NotEmpty(t, items, "BackupSelectionsList must not be empty")

	item, _ := items[0].(map[string]any)
	arn, hasArn := item["IamRoleArn"]
	assert.True(t, hasArn, "ListBackupSelections item must include IamRoleArn")
	assert.Equal(t, roleArn, arn, "IamRoleArn must match what was set on creation")
}

func TestBackupSelection_CreateGetListDelete(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup-vaults/sel-vault", nil)
	planID := createBackupPlan(t, h, "selplan")

	// Create selection
	rec := doREST(t, h, http.MethodPut, fmt.Sprintf("/backup/plans/%s/selections", planID), map[string]any{
		"BackupSelection": map[string]any{
			"SelectionName": "myselection",
			"IamRoleArn":    "arn:aws:iam::123456789012:role/role",
			"Resources":     []string{"arn:aws:ec2:us-east-1:123456789012:instance/*"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	selID, ok := resp["SelectionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, selID)

	// Get selection
	rec2 := doREST(t, h, http.MethodGet, fmt.Sprintf("/backup/plans/%s/selections/%s", planID, selID), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List selections
	rec3 := doREST(t, h, http.MethodGet, fmt.Sprintf("/backup/plans/%s/selections", planID), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	sels, ok := listResp["BackupSelectionsList"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, sels)

	// Delete selection
	rec4 := doREST(t, h, http.MethodDelete, fmt.Sprintf("/backup/plans/%s/selections/%s", planID, selID), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}
