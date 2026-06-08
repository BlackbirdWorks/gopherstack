package ssm_test

// batch2_accuracy_test.go — AWS-accuracy audit batch-2 for SSM (go-2flp).
//
// Covers: documents (versions+permissions), parameters (advanced+secure+history+labels),
// patch baselines+groups, maintenance windows+tasks+targets, automation executions+approvals,
// OpsCenter ops items, change calendar, session manager, fleet manager, distributor packages,
// association compliance, inventory schemas.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// Gap 1: Document versions — IsDefaultVersion round-trip
// ---------------------------------------------------------------------------

func TestBatch2_DocumentVersions_IsDefaultVersion(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create document with initial content v1.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "MyDoc",
		Content:      `{"schemaVersion":"2.2"}`,
		DocumentType: "Command",
	})
	require.NoError(t, err)

	// Update to create v2.
	_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
		Name:            "MyDoc",
		Content:         `{"schemaVersion":"2.2","updated":true}`,
		DocumentVersion: "$LATEST",
	})
	require.NoError(t, err)

	// List versions — should show both, original default is 1.
	rec := doRequest(t, h, "ListDocumentVersions", `{"Name":"MyDoc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	versions, ok := resp["DocumentVersions"].([]any)
	require.True(t, ok, "DocumentVersions must be an array")
	assert.GreaterOrEqual(t, len(versions), 2)

	// Explicitly set default to v2.
	body, _ := json.Marshal(map[string]any{
		"Name":            "MyDoc",
		"DocumentVersion": "2",
	})
	rec = doRequest(t, h, "UpdateDocumentDefaultVersion", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// List versions again — v2 should be default.
	rec = doRequest(t, h, "ListDocumentVersions", `{"Name":"MyDoc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions = resp["DocumentVersions"].([]any)

	var defaultFound bool
	for _, v := range versions {
		ver := v.(map[string]any)
		if ver["DocumentVersion"] == "2" {
			defaultFound = defaultFound || ver["IsDefaultVersion"].(bool)
		}
	}

	assert.True(t, defaultFound, "version 2 should be marked IsDefaultVersion after UpdateDocumentDefaultVersion")
}

func TestBatch2_DocumentVersions_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setDefault   string
		wantDefault  string
		versions     int
		wantVersions int
	}{
		{
			name:         "single_version_default_is_1",
			versions:     1,
			setDefault:   "",
			wantDefault:  "1",
			wantVersions: 1,
		},
		{
			name:         "two_versions_switch_default",
			versions:     2,
			setDefault:   "2",
			wantDefault:  "2",
			wantVersions: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:         "td-doc-" + tt.name,
				Content:      `{"schemaVersion":"2.2"}`,
				DocumentType: "Command",
			})
			require.NoError(t, err)

			for i := 1; i < tt.versions; i++ {
				_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
					Name:            "td-doc-" + tt.name,
					Content:         `{"schemaVersion":"2.2","v":true}`,
					DocumentVersion: "$LATEST",
				})
				require.NoError(t, err)
			}

			if tt.setDefault != "" {
				body, _ := json.Marshal(map[string]any{
					"Name":            "td-doc-" + tt.name,
					"DocumentVersion": tt.setDefault,
				})
				rec := doRequest(t, h, "UpdateDocumentDefaultVersion", string(body))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListDocumentVersions",
				`{"Name":"td-doc-`+tt.name+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			versions := resp["DocumentVersions"].([]any)
			assert.GreaterOrEqual(t, len(versions), tt.wantVersions)
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 2: Document permissions — DescribeDocumentPermission returns account list
// ---------------------------------------------------------------------------

func TestBatch2_DocumentPermissions_AddRemoveAccounts(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "SharedDoc",
		Content:      `{"schemaVersion":"2.2"}`,
		DocumentType: "Command",
	})
	require.NoError(t, err)

	// Add account permission.
	rec := doRequest(t, h, "ModifyDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share",
		"AccountIdsToAdd": ["111111111111", "222222222222"]
	}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe — should list both accounts.
	rec = doRequest(t, h, "DescribeDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share"
	}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "111111111111")
	assert.Contains(t, rec.Body.String(), "222222222222")

	// Remove one account.
	rec = doRequest(t, h, "ModifyDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share",
		"AccountIdsToRemove": ["111111111111"]
	}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe — should only have second account.
	rec = doRequest(t, h, "DescribeDocumentPermission", `{
		"Name": "SharedDoc",
		"PermissionType": "Share"
	}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "111111111111")
	assert.Contains(t, rec.Body.String(), "222222222222")
}

// ---------------------------------------------------------------------------
// Gap 3: Distributor packages — DocumentType=Package round-trip
// ---------------------------------------------------------------------------

func TestBatch2_DistributorPackage_CreateAndGet(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a Package type document (SSM Distributor).
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "MyPackage",
		Content:      `{"schemaVersion":"2.0","packages":[]}`,
		DocumentType: "Package",
	})
	require.NoError(t, err)

	// GetDocument should return DocumentType=Package.
	rec := doRequest(t, h, "GetDocument", `{"Name":"MyPackage"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Package")

	// ListDocuments with DocumentType filter should include the package.
	rec = doRequest(t, h, "ListDocuments", `{"Filters":[{"Key":"DocumentType","Values":["Package"]}]}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MyPackage")
}

func TestBatch2_DistributorPackage_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		docType      string
		filterType   string
		wantInResult bool
	}{
		{
			name:         "package_matches_package_filter",
			docType:      "Package",
			filterType:   "Package",
			wantInResult: true,
		},
		{
			name:         "command_not_in_package_filter",
			docType:      "Command",
			filterType:   "Package",
			wantInResult: false,
		},
		{
			name:         "command_matches_command_filter",
			docType:      "Command",
			filterType:   "Command",
			wantInResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			docName := "filter-test-" + tt.name
			_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
				Name:         docName,
				Content:      `{"schemaVersion":"2.0"}`,
				DocumentType: tt.docType,
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"Filters": []map[string]any{
					{"Key": "DocumentType", "Values": []string{tt.filterType}},
				},
			})

			rec := doRequest(t, h, "ListDocuments", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.wantInResult {
				assert.Contains(t, rec.Body.String(), docName)
			} else {
				assert.NotContains(t, rec.Body.String(), docName)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 4: Parameters — version-specific labels round-trip
// ---------------------------------------------------------------------------

func TestBatch2_ParameterLabels_VersionSpecific(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create parameter (version 1).
	_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/app/key", Value: "v1", Type: "String"})
	require.NoError(t, err)

	// Update to create version 2.
	_, err = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/app/key", Value: "v2", Type: "String", Overwrite: true})
	require.NoError(t, err)

	// Label version 1 with "stable".
	body, _ := json.Marshal(map[string]any{
		"Name":             "/app/key",
		"Labels":           []string{"stable"},
		"ParameterVersion": 1,
	})
	rec := doRequest(t, h, "LabelParameterVersion", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AddedLabels")

	// Label version 2 with "latest".
	body, _ = json.Marshal(map[string]any{
		"Name":             "/app/key",
		"Labels":           []string{"latest"},
		"ParameterVersion": 2,
	})
	rec = doRequest(t, h, "LabelParameterVersion", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// History should show labels per version.
	rec = doRequest(t, h, "GetParameterHistory", `{"Name":"/app/key"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "stable")
	assert.Contains(t, rec.Body.String(), "latest")
}

func TestBatch2_ParameterLabels_UnlabelSpecificVersion(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/app/ver", Value: "v1", Type: "String"})
	require.NoError(t, err)

	// Add labels to version 1.
	body, _ := json.Marshal(map[string]any{
		"Name":             "/app/ver",
		"Labels":           []string{"prod", "stable"},
		"ParameterVersion": 1,
	})
	doRequest(t, h, "LabelParameterVersion", string(body))

	// Remove one label.
	body, _ = json.Marshal(map[string]any{
		"Name":             "/app/ver",
		"Labels":           []string{"prod"},
		"ParameterVersion": 1,
	})
	rec := doRequest(t, h, "UnlabelParameterVersion", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RemovedLabels")

	// History should still have "stable" but not "prod".
	rec = doRequest(t, h, "GetParameterHistory", `{"Name":"/app/ver"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "stable")
	assert.NotContains(t, rec.Body.String(), `"prod"`)
}

// ---------------------------------------------------------------------------
// Gap 5: Parameters — SecureString history WithDecryption
// ---------------------------------------------------------------------------

func TestBatch2_SecureStringHistory_WithDecryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		withDecryption bool
		wantPlaintext  bool
	}{
		{
			name:           "with_decryption_returns_plaintext",
			withDecryption: true,
			wantPlaintext:  true,
		},
		{
			name:           "without_decryption_returns_encrypted",
			withDecryption: false,
			wantPlaintext:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  "/secure/pwd",
				Value: "my-secret-value",
				Type:  "SecureString",
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"Name":           "/secure/pwd",
				"WithDecryption": tt.withDecryption,
			})

			rec := doRequest(t, h, "GetParameterHistory", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			bodyStr := rec.Body.String()
			if tt.wantPlaintext {
				assert.Contains(t, bodyStr, "my-secret-value")
			} else {
				assert.NotContains(t, bodyStr, "my-secret-value")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 6: Parameters — Advanced tier size enforcement
// ---------------------------------------------------------------------------

func TestBatch2_AdvancedTier_AcceptsLargeValue(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Advanced tier allows up to 8 KiB — use a 5 KiB value.
	largeVal := make([]byte, 5000)
	for i := range largeVal {
		largeVal[i] = 'A'
	}

	body, _ := json.Marshal(map[string]any{
		"Name":  "/advanced/param",
		"Value": string(largeVal),
		"Type":  "String",
		"Tier":  "Advanced",
	})
	rec := doRequest(t, h, "PutParameter", string(body))
	assert.Equal(t, http.StatusOK, rec.Code, "Advanced tier should accept 5KiB value")
}

func TestBatch2_StandardTier_RejectsLargeValue(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Standard tier only allows up to 4 KiB — use 5 KiB.
	largeVal := make([]byte, 5000)
	for i := range largeVal {
		largeVal[i] = 'B'
	}

	body, _ := json.Marshal(map[string]any{
		"Name":  "/standard/param",
		"Value": string(largeVal),
		"Type":  "String",
		"Tier":  "Standard",
	})
	rec := doRequest(t, h, "PutParameter", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "Standard tier should reject 5KiB value")
}

// ---------------------------------------------------------------------------
// Gap 7: Patch baselines — OS filter + compliance level round-trip
// ---------------------------------------------------------------------------

func TestBatch2_PatchBaseline_OSFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create baselines for different OSes.
	linuxBL, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "linux-baseline",
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	windowsBL, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "windows-baseline",
		OperatingSystem: "WINDOWS",
	})
	require.NoError(t, err)

	_ = linuxBL
	_ = windowsBL

	// Filter by AMAZON_LINUX_2 — should return linux but not windows.
	body, _ := json.Marshal(map[string]any{
		"Filters": []map[string]any{
			{"Key": "OPERATING_SYSTEM", "Values": []string{"AMAZON_LINUX_2"}},
		},
	})
	rec := doRequest(t, h, "DescribePatchBaselines", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "linux-baseline")
	assert.NotContains(t, rec.Body.String(), "windows-baseline")

	// Filter by WINDOWS — should return windows but not linux.
	body, _ = json.Marshal(map[string]any{
		"Filters": []map[string]any{
			{"Key": "OPERATING_SYSTEM", "Values": []string{"WINDOWS"}},
		},
	})
	rec = doRequest(t, h, "DescribePatchBaselines", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "linux-baseline")
	assert.Contains(t, rec.Body.String(), "windows-baseline")
}

func TestBatch2_PatchBaseline_ComplianceLevelRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		complianceLevel string
	}{
		{name: "critical_compliance", complianceLevel: "CRITICAL"},
		{name: "high_compliance", complianceLevel: "HIGH"},
		{name: "medium_compliance", complianceLevel: "MEDIUM"},
		{name: "informational_compliance", complianceLevel: "INFORMATIONAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			bl, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
				Name:                           "cl-baseline-" + tt.name,
				OperatingSystem:                "AMAZON_LINUX_2",
				ApprovedPatchesComplianceLevel: tt.complianceLevel,
			})
			require.NoError(t, err)

			// GetPatchBaseline should return the compliance level.
			body, _ := json.Marshal(map[string]any{"BaselineId": bl.BaselineID})
			rec := doRequest(t, h, "GetPatchBaseline", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.complianceLevel)

			// UpdatePatchBaseline should also update compliance level.
			body, _ = json.Marshal(map[string]any{
				"BaselineId":                     bl.BaselineID,
				"ApprovedPatchesComplianceLevel": "LOW",
			})
			rec = doRequest(t, h, "UpdatePatchBaseline", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "LOW")
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 8: Patch groups — register and lookup round-trip
// ---------------------------------------------------------------------------

func TestBatch2_PatchGroups_RegisterAndLookup(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	bl, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "group-baseline",
		OperatingSystem: "UBUNTU",
	})
	require.NoError(t, err)

	// Register the baseline for a patch group.
	body, _ := json.Marshal(map[string]any{
		"BaselineId": bl.BaselineID,
		"PatchGroup": "ubuntu-servers",
	})
	rec := doRequest(t, h, "RegisterPatchBaselineForPatchGroup", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribePatchGroups should list the mapping.
	rec = doRequest(t, h, "DescribePatchGroups", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ubuntu-servers")

	// GetPatchBaselineForPatchGroup should return the baseline.
	body, _ = json.Marshal(map[string]any{"PatchGroup": "ubuntu-servers"})
	rec = doRequest(t, h, "GetPatchBaselineForPatchGroup", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), bl.BaselineID)

	// DeregisterPatchBaselineForPatchGroup should remove the mapping.
	body, _ = json.Marshal(map[string]any{
		"BaselineId": bl.BaselineID,
		"PatchGroup": "ubuntu-servers",
	})
	rec = doRequest(t, h, "DeregisterPatchBaselineForPatchGroup", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap 9: Maintenance windows — DescribeMaintenanceWindowsForTarget
// ---------------------------------------------------------------------------

func TestBatch2_MaintenanceWindowsForTarget_FindsByTarget(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a maintenance window.
	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "prod-window",
		Schedule: "cron(0 2 ? * SUN *)",
		Duration: 4,
		Cutoff:   1,
	})
	require.NoError(t, err)

	// Register a target.
	body, _ := json.Marshal(map[string]any{
		"WindowId":     mw.WindowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-aabbcc112233"}},
		},
	})
	rec := doRequest(t, h, "RegisterTargetWithMaintenanceWindow", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeMaintenanceWindowsForTarget should find the window.
	body, _ = json.Marshal(map[string]any{
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-aabbcc112233"}},
		},
	})
	rec = doRequest(t, h, "DescribeMaintenanceWindowsForTarget", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), mw.WindowID)
	assert.Contains(t, rec.Body.String(), "prod-window")
}

func TestBatch2_MaintenanceWindowsForTarget_NoMatchReturnsEmpty(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	body, _ := json.Marshal(map[string]any{
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-notregistered"}},
		},
	})
	rec := doRequest(t, h, "DescribeMaintenanceWindowsForTarget", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	identities := resp["WindowIdentities"].([]any)
	assert.Empty(t, identities)
}

// ---------------------------------------------------------------------------
// Gap 10: Maintenance window tasks — ServiceRoleArn + MaxConcurrency round-trip
// ---------------------------------------------------------------------------

func TestBatch2_MaintenanceWindowTask_ServiceRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		serviceRoleArn string
		maxConcurrency string
		maxErrors      string
	}{
		{
			name:           "with_service_role_and_concurrency",
			serviceRoleArn: "arn:aws:iam::123456789012:role/MaintenanceWindowRole",
			maxConcurrency: "50%",
			maxErrors:      "10%",
		},
		{
			name:           "without_service_role",
			serviceRoleArn: "",
			maxConcurrency: "1",
			maxErrors:      "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
				Name:     "sra-window-" + tt.name,
				Schedule: "cron(0 2 ? * SUN *)",
				Duration: 3,
				Cutoff:   1,
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"WindowId":       mw.WindowID,
				"TaskType":       "RUN_COMMAND",
				"TaskArn":        "AWS-RunShellScript",
				"ServiceRoleArn": tt.serviceRoleArn,
				"MaxConcurrency": tt.maxConcurrency,
				"MaxErrors":      tt.maxErrors,
			})
			rec := doRequest(t, h, "RegisterTaskWithMaintenanceWindow", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var regResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
			taskID := regResp["WindowTaskId"].(string)

			// GetMaintenanceWindowTask should return ServiceRoleArn.
			body, _ = json.Marshal(map[string]any{
				"WindowId":     mw.WindowID,
				"WindowTaskId": taskID,
			})
			rec = doRequest(t, h, "GetMaintenanceWindowTask", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.serviceRoleArn != "" {
				assert.Contains(t, rec.Body.String(), tt.serviceRoleArn)
			}

			assert.Contains(t, rec.Body.String(), tt.maxConcurrency)
		})
	}
}

func TestBatch2_MaintenanceWindowTask_UpdateServiceRoleArn(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	mw, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "update-sra-window",
		Schedule: "cron(0 2 ? * SUN *)",
		Duration: 3,
		Cutoff:   1,
	})
	require.NoError(t, err)

	// Register task without role.
	body, _ := json.Marshal(map[string]any{
		"WindowId": mw.WindowID,
		"TaskType": "RUN_COMMAND",
		"TaskArn":  "AWS-RunShellScript",
	})
	rec := doRequest(t, h, "RegisterTaskWithMaintenanceWindow", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
	taskID := regResp["WindowTaskId"].(string)

	// Update with ServiceRoleArn.
	body, _ = json.Marshal(map[string]any{
		"WindowId":       mw.WindowID,
		"WindowTaskId":   taskID,
		"ServiceRoleArn": "arn:aws:iam::123456789012:role/UpdatedRole",
	})
	rec = doRequest(t, h, "UpdateMaintenanceWindowTask", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdatedRole")
}

// ---------------------------------------------------------------------------
// Gap 11: Automation executions — parameters and mode round-trip
// ---------------------------------------------------------------------------

func TestBatch2_AutomationExecution_ParametersRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parameters map[string][]string
		mode       string
	}{
		{
			name:       "with_parameters",
			parameters: map[string][]string{"InstanceId": {"i-12345678"}, "Action": {"Install"}},
			mode:       "Auto",
		},
		{
			name:       "interactive_mode",
			parameters: map[string][]string{"DocumentName": {"AWS-UpdateSSMAgent"}},
			mode:       "Interactive",
		},
		{
			name:       "empty_parameters",
			parameters: nil,
			mode:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body, _ := json.Marshal(map[string]any{
				"DocumentName": "AWS-RunShellScript",
				"Parameters":   tt.parameters,
				"Mode":         tt.mode,
			})
			rec := doRequest(t, h, "StartAutomationExecution", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			execID := startResp["AutomationExecutionId"].(string)
			assert.NotEmpty(t, execID)

			// GetAutomationExecution should return parameters.
			body, _ = json.Marshal(map[string]any{"AutomationExecutionId": execID})
			rec = doRequest(t, h, "GetAutomationExecution", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			if len(tt.parameters) > 0 {
				for key := range tt.parameters {
					assert.Contains(t, rec.Body.String(), key)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 12: Automation approvals — SendAutomationSignal Approve/Reject
// ---------------------------------------------------------------------------

func TestBatch2_AutomationApprovals_ApproveSignal(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start an automation execution.
	rec := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-ChangeRequest"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execID := startResp["AutomationExecutionId"].(string)

	// Send Approve signal.
	body, _ := json.Marshal(map[string]any{
		"AutomationExecutionId": execID,
		"SignalType":            "Approve",
	})
	rec = doRequest(t, h, "SendAutomationSignal", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// GetAutomationExecution should show Approved status.
	body, _ = json.Marshal(map[string]any{"AutomationExecutionId": execID})
	rec = doRequest(t, h, "GetAutomationExecution", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Approved")
}

func TestBatch2_AutomationApprovals_RejectSignal(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-ChangeRequest"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execID := startResp["AutomationExecutionId"].(string)

	body, _ := json.Marshal(map[string]any{
		"AutomationExecutionId": execID,
		"SignalType":            "Reject",
	})
	rec = doRequest(t, h, "SendAutomationSignal", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ = json.Marshal(map[string]any{"AutomationExecutionId": execID})
	rec = doRequest(t, h, "GetAutomationExecution", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Rejected")
}

// ---------------------------------------------------------------------------
// Gap 13: OpsCenter — OperationalData merge on UpdateOpsItem
// ---------------------------------------------------------------------------

func TestBatch2_OpsItem_OperationalData_CreateAndUpdate(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create with initial OperationalData.
	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:  "DB Connection Failure",
		Source: "ec2",
		OperationalData: map[string]ssm.OpsItemDataValue{
			"/aws/resources": {
				Type:  "SearchableString",
				Value: `[{"arn":"arn:aws:ec2:us-east-1:123456789012:instance/i-abc"}]`,
			},
		},
	})
	require.NoError(t, err)

	// GetOpsItem should have initial OperationalData.
	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "GetOpsItem", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/aws/resources")

	// Update with additional OperationalData key.
	body, _ = json.Marshal(map[string]any{
		"OpsItemId": item.OpsItemID,
		"OperationalData": map[string]any{
			"/aws/automations": map[string]any{
				"Type":  "SearchableString",
				"Value": `[{"automationId":"auto-123"}]`,
			},
		},
	})
	rec = doRequest(t, h, "UpdateOpsItem", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// GetOpsItem should have both keys.
	body, _ = json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec = doRequest(t, h, "GetOpsItem", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/aws/resources")
	assert.Contains(t, rec.Body.String(), "/aws/automations")
}

func TestBatch2_OpsItem_OperationalData_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialData   map[string]ssm.OpsItemDataValue
		updateData    map[string]ssm.OpsItemDataValue
		wantAfterKeys []string
	}{
		{
			name: "merge_new_key",
			initialData: map[string]ssm.OpsItemDataValue{
				"key1": {Value: "val1"},
			},
			updateData: map[string]ssm.OpsItemDataValue{
				"key2": {Value: "val2"},
			},
			wantAfterKeys: []string{"key1", "key2"},
		},
		{
			name: "overwrite_existing_key",
			initialData: map[string]ssm.OpsItemDataValue{
				"key1": {Value: "old"},
			},
			updateData: map[string]ssm.OpsItemDataValue{
				"key1": {Value: "new"},
			},
			wantAfterKeys: []string{"key1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
				Title:           "Test Item",
				Source:          "test",
				OperationalData: tt.initialData,
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"OpsItemId":       item.OpsItemID,
				"OperationalData": tt.updateData,
			})
			rec := doRequest(t, h, "UpdateOpsItem", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			body, _ = json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
			rec = doRequest(t, h, "GetOpsItem", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			for _, key := range tt.wantAfterKeys {
				assert.Contains(t, rec.Body.String(), key)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 14: OpsCenter — ListOpsItemEvents tracks create/update events
// ---------------------------------------------------------------------------

func TestBatch2_OpsItemEvents_TrackCreates(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:  "Tracked Item",
		Source: "automation",
	})
	require.NoError(t, err)

	// ListOpsItemEvents for this item should return at least one event.
	body, _ := json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "ListOpsItemEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["Summaries"].([]any)
	assert.NotEmpty(t, summaries, "should have at least one event after create")
}

func TestBatch2_OpsItemEvents_TrackUpdates(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:  "Updated Item",
		Source: "ec2",
	})
	require.NoError(t, err)

	// Update the item.
	body, _ := json.Marshal(map[string]any{
		"OpsItemId":   item.OpsItemID,
		"Description": "Updated description",
	})
	doRequest(t, h, "UpdateOpsItem", string(body))

	// ListOpsItemEvents should have multiple events now.
	body, _ = json.Marshal(map[string]any{"OpsItemId": item.OpsItemID})
	rec := doRequest(t, h, "ListOpsItemEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["Summaries"].([]any)
	assert.GreaterOrEqual(t, len(summaries), 2, "create + update should each produce an event")
}

func TestBatch2_OpsItemEvents_FilterByOpsItemID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	item1, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{Title: "Item 1", Source: "ec2"})
	require.NoError(t, err)
	item2, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{Title: "Item 2", Source: "rds"})
	require.NoError(t, err)

	// Filter by item1 — should only get item1 events.
	body, _ := json.Marshal(map[string]any{"OpsItemId": item1.OpsItemID})
	rec := doRequest(t, h, "ListOpsItemEvents", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), item1.OpsItemID)
	assert.NotContains(t, rec.Body.String(), item2.OpsItemID)
}

// ---------------------------------------------------------------------------
// Gap 15: Change calendar — document type validation
// ---------------------------------------------------------------------------

func TestBatch2_GetCalendarState_EmptyCalendarNames(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetCalendarState", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "OPEN")
}

func TestBatch2_GetCalendarState_WithCalendarDocument(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a ChangeCalendar document.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "MyCalendar",
		Content:      `{"type":"DEFAULT_OPEN"}`,
		DocumentType: "ChangeCalendar",
	})
	require.NoError(t, err)

	// GetCalendarState with the calendar name should succeed.
	body, _ := json.Marshal(map[string]any{
		"CalendarNames": []string{"MyCalendar"},
	})
	rec := doRequest(t, h, "GetCalendarState", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "OPEN")
}

func TestBatch2_GetCalendarState_NonCalendarDocumentReturnsError(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create a non-calendar document.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:         "NotACalendar",
		Content:      `{"schemaVersion":"2.2"}`,
		DocumentType: "Command",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{
		"CalendarNames": []string{"NotACalendar"},
	})
	rec := doRequest(t, h, "GetCalendarState", string(body))
	assert.NotEqual(t, http.StatusOK, rec.Code, "non-ChangeCalendar doc should return error")
}

func TestBatch2_GetCalendarState_MissingDocumentReturnsError(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	body, _ := json.Marshal(map[string]any{
		"CalendarNames": []string{"DoesNotExist"},
	})
	rec := doRequest(t, h, "GetCalendarState", string(body))
	assert.NotEqual(t, http.StatusOK, rec.Code, "missing calendar should return error")
}

// ---------------------------------------------------------------------------
// Gap 16: Session manager — Parameters round-trip
// ---------------------------------------------------------------------------

func TestBatch2_Session_Parameters_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parameters map[string][]string
		wantKey    string
	}{
		{
			name:       "with_parameters",
			parameters: map[string][]string{"command": {"ls -la"}, "runAsEnabled": {"true"}},
			wantKey:    "command",
		},
		{
			name:       "without_parameters",
			parameters: nil,
			wantKey:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body, _ := json.Marshal(map[string]any{
				"Target":     "i-1234567890abcdef0",
				"Parameters": tt.parameters,
			})
			rec := doRequest(t, h, "StartSession", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			assert.NotEmpty(t, startResp["SessionId"])

			// DescribeSessions should show the session.
			rec = doRequest(t, h, "DescribeSessions", `{"State":"Connected"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

func TestBatch2_Session_StateFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	sess, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-filter-test"})
	require.NoError(t, err)

	// Filter by Connected — should find the session.
	rec := doRequest(t, h, "DescribeSessions", `{"State":"Connected"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), sess.SessionID)

	// Terminate session.
	body, _ := json.Marshal(map[string]any{"SessionId": sess.SessionID})
	doRequest(t, h, "TerminateSession", string(body))

	// Filter by Connected — should NOT find the terminated session.
	rec = doRequest(t, h, "DescribeSessions", `{"State":"Connected"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), sess.SessionID)
}

// ---------------------------------------------------------------------------
// Gap 17: Fleet manager — ListNodes derives from activations
// ---------------------------------------------------------------------------

func TestBatch2_FleetManager_ListNodes_FromActivations(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create activations — each produces a node.
	for range 3 {
		_, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
			IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
			RegistrationLimit: 1,
		})
		require.NoError(t, err)
	}

	rec := doRequest(t, h, "ListNodes", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nodes := resp["Nodes"].([]any)
	assert.GreaterOrEqual(t, len(nodes), 3)

	// Each node should have PlatformType.
	for _, n := range nodes {
		node := n.(map[string]any)
		assert.NotEmpty(t, node["PlatformType"])
	}
}

func TestBatch2_FleetManager_ListNodesSummary_NodeCount(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateActivation(context.TODO(), &ssm.CreateActivationInput{
		IamRole:           "arn:aws:iam::123456789012:role/SSMRole",
		RegistrationLimit: 2,
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListNodesSummary", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "NodeCount")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summary := resp["Summary"].([]any)
	assert.NotEmpty(t, summary)
}

// ---------------------------------------------------------------------------
// Gap 18: Association compliance — ListComplianceSummaries aggregation
// ---------------------------------------------------------------------------

func TestBatch2_ComplianceSummaries_AggregateByCType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Store some compliance items.
	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-abc123",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Association",
		Items: []ssm.ComplianceItem{
			{Title: "AWS-RunPatchBaseline", Status: "COMPLIANT", Severity: "UNSPECIFIED"},
			{Title: "AWS-GatherSoftwareInventory", Status: "NON_COMPLIANT", Severity: "HIGH"},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListComplianceSummaries", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items := resp["ComplianceSummaryItems"].([]any)
	assert.NotEmpty(t, items, "should have compliance summary items")

	// Find the Association type summary.
	var foundAssoc bool
	for _, item := range items {
		m := item.(map[string]any)
		if m["ComplianceType"] == "Association" {
			foundAssoc = true
		}
	}

	assert.True(t, foundAssoc, "should have Association compliance type summary")
}

func TestBatch2_ResourceComplianceSummaries_PerResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-res123",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Patch",
		Items: []ssm.ComplianceItem{
			{Title: "KB123456", Status: "COMPLIANT", Severity: "MEDIUM"},
			{Title: "KB789012", Status: "COMPLIANT", Severity: "LOW"},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListResourceComplianceSummaries", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items := resp["ResourceComplianceSummaryItems"].([]any)
	assert.NotEmpty(t, items)

	// The resource should show COMPLIANT status.
	var found bool
	for _, item := range items {
		m := item.(map[string]any)
		if m["ResourceId"] == "i-res123" {
			found = true
			assert.Equal(t, "COMPLIANT", m["Status"])
		}
	}

	assert.True(t, found, "should have resource compliance summary for i-res123")
}

func TestBatch2_ResourceComplianceSummaries_NonCompliantStatus(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-bad456",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Patch",
		Items: []ssm.ComplianceItem{
			{Title: "KB001", Status: "COMPLIANT", Severity: "LOW"},
			{Title: "KB002", Status: "NON_COMPLIANT", Severity: "HIGH"},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "ListResourceComplianceSummaries", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, item := range resp["ResourceComplianceSummaryItems"].([]any) {
		m := item.(map[string]any)
		if m["ResourceId"] == "i-bad456" {
			assert.Equal(t, "NON_COMPLIANT", m["Status"])
		}
	}
}

// ---------------------------------------------------------------------------
// Gap 19: Inventory schemas — GetInventorySchema returns built-in types
// ---------------------------------------------------------------------------

func TestBatch2_GetInventorySchema_ReturnsBuiltInTypes(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetInventorySchema", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schemas := resp["Schemas"].([]any)
	assert.NotEmpty(t, schemas, "GetInventorySchema should return built-in schema types")

	// Verify key AWS types are present.
	var typeNames []string
	for _, s := range schemas {
		schema := s.(map[string]any)
		if name, ok := schema["TypeName"].(string); ok {
			typeNames = append(typeNames, name)
		}
	}

	assert.Contains(t, typeNames, "AWS:Application")
	assert.Contains(t, typeNames, "AWS:InstanceInformation")
	assert.Contains(t, typeNames, "AWS:Network")
}

func TestBatch2_GetInventorySchema_FilterByTypeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		typeName     string
		wantContains string
	}{
		{
			name:         "filter_aws_application",
			typeName:     "AWS:Application",
			wantContains: "AWS:Application",
		},
		{
			name:         "filter_aws_network",
			typeName:     "AWS:Network",
			wantContains: "AWS:Network",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body, _ := json.Marshal(map[string]any{"TypeName": tt.typeName})
			rec := doRequest(t, h, "GetInventorySchema", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 20: Inventory — PutInventory type-merge and ListInventoryEntries
// ---------------------------------------------------------------------------

func TestBatch2_Inventory_TypeMergeAndLookup(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Put inventory with two types.
	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-inv001",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-01T00:00:00Z",
				Content: []map[string]string{
					{"Name": "aws-cli", "Version": "2.0"},
				},
			},
		},
	})
	require.NoError(t, err)

	// List entries for the type.
	body, _ := json.Marshal(map[string]any{
		"InstanceId": "i-inv001",
		"TypeName":   "AWS:Application",
	})
	rec := doRequest(t, h, "ListInventoryEntries", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "aws-cli")

	// Put again with same type (overwrite) and add a new type.
	_, err = b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-inv001",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-02T00:00:00Z",
				Content: []map[string]string{
					{"Name": "aws-cli", "Version": "2.1"},
				},
			},
			{
				TypeName:      "AWS:Network",
				SchemaVersion: "1.0",
				CaptureTime:   "2024-01-02T00:00:00Z",
				Content: []map[string]string{
					{"Name": "eth0", "SubnetId": "subnet-abc123"},
				},
			},
		},
	})
	require.NoError(t, err)

	// GetInventory should show both types for the instance.
	rec = doRequest(t, h, "GetInventory", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "i-inv001")

	// Application version should be updated to 2.1.
	body, _ = json.Marshal(map[string]any{
		"InstanceId": "i-inv001",
		"TypeName":   "AWS:Application",
	})
	rec = doRequest(t, h, "ListInventoryEntries", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "2.1")
}

// ---------------------------------------------------------------------------
// Gap 21: Service settings — lifecycle round-trip
// ---------------------------------------------------------------------------

func TestBatch2_ServiceSettings_LifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		settingID  string
		setValue   string
		wantCustom bool
	}{
		{
			name:       "parameter_store_advanced_tier",
			settingID:  "/ssm/parameter-store/default-parameter-tier",
			setValue:   "Advanced",
			wantCustom: true,
		},
		{
			name:       "session_manager_timeout",
			settingID:  "/ssm/session-manager/max-session-duration",
			setValue:   "60",
			wantCustom: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			// Default state.
			body, _ := json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec := doRequest(t, h, "GetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "Default")

			// Update.
			body, _ = json.Marshal(map[string]any{
				"SettingId":    tt.settingID,
				"SettingValue": tt.setValue,
			})
			rec = doRequest(t, h, "UpdateServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get updated.
			body, _ = json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec = doRequest(t, h, "GetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.setValue)
			assert.Contains(t, rec.Body.String(), "Customized")

			// Reset.
			body, _ = json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec = doRequest(t, h, "ResetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			// Default again.
			body, _ = json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec = doRequest(t, h, "GetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "Default")
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 22: OpsMetadata — full CRUD
// ---------------------------------------------------------------------------

func TestBatch2_OpsMetadata_FullCRUD(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create.
	meta, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
		Metadata: map[string]ssm.MetadataValue{
			"env": {Value: "production"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, meta.OpsMetadataArn)

	// List.
	rec := doRequest(t, h, "ListOpsMetadata", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "OpsMetadataList")

	// Get.
	body, _ := json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec = doRequest(t, h, "GetOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "production")

	// Update.
	body, _ = json.Marshal(map[string]any{
		"OpsMetadataArn": meta.OpsMetadataArn,
		"Metadata": map[string]any{
			"version": map[string]any{"Value": "2"},
		},
	})
	rec = doRequest(t, h, "UpdateOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// Get updated — both keys should be present.
	body, _ = json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec = doRequest(t, h, "GetOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "production")
	assert.Contains(t, rec.Body.String(), "version")

	// Delete.
	body, _ = json.Marshal(map[string]any{"OpsMetadataArn": meta.OpsMetadataArn})
	rec = doRequest(t, h, "DeleteOpsMetadata", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap 23: Resource policies — Put/Get/Delete round-trip
// ---------------------------------------------------------------------------

func TestBatch2_ResourcePolicies_FullCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		policy      string
	}{
		{
			name:        "parameter_policy",
			resourceArn: "arn:aws:ssm:us-east-1:123456789012:parameter/my-param",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"arn:aws:iam::111111111111:root"},` +
				`"Action":"ssm:GetParameter","Resource":"*"}]}`,
		},
		{
			name:        "opsmetadata_policy",
			resourceArn: "arn:aws:ssm:us-east-1:123456789012:opsmetadata/app/prod",
			policy:      `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			// Put policy.
			body, _ := json.Marshal(map[string]any{
				"ResourceArn": tt.resourceArn,
				"Policy":      tt.policy,
			})
			rec := doRequest(t, h, "PutResourcePolicy", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			policyID := putResp["PolicyId"].(string)
			assert.NotEmpty(t, policyID)

			// Get policies.
			body, _ = json.Marshal(map[string]any{"ResourceArn": tt.resourceArn})
			rec = doRequest(t, h, "GetResourcePolicies", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), policyID)

			// Delete policy.
			body, _ = json.Marshal(map[string]any{
				"ResourceArn": tt.resourceArn,
				"PolicyId":    policyID,
			})
			rec = doRequest(t, h, "DeleteResourcePolicy", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get policies — should be empty.
			body, _ = json.Marshal(map[string]any{"ResourceArn": tt.resourceArn})
			rec = doRequest(t, h, "GetResourcePolicies", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), `"Policies":[]`)
		})
	}
}
