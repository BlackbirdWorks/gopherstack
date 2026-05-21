package ssm_test

// handler_audit1_full_test.go — comprehensive lifecycle tests for audit batch-1.
// Covers: Parameter Store full lifecycle, Document CRUD+permissions,
// MaintenanceWindow+tasks+targets, PatchBaseline+groups, OpsItem, Run Command.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newFullHandler() *ssm.Handler {
	return ssm.NewHandler(ssm.NewInMemoryBackend())
}

func mustPost(t *testing.T, h *ssm.Handler, op string, body any) (int, map[string]any) {
	t.Helper()

	b, err := json.Marshal(body)
	require.NoError(t, err)

	rec := doRequest(t, h, op, string(b))

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}

// ---------------------------------------------------------------------------
// Parameter Store full lifecycle
// ---------------------------------------------------------------------------

func TestFull_ParameterStore_PutGetDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Put
	code, out := mustPost(t, h, "PutParameter", map[string]any{
		"Name":  "/full/p1",
		"Type":  "String",
		"Value": "v1",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.InEpsilon(t, float64(1), out["Version"], 0.01)

	// Get
	code, out = mustPost(t, h, "GetParameter", map[string]any{"Name": "/full/p1"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "v1", out["Parameter"].(map[string]any)["Value"])

	// Overwrite
	code, out = mustPost(t, h, "PutParameter", map[string]any{
		"Name":      "/full/p1",
		"Type":      "String",
		"Value":     "v2",
		"Overwrite": true,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.InEpsilon(t, float64(2), out["Version"], 0.01)

	// Delete
	code, _ = mustPost(t, h, "DeleteParameter", map[string]any{"Name": "/full/p1"})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetParameter", map[string]any{"Name": "/full/p1"})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_ParameterStore_SecureString_EncryptDecrypt(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	_, err := mustPost(t, h, "PutParameter", map[string]any{
		"Name":  "/sec/p",
		"Type":  "SecureString",
		"Value": "mysecret",
		"KeyId": "alias/test",
	})
	require.NotNil(t, err)

	// Get without decrypt → encrypted value
	code, out := mustPost(t, h, "GetParameter", map[string]any{
		"Name":           "/sec/p",
		"WithDecryption": false,
	})
	assert.Equal(t, http.StatusOK, code)
	param := out["Parameter"].(map[string]any)
	assert.NotEqual(t, "mysecret", param["Value"])
	assert.Equal(t, "alias/test", param["KeyId"])

	// Get with decrypt → plaintext
	code, out = mustPost(t, h, "GetParameter", map[string]any{
		"Name":           "/sec/p",
		"WithDecryption": true,
	})
	assert.Equal(t, http.StatusOK, code)
	param = out["Parameter"].(map[string]any)
	assert.Equal(t, "mysecret", param["Value"])
}

func TestFull_ParameterStore_GetParameters_Batch(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for _, name := range []string{"/b/1", "/b/2", "/b/3"} {
		mustPost(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := mustPost(t, h, "GetParameters", map[string]any{
		"Names": []string{"/b/1", "/b/2", "/b/missing"},
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	invalid := out["InvalidParameters"].([]any)
	assert.Len(t, params, 2)
	assert.Len(t, invalid, 1)
	assert.Equal(t, "/b/missing", invalid[0])
}

func TestFull_ParameterStore_DeleteParameters_Batch(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for _, name := range []string{"/del/1", "/del/2"} {
		mustPost(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := mustPost(t, h, "DeleteParameters", map[string]any{
		"Names": []string{"/del/1", "/del/missing"},
	})

	assert.Equal(t, http.StatusOK, code)
	deleted := out["DeletedParameters"].([]any)
	invalid := out["InvalidParameters"].([]any)
	assert.Len(t, deleted, 1)
	assert.Len(t, invalid, 1)
}

func TestFull_ParameterStore_History_Versions(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for i := range 3 {
		mustPost(t, h, "PutParameter", map[string]any{
			"Name":      "/hist/v",
			"Type":      "String",
			"Value":     "version",
			"Overwrite": i > 0,
		})
	}

	code, out := mustPost(t, h, "GetParameterHistory", map[string]any{"Name": "/hist/v"})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 3)
}

func TestFull_ParameterStore_History_Pagination(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for i := range 5 {
		mustPost(t, h, "PutParameter", map[string]any{
			"Name":      "/paginated/p",
			"Type":      "String",
			"Value":     "v",
			"Overwrite": i > 0,
		})
	}

	maxR := int64(2)
	code, out := mustPost(t, h, "GetParameterHistory", map[string]any{
		"Name":       "/paginated/p",
		"MaxResults": maxR,
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 2)
	assert.NotEmpty(t, out["NextToken"])
}

func TestFull_ParameterStore_DescribeParameters_Pagination(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for i := range 5 {
		name := "/pg/" + string(rune('a'+i))
		mustPost(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	maxR := int64(2)
	code, out := mustPost(t, h, "DescribeParameters", map[string]any{"MaxResults": &maxR})
	assert.Equal(t, http.StatusOK, code)
	assert.Len(t, out["Parameters"].([]any), 2)
	nextToken := out["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Next page
	code, out = mustPost(t, h, "DescribeParameters", map[string]any{
		"MaxResults": &maxR,
		"NextToken":  nextToken,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.Len(t, out["Parameters"].([]any), 2)
}

func TestFull_ParameterStore_LabelParameterVersion(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{"Name": "/lbl/p", "Type": "String", "Value": "v1"})

	code, out := mustPost(t, h, "LabelParameterVersion", map[string]any{
		"Name":             "/lbl/p",
		"ParameterVersion": int64(1),
		"Labels":           []string{"prod", "stable"},
	})
	assert.Equal(t, http.StatusOK, code)
	added := out["AddedLabels"].([]any)
	assert.Len(t, added, 2)
}

func TestFull_ParameterStore_UnlabelParameterVersion(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{"Name": "/unlbl/p", "Type": "String", "Value": "v"})
	mustPost(t, h, "LabelParameterVersion", map[string]any{
		"Name":             "/unlbl/p",
		"ParameterVersion": int64(1),
		"Labels":           []string{"prod", "stable"},
	})

	code, out := mustPost(t, h, "UnlabelParameterVersion", map[string]any{
		"Name":             "/unlbl/p",
		"ParameterVersion": int64(1),
		"Labels":           []string{"prod"},
	})
	assert.Equal(t, http.StatusOK, code)
	removed := out["RemovedLabels"].([]any)
	assert.Len(t, removed, 1)
	assert.Equal(t, "prod", removed[0])
}

func TestFull_ParameterStore_Tags(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{"Name": "/tag/p", "Type": "String", "Value": "v"})

	code, _ := mustPost(t, h, "AddTagsToResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
		"Tags":         []map[string]any{{"Key": "env", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := mustPost(t, h, "ListTagsForResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
	})
	assert.Equal(t, http.StatusOK, code)
	tags := out["TagList"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])

	code, _ = mustPost(t, h, "RemoveTagsFromResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
		"TagKeys":      []string{"env"},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out = mustPost(t, h, "ListTagsForResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
	})
	assert.Equal(t, http.StatusOK, code)
	remaining, _ := out["TagList"].([]any)
	assert.Empty(t, remaining)
}

func TestFull_ParameterStore_GetParametersByPath_WithDecryption(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{
		"Name":  "/enc/key",
		"Type":  "SecureString",
		"Value": "secretval",
	})

	code, out := mustPost(t, h, "GetParametersByPath", map[string]any{
		"Path":           "/enc",
		"WithDecryption": true,
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	require.Len(t, params, 1)
	assert.Equal(t, "secretval", params[0].(map[string]any)["Value"])
}

// ---------------------------------------------------------------------------
// Document CRUD + permissions
// ---------------------------------------------------------------------------

func TestFull_Document_CreateGetUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateDocument", map[string]any{
		"Name":         "TestDoc",
		"Content":      `{"schemaVersion":"2.2","description":"test"}`,
		"DocumentType": "Command",
	})
	assert.Equal(t, http.StatusOK, code)
	doc := out["DocumentDescription"].(map[string]any)
	assert.Equal(t, "TestDoc", doc["Name"])
	assert.Equal(t, "Active", doc["Status"])

	// Get
	code, out = mustPost(t, h, "GetDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["Content"])

	// Describe
	code, out = mustPost(t, h, "DescribeDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestDoc", out["Document"].(map[string]any)["Name"])

	// List
	code, out = mustPost(t, h, "ListDocuments", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["DocumentIdentifiers"])

	// Update → new version
	code, _ = mustPost(t, h, "UpdateDocument", map[string]any{
		"Name":            "TestDoc",
		"Content":         `{"schemaVersion":"2.2","description":"v2"}`,
		"DocumentVersion": "$LATEST",
	})
	assert.Equal(t, http.StatusOK, code)

	// ListDocumentVersions
	code, out = mustPost(t, h, "ListDocumentVersions", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)
	versions := out["DocumentVersions"].([]any)
	assert.GreaterOrEqual(t, len(versions), 2)

	// Delete
	code, _ = mustPost(t, h, "DeleteDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetDocument", map[string]any{"Name": "TestDoc"})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_Document_DuplicateCreate(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "DupDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, _ := mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "DupDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_Document_Permissions(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "SharedDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	// Describe permissions
	code, out := mustPost(t, h, "DescribeDocumentPermission", map[string]any{
		"Name":           "SharedDoc",
		"PermissionType": "Share",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["AccountIds"])

	// Modify permissions
	code, _ = mustPost(t, h, "ModifyDocumentPermission", map[string]any{
		"Name":            "SharedDoc",
		"PermissionType":  "Share",
		"AccountIdsToAdd": []string{"123456789012"},
	})
	assert.Equal(t, http.StatusOK, code)

	// Describe after modification
	code, out = mustPost(t, h, "DescribeDocumentPermission", map[string]any{
		"Name":           "SharedDoc",
		"PermissionType": "Share",
	})
	assert.Equal(t, http.StatusOK, code)
	ids := out["AccountIds"].([]any)
	assert.Contains(t, ids, "123456789012")
}

func TestFull_Document_UpdateDefaultVersion(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "VersionDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})
	mustPost(t, h, "UpdateDocument", map[string]any{
		"Name":            "VersionDoc",
		"Content":         `{"schemaVersion":"2.2","v":2}`,
		"DocumentVersion": "$LATEST",
	})

	code, _ := mustPost(t, h, "UpdateDocumentDefaultVersion", map[string]any{
		"Name":            "VersionDoc",
		"DocumentVersion": "2",
	})
	assert.Equal(t, http.StatusOK, code)
}

// ---------------------------------------------------------------------------
// Run Command lifecycle
// ---------------------------------------------------------------------------

func TestFull_Command_SendListGetCancel(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Need a document
	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "RunMe",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	// SendCommand
	code, out := mustPost(t, h, "SendCommand", map[string]any{
		"DocumentName": "RunMe",
		"InstanceIds":  []string{"i-001", "i-002"},
		"Comment":      "integration test",
		"Parameters":   map[string]any{"commands": []string{"echo hello"}},
	})
	assert.Equal(t, http.StatusOK, code)
	cmd := out["Command"].(map[string]any)
	cmdID := cmd["CommandId"].(string)
	assert.NotEmpty(t, cmdID)
	assert.Equal(t, "RunMe", cmd["DocumentName"])
	assert.Equal(t, "integration test", cmd["Comment"])

	// ListCommands
	code, out = mustPost(t, h, "ListCommands", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	commands := out["Commands"].([]any)
	assert.NotEmpty(t, commands)

	// ListCommands filter by CommandId
	code, out = mustPost(t, h, "ListCommands", map[string]any{"CommandId": cmdID})
	assert.Equal(t, http.StatusOK, code)
	commands = out["Commands"].([]any)
	assert.Len(t, commands, 1)

	// GetCommandInvocation
	code, out = mustPost(t, h, "GetCommandInvocation", map[string]any{
		"CommandId":  cmdID,
		"InstanceId": "i-001",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, cmdID, out["CommandId"])
	assert.Equal(t, "integration test", out["Comment"])

	// ListCommandInvocations
	code, out = mustPost(t, h, "ListCommandInvocations", map[string]any{"CommandId": cmdID})
	assert.Equal(t, http.StatusOK, code)
	invocations := out["CommandInvocations"].([]any)
	assert.Len(t, invocations, 2)

	// CancelCommand
	code, _ = mustPost(t, h, "CancelCommand", map[string]any{"CommandId": cmdID})
	assert.Equal(t, http.StatusOK, code)

	// After cancel - command status should be Cancelled
	code, out = mustPost(t, h, "ListCommands", map[string]any{"CommandId": cmdID})
	assert.Equal(t, http.StatusOK, code)
	cancelledCmd := out["Commands"].([]any)[0].(map[string]any)
	assert.Equal(t, "Cancelled", cancelledCmd["Status"])
}

func TestFull_Command_MissingDocument(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	code, _ := mustPost(t, h, "SendCommand", map[string]any{
		"DocumentName": "NonExistentDoc",
		"InstanceIds":  []string{"i-001"},
	})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_Command_ListByInstanceId(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "DocForList",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	_, out1 := mustPost(t, h, "SendCommand", map[string]any{
		"DocumentName": "DocForList",
		"InstanceIds":  []string{"i-target", "i-other"},
	})
	cmdID := out1["Command"].(map[string]any)["CommandId"].(string)

	code, out := mustPost(t, h, "ListCommandInvocations", map[string]any{
		"CommandId":  cmdID,
		"InstanceId": "i-target",
	})
	assert.Equal(t, http.StatusOK, code)
	invocations := out["CommandInvocations"].([]any)
	assert.Len(t, invocations, 1)
	assert.Equal(t, "i-target", invocations[0].(map[string]any)["InstanceId"])
}

func TestFull_Command_S3Output(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "S3Doc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, out := mustPost(t, h, "SendCommand", map[string]any{
		"DocumentName":       "S3Doc",
		"InstanceIds":        []string{"i-s3"},
		"OutputS3BucketName": "results-bucket",
		"OutputS3KeyPrefix":  "cmd/output/",
		"TimeoutSeconds":     300,
	})
	assert.Equal(t, http.StatusOK, code)
	cmd := out["Command"].(map[string]any)
	assert.Equal(t, "results-bucket", cmd["OutputS3BucketName"])
	assert.Equal(t, "cmd/output/", cmd["OutputS3KeyPrefix"])
}

// ---------------------------------------------------------------------------
// MaintenanceWindow full lifecycle
// ---------------------------------------------------------------------------

func TestFull_MaintenanceWindow_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateMaintenanceWindow", map[string]any{
		"Name":              "TestMW",
		"Schedule":          "cron(0 2 ? * SUN *)",
		"Duration":          4,
		"Cutoff":            1,
		"AllowUnassociated": false,
	})
	assert.Equal(t, http.StatusOK, code)
	windowID := out["WindowId"].(string)
	assert.NotEmpty(t, windowID)

	// Get
	code, out = mustPost(t, h, "GetMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestMW", out["Name"])

	// Update
	code, _ = mustPost(t, h, "UpdateMaintenanceWindow", map[string]any{
		"WindowId": windowID,
		"Name":     "UpdatedMW",
	})
	assert.Equal(t, http.StatusOK, code)

	// Describe
	code, out = mustPost(t, h, "DescribeMaintenanceWindows", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	windows := out["WindowIdentities"].([]any)
	assert.NotEmpty(t, windows)

	// Register target
	code, out = mustPost(t, h, "RegisterTargetWithMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-001"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	targetID := out["WindowTargetId"].(string)
	assert.NotEmpty(t, targetID)

	// DescribeTargets
	code, out = mustPost(t, h, "DescribeMaintenanceWindowTargets", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	targets := out["Targets"].([]any)
	assert.Len(t, targets, 1)

	// Register task
	code, out = mustPost(t, h, "RegisterTaskWithMaintenanceWindow", map[string]any{
		"WindowId":       windowID,
		"TaskArn":        "AWS-RunShellScript",
		"TaskType":       "RUN_COMMAND",
		"MaxConcurrency": "1",
		"MaxErrors":      "0",
		"Targets": []map[string]any{
			{"Key": "WindowTargetIds", "Values": []string{targetID}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	taskID := out["WindowTaskId"].(string)
	assert.NotEmpty(t, taskID)

	// DescribeTasks
	code, out = mustPost(t, h, "DescribeMaintenanceWindowTasks", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)
	tasks := out["Tasks"].([]any)
	assert.Len(t, tasks, 1)

	// GetMaintenanceWindowTask
	code, out = mustPost(t, h, "GetMaintenanceWindowTask", map[string]any{
		"WindowId":     windowID,
		"WindowTaskId": taskID,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, taskID, out["WindowTaskId"])

	// UpdateTarget
	code, _ = mustPost(t, h, "UpdateMaintenanceWindowTarget", map[string]any{
		"WindowId":       windowID,
		"WindowTargetId": targetID,
		"Name":           "UpdatedTarget",
	})
	assert.Equal(t, http.StatusOK, code)

	// UpdateTask
	code, _ = mustPost(t, h, "UpdateMaintenanceWindowTask", map[string]any{
		"WindowId":       windowID,
		"WindowTaskId":   taskID,
		"MaxConcurrency": "2",
	})
	assert.Equal(t, http.StatusOK, code)

	// DeregisterTarget
	code, _ = mustPost(t, h, "DeregisterTargetFromMaintenanceWindow", map[string]any{
		"WindowId":       windowID,
		"WindowTargetId": targetID,
	})
	assert.Equal(t, http.StatusOK, code)

	// DeregisterTask
	code, _ = mustPost(t, h, "DeregisterTaskFromMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"WindowTaskId": taskID,
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeleteMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetMaintenanceWindow", map[string]any{"WindowId": windowID})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_MaintenanceWindow_DescribeForTarget(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	_, out := mustPost(t, h, "CreateMaintenanceWindow", map[string]any{
		"Name":     "MW2",
		"Schedule": "cron(0 3 ? * MON *)",
		"Duration": 2,
		"Cutoff":   1,
	})
	windowID := out["WindowId"].(string)

	mustPost(t, h, "RegisterTargetWithMaintenanceWindow", map[string]any{
		"WindowId":     windowID,
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "tag:Env", "Values": []string{"prod"}},
		},
	})

	code, out := mustPost(t, h, "DescribeMaintenanceWindowsForTarget", map[string]any{
		"ResourceType": "INSTANCE",
		"Targets": []map[string]any{
			{"Key": "tag:Env", "Values": []string{"prod"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["WindowIdentities"])
}

// ---------------------------------------------------------------------------
// Patch Baseline + groups lifecycle
// ---------------------------------------------------------------------------

func TestFull_PatchBaseline_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreatePatchBaseline", map[string]any{
		"Name":            "TestBaseline",
		"OperatingSystem": "WINDOWS",
		"Description":     "test patch baseline",
	})
	assert.Equal(t, http.StatusOK, code)
	baselineID := out["BaselineId"].(string)
	assert.NotEmpty(t, baselineID)

	// Get
	code, out = mustPost(t, h, "GetPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestBaseline", out["Name"])

	// Update
	code, _ = mustPost(t, h, "UpdatePatchBaseline", map[string]any{
		"BaselineId":  baselineID,
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, code)

	// Register patch group
	code, _ = mustPost(t, h, "RegisterPatchBaselineForPatchGroup", map[string]any{
		"BaselineId": baselineID,
		"PatchGroup": "prod-servers",
	})
	assert.Equal(t, http.StatusOK, code)

	// DescribePatchGroups
	code, out = mustPost(t, h, "DescribePatchGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	groups := out["Mappings"].([]any)
	assert.NotEmpty(t, groups)

	// GetPatchBaselineForPatchGroup
	code, out = mustPost(t, h, "GetPatchBaselineForPatchGroup", map[string]any{"PatchGroup": "prod-servers"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, baselineID, out["BaselineId"])

	// Set as default
	code, _ = mustPost(t, h, "RegisterDefaultPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)

	// GetDefaultPatchBaseline
	code, out = mustPost(t, h, "GetDefaultPatchBaseline", map[string]any{"OperatingSystem": "WINDOWS"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, baselineID, out["BaselineId"])

	// DescribePatchBaselines
	code, out = mustPost(t, h, "DescribePatchBaselines", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["BaselineIdentities"])

	// DeregisterPatchGroup
	code, _ = mustPost(t, h, "DeregisterPatchBaselineForPatchGroup", map[string]any{
		"BaselineId": baselineID,
		"PatchGroup": "prod-servers",
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeletePatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusBadRequest, code)
}

// ---------------------------------------------------------------------------
// OpsItem CRUD
// ---------------------------------------------------------------------------

func TestFull_OpsItem_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateOpsItem", map[string]any{
		"Title":       "High CPU Usage",
		"Source":      "CloudWatch",
		"Severity":    "2",
		"Category":    "Performance",
		"Description": "CPU exceeded 90% for 15 minutes",
	})
	assert.Equal(t, http.StatusOK, code)
	opsItemID := out["OpsItemId"].(string)
	assert.NotEmpty(t, opsItemID)

	// Get
	code, out = mustPost(t, h, "GetOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)
	item := out["OpsItem"].(map[string]any)
	assert.Equal(t, "High CPU Usage", item["Title"])
	assert.Equal(t, "Open", item["Status"])

	// Describe
	code, out = mustPost(t, h, "DescribeOpsItems", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	items := out["OpsItemSummaries"].([]any)
	assert.NotEmpty(t, items)

	// Update
	code, _ = mustPost(t, h, "UpdateOpsItem", map[string]any{
		"OpsItemId": opsItemID,
		"Status":    "InProgress",
		"Title":     "High CPU - Under Investigation",
	})
	assert.Equal(t, http.StatusOK, code)

	// Verify update
	code, out = mustPost(t, h, "GetOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)
	item = out["OpsItem"].(map[string]any)
	assert.Equal(t, "InProgress", item["Status"])
	assert.Equal(t, "High CPU - Under Investigation", item["Title"])

	// AssociateRelatedItem
	code, out = mustPost(t, h, "AssociateOpsItemRelatedItem", map[string]any{
		"OpsItemId":       opsItemID,
		"AssociationType": "IsParentOf",
		"ResourceType":    "AWS::EC2::Instance",
		"ResourceUri":     "arn:aws:ec2:us-east-1:123:instance/i-abc",
	})
	assert.Equal(t, http.StatusOK, code)
	assocID := out["AssociationId"].(string)
	assert.NotEmpty(t, assocID)

	// ListRelatedItems
	code, out = mustPost(t, h, "ListOpsItemRelatedItems", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)
	relatedItems := out["Summaries"].([]any)
	assert.Len(t, relatedItems, 1)

	// DisassociateRelatedItem
	code, _ = mustPost(t, h, "DisassociateOpsItemRelatedItem", map[string]any{
		"OpsItemId":     opsItemID,
		"AssociationId": assocID,
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeleteOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetOpsItem", map[string]any{"OpsItemId": opsItemID})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestFull_OpsItem_FilterByStatus(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	_, out1 := mustPost(t, h, "CreateOpsItem", map[string]any{
		"Title":  "Item A",
		"Source": "manual",
	})
	id1 := out1["OpsItemId"].(string)

	mustPost(t, h, "CreateOpsItem", map[string]any{
		"Title":  "Item B",
		"Source": "manual",
	})

	// Close item A
	mustPost(t, h, "UpdateOpsItem", map[string]any{
		"OpsItemId": id1,
		"Status":    "Resolved",
	})

	code, out := mustPost(t, h, "DescribeOpsItems", map[string]any{
		"OpsItemFilters": []map[string]any{
			{"Key": "Status", "Values": []string{"Open"}, "Operator": "Equal"},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	items := out["OpsItemSummaries"].([]any)
	assert.Len(t, items, 1)
}

// ---------------------------------------------------------------------------
// OpsMetadata
// ---------------------------------------------------------------------------

func TestFull_OpsMetadata_CreateUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreateOpsMetadata", map[string]any{
		"ResourceId": "i-abc123",
		"Metadata": map[string]any{
			"env": map[string]any{"Value": "prod"},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	arn := out["OpsMetadataArn"].(string)
	assert.NotEmpty(t, arn)

	// Get
	code, out = mustPost(t, h, "GetOpsMetadata", map[string]any{"OpsMetadataArn": arn})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Metadata"])

	// Update
	code, _ = mustPost(t, h, "UpdateOpsMetadata", map[string]any{
		"OpsMetadataArn": arn,
		"MetadataToUpdate": map[string]any{
			"version": map[string]any{"Value": "2"},
		},
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeleteOpsMetadata", map[string]any{"OpsMetadataArn": arn})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetOpsMetadata", map[string]any{"OpsMetadataArn": arn})
	assert.Equal(t, http.StatusBadRequest, code)
}

// ---------------------------------------------------------------------------
// Activation lifecycle
// ---------------------------------------------------------------------------

func TestFull_Activation_CreateList(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	code, out := mustPost(t, h, "CreateActivation", map[string]any{
		"IamRole":           "SSMServiceRole",
		"RegistrationLimit": 5,
		"Description":       "test activation",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["ActivationId"])
	assert.NotEmpty(t, out["ActivationCode"])
}

// ---------------------------------------------------------------------------
// Association lifecycle
// ---------------------------------------------------------------------------

func TestFull_Association_CreateDescribeUpdate(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Need a document
	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "AssocDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	// Create association
	code, out := mustPost(t, h, "CreateAssociation", map[string]any{
		"Name":       "AssocDoc",
		"InstanceId": "i-assoc001",
		"Parameters": map[string]any{"cmd": []string{"echo test"}},
	})
	assert.Equal(t, http.StatusOK, code)
	assocID := out["AssociationDescription"].(map[string]any)["AssociationId"].(string)
	assert.NotEmpty(t, assocID)

	// Describe
	code, out = mustPost(t, h, "DescribeAssociation", map[string]any{"AssociationId": assocID})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["AssociationDescription"])

	// Update
	code, _ = mustPost(t, h, "UpdateAssociation", map[string]any{
		"AssociationId":   assocID,
		"AssociationName": "UpdatedAssoc",
	})
	assert.Equal(t, http.StatusOK, code)
}

func TestFull_Association_Batch(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "CreateDocument", map[string]any{
		"Name":    "BatchDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, out := mustPost(t, h, "CreateAssociationBatch", map[string]any{
		"Entries": []map[string]any{
			{"Name": "BatchDoc", "InstanceId": "i-b1"},
			{"Name": "BatchDoc", "InstanceId": "i-b2"},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	successful := out["Successful"].([]any)
	assert.Len(t, successful, 2)
}

// ---------------------------------------------------------------------------
// Parameter Store — DescribeParameters with multiple filter types
// ---------------------------------------------------------------------------

func TestFull_DescribeParameters_NameFilter_BeginsWith(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for _, name := range []string{"/prod/a", "/prod/b", "/dev/c"} {
		mustPost(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := mustPost(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "Name", "Option": "BeginsWith", "Values": []string{"/prod/"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 2)
}

func TestFull_DescribeParameters_TypeFilter(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{"Name": "/t/str", "Type": "String", "Value": "v"})
	mustPost(t, h, "PutParameter", map[string]any{"Name": "/t/sl", "Type": "StringList", "Value": "a,b,c"})

	code, out := mustPost(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "Type", "Values": []string{"StringList"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
}

// ---------------------------------------------------------------------------
// GetParametersByPath with ParameterFilters
// ---------------------------------------------------------------------------

func TestFull_GetParametersByPath_WithFilters(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{"Name": "/svc/str", "Type": "String", "Value": "v"})
	mustPost(t, h, "PutParameter", map[string]any{"Name": "/svc/sec", "Type": "SecureString", "Value": "s"})

	code, out := mustPost(t, h, "GetParametersByPath", map[string]any{
		"Path": "/svc",
		"ParameterFilters": []map[string]any{
			{"Key": "Type", "Values": []string{"String"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/svc/str", params[0].(map[string]any)["Name"])
}

// ---------------------------------------------------------------------------
// Resource Data Sync
// ---------------------------------------------------------------------------

func TestFull_ResourceDataSync_CreateListDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	code, _ := mustPost(t, h, "CreateResourceDataSync", map[string]any{
		"SyncName": "my-sync",
		"SyncType": "SyncToDestination",
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := mustPost(t, h, "ListResourceDataSync", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	syncs := out["ResourceDataSyncItems"].([]any)
	assert.Len(t, syncs, 1)

	code, _ = mustPost(t, h, "DeleteResourceDataSync", map[string]any{"SyncName": "my-sync"})
	assert.Equal(t, http.StatusOK, code)

	code, out = mustPost(t, h, "ListResourceDataSync", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	syncs = out["ResourceDataSyncItems"].([]any)
	assert.Empty(t, syncs)
}

// ---------------------------------------------------------------------------
// Session Manager full lifecycle
// ---------------------------------------------------------------------------

func TestFull_Session_StartTerminateDescribe(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Start
	code, out := mustPost(t, h, "StartSession", map[string]any{
		"Target":       "i-session-target",
		"DocumentName": "AWS-StartSSHSession",
		"Reason":       "deploy fix",
	})
	assert.Equal(t, http.StatusOK, code)
	sid := out["SessionId"].(string)
	assert.NotEmpty(t, sid)
	assert.NotEmpty(t, out["StreamUrl"])
	assert.NotEmpty(t, out["TokenValue"])

	// DescribeSessions - all
	code, out = mustPost(t, h, "DescribeSessions", map[string]any{"State": ""})
	assert.Equal(t, http.StatusOK, code)
	sessions := out["Sessions"].([]any)
	assert.Len(t, sessions, 1)
	sess := sessions[0].(map[string]any)
	assert.Equal(t, "AWS-StartSSHSession", sess["DocumentName"])
	assert.Equal(t, "deploy fix", sess["Reason"])

	// ResumeSession
	code, out = mustPost(t, h, "ResumeSession", map[string]any{"SessionId": sid})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["SessionId"])

	// Terminate
	code, out = mustPost(t, h, "TerminateSession", map[string]any{"SessionId": sid})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, sid, out["SessionId"])

	// DescribeSessions after terminate — session should be Terminated with EndDate
	code, out = mustPost(t, h, "DescribeSessions", map[string]any{"State": ""})
	assert.Equal(t, http.StatusOK, code)
	allSessions := out["Sessions"].([]any)
	require.Len(t, allSessions, 1)
	terminated := allSessions[0].(map[string]any)
	assert.Equal(t, "Terminated", terminated["Status"])
	assert.Greater(t, terminated["EndDate"].(float64), float64(0))
}

// ---------------------------------------------------------------------------
// Automation execution
// ---------------------------------------------------------------------------

func TestFull_AutomationExecution_StartDescribeStop(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	code, out := mustPost(t, h, "StartAutomationExecution", map[string]any{
		"DocumentName": "AWS-RestartEC2Instance",
		"Parameters":   map[string]any{"InstanceId": []string{"i-auto"}},
	})
	assert.Equal(t, http.StatusOK, code)
	execID := out["AutomationExecutionId"].(string)
	assert.NotEmpty(t, execID)

	code, out = mustPost(t, h, "GetAutomationExecution", map[string]any{
		"AutomationExecutionId": execID,
	})
	assert.Equal(t, http.StatusOK, code)
	exec := out["AutomationExecution"].(map[string]any)
	assert.Equal(t, execID, exec["AutomationExecutionId"])

	code, out = mustPost(t, h, "DescribeAutomationExecutions", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	executions := out["AutomationExecutionMetadataList"].([]any)
	assert.NotEmpty(t, executions)

	code, _ = mustPost(t, h, "StopAutomationExecution", map[string]any{
		"AutomationExecutionId": execID,
		"Type":                  "Cancel",
	})
	assert.Equal(t, http.StatusOK, code)
}

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------

func TestFull_Inventory_PutGetSchema(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	code, _ := mustPost(t, h, "PutInventory", map[string]any{
		"InstanceId": "i-inv001",
		"Items": []map[string]any{
			{
				"TypeName":      "AWS:Application",
				"SchemaVersion": "1.1",
				"CaptureTime":   "2024-01-01T00:00:00Z",
				"Content":       []map[string]any{{"Name": "nginx", "Version": "1.24"}},
			},
		},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := mustPost(t, h, "GetInventory", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Entities"])

	code, out = mustPost(t, h, "GetInventorySchema", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Schemas"])

	code, out = mustPost(t, h, "ListInventoryEntries", map[string]any{
		"InstanceId": "i-inv001",
		"TypeName":   "AWS:Application",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["Entries"])
}

// ---------------------------------------------------------------------------
// Service Settings
// ---------------------------------------------------------------------------

func TestFull_ServiceSetting_GetUpdateReset(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	settingID := "/ssm/parameter-store/high-throughput-enabled"

	code, out := mustPost(t, h, "GetServiceSetting", map[string]any{"SettingId": settingID})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ServiceSetting"])

	code, _ = mustPost(t, h, "UpdateServiceSetting", map[string]any{
		"SettingId":    settingID,
		"SettingValue": "true",
	})
	assert.Equal(t, http.StatusOK, code)

	code, _ = mustPost(t, h, "ResetServiceSetting", map[string]any{"SettingId": settingID})
	assert.Equal(t, http.StatusOK, code)
}

// ---------------------------------------------------------------------------
// Compliance
// ---------------------------------------------------------------------------

func TestFull_Compliance_PutListSummary(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	code, _ := mustPost(t, h, "PutComplianceItems", map[string]any{
		"ResourceId":     "i-comp001",
		"ResourceType":   "ManagedInstance",
		"ComplianceType": "Association",
		"ExecutionSummary": map[string]any{
			"ExecutionTime": "2024-01-01T00:00:00Z",
		},
		"Items": []map[string]any{
			{"Id": "a1", "Title": "patch-1", "Status": "COMPLIANT", "Severity": "UNSPECIFIED"},
		},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := mustPost(t, h, "ListComplianceItems", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ComplianceItems"])

	code, out = mustPost(t, h, "ListComplianceSummaries", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ComplianceSummaryItems"])

	code, out = mustPost(t, h, "ListResourceComplianceSummaries", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ResourceComplianceSummaryItems"])
}
