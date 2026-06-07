package ssm_test

// backend_ops_test.go contains one happy-path test per implemented operation in
// backend_ops.go.  Tests follow the pattern established in backend_test.go:
// create a backend, call the operation, assert the result.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newBackend(t *testing.T) *ssm.InMemoryBackend {
	t.Helper()

	return ssm.NewInMemoryBackend()
}

// createTestBaseline creates a patch baseline and returns its ID.
func createTestBaseline(t *testing.T, b *ssm.InMemoryBackend, name string) string {
	t.Helper()

	out, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            name,
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	return out.BaselineID
}

// createTestWindow creates a maintenance window and returns its ID.
func createTestWindow(t *testing.T, b *ssm.InMemoryBackend) string {
	t.Helper()

	out, err := b.CreateMaintenanceWindow(context.TODO(), &ssm.CreateMaintenanceWindowInput{
		Name:     "test-window",
		Schedule: "cron(0 9 ? * MON *)",
		Duration: 2,
		Cutoff:   1,
	})
	require.NoError(t, err)

	return out.WindowID
}

// createTestOpsItem creates an OpsItem and returns its ID.
func createTestOpsItem(t *testing.T, b *ssm.InMemoryBackend) string {
	t.Helper()

	out, err := b.CreateOpsItem(context.TODO(), &ssm.CreateOpsItemInput{
		Title:       "test-item",
		Source:      "test",
		Description: "test description",
	})
	require.NoError(t, err)

	return out.OpsItemID
}

// ---------------------------------------------------------------------------
// Group 1 — Document ops
// ---------------------------------------------------------------------------

func TestBackendOps_UpdateDocumentDefaultVersion(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Create a document with two versions.
	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "TestDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateDocument(context.TODO(), &ssm.UpdateDocumentInput{
		Name:    "TestDoc",
		Content: `{"schemaVersion":"2.2","updated":true}`,
	})
	require.NoError(t, err)

	// Set default version to "1".
	out, err := b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{
		Name:            "TestDoc",
		DocumentVersion: "1",
	})
	require.NoError(t, err)
	assert.Equal(t, "TestDoc", out.Description.Name)
	assert.Equal(t, "1", out.Description.DefaultVersion)
}

func TestBackendOps_UpdateDocumentDefaultVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.UpdateDocumentDefaultVersion(context.TODO(), &ssm.UpdateDocumentDefaultVersionInput{
		Name:            "NoSuchDoc",
		DocumentVersion: "1",
	})
	require.Error(t, err)
}

func TestBackendOps_UpdateDocumentMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "MetaDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	out, err := b.UpdateDocumentMetadata(context.TODO(), &ssm.UpdateDocumentMetadataInput{
		Name: "MetaDoc",
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_ListDocumentMetadataHistory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateDocument(context.TODO(), &ssm.CreateDocumentInput{
		Name:    "HistoryDoc",
		Content: `{"schemaVersion":"2.2"}`,
	})
	require.NoError(t, err)

	out, err := b.ListDocumentMetadataHistory(context.TODO(), &ssm.ListDocumentMetadataHistoryInput{
		Name: "HistoryDoc",
	})
	require.NoError(t, err)
	assert.Equal(t, "HistoryDoc", out.Name)
	assert.NotNil(t, out.Metadata)
	assert.Empty(t, out.Metadata.ReviewerResponse)
}

// ---------------------------------------------------------------------------
// Group 2 — Inventory
// ---------------------------------------------------------------------------

func TestBackendOps_PutInventory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-1234567890abcdef0",
		Items: []ssm.InventoryItem{
			{
				TypeName:      "AWS:Application",
				SchemaVersion: "1.1",
				CaptureTime:   "2024-01-01T00:00:00Z",
				Content: []map[string]string{
					{"Name": "AmazonSSMAgent", "Version": "3.0.0"},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_GetInventory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-abcdef1234567890",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Network", SchemaVersion: "1.0"},
		},
	})
	require.NoError(t, err)

	out, err := b.GetInventory(context.TODO(), &ssm.GetInventoryInput{})
	require.NoError(t, err)
	assert.Len(t, out.Entities, 1)
	assert.Equal(t, "i-abcdef1234567890", out.Entities[0].ID)
}

func TestBackendOps_GetInventorySchema(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.GetInventorySchema(context.TODO(), &ssm.GetInventorySchemaInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.Schemas)
}

func TestBackendOps_ListInventoryEntries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	content := []map[string]string{{"Name": "sshd", "Version": "1.0"}}

	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-entries-test",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Service", SchemaVersion: "1.0", Content: content},
		},
	})
	require.NoError(t, err)

	out, err := b.ListInventoryEntries(context.TODO(), &ssm.ListInventoryEntriesInput{
		InstanceID: "i-entries-test",
		TypeName:   "AWS:Service",
	})
	require.NoError(t, err)
	assert.Equal(t, "i-entries-test", out.InstanceID)
	assert.Equal(t, "AWS:Service", out.TypeName)
	assert.Len(t, out.Entries, 1)
}

func TestBackendOps_DeleteInventory(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutInventory(context.TODO(), &ssm.PutInventoryInput{
		InstanceID: "i-delete-test",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Application"},
			{TypeName: "AWS:Network"},
		},
	})
	require.NoError(t, err)

	out, err := b.DeleteInventory(context.TODO(), &ssm.DeleteInventoryInput{TypeName: "AWS:Application"})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Verify only AWS:Application was deleted.
	listOut, err := b.ListInventoryEntries(context.TODO(), &ssm.ListInventoryEntriesInput{
		InstanceID: "i-delete-test",
		TypeName:   "AWS:Application",
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.Entries)
}

func TestBackendOps_DescribeInventoryDeletions(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribeInventoryDeletions(context.TODO(), &ssm.DescribeInventoryDeletionsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.InventoryDeletions)
}

// ---------------------------------------------------------------------------
// Group 3 — Compliance
// ---------------------------------------------------------------------------

func TestBackendOps_PutComplianceItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:     "i-compliance-test",
		ResourceType:   "ManagedInstance",
		ComplianceType: "Association",
		Items: []ssm.ComplianceItem{
			{Status: "COMPLIANT", Severity: "UNSPECIFIED", Title: "AssocComp"},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_ListComplianceItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.PutComplianceItems(context.TODO(), &ssm.PutComplianceItemsInput{
		ResourceID:   "i-list-compliance",
		ResourceType: "ManagedInstance",
		Items: []ssm.ComplianceItem{
			{Status: "COMPLIANT", Title: "Item1"},
		},
	})
	require.NoError(t, err)

	out, err := b.ListComplianceItems(context.TODO(), &ssm.ListComplianceItemsInput{
		ResourceID: "i-list-compliance",
	})
	require.NoError(t, err)
	assert.Len(t, out.ComplianceItems, 1)
	assert.Equal(t, "i-list-compliance", out.ComplianceItems[0].ResourceID)
}

func TestBackendOps_ListComplianceSummaries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.ListComplianceSummaries(context.TODO(), &ssm.ListComplianceSummariesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.ComplianceSummaryItems)
}

func TestBackendOps_ListResourceComplianceSummaries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.ListResourceComplianceSummaries(context.TODO(), &ssm.ListResourceComplianceSummariesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.ResourceComplianceSummaryItems)
}

// ---------------------------------------------------------------------------
// Group 4 — Patch baseline / groups
// ---------------------------------------------------------------------------

func TestBackendOps_GetPatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "get-baseline-test")

	out, err := b.GetPatchBaseline(context.TODO(), &ssm.GetPatchBaselineInput{BaselineID: id})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
}

func TestBackendOps_GetDefaultPatchBaseline_NoDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// No default registered; should return a synthetic ID.
	out, err := b.GetDefaultPatchBaseline(context.TODO(), &ssm.GetDefaultPatchBaselineInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out.BaselineID)
}

func TestBackendOps_GetDefaultPatchBaseline_WithDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "default-baseline")

	_, err := b.RegisterDefaultPatchBaseline(context.TODO(), &ssm.RegisterDefaultPatchBaselineInput{
		BaselineID: id,
	})
	require.NoError(t, err)

	out, err := b.GetDefaultPatchBaseline(context.TODO(), &ssm.GetDefaultPatchBaselineInput{})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
}

func TestBackendOps_GetPatchBaselineForPatchGroup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "group-baseline")

	_, err := b.RegisterPatchBaselineForPatchGroup(context.TODO(), &ssm.RegisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "my-group",
	})
	require.NoError(t, err)

	out, err := b.GetPatchBaselineForPatchGroup(context.TODO(), &ssm.GetPatchBaselineForPatchGroupInput{
		PatchGroup: "my-group",
	})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
	assert.Equal(t, "my-group", out.PatchGroup)
}

func TestBackendOps_RegisterDefaultPatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "reg-default-baseline")

	out, err := b.RegisterDefaultPatchBaseline(context.TODO(), &ssm.RegisterDefaultPatchBaselineInput{
		BaselineID: id,
	})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
}

func TestBackendOps_DeregisterPatchBaselineForPatchGroup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "dereg-baseline")

	_, err := b.RegisterPatchBaselineForPatchGroup(context.TODO(), &ssm.RegisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "dereg-group",
	})
	require.NoError(t, err)

	out, err := b.DeregisterPatchBaselineForPatchGroup(context.TODO(), &ssm.DeregisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "dereg-group",
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_DeletePatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "delete-baseline")

	out, err := b.DeletePatchBaseline(context.TODO(), &ssm.DeletePatchBaselineInput{BaselineID: id})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)

	// Should be gone.
	_, err = b.GetPatchBaseline(context.TODO(), &ssm.GetPatchBaselineInput{BaselineID: id})
	require.Error(t, err)
}

func TestBackendOps_DescribePatchBaselines(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestBaseline(t, b, "describe-baseline-1")
	createTestBaseline(t, b, "describe-baseline-2")

	out, err := b.DescribePatchBaselines(context.TODO(), &ssm.DescribePatchBaselinesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.BaselineIdentities), 2)
}

func TestBackendOps_DescribePatchGroups(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "pg-baseline")

	_, err := b.RegisterPatchBaselineForPatchGroup(context.TODO(), &ssm.RegisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "pg-test-group",
	})
	require.NoError(t, err)

	out, err := b.DescribePatchGroups(context.TODO(), &ssm.DescribePatchGroupsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Mappings)
}

func TestBackendOps_DescribePatchGroupState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribePatchGroupState(context.TODO(), &ssm.DescribePatchGroupStateInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, int32(0), out.Instances)
}

func TestBackendOps_DescribePatchProperties(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribePatchProperties(context.TODO(), &ssm.DescribePatchPropertiesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.Properties)
}

func TestBackendOps_DescribeEffectivePatchesForPatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "effective-patches-baseline")

	out, err := b.DescribeEffectivePatchesForPatchBaseline(context.TODO(),
		&ssm.DescribeEffectivePatchesForPatchBaselineInput{BaselineID: id},
	)
	require.NoError(t, err)
	assert.NotNil(t, out.EffectivePatches)
}

func TestBackendOps_GetDeployablePatchSnapshotForInstance(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.GetDeployablePatchSnapshotForInstance(context.TODO(),
		&ssm.GetDeployablePatchSnapshotForInstanceInput{
			InstanceID: "i-snapshot-test",
			SnapshotID: "snap-1234",
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "i-snapshot-test", out.InstanceID)
	assert.Equal(t, "snap-1234", out.SnapshotID)
	assert.NotEmpty(t, out.SnapshotDownloadURL)
}

// ---------------------------------------------------------------------------
// Group 5 — Maintenance window
// ---------------------------------------------------------------------------

func TestBackendOps_GetMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	out, err := b.GetMaintenanceWindow(context.TODO(), &ssm.GetMaintenanceWindowInput{WindowID: wid})
	require.NoError(t, err)
	assert.Equal(t, wid, out.WindowID)
}

func TestBackendOps_DeleteMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	out, err := b.DeleteMaintenanceWindow(context.TODO(), &ssm.DeleteMaintenanceWindowInput{WindowID: wid})
	require.NoError(t, err)
	assert.Equal(t, wid, out.WindowID)

	// Should be gone.
	_, err = b.GetMaintenanceWindow(context.TODO(), &ssm.GetMaintenanceWindowInput{WindowID: wid})
	require.Error(t, err)
}

func TestBackendOps_UpdateMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	out, err := b.UpdateMaintenanceWindow(context.TODO(), &ssm.UpdateMaintenanceWindowInput{
		WindowID: wid,
		Name:     "updated-window",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-window", out.Name)
}

func TestBackendOps_GetMaintenanceWindowTask(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	taskOut, err := b.RegisterTaskWithMaintenanceWindow(context.TODO(), &ssm.RegisterTaskWithMaintenanceWindowInput{
		WindowID: wid,
		TaskArn:  "AWS-RunShellScript",
		TaskType: "RUN_COMMAND",
	})
	require.NoError(t, err)

	out, err := b.GetMaintenanceWindowTask(context.TODO(), &ssm.GetMaintenanceWindowTaskInput{
		WindowID:     wid,
		WindowTaskID: taskOut.WindowTaskID,
	})
	require.NoError(t, err)
	assert.Equal(t, taskOut.WindowTaskID, out.WindowTaskID)
}

func TestBackendOps_RegisterTargetWithMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	out, err := b.RegisterTargetWithMaintenanceWindow(context.TODO(), &ssm.RegisterTargetWithMaintenanceWindowInput{
		WindowID:     wid,
		ResourceType: "INSTANCE",
		Targets:      []ssm.WindowTarget{{Key: "InstanceIds", Values: []string{"i-test"}}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.WindowTargetID)
}

func TestBackendOps_RegisterTaskWithMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	out, err := b.RegisterTaskWithMaintenanceWindow(context.TODO(), &ssm.RegisterTaskWithMaintenanceWindowInput{
		WindowID: wid,
		TaskArn:  "AWS-RunShellScript",
		TaskType: "RUN_COMMAND",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.WindowTaskID)
}

func TestBackendOps_DeregisterTargetFromMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	targetOut, err := b.RegisterTargetWithMaintenanceWindow(context.TODO(), &ssm.RegisterTargetWithMaintenanceWindowInput{
		WindowID:     wid,
		ResourceType: "INSTANCE",
		Targets:      []ssm.WindowTarget{{Key: "InstanceIds", Values: []string{"i-test"}}},
	})
	require.NoError(t, err)

	out, err := b.DeregisterTargetFromMaintenanceWindow(context.TODO(), &ssm.DeregisterTargetFromMaintenanceWindowInput{
		WindowID:       wid,
		WindowTargetID: targetOut.WindowTargetID,
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_DeregisterTaskFromMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	taskOut, err := b.RegisterTaskWithMaintenanceWindow(context.TODO(), &ssm.RegisterTaskWithMaintenanceWindowInput{
		WindowID: wid,
		TaskArn:  "AWS-RunShellScript",
		TaskType: "RUN_COMMAND",
	})
	require.NoError(t, err)

	out, err := b.DeregisterTaskFromMaintenanceWindow(context.TODO(), &ssm.DeregisterTaskFromMaintenanceWindowInput{
		WindowID:     wid,
		WindowTaskID: taskOut.WindowTaskID,
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_DescribeMaintenanceWindows(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestWindow(t, b)

	out, err := b.DescribeMaintenanceWindows(context.TODO(), &ssm.DescribeMaintenanceWindowsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out.WindowIdentities)
}

func TestBackendOps_DescribeMaintenanceWindowsForTarget(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribeMaintenanceWindowsForTarget(context.TODO(), &ssm.DescribeMaintenanceWindowsForTargetInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.WindowIdentities)
}

func TestBackendOps_DescribeMaintenanceWindowTargets(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	_, err := b.RegisterTargetWithMaintenanceWindow(context.TODO(), &ssm.RegisterTargetWithMaintenanceWindowInput{
		WindowID:     wid,
		ResourceType: "INSTANCE",
		Targets:      []ssm.WindowTarget{{Key: "InstanceIds", Values: []string{"i-test"}}},
	})
	require.NoError(t, err)

	out, err := b.DescribeMaintenanceWindowTargets(context.TODO(), &ssm.DescribeMaintenanceWindowTargetsInput{
		WindowID: wid,
	})
	require.NoError(t, err)
	assert.Len(t, out.Targets, 1)
}

func TestBackendOps_DescribeMaintenanceWindowTasks(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	_, err := b.RegisterTaskWithMaintenanceWindow(context.TODO(), &ssm.RegisterTaskWithMaintenanceWindowInput{
		WindowID: wid,
		TaskArn:  "AWS-RunShellScript",
		TaskType: "RUN_COMMAND",
	})
	require.NoError(t, err)

	out, err := b.DescribeMaintenanceWindowTasks(context.TODO(), &ssm.DescribeMaintenanceWindowTasksInput{
		WindowID: wid,
	})
	require.NoError(t, err)
	assert.Len(t, out.Tasks, 1)
}

func TestBackendOps_UpdateMaintenanceWindowTarget(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	targetOut, err := b.RegisterTargetWithMaintenanceWindow(context.TODO(), &ssm.RegisterTargetWithMaintenanceWindowInput{
		WindowID:     wid,
		ResourceType: "INSTANCE",
		Targets:      []ssm.WindowTarget{{Key: "InstanceIds", Values: []string{"i-test"}}},
	})
	require.NoError(t, err)

	out, err := b.UpdateMaintenanceWindowTarget(context.TODO(), &ssm.UpdateMaintenanceWindowTargetInput{
		WindowID:       wid,
		WindowTargetID: targetOut.WindowTargetID,
		Name:           "updated-target",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-target", out.Name)
}

func TestBackendOps_UpdateMaintenanceWindowTask(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	wid := createTestWindow(t, b)

	taskOut, err := b.RegisterTaskWithMaintenanceWindow(context.TODO(), &ssm.RegisterTaskWithMaintenanceWindowInput{
		WindowID: wid,
		TaskArn:  "AWS-RunShellScript",
		TaskType: "RUN_COMMAND",
	})
	require.NoError(t, err)

	priority := int32(5)
	out, err := b.UpdateMaintenanceWindowTask(context.TODO(), &ssm.UpdateMaintenanceWindowTaskInput{
		WindowID:     wid,
		WindowTaskID: taskOut.WindowTaskID,
		Name:         "updated-task",
		Priority:     &priority,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-task", out.Name)
	assert.Equal(t, int32(5), out.Priority)
}

// ---------------------------------------------------------------------------
// Group 6 — OpsItem
// ---------------------------------------------------------------------------

func TestBackendOps_GetOpsItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	out, err := b.GetOpsItem(context.TODO(), &ssm.GetOpsItemInput{OpsItemID: id})
	require.NoError(t, err)
	assert.Equal(t, id, out.OpsItem.OpsItemID)
}

func TestBackendOps_DeleteOpsItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	out, err := b.DeleteOpsItem(context.TODO(), &ssm.DeleteOpsItemInput{OpsItemID: id})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Should be gone.
	_, err = b.GetOpsItem(context.TODO(), &ssm.GetOpsItemInput{OpsItemID: id})
	require.Error(t, err)
}

func TestBackendOps_DescribeOpsItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestOpsItem(t, b)
	createTestOpsItem(t, b)

	out, err := b.DescribeOpsItems(context.TODO(), &ssm.DescribeOpsItemsInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.OpsItemSummaries), 2)
}

func TestBackendOps_UpdateOpsItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	out, err := b.UpdateOpsItem(context.TODO(), &ssm.UpdateOpsItemInput{
		OpsItemID: id,
		Title:     "updated-title",
		Status:    "Resolved",
	})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Verify update took effect.
	getOut, err := b.GetOpsItem(context.TODO(), &ssm.GetOpsItemInput{OpsItemID: id})
	require.NoError(t, err)
	assert.Equal(t, "updated-title", getOut.OpsItem.Title)
	assert.Equal(t, "Resolved", getOut.OpsItem.Status)
}

func TestBackendOps_DisassociateOpsItemRelatedItem(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	// Associate a related item first.
	assocOut, err := b.AssociateOpsItemRelatedItem(context.TODO(), &ssm.AssociateOpsItemRelatedItemInput{
		OpsItemID:       id,
		AssociationType: "IsRelatedTo",
		ResourceType:    "AWS::EC2::Instance",
		ResourceURI:     "arn:aws:ec2:us-east-1:123456789012:instance/i-test",
	})
	require.NoError(t, err)

	// Disassociate.
	out, err := b.DisassociateOpsItemRelatedItem(context.TODO(), &ssm.DisassociateOpsItemRelatedItemInput{
		OpsItemID:     id,
		AssociationID: assocOut.AssociationID,
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestBackendOps_ListOpsItemRelatedItems(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestOpsItem(t, b)

	_, err := b.AssociateOpsItemRelatedItem(context.TODO(), &ssm.AssociateOpsItemRelatedItemInput{
		OpsItemID:       id,
		AssociationType: "IsRelatedTo",
		ResourceType:    "AWS::EC2::Instance",
		ResourceURI:     "arn:aws:ec2:us-east-1:123456789012:instance/i-related",
	})
	require.NoError(t, err)

	out, err := b.ListOpsItemRelatedItems(context.TODO(), &ssm.ListOpsItemRelatedItemsInput{OpsItemID: id})
	require.NoError(t, err)
	assert.Len(t, out.Summaries, 1)
}

func TestBackendOps_ListOpsItemEvents(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.ListOpsItemEvents(context.TODO(), &ssm.ListOpsItemEventsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.Summaries)
}

func TestBackendOps_GetOpsMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	createOut, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-meta-test",
	})
	require.NoError(t, err)

	out, err := b.GetOpsMetadata(context.TODO(), &ssm.GetOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn})
	require.NoError(t, err)
	assert.Equal(t, createOut.OpsMetadataArn, out.OpsMetadataArn)
}

func TestBackendOps_UpdateOpsMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	createOut, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-update-meta",
	})
	require.NoError(t, err)

	out, err := b.UpdateOpsMetadata(context.TODO(), &ssm.UpdateOpsMetadataInput{
		OpsMetadataArn: createOut.OpsMetadataArn,
		Metadata: map[string]ssm.MetadataValue{
			"env": {Value: "prod"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, createOut.OpsMetadataArn, out.OpsMetadataArn)
}

func TestBackendOps_DeleteOpsMetadata(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	createOut, err := b.CreateOpsMetadata(context.TODO(), &ssm.CreateOpsMetadataInput{
		ResourceID: "arn:aws:ec2:us-east-1:123456789012:instance/i-delete-meta",
	})
	require.NoError(t, err)

	out, err := b.DeleteOpsMetadata(context.TODO(), &ssm.DeleteOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn})
	require.NoError(t, err)
	assert.NotNil(t, out)

	// Should be gone.
	_, err = b.GetOpsMetadata(context.TODO(), &ssm.GetOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn})
	require.Error(t, err)
}
