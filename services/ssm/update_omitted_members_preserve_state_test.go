package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestUpdate_OmittedMembersPreserveState locks in the zeroguard census fix
// (gopherstack-uox6): an Update op's optional *string/*int32/*bool member
// must be applied only when the caller sends it. Before the fix these
// fields decoded as plain values, so a second update call that simply
// omitted a field silently blanked it instead of leaving the stored value
// alone. Each case creates a resource, sets a field, then sends a second
// update that omits it and asserts the earlier value survived.
//
// UpdateAssociation is deliberately not a case here: AWS nulls its omitted
// fields instead. See TestUpdateAssociation_ReplacesOmittedFields_RealClient.
func TestUpdate_OmittedMembersPreserveState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "cloud_connector_display_name", run: testCloudConnectorDisplayNamePreserved},
		{name: "document_display_name", run: testDocumentDisplayNamePreserved},
		{name: "maintenance_window_description_and_zero_cutoff", run: testMaintenanceWindowFieldsPreserved},
		{name: "maintenance_window_target_owner", run: testMaintenanceWindowTargetOwnerPreserved},
		{name: "maintenance_window_task_service_role", run: testMaintenanceWindowTaskServiceRolePreserved},
		{name: "ops_item_severity", run: testOpsItemSeverityPreserved},
		{name: "patch_baseline_description", run: testPatchBaselineDescriptionPreserved},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCloudConnectorDisplayNamePreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateCloudConnector(ctx, &ssmsdk.CreateCloudConnectorInput{
		ConfigConnectorArn: aws.String("arn:aws:config::000000000000:config-connector/omit-cc"),
		DisplayName:        aws.String("original-name"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/omit-cloud-connector"),
		Configuration: &ssmtypes.CloudConnectorConfigurationMemberAzureConfiguration{
			Value: ssmtypes.AzureConfiguration{
				ApplicationId: aws.String("app-1"),
				TenantId:      aws.String("tenant-1"),
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateCloudConnector(ctx, &ssmsdk.UpdateCloudConnectorInput{
		CloudConnectorId: created.CloudConnectorId,
		Description:      aws.String("original-description"),
	})
	require.NoError(t, err)

	// A second update that omits both Description and DisplayName must
	// leave them untouched.
	_, err = client.UpdateCloudConnector(ctx, &ssmsdk.UpdateCloudConnectorInput{
		CloudConnectorId: created.CloudConnectorId,
	})
	require.NoError(t, err)

	after, err := client.GetCloudConnector(ctx, &ssmsdk.GetCloudConnectorInput{
		CloudConnectorId: created.CloudConnectorId,
	})
	require.NoError(t, err)
	assert.Equal(t, "original-name", aws.ToString(after.DisplayName))
	assert.Equal(t, "original-description", aws.ToString(after.Description))
}

func testDocumentDisplayNamePreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("OmitDoc"),
		Content: aws.String(`{"schemaVersion":"2.2"}`),
	})
	require.NoError(t, err)

	_, err = client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name:        aws.String("OmitDoc"),
		Content:     aws.String(`{"schemaVersion":"2.2","v":2}`),
		DisplayName: aws.String("friendly-name"),
	})
	require.NoError(t, err)

	// Content is required on every call; DisplayName is omitted here and
	// must survive.
	after, err := client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name:    aws.String("OmitDoc"),
		Content: aws.String(`{"schemaVersion":"2.2","v":3}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "friendly-name", aws.ToString(after.DocumentDescription.DisplayName))
}

func testMaintenanceWindowFieldsPreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("omit-mw"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(4),
		Cutoff:   1,
	})
	require.NoError(t, err)

	withVal, err := client.UpdateMaintenanceWindow(ctx, &ssmsdk.UpdateMaintenanceWindowInput{
		WindowId:    created.WindowId,
		Description: aws.String("first-description"),
		Cutoff:      aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Equal(t, "first-description", aws.ToString(withVal.Description))
	assert.EqualValues(t, 3, withVal.Cutoff)

	// Omits Description and Cutoff entirely -- both must survive.
	withoutVal, err := client.UpdateMaintenanceWindow(ctx, &ssmsdk.UpdateMaintenanceWindowInput{
		WindowId: created.WindowId,
		Name:     aws.String("omit-mw-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first-description", aws.ToString(withoutVal.Description),
		"Description must survive an update that omits it")
	assert.EqualValues(t, 3, withoutVal.Cutoff,
		"Cutoff must survive an update that omits it")

	// Cutoff=0 is a meaningful explicit value (schedule new tasks right up
	// to the end of the window) and must be applied, not treated as omitted.
	zeroed, err := client.UpdateMaintenanceWindow(ctx, &ssmsdk.UpdateMaintenanceWindowInput{
		WindowId: created.WindowId,
		Cutoff:   aws.Int32(0),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 0, zeroed.Cutoff)
	assert.Equal(t, "first-description", aws.ToString(zeroed.Description),
		"fields not sent in this call must remain untouched")
}

func testMaintenanceWindowTargetOwnerPreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("omit-mw-target"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	target, err := client.RegisterTargetWithMaintenanceWindow(ctx, &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
		WindowId:     win.WindowId,
		ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
		Targets: []ssmtypes.Target{
			{Key: aws.String("InstanceIds"), Values: []string{"i-abc"}},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateMaintenanceWindowTarget(ctx, &ssmsdk.UpdateMaintenanceWindowTargetInput{
		WindowId:         win.WindowId,
		WindowTargetId:   target.WindowTargetId,
		OwnerInformation: aws.String("first-owner"),
	})
	require.NoError(t, err)

	after, err := client.UpdateMaintenanceWindowTarget(ctx, &ssmsdk.UpdateMaintenanceWindowTargetInput{
		WindowId:       win.WindowId,
		WindowTargetId: target.WindowTargetId,
		Name:           aws.String("renamed-target"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first-owner", aws.ToString(after.OwnerInformation),
		"OwnerInformation must survive an update that omits it")
	assert.Equal(t, "renamed-target", aws.ToString(after.Name))
}

func testMaintenanceWindowTaskServiceRolePreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("omit-mw-task"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	task, err := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: win.WindowId,
		TaskArn:  aws.String("AWS-RunShellScript"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
	})
	require.NoError(t, err)

	_, err = client.UpdateMaintenanceWindowTask(ctx, &ssmsdk.UpdateMaintenanceWindowTaskInput{
		WindowId:       win.WindowId,
		WindowTaskId:   task.WindowTaskId,
		ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/first-role"),
	})
	require.NoError(t, err)

	after, err := client.UpdateMaintenanceWindowTask(ctx, &ssmsdk.UpdateMaintenanceWindowTaskInput{
		WindowId:     win.WindowId,
		WindowTaskId: task.WindowTaskId,
		Priority:     aws.Int32(7),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/first-role", aws.ToString(after.ServiceRoleArn),
		"ServiceRoleArn must survive an update that omits it")
	assert.Equal(t, int32(7), after.Priority)
}

func testOpsItemSeverityPreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateOpsItem(ctx, &ssmsdk.CreateOpsItemInput{
		Description: aws.String("omit-desc"),
		Source:      aws.String("EC2"),
		Title:       aws.String("omit-title"),
	})
	require.NoError(t, err)

	_, err = client.UpdateOpsItem(ctx, &ssmsdk.UpdateOpsItemInput{
		OpsItemId: created.OpsItemId,
		Severity:  aws.String("2"),
	})
	require.NoError(t, err)

	_, err = client.UpdateOpsItem(ctx, &ssmsdk.UpdateOpsItemInput{
		OpsItemId: created.OpsItemId,
		Title:     aws.String("renamed-title"),
	})
	require.NoError(t, err)

	after, err := client.GetOpsItem(ctx, &ssmsdk.GetOpsItemInput{OpsItemId: created.OpsItemId})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(after.OpsItem.Severity),
		"Severity must survive an update that omits it")
	assert.Equal(t, "renamed-title", aws.ToString(after.OpsItem.Title))
}

func testPatchBaselineDescriptionPreserved(t *testing.T) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
		Name: aws.String("omit-pb"),
	})
	require.NoError(t, err)

	_, err = client.UpdatePatchBaseline(ctx, &ssmsdk.UpdatePatchBaselineInput{
		BaselineId:  created.BaselineId,
		Description: aws.String("first-description"),
	})
	require.NoError(t, err)

	_, err = client.UpdatePatchBaseline(ctx, &ssmsdk.UpdatePatchBaselineInput{
		BaselineId:                     created.BaselineId,
		ApprovedPatchesComplianceLevel: ssmtypes.PatchComplianceLevelCritical,
	})
	require.NoError(t, err)

	after, err := client.GetPatchBaseline(ctx, &ssmsdk.GetPatchBaselineInput{BaselineId: created.BaselineId})
	require.NoError(t, err)
	assert.Equal(t, "first-description", aws.ToString(after.Description),
		"Description must survive an update that omits it")
	assert.Equal(t, ssmtypes.PatchComplianceLevelCritical, after.ApprovedPatchesComplianceLevel)
}
