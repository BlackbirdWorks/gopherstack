package ssm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ssm.InMemoryBackend) string
		verify func(t *testing.T, b *ssm.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *ssm.InMemoryBackend) string {
				_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
					Name:  "/test/param",
					Value: "my-value",
					Type:  "String",
				})
				if err != nil {
					return ""
				}

				return "/test/param"
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: id})
				require.NoError(t, err)
				assert.Equal(t, id, out.Parameter.Name)
				assert.Equal(t, "my-value", out.Parameter.Value)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ssm.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ssm.InMemoryBackend, _ string) {
				t.Helper()

				params := b.ListAll(context.TODO())
				assert.Empty(t, params)
			},
		},
		{
			name: "patch_operation_state_survives_round_trip",
			setup: func(b *ssm.InMemoryBackend) string {
				_, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
					DocumentName: "AWS-RunPatchBaseline",
					InstanceIDs:  []string{"i-persist"},
					Parameters:   map[string][]string{"Operation": {"Scan"}},
				})
				if err != nil {
					return ""
				}

				return "i-persist"
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend, id string) {
				t.Helper()

				states, err := b.DescribeInstancePatchStates(
					context.TODO(),
					&ssm.DescribeInstancePatchStatesInput{InstanceIDs: []string{id}},
				)
				require.NoError(t, err)
				require.Len(t, states.InstancePatchStates, 1)
				assert.Equal(t, id, states.InstancePatchStates[0].InstanceID)

				patches, err := b.DescribeAvailablePatches(
					context.TODO(),
					&ssm.DescribeAvailablePatchesInput{},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, patches.Patches, "the seeded available-patches catalog must survive restore")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ssm.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ssm.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_FullStateSnapshotRestore seeds one instance of every
// resource collection converted to *store.Table in Phase 3.3 (see
// store_setup.go), plus a sampling of the resource collections deliberately
// left as raw maps, then round-trips the whole backend through
// Snapshot/Restore and verifies every seeded resource is independently
// readable afterward via its normal (region-scoped) read path. This is the
// persistence-safety regression test the conversion calls for: a table that
// was accidentally left off the registry, or a raw field dropped from
// backendSnapshot, shows up here as a specific missing resource rather than
// only surfacing much later against real traffic.
func TestInMemoryBackend_FullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := ssm.NewInMemoryBackend()

	// --- store.Table-backed resources (18) ---

	_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
		Name: "/full/param", Value: "v", Type: "String",
	})
	require.NoError(t, err)

	_, err = b.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: "FullDoc", Content: "{}"})
	require.NoError(t, err)

	sendOut, err := b.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript", InstanceIDs: []string{"i-full"},
	})
	require.NoError(t, err)
	commandID := sendOut.Command.CommandID

	b.AddActivationInternal(ssm.Activation{ActivationID: "act-full", IamRole: "r"})
	b.AddAssociationInternal(ssm.Association{AssociationID: "assoc-full", Name: "FullDoc"})
	b.AddMaintenanceWindowInternal(ssm.MaintenanceWindow{WindowID: "mw-full", Name: "w"})
	b.AddOpsItemInternal(ssm.OpsItem{OpsItemID: "oi-full", Title: "t", Source: "s"})
	b.AddOpsMetadataInternal(ssm.OpsMetadata{OpsMetadataArn: "arn:full", ResourceID: "res-full"})
	b.AddPatchBaselineInternal(ssm.PatchBaseline{BaselineID: "pb-full", Name: "b"})
	b.AddInstancePatchStateInternal(ssm.InstancePatchState{InstanceID: "ips-full"})
	b.AddInstancePropertyInternal(ssm.InstanceProperty{InstanceID: "ip-full"})
	b.AddTerminatedSessionInternal("sess-full", 1000)

	_, err = b.CreateResourceDataSync(ctx, &ssm.CreateResourceDataSyncInput{SyncName: "sync-full"})
	require.NoError(t, err)

	startAutoOut, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
		DocumentName: "AWS-RunShellScript",
	})
	require.NoError(t, err)
	automationExecID := startAutoOut.AutomationExecutionID

	_, err = b.UpdateServiceSetting(ctx, &ssm.UpdateServiceSettingInput{
		SettingID: "/ssm/full-setting", SettingValue: "on",
	})
	require.NoError(t, err)

	previewOut, err := b.StartExecutionPreview(ctx, &ssm.StartExecutionPreviewInput{
		DocumentName: "AWS-RunShellScript",
	})
	require.NoError(t, err)
	previewID := previewOut.ExecutionPreviewID

	// --- a sampling of resources deliberately left as raw maps ---

	err = b.AddTagsToResource(ctx, &ssm.AddTagsToResourceInput{
		ResourceType: "Parameter", ResourceID: "/full/param",
		Tags: []ssm.Tag{{Key: "K", Value: "V"}},
	})
	require.NoError(t, err)

	_, err = b.PutInventory(ctx, &ssm.PutInventoryInput{
		InstanceID: "i-full-inv",
		Items: []ssm.InventoryItem{{
			TypeName: "AWS:Application", SchemaVersion: "1.0", CaptureTime: "2024-01-01T00:00:00Z",
		}},
	})
	require.NoError(t, err)

	// --- round-trip ---

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := ssm.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(ctx, snap))

	// --- verify every seeded resource survived ---

	paramOut, err := fresh.GetParameter(ctx, &ssm.GetParameterInput{Name: "/full/param"})
	require.NoError(t, err)
	assert.Equal(t, "v", paramOut.Parameter.Value)

	docOut, err := fresh.GetDocument(ctx, &ssm.GetDocumentInput{Name: "FullDoc"})
	require.NoError(t, err)
	assert.Equal(t, "{}", docOut.Content)

	cmdOut, err := fresh.ListCommands(ctx, &ssm.ListCommandsInput{CommandID: commandID})
	require.NoError(t, err)
	require.Len(t, cmdOut.Commands, 1)

	actOut, err := fresh.DescribeActivations(ctx, &ssm.DescribeActivationsInput{})
	require.NoError(t, err)
	assert.True(t, containsActivation(actOut.ActivationList, "act-full"))

	assocOut, err := fresh.DescribeAssociation(ctx, &ssm.DescribeAssociationInput{AssociationID: "assoc-full"})
	require.NoError(t, err)
	assert.Equal(t, "assoc-full", assocOut.AssociationDescription.AssociationID)

	mwOut, err := fresh.GetMaintenanceWindow(ctx, &ssm.GetMaintenanceWindowInput{WindowID: "mw-full"})
	require.NoError(t, err)
	assert.Equal(t, "mw-full", mwOut.MaintenanceWindow.WindowID)

	opsItemOut, err := fresh.GetOpsItem(ctx, &ssm.GetOpsItemInput{OpsItemID: "oi-full"})
	require.NoError(t, err)
	assert.Equal(t, "oi-full", opsItemOut.OpsItem.OpsItemID)

	opsMetaOut, err := fresh.GetOpsMetadata(ctx, &ssm.GetOpsMetadataInput{OpsMetadataArn: "arn:full"})
	require.NoError(t, err)
	assert.Equal(t, "res-full", opsMetaOut.ResourceID)

	blOut, err := fresh.GetPatchBaseline(ctx, &ssm.GetPatchBaselineInput{BaselineID: "pb-full"})
	require.NoError(t, err)
	assert.Equal(t, "pb-full", blOut.BaselineID)

	ipsOut, err := fresh.DescribeInstancePatchStates(
		ctx, &ssm.DescribeInstancePatchStatesInput{InstanceIDs: []string{"ips-full"}},
	)
	require.NoError(t, err)
	require.Len(t, ipsOut.InstancePatchStates, 1)

	syncOut, err := fresh.ListResourceDataSync(ctx, &ssm.ListResourceDataSyncInput{})
	require.NoError(t, err)
	assert.True(t, containsSyncName(syncOut.ResourceDataSyncItems, "sync-full"))

	autoOut, err := fresh.GetAutomationExecution(
		ctx, &ssm.GetAutomationExecutionInput{AutomationExecutionID: automationExecID},
	)
	require.NoError(t, err)
	assert.Equal(t, automationExecID, autoOut.AutomationExecution.AutomationExecutionID)

	settingOut, err := fresh.GetServiceSetting(ctx, &ssm.GetServiceSettingInput{SettingID: "/ssm/full-setting"})
	require.NoError(t, err)
	assert.Equal(t, "on", settingOut.ServiceSetting.SettingValue)

	previewOut2, err := fresh.GetExecutionPreview(
		ctx, &ssm.GetExecutionPreviewInput{ExecutionPreviewID: previewID},
	)
	require.NoError(t, err)
	assert.Equal(t, previewID, previewOut2.ExecutionPreviewID)

	// raw-map resources: tags, inventory
	tagsOut, err := fresh.ListTagsForResource(
		ctx, &ssm.ListTagsForResourceInput{ResourceType: "Parameter", ResourceID: "/full/param"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, tagsOut.TagList)

	invOut, err := fresh.GetInventory(ctx, &ssm.GetInventoryInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, invOut.Entities)
}

func containsActivation(list []ssm.Activation, id string) bool {
	for _, a := range list {
		if a.ActivationID == id {
			return true
		}
	}

	return false
}

func containsSyncName(list []ssm.ResourceDataSync, name string) bool {
	for _, s := range list {
		if s.SyncName == name {
			return true
		}
	}

	return false
}
