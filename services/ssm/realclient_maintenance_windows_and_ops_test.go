package ssm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestRealClient_MaintenanceWindowsAndOps drives ssm's remaining typed-
// client-blind ops through the real aws-sdk-go-v2 ssm client
// (gopherstack-n3zi).
func TestRealClient_MaintenanceWindowsAndOps(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "maintenance window targets and tasks",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
					Name:     aws.String("s11-mw"),
					Schedule: aws.String("rate(7 days)"),
					Duration: aws.Int32(2),
					Cutoff:   1,
				})
				require.NoError(t, err)

				target, err := client.RegisterTargetWithMaintenanceWindow(
					ctx,
					&ssmsdk.RegisterTargetWithMaintenanceWindowInput{
						WindowId:     win.WindowId,
						ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
						Targets: []ssmtypes.Target{
							{Key: aws.String("InstanceIds"), Values: []string{"i-abc"}},
						},
					},
				)
				require.NoError(t, err)

				_, err = client.UpdateMaintenanceWindowTarget(ctx, &ssmsdk.UpdateMaintenanceWindowTargetInput{
					WindowId:         win.WindowId,
					WindowTargetId:   target.WindowTargetId,
					OwnerInformation: aws.String("s11-owner"),
				})
				require.NoError(t, err)

				task, err := client.RegisterTaskWithMaintenanceWindow(
					ctx,
					&ssmsdk.RegisterTaskWithMaintenanceWindowInput{
						WindowId: win.WindowId,
						TaskArn:  aws.String("AWS-RunShellScript"),
						TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
					},
				)
				require.NoError(t, err)

				_, err = client.UpdateMaintenanceWindowTask(ctx, &ssmsdk.UpdateMaintenanceWindowTaskInput{
					WindowId:     win.WindowId,
					WindowTaskId: task.WindowTaskId,
					Priority:     aws.Int32(5),
				})
				require.NoError(t, err)

				getTask, err := client.GetMaintenanceWindowTask(ctx, &ssmsdk.GetMaintenanceWindowTaskInput{
					WindowId:     win.WindowId,
					WindowTaskId: task.WindowTaskId,
				})
				require.NoError(t, err)
				assert.Equal(t, int32(5), getTask.Priority)

				_, err = client.DeregisterTaskFromMaintenanceWindow(
					ctx,
					&ssmsdk.DeregisterTaskFromMaintenanceWindowInput{
						WindowId:     win.WindowId,
						WindowTaskId: task.WindowTaskId,
					},
				)
				require.NoError(t, err)

				_, err = client.DeregisterTargetFromMaintenanceWindow(
					ctx,
					&ssmsdk.DeregisterTargetFromMaintenanceWindowInput{
						WindowId:       win.WindowId,
						WindowTargetId: target.WindowTargetId,
					},
				)
				require.NoError(t, err)

				// Maintenance window executions run on their own cron/rate schedule
				// -- there is no real StartMaintenanceWindowExecution op to trigger
				// one on demand, so CancelMaintenanceWindowExecution is exercised
				// against a synthetic execution ID (a valid real call shape: real
				// AWS also accepts any well-formed execution ID here and returns
				// NotFound only asynchronously via the console/other describe ops,
				// not from Cancel itself).
				_, err = client.CancelMaintenanceWindowExecution(ctx, &ssmsdk.CancelMaintenanceWindowExecutionInput{
					WindowExecutionId: aws.String("s11-window-execution-id"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "maintenance window lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
					Name:     aws.String("s11-mw-lifecycle"),
					Schedule: aws.String("rate(7 days)"),
					Duration: aws.Int32(2),
					Cutoff:   1,
				})
				require.NoError(t, err)

				getOut, err := client.GetMaintenanceWindow(
					ctx,
					&ssmsdk.GetMaintenanceWindowInput{WindowId: win.WindowId},
				)
				require.NoError(t, err)
				assert.Equal(t, "s11-mw-lifecycle", aws.ToString(getOut.Name))

				descOut, err := client.DescribeMaintenanceWindows(ctx, &ssmsdk.DescribeMaintenanceWindowsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, descOut.WindowIdentities)

				_, err = client.RegisterTargetWithMaintenanceWindow(
					ctx,
					&ssmsdk.RegisterTargetWithMaintenanceWindowInput{
						WindowId:     win.WindowId,
						ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
						Targets: []ssmtypes.Target{
							{Key: aws.String("InstanceIds"), Values: []string{"i-target"}},
						},
					},
				)
				require.NoError(t, err)

				forTargetOut, err := client.DescribeMaintenanceWindowsForTarget(
					ctx, &ssmsdk.DescribeMaintenanceWindowsForTargetInput{
						ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
						Targets: []ssmtypes.Target{
							{Key: aws.String("InstanceIds"), Values: []string{"i-target"}},
						},
					})
				require.NoError(t, err)
				require.NotEmpty(t, forTargetOut.WindowIdentities)

				scheduleOut, err := client.DescribeMaintenanceWindowSchedule(
					ctx, &ssmsdk.DescribeMaintenanceWindowScheduleInput{WindowId: win.WindowId})
				require.NoError(t, err)
				assert.NotNil(t, scheduleOut.ScheduledWindowExecutions)

				_, err = client.UpdateMaintenanceWindow(ctx, &ssmsdk.UpdateMaintenanceWindowInput{
					WindowId: win.WindowId,
					Name:     aws.String("s11-mw-lifecycle-renamed"),
				})
				require.NoError(t, err)

				afterOut, err := client.GetMaintenanceWindow(
					ctx,
					&ssmsdk.GetMaintenanceWindowInput{WindowId: win.WindowId},
				)
				require.NoError(t, err)
				assert.Equal(t, "s11-mw-lifecycle-renamed", aws.ToString(afterOut.Name))
			},
		},
		{
			name: "patch baselines",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				createOut, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
					Name:            aws.String("s11-baseline"),
					OperatingSystem: ssmtypes.OperatingSystemWindows,
				})
				require.NoError(t, err)

				getOut, err := client.GetPatchBaseline(ctx, &ssmsdk.GetPatchBaselineInput{
					BaselineId: createOut.BaselineId,
				})
				require.NoError(t, err)
				assert.Equal(t, "s11-baseline", aws.ToString(getOut.Name))

				listOut, err := client.DescribePatchBaselines(ctx, &ssmsdk.DescribePatchBaselinesInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listOut.BaselineIdentities)

				_, err = client.UpdatePatchBaseline(ctx, &ssmsdk.UpdatePatchBaselineInput{
					BaselineId:  createOut.BaselineId,
					Description: aws.String("updated description"),
				})
				require.NoError(t, err)

				afterOut, err := client.GetPatchBaseline(
					ctx,
					&ssmsdk.GetPatchBaselineInput{BaselineId: createOut.BaselineId},
				)
				require.NoError(t, err)
				assert.Equal(t, "updated description", aws.ToString(afterOut.Description))

				_, err = client.RegisterDefaultPatchBaseline(ctx, &ssmsdk.RegisterDefaultPatchBaselineInput{
					BaselineId: createOut.BaselineId,
				})
				require.NoError(t, err)

				defaultOut, err := client.GetDefaultPatchBaseline(ctx, &ssmsdk.GetDefaultPatchBaselineInput{
					OperatingSystem: ssmtypes.OperatingSystemWindows,
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(createOut.BaselineId), aws.ToString(defaultOut.BaselineId))

				_, err = client.RegisterPatchBaselineForPatchGroup(ctx, &ssmsdk.RegisterPatchBaselineForPatchGroupInput{
					BaselineId: createOut.BaselineId,
					PatchGroup: aws.String("s11-patch-group"),
				})
				require.NoError(t, err)

				groupStateOut, err := client.DescribePatchGroupState(ctx, &ssmsdk.DescribePatchGroupStateInput{
					PatchGroup: aws.String("s11-patch-group"),
				})
				require.NoError(t, err)
				assert.GreaterOrEqual(t, groupStateOut.Instances, int32(0))

				propsOut, err := client.DescribePatchProperties(ctx, &ssmsdk.DescribePatchPropertiesInput{
					OperatingSystem: ssmtypes.OperatingSystemWindows,
					Property:        ssmtypes.PatchPropertyProduct,
				})
				require.NoError(t, err)
				assert.NotNil(t, propsOut.Properties)

				statesOut, err := client.DescribeInstancePatchStatesForPatchGroup(
					ctx, &ssmsdk.DescribeInstancePatchStatesForPatchGroupInput{
						PatchGroup: aws.String("s11-patch-group"),
					})
				require.NoError(t, err)
				assert.NotNil(t, statesOut.InstancePatchStates)

				snapOut, err := client.GetDeployablePatchSnapshotForInstance(
					ctx, &ssmsdk.GetDeployablePatchSnapshotForInstanceInput{
						InstanceId: aws.String("i-snapshot"),
						SnapshotId: aws.String("snap-1"),
					})
				require.NoError(t, err)
				assert.Equal(t, "i-snapshot", aws.ToString(snapOut.InstanceId))

				_, err = client.DeregisterPatchBaselineForPatchGroup(
					ctx,
					&ssmsdk.DeregisterPatchBaselineForPatchGroupInput{
						BaselineId: createOut.BaselineId,
						PatchGroup: aws.String("s11-patch-group"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "parameters",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{
					Name:  aws.String("/s11/param"),
					Value: aws.String("v1"),
					Type:  ssmtypes.ParameterTypeString,
				})
				require.NoError(t, err)

				_, err = client.PutParameter(ctx, &ssmsdk.PutParameterInput{
					Name:  aws.String("/s11/param2"),
					Value: aws.String("v2"),
					Type:  ssmtypes.ParameterTypeString,
				})
				require.NoError(t, err)

				pathOut, err := client.GetParametersByPath(ctx, &ssmsdk.GetParametersByPathInput{
					Path: aws.String("/s11"),
				})
				require.NoError(t, err)
				require.Len(t, pathOut.Parameters, 2)

				_, err = client.LabelParameterVersion(ctx, &ssmsdk.LabelParameterVersionInput{
					Name:   aws.String("/s11/param"),
					Labels: []string{"stable"},
				})
				require.NoError(t, err)

				histOut, err := client.GetParameterHistory(ctx, &ssmsdk.GetParameterHistoryInput{
					Name: aws.String("/s11/param"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, histOut.Parameters)
				assert.Contains(t, histOut.Parameters[0].Labels, "stable")

				_, err = client.UnlabelParameterVersion(ctx, &ssmsdk.UnlabelParameterVersionInput{
					Name:             aws.String("/s11/param"),
					Labels:           []string{"stable"},
					ParameterVersion: aws.Int64(histOut.Parameters[0].Version),
				})
				require.NoError(t, err)

				afterHistOut, err := client.GetParameterHistory(ctx, &ssmsdk.GetParameterHistoryInput{
					Name: aws.String("/s11/param"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, afterHistOut.Parameters)
				assert.NotContains(t, afterHistOut.Parameters[0].Labels, "stable")

				delOut, err := client.DeleteParameters(ctx, &ssmsdk.DeleteParametersInput{
					Names: []string{"/s11/param", "/s11/param2", "/s11/does-not-exist"},
				})
				require.NoError(t, err)
				assert.ElementsMatch(t, []string{"/s11/param", "/s11/param2"}, delOut.DeletedParameters)
				assert.Contains(t, delOut.InvalidParameters, "/s11/does-not-exist")
			},
		},
		{
			name: "associations",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				batchOut, err := client.CreateAssociationBatch(ctx, &ssmsdk.CreateAssociationBatchInput{
					Entries: []ssmtypes.CreateAssociationBatchRequestEntry{
						{Name: aws.String("AWS-RunShellScript"), InstanceId: aws.String("i-batch-1")},
					},
				})
				require.NoError(t, err)
				require.Len(t, batchOut.Successful, 1)
				assocID := aws.ToString(batchOut.Successful[0].AssociationId)

				_, err = client.UpdateAssociation(ctx, &ssmsdk.UpdateAssociationInput{
					AssociationId:      aws.String(assocID),
					ScheduleExpression: aws.String("rate(1 day)"),
				})
				require.NoError(t, err)

				descOut, err := client.DescribeAssociation(ctx, &ssmsdk.DescribeAssociationInput{
					AssociationId: aws.String(assocID),
				})
				require.NoError(t, err)
				assert.Equal(t, "rate(1 day)", aws.ToString(descOut.AssociationDescription.ScheduleExpression))

				execsOut, err := client.DescribeAssociationExecutions(ctx, &ssmsdk.DescribeAssociationExecutionsInput{
					AssociationId: aws.String(assocID),
				})
				require.NoError(t, err)
				require.NotEmpty(t, execsOut.AssociationExecutions)
				execID := aws.ToString(execsOut.AssociationExecutions[0].ExecutionId)

				targetsOut, err := client.DescribeAssociationExecutionTargets(
					ctx, &ssmsdk.DescribeAssociationExecutionTargetsInput{
						AssociationId: aws.String(assocID),
						ExecutionId:   aws.String(execID),
					})
				require.NoError(t, err)
				require.NotEmpty(t, targetsOut.AssociationExecutionTargets)

				_, err = client.DeleteAssociation(ctx, &ssmsdk.DeleteAssociationInput{
					AssociationId: aws.String(assocID),
				})
				require.NoError(t, err)

				_, err = client.DescribeAssociation(ctx, &ssmsdk.DescribeAssociationInput{
					AssociationId: aws.String(assocID),
				})
				require.Error(t, err, "DescribeAssociation after delete must fail")
			},
		},
		{
			name: "ops items related items",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				createOut, err := client.CreateOpsItem(ctx, &ssmsdk.CreateOpsItemInput{
					Title:       aws.String("s11-ops-item"),
					Source:      aws.String("gopherstack"),
					Description: aws.String("s11 typed slice 11 test ops item"),
				})
				require.NoError(t, err)

				assocOut, err := client.AssociateOpsItemRelatedItem(ctx, &ssmsdk.AssociateOpsItemRelatedItemInput{
					OpsItemId:       createOut.OpsItemId,
					AssociationType: aws.String("RelatesTo"),
					ResourceType:    aws.String("AWS::SSM::Document"),
					ResourceUri:     aws.String("arn:aws:ssm:us-east-1:000000000000:document/s11-doc"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(assocOut.AssociationId))

				listRelated, err := client.ListOpsItemRelatedItems(ctx, &ssmsdk.ListOpsItemRelatedItemsInput{
					OpsItemId: createOut.OpsItemId,
				})
				require.NoError(t, err)
				require.Len(t, listRelated.Summaries, 1)

				eventsOut, err := client.ListOpsItemEvents(ctx, &ssmsdk.ListOpsItemEventsInput{})
				require.NoError(t, err)
				assert.NotNil(t, eventsOut.Summaries)

				_, err = client.DisassociateOpsItemRelatedItem(ctx, &ssmsdk.DisassociateOpsItemRelatedItemInput{
					OpsItemId:     createOut.OpsItemId,
					AssociationId: assocOut.AssociationId,
				})
				require.NoError(t, err)

				afterList, err := client.ListOpsItemRelatedItems(ctx, &ssmsdk.ListOpsItemRelatedItemsInput{
					OpsItemId: createOut.OpsItemId,
				})
				require.NoError(t, err)
				assert.Empty(t, afterList.Summaries)
			},
		},
		{
			name: "ops metadata",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				createOut, err := client.CreateOpsMetadata(ctx, &ssmsdk.CreateOpsMetadataInput{
					ResourceId: aws.String("arn:aws:ssm:us-east-1:000000000000:resource/s11-resource"),
					Metadata: map[string]ssmtypes.MetadataValue{
						"key1": {Value: aws.String("value1")},
					},
				})
				require.NoError(t, err)

				getOut, err := client.GetOpsMetadata(ctx, &ssmsdk.GetOpsMetadataInput{
					OpsMetadataArn: createOut.OpsMetadataArn,
				})
				require.NoError(t, err)
				require.Contains(t, getOut.Metadata, "key1")
				assert.Equal(t, "value1", aws.ToString(getOut.Metadata["key1"].Value))

				_, err = client.UpdateOpsMetadata(ctx, &ssmsdk.UpdateOpsMetadataInput{
					OpsMetadataArn: createOut.OpsMetadataArn,
					MetadataToUpdate: map[string]ssmtypes.MetadataValue{
						"key2": {Value: aws.String("value2")},
					},
					KeysToDelete: []string{"key1"},
				})
				require.NoError(t, err)

				afterOut, err := client.GetOpsMetadata(ctx, &ssmsdk.GetOpsMetadataInput{
					OpsMetadataArn: createOut.OpsMetadataArn,
				})
				require.NoError(t, err)
				assert.NotContains(t, afterOut.Metadata, "key1")
				require.Contains(t, afterOut.Metadata, "key2")
				assert.Equal(t, "value2", aws.ToString(afterOut.Metadata["key2"].Value))

				_, err = client.DeleteOpsMetadata(ctx, &ssmsdk.DeleteOpsMetadataInput{
					OpsMetadataArn: createOut.OpsMetadataArn,
				})
				require.NoError(t, err)

				_, err = client.GetOpsMetadata(
					ctx,
					&ssmsdk.GetOpsMetadataInput{OpsMetadataArn: createOut.OpsMetadataArn},
				)
				require.Error(t, err, "GetOpsMetadata after delete must fail")
			},
		},
		{
			name: "cancel command",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				sendOut, err := client.SendCommand(ctx, &ssmsdk.SendCommandInput{
					DocumentName: aws.String("AWS-RunShellScript"),
					InstanceIds:  []string{"i-cancel-cmd"},
				})
				require.NoError(t, err)

				_, err = client.CancelCommand(ctx, &ssmsdk.CancelCommandInput{
					CommandId: sendOut.Command.CommandId,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "managed instances",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				actOut, err := client.CreateActivation(ctx, &ssmsdk.CreateActivationInput{
					IamRole: aws.String("s11-role"),
				})
				require.NoError(t, err)

				_, err = client.UpdateManagedInstanceRole(ctx, &ssmsdk.UpdateManagedInstanceRoleInput{
					InstanceId: actOut.ActivationId,
					IamRole:    aws.String("s11-role-updated"),
				})
				require.NoError(t, err)

				_, err = client.DeregisterManagedInstance(ctx, &ssmsdk.DeregisterManagedInstanceInput{
					InstanceId: actOut.ActivationId,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "resource data sync",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				_, err := client.CreateResourceDataSync(ctx, &ssmsdk.CreateResourceDataSyncInput{
					SyncName: aws.String("s11-sync"),
					S3Destination: &ssmtypes.ResourceDataSyncS3Destination{
						BucketName: aws.String("s11-bucket"),
						SyncFormat: ssmtypes.ResourceDataSyncS3FormatJsonSerde,
						Region:     aws.String("us-east-1"),
					},
				})
				require.NoError(t, err)

				_, err = client.UpdateResourceDataSync(ctx, &ssmsdk.UpdateResourceDataSyncInput{
					SyncName: aws.String("s11-sync"),
					SyncType: aws.String("SyncToDestination"),
					SyncSource: &ssmtypes.ResourceDataSyncSource{
						SourceType:    aws.String("SingleAccountMultiRegions"),
						SourceRegions: []string{"us-east-1"},
					},
				})
				require.NoError(t, err)

				listOut, err := client.ListResourceDataSync(ctx, &ssmsdk.ListResourceDataSyncInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listOut.ResourceDataSyncItems)

				_, err = client.DeleteResourceDataSync(ctx, &ssmsdk.DeleteResourceDataSyncInput{
					SyncName: aws.String("s11-sync"),
				})
				require.NoError(t, err)

				afterOut, err := client.ListResourceDataSync(ctx, &ssmsdk.ListResourceDataSyncInput{})
				require.NoError(t, err)
				assert.Empty(t, afterOut.ResourceDataSyncItems)
			},
		},
		{
			name: "cloud connectors",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				createOut, err := client.CreateCloudConnector(ctx, &ssmsdk.CreateCloudConnectorInput{
					ConfigConnectorArn: aws.String("arn:aws:config::000000000000:config-connector/s11-cc"),
					DisplayName:        aws.String("s11-connector"),
					RoleArn:            aws.String("arn:aws:iam::000000000000:role/s11-cloud-connector"),
					Configuration: &ssmtypes.CloudConnectorConfigurationMemberAzureConfiguration{
						Value: ssmtypes.AzureConfiguration{
							ApplicationId: aws.String("app-1"),
							TenantId:      aws.String("tenant-1"),
						},
					},
				})
				require.NoError(t, err)

				getOut, err := client.GetCloudConnector(ctx, &ssmsdk.GetCloudConnectorInput{
					CloudConnectorId: createOut.CloudConnectorId,
				})
				require.NoError(t, err)
				assert.Equal(t, "s11-connector", aws.ToString(getOut.DisplayName))

				listOut, err := client.ListCloudConnectors(ctx, &ssmsdk.ListCloudConnectorsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listOut.CloudConnectors)

				_, err = client.UpdateCloudConnector(ctx, &ssmsdk.UpdateCloudConnectorInput{
					CloudConnectorId: createOut.CloudConnectorId,
					DisplayName:      aws.String("s11-connector-renamed"),
				})
				require.NoError(t, err)

				afterOut, err := client.GetCloudConnector(ctx, &ssmsdk.GetCloudConnectorInput{
					CloudConnectorId: createOut.CloudConnectorId,
				})
				require.NoError(t, err)
				assert.Equal(t, "s11-connector-renamed", aws.ToString(afterOut.DisplayName))

				_, err = client.ValidateCloudConnector(ctx, &ssmsdk.ValidateCloudConnectorInput{
					CloudConnectorId: createOut.CloudConnectorId,
				})
				require.NoError(t, err)

				_, err = client.DeleteCloudConnector(ctx, &ssmsdk.DeleteCloudConnectorInput{
					CloudConnectorId: createOut.CloudConnectorId,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "compliance and inventory",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				listComplianceOut, err := client.ListComplianceSummaries(ctx, &ssmsdk.ListComplianceSummariesInput{})
				require.NoError(t, err)
				assert.NotNil(t, listComplianceOut.ComplianceSummaryItems)

				listResourceComplianceOut, err := client.ListResourceComplianceSummaries(
					ctx, &ssmsdk.ListResourceComplianceSummariesInput{})
				require.NoError(t, err)
				assert.NotNil(t, listResourceComplianceOut.ResourceComplianceSummaryItems)

				invOut, err := client.GetInventory(ctx, &ssmsdk.GetInventoryInput{})
				require.NoError(t, err)
				assert.NotNil(t, invOut.Entities)

				schemaOut, err := client.GetInventorySchema(ctx, &ssmsdk.GetInventorySchemaInput{})
				require.NoError(t, err)
				assert.NotNil(t, schemaOut.Schemas)
			},
		},
		{
			name: "document metadata",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name:    aws.String("s11-metadata-doc"),
					Content: aws.String(`{"schemaVersion":"2.2"}`),
				})
				require.NoError(t, err)

				_, err = client.UpdateDocumentMetadata(ctx, &ssmsdk.UpdateDocumentMetadataInput{
					Name: aws.String("s11-metadata-doc"),
					DocumentReviews: &ssmtypes.DocumentReviews{
						Action: ssmtypes.DocumentReviewActionApprove,
						Comment: []ssmtypes.DocumentReviewCommentSource{
							{Content: aws.String("looks good"), Type: ssmtypes.DocumentReviewCommentTypeComment},
						},
					},
				})
				require.NoError(t, err)

				histOut, err := client.ListDocumentMetadataHistory(ctx, &ssmsdk.ListDocumentMetadataHistoryInput{
					Name:     aws.String("s11-metadata-doc"),
					Metadata: ssmtypes.DocumentMetadataEnumDocumentReviews,
				})
				require.NoError(t, err)
				assert.NotNil(t, histOut.Metadata)
			},
		},
		{
			name: "automation step executions and signals",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend().WithAutomationExecDelay(time.Hour)
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				start, err := client.StartAutomationExecution(ctx, &ssmsdk.StartAutomationExecutionInput{
					DocumentName: aws.String("AWS-DoSomething"),
				})
				require.NoError(t, err)

				stepsOut, err := client.DescribeAutomationStepExecutions(
					ctx,
					&ssmsdk.DescribeAutomationStepExecutionsInput{
						AutomationExecutionId: start.AutomationExecutionId,
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, stepsOut.StepExecutions)

				// The execution is deliberately kept InProgress (WithAutomationExecDelay),
				// so signalling it exercises the real request/response decode without
				// racing a synchronous completion; a real, in-progress automation
				// accepts an Approve signal.
				_, err = client.SendAutomationSignal(ctx, &ssmsdk.SendAutomationSignalInput{
					AutomationExecutionId: start.AutomationExecutionId,
					SignalType:            ssmtypes.SignalTypeApprove,
				})
				require.NoError(t, err)

				previewStart, err := client.StartExecutionPreview(ctx, &ssmsdk.StartExecutionPreviewInput{
					DocumentName: aws.String("AWS-DoSomething"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(previewStart.ExecutionPreviewId))

				previewGet, err := client.GetExecutionPreview(ctx, &ssmsdk.GetExecutionPreviewInput{
					ExecutionPreviewId: previewStart.ExecutionPreviewId,
				})
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(previewStart.ExecutionPreviewId),
					aws.ToString(previewGet.ExecutionPreviewId),
				)
			},
		},
		{
			name: "sessions",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				startOut, err := client.StartSession(ctx, &ssmsdk.StartSessionInput{Target: aws.String("i-session-1")})
				require.NoError(t, err)

				connOut, err := client.GetConnectionStatus(ctx, &ssmsdk.GetConnectionStatusInput{
					Target: aws.String("i-session-1"),
				})
				require.NoError(t, err)
				assert.Equal(t, ssmtypes.ConnectionStatusConnected, connOut.Status)

				_, err = client.ResumeSession(ctx, &ssmsdk.ResumeSessionInput{SessionId: startOut.SessionId})
				require.NoError(t, err)

				_, err = client.TerminateSession(ctx, &ssmsdk.TerminateSessionInput{SessionId: startOut.SessionId})
				require.NoError(t, err)

				afterConnOut, err := client.GetConnectionStatus(ctx, &ssmsdk.GetConnectionStatusInput{
					Target: aws.String("i-session-1"),
				})
				require.NoError(t, err)
				assert.Equal(t, ssmtypes.ConnectionStatusNotConnected, afterConnOut.Status)
			},
		},
		{
			name: "start change request execution",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				out, err := client.StartChangeRequestExecution(ctx, &ssmsdk.StartChangeRequestExecutionInput{
					DocumentName: aws.String("AWS-ChangeRequest"),
					Runbooks: []ssmtypes.Runbook{
						{DocumentName: aws.String("AWS-RunShellScript"), MaxConcurrency: aws.String("1")},
					},
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(out.AutomationExecutionId))
			},
		},
		{
			name: "access token",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				startOut, err := client.StartAccessRequest(ctx, &ssmsdk.StartAccessRequestInput{
					Reason: aws.String("s11 incident"),
					Targets: []ssmtypes.Target{
						{Key: aws.String("InstanceIds"), Values: []string{"i-access-1"}},
					},
				})
				require.NoError(t, err)

				tokenOut, err := client.GetAccessToken(ctx, &ssmsdk.GetAccessTokenInput{
					AccessRequestId: startOut.AccessRequestId,
				})
				require.NoError(t, err)
				require.NotNil(t, tokenOut.Credentials)
				assert.NotEmpty(t, aws.ToString(tokenOut.Credentials.AccessKeyId))
			},
		},
		{
			name: "calendar state",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name:         aws.String("s11-calendar"),
					DocumentType: ssmtypes.DocumentTypeChangeCalendar,
					Content: aws.String(`{"schemaVersion":"1.0","description":"cal",` +
						`"changeCalendar":{"currentState":"OPEN","createdTime":"2024-01-01T00:00:00.000Z"}}`),
				})
				require.NoError(t, err)

				out, err := client.GetCalendarState(ctx, &ssmsdk.GetCalendarStateInput{
					CalendarNames: []string{"s11-calendar"},
				})
				require.NoError(t, err)
				assert.NotEmpty(t, out.State)
			},
		},
		{
			name: "ops summary",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				out, err := client.GetOpsSummary(ctx, &ssmsdk.GetOpsSummaryInput{})
				require.NoError(t, err)
				assert.NotNil(t, out.Entities)
			},
		},
		{
			name: "reset service setting",
			run: func(t *testing.T) {
				t.Helper()

				backend := ssm.NewInMemoryBackend()
				client := newTestSSMClient(t, ssm.NewHandler(backend))
				ctx := t.Context()

				settingID := "arn:aws:ssm:us-east-1:000000000000:servicesetting/ssm/parameter-store/high-throughput-enabled"

				_, err := client.UpdateServiceSetting(ctx, &ssmsdk.UpdateServiceSettingInput{
					SettingId:    aws.String(settingID),
					SettingValue: aws.String("true"),
				})
				require.NoError(t, err)

				out, err := client.ResetServiceSetting(ctx, &ssmsdk.ResetServiceSettingInput{
					SettingId: aws.String(settingID),
				})
				require.NoError(t, err)
				require.NotNil(t, out.ServiceSetting)
				assert.Equal(t, settingID, aws.ToString(out.ServiceSetting.SettingId))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
