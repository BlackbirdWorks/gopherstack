package ssm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func enumStrings[T ~string](vs []T) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = string(v)
	}

	return out
}

func TestSDK_EnumInputValidation(t *testing.T) {
	t.Parallel()

	const (
		valErr  = "ValidationException"
		permErr = "InvalidPermissionType"
	)

	type callFn func(ctx context.Context, c *ssmsdk.Client, v string) error

	tests := []struct {
		call   callFn
		name   string
		field  string
		code   string
		values []string
	}{
		{name: "CreateAssociation.ComplianceSeverity", field: "ComplianceSeverity", code: valErr,
			values: enumStrings(ssmtypes.AssociationComplianceSeverity("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
					Name: aws.String(
						"AWS-RunShellScript",
					), ComplianceSeverity: ssmtypes.AssociationComplianceSeverity(v),
				})

				return err
			}},
		{name: "CreateAssociation.SyncCompliance", field: "SyncCompliance", code: valErr,
			values: enumStrings(ssmtypes.AssociationSyncCompliance("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
					Name: aws.String("AWS-RunShellScript"), SyncCompliance: ssmtypes.AssociationSyncCompliance(v),
				})

				return err
			}},
		{name: "UpdateAssociation.ComplianceSeverity", field: "ComplianceSeverity", code: valErr,
			values: enumStrings(ssmtypes.AssociationComplianceSeverity("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdateAssociation(ctx, &ssmsdk.UpdateAssociationInput{
					AssociationId: aws.String("a"), ComplianceSeverity: ssmtypes.AssociationComplianceSeverity(v),
				})

				return err
			}},
		{name: "UpdateAssociation.SyncCompliance", field: "SyncCompliance", code: valErr,
			values: enumStrings(ssmtypes.AssociationSyncCompliance("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdateAssociation(ctx, &ssmsdk.UpdateAssociationInput{
					AssociationId: aws.String("a"), SyncCompliance: ssmtypes.AssociationSyncCompliance(v),
				})

				return err
			}},
		{name: "CreateDocument.DocumentFormat", field: "DocumentFormat", code: valErr,
			values: enumStrings(ssmtypes.DocumentFormat("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name: aws.String("d"), Content: aws.String("{}"), DocumentFormat: ssmtypes.DocumentFormat(v),
				})

				return err
			}},
		{name: "CreateDocument.DocumentType", field: "DocumentType", code: valErr,
			values: enumStrings(ssmtypes.DocumentType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name: aws.String("d"), Content: aws.String("{}"), DocumentType: ssmtypes.DocumentType(v),
				})

				return err
			}},
		{name: "GetDocument.DocumentFormat", field: "DocumentFormat", code: valErr,
			values: enumStrings(ssmtypes.DocumentFormat("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.GetDocument(ctx, &ssmsdk.GetDocumentInput{
					Name: aws.String("d"), DocumentFormat: ssmtypes.DocumentFormat(v),
				})

				return err
			}},
		{name: "UpdateDocument.DocumentFormat", field: "DocumentFormat", code: valErr,
			values: enumStrings(ssmtypes.DocumentFormat("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
					Name: aws.String("d"), Content: aws.String("{}"), DocumentFormat: ssmtypes.DocumentFormat(v),
				})

				return err
			}},
		{name: "DescribeDocumentPermission.PermissionType", field: "PermissionType", code: permErr,
			values: enumStrings(ssmtypes.DocumentPermissionType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribeDocumentPermission(ctx, &ssmsdk.DescribeDocumentPermissionInput{
					Name: aws.String("d"), PermissionType: ssmtypes.DocumentPermissionType(v),
				})

				return err
			}},
		{name: "ModifyDocumentPermission.PermissionType", field: "PermissionType", code: permErr,
			values: enumStrings(ssmtypes.DocumentPermissionType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.ModifyDocumentPermission(ctx, &ssmsdk.ModifyDocumentPermissionInput{
					Name: aws.String("d"), PermissionType: ssmtypes.DocumentPermissionType(v),
				})

				return err
			}},
		{name: "ListDocumentMetadataHistory.Metadata", field: "Metadata", code: valErr,
			values: enumStrings(ssmtypes.DocumentMetadataEnum("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.ListDocumentMetadataHistory(ctx, &ssmsdk.ListDocumentMetadataHistoryInput{
					Name: aws.String("d"), Metadata: ssmtypes.DocumentMetadataEnum(v),
				})

				return err
			}},
		{name: "CreatePatchBaseline.ApprovedPatchesComplianceLevel", field: "ApprovedPatchesComplianceLevel",
			code: valErr, values: enumStrings(ssmtypes.PatchComplianceLevel("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
					Name: aws.String("b"), ApprovedPatchesComplianceLevel: ssmtypes.PatchComplianceLevel(v),
				})

				return err
			}},
		{name: "CreatePatchBaseline.AvailableSecurityUpdatesComplianceStatus",
			field: "AvailableSecurityUpdatesComplianceStatus", code: valErr,
			values: enumStrings(ssmtypes.PatchComplianceStatus("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
					Name: aws.String("b"), AvailableSecurityUpdatesComplianceStatus: ssmtypes.PatchComplianceStatus(v),
				})

				return err
			}},
		{name: "CreatePatchBaseline.OperatingSystem", field: "OperatingSystem", code: valErr,
			values: enumStrings(ssmtypes.OperatingSystem("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
					Name: aws.String("b"), OperatingSystem: ssmtypes.OperatingSystem(v),
				})

				return err
			}},
		{name: "CreatePatchBaseline.RejectedPatchesAction", field: "RejectedPatchesAction", code: valErr,
			values: enumStrings(ssmtypes.PatchAction("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
					Name: aws.String("b"), RejectedPatchesAction: ssmtypes.PatchAction(v),
				})

				return err
			}},
		{name: "UpdatePatchBaseline.ApprovedPatchesComplianceLevel", field: "ApprovedPatchesComplianceLevel",
			code: valErr, values: enumStrings(ssmtypes.PatchComplianceLevel("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdatePatchBaseline(ctx, &ssmsdk.UpdatePatchBaselineInput{
					BaselineId: aws.String("pb-1"), ApprovedPatchesComplianceLevel: ssmtypes.PatchComplianceLevel(v),
				})

				return err
			}},
		{name: "UpdatePatchBaseline.AvailableSecurityUpdatesComplianceStatus",
			field: "AvailableSecurityUpdatesComplianceStatus", code: valErr,
			values: enumStrings(ssmtypes.PatchComplianceStatus("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdatePatchBaseline(ctx, &ssmsdk.UpdatePatchBaselineInput{
					BaselineId: aws.String(
						"pb-1",
					), AvailableSecurityUpdatesComplianceStatus: ssmtypes.PatchComplianceStatus(v),
				})

				return err
			}},
		{name: "UpdatePatchBaseline.RejectedPatchesAction", field: "RejectedPatchesAction", code: valErr,
			values: enumStrings(ssmtypes.PatchAction("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdatePatchBaseline(ctx, &ssmsdk.UpdatePatchBaselineInput{
					BaselineId: aws.String("pb-1"), RejectedPatchesAction: ssmtypes.PatchAction(v),
				})

				return err
			}},
		{name: "DescribePatchProperties.OperatingSystem", field: "OperatingSystem", code: valErr,
			values: enumStrings(ssmtypes.OperatingSystem("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribePatchProperties(ctx, &ssmsdk.DescribePatchPropertiesInput{
					OperatingSystem: ssmtypes.OperatingSystem(v), Property: ssmtypes.PatchPropertyProduct,
				})

				return err
			}},
		{name: "DescribePatchProperties.PatchSet", field: "PatchSet", code: valErr,
			values: enumStrings(ssmtypes.PatchSet("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribePatchProperties(ctx, &ssmsdk.DescribePatchPropertiesInput{
					OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2, Property: ssmtypes.PatchPropertyProduct,
					PatchSet: ssmtypes.PatchSet(v),
				})

				return err
			}},
		{name: "DescribePatchProperties.Property", field: "Property", code: valErr,
			values: enumStrings(ssmtypes.PatchProperty("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribePatchProperties(ctx, &ssmsdk.DescribePatchPropertiesInput{
					OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2, Property: ssmtypes.PatchProperty(v),
				})

				return err
			}},
		{name: "GetDefaultPatchBaseline.OperatingSystem", field: "OperatingSystem", code: valErr,
			values: enumStrings(ssmtypes.OperatingSystem("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.GetDefaultPatchBaseline(ctx, &ssmsdk.GetDefaultPatchBaselineInput{
					OperatingSystem: ssmtypes.OperatingSystem(v),
				})

				return err
			}},
		{name: "GetPatchBaselineForPatchGroup.OperatingSystem", field: "OperatingSystem", code: valErr,
			values: enumStrings(ssmtypes.OperatingSystem("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.GetPatchBaselineForPatchGroup(ctx, &ssmsdk.GetPatchBaselineForPatchGroupInput{
					PatchGroup: aws.String("g"), OperatingSystem: ssmtypes.OperatingSystem(v),
				})

				return err
			}},
		{name: "DeleteInventory.SchemaDeleteOption", field: "SchemaDeleteOption", code: "InvalidOptionException",
			values: enumStrings(ssmtypes.InventorySchemaDeleteOption("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DeleteInventory(ctx, &ssmsdk.DeleteInventoryInput{
					TypeName: aws.String("Custom:X"), SchemaDeleteOption: ssmtypes.InventorySchemaDeleteOption(v),
				})

				return err
			}},
		{name: "DescribeMaintenanceWindowSchedule.ResourceType", field: "ResourceType", code: valErr,
			values: enumStrings(ssmtypes.MaintenanceWindowResourceType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribeMaintenanceWindowSchedule(ctx, &ssmsdk.DescribeMaintenanceWindowScheduleInput{
					ResourceType: ssmtypes.MaintenanceWindowResourceType(v),
				})

				return err
			}},
		{name: "DescribeMaintenanceWindowsForTarget.ResourceType", field: "ResourceType", code: valErr,
			values: enumStrings(ssmtypes.MaintenanceWindowResourceType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribeMaintenanceWindowsForTarget(ctx, &ssmsdk.DescribeMaintenanceWindowsForTargetInput{
					ResourceType: ssmtypes.MaintenanceWindowResourceType(v),
					Targets:      []ssmtypes.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-1"}}},
				})

				return err
			}},
		{name: "RegisterTargetWithMaintenanceWindow.ResourceType", field: "ResourceType", code: valErr,
			values: enumStrings(ssmtypes.MaintenanceWindowResourceType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.RegisterTargetWithMaintenanceWindow(ctx, &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
					WindowId: aws.String("mw-1"), ResourceType: ssmtypes.MaintenanceWindowResourceType(v),
					Targets: []ssmtypes.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-1"}}},
				})

				return err
			}},
		{name: "RegisterTaskWithMaintenanceWindow.CutoffBehavior", field: "CutoffBehavior", code: valErr,
			values: enumStrings(ssmtypes.MaintenanceWindowTaskCutoffBehavior("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
					WindowId: aws.String("mw-1"), TaskArn: aws.String("AWS-RunShellScript"),
					TaskType:       ssmtypes.MaintenanceWindowTaskTypeRunCommand,
					CutoffBehavior: ssmtypes.MaintenanceWindowTaskCutoffBehavior(v),
				})

				return err
			}},
		{name: "RegisterTaskWithMaintenanceWindow.TaskType", field: "TaskType", code: valErr,
			values: enumStrings(ssmtypes.MaintenanceWindowTaskType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
					WindowId: aws.String("mw-1"), TaskArn: aws.String("AWS-RunShellScript"),
					TaskType: ssmtypes.MaintenanceWindowTaskType(v),
				})

				return err
			}},
		{name: "UpdateMaintenanceWindowTask.CutoffBehavior", field: "CutoffBehavior", code: valErr,
			values: enumStrings(ssmtypes.MaintenanceWindowTaskCutoffBehavior("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdateMaintenanceWindowTask(ctx, &ssmsdk.UpdateMaintenanceWindowTaskInput{
					WindowId: aws.String("mw-1"), WindowTaskId: aws.String("t-1"),
					CutoffBehavior: ssmtypes.MaintenanceWindowTaskCutoffBehavior(v),
				})

				return err
			}},
		{name: "DescribeSessions.State", field: "State", code: valErr,
			values: enumStrings(ssmtypes.SessionState("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.DescribeSessions(ctx, &ssmsdk.DescribeSessionsInput{State: ssmtypes.SessionState(v)})

				return err
			}},
		{name: "PutComplianceItems.UploadType", field: "UploadType", code: valErr,
			values: enumStrings(ssmtypes.ComplianceUploadType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.PutComplianceItems(ctx, &ssmsdk.PutComplianceItemsInput{
					ResourceId: aws.String("i-1"), ResourceType: aws.String("ManagedInstance"),
					ComplianceType: aws.String("Custom:X"), UploadType: ssmtypes.ComplianceUploadType(v),
					ExecutionSummary: &ssmtypes.ComplianceExecutionSummary{
						ExecutionTime: aws.Time(time.Unix(1700000000, 0)),
					},
					Items: []ssmtypes.ComplianceItemEntry{{
						Id: aws.String("1"), Severity: ssmtypes.ComplianceSeverityLow,
						Status: ssmtypes.ComplianceStatusCompliant,
					}},
				})

				return err
			}},
		{name: "PutParameter.Tier", field: "Tier", code: valErr,
			values: enumStrings(ssmtypes.ParameterTier("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.PutParameter(ctx, &ssmsdk.PutParameterInput{
					Name: aws.String("/p"), Value: aws.String("v"), Type: ssmtypes.ParameterTypeString,
					Tier: ssmtypes.ParameterTier(v), Overwrite: aws.Bool(true),
				})

				return err
			}},
		{name: "PutParameter.Type", field: "Type", code: "UnsupportedParameterType",
			values: enumStrings(ssmtypes.ParameterType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.PutParameter(ctx, &ssmsdk.PutParameterInput{
					Name: aws.String("/p"), Value: aws.String("v"), Type: ssmtypes.ParameterType(v),
					Overwrite: aws.Bool(true),
				})

				return err
			}},
		{name: "SendAutomationSignal.SignalType", field: "SignalType", code: valErr,
			values: enumStrings(ssmtypes.SignalType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.SendAutomationSignal(ctx, &ssmsdk.SendAutomationSignalInput{
					AutomationExecutionId: aws.String("e"), SignalType: ssmtypes.SignalType(v),
				})

				return err
			}},
		{name: "SendCommand.DocumentHashType", field: "DocumentHashType", code: valErr,
			values: enumStrings(ssmtypes.DocumentHashType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.SendCommand(ctx, &ssmsdk.SendCommandInput{
					DocumentName: aws.String("AWS-RunShellScript"), InstanceIds: []string{"i-1"},
					DocumentHashType: ssmtypes.DocumentHashType(v),
				})

				return err
			}},
		{name: "StartAutomationExecution.Mode", field: "Mode", code: valErr,
			values: enumStrings(ssmtypes.ExecutionMode("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.StartAutomationExecution(ctx, &ssmsdk.StartAutomationExecutionInput{
					DocumentName: aws.String("d"), Mode: ssmtypes.ExecutionMode(v),
				})

				return err
			}},
		{name: "StopAutomationExecution.Type", field: "Type", code: valErr,
			values: enumStrings(ssmtypes.StopType("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.StopAutomationExecution(ctx, &ssmsdk.StopAutomationExecutionInput{
					AutomationExecutionId: aws.String("e"), Type: ssmtypes.StopType(v),
				})

				return err
			}},
		{name: "UpdateOpsItem.Status", field: "Status", code: valErr,
			values: enumStrings(ssmtypes.OpsItemStatus("").Values()),
			call: func(ctx context.Context, c *ssmsdk.Client, v string) error {
				_, err := c.UpdateOpsItem(ctx, &ssmsdk.UpdateOpsItemInput{
					OpsItemId: aws.String("oi-1"), Status: ssmtypes.OpsItemStatus(v),
				})

				return err
			}},
	}

	base := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	client := ssmsdk.New(base.Options(), func(o *ssmsdk.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			_, _ = s.Initialize.Remove("OperationInputValidation")

			return nil
		})
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, tt.values)

			t.Run("invalid", func(t *testing.T) {
				t.Parallel()

				err := tt.call(t.Context(), client, "NOT_A_REAL_VALUE")

				apiErr, ok := errors.AsType[smithy.APIError](err)
				require.True(t, ok, "got %v", err)
				assert.Equal(t, tt.code, apiErr.ErrorCode())
				assert.Contains(t, apiErr.ErrorMessage(), "invalid "+tt.field)
			})

			t.Run("every sdk value accepted", func(t *testing.T) {
				t.Parallel()

				for _, v := range tt.values {
					err := tt.call(t.Context(), client, v)

					if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
						assert.NotContains(t, apiErr.ErrorMessage(), "invalid "+tt.field,
							"%s rejected sdk value %q: %v", tt.field, v, err)
					}
				}
			})
		})
	}
}
