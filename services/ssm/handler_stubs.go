package ssm

import (
	"context"
	"encoding/json"
)

// handler_stubs.go wires the 123 stub backend methods into the HTTP dispatch table.

//nolint:funlen,gocognit,gocyclo,cyclop // This function is intentionally long — it registers all stub operations.
func (h *Handler) ssmStubOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"CreateResourceDataSync": func(ctx context.Context, b []byte) (any, error) {
			var input CreateResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateResourceDataSync(ctx, &input)
		},
		"DeleteActivation": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteActivationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteActivation(ctx, &input)
		},
		"DeleteAssociation": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteAssociation(ctx, &input)
		},
		"DeleteInventory": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteInventoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteInventory(ctx, &input)
		},
		"DeleteMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteMaintenanceWindow(ctx, &input)
		},
		"DeleteOpsItem": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteOpsItem(ctx, &input)
		},
		"DeleteOpsMetadata": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteOpsMetadata(ctx, &input)
		},
		"DeletePatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input DeletePatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeletePatchBaseline(ctx, &input)
		},
		"DeleteResourceDataSync": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteResourceDataSync(ctx, &input)
		},
		"DeleteResourcePolicy": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteResourcePolicy(ctx, &input)
		},
		"DeregisterManagedInstance": func(ctx context.Context, b []byte) (any, error) {
			var input DeregisterManagedInstanceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterManagedInstance(ctx, &input)
		},
		"DeregisterPatchBaselineForPatchGroup": func(ctx context.Context, b []byte) (any, error) {
			var input DeregisterPatchBaselineForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterPatchBaselineForPatchGroup(ctx, &input)
		},
		"DeregisterTargetFromMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input DeregisterTargetFromMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterTargetFromMaintenanceWindow(ctx, &input)
		},
		"DeregisterTaskFromMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input DeregisterTaskFromMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterTaskFromMaintenanceWindow(ctx, &input)
		},
		"DescribeActivations": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeActivationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeActivations(ctx, &input)
		},
		"DescribeAssociation": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAssociation(ctx, &input)
		},
		"DescribeAssociationExecutionTargets": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeAssociationExecutionTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAssociationExecutionTargets(ctx, &input)
		},
		"DescribeAssociationExecutions": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeAssociationExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAssociationExecutions(ctx, &input)
		},
		"DescribeAutomationExecutions": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeAutomationExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAutomationExecutions(ctx, &input)
		},
		"DescribeAutomationStepExecutions": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeAutomationStepExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAutomationStepExecutions(ctx, &input)
		},
		"DescribeAvailablePatches": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeAvailablePatchesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAvailablePatches(ctx, &input)
		},
		"DescribeEffectiveInstanceAssociations": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeEffectiveInstanceAssociationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEffectiveInstanceAssociations(ctx, &input)
		},
		"DescribeEffectivePatchesForPatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeEffectivePatchesForPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEffectivePatchesForPatchBaseline(ctx, &input)
		},
		"DescribeInstanceAssociationsStatus": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInstanceAssociationsStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstanceAssociationsStatus(ctx, &input)
		},
		"DescribeInstanceInformation": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInstanceInformationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstanceInformation(ctx, &input)
		},
		"DescribeInstancePatchStates": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInstancePatchStatesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstancePatchStates(ctx, &input)
		},
		"DescribeInstancePatchStatesForPatchGroup": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInstancePatchStatesForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstancePatchStatesForPatchGroup(ctx, &input)
		},
		"DescribeInstancePatches": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInstancePatchesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstancePatches(ctx, &input)
		},
		"DescribeInstanceProperties": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInstancePropertiesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstanceProperties(ctx, &input)
		},
		"DescribeInventoryDeletions": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeInventoryDeletionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInventoryDeletions(ctx, &input)
		},
		"DescribeMaintenanceWindowExecutionTaskInvocations": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowExecutionTaskInvocationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowExecutionTaskInvocations(ctx, &input)
		},
		"DescribeMaintenanceWindowExecutionTasks": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowExecutionTasksInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowExecutionTasks(ctx, &input)
		},
		"DescribeMaintenanceWindowExecutions": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowExecutions(ctx, &input)
		},
		"DescribeMaintenanceWindowSchedule": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowScheduleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowSchedule(ctx, &input)
		},
		"DescribeMaintenanceWindowTargets": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowTargets(ctx, &input)
		},
		"DescribeMaintenanceWindowTasks": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowTasksInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowTasks(ctx, &input)
		},
		"DescribeMaintenanceWindows": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindows(ctx, &input)
		},
		"DescribeMaintenanceWindowsForTarget": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeMaintenanceWindowsForTargetInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowsForTarget(ctx, &input)
		},
		"DescribeOpsItems": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeOpsItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeOpsItems(ctx, &input)
		},
		"DescribePatchBaselines": func(ctx context.Context, b []byte) (any, error) {
			var input DescribePatchBaselinesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchBaselines(ctx, &input)
		},
		"DescribePatchGroupState": func(ctx context.Context, b []byte) (any, error) {
			var input DescribePatchGroupStateInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchGroupState(ctx, &input)
		},
		"DescribePatchGroups": func(ctx context.Context, b []byte) (any, error) {
			var input DescribePatchGroupsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchGroups(ctx, &input)
		},
		"DescribePatchProperties": func(ctx context.Context, b []byte) (any, error) {
			var input DescribePatchPropertiesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchProperties(ctx, &input)
		},
		"DescribeSessions": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeSessionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSessions(ctx, &input)
		},
		"DisassociateOpsItemRelatedItem": func(ctx context.Context, b []byte) (any, error) {
			var input DisassociateOpsItemRelatedItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DisassociateOpsItemRelatedItem(ctx, &input)
		},
		"GetAccessToken": func(ctx context.Context, b []byte) (any, error) {
			var input GetAccessTokenInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetAccessToken(ctx, &input)
		},
		"GetAutomationExecution": func(ctx context.Context, b []byte) (any, error) {
			var input GetAutomationExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetAutomationExecution(ctx, &input)
		},
		"GetCalendarState": func(ctx context.Context, b []byte) (any, error) {
			var input GetCalendarStateInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetCalendarState(ctx, &input)
		},
		"GetConnectionStatus": func(ctx context.Context, b []byte) (any, error) {
			var input GetConnectionStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetConnectionStatus(ctx, &input)
		},
		"GetDefaultPatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input GetDefaultPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetDefaultPatchBaseline(ctx, &input)
		},
		"GetDeployablePatchSnapshotForInstance": func(ctx context.Context, b []byte) (any, error) {
			var input GetDeployablePatchSnapshotForInstanceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetDeployablePatchSnapshotForInstance(ctx, &input)
		},
		"GetExecutionPreview": func(ctx context.Context, b []byte) (any, error) {
			var input GetExecutionPreviewInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetExecutionPreview(ctx, &input)
		},
		"GetInventory": func(ctx context.Context, b []byte) (any, error) {
			var input GetInventoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetInventory(ctx, &input)
		},
		"GetInventorySchema": func(ctx context.Context, b []byte) (any, error) {
			var input GetInventorySchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetInventorySchema(ctx, &input)
		},
		"GetMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input GetMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindow(ctx, &input)
		},
		"GetMaintenanceWindowExecution": func(ctx context.Context, b []byte) (any, error) {
			var input GetMaintenanceWindowExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowExecution(ctx, &input)
		},
		"GetMaintenanceWindowExecutionTask": func(ctx context.Context, b []byte) (any, error) {
			var input GetMaintenanceWindowExecutionTaskInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowExecutionTask(ctx, &input)
		},
		"GetMaintenanceWindowExecutionTaskInvocation": func(ctx context.Context, b []byte) (any, error) {
			var input GetMaintenanceWindowExecutionTaskInvocationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowExecutionTaskInvocation(ctx, &input)
		},
		"GetMaintenanceWindowTask": func(ctx context.Context, b []byte) (any, error) {
			var input GetMaintenanceWindowTaskInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowTask(ctx, &input)
		},
		"GetOpsItem": func(ctx context.Context, b []byte) (any, error) {
			var input GetOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetOpsItem(ctx, &input)
		},
		"GetOpsMetadata": func(ctx context.Context, b []byte) (any, error) {
			var input GetOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetOpsMetadata(ctx, &input)
		},
		"GetOpsSummary": func(ctx context.Context, b []byte) (any, error) {
			var input GetOpsSummaryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetOpsSummary(ctx, &input)
		},
		"GetPatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input GetPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPatchBaseline(ctx, &input)
		},
		"GetPatchBaselineForPatchGroup": func(ctx context.Context, b []byte) (any, error) {
			var input GetPatchBaselineForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPatchBaselineForPatchGroup(ctx, &input)
		},
		"GetResourcePolicies": func(ctx context.Context, b []byte) (any, error) {
			var input GetResourcePoliciesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetResourcePolicies(ctx, &input)
		},
		"GetServiceSetting": func(ctx context.Context, b []byte) (any, error) {
			var input GetServiceSettingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetServiceSetting(ctx, &input)
		},
		"LabelParameterVersion": func(ctx context.Context, b []byte) (any, error) {
			var input LabelParameterVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.LabelParameterVersion(ctx, &input)
		},
		"ListAssociationVersions": func(ctx context.Context, b []byte) (any, error) {
			var input ListAssociationVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAssociationVersions(ctx, &input)
		},
		"ListAssociations": func(ctx context.Context, b []byte) (any, error) {
			var input ListAssociationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAssociations(ctx, &input)
		},
		"ListComplianceItems": func(ctx context.Context, b []byte) (any, error) {
			var input ListComplianceItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListComplianceItems(ctx, &input)
		},
		"ListComplianceSummaries": func(ctx context.Context, b []byte) (any, error) {
			var input ListComplianceSummariesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListComplianceSummaries(ctx, &input)
		},
		"ListDocumentMetadataHistory": func(ctx context.Context, b []byte) (any, error) {
			var input ListDocumentMetadataHistoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListDocumentMetadataHistory(ctx, &input)
		},
		"ListInventoryEntries": func(ctx context.Context, b []byte) (any, error) {
			var input ListInventoryEntriesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListInventoryEntries(ctx, &input)
		},
		"ListNodes": func(ctx context.Context, b []byte) (any, error) {
			var input ListNodesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListNodes(ctx, &input)
		},
		"ListNodesSummary": func(ctx context.Context, b []byte) (any, error) {
			var input ListNodesSummaryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListNodesSummary(ctx, &input)
		},
		"ListOpsItemEvents": func(ctx context.Context, b []byte) (any, error) {
			var input ListOpsItemEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListOpsItemEvents(ctx, &input)
		},
		"ListOpsItemRelatedItems": func(ctx context.Context, b []byte) (any, error) {
			var input ListOpsItemRelatedItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListOpsItemRelatedItems(ctx, &input)
		},
		"ListOpsMetadata": func(ctx context.Context, b []byte) (any, error) {
			var input ListOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListOpsMetadata(ctx, &input)
		},
		"ListResourceComplianceSummaries": func(ctx context.Context, b []byte) (any, error) {
			var input ListResourceComplianceSummariesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListResourceComplianceSummaries(ctx, &input)
		},
		"ListResourceDataSync": func(ctx context.Context, b []byte) (any, error) {
			var input ListResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListResourceDataSync(ctx, &input)
		},
		"PutComplianceItems": func(ctx context.Context, b []byte) (any, error) {
			var input PutComplianceItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutComplianceItems(ctx, &input)
		},
		"PutInventory": func(ctx context.Context, b []byte) (any, error) {
			var input PutInventoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutInventory(ctx, &input)
		},
		"PutResourcePolicy": func(ctx context.Context, b []byte) (any, error) {
			var input PutResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutResourcePolicy(ctx, &input)
		},
		"RegisterDefaultPatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input RegisterDefaultPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterDefaultPatchBaseline(ctx, &input)
		},
		"RegisterPatchBaselineForPatchGroup": func(ctx context.Context, b []byte) (any, error) {
			var input RegisterPatchBaselineForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterPatchBaselineForPatchGroup(ctx, &input)
		},
		"RegisterTargetWithMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input RegisterTargetWithMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterTargetWithMaintenanceWindow(ctx, &input)
		},
		"RegisterTaskWithMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input RegisterTaskWithMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterTaskWithMaintenanceWindow(ctx, &input)
		},
		"ResetServiceSetting": func(ctx context.Context, b []byte) (any, error) {
			var input ResetServiceSettingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ResetServiceSetting(ctx, &input)
		},
		"ResumeSession": func(ctx context.Context, b []byte) (any, error) {
			var input ResumeSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ResumeSession(ctx, &input)
		},
		"SendAutomationSignal": func(ctx context.Context, b []byte) (any, error) {
			var input SendAutomationSignalInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.SendAutomationSignal(ctx, &input)
		},
		"StartAccessRequest": func(ctx context.Context, b []byte) (any, error) {
			var input StartAccessRequestInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartAccessRequest(ctx, &input)
		},
		"StartAssociationsOnce": func(ctx context.Context, b []byte) (any, error) {
			var input StartAssociationsOnceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartAssociationsOnce(ctx, &input)
		},
		"StartAutomationExecution": func(ctx context.Context, b []byte) (any, error) {
			var input StartAutomationExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartAutomationExecution(ctx, &input)
		},
		"StartChangeRequestExecution": func(ctx context.Context, b []byte) (any, error) {
			var input StartChangeRequestExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartChangeRequestExecution(ctx, &input)
		},
		"StartExecutionPreview": func(ctx context.Context, b []byte) (any, error) {
			var input StartExecutionPreviewInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartExecutionPreview(ctx, &input)
		},
		"StartSession": func(ctx context.Context, b []byte) (any, error) {
			var input StartSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartSession(ctx, &input)
		},
		"StopAutomationExecution": func(ctx context.Context, b []byte) (any, error) {
			var input StopAutomationExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StopAutomationExecution(ctx, &input)
		},
		"TerminateSession": func(ctx context.Context, b []byte) (any, error) {
			var input TerminateSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.TerminateSession(ctx, &input)
		},
		"UnlabelParameterVersion": func(ctx context.Context, b []byte) (any, error) {
			var input UnlabelParameterVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UnlabelParameterVersion(ctx, &input)
		},
		"UpdateAssociation": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateAssociation(ctx, &input)
		},
		"UpdateAssociationStatus": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateAssociationStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateAssociationStatus(ctx, &input)
		},
		"UpdateDocumentDefaultVersion": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateDocumentDefaultVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateDocumentDefaultVersion(ctx, &input)
		},
		"UpdateDocumentMetadata": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateDocumentMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateDocumentMetadata(ctx, &input)
		},
		"UpdateMaintenanceWindow": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMaintenanceWindow(ctx, &input)
		},
		"UpdateMaintenanceWindowTarget": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateMaintenanceWindowTargetInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMaintenanceWindowTarget(ctx, &input)
		},
		"UpdateMaintenanceWindowTask": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateMaintenanceWindowTaskInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMaintenanceWindowTask(ctx, &input)
		},
		"UpdateManagedInstanceRole": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateManagedInstanceRoleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateManagedInstanceRole(ctx, &input)
		},
		"UpdateOpsItem": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateOpsItem(ctx, &input)
		},
		"UpdateOpsMetadata": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateOpsMetadata(ctx, &input)
		},
		"UpdatePatchBaseline": func(ctx context.Context, b []byte) (any, error) {
			var input UpdatePatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdatePatchBaseline(ctx, &input)
		},
		"UpdateResourceDataSync": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateResourceDataSync(ctx, &input)
		},
		"UpdateServiceSetting": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateServiceSettingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateServiceSetting(ctx, &input)
		},
	}
}
