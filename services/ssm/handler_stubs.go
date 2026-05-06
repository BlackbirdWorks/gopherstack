package ssm

import "encoding/json"

// handler_stubs.go wires the 123 stub backend methods into the HTTP dispatch table.

//nolint:funlen,gocognit,gocyclo,cyclop // This function is intentionally long — it registers all stub operations.
func (h *Handler) ssmStubOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"CreateResourceDataSync": func(b []byte) (any, error) {
			var input CreateResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateResourceDataSync(&input)
		},
		"DeleteActivation": func(b []byte) (any, error) {
			var input DeleteActivationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteActivation(&input)
		},
		"DeleteAssociation": func(b []byte) (any, error) {
			var input DeleteAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteAssociation(&input)
		},
		"DeleteInventory": func(b []byte) (any, error) {
			var input DeleteInventoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteInventory(&input)
		},
		"DeleteMaintenanceWindow": func(b []byte) (any, error) {
			var input DeleteMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteMaintenanceWindow(&input)
		},
		"DeleteOpsItem": func(b []byte) (any, error) {
			var input DeleteOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteOpsItem(&input)
		},
		"DeleteOpsMetadata": func(b []byte) (any, error) {
			var input DeleteOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteOpsMetadata(&input)
		},
		"DeletePatchBaseline": func(b []byte) (any, error) {
			var input DeletePatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeletePatchBaseline(&input)
		},
		"DeleteResourceDataSync": func(b []byte) (any, error) {
			var input DeleteResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteResourceDataSync(&input)
		},
		"DeleteResourcePolicy": func(b []byte) (any, error) {
			var input DeleteResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteResourcePolicy(&input)
		},
		"DeregisterManagedInstance": func(b []byte) (any, error) {
			var input DeregisterManagedInstanceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterManagedInstance(&input)
		},
		"DeregisterPatchBaselineForPatchGroup": func(b []byte) (any, error) {
			var input DeregisterPatchBaselineForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterPatchBaselineForPatchGroup(&input)
		},
		"DeregisterTargetFromMaintenanceWindow": func(b []byte) (any, error) {
			var input DeregisterTargetFromMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterTargetFromMaintenanceWindow(&input)
		},
		"DeregisterTaskFromMaintenanceWindow": func(b []byte) (any, error) {
			var input DeregisterTaskFromMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeregisterTaskFromMaintenanceWindow(&input)
		},
		"DescribeActivations": func(b []byte) (any, error) {
			var input DescribeActivationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeActivations(&input)
		},
		"DescribeAssociation": func(b []byte) (any, error) {
			var input DescribeAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAssociation(&input)
		},
		"DescribeAssociationExecutionTargets": func(b []byte) (any, error) {
			var input DescribeAssociationExecutionTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAssociationExecutionTargets(&input)
		},
		"DescribeAssociationExecutions": func(b []byte) (any, error) {
			var input DescribeAssociationExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAssociationExecutions(&input)
		},
		"DescribeAutomationExecutions": func(b []byte) (any, error) {
			var input DescribeAutomationExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAutomationExecutions(&input)
		},
		"DescribeAutomationStepExecutions": func(b []byte) (any, error) {
			var input DescribeAutomationStepExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAutomationStepExecutions(&input)
		},
		"DescribeAvailablePatches": func(b []byte) (any, error) {
			var input DescribeAvailablePatchesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeAvailablePatches(&input)
		},
		"DescribeEffectiveInstanceAssociations": func(b []byte) (any, error) {
			var input DescribeEffectiveInstanceAssociationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEffectiveInstanceAssociations(&input)
		},
		"DescribeEffectivePatchesForPatchBaseline": func(b []byte) (any, error) {
			var input DescribeEffectivePatchesForPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeEffectivePatchesForPatchBaseline(&input)
		},
		"DescribeInstanceAssociationsStatus": func(b []byte) (any, error) {
			var input DescribeInstanceAssociationsStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstanceAssociationsStatus(&input)
		},
		"DescribeInstanceInformation": func(b []byte) (any, error) {
			var input DescribeInstanceInformationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstanceInformation(&input)
		},
		"DescribeInstancePatchStates": func(b []byte) (any, error) {
			var input DescribeInstancePatchStatesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstancePatchStates(&input)
		},
		"DescribeInstancePatchStatesForPatchGroup": func(b []byte) (any, error) {
			var input DescribeInstancePatchStatesForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstancePatchStatesForPatchGroup(&input)
		},
		"DescribeInstancePatches": func(b []byte) (any, error) {
			var input DescribeInstancePatchesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstancePatches(&input)
		},
		"DescribeInstanceProperties": func(b []byte) (any, error) {
			var input DescribeInstancePropertiesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInstanceProperties(&input)
		},
		"DescribeInventoryDeletions": func(b []byte) (any, error) {
			var input DescribeInventoryDeletionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeInventoryDeletions(&input)
		},
		"DescribeMaintenanceWindowExecutionTaskInvocations": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowExecutionTaskInvocationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowExecutionTaskInvocations(&input)
		},
		"DescribeMaintenanceWindowExecutionTasks": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowExecutionTasksInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowExecutionTasks(&input)
		},
		"DescribeMaintenanceWindowExecutions": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowExecutions(&input)
		},
		"DescribeMaintenanceWindowSchedule": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowScheduleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowSchedule(&input)
		},
		"DescribeMaintenanceWindowTargets": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowTargetsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowTargets(&input)
		},
		"DescribeMaintenanceWindowTasks": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowTasksInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowTasks(&input)
		},
		"DescribeMaintenanceWindows": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindows(&input)
		},
		"DescribeMaintenanceWindowsForTarget": func(b []byte) (any, error) {
			var input DescribeMaintenanceWindowsForTargetInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeMaintenanceWindowsForTarget(&input)
		},
		"DescribeOpsItems": func(b []byte) (any, error) {
			var input DescribeOpsItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeOpsItems(&input)
		},
		"DescribePatchBaselines": func(b []byte) (any, error) {
			var input DescribePatchBaselinesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchBaselines(&input)
		},
		"DescribePatchGroupState": func(b []byte) (any, error) {
			var input DescribePatchGroupStateInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchGroupState(&input)
		},
		"DescribePatchGroups": func(b []byte) (any, error) {
			var input DescribePatchGroupsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchGroups(&input)
		},
		"DescribePatchProperties": func(b []byte) (any, error) {
			var input DescribePatchPropertiesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribePatchProperties(&input)
		},
		"DescribeSessions": func(b []byte) (any, error) {
			var input DescribeSessionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSessions(&input)
		},
		"DisassociateOpsItemRelatedItem": func(b []byte) (any, error) {
			var input DisassociateOpsItemRelatedItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DisassociateOpsItemRelatedItem(&input)
		},
		"GetAccessToken": func(b []byte) (any, error) {
			var input GetAccessTokenInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetAccessToken(&input)
		},
		"GetAutomationExecution": func(b []byte) (any, error) {
			var input GetAutomationExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetAutomationExecution(&input)
		},
		"GetCalendarState": func(b []byte) (any, error) {
			var input GetCalendarStateInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetCalendarState(&input)
		},
		"GetConnectionStatus": func(b []byte) (any, error) {
			var input GetConnectionStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetConnectionStatus(&input)
		},
		"GetDefaultPatchBaseline": func(b []byte) (any, error) {
			var input GetDefaultPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetDefaultPatchBaseline(&input)
		},
		"GetDeployablePatchSnapshotForInstance": func(b []byte) (any, error) {
			var input GetDeployablePatchSnapshotForInstanceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetDeployablePatchSnapshotForInstance(&input)
		},
		"GetExecutionPreview": func(b []byte) (any, error) {
			var input GetExecutionPreviewInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetExecutionPreview(&input)
		},
		"GetInventory": func(b []byte) (any, error) {
			var input GetInventoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetInventory(&input)
		},
		"GetInventorySchema": func(b []byte) (any, error) {
			var input GetInventorySchemaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetInventorySchema(&input)
		},
		"GetMaintenanceWindow": func(b []byte) (any, error) {
			var input GetMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindow(&input)
		},
		"GetMaintenanceWindowExecution": func(b []byte) (any, error) {
			var input GetMaintenanceWindowExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowExecution(&input)
		},
		"GetMaintenanceWindowExecutionTask": func(b []byte) (any, error) {
			var input GetMaintenanceWindowExecutionTaskInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowExecutionTask(&input)
		},
		"GetMaintenanceWindowExecutionTaskInvocation": func(b []byte) (any, error) {
			var input GetMaintenanceWindowExecutionTaskInvocationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowExecutionTaskInvocation(&input)
		},
		"GetMaintenanceWindowTask": func(b []byte) (any, error) {
			var input GetMaintenanceWindowTaskInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetMaintenanceWindowTask(&input)
		},
		"GetOpsItem": func(b []byte) (any, error) {
			var input GetOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetOpsItem(&input)
		},
		"GetOpsMetadata": func(b []byte) (any, error) {
			var input GetOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetOpsMetadata(&input)
		},
		"GetOpsSummary": func(b []byte) (any, error) {
			var input GetOpsSummaryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetOpsSummary(&input)
		},
		"GetPatchBaseline": func(b []byte) (any, error) {
			var input GetPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPatchBaseline(&input)
		},
		"GetPatchBaselineForPatchGroup": func(b []byte) (any, error) {
			var input GetPatchBaselineForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPatchBaselineForPatchGroup(&input)
		},
		"GetResourcePolicies": func(b []byte) (any, error) {
			var input GetResourcePoliciesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetResourcePolicies(&input)
		},
		"GetServiceSetting": func(b []byte) (any, error) {
			var input GetServiceSettingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetServiceSetting(&input)
		},
		"LabelParameterVersion": func(b []byte) (any, error) {
			var input LabelParameterVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.LabelParameterVersion(&input)
		},
		"ListAssociationVersions": func(b []byte) (any, error) {
			var input ListAssociationVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAssociationVersions(&input)
		},
		"ListAssociations": func(b []byte) (any, error) {
			var input ListAssociationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAssociations(&input)
		},
		"ListComplianceItems": func(b []byte) (any, error) {
			var input ListComplianceItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListComplianceItems(&input)
		},
		"ListComplianceSummaries": func(b []byte) (any, error) {
			var input ListComplianceSummariesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListComplianceSummaries(&input)
		},
		"ListDocumentMetadataHistory": func(b []byte) (any, error) {
			var input ListDocumentMetadataHistoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListDocumentMetadataHistory(&input)
		},
		"ListInventoryEntries": func(b []byte) (any, error) {
			var input ListInventoryEntriesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListInventoryEntries(&input)
		},
		"ListNodes": func(b []byte) (any, error) {
			var input ListNodesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListNodes(&input)
		},
		"ListNodesSummary": func(b []byte) (any, error) {
			var input ListNodesSummaryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListNodesSummary(&input)
		},
		"ListOpsItemEvents": func(b []byte) (any, error) {
			var input ListOpsItemEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListOpsItemEvents(&input)
		},
		"ListOpsItemRelatedItems": func(b []byte) (any, error) {
			var input ListOpsItemRelatedItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListOpsItemRelatedItems(&input)
		},
		"ListOpsMetadata": func(b []byte) (any, error) {
			var input ListOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListOpsMetadata(&input)
		},
		"ListResourceComplianceSummaries": func(b []byte) (any, error) {
			var input ListResourceComplianceSummariesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListResourceComplianceSummaries(&input)
		},
		"ListResourceDataSync": func(b []byte) (any, error) {
			var input ListResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListResourceDataSync(&input)
		},
		"PutComplianceItems": func(b []byte) (any, error) {
			var input PutComplianceItemsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutComplianceItems(&input)
		},
		"PutInventory": func(b []byte) (any, error) {
			var input PutInventoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutInventory(&input)
		},
		"PutResourcePolicy": func(b []byte) (any, error) {
			var input PutResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutResourcePolicy(&input)
		},
		"RegisterDefaultPatchBaseline": func(b []byte) (any, error) {
			var input RegisterDefaultPatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterDefaultPatchBaseline(&input)
		},
		"RegisterPatchBaselineForPatchGroup": func(b []byte) (any, error) {
			var input RegisterPatchBaselineForPatchGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterPatchBaselineForPatchGroup(&input)
		},
		"RegisterTargetWithMaintenanceWindow": func(b []byte) (any, error) {
			var input RegisterTargetWithMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterTargetWithMaintenanceWindow(&input)
		},
		"RegisterTaskWithMaintenanceWindow": func(b []byte) (any, error) {
			var input RegisterTaskWithMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RegisterTaskWithMaintenanceWindow(&input)
		},
		"ResetServiceSetting": func(b []byte) (any, error) {
			var input ResetServiceSettingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ResetServiceSetting(&input)
		},
		"ResumeSession": func(b []byte) (any, error) {
			var input ResumeSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ResumeSession(&input)
		},
		"SendAutomationSignal": func(b []byte) (any, error) {
			var input SendAutomationSignalInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.SendAutomationSignal(&input)
		},
		"StartAccessRequest": func(b []byte) (any, error) {
			var input StartAccessRequestInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartAccessRequest(&input)
		},
		"StartAssociationsOnce": func(b []byte) (any, error) {
			var input StartAssociationsOnceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartAssociationsOnce(&input)
		},
		"StartAutomationExecution": func(b []byte) (any, error) {
			var input StartAutomationExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartAutomationExecution(&input)
		},
		"StartChangeRequestExecution": func(b []byte) (any, error) {
			var input StartChangeRequestExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartChangeRequestExecution(&input)
		},
		"StartExecutionPreview": func(b []byte) (any, error) {
			var input StartExecutionPreviewInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartExecutionPreview(&input)
		},
		"StartSession": func(b []byte) (any, error) {
			var input StartSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StartSession(&input)
		},
		"StopAutomationExecution": func(b []byte) (any, error) {
			var input StopAutomationExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StopAutomationExecution(&input)
		},
		"TerminateSession": func(b []byte) (any, error) {
			var input TerminateSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.TerminateSession(&input)
		},
		"UnlabelParameterVersion": func(b []byte) (any, error) {
			var input UnlabelParameterVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UnlabelParameterVersion(&input)
		},
		"UpdateAssociation": func(b []byte) (any, error) {
			var input UpdateAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateAssociation(&input)
		},
		"UpdateAssociationStatus": func(b []byte) (any, error) {
			var input UpdateAssociationStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateAssociationStatus(&input)
		},
		"UpdateDocumentDefaultVersion": func(b []byte) (any, error) {
			var input UpdateDocumentDefaultVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateDocumentDefaultVersion(&input)
		},
		"UpdateDocumentMetadata": func(b []byte) (any, error) {
			var input UpdateDocumentMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateDocumentMetadata(&input)
		},
		"UpdateMaintenanceWindow": func(b []byte) (any, error) {
			var input UpdateMaintenanceWindowInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMaintenanceWindow(&input)
		},
		"UpdateMaintenanceWindowTarget": func(b []byte) (any, error) {
			var input UpdateMaintenanceWindowTargetInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMaintenanceWindowTarget(&input)
		},
		"UpdateMaintenanceWindowTask": func(b []byte) (any, error) {
			var input UpdateMaintenanceWindowTaskInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateMaintenanceWindowTask(&input)
		},
		"UpdateManagedInstanceRole": func(b []byte) (any, error) {
			var input UpdateManagedInstanceRoleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateManagedInstanceRole(&input)
		},
		"UpdateOpsItem": func(b []byte) (any, error) {
			var input UpdateOpsItemInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateOpsItem(&input)
		},
		"UpdateOpsMetadata": func(b []byte) (any, error) {
			var input UpdateOpsMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateOpsMetadata(&input)
		},
		"UpdatePatchBaseline": func(b []byte) (any, error) {
			var input UpdatePatchBaselineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdatePatchBaseline(&input)
		},
		"UpdateResourceDataSync": func(b []byte) (any, error) {
			var input UpdateResourceDataSyncInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateResourceDataSync(&input)
		},
		"UpdateServiceSetting": func(b []byte) (any, error) {
			var input UpdateServiceSettingInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateServiceSetting(&input)
		},
	}
}
