package ssm

// StorageBackend defines the interface for an SSM Parameter Store backend.
type StorageBackend interface {
	PutParameter(input *PutParameterInput) (*PutParameterOutput, error)
	GetParameter(input *GetParameterInput) (*GetParameterOutput, error)
	GetParameters(input *GetParametersInput) (*GetParametersOutput, error)
	DeleteParameter(input *DeleteParameterInput) (*DeleteParameterOutput, error)
	DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error)
	GetParameterHistory(input *GetParameterHistoryInput) (*GetParameterHistoryOutput, error)
	GetParametersByPath(input *GetParametersByPathInput) (*GetParametersByPathOutput, error)
	DescribeParameters(input *DescribeParametersInput) (*DescribeParametersOutput, error)
	AddTagsToResource(input *AddTagsToResourceInput) error
	RemoveTagsFromResource(input *RemoveTagsFromResourceInput) error
	ListTagsForResource(input *ListTagsForResourceInput) (*ListTagsForResourceOutput, error)
	ListAll() []Parameter
	// Document operations.
	CreateDocument(input *CreateDocumentInput) (*CreateDocumentOutput, error)
	GetDocument(input *GetDocumentInput) (*GetDocumentOutput, error)
	DescribeDocument(input *DescribeDocumentInput) (*DescribeDocumentOutput, error)
	ListDocuments(input *ListDocumentsInput) (*ListDocumentsOutput, error)
	UpdateDocument(input *UpdateDocumentInput) (*UpdateDocumentOutput, error)
	DeleteDocument(input *DeleteDocumentInput) (*DeleteDocumentOutput, error)
	DescribeDocumentPermission(input *DescribeDocumentPermissionInput) (*DescribeDocumentPermissionOutput, error)
	ModifyDocumentPermission(input *ModifyDocumentPermissionInput) (*ModifyDocumentPermissionOutput, error)
	ListDocumentVersions(input *ListDocumentVersionsInput) (*ListDocumentVersionsOutput, error)
	// Command operations.
	SendCommand(input *SendCommandInput) (*SendCommandOutput, error)
	ListCommands(input *ListCommandsInput) (*ListCommandsOutput, error)
	GetCommandInvocation(input *GetCommandInvocationInput) (*GetCommandInvocationOutput, error)
	ListCommandInvocations(input *ListCommandInvocationsInput) (*ListCommandInvocationsOutput, error)
	// New operations.
	CancelCommand(input *CancelCommandInput) (*CancelCommandOutput, error)
	CancelMaintenanceWindowExecution(
		input *CancelMaintenanceWindowExecutionInput,
	) (*CancelMaintenanceWindowExecutionOutput, error)
	CreateActivation(input *CreateActivationInput) (*CreateActivationOutput, error)
	CreateAssociation(input *CreateAssociationInput) (*CreateAssociationOutput, error)
	CreateAssociationBatch(input *CreateAssociationBatchInput) (*CreateAssociationBatchOutput, error)
	CreateMaintenanceWindow(input *CreateMaintenanceWindowInput) (*CreateMaintenanceWindowOutput, error)
	CreateOpsItem(input *CreateOpsItemInput) (*CreateOpsItemOutput, error)
	CreateOpsMetadata(input *CreateOpsMetadataInput) (*CreateOpsMetadataOutput, error)
	CreatePatchBaseline(input *CreatePatchBaselineInput) (*CreatePatchBaselineOutput, error)
	AssociateOpsItemRelatedItem(input *AssociateOpsItemRelatedItemInput) (*AssociateOpsItemRelatedItemOutput, error)
	// Document metadata operations (Group 1).
	UpdateDocumentDefaultVersion(
		input *UpdateDocumentDefaultVersionInput,
	) (*UpdateDocumentDefaultVersionOutput, error)
	UpdateDocumentMetadata(input *UpdateDocumentMetadataInput) (*StubOutput, error)
	ListDocumentMetadataHistory(input *ListDocumentMetadataHistoryInput) (*ListDocumentMetadataHistoryOutput, error)
	// Inventory operations (Group 2).
	PutInventory(input *PutInventoryInput) (*StubOutput, error)
	GetInventory(input *GetInventoryInput) (*GetInventoryOutput, error)
	GetInventorySchema(input *GetInventorySchemaInput) (*GetInventorySchemaOutput, error)
	ListInventoryEntries(input *ListInventoryEntriesInput) (*ListInventoryEntriesOutput, error)
	DeleteInventory(input *DeleteInventoryInput) (*StubOutput, error)
	DescribeInventoryDeletions(input *DescribeInventoryDeletionsInput) (*DescribeInventoryDeletionsOutput, error)
	// Compliance operations (Group 3).
	PutComplianceItems(input *PutComplianceItemsInput) (*StubOutput, error)
	ListComplianceItems(input *ListComplianceItemsInput) (*ListComplianceItemsOutput, error)
	ListComplianceSummaries(input *ListComplianceSummariesInput) (*ListComplianceSummariesOutput, error)
	ListResourceComplianceSummaries(
		input *ListResourceComplianceSummariesInput,
	) (*ListResourceComplianceSummariesOutput, error)
	// Patch baseline operations (Group 4).
	GetPatchBaseline(input *GetPatchBaselineInput) (*GetPatchBaselineOutput, error)
	GetDefaultPatchBaseline(input *GetDefaultPatchBaselineInput) (*GetDefaultPatchBaselineOutput, error)
	GetPatchBaselineForPatchGroup(
		input *GetPatchBaselineForPatchGroupInput,
	) (*GetPatchBaselineForPatchBaselineOutput, error)
	RegisterDefaultPatchBaseline(
		input *RegisterDefaultPatchBaselineInput,
	) (*RegisterDefaultPatchBaselineOutput, error)
	RegisterPatchBaselineForPatchGroup(
		input *RegisterPatchBaselineForPatchGroupInput,
	) (*RegisterPatchBaselineForPatchGroupOutput, error)
	DeregisterPatchBaselineForPatchGroup(input *DeregisterPatchBaselineForPatchGroupInput) (*StubOutput, error)
	DeletePatchBaseline(input *DeletePatchBaselineInput) (*DeletePatchBaselineOutput, error)
	DescribePatchBaselines(input *DescribePatchBaselinesInput) (*DescribePatchBaselinesOutput, error)
	DescribePatchGroups(input *DescribePatchGroupsInput) (*DescribePatchGroupsOutput, error)
	DescribePatchGroupState(input *DescribePatchGroupStateInput) (*DescribePatchGroupStateOutput, error)
	DescribePatchProperties(input *DescribePatchPropertiesInput) (*DescribePatchPropertiesOutput, error)
	DescribeEffectivePatchesForPatchBaseline(
		input *DescribeEffectivePatchesForPatchBaselineInput,
	) (*DescribeEffectivePatchesForPatchBaselineOutput, error)
	GetDeployablePatchSnapshotForInstance(
		input *GetDeployablePatchSnapshotForInstanceInput,
	) (*GetDeployablePatchSnapshotForInstanceOutput, error)
	// Maintenance window operations (Group 5).
	GetMaintenanceWindow(input *GetMaintenanceWindowInput) (*GetMaintenanceWindowOutput, error)
	DeleteMaintenanceWindow(input *DeleteMaintenanceWindowInput) (*DeleteMaintenanceWindowOutput, error)
	UpdateMaintenanceWindow(input *UpdateMaintenanceWindowInput) (*UpdateMaintenanceWindowOutput, error)
	GetMaintenanceWindowTask(input *GetMaintenanceWindowTaskInput) (*GetMaintenanceWindowTaskOutput, error)
	RegisterTargetWithMaintenanceWindow(
		input *RegisterTargetWithMaintenanceWindowInput,
	) (*RegisterTargetWithMaintenanceWindowOutput, error)
	RegisterTaskWithMaintenanceWindow(
		input *RegisterTaskWithMaintenanceWindowInput,
	) (*RegisterTaskWithMaintenanceWindowOutput, error)
	DeregisterTargetFromMaintenanceWindow(input *DeregisterTargetFromMaintenanceWindowInput) (*StubOutput, error)
	DeregisterTaskFromMaintenanceWindow(input *DeregisterTaskFromMaintenanceWindowInput) (*StubOutput, error)
	DescribeMaintenanceWindows(input *DescribeMaintenanceWindowsInput) (*DescribeMaintenanceWindowsOutput, error)
	DescribeMaintenanceWindowsForTarget(
		input *DescribeMaintenanceWindowsForTargetInput,
	) (*DescribeMaintenanceWindowsForTargetOutput, error)
	DescribeMaintenanceWindowTargets(
		input *DescribeMaintenanceWindowTargetsInput,
	) (*DescribeMaintenanceWindowTargetsOutput, error)
	DescribeMaintenanceWindowTasks(
		input *DescribeMaintenanceWindowTasksInput,
	) (*DescribeMaintenanceWindowTasksOutput, error)
	UpdateMaintenanceWindowTarget(
		input *UpdateMaintenanceWindowTargetInput,
	) (*UpdateMaintenanceWindowTargetOutput, error)
	UpdateMaintenanceWindowTask(input *UpdateMaintenanceWindowTaskInput) (*UpdateMaintenanceWindowTaskOutput, error)
	// OpsItem operations (Group 6).
	GetOpsItem(input *GetOpsItemInput) (*GetOpsItemOutput, error)
	DeleteOpsItem(input *DeleteOpsItemInput) (*StubOutput, error)
	DescribeOpsItems(input *DescribeOpsItemsInput) (*DescribeOpsItemsOutput, error)
	UpdateOpsItem(input *UpdateOpsItemInput) (*StubOutput, error)
	DisassociateOpsItemRelatedItem(input *DisassociateOpsItemRelatedItemInput) (*StubOutput, error)
	ListOpsItemRelatedItems(input *ListOpsItemRelatedItemsInput) (*ListOpsItemRelatedItemsOutput, error)
	ListOpsItemEvents(input *ListOpsItemEventsInput) (*ListOpsItemEventsOutput, error)
	GetOpsMetadata(input *GetOpsMetadataInput) (*GetOpsMetadataOutput, error)
	UpdateOpsMetadata(input *UpdateOpsMetadataInput) (*UpdateOpsMetadataOutput, error)
	DeleteOpsMetadata(input *DeleteOpsMetadataInput) (*StubOutput, error)
	// Remaining stub operations.
	CreateResourceDataSync(input *CreateResourceDataSyncInput) (*StubOutput, error)
	DeleteActivation(input *DeleteActivationInput) (*StubOutput, error)
	DeleteAssociation(input *DeleteAssociationInput) (*StubOutput, error)
	DeleteResourceDataSync(input *DeleteResourceDataSyncInput) (*StubOutput, error)
	DeleteResourcePolicy(input *DeleteResourcePolicyInput) (*StubOutput, error)
	DeregisterManagedInstance(input *DeregisterManagedInstanceInput) (*StubOutput, error)
	DescribeActivations(input *DescribeActivationsInput) (*DescribeActivationsOutput, error)
	DescribeAssociation(input *DescribeAssociationInput) (*DescribeAssociationOutput, error)
	DescribeAssociationExecutionTargets(
		input *DescribeAssociationExecutionTargetsInput,
	) (*DescribeAssociationExecutionTargetsOutputFull, error)
	DescribeAssociationExecutions(
		input *DescribeAssociationExecutionsInput,
	) (*DescribeAssociationExecutionsOutputFull, error)
	DescribeAutomationExecutions(
		input *DescribeAutomationExecutionsInput,
	) (*DescribeAutomationExecutionsOutputFull, error)
	DescribeAutomationStepExecutions(
		input *DescribeAutomationStepExecutionsInput,
	) (*DescribeAutomationStepExecutionsOutputFull, error)
	DescribeAvailablePatches(input *DescribeAvailablePatchesInput) (*DescribeAvailablePatchesOutput, error)
	DescribeEffectiveInstanceAssociations(
		input *DescribeEffectiveInstanceAssociationsInput,
	) (*DescribeEffectiveInstanceAssociationsOutputFull, error)
	DescribeInstanceAssociationsStatus(
		input *DescribeInstanceAssociationsStatusInput,
	) (*DescribeInstanceAssociationsStatusOutputFull, error)
	DescribeInstanceInformation(input *DescribeInstanceInformationInput) (*DescribeInstanceInformationOutputFull, error)
	DescribeInstancePatchStates(input *DescribeInstancePatchStatesInput) (*DescribeInstancePatchStatesOutputFull, error)
	DescribeInstancePatchStatesForPatchGroup(
		input *DescribeInstancePatchStatesForPatchGroupInput,
	) (*DescribeInstancePatchStatesForPatchGroupOutput, error)
	DescribeInstancePatches(input *DescribeInstancePatchesInput) (*DescribeInstancePatchesOutput, error)
	DescribeInstanceProperties(input *DescribeInstancePropertiesInput) (*DescribeInstancePropertiesOutput, error)
	DescribeMaintenanceWindowExecutionTaskInvocations(
		input *DescribeMaintenanceWindowExecutionTaskInvocationsInput,
	) (*DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull, error)
	DescribeMaintenanceWindowExecutionTasks(
		input *DescribeMaintenanceWindowExecutionTasksInput,
	) (*DescribeMaintenanceWindowExecutionTasksOutputFull, error)
	DescribeMaintenanceWindowExecutions(
		input *DescribeMaintenanceWindowExecutionsInput,
	) (*DescribeMaintenanceWindowExecutionsOutputFull, error)
	DescribeMaintenanceWindowSchedule(
		input *DescribeMaintenanceWindowScheduleInput,
	) (*DescribeMaintenanceWindowScheduleOutputFull, error)
	DescribeSessions(input *DescribeSessionsInput) (*DescribeSessionsOutputFull, error)
	GetAccessToken(input *GetAccessTokenInput) (*GetAccessTokenOutputFull, error)
	GetAutomationExecution(input *GetAutomationExecutionInput) (*GetAutomationExecutionOutputFull, error)
	GetCalendarState(input *GetCalendarStateInput) (*GetCalendarStateOutputFull, error)
	GetConnectionStatus(input *GetConnectionStatusInput) (*GetConnectionStatusOutputFull, error)
	GetExecutionPreview(input *GetExecutionPreviewInput) (*GetExecutionPreviewOutputFull, error)
	GetMaintenanceWindowExecution(
		input *GetMaintenanceWindowExecutionInput,
	) (*GetMaintenanceWindowExecutionOutputFull, error)
	GetMaintenanceWindowExecutionTask(
		input *GetMaintenanceWindowExecutionTaskInput,
	) (*GetMaintenanceWindowExecutionTaskOutputFull, error)
	GetMaintenanceWindowExecutionTaskInvocation(
		input *GetMaintenanceWindowExecutionTaskInvocationInput,
	) (*GetMaintenanceWindowExecutionTaskInvocationOutputFull, error)
	GetOpsSummary(input *GetOpsSummaryInput) (*GetOpsSummaryOutputFull, error)
	GetResourcePolicies(input *GetResourcePoliciesInput) (*GetResourcePoliciesOutputFull, error)
	GetServiceSetting(input *GetServiceSettingInput) (*GetServiceSettingOutputFull, error)
	LabelParameterVersion(input *LabelParameterVersionInput) (*LabelParameterVersionOutputFull, error)
	ListAssociationVersions(input *ListAssociationVersionsInput) (*ListAssociationVersionsOutputFull, error)
	ListAssociations(input *ListAssociationsInput) (*ListAssociationsOutput, error)
	ListNodes(input *ListNodesInput) (*ListNodesOutputFull, error)
	ListNodesSummary(input *ListNodesSummaryInput) (*ListNodesSummaryOutputFull, error)
	ListOpsMetadata(input *ListOpsMetadataInput) (*ListOpsMetadataOutputFull, error)
	ListResourceDataSync(input *ListResourceDataSyncInput) (*ListResourceDataSyncOutputFull, error)
	PutResourcePolicy(input *PutResourcePolicyInput) (*PutResourcePolicyOutputFull, error)
	ResetServiceSetting(input *ResetServiceSettingInput) (*ResetServiceSettingOutputFull, error)
	ResumeSession(input *ResumeSessionInput) (*ResumeSessionOutputFull, error)
	SendAutomationSignal(input *SendAutomationSignalInput) (*StubOutput, error)
	StartAccessRequest(input *StartAccessRequestInput) (*StartAccessRequestOutputFull, error)
	StartAssociationsOnce(input *StartAssociationsOnceInput) (*StubOutput, error)
	StartAutomationExecution(input *StartAutomationExecutionInput) (*StartAutomationExecutionOutputFull, error)
	StartChangeRequestExecution(
		input *StartChangeRequestExecutionInput,
	) (*StartChangeRequestExecutionOutputFull, error)
	StartExecutionPreview(input *StartExecutionPreviewInput) (*StartExecutionPreviewOutputFull, error)
	StartSession(input *StartSessionInput) (*StartSessionOutput, error)
	StopAutomationExecution(input *StopAutomationExecutionInput) (*StubOutput, error)
	TerminateSession(input *TerminateSessionInput) (*TerminateSessionOutput, error)
	UnlabelParameterVersion(input *UnlabelParameterVersionInput) (*UnlabelParameterVersionOutputFull, error)
	UpdateAssociation(input *UpdateAssociationInput) (*UpdateAssociationOutput, error)
	UpdateAssociationStatus(input *UpdateAssociationStatusInput) (*UpdateAssociationStatusOutputFull, error)
	UpdateManagedInstanceRole(input *UpdateManagedInstanceRoleInput) (*StubOutput, error)
	UpdatePatchBaseline(input *UpdatePatchBaselineInput) (*UpdatePatchBaselineOutput, error)
	UpdateResourceDataSync(input *UpdateResourceDataSyncInput) (*StubOutput, error)
	UpdateServiceSetting(input *UpdateServiceSettingInput) (*StubOutput, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
