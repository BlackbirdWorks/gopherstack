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
	) (*DescribeAssociationExecutionTargetsOutput, error)
	DescribeAssociationExecutions(
		input *DescribeAssociationExecutionsInput,
	) (*DescribeAssociationExecutionsOutput, error)
	DescribeAutomationExecutions(
		input *DescribeAutomationExecutionsInput,
	) (*DescribeAutomationExecutionsOutput, error)
	DescribeAutomationStepExecutions(
		input *DescribeAutomationStepExecutionsInput,
	) (*DescribeAutomationStepExecutionsOutput, error)
	DescribeAvailablePatches(input *DescribeAvailablePatchesInput) (*DescribeAvailablePatchesOutput, error)
	DescribeEffectiveInstanceAssociations(
		input *DescribeEffectiveInstanceAssociationsInput,
	) (*DescribeEffectiveInstanceAssociationsOutput, error)
	DescribeInstanceAssociationsStatus(
		input *DescribeInstanceAssociationsStatusInput,
	) (*DescribeInstanceAssociationsStatusOutput, error)
	DescribeInstanceInformation(input *DescribeInstanceInformationInput) (*DescribeInstanceInformationOutput, error)
	DescribeInstancePatchStates(input *DescribeInstancePatchStatesInput) (*DescribeInstancePatchStatesOutput, error)
	DescribeInstancePatchStatesForPatchGroup(
		input *DescribeInstancePatchStatesForPatchGroupInput,
	) (*DescribeInstancePatchStatesForPatchGroupOutput, error)
	DescribeInstancePatches(input *DescribeInstancePatchesInput) (*DescribeInstancePatchesOutput, error)
	DescribeInstanceProperties(input *DescribeInstancePropertiesInput) (*DescribeInstancePropertiesOutput, error)
	DescribeMaintenanceWindowExecutionTaskInvocations(
		input *DescribeMaintenanceWindowExecutionTaskInvocationsInput,
	) (*DescribeMaintenanceWindowExecutionTaskInvocationsOutput, error)
	DescribeMaintenanceWindowExecutionTasks(
		input *DescribeMaintenanceWindowExecutionTasksInput,
	) (*DescribeMaintenanceWindowExecutionTasksOutput, error)
	DescribeMaintenanceWindowExecutions(
		input *DescribeMaintenanceWindowExecutionsInput,
	) (*DescribeMaintenanceWindowExecutionsOutput, error)
	DescribeMaintenanceWindowSchedule(
		input *DescribeMaintenanceWindowScheduleInput,
	) (*DescribeMaintenanceWindowScheduleOutput, error)
	DescribeSessions(input *DescribeSessionsInput) (*DescribeSessionsOutput, error)
	GetAccessToken(input *GetAccessTokenInput) (*GetAccessTokenOutput, error)
	GetAutomationExecution(input *GetAutomationExecutionInput) (*GetAutomationExecutionOutput, error)
	GetCalendarState(input *GetCalendarStateInput) (*GetCalendarStateOutput, error)
	GetConnectionStatus(input *GetConnectionStatusInput) (*GetConnectionStatusOutput, error)
	GetExecutionPreview(input *GetExecutionPreviewInput) (*GetExecutionPreviewOutput, error)
	GetMaintenanceWindowExecution(
		input *GetMaintenanceWindowExecutionInput,
	) (*GetMaintenanceWindowExecutionOutput, error)
	GetMaintenanceWindowExecutionTask(
		input *GetMaintenanceWindowExecutionTaskInput,
	) (*GetMaintenanceWindowExecutionTaskOutput, error)
	GetMaintenanceWindowExecutionTaskInvocation(
		input *GetMaintenanceWindowExecutionTaskInvocationInput,
	) (*GetMaintenanceWindowExecutionTaskInvocationOutput, error)
	GetOpsSummary(input *GetOpsSummaryInput) (*GetOpsSummaryOutput, error)
	GetResourcePolicies(input *GetResourcePoliciesInput) (*GetResourcePoliciesOutput, error)
	GetServiceSetting(input *GetServiceSettingInput) (*GetServiceSettingOutput, error)
	LabelParameterVersion(input *LabelParameterVersionInput) (*LabelParameterVersionOutput, error)
	ListAssociationVersions(input *ListAssociationVersionsInput) (*ListAssociationVersionsOutput, error)
	ListAssociations(input *ListAssociationsInput) (*ListAssociationsOutput, error)
	ListNodes(input *ListNodesInput) (*ListNodesOutput, error)
	ListNodesSummary(input *ListNodesSummaryInput) (*ListNodesSummaryOutput, error)
	ListOpsMetadata(input *ListOpsMetadataInput) (*ListOpsMetadataOutput, error)
	ListResourceDataSync(input *ListResourceDataSyncInput) (*ListResourceDataSyncOutput, error)
	PutResourcePolicy(input *PutResourcePolicyInput) (*PutResourcePolicyOutput, error)
	ResetServiceSetting(input *ResetServiceSettingInput) (*ResetServiceSettingOutput, error)
	ResumeSession(input *ResumeSessionInput) (*ResumeSessionOutput, error)
	SendAutomationSignal(input *SendAutomationSignalInput) (*StubOutput, error)
	StartAccessRequest(input *StartAccessRequestInput) (*StartAccessRequestOutput, error)
	StartAssociationsOnce(input *StartAssociationsOnceInput) (*StubOutput, error)
	StartAutomationExecution(input *StartAutomationExecutionInput) (*StartAutomationExecutionOutput, error)
	StartChangeRequestExecution(
		input *StartChangeRequestExecutionInput,
	) (*StartChangeRequestExecutionOutput, error)
	StartExecutionPreview(input *StartExecutionPreviewInput) (*StartExecutionPreviewOutput, error)
	StartSession(input *StartSessionInput) (*StartSessionOutput, error)
	StopAutomationExecution(input *StopAutomationExecutionInput) (*StubOutput, error)
	TerminateSession(input *TerminateSessionInput) (*TerminateSessionOutput, error)
	UnlabelParameterVersion(input *UnlabelParameterVersionInput) (*UnlabelParameterVersionOutput, error)
	UpdateAssociation(input *UpdateAssociationInput) (*UpdateAssociationOutput, error)
	UpdateAssociationStatus(input *UpdateAssociationStatusInput) (*UpdateAssociationStatusOutput, error)
	UpdateManagedInstanceRole(input *UpdateManagedInstanceRoleInput) (*StubOutput, error)
	UpdatePatchBaseline(input *UpdatePatchBaselineInput) (*UpdatePatchBaselineOutput, error)
	UpdateResourceDataSync(input *UpdateResourceDataSyncInput) (*StubOutput, error)
	UpdateServiceSetting(input *UpdateServiceSettingInput) (*StubOutput, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
