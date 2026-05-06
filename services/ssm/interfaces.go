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
	// Stub operations (acknowledged not-implemented ops now routed through handler).
	CreateResourceDataSync(input *CreateResourceDataSyncInput) (*StubOutput, error)
	DeleteActivation(input *DeleteActivationInput) (*StubOutput, error)
	DeleteAssociation(input *DeleteAssociationInput) (*StubOutput, error)
	DeleteInventory(input *DeleteInventoryInput) (*StubOutput, error)
	DeleteMaintenanceWindow(input *DeleteMaintenanceWindowInput) (*StubOutput, error)
	DeleteOpsItem(input *DeleteOpsItemInput) (*StubOutput, error)
	DeleteOpsMetadata(input *DeleteOpsMetadataInput) (*StubOutput, error)
	DeletePatchBaseline(input *DeletePatchBaselineInput) (*StubOutput, error)
	DeleteResourceDataSync(input *DeleteResourceDataSyncInput) (*StubOutput, error)
	DeleteResourcePolicy(input *DeleteResourcePolicyInput) (*StubOutput, error)
	DeregisterManagedInstance(input *DeregisterManagedInstanceInput) (*StubOutput, error)
	DeregisterPatchBaselineForPatchGroup(input *DeregisterPatchBaselineForPatchGroupInput) (*StubOutput, error)
	DeregisterTargetFromMaintenanceWindow(input *DeregisterTargetFromMaintenanceWindowInput) (*StubOutput, error)
	DeregisterTaskFromMaintenanceWindow(input *DeregisterTaskFromMaintenanceWindowInput) (*StubOutput, error)
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
	DescribeEffectivePatchesForPatchBaseline(
		input *DescribeEffectivePatchesForPatchBaselineInput,
	) (*DescribeEffectivePatchesForPatchBaselineOutput, error)
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
	DescribeInventoryDeletions(input *DescribeInventoryDeletionsInput) (*DescribeInventoryDeletionsOutput, error)
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
	DescribeMaintenanceWindowTargets(
		input *DescribeMaintenanceWindowTargetsInput,
	) (*DescribeMaintenanceWindowTargetsOutput, error)
	DescribeMaintenanceWindowTasks(
		input *DescribeMaintenanceWindowTasksInput,
	) (*DescribeMaintenanceWindowTasksOutput, error)
	DescribeMaintenanceWindows(input *DescribeMaintenanceWindowsInput) (*DescribeMaintenanceWindowsOutput, error)
	DescribeMaintenanceWindowsForTarget(
		input *DescribeMaintenanceWindowsForTargetInput,
	) (*DescribeMaintenanceWindowsForTargetOutput, error)
	DescribeOpsItems(input *DescribeOpsItemsInput) (*DescribeOpsItemsOutput, error)
	DescribePatchBaselines(input *DescribePatchBaselinesInput) (*DescribePatchBaselinesOutput, error)
	DescribePatchGroupState(input *DescribePatchGroupStateInput) (*DescribePatchGroupStateOutput, error)
	DescribePatchGroups(input *DescribePatchGroupsInput) (*DescribePatchGroupsOutput, error)
	DescribePatchProperties(input *DescribePatchPropertiesInput) (*DescribePatchPropertiesOutput, error)
	DescribeSessions(input *DescribeSessionsInput) (*DescribeSessionsOutput, error)
	DisassociateOpsItemRelatedItem(input *DisassociateOpsItemRelatedItemInput) (*StubOutput, error)
	GetAccessToken(input *GetAccessTokenInput) (*GetAccessTokenOutput, error)
	GetAutomationExecution(input *GetAutomationExecutionInput) (*GetAutomationExecutionOutput, error)
	GetCalendarState(input *GetCalendarStateInput) (*GetCalendarStateOutput, error)
	GetConnectionStatus(input *GetConnectionStatusInput) (*GetConnectionStatusOutput, error)
	GetDefaultPatchBaseline(input *GetDefaultPatchBaselineInput) (*GetDefaultPatchBaselineOutput, error)
	GetDeployablePatchSnapshotForInstance(
		input *GetDeployablePatchSnapshotForInstanceInput,
	) (*GetDeployablePatchSnapshotForInstanceOutput, error)
	GetExecutionPreview(input *GetExecutionPreviewInput) (*GetExecutionPreviewOutput, error)
	GetInventory(input *GetInventoryInput) (*GetInventoryOutput, error)
	GetInventorySchema(input *GetInventorySchemaInput) (*GetInventorySchemaOutput, error)
	GetMaintenanceWindow(input *GetMaintenanceWindowInput) (*GetMaintenanceWindowOutput, error)
	GetMaintenanceWindowExecution(
		input *GetMaintenanceWindowExecutionInput,
	) (*GetMaintenanceWindowExecutionOutput, error)
	GetMaintenanceWindowExecutionTask(
		input *GetMaintenanceWindowExecutionTaskInput,
	) (*GetMaintenanceWindowExecutionTaskOutput, error)
	GetMaintenanceWindowExecutionTaskInvocation(
		input *GetMaintenanceWindowExecutionTaskInvocationInput,
	) (*GetMaintenanceWindowExecutionTaskInvocationOutput, error)
	GetMaintenanceWindowTask(input *GetMaintenanceWindowTaskInput) (*GetMaintenanceWindowTaskOutput, error)
	GetOpsItem(input *GetOpsItemInput) (*GetOpsItemOutput, error)
	GetOpsMetadata(input *GetOpsMetadataInput) (*GetOpsMetadataOutput, error)
	GetOpsSummary(input *GetOpsSummaryInput) (*GetOpsSummaryOutput, error)
	GetPatchBaseline(input *GetPatchBaselineInput) (*GetPatchBaselineOutput, error)
	GetPatchBaselineForPatchGroup(
		input *GetPatchBaselineForPatchGroupInput,
	) (*GetPatchBaselineForPatchGroupOutput, error)
	GetResourcePolicies(input *GetResourcePoliciesInput) (*GetResourcePoliciesOutput, error)
	GetServiceSetting(input *GetServiceSettingInput) (*GetServiceSettingOutput, error)
	LabelParameterVersion(input *LabelParameterVersionInput) (*LabelParameterVersionOutput, error)
	ListAssociationVersions(input *ListAssociationVersionsInput) (*ListAssociationVersionsOutput, error)
	ListAssociations(input *ListAssociationsInput) (*ListAssociationsOutput, error)
	ListComplianceItems(input *ListComplianceItemsInput) (*ListComplianceItemsOutput, error)
	ListComplianceSummaries(input *ListComplianceSummariesInput) (*ListComplianceSummariesOutput, error)
	ListDocumentMetadataHistory(input *ListDocumentMetadataHistoryInput) (*ListDocumentMetadataHistoryOutput, error)
	ListInventoryEntries(input *ListInventoryEntriesInput) (*ListInventoryEntriesOutput, error)
	ListNodes(input *ListNodesInput) (*ListNodesOutput, error)
	ListNodesSummary(input *ListNodesSummaryInput) (*ListNodesSummaryOutput, error)
	ListOpsItemEvents(input *ListOpsItemEventsInput) (*ListOpsItemEventsOutput, error)
	ListOpsItemRelatedItems(input *ListOpsItemRelatedItemsInput) (*ListOpsItemRelatedItemsOutput, error)
	ListOpsMetadata(input *ListOpsMetadataInput) (*ListOpsMetadataOutput, error)
	ListResourceComplianceSummaries(
		input *ListResourceComplianceSummariesInput,
	) (*ListResourceComplianceSummariesOutput, error)
	ListResourceDataSync(input *ListResourceDataSyncInput) (*ListResourceDataSyncOutput, error)
	PutComplianceItems(input *PutComplianceItemsInput) (*StubOutput, error)
	PutInventory(input *PutInventoryInput) (*StubOutput, error)
	PutResourcePolicy(input *PutResourcePolicyInput) (*PutResourcePolicyOutput, error)
	RegisterDefaultPatchBaseline(
		input *RegisterDefaultPatchBaselineInput,
	) (*RegisterDefaultPatchBaselineOutput, error)
	RegisterPatchBaselineForPatchGroup(
		input *RegisterPatchBaselineForPatchGroupInput,
	) (*RegisterPatchBaselineForPatchGroupOutput, error)
	RegisterTargetWithMaintenanceWindow(
		input *RegisterTargetWithMaintenanceWindowInput,
	) (*RegisterTargetWithMaintenanceWindowOutput, error)
	RegisterTaskWithMaintenanceWindow(
		input *RegisterTaskWithMaintenanceWindowInput,
	) (*RegisterTaskWithMaintenanceWindowOutput, error)
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
	UpdateDocumentDefaultVersion(
		input *UpdateDocumentDefaultVersionInput,
	) (*UpdateDocumentDefaultVersionOutput, error)
	UpdateDocumentMetadata(input *UpdateDocumentMetadataInput) (*StubOutput, error)
	UpdateMaintenanceWindow(input *UpdateMaintenanceWindowInput) (*UpdateMaintenanceWindowOutput, error)
	UpdateMaintenanceWindowTarget(
		input *UpdateMaintenanceWindowTargetInput,
	) (*UpdateMaintenanceWindowTargetOutput, error)
	UpdateMaintenanceWindowTask(input *UpdateMaintenanceWindowTaskInput) (*UpdateMaintenanceWindowTaskOutput, error)
	UpdateManagedInstanceRole(input *UpdateManagedInstanceRoleInput) (*StubOutput, error)
	UpdateOpsItem(input *UpdateOpsItemInput) (*StubOutput, error)
	UpdateOpsMetadata(input *UpdateOpsMetadataInput) (*UpdateOpsMetadataOutput, error)
	UpdatePatchBaseline(input *UpdatePatchBaselineInput) (*UpdatePatchBaselineOutput, error)
	UpdateResourceDataSync(input *UpdateResourceDataSyncInput) (*StubOutput, error)
	UpdateServiceSetting(input *UpdateServiceSettingInput) (*StubOutput, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
