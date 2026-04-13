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
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
