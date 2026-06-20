package ssm

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ParameterInlinePolicy is a lifecycle policy attached to a parameter.
type ParameterInlinePolicy struct {
	PolicyText   string `json:"PolicyText"`
	PolicyType   string `json:"PolicyType"`
	PolicyStatus string `json:"PolicyStatus"`
}

// Parameter represents a single SSM Parameter.
type Parameter struct {
	Name           string     `json:"Name"`
	Type           string     `json:"Type"`
	Value          string     `json:"Value"`
	Tags           *tags.Tags `json:"Tags,omitempty"`
	Description    string     `json:"Description,omitempty"`
	KeyID          string     `json:"KeyId,omitempty"`
	Tier           string     `json:"Tier,omitempty"`
	AllowedPattern string     `json:"AllowedPattern,omitempty"`
	DataType       string     `json:"DataType,omitempty"`
	Policies       string     `json:"Policies,omitempty"`
	// ARN is the Amazon Resource Name of the parameter. Real AWS SSM returns this
	// field on GetParameter, GetParameters, and GetParametersByPath responses.
	ARN string `json:"ARN,omitempty"`
	// Selector is the version or label selector used to retrieve this parameter,
	// e.g. ":3" or ":prod". Empty when the latest version is returned without a
	// selector. AWS echoes the selector back in the GetParameter response.
	Selector         string  `json:"Selector,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
	Version          int64   `json:"Version"`
}

// PutParameterInput represents the request payload for PutParameter.
type PutParameterInput struct {
	Name           string `json:"Name"`
	Type           string `json:"Type"`
	Value          string `json:"Value"`
	Description    string `json:"Description,omitempty"`
	KeyID          string `json:"KeyId,omitempty"`
	Tier           string `json:"Tier,omitempty"`
	AllowedPattern string `json:"AllowedPattern,omitempty"`
	DataType       string `json:"DataType,omitempty"`
	Policies       string `json:"Policies,omitempty"`
	Overwrite      bool   `json:"Overwrite,omitempty"`
}

// PutParameterOutput represents the response payload for PutParameter.
type PutParameterOutput struct {
	Tier    string `json:"Tier,omitempty"`
	Version int64  `json:"Version"`
}

// GetParameterInput represents the request payload for GetParameter.
type GetParameterInput struct {
	Name           string `json:"Name"`
	WithDecryption bool   `json:"WithDecryption,omitempty"`
}

// GetParameterOutput represents the response payload for GetParameter.
type GetParameterOutput struct {
	Parameter Parameter `json:"Parameter"`
}

// GetParametersInput represents the request payload for GetParameters.
type GetParametersInput struct {
	Names          []string `json:"Names"`
	WithDecryption bool     `json:"WithDecryption,omitempty"`
}

// GetParametersOutput represents the response payload for GetParameters.
type GetParametersOutput struct {
	Parameters        []Parameter `json:"Parameters"`
	InvalidParameters []string    `json:"InvalidParameters"`
}

// DeleteParameterInput represents the request payload for DeleteParameter.
type DeleteParameterInput struct {
	Name string `json:"Name"`
}

// DeleteParameterOutput represents the response payload for DeleteParameter.
type DeleteParameterOutput struct{}

// DeleteParametersInput represents the request payload for DeleteParameters.
type DeleteParametersInput struct {
	Names []string `json:"Names"`
}

// DeleteParametersOutput represents the response payload for DeleteParameters.
type DeleteParametersOutput struct {
	DeletedParameters []string `json:"DeletedParameters"`
	InvalidParameters []string `json:"InvalidParameters"`
}

// ParameterHistory represents a historical version of a parameter.
type ParameterHistory struct {
	Name             string   `json:"Name"`
	Type             string   `json:"Type"`
	Value            string   `json:"Value"`
	KeyID            string   `json:"KeyId,omitempty"`
	Tier             string   `json:"Tier,omitempty"`
	AllowedPattern   string   `json:"AllowedPattern,omitempty"`
	DataType         string   `json:"DataType,omitempty"`
	Description      string   `json:"Description,omitempty"`
	Labels           []string `json:"Labels,omitempty"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
	Version          int64    `json:"Version"`
}

// GetParameterHistoryInput represents the request payload for GetParameterHistory.
type GetParameterHistoryInput struct {
	Name           string `json:"Name"`
	MaxResults     *int64 `json:"MaxResults,omitempty"` // 0 to 50, defaults to 50
	NextToken      string `json:"NextToken,omitempty"`
	WithDecryption bool   `json:"WithDecryption,omitempty"`
}

// GetParameterHistoryOutput represents the response payload for GetParameterHistory.
type GetParameterHistoryOutput struct {
	NextToken  string             `json:"NextToken,omitempty"`
	Parameters []ParameterHistory `json:"Parameters"`
}

// ParameterFilter is a filter criterion for parameter queries.
type ParameterFilter struct {
	// Key is the filter key: Name, Type, KeyId, etc.
	Key string `json:"Key"`
	// Option is the comparison operator: Equals, BeginsWith, Contains.
	Option string `json:"Option,omitempty"`
	// Values contains the values to match against.
	Values []string `json:"Values"`
}

// GetParametersByPathInput is the request payload for GetParametersByPath.
type GetParametersByPathInput struct {
	MaxResults       *int64            `json:"MaxResults,omitempty"`
	Path             string            `json:"Path"`
	NextToken        string            `json:"NextToken,omitempty"`
	ParameterFilters []ParameterFilter `json:"ParameterFilters,omitempty"`
	WithDecryption   bool              `json:"WithDecryption,omitempty"`
	Recursive        bool              `json:"Recursive,omitempty"`
}

// GetParametersByPathOutput is the response payload for GetParametersByPath.
type GetParametersByPathOutput struct {
	NextToken  string      `json:"NextToken,omitempty"`
	Parameters []Parameter `json:"Parameters"`
}

// ParameterMetadata contains parameter metadata without the parameter value.
type ParameterMetadata struct {
	Name             string  `json:"Name"`
	Type             string  `json:"Type"`
	Description      string  `json:"Description,omitempty"`
	KeyID            string  `json:"KeyId,omitempty"`
	Tier             string  `json:"Tier,omitempty"`
	AllowedPattern   string  `json:"AllowedPattern,omitempty"`
	DataType         string  `json:"DataType,omitempty"`
	Policies         string  `json:"Policies,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
	Version          int64   `json:"Version"`
}

// DescribeParametersInput is the request payload for DescribeParameters.
type DescribeParametersInput struct {
	MaxResults       *int64            `json:"MaxResults,omitempty"`
	NextToken        string            `json:"NextToken,omitempty"`
	ParameterFilters []ParameterFilter `json:"ParameterFilters,omitempty"`
}

// DescribeParametersOutput is the response payload for DescribeParameters.
type DescribeParametersOutput struct {
	NextToken  string              `json:"NextToken,omitempty"`
	Parameters []ParameterMetadata `json:"Parameters"`
}

// Tag represents a key/value tag pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// AddTagsToResourceInput is the request payload for AddTagsToResource.
type AddTagsToResourceInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
	Tags         []Tag  `json:"Tags"`
}

// RemoveTagsFromResourceInput is the request payload for RemoveTagsFromResource.
type RemoveTagsFromResourceInput struct {
	ResourceType string   `json:"ResourceType"`
	ResourceID   string   `json:"ResourceId"`
	TagKeys      []string `json:"TagKeys"`
}

// ListTagsForResourceInput is the request payload for ListTagsForResource.
type ListTagsForResourceInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

// ListTagsForResourceOutput is the response payload for ListTagsForResource.
type ListTagsForResourceOutput struct {
	TagList []Tag `json:"TagList"`
}

// UnixTimeFloat returns a unix timestamp float required by some AWS SDKs.
const nanoToSeconds = 1e9

func UnixTimeFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / nanoToSeconds
}

// Document type constants.
const (
	DocumentTypeCommand    = "Command"
	DocumentTypeAutomation = "Automation"
	DocumentTypePolicy     = "Policy"
	DocumentTypeSession    = "Session"
)

// AttachmentsSource is a reference to attachments for a document.
type AttachmentsSource struct {
	Key    string   `json:"Key,omitempty"`
	Name   string   `json:"Name,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// DocumentAttachment describes a document attachment.
type DocumentAttachment struct {
	Name string `json:"Name,omitempty"`
	URL  string `json:"Url,omitempty"`
	Hash string `json:"Hash,omitempty"`
	Size int64  `json:"Size,omitempty"`
}

// DocumentRequires describes a document dependency.
type DocumentRequires struct {
	Name    string `json:"Name"`
	Version string `json:"Version,omitempty"`
}

// Document represents an SSM document.
type Document struct {
	TargetType        string               `json:"TargetType,omitempty"`
	LatestVersion     string               `json:"LatestVersion"`
	DocumentType      string               `json:"DocumentType"`
	DocumentFormat    string               `json:"DocumentFormat"`
	Status            string               `json:"Status"`
	StatusInformation string               `json:"StatusInformation,omitempty"`
	DefaultVersion    string               `json:"DefaultVersion"`
	Name              string               `json:"Name"`
	Content           string               `json:"Content"`
	SchemaVersion     string               `json:"SchemaVersion"`
	Description       string               `json:"Description,omitempty"`
	DocumentVersion   string               `json:"DocumentVersion"`
	PlatformTypes     []string             `json:"PlatformTypes,omitempty"`
	Attachments       []DocumentAttachment `json:"Attachments,omitempty"`
	Requires          []DocumentRequires   `json:"Requires,omitempty"`
	CreatedDate       float64              `json:"CreatedDate"`
}

// DocumentVersion represents a specific version of an SSM document.
type DocumentVersion struct {
	Name             string  `json:"Name"`
	DocumentVersion  string  `json:"DocumentVersion"`
	DocumentFormat   string  `json:"DocumentFormat"`
	Status           string  `json:"Status"`
	Content          string  `json:"Content,omitempty"`
	CreatedDate      float64 `json:"CreatedDate"`
	IsDefaultVersion bool    `json:"IsDefaultVersion"`
}

// DocumentPermissionInfo contains the sharing permissions of a document.
type DocumentPermissionInfo struct {
	AccountIDs             []string `json:"AccountIds"`
	AccountSharingInfoList []any    `json:"AccountSharingInfoList"`
}

// DocumentIdentifier is a lightweight document listing entry.
type DocumentIdentifier struct {
	Name            string   `json:"Name"`
	DocumentType    string   `json:"DocumentType"`
	DocumentFormat  string   `json:"DocumentFormat"`
	DocumentVersion string   `json:"DocumentVersion"`
	SchemaVersion   string   `json:"SchemaVersion"`
	PlatformTypes   []string `json:"PlatformTypes,omitempty"`
}

// DocumentFilter is a filter criterion for ListDocuments.
type DocumentFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// CreateDocumentInput is the request payload for CreateDocument.
type CreateDocumentInput struct {
	Name           string              `json:"Name"`
	Content        string              `json:"Content"`
	DocumentType   string              `json:"DocumentType,omitempty"`
	DocumentFormat string              `json:"DocumentFormat,omitempty"`
	TargetType     string              `json:"TargetType,omitempty"`
	Description    string              `json:"Description,omitempty"`
	PlatformTypes  []string            `json:"PlatformTypes,omitempty"`
	Attachments    []AttachmentsSource `json:"Attachments,omitempty"`
	Requires       []DocumentRequires  `json:"Requires,omitempty"`
}

// CreateDocumentOutput is the response payload for CreateDocument.
type CreateDocumentOutput struct {
	DocumentDescription Document `json:"DocumentDescription"`
}

// GetDocumentInput is the request payload for GetDocument.
type GetDocumentInput struct {
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
	DocumentFormat  string `json:"DocumentFormat,omitempty"`
}

// GetDocumentOutput is the response payload for GetDocument.
type GetDocumentOutput struct {
	Name            string `json:"Name"`
	Content         string `json:"Content"`
	DocumentType    string `json:"DocumentType"`
	DocumentFormat  string `json:"DocumentFormat"`
	DocumentVersion string `json:"DocumentVersion"`
	Status          string `json:"Status"`
}

// DescribeDocumentInput is the request payload for DescribeDocument.
type DescribeDocumentInput struct {
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
}

// DescribeDocumentOutput is the response payload for DescribeDocument.
type DescribeDocumentOutput struct {
	Document Document `json:"Document"`
}

// ListDocumentsInput is the request payload for ListDocuments.
type ListDocumentsInput struct {
	MaxResults      *int64           `json:"MaxResults,omitempty"`
	NextToken       string           `json:"NextToken,omitempty"`
	Filters         []DocumentFilter `json:"Filters,omitempty"`
	DocumentFilters []DocumentFilter `json:"DocumentFilters,omitempty"`
}

// ListDocumentsOutput is the response payload for ListDocuments.
type ListDocumentsOutput struct {
	NextToken           string               `json:"NextToken,omitempty"`
	DocumentIdentifiers []DocumentIdentifier `json:"DocumentIdentifiers"`
}

// UpdateDocumentInput is the request payload for UpdateDocument.
type UpdateDocumentInput struct {
	Name            string `json:"Name"`
	Content         string `json:"Content"`
	DocumentFormat  string `json:"DocumentFormat,omitempty"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
}

// UpdateDocumentOutput is the response payload for UpdateDocument.
type UpdateDocumentOutput struct {
	DocumentDescription Document `json:"DocumentDescription"`
}

// DeleteDocumentInput is the request payload for DeleteDocument.
type DeleteDocumentInput struct {
	Name string `json:"Name"`
}

// DeleteDocumentOutput is the response payload for DeleteDocument.
type DeleteDocumentOutput struct{}

// DescribeDocumentPermissionInput is the request payload for DescribeDocumentPermission.
type DescribeDocumentPermissionInput struct {
	Name           string `json:"Name"`
	PermissionType string `json:"PermissionType"`
}

// DescribeDocumentPermissionOutput is the response payload for DescribeDocumentPermission.
type DescribeDocumentPermissionOutput struct {
	AccountIDs             []string `json:"AccountIds"`
	AccountSharingInfoList []any    `json:"AccountSharingInfoList"`
}

// ModifyDocumentPermissionInput is the request payload for ModifyDocumentPermission.
type ModifyDocumentPermissionInput struct {
	Name               string   `json:"Name"`
	PermissionType     string   `json:"PermissionType"`
	AccountIDsToAdd    []string `json:"AccountIdsToAdd,omitempty"`
	AccountIDsToRemove []string `json:"AccountIdsToRemove,omitempty"`
}

// ModifyDocumentPermissionOutput is the response payload for ModifyDocumentPermission.
type ModifyDocumentPermissionOutput struct{}

// ListDocumentVersionsInput is the request payload for ListDocumentVersions.
type ListDocumentVersionsInput struct {
	Name       string `json:"Name"`
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListDocumentVersionsOutput is the response payload for ListDocumentVersions.
type ListDocumentVersionsOutput struct {
	NextToken        string            `json:"NextToken,omitempty"`
	DocumentVersions []DocumentVersion `json:"DocumentVersions"`
}

// Command represents a recorded SSM command.
type Command struct {
	Parameters         map[string][]string `json:"Parameters,omitempty"`
	CommandID          string              `json:"CommandId"`
	DocumentName       string              `json:"DocumentName"`
	Status             string              `json:"Status"`
	Comment            string              `json:"Comment,omitempty"`
	OutputS3BucketName string              `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix  string              `json:"OutputS3KeyPrefix,omitempty"`
	OutputS3Region     string              `json:"OutputS3Region,omitempty"`
	StatusDetails      string              `json:"StatusDetails,omitempty"`
	InstanceIDs        []string            `json:"InstanceIds,omitempty"`
	Targets            []any               `json:"Targets,omitempty"`
	RequestedDateTime  float64             `json:"RequestedDateTime"`
	ExpiresAfter       float64             `json:"ExpiresAfter"`
	TimeoutSeconds     int32               `json:"TimeoutSeconds,omitempty"`
}

// CommandInvocation represents the invocation of a command on an instance.
type CommandInvocation struct {
	CommandID             string  `json:"CommandId"`
	InstanceID            string  `json:"InstanceId"`
	DocumentName          string  `json:"DocumentName"`
	Status                string  `json:"Status"`
	StatusDetails         string  `json:"StatusDetails"`
	StandardOutputContent string  `json:"StandardOutputContent,omitempty"`
	StandardErrorContent  string  `json:"StandardErrorContent,omitempty"`
	StandardOutputURL     string  `json:"StandardOutputUrl,omitempty"`
	StandardErrorURL      string  `json:"StandardErrorUrl,omitempty"`
	Comment               string  `json:"Comment,omitempty"`
	RequestedDateTime     float64 `json:"RequestedDateTime"`
}

// SendCommandInput is the request payload for SendCommand.
type SendCommandInput struct {
	Parameters         map[string][]string `json:"Parameters,omitempty"`
	DocumentName       string              `json:"DocumentName"`
	Comment            string              `json:"Comment,omitempty"`
	OutputS3BucketName string              `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix  string              `json:"OutputS3KeyPrefix,omitempty"`
	OutputS3Region     string              `json:"OutputS3Region,omitempty"`
	InstanceIDs        []string            `json:"InstanceIds,omitempty"`
	Targets            []any               `json:"Targets,omitempty"`
	TimeoutSeconds     int32               `json:"TimeoutSeconds,omitempty"`
}

// SendCommandOutput is the response payload for SendCommand.
type SendCommandOutput struct {
	Command Command `json:"Command"`
}

// ListCommandsInput is the request payload for ListCommands.
type ListCommandsInput struct {
	CommandID  string `json:"CommandId,omitempty"`
	InstanceID string `json:"InstanceId,omitempty"`
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListCommandsOutput is the response payload for ListCommands.
type ListCommandsOutput struct {
	NextToken string    `json:"NextToken,omitempty"`
	Commands  []Command `json:"Commands"`
}

// GetCommandInvocationInput is the request payload for GetCommandInvocation.
type GetCommandInvocationInput struct {
	CommandID  string `json:"CommandId"`
	InstanceID string `json:"InstanceId"`
}

// GetCommandInvocationOutput is the response payload for GetCommandInvocation.
type GetCommandInvocationOutput struct {
	CommandID             string `json:"CommandId"`
	InstanceID            string `json:"InstanceId"`
	DocumentName          string `json:"DocumentName"`
	Status                string `json:"Status"`
	StatusDetails         string `json:"StatusDetails"`
	StandardOutputContent string `json:"StandardOutputContent,omitempty"`
	StandardErrorContent  string `json:"StandardErrorContent,omitempty"`
	StandardOutputURL     string `json:"StandardOutputUrl,omitempty"`
	StandardErrorURL      string `json:"StandardErrorUrl,omitempty"`
	Comment               string `json:"Comment,omitempty"`
}

// ListCommandInvocationsInput is the request payload for ListCommandInvocations.
type ListCommandInvocationsInput struct {
	CommandID  string `json:"CommandId,omitempty"`
	InstanceID string `json:"InstanceId,omitempty"`
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListCommandInvocationsOutput is the response payload for ListCommandInvocations.
type ListCommandInvocationsOutput struct {
	NextToken          string              `json:"NextToken,omitempty"`
	CommandInvocations []CommandInvocation `json:"CommandInvocations"`
}

// Activation represents an SSM activation for managed instances.
type Activation struct {
	ActivationID        string  `json:"ActivationId"`
	ActivationCode      string  `json:"ActivationCode"`
	Description         string  `json:"Description,omitempty"`
	DefaultInstanceName string  `json:"DefaultInstanceName,omitempty"`
	IamRole             string  `json:"IamRole"`
	RegistrationLimit   int32   `json:"RegistrationLimit"`
	RegistrationsCount  int32   `json:"RegistrationsCount"`
	ExpirationDate      float64 `json:"ExpirationDate"`
	Expired             bool    `json:"Expired"`
	CreatedDate         float64 `json:"CreatedDate"`
}

// CreateActivationInput is the request payload for CreateActivation.
type CreateActivationInput struct {
	DefaultInstanceName string  `json:"DefaultInstanceName,omitempty"`
	Description         string  `json:"Description,omitempty"`
	IamRole             string  `json:"IamRole"`
	Tags                []Tag   `json:"Tags,omitempty"`
	ExpirationDate      float64 `json:"ExpirationDate,omitempty"`
	RegistrationLimit   int32   `json:"RegistrationLimit,omitempty"`
}

// CreateActivationOutput is the response payload for CreateActivation.
type CreateActivationOutput struct {
	ActivationCode string `json:"ActivationCode"`
	ActivationID   string `json:"ActivationId"`
}

// AssociationTarget is a target for an association (key/values).
type AssociationTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// Association represents an SSM association between a document and targets.
type Association struct {
	AssociationID             string               `json:"AssociationId"`
	AssociationName           string               `json:"AssociationName,omitempty"`
	DocumentVersion           string               `json:"DocumentVersion,omitempty"`
	InstanceID                string               `json:"InstanceId,omitempty"`
	Name                      string               `json:"Name"`
	Overview                  *AssociationOverview `json:"Overview,omitempty"`
	Parameters                map[string][]string  `json:"Parameters,omitempty"`
	Targets                   []AssociationTarget  `json:"Targets,omitempty"`
	LastUpdateAssociationDate float64              `json:"LastUpdateAssociationDate"`
}

// AssociationOverview is a summary of an association.
type AssociationOverview struct {
	Status string `json:"Status"`
}

// CreateAssociationInput is the request payload for CreateAssociation.
type CreateAssociationInput struct {
	Name            string              `json:"Name"`
	AssociationName string              `json:"AssociationName,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	InstanceID      string              `json:"InstanceId,omitempty"`
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	Targets         []AssociationTarget `json:"Targets,omitempty"`
}

// CreateAssociationOutput is the response payload for CreateAssociation.
type CreateAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// CreateAssociationBatchRequestEntry is a single entry in a batch create association request.
type CreateAssociationBatchRequestEntry struct {
	Name            string              `json:"Name"`
	AssociationName string              `json:"AssociationName,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	InstanceID      string              `json:"InstanceId,omitempty"`
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	Targets         []AssociationTarget `json:"Targets,omitempty"`
}

// FailedCreateAssociation represents a failed association entry in a batch.
type FailedCreateAssociation struct {
	Message string                             `json:"Message"`
	Fault   string                             `json:"Fault"`
	Entry   CreateAssociationBatchRequestEntry `json:"Entry"`
}

// CreateAssociationBatchInput is the request payload for CreateAssociationBatch.
type CreateAssociationBatchInput struct {
	Entries []CreateAssociationBatchRequestEntry `json:"Entries"`
}

// CreateAssociationBatchOutput is the response payload for CreateAssociationBatch.
type CreateAssociationBatchOutput struct {
	Failed     []FailedCreateAssociation `json:"Failed"`
	Successful []Association             `json:"Successful"`
}

// MaintenanceWindow represents an SSM maintenance window.
type MaintenanceWindow struct {
	WindowID                 string  `json:"WindowId"`
	Name                     string  `json:"Name"`
	Description              string  `json:"Description,omitempty"`
	Schedule                 string  `json:"Schedule"`
	Duration                 int32   `json:"Duration"`
	Cutoff                   int32   `json:"Cutoff"`
	AllowUnassociatedTargets bool    `json:"AllowUnassociatedTargets"`
	Enabled                  bool    `json:"Enabled"`
	CreatedDate              float64 `json:"CreatedDate"`
	ModifiedDate             float64 `json:"ModifiedDate"`
}

// CreateMaintenanceWindowInput is the request payload for CreateMaintenanceWindow.
type CreateMaintenanceWindowInput struct {
	Name                     string `json:"Name"`
	Description              string `json:"Description,omitempty"`
	Schedule                 string `json:"Schedule"`
	Tags                     []Tag  `json:"Tags,omitempty"`
	Duration                 int32  `json:"Duration"`
	Cutoff                   int32  `json:"Cutoff"`
	AllowUnassociatedTargets bool   `json:"AllowUnassociatedTargets"`
}

// CreateMaintenanceWindowOutput is the response payload for CreateMaintenanceWindow.
type CreateMaintenanceWindowOutput struct {
	WindowID string `json:"WindowId"`
}

// CancelMaintenanceWindowExecutionInput is the request payload for CancelMaintenanceWindowExecution.
type CancelMaintenanceWindowExecutionInput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
}

// CancelMaintenanceWindowExecutionOutput is the response payload for CancelMaintenanceWindowExecution.
type CancelMaintenanceWindowExecutionOutput struct {
	WindowExecutionID string `json:"WindowExecutionId"`
}

// CancelCommandInput is the request payload for CancelCommand.
type CancelCommandInput struct {
	CommandID   string   `json:"CommandId"`
	InstanceIDs []string `json:"InstanceIds,omitempty"`
}

// CancelCommandOutput is the response payload for CancelCommand.
type CancelCommandOutput struct{}

// OpsItemDataValue represents a value in OpsItem OperationalData.
type OpsItemDataValue struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

// OpsItem represents an SSM OpsItem.
type OpsItem struct {
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	OpsItemID        string                      `json:"OpsItemId"`
	OpsItemArn       string                      `json:"OpsItemArn,omitempty"`
	OpsItemType      string                      `json:"OpsItemType,omitempty"`
	Title            string                      `json:"Title"`
	Source           string                      `json:"Source"`
	Description      string                      `json:"Description,omitempty"`
	Status           string                      `json:"Status"`
	Severity         string                      `json:"Severity,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	CreatedTime      float64                     `json:"CreatedTime"`
	LastModifiedTime float64                     `json:"LastModifiedTime"`
}

// OpsItemRelatedItem represents an item related to an OpsItem.
type OpsItemRelatedItem struct {
	AssociationID   string `json:"AssociationId"`
	AssociationType string `json:"AssociationType"`
	ResourceType    string `json:"ResourceType"`
	ResourceURI     string `json:"ResourceUri"`
}

// CreateOpsItemInput is the request payload for CreateOpsItem.
type CreateOpsItemInput struct {
	Title           string                      `json:"Title"`
	Source          string                      `json:"Source"`
	Description     string                      `json:"Description,omitempty"`
	OpsItemType     string                      `json:"OpsItemType,omitempty"`
	Severity        string                      `json:"Severity,omitempty"`
	Category        string                      `json:"Category,omitempty"`
	OperationalData map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	Tags            []Tag                       `json:"Tags,omitempty"`
}

// CreateOpsItemOutput is the response payload for CreateOpsItem.
type CreateOpsItemOutput struct {
	OpsItemArn string `json:"OpsItemArn,omitempty"`
	OpsItemID  string `json:"OpsItemId"`
}

// AssociateOpsItemRelatedItemInput is the request payload for AssociateOpsItemRelatedItem.
type AssociateOpsItemRelatedItemInput struct {
	OpsItemID       string `json:"OpsItemId"`
	AssociationType string `json:"AssociationType"`
	ResourceType    string `json:"ResourceType"`
	ResourceURI     string `json:"ResourceUri"`
}

// AssociateOpsItemRelatedItemOutput is the response payload for AssociateOpsItemRelatedItem.
type AssociateOpsItemRelatedItemOutput struct {
	AssociationID string `json:"AssociationId"`
}

// MetadataValue represents a single metadata entry value.
type MetadataValue struct {
	Value string `json:"Value,omitempty"`
}

// OpsMetadata represents SSM OpsMetadata for a resource.
type OpsMetadata struct {
	Metadata         map[string]MetadataValue `json:"Metadata,omitempty"`
	OpsMetadataArn   string                   `json:"OpsMetadataArn"`
	ResourceID       string                   `json:"ResourceId"`
	CreationDate     float64                  `json:"CreationDate"`
	LastModifiedDate float64                  `json:"LastModifiedDate"`
}

// CreateOpsMetadataInput is the request payload for CreateOpsMetadata.
type CreateOpsMetadataInput struct {
	ResourceID string                   `json:"ResourceId"`
	Metadata   map[string]MetadataValue `json:"Metadata,omitempty"`
	Tags       []Tag                    `json:"Tags,omitempty"`
}

// CreateOpsMetadataOutput is the response payload for CreateOpsMetadata.
type CreateOpsMetadataOutput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// PatchBaseline represents an SSM patch baseline.
type PatchBaseline struct {
	BaselineID                     string   `json:"BaselineId"`
	Name                           string   `json:"Name"`
	Description                    string   `json:"Description,omitempty"`
	OperatingSystem                string   `json:"OperatingSystem,omitempty"`
	ApprovedPatchesComplianceLevel string   `json:"ApprovedPatchesComplianceLevel,omitempty"`
	ApprovedPatches                []string `json:"ApprovedPatches,omitempty"`
	RejectedPatches                []string `json:"RejectedPatches,omitempty"`
	CreatedDate                    float64  `json:"CreatedDate"`
	ModifiedDate                   float64  `json:"ModifiedDate"`
}

// CreatePatchBaselineInput is the request payload for CreatePatchBaseline.
type CreatePatchBaselineInput struct {
	Name                           string   `json:"Name"`
	Description                    string   `json:"Description,omitempty"`
	OperatingSystem                string   `json:"OperatingSystem,omitempty"`
	ApprovedPatches                []string `json:"ApprovedPatches,omitempty"`
	RejectedPatches                []string `json:"RejectedPatches,omitempty"`
	ApprovedPatchesComplianceLevel string   `json:"ApprovedPatchesComplianceLevel,omitempty"`
	Tags                           []Tag    `json:"Tags,omitempty"`
}

// CreatePatchBaselineOutput is the response payload for CreatePatchBaseline.
type CreatePatchBaselineOutput struct {
	BaselineID string `json:"BaselineId"`
}

// MaintenanceWindowTarget represents a registered target for a maintenance window.
type MaintenanceWindowTarget struct {
	WindowID       string         `json:"WindowId"`
	WindowTargetID string         `json:"WindowTargetId"`
	ResourceType   string         `json:"ResourceType"`
	OwnerInfo      string         `json:"OwnerInfo,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// MaintenanceWindowTask represents a registered task for a maintenance window.
type MaintenanceWindowTask struct {
	WindowID       string `json:"WindowId"`
	WindowTaskID   string `json:"WindowTaskId"`
	TaskArn        string `json:"TaskArn"`
	TaskType       string `json:"TaskType"`
	Name           string `json:"Name,omitempty"`
	Description    string `json:"Description,omitempty"`
	ServiceRoleArn string `json:"ServiceRoleArn,omitempty"`
	MaxConcurrency string `json:"MaxConcurrency,omitempty"`
	MaxErrors      string `json:"MaxErrors,omitempty"`
	Priority       int32  `json:"Priority,omitempty"`
}

// SessionOutputS3 holds S3 output configuration for a session.
type SessionOutputS3 struct {
	S3BucketName string `json:"S3BucketName,omitempty"`
	S3KeyPrefix  string `json:"S3KeyPrefix,omitempty"`
}

// Session represents an SSM Session Manager session.
type Session struct {
	OutputURL               *SessionOutputS3    `json:"OutputUrl,omitempty"`
	Parameters              map[string][]string `json:"Parameters,omitempty"`
	SessionID               string              `json:"SessionId"`
	Target                  string              `json:"Target"`
	Status                  string              `json:"Status"`
	StreamURL               string              `json:"StreamUrl"`
	TokenValue              string              `json:"TokenValue"`
	Owner                   string              `json:"Owner,omitempty"`
	Reason                  string              `json:"Reason,omitempty"`
	DocumentName            string              `json:"DocumentName,omitempty"`
	CloudWatchLogGroupName  string              `json:"CloudWatchLogGroupName,omitempty"`
	StartDate               float64             `json:"StartDate"`
	EndDate                 float64             `json:"EndDate,omitempty"`
	CloudWatchOutputEnabled bool                `json:"CloudWatchOutputEnabled,omitempty"`
}
