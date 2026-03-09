package ssm

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Parameter represents a single SSM Parameter.
type Parameter struct {
	Name             string     `json:"Name"`
	Type             string     `json:"Type"`
	Value            string     `json:"Value"`
	Tags             *tags.Tags `json:"Tags,omitempty"`
	Description      string     `json:"Description,omitempty"`
	Version          int64      `json:"Version"`
	LastModifiedDate float64    `json:"LastModifiedDate"`
}

// PutParameterInput represents the request payload for PutParameter.
type PutParameterInput struct {
	Name        string `json:"Name"`
	Type        string `json:"Type"`
	Value       string `json:"Value"`
	Description string `json:"Description,omitempty"`
	Overwrite   bool   `json:"Overwrite,omitempty"`
}

// PutParameterOutput represents the response payload for PutParameter.
type PutParameterOutput struct {
	Version int64 `json:"Version"`
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
	Labels           []string `json:"Labels,omitempty"`
	Version          int64    `json:"Version"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
}

// GetParameterHistoryInput represents the request payload for GetParameterHistory.
type GetParameterHistoryInput struct {
	Name       string `json:"Name"`
	MaxResults *int64 `json:"MaxResults,omitempty"` // 0 to 50, defaults to 50
	NextToken  string `json:"NextToken,omitempty"`
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
	Version          int64   `json:"Version"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
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

// Document represents an SSM document.
type Document struct {
	TargetType        string   `json:"TargetType,omitempty"`
	LatestVersion     string   `json:"LatestVersion"`
	DocumentType      string   `json:"DocumentType"`
	DocumentFormat    string   `json:"DocumentFormat"`
	Status            string   `json:"Status"`
	StatusInformation string   `json:"StatusInformation,omitempty"`
	DefaultVersion    string   `json:"DefaultVersion"`
	Name              string   `json:"Name"`
	Content           string   `json:"Content"`
	SchemaVersion     string   `json:"SchemaVersion"`
	Description       string   `json:"Description,omitempty"`
	DocumentVersion   string   `json:"DocumentVersion"`
	PlatformTypes     []string `json:"PlatformTypes,omitempty"`
	CreatedDate       float64  `json:"CreatedDate"`
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
	Name           string   `json:"Name"`
	Content        string   `json:"Content"`
	DocumentType   string   `json:"DocumentType,omitempty"`
	DocumentFormat string   `json:"DocumentFormat,omitempty"`
	TargetType     string   `json:"TargetType,omitempty"`
	Description    string   `json:"Description,omitempty"`
	PlatformTypes  []string `json:"PlatformTypes,omitempty"`
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
	Parameters        map[string][]string `json:"Parameters,omitempty"`
	CommandID         string              `json:"CommandId"`
	DocumentName      string              `json:"DocumentName"`
	Status            string              `json:"Status"`
	Comment           string              `json:"Comment,omitempty"`
	InstanceIDs       []string            `json:"InstanceIds,omitempty"`
	Targets           []any               `json:"Targets,omitempty"`
	RequestedDateTime float64             `json:"RequestedDateTime"`
	ExpiresAfter      float64             `json:"ExpiresAfter"`
}

// CommandInvocation represents the invocation of a command on an instance.
type CommandInvocation struct {
	CommandID         string  `json:"CommandId"`
	InstanceID        string  `json:"InstanceId"`
	DocumentName      string  `json:"DocumentName"`
	Status            string  `json:"Status"`
	StatusDetails     string  `json:"StatusDetails"`
	RequestedDateTime float64 `json:"RequestedDateTime"`
}

// SendCommandInput is the request payload for SendCommand.
type SendCommandInput struct {
	DocumentName string              `json:"DocumentName"`
	InstanceIDs  []string            `json:"InstanceIds,omitempty"`
	Parameters   map[string][]string `json:"Parameters,omitempty"`
	Comment      string              `json:"Comment,omitempty"`
	Targets      []any               `json:"Targets,omitempty"`
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
	CommandID     string `json:"CommandId"`
	InstanceID    string `json:"InstanceId"`
	DocumentName  string `json:"DocumentName"`
	Status        string `json:"Status"`
	StatusDetails string `json:"StatusDetails"`
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
