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

// Document represents an SSM Document.
type Document struct {
	Permissions     map[string]string `json:"-"`
	Status          string            `json:"Status"`
	LatestVersion   string            `json:"LatestVersion"`
	Content         string            `json:"Content"`
	Description     string            `json:"Description,omitempty"`
	Owner           string            `json:"Owner,omitempty"`
	Name            string            `json:"Name"`
	DocumentVersion string            `json:"DocumentVersion"`
	DocumentFormat  string            `json:"DocumentFormat"`
	DefaultVersion  string            `json:"DefaultVersion"`
	SchemaVersion   string            `json:"SchemaVersion,omitempty"`
	DocumentType    string            `json:"DocumentType"`
	Parameters      []DocumentParam   `json:"Parameters,omitempty"`
	Tags            []Tag             `json:"Tags,omitempty"`
	CreatedDate     float64           `json:"CreatedDate"`
}

// DocumentParam describes a parameter in an SSM document.
type DocumentParam struct {
	Name         string `json:"Name"`
	Type         string `json:"Type"`
	DefaultValue string `json:"DefaultValue,omitempty"`
	Description  string `json:"Description,omitempty"`
}

// DocumentVersion represents a version of an SSM Document.
type DocumentVersion struct {
	Name             string  `json:"Name"`
	DocumentVersion  string  `json:"DocumentVersion"`
	DocumentFormat   string  `json:"DocumentFormat,omitempty"`
	Status           string  `json:"Status"`
	CreatedDate      float64 `json:"CreatedDate"`
	IsDefaultVersion bool    `json:"IsDefaultVersion"`
}

// DocumentIdentifier summarises a document for listing.
type DocumentIdentifier struct {
	Name            string `json:"Name"`
	DocumentType    string `json:"DocumentType"`
	DocumentFormat  string `json:"DocumentFormat,omitempty"`
	Owner           string `json:"Owner,omitempty"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
	SchemaVersion   string `json:"SchemaVersion,omitempty"`
}

// AccountSharingInfo holds account sharing permission for a document.
type AccountSharingInfo struct {
	AccountID             string `json:"AccountId"`
	SharedDocumentVersion string `json:"SharedDocumentVersion,omitempty"`
}

// CreateDocumentInput is the request payload for CreateDocument.
type CreateDocumentInput struct {
	Name           string `json:"Name"`
	Content        string `json:"Content"`
	DocumentType   string `json:"DocumentType,omitempty"`
	DocumentFormat string `json:"DocumentFormat,omitempty"`
	Description    string `json:"Description,omitempty"`
	Tags           []Tag  `json:"Tags,omitempty"`
}

// CreateDocumentOutput is the response payload for CreateDocument.
type CreateDocumentOutput struct {
	DocumentDescription DocumentDescription `json:"DocumentDescription"`
}

// DocumentDescription is returned when creating or describing a document.
type DocumentDescription struct {
	Name            string          `json:"Name"`
	DocumentType    string          `json:"DocumentType"`
	DocumentFormat  string          `json:"DocumentFormat"`
	Description     string          `json:"Description,omitempty"`
	Owner           string          `json:"Owner,omitempty"`
	Status          string          `json:"Status"`
	DocumentVersion string          `json:"DocumentVersion"`
	LatestVersion   string          `json:"LatestVersion"`
	DefaultVersion  string          `json:"DefaultVersion"`
	SchemaVersion   string          `json:"SchemaVersion,omitempty"`
	Tags            []Tag           `json:"Tags,omitempty"`
	Parameters      []DocumentParam `json:"Parameters,omitempty"`
	CreatedDate     float64         `json:"CreatedDate"`
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
	Document DocumentDescription `json:"Document"`
}

// ListDocumentsInput is the request payload for ListDocuments.
type ListDocumentsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
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
	DocumentDescription DocumentDescription `json:"DocumentDescription"`
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
	AccountIDs         []string             `json:"AccountIds"`
	AccountSharingInfo []AccountSharingInfo `json:"AccountSharingInfoList"`
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
	MaxResults *int64 `json:"MaxResults,omitempty"`
	Name       string `json:"Name"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListDocumentVersionsOutput is the response payload for ListDocumentVersions.
type ListDocumentVersionsOutput struct {
	NextToken        string            `json:"NextToken,omitempty"`
	DocumentVersions []DocumentVersion `json:"DocumentVersions"`
}

// Command represents a recorded SSM SendCommand result.
type Command struct {
	CommandID         string   `json:"CommandId"`
	DocumentName      string   `json:"DocumentName"`
	Status            string   `json:"Status"`
	Comment           string   `json:"Comment,omitempty"`
	InstanceIDs       []string `json:"InstanceIds,omitempty"`
	RequestedDateTime float64  `json:"RequestedDateTime"`
}

// SendCommandInput is the request payload for SendCommand.
type SendCommandInput struct {
	Parameters   map[string][]string `json:"Parameters,omitempty"`
	DocumentName string              `json:"DocumentName"`
	Comment      string              `json:"Comment,omitempty"`
	InstanceIDs  []string            `json:"InstanceIds,omitempty"`
}

// SendCommandOutput is the response payload for SendCommand.
type SendCommandOutput struct {
	Command Command `json:"Command"`
}

// ListCommandsInput is the request payload for ListCommands.
type ListCommandsInput struct {
	CommandID  string `json:"CommandId,omitempty"`
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
}

// CommandInvocation represents a single command invocation record.
type CommandInvocation struct {
	CommandID         string  `json:"CommandId"`
	InstanceID        string  `json:"InstanceId"`
	DocumentName      string  `json:"DocumentName"`
	Status            string  `json:"Status"`
	RequestedDateTime float64 `json:"RequestedDateTime"`
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

// UnixTimeFloat returns a unix timestamp float required by some AWS SDKs.
const nanoToSeconds = 1e9

func UnixTimeFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / nanoToSeconds
}
