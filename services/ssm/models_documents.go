package ssm

// UpdateDocumentMetadataOutput is the response for UpdateDocumentMetadata.
type UpdateDocumentMetadataOutput struct{}

// AttachmentsSource is a reference to attachments for a document.
type AttachmentsSource struct {
	Key    string   `json:"Key,omitempty"`
	Name   string   `json:"Name,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// AttachmentInformation is the wire shape of one entry in
// DocumentDescription.AttachmentsInformation (aws-sdk-go-v2/service/ssm@v1.73.4
// types.AttachmentInformation) -- it carries only the attachment's Name.
// Hash/Size/Url belong to a different type, AttachmentContent, returned by
// GetDocument's AttachmentsContent -- not modeled here, since deriving a real
// hash/size/URL would mean fabricating S3 object state this emulator has no
// backing store for.
type AttachmentInformation struct {
	Name string `json:"Name,omitempty"`
}

// DocumentRequires describes a document dependency.
type DocumentRequires struct {
	Name    string `json:"Name"`
	Version string `json:"Version,omitempty"`
}

// Document represents an SSM document.
type Document struct {
	TargetType             string                  `json:"TargetType,omitempty"`
	LatestVersion          string                  `json:"LatestVersion"`
	DocumentType           string                  `json:"DocumentType"`
	DocumentFormat         string                  `json:"DocumentFormat"`
	Status                 string                  `json:"Status"`
	StatusInformation      string                  `json:"StatusInformation,omitempty"`
	DefaultVersion         string                  `json:"DefaultVersion"`
	Name                   string                  `json:"Name"`
	DisplayName            string                  `json:"DisplayName,omitempty"`
	Content                string                  `json:"Content"`
	SchemaVersion          string                  `json:"SchemaVersion"`
	Description            string                  `json:"Description,omitempty"`
	DocumentVersion        string                  `json:"DocumentVersion"`
	Hash                   string                  `json:"Hash,omitempty"`
	HashType               string                  `json:"HashType,omitempty"`
	Sha1                   string                  `json:"Sha1,omitempty"`
	PlatformTypes          []string                `json:"PlatformTypes,omitempty"`
	AttachmentsInformation []AttachmentInformation `json:"AttachmentsInformation,omitempty"`
	Requires               []DocumentRequires      `json:"Requires,omitempty"`
	CreatedDate            float64                 `json:"CreatedDate"`
}

// DocumentDescription is the document metadata shape returned by
// CreateDocument, UpdateDocument, and DescribeDocument. Unlike Document (the
// internal storage representation), AWS's real DocumentDescription structure
// deliberately omits Content — only GetDocument returns document content, to
// avoid every metadata call re-transmitting potentially large document bodies.
type DocumentDescription struct {
	TargetType             string                  `json:"TargetType,omitempty"`
	LatestVersion          string                  `json:"LatestVersion"`
	DocumentType           string                  `json:"DocumentType"`
	DocumentFormat         string                  `json:"DocumentFormat"`
	Status                 string                  `json:"Status"`
	StatusInformation      string                  `json:"StatusInformation,omitempty"`
	DefaultVersion         string                  `json:"DefaultVersion"`
	Name                   string                  `json:"Name"`
	DisplayName            string                  `json:"DisplayName,omitempty"`
	SchemaVersion          string                  `json:"SchemaVersion"`
	Description            string                  `json:"Description,omitempty"`
	DocumentVersion        string                  `json:"DocumentVersion"`
	Hash                   string                  `json:"Hash,omitempty"`
	HashType               string                  `json:"HashType,omitempty"`
	Sha1                   string                  `json:"Sha1,omitempty"`
	PlatformTypes          []string                `json:"PlatformTypes,omitempty"`
	AttachmentsInformation []AttachmentInformation `json:"AttachmentsInformation,omitempty"`
	Requires               []DocumentRequires      `json:"Requires,omitempty"`
	Tags                   []Tag                   `json:"Tags,omitempty"`
	CreatedDate            float64                 `json:"CreatedDate"`
}

// DocumentVersion represents a specific version of an SSM document.
type DocumentVersion struct {
	Name             string  `json:"Name"`
	DisplayName      string  `json:"DisplayName,omitempty"`
	DocumentVersion  string  `json:"DocumentVersion"`
	DocumentFormat   string  `json:"DocumentFormat"`
	Status           string  `json:"Status"`
	Content          string  `json:"Content,omitempty"`
	CreatedDate      float64 `json:"CreatedDate"`
	IsDefaultVersion bool    `json:"IsDefaultVersion"`
}

// DocumentIdentifier is a lightweight document listing entry.
type DocumentIdentifier struct {
	Name            string             `json:"Name"`
	DisplayName     string             `json:"DisplayName,omitempty"`
	DocumentType    string             `json:"DocumentType"`
	DocumentFormat  string             `json:"DocumentFormat"`
	DocumentVersion string             `json:"DocumentVersion"`
	SchemaVersion   string             `json:"SchemaVersion"`
	TargetType      string             `json:"TargetType,omitempty"`
	PlatformTypes   []string           `json:"PlatformTypes,omitempty"`
	Requires        []DocumentRequires `json:"Requires,omitempty"`
	Tags            []Tag              `json:"Tags,omitempty"`
	CreatedDate     float64            `json:"CreatedDate"`
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
	DisplayName    string              `json:"DisplayName,omitempty"`
	DocumentType   string              `json:"DocumentType,omitempty"`
	DocumentFormat string              `json:"DocumentFormat,omitempty"`
	TargetType     string              `json:"TargetType,omitempty"`
	Description    string              `json:"Description,omitempty"`
	PlatformTypes  []string            `json:"PlatformTypes,omitempty"`
	Attachments    []AttachmentsSource `json:"Attachments,omitempty"`
	Requires       []DocumentRequires  `json:"Requires,omitempty"`
	Tags           []Tag               `json:"Tags,omitempty"`
}

// CreateDocumentOutput is the response payload for CreateDocument.
type CreateDocumentOutput struct {
	DocumentDescription DocumentDescription `json:"DocumentDescription"`
}

// GetDocumentInput is the request payload for GetDocument.
type GetDocumentInput struct {
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
	DocumentFormat  string `json:"DocumentFormat,omitempty"`
}

// GetDocumentOutput is the response payload for GetDocument.
type GetDocumentOutput struct {
	Name              string             `json:"Name"`
	Content           string             `json:"Content"`
	DisplayName       string             `json:"DisplayName,omitempty"`
	DocumentType      string             `json:"DocumentType"`
	DocumentFormat    string             `json:"DocumentFormat"`
	DocumentVersion   string             `json:"DocumentVersion"`
	Status            string             `json:"Status"`
	StatusInformation string             `json:"StatusInformation,omitempty"`
	Requires          []DocumentRequires `json:"Requires,omitempty"`
	CreatedDate       float64            `json:"CreatedDate"`
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
	Name            string              `json:"Name"`
	Content         string              `json:"Content"`
	DisplayName     string              `json:"DisplayName,omitempty"`
	DocumentFormat  string              `json:"DocumentFormat,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	TargetType      string              `json:"TargetType,omitempty"`
	Attachments     []AttachmentsSource `json:"Attachments,omitempty"`
}

// UpdateDocumentOutput is the response payload for UpdateDocument.
type UpdateDocumentOutput struct {
	DocumentDescription DocumentDescription `json:"DocumentDescription"`
}

// DeleteDocumentInput is the request payload for DeleteDocument. DocumentVersion/
// VersionName scope the delete to a single version (aws-sdk-go-v2/service/
// ssm@v1.73.4 api_op_DeleteDocument.go:34-49: "If not provided, all versions of
// the document are deleted"); omitting both deletes every version. Force is
// parsed but not consulted -- real AWS requires it only to delete a document of
// type ApplicationConfigurationSchema, which this backend does not model.
type DeleteDocumentInput struct {
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
	VersionName     string `json:"VersionName,omitempty"`
	Force           bool   `json:"Force,omitempty"`
}

// DeleteDocumentOutput is the response payload for DeleteDocument.
type DeleteDocumentOutput struct{}

// DescribeDocumentPermissionInput is the request payload for DescribeDocumentPermission.
type DescribeDocumentPermissionInput struct {
	Name           string `json:"Name"`
	PermissionType string `json:"PermissionType"`
	MaxResults     *int64 `json:"MaxResults,omitempty"`
	NextToken      string `json:"NextToken,omitempty"`
}

// AccountSharingInfo is the wire shape of one DescribeDocumentPermissionOutput.
// AccountSharingInfoList entry (aws-sdk-go-v2/service/ssm@v1.73.4
// types.AccountSharingInfo: AccountId, SharedDocumentVersion).
type AccountSharingInfo struct {
	AccountID             string `json:"AccountId,omitempty"`
	SharedDocumentVersion string `json:"SharedDocumentVersion,omitempty"`
}

// DescribeDocumentPermissionOutput is the response payload for DescribeDocumentPermission.
type DescribeDocumentPermissionOutput struct {
	NextToken              string               `json:"NextToken,omitempty"`
	AccountIDs             []string             `json:"AccountIds"`
	AccountSharingInfoList []AccountSharingInfo `json:"AccountSharingInfoList"`
}

// ModifyDocumentPermissionInput is the request payload for ModifyDocumentPermission.
// SharedDocumentVersion pins the version shared with the added accounts; if
// omitted, real AWS shares the document's current DefaultVersion instead
// (api_op_ModifyDocumentPermission.go:51-53).
type ModifyDocumentPermissionInput struct {
	Name                  string   `json:"Name"`
	PermissionType        string   `json:"PermissionType"`
	SharedDocumentVersion string   `json:"SharedDocumentVersion,omitempty"`
	AccountIDsToAdd       []string `json:"AccountIdsToAdd,omitempty"`
	AccountIDsToRemove    []string `json:"AccountIdsToRemove,omitempty"`
}

// ModifyDocumentPermissionOutput is the response payload for ModifyDocumentPermission.
type ModifyDocumentPermissionOutput struct{}

// ListDocumentVersionsInput is the request payload for ListDocumentVersions.
type ListDocumentVersionsInput struct {
	Name       string `json:"Name"`
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// DocumentVersionInfo is the narrow metadata shape for ListDocumentVersionsOutput
// matching AWS SDK v2 types.DocumentVersionInfo (api_op_ListDocumentVersions.go).
// Deliberately omits Content.
type DocumentVersionInfo struct {
	Name              string  `json:"Name"`
	DisplayName       string  `json:"DisplayName,omitempty"`
	DocumentVersion   string  `json:"DocumentVersion"`
	DocumentFormat    string  `json:"DocumentFormat"`
	Status            string  `json:"Status"`
	StatusInformation string  `json:"StatusInformation,omitempty"`
	VersionName       string  `json:"VersionName,omitempty"`
	CreatedDate       float64 `json:"CreatedDate"`
	IsDefaultVersion  bool    `json:"IsDefaultVersion"`
}

// ListDocumentVersionsOutput is the response payload for ListDocumentVersions.
type ListDocumentVersionsOutput struct {
	NextToken        string                `json:"NextToken,omitempty"`
	DocumentVersions []DocumentVersionInfo `json:"DocumentVersions"`
}

// UpdateDocumentDefaultVersionInput is the request payload for UpdateDocumentDefaultVersion.
type UpdateDocumentDefaultVersionInput struct {
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion"`
}

// UpdateDocumentDefaultVersionOutput is the response payload for UpdateDocumentDefaultVersion.
type UpdateDocumentDefaultVersionOutput struct {
	Description *DocumentDefaultVersionDescription `json:"Description,omitempty"`
}

// DocumentDefaultVersionDescription describes a document's default version.
type DocumentDefaultVersionDescription struct {
	Name               string `json:"Name"`
	DefaultVersion     string `json:"DefaultVersion"`
	DefaultVersionName string `json:"DefaultVersionName,omitempty"`
}

// UpdateDocumentMetadataInput is the request payload for UpdateDocumentMetadata.
// Fields ordered for alignment.
type UpdateDocumentMetadataInput struct {
	DocumentReviews *DocumentReviews `json:"DocumentReviews,omitempty"`
	Name            string           `json:"Name"`
	DocumentVersion string           `json:"DocumentVersion,omitempty"`
}

// DocumentReviews holds review metadata for a document version.
type DocumentReviews struct {
	Action  string                        `json:"Action"`
	Comment []DocumentReviewCommentSource `json:"Comment,omitempty"`
}

// DocumentReviewCommentSource is a single review comment.
type DocumentReviewCommentSource struct {
	Type    string `json:"Type,omitempty"`
	Content string `json:"Content,omitempty"`
}

// ListDocumentMetadataHistoryInput is the request payload.
// Fields ordered for alignment.
type ListDocumentMetadataHistoryInput struct {
	MaxResults      *int64 `json:"MaxResults,omitempty"`
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
	Metadata        string `json:"Metadata,omitempty"`
	NextToken       string `json:"NextToken,omitempty"`
}

// ListDocumentMetadataHistoryOutput is the response payload.
type ListDocumentMetadataHistoryOutput struct {
	Metadata        *DocumentMetadataResponseInfo `json:"Metadata,omitempty"`
	Name            string                        `json:"Name,omitempty"`
	DocumentVersion string                        `json:"DocumentVersion,omitempty"`
	Author          string                        `json:"Author,omitempty"`
	NextToken       string                        `json:"NextToken,omitempty"`
}

// DocumentMetadataResponseInfo holds review history.
type DocumentMetadataResponseInfo struct {
	ReviewerResponse []DocumentReviewerResponseSource `json:"ReviewerResponse,omitempty"`
}

// DocumentReviewerResponseSource is a single reviewer response.
// Fields ordered for alignment.
type DocumentReviewerResponseSource struct {
	ReviewStatus string                        `json:"ReviewStatus,omitempty"`
	Reviewer     string                        `json:"Reviewer,omitempty"`
	Comment      []DocumentReviewCommentSource `json:"Comment,omitempty"`
	CreatedTime  float64                       `json:"CreatedTime,omitempty"`
	UpdatedTime  float64                       `json:"UpdatedTime,omitempty"`
}
