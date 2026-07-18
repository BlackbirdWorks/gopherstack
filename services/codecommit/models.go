package codecommit

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	prStatusOpen   = "OPEN"
	prStatusClosed = "CLOSED"

	fileModeDefault = "NORMAL"

	// maxBatchGetRepositories is the AWS limit for BatchGetRepositories.
	maxBatchGetRepositories = 25

	// maxBranchNameLength is the maximum allowed CodeCommit branch name length.
	maxBranchNameLength = 256
)

// ApprovalRuleTemplate represents an AWS CodeCommit approval rule template.
type ApprovalRuleTemplate struct {
	CreationDate                    time.Time `json:"creationDate"`
	LastModifiedDate                time.Time `json:"lastModifiedDate"`
	ApprovalRuleTemplateID          string    `json:"approvalRuleTemplateId"`
	ApprovalRuleTemplateName        string    `json:"approvalRuleTemplateName"`
	ApprovalRuleTemplateARN         string    `json:"approvalRuleTemplateArn"`
	ApprovalRuleTemplateContent     string    `json:"approvalRuleTemplateContent"`
	ApprovalRuleTemplateDescription string    `json:"approvalRuleTemplateDescription,omitempty"`
	LastModifiedUser                string    `json:"lastModifiedUser,omitempty"`
	RuleContentSha256               string    `json:"ruleContentSha256"`
}

// Branch represents a CodeCommit branch.
type Branch struct {
	BranchName     string `json:"branchName"`
	CommitID       string `json:"commitId"`
	RepositoryName string `json:"repositoryName"`
}

// Commit represents a CodeCommit commit.
type Commit struct {
	CreatedAt      time.Time `json:"createdAt"`
	CommitID       string    `json:"commitId"`
	TreeID         string    `json:"treeId"`
	Message        string    `json:"message,omitempty"`
	AdditionalData string    `json:"additionalData,omitempty"`
	AuthorName     string    `json:"authorName,omitempty"`
	AuthorEmail    string    `json:"authorEmail,omitempty"`
	CommitterName  string    `json:"committerName,omitempty"`
	CommitterEmail string    `json:"committerEmail,omitempty"`
	RepositoryName string    `json:"repositoryName"`
	Parents        []string  `json:"parents,omitempty"`
}

// PutFileEntry describes a file to add or overwrite in a CreateCommit call.
type PutFileEntry struct {
	FilePath    string `json:"filePath"`
	FileMode    string `json:"fileMode"`
	FileContent []byte `json:"fileContent"`
}

// PullRequestTarget represents a target for a pull request.
type PullRequestTarget struct {
	RepositoryName       string `json:"repositoryName"`
	SourceReference      string `json:"sourceReference"`
	DestinationReference string `json:"destinationReference,omitempty"`
	SourceCommit         string `json:"sourceCommit,omitempty"`
	DestinationCommit    string `json:"destinationCommit,omitempty"`
	MergeBase            string `json:"mergeBase,omitempty"`
}

// PullRequest represents a CodeCommit pull request.
type PullRequest struct {
	CreationDate       time.Time           `json:"creationDate"`
	LastActivityDate   time.Time           `json:"lastActivityDate"`
	PullRequestID      string              `json:"pullRequestId"`
	Title              string              `json:"title"`
	Description        string              `json:"description,omitempty"`
	AuthorARN          string              `json:"authorArn,omitempty"`
	PullRequestStatus  string              `json:"pullRequestStatus"`
	ClientRequestToken string              `json:"clientRequestToken,omitempty"`
	RevisionID         string              `json:"revisionId"`
	PullRequestTargets []PullRequestTarget `json:"pullRequestTargets"`
}

// Repository represents an AWS CodeCommit repository.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateRepository.
type Repository struct {
	CreationDate     time.Time  `json:"creationDate"`
	LastModifiedDate time.Time  `json:"lastModifiedDate"`
	Tags             *tags.Tags `json:"tags,omitempty"`
	RepositoryName   string     `json:"repositoryName"`
	RepositoryID     string     `json:"repositoryId"`
	ARN              string     `json:"arn"`
	Description      string     `json:"repositoryDescription,omitempty"`
	AccountID        string     `json:"accountId"`
	Region           string     `json:"-"`
	CloneURLHTTP     string     `json:"cloneUrlHttp"`
	CloneURLSSH      string     `json:"cloneUrlSsh"`
	KmsKeyID         string     `json:"kmsKeyId,omitempty"`
	DefaultBranch    string     `json:"defaultBranch,omitempty"`
}

// PullRequestApproval represents a user's approval state for a pull request.
type PullRequestApproval struct {
	UserARN       string `json:"userArn"`
	ApprovalState string `json:"approvalState"`
}

// PullRequestApprovalRule represents an approval rule on a pull request.
//
// PRID identifies the owning pull request. It exists purely so the flattened
// store.Table[PullRequestApprovalRule] (see store_setup.go; this collection
// was previously nested prID -> ruleName -> *rule) can derive its composite
// "prID|ruleName" key from the value alone; it is not part of the CodeCommit
// wire API, hence json:"-".
type PullRequestApprovalRule struct {
	RuleID              string `json:"approvalRuleId"`
	RuleName            string `json:"approvalRuleName"`
	ApprovalRuleContent string `json:"approvalRuleContent"`
	PRID                string `json:"-"`
}

// PullRequestEvent represents an event on a pull request.
type PullRequestEvent struct {
	PullRequestEventType string `json:"pullRequestEventType"`
	EventDate            string `json:"eventDate"`
}

// RuleEvaluation represents the evaluation of a pull request approval rule.
type RuleEvaluation struct {
	RuleName  string `json:"approvalRuleName"`
	Satisfied bool   `json:"satisfied"`
}

// Comment represents a CodeCommit comment.
type Comment struct {
	CommentID        string `json:"commentId"`
	Content          string `json:"content"`
	AuthorARN        string `json:"authorArn"`
	CreationDate     string `json:"creationDate"`
	LastModifiedDate string `json:"lastModifiedDate"`
	// InReplyTo links to parent comment for replies
	InReplyTo string `json:"inReplyTo,omitempty"`
	// PRid links comment to a pull request
	PRid string `json:"-"`
	// RepoName + AfterCommitID for commit comments
	RepoName      string `json:"-"`
	AfterCommitID string `json:"-"`
	Deleted       bool   `json:"deleted"`
}

// Reaction represents a reaction emoji by a user on a comment.
type Reaction struct {
	Emoji   string `json:"emoji"`
	UserARN string `json:"userArn"`
}

// File represents a file stored in CodeCommit.
//
// RepoName identifies the owning repository. It exists purely so the
// flattened store.Table[File] (see store_setup.go; this collection was
// previously nested repoName -> filePath -> *File) can derive its composite
// "repoName|filePath" key from the value alone; it is not part of the
// CodeCommit wire API, hence json:"-".
type File struct {
	FilePath        string `json:"filePath"`
	CommitSpecifier string `json:"commitSpecifier"`
	BlobID          string `json:"blobId"`
	FileMode        string `json:"fileMode"`
	RepoName        string `json:"-"`
	FileContent     []byte `json:"fileContent"`
}

// RepositoryTrigger represents a trigger on a repository.
type RepositoryTrigger struct {
	Name           string   `json:"name"`
	DestinationARN string   `json:"destinationArn"`
	Events         []string `json:"events"`
}

// BlobInfo holds per-blob metadata in a file difference, matching real AWS shape.
type BlobInfo struct {
	BlobID string `json:"blobId"`
	Path   string `json:"path"`
	Mode   string `json:"mode"`
}

// FileDifference represents a file difference between two commits.
type FileDifference struct {
	AfterBlob  *BlobInfo `json:"afterBlob"`
	BeforeBlob *BlobInfo `json:"beforeBlob"`
	ChangeType string    `json:"changeType"`
}

// BatchAssociationError holds the error info for a single failed batch association.
type BatchAssociationError struct {
	RepositoryName string `json:"repositoryName"`
	ErrorCode      string `json:"errorCode"`
	ErrorMessage   string `json:"errorMessage"`
}

// BatchCommitError holds error information for a failed batch commit retrieval.
type BatchCommitError struct {
	CommitID     string `json:"commitId"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// BatchDescribeMergeConflictsResult holds the result of a merge conflict description.
type BatchDescribeMergeConflictsResult struct {
	DestinationCommitID string          `json:"destinationCommitId"`
	SourceCommitID      string          `json:"sourceCommitId"`
	BaseCommitID        string          `json:"baseCommitId,omitempty"`
	Conflicts           []MergeConflict `json:"conflicts"`
	Errors              []ConflictError `json:"errors,omitempty"`
}

// MergeConflict represents a single file conflict.
type MergeConflict struct {
	MergeHunks       []MergeHunk      `json:"mergeHunks,omitempty"`
	ConflictMetadata ConflictMetadata `json:"conflictMetadata"`
}

// ConflictMetadata holds metadata about a merge conflict.
type ConflictMetadata struct {
	FilePath          string           `json:"filePath"`
	NumberOfConflicts int              `json:"numberOfConflicts"`
	IsBinaryFile      FileBinaryStatus `json:"isBinaryFile"`
	ContentConflict   bool             `json:"contentConflict"`
}

// FileBinaryStatus holds whether each version of a file is binary.
type FileBinaryStatus struct {
	Source      bool `json:"source"`
	Destination bool `json:"destination"`
	Base        bool `json:"base"`
}

// MergeHunk represents a merge hunk.
type MergeHunk struct {
	Source      *MergeHunkDetail `json:"source,omitempty"`
	Destination *MergeHunkDetail `json:"destination,omitempty"`
	Base        *MergeHunkDetail `json:"base,omitempty"`
	IsConflict  bool             `json:"isConflict"`
}

// MergeHunkDetail represents details about a merge hunk.
type MergeHunkDetail struct {
	HunkContent string `json:"hunkContent"`
	StartLine   int    `json:"startLine"`
	EndLine     int    `json:"endLine"`
}

// ConflictError represents an error encountered while describing a conflict.
type ConflictError struct {
	FilePath     string `json:"filePath"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}
