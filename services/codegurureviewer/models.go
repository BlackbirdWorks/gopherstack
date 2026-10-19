package codegurureviewer

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// Provider types and states from the SDK enums.
const (
	providerCodeCommit = "CodeCommit"
	providerBitbucket  = "Bitbucket"
	providerGHES       = "GitHubEnterpriseServer"
	providerGitHub     = "GitHub"
	providerS3         = "S3Bucket"

	stateAssociating    = "Associating"
	stateAssociated     = "Associated"
	stateDisassociating = "Disassociating"
	stateDisassociated  = "Disassociated"
	stateFailed         = "Failed"

	jobPending   = "Pending"
	jobCompleted = "Completed"
	jobFailed    = "Failed"

	typeRepositoryAnalysis = "RepositoryAnalysis"
	typePullRequest        = "PullRequest"

	configPresent = "Present"
	configAbsent  = "Absent"
)

// CodeCommitRepository names a CodeCommit repository.
type CodeCommitRepository struct {
	Name string `json:"Name"`
}

// ThirdPartySourceRepository names a Bitbucket or GitHub Enterprise Server repository.
type ThirdPartySourceRepository struct {
	ConnectionArn string `json:"ConnectionArn"`
	Name          string `json:"Name"`
	Owner         string `json:"Owner"`
}

// S3Repository names an S3 repository to associate.
type S3Repository struct {
	BucketName string `json:"BucketName"`
	Name       string `json:"Name"`
}

// Repository is the AssociateRepository union.
type Repository struct {
	Bitbucket              *ThirdPartySourceRepository `json:"Bitbucket,omitempty"`
	CodeCommit             *CodeCommitRepository       `json:"CodeCommit,omitempty"`
	GitHubEnterpriseServer *ThirdPartySourceRepository `json:"GitHubEnterpriseServer,omitempty"`
	S3Bucket               *S3Repository               `json:"S3Bucket,omitempty"`
}

// KMSKeyDetails is the repository encryption setting.
type KMSKeyDetails struct {
	EncryptionOption string `json:"EncryptionOption,omitempty"`
	KMSKeyID         string `json:"KMSKeyId,omitempty"`
}

// CodeArtifacts holds the S3 keys of the source and build archives.
type CodeArtifacts struct {
	BuildArtifactsObjectKey      string `json:"BuildArtifactsObjectKey,omitempty"`
	SourceCodeArtifactsObjectKey string `json:"SourceCodeArtifactsObjectKey,omitempty"`
}

// S3RepositoryDetails locates an S3 repository's archives.
type S3RepositoryDetails struct {
	CodeArtifacts *CodeArtifacts `json:"CodeArtifacts,omitempty"`
	BucketName    string         `json:"BucketName,omitempty"`
}

// RepositoryHead selects a branch.
type RepositoryHead struct {
	BranchName string `json:"BranchName"`
}

// S3BucketRepository is the S3 member of SourceCodeType.
type S3BucketRepository struct {
	Details *S3RepositoryDetails `json:"Details,omitempty"`
	Name    string               `json:"Name"`
}

// SourceCodeType says which code a review analyzes.
type SourceCodeType struct {
	RepositoryHead     *RepositoryHead     `json:"RepositoryHead,omitempty"`
	S3BucketRepository *S3BucketRepository `json:"S3BucketRepository,omitempty"`
}

// RepositoryAnalysis is the CreateCodeReview repository-analysis request.
type RepositoryAnalysis struct {
	RepositoryHead *RepositoryHead `json:"RepositoryHead,omitempty"`
	SourceCodeType *SourceCodeType `json:"SourceCodeType,omitempty"`
}

// CodeReviewType is the CreateCodeReview type; only RepositoryAnalysis can be created.
type CodeReviewType struct {
	RepositoryAnalysis *RepositoryAnalysis `json:"RepositoryAnalysis,omitempty"`
	AnalysisTypes      []string            `json:"AnalysisTypes,omitempty"`
}

// Metrics are the totals of a completed review.
type Metrics struct {
	FindingsCount              int64 `json:"FindingsCount"`
	MeteredLinesOfCodeCount    int64 `json:"MeteredLinesOfCodeCount"`
	SuppressedLinesOfCodeCount int64 `json:"SuppressedLinesOfCodeCount"`
}

// RuleMetadata describes the rule behind a recommendation.
type RuleMetadata struct {
	LongDescription  string   `json:"LongDescription,omitempty"`
	RuleID           string   `json:"RuleId,omitempty"`
	RuleName         string   `json:"RuleName,omitempty"`
	ShortDescription string   `json:"ShortDescription,omitempty"`
	RuleTags         []string `json:"RuleTags,omitempty"`
}

// Recommendation is one finding of a review (wire RecommendationSummary).
type Recommendation struct {
	RuleMetadata           *RuleMetadata `json:"RuleMetadata,omitempty"`
	Description            string        `json:"Description"`
	FilePath               string        `json:"FilePath"`
	RecommendationCategory string        `json:"RecommendationCategory"`
	RecommendationID       string        `json:"RecommendationId"`
	Severity               string        `json:"Severity"`
	StartLine              int32         `json:"StartLine"`
	EndLine                int32         `json:"EndLine"`
}

// RepositoryAssociation is the wire shape of an association.
type RepositoryAssociation struct {
	CreatedTimeStamp     awstime.EpochTime    `json:"CreatedTimeStamp"`
	LastUpdatedTimeStamp awstime.EpochTime    `json:"LastUpdatedTimeStamp"`
	KMSKeyDetails        *KMSKeyDetails       `json:"KMSKeyDetails,omitempty"`
	S3RepositoryDetails  *S3RepositoryDetails `json:"S3RepositoryDetails,omitempty"`
	AssociationArn       string               `json:"AssociationArn"`
	AssociationID        string               `json:"AssociationId"`
	ConnectionArn        string               `json:"ConnectionArn,omitempty"`
	Name                 string               `json:"Name"`
	Owner                string               `json:"Owner"`
	ProviderType         string               `json:"ProviderType"`
	State                string               `json:"State"`
	StateReason          string               `json:"StateReason,omitempty"`
}

// RepositoryAssociationSummary is the list shape of an association.
type RepositoryAssociationSummary struct {
	LastUpdatedTimeStamp awstime.EpochTime `json:"LastUpdatedTimeStamp"`
	AssociationArn       string            `json:"AssociationArn"`
	AssociationID        string            `json:"AssociationId"`
	ConnectionArn        string            `json:"ConnectionArn,omitempty"`
	Name                 string            `json:"Name"`
	Owner                string            `json:"Owner"`
	ProviderType         string            `json:"ProviderType"`
	State                string            `json:"State"`
}

// CodeReview is the wire shape of a review.
type CodeReview struct {
	CreatedTimeStamp     awstime.EpochTime `json:"CreatedTimeStamp"`
	LastUpdatedTimeStamp awstime.EpochTime `json:"LastUpdatedTimeStamp"`
	Metrics              *Metrics          `json:"Metrics,omitempty"`
	SourceCodeType       *SourceCodeType   `json:"SourceCodeType,omitempty"`
	RepositoryName       string            `json:"RepositoryName"`
	AssociationArn       string            `json:"AssociationArn"`
	CodeReviewArn        string            `json:"CodeReviewArn"`
	ConfigFileState      string            `json:"ConfigFileState,omitempty"`
	Name                 string            `json:"Name"`
	Owner                string            `json:"Owner"`
	ProviderType         string            `json:"ProviderType"`
	PullRequestID        string            `json:"PullRequestId,omitempty"`
	State                string            `json:"State"`
	StateReason          string            `json:"StateReason,omitempty"`
	Type                 string            `json:"Type"`
	AnalysisTypes        []string          `json:"AnalysisTypes,omitempty"`
}

// MetricsSummary is the list shape of Metrics.
type MetricsSummary = Metrics

// CodeReviewSummary is the list shape of a review.
type CodeReviewSummary struct {
	CreatedTimeStamp     awstime.EpochTime `json:"CreatedTimeStamp"`
	LastUpdatedTimeStamp awstime.EpochTime `json:"LastUpdatedTimeStamp"`
	MetricsSummary       *MetricsSummary   `json:"MetricsSummary,omitempty"`
	SourceCodeType       *SourceCodeType   `json:"SourceCodeType,omitempty"`
	CodeReviewArn        string            `json:"CodeReviewArn"`
	Name                 string            `json:"Name"`
	Owner                string            `json:"Owner"`
	ProviderType         string            `json:"ProviderType"`
	PullRequestID        string            `json:"PullRequestId,omitempty"`
	RepositoryName       string            `json:"RepositoryName"`
	State                string            `json:"State"`
	Type                 string            `json:"Type"`
}

// RecommendationFeedback is one user's reactions to one recommendation.
type RecommendationFeedback struct {
	CreatedTimeStamp     awstime.EpochTime `json:"CreatedTimeStamp"`
	LastUpdatedTimeStamp awstime.EpochTime `json:"LastUpdatedTimeStamp"`
	CodeReviewArn        string            `json:"CodeReviewArn"`
	RecommendationID     string            `json:"RecommendationId"`
	UserID               string            `json:"UserId"`
	Reactions            []string          `json:"Reactions"`
}

// RecommendationFeedbackSummary is the list shape of feedback.
type RecommendationFeedbackSummary struct {
	RecommendationID string   `json:"RecommendationId"`
	UserID           string   `json:"UserId"`
	Reactions        []string `json:"Reactions"`
}

// associationRecord is the persisted association.
type associationRecord struct {
	Tags          map[string]string    `json:"tags,omitempty"`
	KMS           *KMSKeyDetails       `json:"kms,omitempty"`
	S3            *S3RepositoryDetails `json:"s3,omitempty"`
	Created       awstime.EpochTime    `json:"created"`
	Updated       awstime.EpochTime    `json:"updated"`
	SettleAt      awstime.EpochTime    `json:"settleAt"`
	ARN           string               `json:"arn"`
	ID            string               `json:"id"`
	Name          string               `json:"name"`
	Owner         string               `json:"owner"`
	ProviderType  string               `json:"providerType"`
	ConnectionARN string               `json:"connectionArn,omitempty"`
	State         string               `json:"state"`
	StateReason   string               `json:"stateReason,omitempty"`
}

// reviewRecord is the persisted review with the recommendations found when it ran.
type reviewRecord struct {
	Created         awstime.EpochTime `json:"created"`
	Updated         awstime.EpochTime `json:"updated"`
	SettleAt        awstime.EpochTime `json:"settleAt"`
	SourceCodeType  *SourceCodeType   `json:"sourceCodeType,omitempty"`
	Metrics         *Metrics          `json:"metrics,omitempty"`
	AssociationARN  string            `json:"associationArn"`
	ARN             string            `json:"arn"`
	Name            string            `json:"name"`
	Owner           string            `json:"owner"`
	ProviderType    string            `json:"providerType"`
	RepositoryName  string            `json:"repositoryName"`
	Type            string            `json:"type"`
	FinalState      string            `json:"finalState"`
	StateReason     string            `json:"stateReason,omitempty"`
	ConfigFile      string            `json:"configFile"`
	PullRequestID   string            `json:"pullRequestId,omitempty"`
	Recommendations []Recommendation  `json:"recommendations,omitempty"`
	AnalysisTypes   []string          `json:"analysisTypes,omitempty"`
}

// feedbackRecord is the persisted feedback.
type feedbackRecord struct {
	Created          awstime.EpochTime `json:"created"`
	Updated          awstime.EpochTime `json:"updated"`
	CodeReviewARN    string            `json:"codeReviewArn"`
	RecommendationID string            `json:"recommendationId"`
	UserID           string            `json:"userId"`
	Reactions        []string          `json:"reactions"`
}

func associationKey(v *associationRecord) string { return v.ARN }
func reviewKey(v *reviewRecord) string           { return v.ARN }
func feedbackKey(v *feedbackRecord) string {
	return v.CodeReviewARN + "|" + v.RecommendationID + "|" + v.UserID
}
