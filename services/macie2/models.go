package macie2

import "time"

// storedAllowList holds the allow list with all fields.
type storedAllowList struct {
	AllowListDetail
}

// storedCustomDataID holds the custom data identifier with internal fields.
type storedCustomDataID struct {
	CustomDataIdentifier
	Deleted bool `json:"deleted"`
}

// storedFindingsFilter holds the findings filter with all fields.
type storedFindingsFilter struct {
	FindingsFilterDetail
}

// storedFinding holds a Macie finding.
type storedFinding struct {
	Finding
}

// Session represents the Macie account state.
type Session struct {
	CreatedAt                  time.Time `json:"createdAt"`
	UpdatedAt                  time.Time `json:"updatedAt"`
	FindingPublishingFrequency string    `json:"findingPublishingFrequency"`
	ServiceRole                string    `json:"serviceRole"`
	Status                     string    `json:"status"`
	Enabled                    bool      `json:"-"`
}

// AllowListCriteria holds criteria for an allow list.
type AllowListCriteria struct {
	Regex       *string      `json:"regex,omitempty"`
	S3WordsList *S3WordsList `json:"s3WordsList,omitempty"`
}

// S3WordsList references an S3 object containing ignore words.
type S3WordsList struct {
	BucketName string `json:"bucketName"`
	ObjectKey  string `json:"objectKey"`
}

// AllowListStatus describes the status of an allow list.
type AllowListStatus struct {
	Code        string `json:"code"`
	Description string `json:"description,omitempty"`
}

// AllowListSummary is the summary view of an allow list.
type AllowListSummary struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	Arn         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
}

// AllowListDetail is the full detail view of an allow list.
type AllowListDetail struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	Criteria    AllowListCriteria `json:"criteria"`
	Status      AllowListStatus   `json:"status"`
	Arn         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
}

// CustomDataIdentifier represents a custom data identifier.
type CustomDataIdentifier struct {
	CreatedAt            time.Time         `json:"createdAt"`
	Tags                 map[string]string `json:"tags,omitempty"`
	Arn                  string            `json:"arn"`
	Description          string            `json:"description,omitempty"`
	ID                   string            `json:"id"`
	Name                 string            `json:"name"`
	Regex                string            `json:"regex"`
	IgnoreWords          []string          `json:"ignoreWords,omitempty"`
	Keywords             []string          `json:"keywords,omitempty"`
	MaximumMatchDistance int32             `json:"maximumMatchDistance"`
}

// CustomDataIdentifierSummary is the summary view of a custom data identifier.
type CustomDataIdentifierSummary struct {
	Arn         string    `json:"arn"`
	CreatedAt   time.Time `json:"createdAt"`
	Description string    `json:"description,omitempty"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
}

// FindingsFilterDetail is the full detail of a findings filter.
type FindingsFilterDetail struct {
	FindingCriteria map[string]any    `json:"findingCriteria,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	Action          string            `json:"action"`
	Arn             string            `json:"arn"`
	Description     string            `json:"description,omitempty"`
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Position        int32             `json:"position"`
}

// FindingsFilterSummary is the summary view of a findings filter.
type FindingsFilterSummary struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Action      string            `json:"action"`
	Arn         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Position    int32             `json:"position"`
}

// FindingType represents the type of a finding.
type FindingType string

// Finding represents a Macie finding.
type Finding struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	AccountID   string    `json:"accountId"`
	Category    string    `json:"category"`
	Description string    `json:"description"`
	ID          string    `json:"id"`
	Region      string    `json:"region"`
	Title       string    `json:"title"`
	Type        string    `json:"type"`
	Severity    Severity  `json:"severity"`
	Archived    bool      `json:"archived"`
}

// Severity holds finding severity details.
type Severity struct {
	Description string  `json:"description"`
	Score       float64 `json:"score"`
}

// FindingStatisticsGroup holds a group of finding statistics.
type FindingStatisticsGroup struct {
	GroupKey string `json:"groupKey"`
	Count    int64  `json:"count"`
}

// ClassificationJob represents a Macie classification job.
type ClassificationJob struct {
	Tags               map[string]string `json:"tags,omitempty"`
	S3JobDefinition    map[string]any    `json:"s3JobDefinition,omitempty"`
	ScheduleFrequency  map[string]any    `json:"scheduleFrequency,omitempty"`
	LastRunTime        *time.Time        `json:"lastRunTime,omitempty"`
	CreatedAt          time.Time         `json:"createdAt"`
	Arn                string            `json:"jobArn"`
	ClientToken        string            `json:"clientToken,omitempty"`
	Description        string            `json:"description,omitempty"`
	JobID              string            `json:"jobId"`
	JobStatus          string            `json:"jobStatus"`
	JobType            string            `json:"jobType"`
	Name               string            `json:"name"`
	SamplingPercentage int32             `json:"samplingPercentage"`
	InitialRun         bool              `json:"initialRun"`
}

// ClassificationJobSummary is the list-view of a classification job.
type ClassificationJobSummary struct {
	Tags        map[string]string `json:"tags,omitempty"`
	LastRunTime *time.Time        `json:"lastRunTime,omitempty"`
	CreatedAt   time.Time         `json:"createdAt"`
	Description string            `json:"description,omitempty"`
	JobID       string            `json:"jobId"`
	JobStatus   string            `json:"jobStatus"`
	JobType     string            `json:"jobType"`
	Name        string            `json:"name"`
}

// Member represents a Macie member account.
type Member struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	InvitedAt              time.Time         `json:"invitedAt"`
	UpdatedAt              time.Time         `json:"updatedAt"`
	Arn                    string            `json:"arn"`
	AccountID              string            `json:"accountId"`
	AdministratorAccountID string            `json:"administratorAccountId,omitempty"`
	Email                  string            `json:"email"`
	MasteredBy             string            `json:"masterAccountId,omitempty"`
	RelationshipStatus     string            `json:"relationshipStatus"`
}

// Invitation represents a Macie invitation.
type Invitation struct {
	AccountID          string    `json:"accountId"`
	InvitationID       string    `json:"invitationId"`
	InvitedAt          time.Time `json:"invitedAt"`
	RelationshipStatus string    `json:"relationshipStatus"`
}

// UnprocessedAccount describes an account that could not be processed.
type UnprocessedAccount struct {
	AccountID    string `json:"accountId"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// AdministratorAccount represents the administrator account relationship.
type AdministratorAccount struct {
	AccountID          string    `json:"accountId"`
	InvitationID       string    `json:"invitationId"`
	InvitedAt          time.Time `json:"invitedAt"`
	RelationshipStatus string    `json:"relationshipStatus"`
}

// OrgAdminAccount represents an organization admin account.
type OrgAdminAccount struct {
	AccountID string `json:"accountId"`
	Status    string `json:"status"`
}

// OrgConfig holds organization-level Macie configuration.
type OrgConfig struct {
	DataSources            map[string]any   `json:"dataSources,omitempty"`
	Features               []map[string]any `json:"features,omitempty"`
	AutoEnable             bool             `json:"autoEnable"`
	MaxAccountLimitReached bool             `json:"maxAccountLimitReached"`
}

// AutoDiscoveryConfig holds automated discovery configuration.
type AutoDiscoveryConfig struct {
	AutoEnableOrganizationMembers string `json:"autoEnableOrganizationMembers,omitempty"`
	Status                        string `json:"status"`
}

// AutoDiscoveryAccount holds automated discovery status for an account.
type AutoDiscoveryAccount struct {
	AccountID string `json:"accountId"`
	Email     string `json:"email,omitempty"`
	Status    string `json:"status"`
}

// AutoDiscoveryAccountUpdate is a requested status change for an account.
type AutoDiscoveryAccountUpdate struct {
	AccountID string `json:"accountId"`
	Status    string `json:"status"`
}

// ClassificationExportConfig holds classification result export configuration.
type ClassificationExportConfig struct {
	S3Destination *ClassificationExportS3Dest `json:"s3Destination,omitempty"`
}

// ClassificationExportS3Dest holds S3 destination details.
type ClassificationExportS3Dest struct {
	BucketName string `json:"bucketName"`
	KeyPrefix  string `json:"keyPrefix,omitempty"`
	KmsKeyArn  string `json:"kmsKeyArn,omitempty"`
}

// ClassificationScope represents a classification scope resource.
type ClassificationScope struct {
	Tags      map[string]string      `json:"tags,omitempty"`
	S3        *ClassificationScopeS3 `json:"s3,omitempty"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
}

// ClassificationScopeS3 holds S3 exclusion criteria.
type ClassificationScopeS3 struct {
	Excludes map[string]any `json:"excludes,omitempty"`
}

// ClassificationScopeSummary is the list-view of a classification scope.
type ClassificationScopeSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// FindingsPublicationConfig holds findings publication configuration.
type FindingsPublicationConfig struct {
	SecurityHubConfiguration      *SecurityHubConfig `json:"securityHubConfiguration,omitempty"`
	ClientToken                   string             `json:"clientToken,omitempty"`
	PublishClassificationFindings bool               `json:"publishClassificationFindings"`
	PublishPolicyFindings         bool               `json:"publishPolicyFindings"`
}

// SecurityHubConfig holds Security Hub integration settings.
type SecurityHubConfig struct {
	PublishClassificationFindings bool `json:"publishClassificationFindings"`
	PublishPolicyFindings         bool `json:"publishPolicyFindings"`
}

// ResourceProfile holds sensitivity profile data for a bucket.
type ResourceProfile struct {
	Statistics               *ResourceStatistics `json:"statistics,omitempty"`
	ResourceArn              string              `json:"resourceArn"`
	SensitivityScore         int32               `json:"sensitivityScore"`
	SensitivityScoreOverride bool                `json:"sensitivityScoreOverride"`
}

// ResourceStatistics holds classification result counts for a bucket.
type ResourceStatistics struct {
	LastRunErroredAt                   *time.Time `json:"lastRunErroredAt,omitempty"`
	LastRunAt                          *time.Time `json:"lastRunAt,omitempty"`
	TotalBytesClassified               int64      `json:"totalBytesClassified"`
	TotalDetections                    int64      `json:"totalDetections"`
	TotalDetectionsWithoutSuppression  int64      `json:"totalDetectionsWithoutSuppression"`
	TotalItemsClassified               int64      `json:"totalItemsClassified"`
	TotalItemsSkipped                  int64      `json:"totalItemsSkipped"`
	TotalItemsSkippedInvalidEncryption int64      `json:"totalItemsSkippedInvalidEncryption"`
	TotalItemsSkippedInvalidKms        int64      `json:"totalItemsSkippedInvalidKms"`
	TotalItemsSkippedPermissionError   int64      `json:"totalItemsSkippedPermissionError"`
}

// ResourceProfileArtifact is a single artifact in a resource profile.
type ResourceProfileArtifact struct {
	Arn       string `json:"arn"`
	Type      string `json:"type,omitempty"`
	Sensitive bool   `json:"sensitive"`
}

// ResourceProfileDetection is a data identifier detection result.
type ResourceProfileDetection struct {
	Arn        string `json:"arn,omitempty"`
	ID         string `json:"id,omitempty"`
	Name       string `json:"name,omitempty"`
	Type       string `json:"type,omitempty"`
	Count      int64  `json:"count"`
	Suppressed bool   `json:"suppressed"`
}

// RevealConfiguration holds sensitive data reveal configuration.
type RevealConfiguration struct {
	KmsKeyID string `json:"kmsKeyId,omitempty"`
	Status   string `json:"status"`
}

// SensitivityInspectionTemplate holds template configuration.
type SensitivityInspectionTemplate struct {
	Excludes    map[string]any `json:"excludes,omitempty"`
	Includes    map[string]any `json:"includes,omitempty"`
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
}

// SensitivityInspectionTemplateSummary is the list-view of a template.
type SensitivityInspectionTemplateSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// UsageRecord holds usage data for a single account.
type UsageRecord struct {
	AccountID                            string           `json:"accountId"`
	AutomatedDiscoveryFreeTrialStartDate *time.Time       `json:"automatedDiscoveryFreeTrialStartDate,omitempty"`
	FreeTrialStartDate                   *time.Time       `json:"freeTrialStartDate,omitempty"`
	Usage                                []UsageByAccount `json:"usage,omitempty"`
}

// UsageByAccount holds usage data for one type.
type UsageByAccount struct {
	Currency      string         `json:"currency,omitempty"`
	EstimatedCost string         `json:"estimatedCost,omitempty"`
	ServiceLimit  map[string]any `json:"serviceLimit,omitempty"`
	Type          string         `json:"type,omitempty"`
}

// S3BucketMetadata holds Macie's view of an S3 bucket for DescribeBuckets.
type S3BucketMetadata struct {
	AccountID               string           `json:"accountId"`
	BucketArn               string           `json:"bucketArn"`
	BucketName              string           `json:"bucketName"`
	Region                  string           `json:"region"`
	PublicAccess            string           `json:"publicAccess"`
	EncryptionType          string           `json:"encryptionType"`
	SharedAccess            string           `json:"sharedAccess"`
	Tags                    []map[string]any `json:"tags,omitempty"`
	ClassifiableObjectCount int64            `json:"classifiableObjectCount"`
	ClassifiableSizeInBytes int64            `json:"classifiableSizeInBytes"`
	ObjectCount             int64            `json:"objectCount"`
	SizeInBytes             int64            `json:"sizeInBytes"`
}

// UsageTotal holds aggregated usage totals.
type UsageTotal struct {
	Currency      string `json:"currency,omitempty"`
	EstimatedCost string `json:"estimatedCost,omitempty"`
	Type          string `json:"type,omitempty"`
}

// ManagedDataIdentifier describes a built-in Macie data identifier.
type ManagedDataIdentifier struct {
	Category string `json:"category"`
	ID       string `json:"id"`
}
