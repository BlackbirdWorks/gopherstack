package macie2

import "time"

// storedAllowList holds the allow list with all fields.
type storedAllowList struct {
	AllowListDetail
}

// storedCustomDataID holds the custom data identifier with internal fields.
// The soft-delete flag lives directly on the embedded CustomDataIdentifier
// (its Deleted field) since the real GetCustomDataIdentifierOutput/
// BatchGetCustomDataIdentifierSummary shapes both carry it too -- no
// separate internal-only field is needed.
type storedCustomDataID struct {
	CustomDataIdentifier
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
	SeverityLevels       []SeverityLevel   `json:"severityLevels,omitempty"`
	MaximumMatchDistance int32             `json:"maximumMatchDistance"`
	// Deleted reflects real GetCustomDataIdentifierOutput/
	// BatchGetCustomDataIdentifierSummary soft-delete semantics: Amazon Macie
	// never hard-deletes a custom data identifier, so Get/BatchGet keep
	// returning it (with deleted:true) after DeleteCustomDataIdentifier.
	Deleted bool `json:"deleted"`
}

// SeverityLevel specifies a severity level for findings that a custom data
// identifier produces, keyed by an occurrence-count threshold.
type SeverityLevel struct {
	Severity             string `json:"severity"`
	OccurrencesThreshold int64  `json:"occurrencesThreshold"`
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

// PolicyDetails provides the details of the policy finding.
type PolicyDetails struct {
	Action *FindingAction `json:"action,omitempty"`
	Actor  *FindingActor  `json:"actor,omitempty"`
}

// FindingAction specifies information about action that produced finding.
type FindingAction struct {
	ActionType string `json:"actionType,omitempty"`
}

// FindingActor provides information about an entity that performed action.
type FindingActor struct {
	UserIdentity *UserIdentity `json:"userIdentity,omitempty"`
}

// UserIdentity provides information about the user/entity that performed the action.
type UserIdentity struct {
	Type        string `json:"type,omitempty"`
	UserName    string `json:"userName,omitempty"`
	PrincipalID string `json:"principalId,omitempty"`
}

// Finding represents a Macie finding.
type Finding struct {
	CreatedAt             time.Time              `json:"createdAt"`
	UpdatedAt             time.Time              `json:"updatedAt"`
	ClassificationDetails *ClassificationDetails `json:"classificationDetails,omitempty"`
	ResourcesAffected     *ResourcesAffected     `json:"resourcesAffected,omitempty"`
	PolicyDetails         *PolicyDetails         `json:"policyDetails,omitempty"`
	AccountID             string                 `json:"accountId"`
	Category              string                 `json:"category"`
	Description           string                 `json:"description"`
	ID                    string                 `json:"id"`
	Partition             string                 `json:"partition,omitempty"`
	Region                string                 `json:"region"`
	SchemaVersion         string                 `json:"schemaVersion,omitempty"`
	Title                 string                 `json:"title"`
	Type                  string                 `json:"type"`
	Severity              Severity               `json:"severity"`
	Count                 int64                  `json:"count"`
	Archived              bool                   `json:"archived"`
	Sample                bool                   `json:"sample"`
}

// ClassificationDetails describes how a sensitive-data finding was produced.
// Nil for policy findings.
type ClassificationDetails struct {
	Result     *ClassificationResult `json:"result,omitempty"`
	JobArn     string                `json:"jobArn,omitempty"`
	JobID      string                `json:"jobId,omitempty"`
	OriginType string                `json:"originType,omitempty"`
}

// ClassificationResult holds the status and other details of a sensitive
// data finding.
type ClassificationResult struct {
	Status         *ClassificationResultStatus `json:"status,omitempty"`
	MimeType       string                      `json:"mimeType,omitempty"`
	SizeClassified int64                       `json:"sizeClassified,omitempty"`
}

// ClassificationResultStatus holds the status of a classification result.
type ClassificationResultStatus struct {
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// ResourcesAffected identifies the S3 bucket/object a finding applies to.
type ResourcesAffected struct {
	S3Bucket *AffectedS3Bucket `json:"s3Bucket,omitempty"`
	S3Object *AffectedS3Object `json:"s3Object,omitempty"`
}

// AffectedS3Bucket is the bucket-level detail of ResourcesAffected.
type AffectedS3Bucket struct {
	Arn  string `json:"arn,omitempty"`
	Name string `json:"name,omitempty"`
}

// AffectedS3Object is the object-level detail of ResourcesAffected.
type AffectedS3Object struct {
	BucketArn string `json:"bucketArn,omitempty"`
	Key       string `json:"key,omitempty"`
	Path      string `json:"path,omitempty"`
}

// Severity holds finding severity details. Score is an integer 1 (Low) to 3
// (High) on the wire (types.Severity.Score is *int64 in the real SDK) -- not
// a fractional value.
type Severity struct {
	Description string `json:"description"`
	Score       int64  `json:"score"`
}

// FindingStatisticsGroup holds a group of finding statistics.
type FindingStatisticsGroup struct {
	GroupKey string `json:"groupKey"`
	Count    int64  `json:"count"`
}

// ClassificationJob represents a Macie classification job.
type ClassificationJob struct {
	Tags                          map[string]string      `json:"tags,omitempty"`
	S3JobDefinition               map[string]any         `json:"s3JobDefinition,omitempty"`
	ScheduleFrequency             map[string]any         `json:"scheduleFrequency,omitempty"`
	LastRunTime                   *time.Time             `json:"lastRunTime,omitempty"`
	LastRunErrorStatus            *JobLastRunErrorStatus `json:"lastRunErrorStatus,omitempty"`
	Statistics                    *JobStatistics         `json:"statistics,omitempty"`
	UserPausedDetails             *JobUserPausedDetails  `json:"userPausedDetails,omitempty"`
	CreatedAt                     time.Time              `json:"createdAt"`
	Arn                           string                 `json:"jobArn"`
	ClientToken                   string                 `json:"clientToken,omitempty"`
	Description                   string                 `json:"description,omitempty"`
	JobID                         string                 `json:"jobId"`
	JobStatus                     string                 `json:"jobStatus"`
	JobType                       string                 `json:"jobType"`
	ManagedDataIdentifierSelector string                 `json:"managedDataIdentifierSelector,omitempty"`
	Name                          string                 `json:"name"`
	AllowListIDs                  []string               `json:"allowListIds,omitempty"`
	CustomDataIdentifierIDs       []string               `json:"customDataIdentifierIds,omitempty"`
	ManagedDataIdentifierIDs      []string               `json:"managedDataIdentifierIds,omitempty"`
	SamplingPercentage            int32                  `json:"samplingPercentage"`
	InitialRun                    bool                   `json:"initialRun"`
}

// JobLastRunErrorStatus indicates whether account- or bucket-level access
// errors occurred during a classification job's most recent run.
type JobLastRunErrorStatus struct {
	Code string `json:"code,omitempty"`
}

// JobStatistics holds run-count and remaining-object processing stats for a
// classification job.
type JobStatistics struct {
	ApproximateNumberOfObjectsToProcess float64 `json:"approximateNumberOfObjectsToProcess"`
	NumberOfRuns                        float64 `json:"numberOfRuns"`
}

// JobUserPausedDetails records when a job was paused by the user and when it
// will expire if not resumed. Present only while JobStatus is USER_PAUSED.
type JobUserPausedDetails struct {
	JobExpiresAt                        *time.Time `json:"jobExpiresAt,omitempty"`
	JobPausedAt                         *time.Time `json:"jobPausedAt,omitempty"`
	JobImminentExpirationHealthEventArn string     `json:"jobImminentExpirationHealthEventArn,omitempty"`
}

// ClassificationJobSummary is the list-view of a classification job.
type ClassificationJobSummary struct {
	Tags               map[string]string      `json:"tags,omitempty"`
	LastRunTime        *time.Time             `json:"lastRunTime,omitempty"`
	BucketCriteria     any                    `json:"bucketCriteria,omitempty"`
	BucketDefinitions  any                    `json:"bucketDefinitions,omitempty"`
	LastRunErrorStatus *JobLastRunErrorStatus `json:"lastRunErrorStatus,omitempty"`
	UserPausedDetails  *JobUserPausedDetails  `json:"userPausedDetails,omitempty"`
	CreatedAt          time.Time              `json:"createdAt"`
	Description        string                 `json:"description,omitempty"`
	JobID              string                 `json:"jobId"`
	JobStatus          string                 `json:"jobStatus"`
	JobType            string                 `json:"jobType"`
	Name               string                 `json:"name"`
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

// OrgConfig holds organization-level Macie configuration. Matches
// DescribeOrganizationConfigurationOutput exactly: AutoEnable and
// MaxAccountLimitReached are its only two fields in the real SDK.
type OrgConfig struct {
	AutoEnable             bool `json:"autoEnable"`
	MaxAccountLimitReached bool `json:"maxAccountLimitReached"`
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
	Statistics                 *ResourceStatistics `json:"statistics,omitempty"`
	ResourceArn                string              `json:"resourceArn"`
	SensitivityScore           int32               `json:"sensitivityScore"`
	SensitivityScoreOverridden bool                `json:"sensitivityScoreOverridden"`
}

// ResourceStatistics holds classification result counts for a bucket. Real
// GetResourceProfileOutput.Statistics never round-trips a set value in this
// backend (nothing populates it beyond the zero-value struct on read), so
// these key names are unverifiable by a real-client test -- fixed to match
// deserializers.go's ResourceStatistics EqualFold list, disclosed untested.
type ResourceStatistics struct {
	LastRunErroredAt                   *time.Time `json:"lastRunErroredAt,omitempty"`
	LastRunAt                          *time.Time `json:"lastRunAt,omitempty"`
	TotalBytesClassified               int64      `json:"totalBytesClassified"`
	TotalDetections                    int64      `json:"totalDetections"`
	TotalDetectionsSuppressed          int64      `json:"totalDetectionsSuppressed"`
	TotalItemsClassified               int64      `json:"totalItemsClassified"`
	TotalItemsSkipped                  int64      `json:"totalItemsSkipped"`
	TotalItemsSkippedInvalidEncryption int64      `json:"totalItemsSkippedInvalidEncryption"`
	TotalItemsSkippedInvalidKms        int64      `json:"totalItemsSkippedInvalidKms"`
	TotalItemsSkippedPermissionDenied  int64      `json:"totalItemsSkippedPermissionDenied"`
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
