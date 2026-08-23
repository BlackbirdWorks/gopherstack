package macie2

import (
	"context"
)

// StorageBackend is the interface for Macie2 storage operations.
type StorageBackend interface {
	// Session management
	GetSession() *Session
	EnableMacie(clientToken, frequency, status string) error
	DisableMacie() error
	UpdateMacieSession(frequency, status string) error

	// Classification job operations
	CreateClassificationJob(
		name, description, jobType, clientToken string,
		s3JobDefinition, scheduleFrequency map[string]any,
		allowListIDs, customDataIdentifierIDs, managedDataIdentifierIDs []string,
		managedDataIdentifierSelector string,
		tags map[string]string,
		samplingPercentage int32,
		initialRun bool,
	) (id, jobArn string, err error)
	DescribeClassificationJob(jobID string) (*ClassificationJob, error)
	ListClassificationJobs(
		filterCriteria map[string]any,
		maxResults int,
		nextToken string,
	) ([]*ClassificationJobSummary, string, error)
	UpdateClassificationJob(jobID, status string) error

	// Member operations
	CreateMember(accountID, email string, tags map[string]string) error
	GetMember(accountID string) (*Member, error)
	DeleteMember(accountID string) error
	ListMembers(onlyAssociated bool, limit int, token string) ([]*Member, string, error)
	DisassociateMember(accountID string) error
	UpdateMemberSession(accountID, status string) error

	// Invitation operations
	CreateInvitations(
		accountIDs []string,
		message string,
		disableEmail bool,
	) ([]UnprocessedAccount, error)
	AcceptInvitation(administratorAccountID, invitationID string) error
	DeclineInvitations(accountIDs []string) ([]UnprocessedAccount, error)
	DeleteInvitations(accountIDs []string) ([]UnprocessedAccount, error)
	GetInvitationsCount() (int64, error)
	ListInvitations() ([]*Invitation, error)

	// Administrator / master account operations
	GetAdministratorAccount() (*AdministratorAccount, error)
	GetMasterAccount() (*AdministratorAccount, error)
	DisassociateFromAdministratorAccount() error
	DisassociateFromMasterAccount() error

	// Organization admin operations
	EnableOrganizationAdminAccount(accountID string) error
	DisableOrganizationAdminAccount(accountID string) error
	ListOrganizationAdminAccounts() ([]*OrgAdminAccount, error)

	// Organization configuration
	DescribeOrganizationConfiguration() (*OrgConfig, error)
	UpdateOrganizationConfiguration(autoEnable bool) error

	// Automated discovery operations
	GetAutomatedDiscoveryConfiguration() (*AutoDiscoveryConfig, error)
	UpdateAutomatedDiscoveryConfiguration(autoEnableMembers, status string) error
	ListAutomatedDiscoveryAccounts() ([]*AutoDiscoveryAccount, error)
	BatchUpdateAutomatedDiscoveryAccounts(updates []AutoDiscoveryAccountUpdate) error

	// Bucket operations
	DescribeBuckets(criteria map[string]any) ([]map[string]any, error)
	GetBucketStatistics(accountID string) (map[string]any, error)

	// Batch custom data identifier
	BatchGetCustomDataIdentifiers(ids []string) ([]*CustomDataIdentifier, error)

	// Classification export configuration
	GetClassificationExportConfiguration() (*ClassificationExportConfig, error)
	PutClassificationExportConfiguration(cfg *ClassificationExportConfig) error

	// Classification scope operations
	GetClassificationScope(scopeID string) (*ClassificationScope, error)
	ListClassificationScopes() ([]*ClassificationScopeSummary, error)
	UpdateClassificationScope(scopeID string, s3 *ClassificationScopeS3Update) error

	// Findings publication configuration
	GetFindingsPublicationConfiguration() (*FindingsPublicationConfig, error)
	PutFindingsPublicationConfiguration(cfg *FindingsPublicationConfig) error

	// Resource profile operations
	GetResourceProfile(resourceARN string) (*ResourceProfile, error)
	UpdateResourceProfile(resourceARN string, sensitivityScore int32) error
	ListResourceProfileArtifacts(resourceARN string) ([]ResourceProfileArtifact, error)
	ListResourceProfileDetections(resourceARN string) ([]ResourceProfileDetection, error)
	UpdateResourceProfileDetections(
		resourceARN string,
		suppressDataIdentifiers []map[string]any,
	) error

	// Reveal configuration
	GetRevealConfiguration() (*RevealConfiguration, error)
	UpdateRevealConfiguration(kmsKeyID, status string) error

	// Sensitive data occurrences
	GetSensitiveDataOccurrences(findingID string) (map[string]any, error)
	GetSensitiveDataOccurrencesAvailability(findingID string) (string, []string, error)

	// Sensitivity inspection template operations
	GetSensitivityInspectionTemplate(templateID string) (*SensitivityInspectionTemplate, error)
	ListSensitivityInspectionTemplates() ([]*SensitivityInspectionTemplateSummary, error)
	UpdateSensitivityInspectionTemplate(
		templateID, name string,
		description *string,
		excludes, includes map[string]any,
	) error

	// Usage operations
	GetUsageStatistics(
		filterBy []map[string]any,
		maxResults int,
		nextToken, sortBy string,
	) ([]UsageRecord, string, error)
	GetUsageTotals(timeRange string) ([]UsageTotal, error)

	// Managed data identifiers
	ListManagedDataIdentifiers() ([]ManagedDataIdentifier, error)

	// Search resources
	SearchResources(
		bucketCriteria map[string]any,
		maxResults int,
		nextToken string,
	) ([]map[string]any, string, error)

	// Allow list operations
	CreateAllowList(
		name, description string,
		criteria AllowListCriteria,
		tags map[string]string,
	) (*AllowListSummary, error)
	GetAllowList(id string) (*AllowListDetail, error)
	UpdateAllowList(
		id, name, description string,
		criteria AllowListCriteria,
	) (*AllowListSummary, error)
	DeleteAllowList(id string) error
	ListAllowLists(limit int, token string) ([]*AllowListSummary, string, error)

	// Custom data identifier operations
	CreateCustomDataIdentifier(
		name, description, regex string,
		ignoreWords, keywords []string,
		severityLevels []SeverityLevel,
		maxMatchDistance *int32,
		tags map[string]string,
	) (string, error)
	GetCustomDataIdentifier(id string) (*CustomDataIdentifier, error)
	DeleteCustomDataIdentifier(id string) error
	ListCustomDataIdentifiers(limit int, token string) ([]*CustomDataIdentifierSummary, string, error)
	TestCustomDataIdentifier(
		regex string,
		ignoreWords, keywords []string,
		maxMatchDistance *int32,
		sampleText string,
	) (int32, error)

	// Findings filter operations
	CreateFindingsFilter(
		name, description, action string,
		position *int32,
		criteria map[string]any,
		tags map[string]string,
	) (*FindingsFilterSummary, error)
	GetFindingsFilter(id string) (*FindingsFilterDetail, error)
	UpdateFindingsFilter(
		id, name, description, action string,
		position *int32,
		criteria map[string]any,
	) (*FindingsFilterSummary, error)
	DeleteFindingsFilter(id string) error
	ListFindingsFilters(limit int, token string) ([]*FindingsFilterSummary, string, error)

	// Finding operations
	GetFindings(findingIDs []string) ([]*Finding, error)
	ListFindings(
		criteria map[string]any,
		maxResults int,
		nextToken string,
	) ([]string, string, error)
	CreateSampleFindings(findingTypes []string) error
	GetFindingStatistics(groupBy string, criteria map[string]any) ([]FindingStatisticsGroup, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Lifecycle
	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
