package guardduty

import "context"

// StorageBackend is the interface for GuardDuty storage operations.
type StorageBackend interface {
	CreateDetector(enable bool, frequency string, tags map[string]string, features []DetectorFeature) (*Detector, error)
	GetDetector(detectorID string) (*Detector, error)
	UpdateDetector(detectorID string, enable *bool, frequency string, features []DetectorFeature) error
	DeleteDetector(detectorID string) error
	ListDetectors() []string

	CreateFilter(
		detectorID, name, description, action string,
		rank int32,
		findingCriteria map[string]any,
		tags map[string]string,
	) (*Filter, error)
	GetFilter(detectorID, filterName string) (*Filter, error)
	UpdateFilter(
		detectorID, filterName, description, action string,
		rank int32,
		findingCriteria map[string]any,
	) (*Filter, error)
	DeleteFilter(detectorID, filterName string) error
	ListFilters(detectorID string) ([]string, error)

	GetFindings(detectorID string, findingIDs []string) ([]*Finding, error)
	ListFindings(detectorID string, query FindingsQuery) (ids []string, nextToken string, err error)
	ArchiveFindings(detectorID string, findingIDs []string) error
	UnarchiveFindings(detectorID string, findingIDs []string) error
	CreateSampleFindings(detectorID string, findingTypes []string) error
	GetFindingsStatistics(detectorID string, query FindingStatisticsQuery) (map[string]any, error)
	UpdateFindingsFeedback(detectorID string, findingIDs []string, feedback string) error

	CreateIPSet(
		detectorID, name, format, location string,
		activate bool,
		tags map[string]string,
		expectedBucketOwner string,
	) (*IPSet, error)
	GetIPSet(detectorID, ipSetID string) (*IPSet, error)
	UpdateIPSet(detectorID, ipSetID, name, location string, activate *bool, expectedBucketOwner string) error
	DeleteIPSet(detectorID, ipSetID string) error
	ListIPSets(detectorID string) ([]string, error)

	CreateThreatIntelSet(
		detectorID, name, format, location string,
		activate bool,
		tags map[string]string,
		expectedBucketOwner string,
	) (*ThreatIntelSet, error)
	GetThreatIntelSet(detectorID, setID string) (*ThreatIntelSet, error)
	UpdateThreatIntelSet(detectorID, setID, name, location string, activate *bool, expectedBucketOwner string) error
	DeleteThreatIntelSet(detectorID, setID string) error
	ListThreatIntelSets(detectorID string) ([]string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Member management
	CreateMembers(detectorID string, accountDetails []map[string]any) ([]*Member, []map[string]any)
	DeleteMembers(detectorID string, accountIDs []string) ([]map[string]any, error)
	GetMembers(detectorID string, accountIDs []string) ([]*Member, []map[string]any, error)
	InviteMembers(detectorID string, accountIDs []string) ([]map[string]any, error)
	ListMembers(detectorID string, onlyAssociated bool) ([]*Member, error)
	StartMonitoringMembers(detectorID string, accountIDs []string) ([]map[string]any, error)
	StopMonitoringMembers(detectorID string, accountIDs []string) ([]map[string]any, error)
	DisassociateMembers(detectorID string, accountIDs []string) ([]map[string]any, error)
	GetMemberDetectors(detectorID string, accountIDs []string) ([]map[string]any, []map[string]any, error)
	UpdateMemberDetectors(detectorID string, accountIDs []string) ([]map[string]any, error)

	// Invitation management
	AcceptAdministratorInvitation(detectorID, administratorID, invitationID string) error
	AcceptInvitation(detectorID, masterID, invitationID string) error
	GetAdministratorAccount(detectorID string) (*AdminAccount, error)
	GetMasterAccount(detectorID string) (*AdminAccount, error)
	DisassociateFromAdministratorAccount(detectorID string) error
	DisassociateFromMasterAccount(detectorID string) error
	DeclineInvitations(accountIDs []string) []map[string]any
	DeleteInvitations(accountIDs []string) []map[string]any
	GetInvitationsCount() int
	ListInvitations() []*Invitation

	// Organization management
	EnableOrganizationAdminAccount(adminAccountID string) error
	DisableOrganizationAdminAccount(adminAccountID string) error
	ListOrganizationAdminAccounts() []*OrgAdminAccount
	DescribeOrganizationConfiguration(detectorID string) (*OrgConfig, error)
	UpdateOrganizationConfiguration(
		detectorID string,
		autoEnable bool,
		autoEnableOrganizationMembers string,
		features []OrgFeature,
	) error
	GetOrganizationStatistics() map[string]any

	// Publishing destinations
	CreatePublishingDestination(
		detectorID, destType string,
		props DestinationProperties,
		tags map[string]string,
	) (*PublishingDestination, error)
	DeletePublishingDestination(detectorID, destID string) error
	DescribePublishingDestination(detectorID, destID string) (*PublishingDestination, error)
	ListPublishingDestinations(detectorID string) ([]*PublishingDestination, error)
	UpdatePublishingDestination(detectorID, destID string, props DestinationProperties) error

	// Malware scanning
	DescribeMalwareScans(detectorID string, q MalwareScanQuery) ([]*MalwareScan, string, error)
	ListMalwareScans(q MalwareScanQuery) ([]*MalwareScan, string, error)
	StartMalwareScan(resourceARN string) (string, error)
	GetMalwareScan(scanID string) (*MalwareScan, error)
	GetMalwareScanSettings(detectorID string) (*MalwareScanSettings, error)
	UpdateMalwareScanSettings(detectorID string, settings *MalwareScanSettings) error
	GetUsageStatistics(detectorID string, query UsageQuery) (map[string]any, error)
	GetRemainingFreeTrialDays(detectorID string, accountIDs []string) (map[string]any, error)
	GetCoverageStatistics(detectorID string, statisticsType []string) (map[string]any, error)
	ListCoverage(detectorID string) ([]map[string]any, error)

	// Malware protection plans
	CreateMalwareProtectionPlan(
		role string,
		protectedResource, actions map[string]any,
		tags map[string]string,
	) (*MalwareProtectionPlan, error)
	DeleteMalwareProtectionPlan(planID string) error
	GetMalwareProtectionPlan(planID string) (*MalwareProtectionPlan, error)
	ListMalwareProtectionPlans() []*MalwareProtectionPlan
	UpdateMalwareProtectionPlan(planID, role string, protectedResource, actions map[string]any) error
	SendObjectMalwareScan(s3ObjectDetails map[string]any) (string, error)

	// Threat entity sets
	CreateThreatEntitySet(
		detectorID, name, format, location string,
		activate bool,
		tags map[string]string,
		expectedBucketOwner string,
	) (*ThreatEntitySet, error)
	GetThreatEntitySet(detectorID, setID string) (*ThreatEntitySet, error)
	ListThreatEntitySets(detectorID string) ([]string, error)
	UpdateThreatEntitySet(detectorID, setID, name, location string, activate *bool, expectedBucketOwner string) error
	DeleteThreatEntitySet(detectorID, setID string) error

	// Trusted entity sets
	CreateTrustedEntitySet(
		detectorID, name, format, location string,
		activate bool,
		tags map[string]string,
		expectedBucketOwner string,
	) (*TrustedEntitySet, error)
	GetTrustedEntitySet(detectorID, setID string) (*TrustedEntitySet, error)
	ListTrustedEntitySets(detectorID string) ([]string, error)
	UpdateTrustedEntitySet(detectorID, setID, name, location string, activate *bool, expectedBucketOwner string) error
	DeleteTrustedEntitySet(detectorID, setID string) error

	// Investigations (GuardDuty Extended Threat Detection)
	CreateInvestigation(detectorID, triggerPrompt string) (*Investigation, error)
	GetInvestigation(detectorID, investigationID string) (*Investigation, error)
	ListInvestigations(
		detectorID string, q InvestigationsQuery,
	) (investigations []*Investigation, nextToken string, err error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
