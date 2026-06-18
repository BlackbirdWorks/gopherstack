package inspector2

// StorageBackend is the interface for Inspector2 storage operations.
type StorageBackend interface {
	Enable(resourceTypes []string) error
	Disable(resourceTypes []string) error
	IsEnabled() bool
	GetStatus() *AccountStatusResponse

	CreateFilter(
		name, action, description, reason string,
		criteria map[string]any,
		tags map[string]string,
	) (*Filter, error)
	UpdateFilter(arn, action, description, reason string, criteria map[string]any) (*Filter, error)
	DeleteFilter(arn string) error
	ListFilters(arns []string, action string) ([]*Filter, error)

	ListFindings(maxResults int32, nextToken string, filterCriteria map[string]any) ([]*Finding, string, error)
	SeedFinding(f Finding) (*Finding, error)
	FindingSeverityCounts() map[string]int64
	AddFinding(findingType, severityLabel, status, title, description string, resources []FindingResource) string

	GetConfiguration() *Configuration
	UpdateConfiguration(ec2ScanMode, ecrRescanDuration string) error

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Member operations
	AssociateMember(accountID string) error
	DisassociateMember(accountID string) error
	GetMember(accountID string) (*Member, error)
	ListMembers(onlyAssociated bool) ([]*Member, error)

	// Delegated admin operations
	EnableDelegatedAdminAccount(accountID string) error
	DisableDelegatedAdminAccount(accountID string) error
	GetDelegatedAdminAccount() (*DelegatedAdminAccount, error)
	ListDelegatedAdminAccounts() ([]*DelegatedAdminAccount, error)

	// Organization configuration
	DescribeOrganizationConfiguration() OrgConfiguration
	UpdateOrganizationConfiguration(cfg OrgConfiguration) error

	// EC2 deep inspection
	GetEc2DeepInspectionConfiguration() Ec2DeepInspectionConfig
	UpdateEc2DeepInspectionConfiguration(paths []string) error
	UpdateOrgEc2DeepInspectionConfiguration(paths []string) error
	BatchGetMemberEc2DeepInspectionStatus(accountIDs []string) []*MemberEc2DeepInspectionStatus
	BatchUpdateMemberEc2DeepInspectionStatus(updates []*MemberEc2DeepInspectionStatus) []*MemberEc2DeepInspectionStatus

	// Encryption key operations
	GetEncryptionKey(resourceType, scanType string) (*EncryptionKey, error)
	ResetEncryptionKey(resourceType, scanType string) error
	UpdateEncryptionKey(kmsKeyID, resourceType, scanType string) error

	// CIS scan configuration
	CreateCisScanConfiguration(
		name string,
		schedule map[string]any,
		targets map[string]any,
		tags map[string]string,
	) (*CisScanConfiguration, error)
	DeleteCisScanConfiguration(configARN string) error
	UpdateCisScanConfiguration(
		configARN string,
		name string,
		schedule map[string]any,
		targets map[string]any,
	) (*CisScanConfiguration, error)
	ListCisScanConfigurations() ([]*CisScanConfiguration, error)

	// CIS session operations
	StartCisSession(scanJobID, sessionToken string) (*CisSession, error)
	StopCisSession(scanJobID string) error
	SendCisSessionHealth(scanJobID string) error
	SendCisSessionTelemetry(scanJobID string, messages map[string]any) error
	GetCisScanReport(scanJobID string) (map[string]any, error)
	GetCisScanResultDetails(scanJobID string) (map[string]any, error)
	ListCisScans() ([]map[string]any, error)
	ListCisScanResultsAggregatedByChecks(scanJobID string) ([]map[string]any, error)
	ListCisScanResultsAggregatedByTargetResource(scanJobID string) ([]map[string]any, error)

	// Code security integration
	CreateCodeSecurityIntegration(
		name, integType string,
		tags map[string]string,
		details map[string]any,
	) (*CodeSecurityIntegration, error)
	DeleteCodeSecurityIntegration(integrationARN string) error
	GetCodeSecurityIntegration(integrationARN string) (*CodeSecurityIntegration, error)
	UpdateCodeSecurityIntegration(integrationARN string, details map[string]any) (*CodeSecurityIntegration, error)
	ListCodeSecurityIntegrations() ([]*CodeSecurityIntegration, error)

	// Code security scan configuration
	CreateCodeSecurityScanConfiguration(
		name string,
		scopeSettings map[string]any,
		periodicConfig map[string]any,
		tags map[string]string,
	) (*CodeSecurityScanConfiguration, error)
	DeleteCodeSecurityScanConfiguration(scanConfigARN string) error
	GetCodeSecurityScanConfiguration(scanConfigARN string) (*CodeSecurityScanConfiguration, error)
	UpdateCodeSecurityScanConfiguration(
		scanConfigARN string,
		scopeSettings map[string]any,
		periodicConfig map[string]any,
	) (*CodeSecurityScanConfiguration, error)
	ListCodeSecurityScanConfigurations() ([]*CodeSecurityScanConfiguration, error)
	BatchAssociateCodeSecurityScanConfiguration(scanConfigARN string, resources []string) ([]map[string]any, error)
	BatchDisassociateCodeSecurityScanConfiguration(scanConfigARN string, resources []string) ([]map[string]any, error)
	ListCodeSecurityScanConfigurationAssociations(
		scanConfigARN string,
	) ([]*CodeSecurityScanConfigurationAssociation, error)
	StartCodeSecurityScan(resourceID string) (map[string]any, error)
	GetCodeSecurityScan(scanID string) (map[string]any, error)

	// Findings report
	CreateFindingsReport(destination map[string]any) (*FindingsReport, error)
	CancelFindingsReport(reportID string) error
	GetFindingsReportStatus(reportID string) (*FindingsReport, error)

	// SBOM export
	CreateSbomExport(destination map[string]any) (*SbomExport, error)
	CancelSbomExport(reportID string) error
	GetSbomExport(reportID string) (*SbomExport, error)

	// Coverage
	ListCoverage(filters map[string]any, maxResults int32, nextToken string) ([]*CoverageEntry, string, error)
	ListCoverageStatistics(filters map[string]any) (map[string]any, error)

	// Finding aggregations / usage
	ListFindingAggregations(aggregationType string, filters map[string]any) (map[string]any, error)
	ListUsageTotals(accountIDs []string) ([]map[string]any, error)
	ListAccountPermissions(service string) ([]*AccountPermission, error)
	SearchVulnerabilities(filterCriteria map[string]any, nextToken string) ([]map[string]any, string, error)

	// Batch / misc
	BatchGetCodeSnippet(findingARNs []string) (map[string]any, error)
	BatchGetFindingDetails(findingARNs []map[string]any) (map[string]any, error)
	BatchGetFreeTrialInfo(accountIDs []string) (map[string]any, error)
	GetClustersForImage(filters map[string]any) (map[string]any, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
