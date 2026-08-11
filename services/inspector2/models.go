package inspector2

import "time"

// Filter represents an Inspector2 findings filter.
type Filter struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Criteria    map[string]any    `json:"filterCriteria,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Arn         string            `json:"arn"`
	Name        string            `json:"name"`
	Action      string            `json:"action"`
	Description string            `json:"description,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	OwnerID     string            `json:"ownerId"`
}

// Finding represents an Inspector2 finding. The store is seedable so callers
// (tests, fixtures, the dashboard) can inject realistic findings that
// ListFindings will then return and filter — behavior that exceeds LocalStack,
// which always returns an empty list.
//
// EpssScore/RiskScore/Cwes/ReferenceUrls/Tools/Ttps back BatchGetFindingDetails
// only (real FindingDetail shape) -- findingToWire (ListFindings' wire
// shape) never renders them, matching the real API where these live on a
// separate FindingDetail resource, not on Finding itself. (Field order below
// is fieldalignment-optimized, not declaration/doc order.)
type Finding struct {
	FirstObservedAt time.Time         `json:"firstObservedAt"`
	LastObservedAt  time.Time         `json:"lastObservedAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
	Title           string            `json:"title,omitempty"`
	FindingArn      string            `json:"findingArn"`
	ResourceID      string            `json:"-"`
	ResourceType    string            `json:"-"`
	FixAvailable    string            `json:"fixAvailable,omitempty"`
	Description     string            `json:"description"`
	AccountID       string            `json:"awsAccountId"`
	Type            string            `json:"type"`
	Status          string            `json:"status"`
	Resources       []FindingResource `json:"resources,omitempty"`
	Cwes            []string          `json:"cwes,omitempty"`
	Severity        FindingSeverity   `json:"severity"`
	Tools           []string          `json:"tools,omitempty"`
	ReferenceUrls   []string          `json:"referenceUrls,omitempty"`
	// Ttps holds FindingDetail.Ttps (MITRE adversary tactics/techniques/
	// procedures) -- a plain string list, identical in shape to Tools/Cwes/
	// ReferenceUrls above, added alongside them this pass (field-diffed
	// against types.FindingDetail; previously omitted despite being no more
	// complex than the fields already modeled).
	Ttps      []string `json:"ttps,omitempty"`
	EpssScore float64  `json:"epssScore,omitempty"`
	RiskScore int32    `json:"riskScore,omitempty"`
}

// CodeLine is a single line of a retrieved code snippet (real CodeLine shape).
type CodeLine struct {
	Content    string `json:"content"`
	LineNumber int32  `json:"lineNumber"`
}

// SuggestedFix is a suggested remediation for a code vulnerability finding
// (real SuggestedFix shape).
type SuggestedFix struct {
	Code        string `json:"code,omitempty"`
	Description string `json:"description,omitempty"`
}

// codeSnippet holds the seeded code snippet content for one finding ARN,
// backing BatchGetCodeSnippet. Real AWS only ever returns a snippet for
// findings it has associated code context with; gopherstack has no static
// analysis engine to derive that content, so it must be seeded (same
// additive-capability precedent as SeedFinding/SeedCoverage/SeedVulnerability).
type codeSnippet struct {
	FindingArn     string         `json:"findingArn"`
	Lines          []CodeLine     `json:"codeSnippet,omitempty"`
	SuggestedFixes []SuggestedFix `json:"suggestedFixes,omitempty"`
	StartLine      int32          `json:"startLine,omitempty"`
	EndLine        int32          `json:"endLine,omitempty"`
}

// FindingSeverity holds severity details for a finding.
type FindingSeverity struct {
	Label string  `json:"label"`
	Score float64 `json:"score,omitempty"`
}

// FindingResource describes a resource associated with a finding.
type FindingResource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// storedFinding wraps Finding for internal storage.
type storedFinding struct {
	Finding
}

// Configuration holds Inspector2 scan configuration.
type Configuration struct {
	Ec2ScanMode       string `json:"ec2ScanMode"`
	EcrRescanDuration string `json:"ecrRescanDuration"`
}

// AccountStatusResponse holds Enable/Disable/BatchGetAccountStatus output.
type AccountStatusResponse struct {
	AccountID    string `json:"accountId"`
	Status       string `json:"status"`
	Ec2Status    string `json:"ec2Status"`
	EcrStatus    string `json:"ecrStatus"`
	LambdaStatus string `json:"lambdaStatus"`
}

// Member represents an Inspector2 member account.
type Member struct {
	UpdatedAt               time.Time `json:"updatedAt"`
	AccountID               string    `json:"accountId"`
	DelegatedAdminAccountID string    `json:"delegatedAdminAccountId"`
	Email                   string    `json:"email"`
	RelationshipStatus      string    `json:"relationshipStatus"`
}

// DelegatedAdminAccount represents a delegated admin account.
type DelegatedAdminAccount struct {
	AccountID string `json:"accountId"`
	Status    string `json:"status"`
}

// OrgConfiguration holds organization-level Inspector2 settings.
type OrgConfiguration struct {
	AutoEnable             bool `json:"autoEnable"`
	MaxAccountLimitReached bool `json:"maxAccountLimitReached"`
}

// OrgEc2DeepInspectionConfig holds org-level EC2 deep inspection settings.
type OrgEc2DeepInspectionConfig struct {
	CustomPaths []string `json:"orgPackagePaths"`
}

// Ec2DeepInspectionConfig holds EC2 deep inspection configuration.
type Ec2DeepInspectionConfig struct {
	Status       string   `json:"status"`
	ErrorMessage string   `json:"errorMessage,omitempty"`
	PackagePaths []string `json:"packagePaths"`
}

// MemberEc2DeepInspectionStatus holds EC2 deep inspection status for a member.
type MemberEc2DeepInspectionStatus struct {
	AccountID    string   `json:"accountId"`
	Status       string   `json:"status"`
	ErrorMessage string   `json:"errorMessage,omitempty"`
	PackagePaths []string `json:"packagePaths"`
}

// EncryptionKey holds an encryption key for a resource type.
type EncryptionKey struct {
	KmsKeyID     string `json:"kmsKeyId"`
	ResourceType string `json:"resourceType"`
	ScanType     string `json:"scanType"`
}

// CisScanConfiguration represents a CIS scan configuration.
type CisScanConfiguration struct {
	Tags       map[string]string `json:"tags,omitempty"`
	ScheduleV2 map[string]any    `json:"schedule,omitempty"`
	Targets    map[string]any    `json:"targets,omitempty"`
	Arn        string            `json:"scanConfigurationArn"`
	Name       string            `json:"scanName"`
	OwnedBy    string            `json:"ownedBy"`
}

// CisSession represents an active CIS scan session.
type CisSession struct {
	StartedAt    time.Time `json:"startedAt"`
	ScanJobID    string    `json:"scanJobId"`
	SessionToken string    `json:"sessionToken"`
	Status       string    `json:"status"`
}

// CisCheckResult is a single CIS benchmark check outcome for one target resource.
type CisCheckResult struct {
	CheckID      string `json:"checkId"`
	CheckDescr   string `json:"checkDescription"`
	Level        string `json:"level"`
	Platform     string `json:"platform"`
	Status       string `json:"status"`
	TargetID     string `json:"targetResourceId"`
	AccountID    string `json:"accountId"`
	StatusReason string `json:"statusReason,omitempty"`
}

// CisScan is a completed CIS scan run produced from a scan configuration. It
// carries the per-check results so the report, result-detail and aggregation
// operations all derive from the same stored state rather than canned data.
type CisScan struct {
	ScheduledAt          time.Time         `json:"scheduledBy"`
	FinishedAt           time.Time         `json:"finishedAt"`
	ScanArn              string            `json:"scanArn"`
	ScanConfigurationArn string            `json:"scanConfigurationArn"`
	ScanName             string            `json:"scanName"`
	Status               string            `json:"status"`
	SecurityLevel        string            `json:"securityLevel"`
	TargetAccountID      string            `json:"targetAccountId"`
	Results              []*CisCheckResult `json:"results"`
	TotalChecks          int               `json:"totalChecks"`
	FailedChecks         int               `json:"failedChecks"`
}

// CodeSecurityIntegration represents a code security integration.
type CodeSecurityIntegration struct {
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
	Tags           map[string]string `json:"tags,omitempty"`
	IntegrationArn string            `json:"integrationArn"`
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Status         string            `json:"status"`
}

// CodeSecurityScanConfiguration represents a code security scan configuration.
//
// Level and RuleSetCategories mirror CreateCodeSecurityScanConfigurationInput's
// required members: level sits alongside the nested "configuration" object on
// the wire, while RuleSetCategories/PeriodicScanConfig/
// ContinuousIntegrationScanConfig all nest *under* "configuration" (confirmed
// via serializers.go's awsRestjson1_serializeDocumentCodeSecurityScanConfiguration)
// -- see codeSecurityScanConfigToWire for the reshape. UpdateCodeSecurityScanConfiguration
// only ever replaces that nested configuration object:
// UpdateCodeSecurityScanConfigurationInput has just "configuration" and
// "scanConfigurationArn" members, so level/scopeSettings/name are immutable
// after creation on the real API. There is no "status" member anywhere on the
// real CodeSecurityScanConfiguration/GetCodeSecurityScanConfigurationOutput
// shape -- an earlier revision fabricated one; do not reintroduce it.
type CodeSecurityScanConfiguration struct {
	CreatedAt                       time.Time
	UpdatedAt                       time.Time
	ScopeSettings                   map[string]any
	PeriodicScanConfig              map[string]any
	ContinuousIntegrationScanConfig map[string]any
	Tags                            map[string]string
	Arn                             string
	Name                            string
	Level                           string
	RuleSetCategories               []string
}

// CodeSecurityScanConfigurationAssociation links a scan config to a repository.
type CodeSecurityScanConfigurationAssociation struct {
	ScanConfigurationArn string `json:"scanConfigurationArn"`
	Resource             string `json:"resource"`
	Status               string `json:"status"`
}

// FindingsReport represents an async findings report job. FilterCriteria and
// ReportFormat are captured from CreateFindingsReport's request and echoed
// back by GetFindingsReportStatus (real GetFindingsReportStatusOutput wire
// keys: destination/errorCode/errorMessage/filterCriteria/reportId/status --
// note the real shape has no createdAt member at all, so CreatedAt below is
// backend bookkeeping only and must never reach the wire).
type FindingsReport struct {
	CreatedAt      time.Time      `json:"createdAt"`
	Destination    map[string]any `json:"destination,omitempty"`
	FilterCriteria map[string]any `json:"filterCriteria,omitempty"`
	ReportID       string         `json:"reportId"`
	ReportFormat   string         `json:"reportFormat,omitempty"`
	Status         string         `json:"status"`
	ErrorCode      string         `json:"errorCode,omitempty"`
	ErrorMessage   string         `json:"errorMessage,omitempty"`
}

// SbomExport represents an async SBOM export job. Real GetSbomExportOutput
// wire keys: errorCode/errorMessage/filterCriteria/format/reportId/
// s3Destination/status (no createdAt member; see FindingsReport's doc comment).
type SbomExport struct {
	CreatedAt      time.Time      `json:"createdAt"`
	Destination    map[string]any `json:"s3Destination,omitempty"`
	FilterCriteria map[string]any `json:"filterCriteria,omitempty"`
	ReportID       string         `json:"reportId"`
	Format         string         `json:"format,omitempty"`
	Status         string         `json:"status"`
	ErrorCode      string         `json:"errorCode,omitempty"`
	ErrorMessage   string         `json:"errorMessage,omitempty"`
}

// CoverageScanStatus mirrors the real ScanStatus shape (statusCode/reason).
type CoverageScanStatus struct {
	StatusCode string `json:"statusCode"`
	Reason     string `json:"reason,omitempty"`
}

// CoverageEntry represents a resource covered by Inspector2, matching the
// real CoveredResource shape (accountId/resourceId/resourceType/scanType are
// required; lastScannedAt/scanMode/scanStatus are optional). Seeded via
// SeedCoverage -- ListCoverage/ListCoverageStatistics were previously
// hardwired-empty stubs with no way to populate real data, unlike Finding's
// SeedFinding.
type CoverageEntry struct {
	LastScannedAt time.Time           `json:"lastScannedAt"`
	ScanStatus    *CoverageScanStatus `json:"scanStatus,omitempty"`
	AccountID     string              `json:"accountId"`
	ResourceID    string              `json:"resourceId"`
	ResourceType  string              `json:"resourceType"`
	ScanType      string              `json:"scanType"`
	ScanMode      string              `json:"scanMode,omitempty"`
}

// Vulnerability represents a known vulnerability, matching the real
// Vulnerability shape's field names (wire key "id", not "vulnerabilityId";
// "vendorSeverity", not the gopherstack-invented "severity"). Only the
// scalar/list fields are modeled -- the nested AtigData/CisaData/Cvss*/Epss/
// ExploitObserved objects are real but omitted here (an omitted optional
// field is unset on the wire, not wire-breaking, unlike a wrong key name).
// Seeded via SeedVulnerability, following the same additive-capability
// precedent as Finding's SeedFinding: SearchVulnerabilities queries AWS's
// own global vulnerability intelligence database in real Inspector2, which
// gopherstack has no equivalent data source for.
type Vulnerability struct {
	VendorCreatedAt        time.Time `json:"vendorCreatedAt"`
	VendorUpdatedAt        time.Time `json:"vendorUpdatedAt"`
	ID                     string    `json:"id"`
	Description            string    `json:"description,omitempty"`
	Source                 string    `json:"source,omitempty"`
	SourceURL              string    `json:"sourceUrl,omitempty"`
	VendorSeverity         string    `json:"vendorSeverity,omitempty"`
	Cwes                   []string  `json:"cwes,omitempty"`
	ReferenceUrls          []string  `json:"referenceUrls,omitempty"`
	RelatedVulnerabilities []string  `json:"relatedVulnerabilities,omitempty"`
}

// AccountPermission represents an Inspector2 account-level permission,
// matching the real Permission shape (operation/service). The prior "status"
// field was a gopherstack-invented member with no counterpart in the real
// API -- deleted in favor of the real "service" field.
type AccountPermission struct {
	Operation string `json:"operation"`
	Service   string `json:"service"`
}

// ConnectorHealth reports the connectivity/authorization health of a
// Connector, matching the real ConnectorHealth shape. Real
// ConnectorHealthStatus includes PENDING_AUTHORIZATION: an Azure connector
// requires completing an external Azure AD app-consent (OAuth) flow in the
// Azure portal before Amazon Inspector can reach the tenant, and none of
// CreateConnector/UpdateConnector/DeleteConnector/ListConnectors/
// ListConnectorScanConfigurations/UpdateConnectorScanConfiguration (the
// entire connector SDK surface) drives or observes that step. See
// connectors.go's CreateConnector doc comment for why this backend leaves
// Health permanently at PENDING_AUTHORIZATION rather than faking a
// transition to CONNECTED.
type ConnectorHealth struct {
	LastCheckedAt   time.Time `json:"lastCheckedAt"`
	ConnectorStatus string    `json:"connectorStatus"`
	Message         string    `json:"message,omitempty"`
}

// ConnectorScopeSetting represents one scanning type's Azure resource scope
// (VM, container image, or serverless), matching the real ScopeConfiguration
// shape. State reflects that this backend can never validate a submitted
// scope against a live Azure tenant (the connector itself never leaves
// PENDING_AUTHORIZATION), so State is always PENDING here, never ACTIVE/
// ERROR/DISABLED.
type ConnectorScopeSetting struct {
	ScopeType   string   `json:"scopeType"`
	State       string   `json:"state,omitempty"`
	StateReason string   `json:"stateReason,omitempty"`
	ScopeValues []string `json:"scopeValues,omitempty"`
}

// ConnectorScopeConfiguration mirrors the real AzureScopeConfiguration shape:
// the scope of Azure resources scanned, defined separately per scanning type.
type ConnectorScopeConfiguration struct {
	ContainerImageScanning *ConnectorScopeSetting `json:"containerImageScanning,omitempty"`
	ServerlessScanning     *ConnectorScopeSetting `json:"serverlessScanning,omitempty"`
	VMScanning             *ConnectorScopeSetting `json:"vmScanning,omitempty"`
}

// Connector represents an Amazon Inspector connector linking an external
// cloud provider (currently only Azure, per the real ConnectorCloudProvider
// enum's single AZURE value) to Inspector for vulnerability scanning,
// matching the real Connector shape (types.Connector, field-diffed against
// aws-sdk-go-v2/service/inspector2/types@v1.54.1's deserializers.go).
type Connector struct {
	CreatedAt              time.Time                    `json:"createdAt"`
	UpdatedAt              time.Time                    `json:"updatedAt"`
	Health                 *ConnectorHealth             `json:"health,omitempty"`
	ScopeConfiguration     *ConnectorScopeConfiguration `json:"scopeConfiguration,omitempty"`
	Tags                   map[string]string            `json:"tags,omitempty"`
	ConnectorArn           string                       `json:"connectorArn"`
	Name                   string                       `json:"name"`
	Description            string                       `json:"description,omitempty"`
	Provider               string                       `json:"provider"`
	AwsConfigConnectorArn  string                       `json:"awsConfigConnectorArn,omitempty"`
	EnablementStatus       string                       `json:"enablementStatus"`
	EnablementStatusReason string                       `json:"enablementStatusReason,omitempty"`
	AzureRegions           []string                     `json:"azureRegions,omitempty"`
	AutoInstallVMScanner   bool                         `json:"autoInstallVMScanner"`
}

// ConnectorContainerImageScanConfig mirrors the real
// ConnectorContainerImageScanConfiguration shape: the rescan-duration
// settings applied to container images pulled/pushed through a connector's
// connected registries.
type ConnectorContainerImageScanConfig struct {
	PullDuration string `json:"pullDuration,omitempty"`
	PushDuration string `json:"pushDuration,omitempty"`
}

// ConnectorScanConfiguration represents the scan settings applied to
// resources discovered through every connector sharing one AWS Config
// connector ARN, matching the real ConnectorScanConfiguration shape. Stored
// keyed by AwsConfigConnectorArn -- there is no CreateConnectorScanConfiguration
// operation in the real API, so UpdateConnectorScanConfiguration is the only
// write path (see connectors.go).
type ConnectorScanConfiguration struct {
	ContainerImageScanning *ConnectorContainerImageScanConfig `json:"containerImageScanning,omitempty"`
	AwsConfigConnectorArn  string                             `json:"awsConfigConnectorArn"`
}

// ConnectorScanConfigurationItem represents one entry of
// ListConnectorScanConfigurations' result, matching the real
// ConnectorScanConfigurationItem shape: the AWS Config connector ARN, the
// (dynamically derived) list of connector ARNs currently sharing it, and its
// scan configuration.
type ConnectorScanConfigurationItem struct {
	ScanConfiguration     *ConnectorScanConfiguration `json:"scanConfiguration"`
	AwsConfigConnectorArn string                      `json:"awsConfigConnectorArn"`
	ConnectorArns         []string                    `json:"connectorArns"`
}
