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
type Finding struct {
	FirstObservedAt time.Time         `json:"firstObservedAt"`
	LastObservedAt  time.Time         `json:"lastObservedAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
	Description     string            `json:"description"`
	AccountID       string            `json:"awsAccountId"`
	Type            string            `json:"type"`
	Status          string            `json:"status"`
	Title           string            `json:"title,omitempty"`
	FindingArn      string            `json:"findingArn"`
	FixAvailable    string            `json:"fixAvailable,omitempty"`
	ResourceType    string            `json:"-"`
	ResourceID      string            `json:"-"`
	Severity        FindingSeverity   `json:"severity"`
	Resources       []FindingResource `json:"resources,omitempty"`
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
type CodeSecurityScanConfiguration struct {
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
	ScopeSettings      map[string]any    `json:"scopeSettings,omitempty"`
	PeriodicScanConfig map[string]any    `json:"periodicScanConfiguration,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	Arn                string            `json:"scanConfigurationArn"`
	Name               string            `json:"name"`
	IntegrationArn     string            `json:"integrationArn,omitempty"`
	Status             string            `json:"status"`
}

// CodeSecurityScanConfigurationAssociation links a scan config to a repository.
type CodeSecurityScanConfigurationAssociation struct {
	ScanConfigurationArn string `json:"scanConfigurationArn"`
	Resource             string `json:"resource"`
	Status               string `json:"status"`
}

// FindingsReport represents an async findings report job.
type FindingsReport struct {
	CreatedAt    time.Time      `json:"createdAt"`
	Destination  map[string]any `json:"destination,omitempty"`
	ReportID     string         `json:"reportId"`
	Status       string         `json:"status"`
	ErrorCode    string         `json:"errorCode,omitempty"`
	ErrorMessage string         `json:"errorMessage,omitempty"`
}

// SbomExport represents an async SBOM export job.
type SbomExport struct {
	CreatedAt    time.Time      `json:"createdAt"`
	Destination  map[string]any `json:"destination,omitempty"`
	ReportID     string         `json:"reportId"`
	Status       string         `json:"status"`
	ErrorCode    string         `json:"errorCode,omitempty"`
	ErrorMessage string         `json:"errorMessage,omitempty"`
}

// CoverageEntry represents a resource covered by Inspector2.
type CoverageEntry struct {
	ScanStatus   map[string]any `json:"scanStatus"`
	AccountID    string         `json:"accountId"`
	ResourceID   string         `json:"resourceId"`
	ResourceType string         `json:"resourceType"`
	ScanType     string         `json:"scanType"`
}

// Vulnerability represents a known vulnerability.
type Vulnerability struct {
	VulnerabilityID string `json:"vulnerabilityId"`
	Description     string `json:"description"`
	Severity        string `json:"severity"`
}

// AccountPermission represents an Inspector2 account-level permission.
type AccountPermission struct {
	Operation string `json:"operation"`
	Status    string `json:"status"`
}
