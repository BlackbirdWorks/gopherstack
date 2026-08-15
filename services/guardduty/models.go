package guardduty

import "time"

// Detector represents a GuardDuty detector.
type Detector struct {
	CreatedAt                  time.Time         `json:"createdAt"`
	UpdatedAt                  time.Time         `json:"updatedAt"`
	DetectorID                 string            `json:"detectorId"`
	Status                     string            `json:"status"`
	FindingPublishingFrequency string            `json:"findingPublishingFrequency,omitempty"`
	ServiceRole                string            `json:"serviceRole"`
	Tags                       map[string]string `json:"tags,omitempty"`
	Features                   []DetectorFeature `json:"features,omitempty"`
}

// DetectorFeature represents a feature configuration for a detector.
type DetectorFeature struct {
	Name                    string             `json:"name"`
	Status                  string             `json:"status"`
	AdditionalConfiguration []AdditionalConfig `json:"additionalConfiguration,omitempty"`
}

// AdditionalConfig holds extra feature-level configuration.
type AdditionalConfig struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Filter represents a GuardDuty filter.
type Filter struct {
	CreatedAt       time.Time         `json:"createdAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
	FindingCriteria map[string]any    `json:"findingCriteria,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	Action          string            `json:"action"`
	DetectorID      string            `json:"-"`
	Rank            int32             `json:"rank"`
	// Version mirrors real GetFilterOutput.Version ("Every time the filter
	// is updated, the version increments by 1").
	Version int64 `json:"version"`
}

// Finding represents a GuardDuty finding.
type Finding struct {
	AccountID     string          `json:"accountId"`
	SchemaVersion string          `json:"schemaVersion"`
	CreatedAt     string          `json:"createdAt"`
	Description   string          `json:"description"`
	DetectorID    string          `json:"detectorId"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Title         string          `json:"title"`
	Region        string          `json:"region"`
	UpdatedAt     string          `json:"updatedAt"`
	Arn           string          `json:"arn"`
	Resource      FindingResource `json:"resource"`
	Service       FindingService  `json:"service"`
	Severity      float64         `json:"severity"`
}

// FindingService holds service-level metadata for a finding.
type FindingService struct {
	DetectorID     string `json:"detectorId"`
	EventFirstSeen string `json:"eventFirstSeen"`
	EventLastSeen  string `json:"eventLastSeen"`
	ResourceRole   string `json:"resourceRole"`
	ServiceName    string `json:"serviceName"`
	UserFeedback   string `json:"userFeedback,omitempty"`
	Count          int32  `json:"count"`
	Archived       bool   `json:"archived"`
}

// FindingResource describes the AWS resource involved in a finding.
type FindingResource struct {
	ResourceType string `json:"resourceType"`
}

// IPSet represents a GuardDuty IP set.
type IPSet struct {
	CreatedAt  time.Time         `json:"createdAt"`
	UpdatedAt  time.Time         `json:"updatedAt"`
	IPSetID    string            `json:"ipSetId"`
	Name       string            `json:"name"`
	Format     string            `json:"format"`
	Location   string            `json:"location"`
	Status     string            `json:"status"`
	Tags       map[string]string `json:"tags,omitempty"`
	DetectorID string            `json:"-"`
	// ExpectedBucketOwner mirrors real GetIPSetOutput.ExpectedBucketOwner:
	// present only if supplied at creation or update time (CreateIPSetInput/
	// UpdateIPSetInput both carry it).
	ExpectedBucketOwner string `json:"expectedBucketOwner,omitempty"`
}

// ThreatIntelSet represents a GuardDuty threat intelligence set.
type ThreatIntelSet struct {
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	ThreatIntelSetID string            `json:"threatIntelSetId"`
	Name             string            `json:"name"`
	Format           string            `json:"format"`
	Location         string            `json:"location"`
	Status           string            `json:"status"`
	Tags             map[string]string `json:"tags,omitempty"`
	DetectorID       string            `json:"-"`
	// ExpectedBucketOwner mirrors real GetThreatIntelSetOutput.ExpectedBucketOwner,
	// same as IPSet.ExpectedBucketOwner above.
	ExpectedBucketOwner string `json:"expectedBucketOwner,omitempty"`
}

// Member represents a GuardDuty member account.
type Member struct {
	UpdatedAt          time.Time `json:"updatedAt"`
	AccountID          string    `json:"accountId"`
	AdministratorID    string    `json:"administratorId"`
	MasterID           string    `json:"masterId"`
	DetectorID         string    `json:"detectorId"`
	Email              string    `json:"email"`
	RelationshipStatus string    `json:"relationshipStatus"`
	InvitedAt          string    `json:"invitedAt"`
}

// Invitation represents a pending GuardDuty invitation.
type Invitation struct {
	AccountID          string `json:"accountId"`
	InvitationID       string `json:"invitationId"`
	InvitedAt          string `json:"invitedAt"`
	RelationshipStatus string `json:"relationshipStatus"`
}

// AdminAccount represents the administrator account relationship for GetAdministratorAccount.
type AdminAccount struct {
	AccountID          string `json:"accountId"`
	InvitationID       string `json:"invitationId"`
	InvitedAt          string `json:"invitedAt"`
	RelationshipStatus string `json:"relationshipStatus"`
	// detectorID is the store.Table composite-key qualifier (see
	// adminAccountTableKeyFn in store_setup.go); it has no wire shape of its
	// own -- AdminAccount carries no identity field, so this was added
	// purely for the table's key -- and is carried through persistence via
	// byDetectorDTO (see persistence.go).
	detectorID string
}

// OrgAdminAccount represents an organization admin account.
type OrgAdminAccount struct {
	AdminAccountID string `json:"adminAccountId"`
	AdminStatus    string `json:"adminStatus"`
}

// OrgConfig holds org-level GuardDuty configuration.
type OrgConfig struct {
	DataSources                   map[string]any `json:"dataSources"`
	detectorID                    string
	AutoEnableOrganizationMembers string       `json:"autoEnableOrganizationMembers,omitempty"`
	Features                      []OrgFeature `json:"features"`
	AutoEnable                    bool         `json:"autoEnable"`
	MemberAccountLimitReached     bool         `json:"memberAccountLimitReached"`
}

// OrgFeature holds org-level feature configuration.
type OrgFeature struct {
	AutoEnable string `json:"autoEnable"`
	Name       string `json:"name"`
}

// PublishingDestination represents a GuardDuty publishing destination.
type PublishingDestination struct {
	DestinationProperties      DestinationProperties `json:"destinationProperties"`
	Tags                       map[string]string     `json:"tags,omitempty"`
	DestinationID              string                `json:"destinationId"`
	DestinationType            string                `json:"destinationType"`
	Status                     string                `json:"status"`
	ServicePrincipal           string                `json:"servicePrincipal,omitempty"`
	DetectorID                 string                `json:"-"`
	PublishingFailureStartedAt int64                 `json:"publishingFailureStartedAt,omitempty"`
}

// DestinationProperties holds properties for a publishing destination.
type DestinationProperties struct {
	DestinationArn string `json:"destinationArn,omitempty"`
	KmsKeyArn      string `json:"kmsKeyArn,omitempty"`
}

// MalwareScan represents a GuardDuty malware scan result. It backs both the
// older Scan shape (DescribeMalwareScans/ListMalwareScans) and the richer,
// distinct GetMalwareScanOutput shape (see handleGetMalwareScan) -- the two
// real API shapes only share scanId/detectorId/scanStatus/scanType by name,
// so this struct is a superset covering both.
type MalwareScan struct {
	ScanID                string         `json:"scanId"`
	DetectorID            string         `json:"detectorId"`
	AdminDetectorID       string         `json:"adminDetectorId,omitempty"`
	AccountID             string         `json:"accountId"`
	ResourceArn           string         `json:"resourceArn,omitempty"`
	ResourceType          string         `json:"resourceType,omitempty"`
	ScanCategory          string         `json:"scanCategory,omitempty"`
	ScanStartTime         time.Time      `json:"scanStartTime"`
	ScanEndTime           time.Time      `json:"scanEndTime"`
	ScanStatus            string         `json:"scanStatus"`
	ScanType              string         `json:"scanType"`
	ScanStatusReason      string         `json:"scanStatusReason,omitempty"`
	TriggerDetails        map[string]any `json:"triggerDetails"`
	ResourceDetails       map[string]any `json:"resourceDetails"`
	Findings              []any          `json:"findings"`
	ScannedResourcesCount int32          `json:"scannedResourcesCount"`
	SkippedResourcesCount int32          `json:"skippedResourcesCount"`
	FailedResourcesCount  int32          `json:"failedResourcesCount"`
}

// MalwareScanSettings holds malware scan configuration for a detector.
type MalwareScanSettings struct {
	ScanResourceCriteria    map[string]any `json:"scanResourceCriteria"`
	EbsSnapshotPreservation string         `json:"ebsSnapshotPreservation"`
	// detectorID is the store.Table composite-key qualifier (see
	// malwareScanSettingsTableKeyFn in store_setup.go); see
	// AdminAccount.detectorID.
	detectorID string
}

// MalwareProtectionPlan represents a malware protection plan.
type MalwareProtectionPlan struct {
	CreatedAt               time.Time         `json:"createdAt"`
	ProtectedResource       map[string]any    `json:"protectedResource"`
	Actions                 map[string]any    `json:"actions"`
	Tags                    map[string]string `json:"tags,omitempty"`
	MalwareProtectionPlanID string            `json:"malwareProtectionPlanId"`
	Arn                     string            `json:"arn"`
	Role                    string            `json:"role"`
	Status                  string            `json:"status"`
	StatusReasons           []any             `json:"statusReasons"`
}

// ThreatEntitySet represents a GuardDuty threat entity set.
type ThreatEntitySet struct {
	CreatedAt           time.Time         `json:"createdAt"`
	UpdatedAt           time.Time         `json:"updatedAt"`
	Tags                map[string]string `json:"tags,omitempty"`
	ThreatEntitySetID   string            `json:"threatEntitySetId"`
	DetectorID          string            `json:"-"`
	Name                string            `json:"name"`
	Format              string            `json:"format"`
	Location            string            `json:"location"`
	Status              string            `json:"status"`
	ExpectedBucketOwner string            `json:"expectedBucketOwner,omitempty"`
}

// TrustedEntitySet represents a GuardDuty trusted entity set.
type TrustedEntitySet struct {
	CreatedAt           time.Time         `json:"createdAt"`
	UpdatedAt           time.Time         `json:"updatedAt"`
	Tags                map[string]string `json:"tags,omitempty"`
	TrustedEntitySetID  string            `json:"trustedEntitySetId"`
	DetectorID          string            `json:"-"`
	Name                string            `json:"name"`
	Format              string            `json:"format"`
	Location            string            `json:"location"`
	Status              string            `json:"status"`
	ExpectedBucketOwner string            `json:"expectedBucketOwner,omitempty"`
}

// Investigation represents a GuardDuty Extended Threat Detection
// investigation (CreateInvestigation/GetInvestigation/ListInvestigations).
//
// This backend has no threat-analysis engine: it cannot correlate findings,
// perform account-level analysis, score risk/confidence, or produce a
// natural-language summary. Every investigation this backend creates is
// therefore modeled honestly as perpetually RUNNING -- the same treatment
// this package already gives MalwareScan, which for the identical reason
// never transitions out of RUNNING either (see GetMalwareScan's PARITY.md
// note: "this backend's scans never transition to SKIPPED/COMPLETED/
// FAILED").
//
// Cloud, Confidence, Error, Metadata, Risk, RiskLevel, EndTime, and Summary
// are real *optional* members on types.Investigation/types.InvestigationSummary
// that AWS only populates once analysis actually runs (Cloud/Metadata) or
// completes/fails (Confidence/EndTime/Error/Risk/RiskLevel/Summary). Since
// that never happens here, they stay at their zero value below and every
// wire response correctly omits them -- see handler_investigations.go --
// rather than fabricating severity scores, threat indicators, related
// findings, or anomaly counts this emulator has no way to compute for real.
type Investigation struct {
	StartTime       time.Time `json:"startTime"`
	EndTime         time.Time `json:"endTime"`
	InvestigationID string    `json:"investigationId"`
	Status          string    `json:"status"`
	TriggerPrompt   string    `json:"triggerPrompt"`
	TriggeredBy     string    `json:"triggeredBy"`
	Confidence      string    `json:"confidence,omitempty"`
	RiskLevel       string    `json:"riskLevel,omitempty"`
	Risk            string    `json:"risk,omitempty"`
	ErrorDetail     string    `json:"error,omitempty"`
	Summary         string    `json:"summary,omitempty"`
	DetectorID      string    `json:"-"`
}
