package inspector2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// --- sentinel errors ---

var (
	// ErrMemberNotFound is returned when a member account is not found.
	ErrMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrMemberAlreadyExists is returned when a member already exists.
	ErrMemberAlreadyExists = awserr.New(errConflict, awserr.ErrConflict)
	// ErrDelegatedAdminNotFound is returned when a delegated admin is not found.
	ErrDelegatedAdminNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDelegatedAdminAlreadyExists is returned on duplicate enable.
	ErrDelegatedAdminAlreadyExists = awserr.New(errConflict, awserr.ErrConflict)
	// ErrCisScanConfigNotFound is returned when a CIS scan config is missing.
	ErrCisScanConfigNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCodeSecurityIntegrationNotFound is returned when a code security integration is missing.
	ErrCodeSecurityIntegrationNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCodeSecurityScanConfigNotFound is returned when a code security scan config is missing.
	ErrCodeSecurityScanConfigNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrReportNotFound is returned when a findings report is missing.
	ErrReportNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrSbomExportNotFound is returned when an SBOM export is missing.
	ErrSbomExportNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCisSessionNotFound is returned when a CIS session is missing.
	ErrCisSessionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)

// CIS scan and check status values (AWS Inspector2 CIS API).
const (
	cisScanStatusCompleted = "COMPLETED"

	cisCheckStatusPassed = "PASSED"
	cisCheckStatusFailed = "FAILED"

	cisLevel1        = "LEVEL_1"
	cisPlatform      = "AMAZON_LINUX_2"
	cisReportSuccess = "SUCCEEDED"

	keyScanArn  = "scanArn"
	keyPlatform = "platform"
)

// --- domain types ---

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

// --- InMemoryBackend extension: appendix A fields ---
// Fields are added to InMemoryBackend by extending the Snapshot/Restore cycle
// and the Reset method via init-time patching isn't possible in Go;
// instead we embed the appendix state in a separate struct and store a pointer.

// appendixAState holds all appendix A data.
type appendixAState struct {
	CodeSecurityIntegrations map[string]*CodeSecurityIntegration                    `json:"codeSecurityIntegrations"`
	CisScanConfigs           map[string]*CisScanConfiguration                       `json:"cisScanConfigs"`
	CisScans                 map[string]*CisScan                                    `json:"cisScans"`
	SbomExports              map[string]*SbomExport                                 `json:"sbomExports"`
	FindingsReports          map[string]*FindingsReport                             `json:"findingsReports"`
	CodeSecurityScans        map[string]map[string]any                              `json:"codeSecurityScans"`
	MemberEc2Status          map[string]*MemberEc2DeepInspectionStatus              `json:"memberEc2Status"`
	DelegatedAdmins          map[string]*DelegatedAdminAccount                      `json:"delegatedAdmins"`
	CodeSecurityScanConfigs  map[string]*CodeSecurityScanConfiguration              `json:"codeSecurityScanConfigs"`
	EncryptionKeys           map[string]*EncryptionKey                              `json:"encryptionKeys"`
	Members                  map[string]*Member                                     `json:"members"`
	CisSessions              map[string]*CisSession                                 `json:"cisSessions"`
	ScanConfigAssociations   map[string][]*CodeSecurityScanConfigurationAssociation `json:"scanConfigAssociations"`
	Ec2DeepConfig            Ec2DeepInspectionConfig                                `json:"ec2DeepConfig"`
	OrgEc2Config             OrgEc2DeepInspectionConfig                             `json:"orgEc2Config"`
	OrgConfig                OrgConfiguration                                       `json:"orgConfig"`
}

func newAppendixAState() *appendixAState {
	return &appendixAState{
		Members:                  make(map[string]*Member),
		DelegatedAdmins:          make(map[string]*DelegatedAdminAccount),
		MemberEc2Status:          make(map[string]*MemberEc2DeepInspectionStatus),
		EncryptionKeys:           make(map[string]*EncryptionKey),
		CisScanConfigs:           make(map[string]*CisScanConfiguration),
		CisScans:                 make(map[string]*CisScan),
		CisSessions:              make(map[string]*CisSession),
		CodeSecurityIntegrations: make(map[string]*CodeSecurityIntegration),
		CodeSecurityScanConfigs:  make(map[string]*CodeSecurityScanConfiguration),
		ScanConfigAssociations:   make(map[string][]*CodeSecurityScanConfigurationAssociation),
		CodeSecurityScans:        make(map[string]map[string]any),
		FindingsReports:          make(map[string]*FindingsReport),
		SbomExports:              make(map[string]*SbomExport),
		Ec2DeepConfig: Ec2DeepInspectionConfig{
			Status:       statusDisabled,
			PackagePaths: []string{},
		},
	}
}

// --- ARN builders ---

func (b *InMemoryBackend) buildCisScanConfigARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "cis-scan-configuration/"+uuid.New().String())
}

func (b *InMemoryBackend) buildCisScanARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "cis-scan/"+uuid.New().String())
}

func (b *InMemoryBackend) buildCodeSecurityIntegrationARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "integration/code-security/"+uuid.New().String())
}

func (b *InMemoryBackend) buildCodeSecurityScanConfigARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "code-security-scan-configuration/"+uuid.New().String())
}

func (b *InMemoryBackend) buildReportARN() string {
	return uuid.New().String()
}

// --- Member operations ---

// AssociateMember adds a member account.
func (b *InMemoryBackend) AssociateMember(accountID string) error {
	b.mu.Lock("AssociateMember")
	defer b.mu.Unlock()

	if accountID == "" {
		return fmt.Errorf("%w: accountId is required", ErrValidation)
	}

	b.ax.Members[accountID] = &Member{
		AccountID:               accountID,
		DelegatedAdminAccountID: b.accountID,
		RelationshipStatus:      "ENABLED", //nolint:goconst // existing issue.
		UpdatedAt:               time.Now().UTC(),
	}

	return nil
}

// DisassociateMember removes a member account.
func (b *InMemoryBackend) DisassociateMember(accountID string) error {
	b.mu.Lock("DisassociateMember")
	defer b.mu.Unlock()

	if _, ok := b.ax.Members[accountID]; !ok {
		return ErrMemberNotFound
	}

	delete(b.ax.Members, accountID)

	return nil
}

// GetMember returns a member account.
func (b *InMemoryBackend) GetMember(accountID string) (*Member, error) {
	b.mu.RLock("GetMember")
	defer b.mu.RUnlock()

	m, ok := b.ax.Members[accountID]
	if !ok {
		return nil, ErrMemberNotFound
	}

	cp := *m

	return &cp, nil
}

// ListMembers returns all member accounts, optionally only associated ones.
func (b *InMemoryBackend) ListMembers(onlyAssociated bool) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	ids := collections.SortedKeys(b.ax.Members)

	result := make([]*Member, 0, len(ids))

	for _, id := range ids {
		m := b.ax.Members[id]
		if onlyAssociated && m.RelationshipStatus != "ENABLED" {
			continue
		}

		cp := *m
		result = append(result, &cp)
	}

	return result, nil
}

// --- Delegated admin account operations ---

// EnableDelegatedAdminAccount enables a delegated admin account.
func (b *InMemoryBackend) EnableDelegatedAdminAccount(accountID string) error {
	b.mu.Lock("EnableDelegatedAdminAccount")
	defer b.mu.Unlock()

	if accountID == "" {
		return fmt.Errorf("%w: accountId is required", ErrValidation)
	}

	if existing, ok := b.ax.DelegatedAdmins[accountID]; ok && existing.Status == "ENABLED" {
		return ErrDelegatedAdminAlreadyExists
	}

	b.ax.DelegatedAdmins[accountID] = &DelegatedAdminAccount{
		AccountID: accountID,
		Status:    "ENABLED",
	}

	return nil
}

// DisableDelegatedAdminAccount disables a delegated admin account.
func (b *InMemoryBackend) DisableDelegatedAdminAccount(accountID string) error {
	b.mu.Lock("DisableDelegatedAdminAccount")
	defer b.mu.Unlock()

	if _, ok := b.ax.DelegatedAdmins[accountID]; !ok {
		return ErrDelegatedAdminNotFound
	}

	delete(b.ax.DelegatedAdmins, accountID)

	return nil
}

// GetDelegatedAdminAccount returns the delegated admin account.
func (b *InMemoryBackend) GetDelegatedAdminAccount() (*DelegatedAdminAccount, error) {
	b.mu.RLock("GetDelegatedAdminAccount")
	defer b.mu.RUnlock()

	for _, d := range b.ax.DelegatedAdmins {
		cp := *d

		return &cp, nil
	}

	return nil, ErrDelegatedAdminNotFound
}

// ListDelegatedAdminAccounts returns all delegated admin accounts.
func (b *InMemoryBackend) ListDelegatedAdminAccounts() ([]*DelegatedAdminAccount, error) {
	b.mu.RLock("ListDelegatedAdminAccounts")
	defer b.mu.RUnlock()

	ids := collections.SortedKeys(b.ax.DelegatedAdmins)

	result := make([]*DelegatedAdminAccount, 0, len(ids))

	for _, id := range ids {
		cp := *b.ax.DelegatedAdmins[id]
		result = append(result, &cp)
	}

	return result, nil
}

// --- Organization configuration ---

// DescribeOrganizationConfiguration returns org-level Inspector2 configuration.
func (b *InMemoryBackend) DescribeOrganizationConfiguration() OrgConfiguration {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	return b.ax.OrgConfig
}

// UpdateOrganizationConfiguration updates org-level Inspector2 configuration.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(cfg OrgConfiguration) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	b.ax.OrgConfig = cfg

	return nil
}

// --- EC2 Deep Inspection ---

// GetEc2DeepInspectionConfiguration returns EC2 deep inspection config.
func (b *InMemoryBackend) GetEc2DeepInspectionConfiguration() Ec2DeepInspectionConfig {
	b.mu.RLock("GetEc2DeepInspectionConfiguration")
	defer b.mu.RUnlock()

	cp := b.ax.Ec2DeepConfig
	cp.PackagePaths = append([]string(nil), b.ax.Ec2DeepConfig.PackagePaths...)

	return cp
}

// UpdateEc2DeepInspectionConfiguration updates EC2 deep inspection config.
func (b *InMemoryBackend) UpdateEc2DeepInspectionConfiguration(paths []string) error {
	b.mu.Lock("UpdateEc2DeepInspectionConfiguration")
	defer b.mu.Unlock()

	b.ax.Ec2DeepConfig.PackagePaths = append([]string(nil), paths...)
	b.ax.Ec2DeepConfig.Status = statusEnabled

	return nil
}

// UpdateOrgEc2DeepInspectionConfiguration updates org-level EC2 deep inspection config.
func (b *InMemoryBackend) UpdateOrgEc2DeepInspectionConfiguration(paths []string) error {
	b.mu.Lock("UpdateOrgEc2DeepInspectionConfiguration")
	defer b.mu.Unlock()

	b.ax.OrgEc2Config.CustomPaths = append([]string(nil), paths...)

	return nil
}

// BatchGetMemberEc2DeepInspectionStatus returns EC2 deep inspection status for member accounts.
func (b *InMemoryBackend) BatchGetMemberEc2DeepInspectionStatus(accountIDs []string) []*MemberEc2DeepInspectionStatus {
	b.mu.RLock("BatchGetMemberEc2DeepInspectionStatus")
	defer b.mu.RUnlock()

	result := make([]*MemberEc2DeepInspectionStatus, 0, len(accountIDs))

	for _, id := range accountIDs {
		if s, ok := b.ax.MemberEc2Status[id]; ok {
			cp := *s
			result = append(result, &cp)
		} else {
			result = append(result, &MemberEc2DeepInspectionStatus{
				AccountID:    id,
				PackagePaths: []string{},
				Status:       statusDisabled,
			})
		}
	}

	return result
}

// BatchUpdateMemberEc2DeepInspectionStatus updates EC2 deep inspection status for member accounts.
func (b *InMemoryBackend) BatchUpdateMemberEc2DeepInspectionStatus(
	updates []*MemberEc2DeepInspectionStatus,
) []*MemberEc2DeepInspectionStatus {
	b.mu.Lock("BatchUpdateMemberEc2DeepInspectionStatus")
	defer b.mu.Unlock()

	result := make([]*MemberEc2DeepInspectionStatus, 0, len(updates))

	for _, u := range updates {
		paths := append([]string(nil), u.PackagePaths...)
		s := &MemberEc2DeepInspectionStatus{
			AccountID:    u.AccountID,
			PackagePaths: paths,
			Status:       statusEnabled,
		}
		b.ax.MemberEc2Status[u.AccountID] = s
		cp := *s
		result = append(result, &cp)
	}

	return result
}

// --- Encryption Key ---

// GetEncryptionKey returns encryption key info for the given resource type and scan type.
func (b *InMemoryBackend) GetEncryptionKey(resourceType, scanType string) (*EncryptionKey, error) {
	b.mu.RLock("GetEncryptionKey")
	defer b.mu.RUnlock()

	key := resourceType + "/" + scanType
	if k, ok := b.ax.EncryptionKeys[key]; ok {
		cp := *k

		return &cp, nil
	}

	// Return default AWS-managed key info.
	return &EncryptionKey{
		KmsKeyID:     "AWS_OWNED_KEY",
		ResourceType: resourceType,
		ScanType:     scanType,
	}, nil
}

// ResetEncryptionKey resets the encryption key to the AWS-managed default.
func (b *InMemoryBackend) ResetEncryptionKey(resourceType, scanType string) error {
	b.mu.Lock("ResetEncryptionKey")
	defer b.mu.Unlock()

	key := resourceType + "/" + scanType
	delete(b.ax.EncryptionKeys, key)

	return nil
}

// UpdateEncryptionKey sets a customer-managed KMS key for the given resource and scan type.
func (b *InMemoryBackend) UpdateEncryptionKey(kmsKeyID, resourceType, scanType string) error {
	b.mu.Lock("UpdateEncryptionKey")
	defer b.mu.Unlock()

	if kmsKeyID == "" || resourceType == "" || scanType == "" {
		return fmt.Errorf("%w: kmsKeyId, resourceType, and scanType are required", ErrValidation)
	}

	key := resourceType + "/" + scanType
	b.ax.EncryptionKeys[key] = &EncryptionKey{
		KmsKeyID:     kmsKeyID,
		ResourceType: resourceType,
		ScanType:     scanType,
	}

	return nil
}

// --- CIS Scan Configuration ---

// CreateCisScanConfiguration creates a new CIS scan configuration.
func (b *InMemoryBackend) CreateCisScanConfiguration(
	name string,
	schedule map[string]any,
	targets map[string]any,
	tags map[string]string,
) (*CisScanConfiguration, error) {
	b.mu.Lock("CreateCisScanConfiguration")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: scanName is required", ErrValidation)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	cfgARN := b.buildCisScanConfigARN()
	cfg := &CisScanConfiguration{
		Arn:        cfgARN,
		Name:       name,
		OwnedBy:    b.accountID,
		Tags:       tags,
		ScheduleV2: schedule,
		Targets:    targets,
	}
	b.ax.CisScanConfigs[cfgARN] = cfg

	// Materialize a completed scan run for the config so the result/report/
	// aggregation operations reflect real configuration state instead of canned
	// data. Real AWS runs scans asynchronously on the configured schedule; we
	// model the steady-state outcome at creation time.
	scan := b.buildCisScanForConfig(cfg)
	b.ax.CisScans[scan.ScanArn] = scan

	return cfg, nil
}

// cisCheckCatalog is a small representative slice of the CIS benchmark checks
// that a scan evaluates per target. Results are derived deterministically from
// these so report/detail/aggregation operations stay internally consistent.
func cisCheckCatalog() []CisCheckResult {
	return []CisCheckResult{
		{
			CheckID:    "1.1.1",
			CheckDescr: "Ensure mounting of cramfs filesystems is disabled",
			Level:      cisLevel1,
			Platform:   cisPlatform,
			Status:     cisCheckStatusPassed,
		},
		{
			CheckID:    "1.3.1",
			CheckDescr: "Ensure AIDE is installed",
			Level:      cisLevel1,
			Platform:   cisPlatform,
			Status:     cisCheckStatusFailed,
		},
		{
			CheckID:    "5.2.1",
			CheckDescr: "Ensure permissions on /etc/ssh/sshd_config are configured",
			Level:      cisLevel1,
			Platform:   cisPlatform,
			Status:     cisCheckStatusPassed,
		},
	}
}

// cisTargetAccounts extracts the account IDs a scan targets, defaulting to the
// backend account when the configuration does not name any.
func (b *InMemoryBackend) cisTargetAccounts(targets map[string]any) []string {
	var accounts []string

	if raw, ok := targets["accountIds"].([]any); ok {
		for _, v := range raw {
			if id, isStr := v.(string); isStr && id != "" {
				accounts = append(accounts, id)
			}
		}
	}

	if len(accounts) == 0 {
		accounts = []string{b.accountID}
	}

	return accounts
}

// buildCisScanForConfig constructs a completed scan, including per-target check
// results, from a scan configuration. Caller must hold the write lock.
func (b *InMemoryBackend) buildCisScanForConfig(cfg *CisScanConfiguration) *CisScan {
	now := time.Now().UTC()
	accounts := b.cisTargetAccounts(cfg.Targets)
	catalog := cisCheckCatalog()

	results := make([]*CisCheckResult, 0, len(accounts)*len(catalog))
	failed := 0

	for _, acct := range accounts {
		targetID := "i-" + uuid.New().String()[:17]

		for i := range catalog {
			res := catalog[i]
			res.AccountID = acct
			res.TargetID = targetID

			if res.Status == cisCheckStatusFailed {
				failed++
				res.StatusReason = "remediation required"
			}

			rc := res
			results = append(results, &rc)
		}
	}

	return &CisScan{
		ScanArn:              b.buildCisScanARN(),
		ScanConfigurationArn: cfg.Arn,
		ScanName:             cfg.Name,
		Status:               cisScanStatusCompleted,
		SecurityLevel:        cisLevel1,
		ScheduledAt:          now,
		FinishedAt:           now,
		TargetAccountID:      accounts[0],
		TotalChecks:          len(results),
		FailedChecks:         failed,
		Results:              results,
	}
}

// DeleteCisScanConfiguration deletes a CIS scan configuration.
func (b *InMemoryBackend) DeleteCisScanConfiguration(configARN string) error {
	b.mu.Lock("DeleteCisScanConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.ax.CisScanConfigs[configARN]; !ok {
		return ErrCisScanConfigNotFound
	}

	delete(b.ax.CisScanConfigs, configARN)

	// Drop any scans materialized from this configuration so list/result
	// operations stop reporting them.
	for scanARN, s := range b.ax.CisScans {
		if s.ScanConfigurationArn == configARN {
			delete(b.ax.CisScans, scanARN)
		}
	}

	return nil
}

// UpdateCisScanConfiguration updates a CIS scan configuration.
func (b *InMemoryBackend) UpdateCisScanConfiguration(
	configARN string,
	name string,
	schedule map[string]any,
	targets map[string]any,
) (*CisScanConfiguration, error) {
	b.mu.Lock("UpdateCisScanConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.ax.CisScanConfigs[configARN]
	if !ok {
		return nil, ErrCisScanConfigNotFound
	}

	if name != "" {
		cfg.Name = name
	}

	if schedule != nil {
		cfg.ScheduleV2 = schedule
	}

	if targets != nil {
		cfg.Targets = targets
	}

	return cfg, nil
}

// ListCisScanConfigurations returns CIS scan configurations.
func (b *InMemoryBackend) ListCisScanConfigurations() ([]*CisScanConfiguration, error) {
	b.mu.RLock("ListCisScanConfigurations")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.ax.CisScanConfigs)

	result := make([]*CisScanConfiguration, 0, len(arns))

	for _, a := range arns {
		cp := *b.ax.CisScanConfigs[a]
		result = append(result, &cp)
	}

	return result, nil
}

// --- CIS Session ---

// StartCisSession starts a new CIS scan session.
func (b *InMemoryBackend) StartCisSession(scanJobID, sessionToken string) (*CisSession, error) {
	b.mu.Lock("StartCisSession")
	defer b.mu.Unlock()

	if scanJobID == "" {
		return nil, fmt.Errorf("%w: scanJobId is required", ErrValidation)
	}

	sess := &CisSession{
		ScanJobID:    scanJobID,
		SessionToken: sessionToken,
		Status:       "ACTIVE", //nolint:goconst // existing issue.
		StartedAt:    time.Now().UTC(),
	}
	b.ax.CisSessions[scanJobID] = sess

	return sess, nil
}

// StopCisSession stops a CIS scan session.
func (b *InMemoryBackend) StopCisSession(scanJobID string) error {
	b.mu.Lock("StopCisSession")
	defer b.mu.Unlock()

	sess, ok := b.ax.CisSessions[scanJobID]
	if !ok {
		return ErrCisSessionNotFound
	}

	sess.Status = "STOPPING"

	return nil
}

// SendCisSessionHealth acknowledges CIS session health.
func (b *InMemoryBackend) SendCisSessionHealth(_ string) error {
	return nil
}

// SendCisSessionTelemetry records CIS session telemetry (no-op in memory).
func (b *InMemoryBackend) SendCisSessionTelemetry(_ string, _ map[string]any) error {
	return nil
}

// findCisScan returns the stored scan for either its scan ARN or the ARN of the
// configuration that produced it. Caller must hold at least an RLock.
func (b *InMemoryBackend) findCisScan(scanOrConfigARN string) *CisScan {
	if s, ok := b.ax.CisScans[scanOrConfigARN]; ok {
		return s
	}

	for _, s := range b.ax.CisScans {
		if s.ScanConfigurationArn == scanOrConfigARN {
			return s
		}
	}

	return nil
}

// GetCisScanReport returns the report for a completed CIS scan. The status and
// counts are drawn from the stored scan produced by its configuration. For an
// unrecognized scan ARN it returns a benign SUCCEEDED report with no findings,
// matching AWS, which does not error on missing report targets.
func (b *InMemoryBackend) GetCisScanReport(scanArn string) (map[string]any, error) {
	b.mu.RLock("GetCisScanReport")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return map[string]any{
			keyStatus: cisReportSuccess,
			"url":     "",
		}, nil
	}

	return map[string]any{
		keyStatus:      cisReportSuccess,
		"url":          "",
		keyScanArn:     scan.ScanArn,
		"totalChecks":  scan.TotalChecks,
		"failedChecks": scan.FailedChecks,
	}, nil
}

// GetCisScanResultDetails returns the per-check results for a CIS scan. Results
// reflect the scan generated for the configuration; an unknown scan ARN yields
// an empty result set rather than an error (AWS behavior for absent scans).
func (b *InMemoryBackend) GetCisScanResultDetails(scanArn string) (map[string]any, error) {
	b.mu.RLock("GetCisScanResultDetails")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return map[string]any{"checkResults": []any{}}, nil
	}

	checkResults := make([]map[string]any, 0, len(scan.Results))
	for _, r := range scan.Results {
		entry := map[string]any{
			keyScanArn:         scan.ScanArn,
			"checkId":          r.CheckID,
			"checkDescription": r.CheckDescr,
			"level":            r.Level,
			keyPlatform:        r.Platform,
			keyStatus:          r.Status,
			keyAccountID:       r.AccountID,
			"targetResourceId": r.TargetID,
		}
		if r.StatusReason != "" {
			entry["statusReason"] = r.StatusReason
		}

		checkResults = append(checkResults, entry)
	}

	return map[string]any{"checkResults": checkResults}, nil
}

// ListCisScans returns all completed CIS scans, sorted by scan ARN for stable
// pagination-free ordering. Each entry summarizes the scan produced from a
// configuration.
func (b *InMemoryBackend) ListCisScans() ([]map[string]any, error) {
	b.mu.RLock("ListCisScans")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.ax.CisScans)

	result := make([]map[string]any, 0, len(arns))

	for _, a := range arns {
		s := b.ax.CisScans[a]
		result = append(result, map[string]any{
			keyScanArn:              s.ScanArn,
			keyScanConfigurationArn: s.ScanConfigurationArn,
			"scanName":              s.ScanName,
			keyStatus:               s.Status,
			"securityLevel":         s.SecurityLevel,
			"scheduledBy":           s.ScheduledAt.Format(time.RFC3339),
			"failedChecks":          s.FailedChecks,
			"totalChecks":           s.TotalChecks,
			"targetAccountId":       s.TargetAccountID,
		})
	}

	return result, nil
}

// ListCisScanResultsAggregatedByChecks groups a scan's results by check ID,
// reporting passed/failed/skipped counts per check. An unknown scan ARN yields
// an empty aggregation list.
func (b *InMemoryBackend) ListCisScanResultsAggregatedByChecks(scanArn string) ([]map[string]any, error) {
	b.mu.RLock("ListCisScanResultsAggregatedByChecks")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return []map[string]any{}, nil
	}

	type checkAgg struct {
		descr        string
		level        string
		platform     string
		passed       int64
		failed       int64
		skipped      int64
		firstSeenIdx int
	}

	aggs := make(map[string]*checkAgg)
	order := make([]string, 0)

	for i, r := range scan.Results {
		a, ok := aggs[r.CheckID]
		if !ok {
			a = &checkAgg{descr: r.CheckDescr, level: r.Level, platform: r.Platform, firstSeenIdx: i}
			aggs[r.CheckID] = a
			order = append(order, r.CheckID)
		}

		switch r.Status {
		case cisCheckStatusPassed:
			a.passed++
		case cisCheckStatusFailed:
			a.failed++
		default:
			a.skipped++
		}
	}

	sort.Strings(order)

	result := make([]map[string]any, 0, len(order))
	for _, id := range order {
		a := aggs[id]
		result = append(result, map[string]any{
			keyScanArn:         scan.ScanArn,
			"checkId":          id,
			"checkDescription": a.descr,
			"level":            a.level,
			keyPlatform:        a.platform,
			"statusCounts": map[string]any{
				"passed":  a.passed,
				"failed":  a.failed,
				"skipped": a.skipped,
			},
		})
	}

	return result, nil
}

// ListCisScanResultsAggregatedByTargetResource groups a scan's results by target
// resource, reporting passed/failed/skipped counts per resource. An unknown scan
// ARN yields an empty aggregation list.
func (b *InMemoryBackend) ListCisScanResultsAggregatedByTargetResource(
	scanArn string,
) ([]map[string]any, error) {
	b.mu.RLock("ListCisScanResultsAggregatedByTargetResource")
	defer b.mu.RUnlock()

	scan := b.findCisScan(scanArn)
	if scan == nil {
		return []map[string]any{}, nil
	}

	type targetAgg struct {
		accountID string
		platform  string
		passed    int64
		failed    int64
		skipped   int64
	}

	aggs := make(map[string]*targetAgg)
	order := make([]string, 0)

	for _, r := range scan.Results {
		a, ok := aggs[r.TargetID]
		if !ok {
			a = &targetAgg{accountID: r.AccountID, platform: r.Platform}
			aggs[r.TargetID] = a
			order = append(order, r.TargetID)
		}

		switch r.Status {
		case cisCheckStatusPassed:
			a.passed++
		case cisCheckStatusFailed:
			a.failed++
		default:
			a.skipped++
		}
	}

	sort.Strings(order)

	result := make([]map[string]any, 0, len(order))
	for _, tid := range order {
		a := aggs[tid]
		result = append(result, map[string]any{
			keyScanArn:         scan.ScanArn,
			"targetResourceId": tid,
			keyAccountID:       a.accountID,
			keyPlatform:        a.platform,
			"statusCounts": map[string]any{
				"passed":  a.passed,
				"failed":  a.failed,
				"skipped": a.skipped,
			},
		})
	}

	return result, nil
}

// --- Code Security Integration ---

// CreateCodeSecurityIntegration creates a new code security integration.
func (b *InMemoryBackend) CreateCodeSecurityIntegration(
	name, integType string,
	tags map[string]string,
	details map[string]any,
) (*CodeSecurityIntegration, error) {
	b.mu.Lock("CreateCodeSecurityIntegration")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	integARN := b.buildCodeSecurityIntegrationARN()
	now := time.Now().UTC()
	integ := &CodeSecurityIntegration{
		IntegrationArn: integARN,
		Name:           name,
		Type:           integType,
		Status:         "ACTIVE",
		Tags:           tags,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_ = details
	b.ax.CodeSecurityIntegrations[integARN] = integ

	return integ, nil
}

// DeleteCodeSecurityIntegration deletes a code security integration.
func (b *InMemoryBackend) DeleteCodeSecurityIntegration(integrationARN string) error {
	b.mu.Lock("DeleteCodeSecurityIntegration")
	defer b.mu.Unlock()

	if _, ok := b.ax.CodeSecurityIntegrations[integrationARN]; !ok {
		return ErrCodeSecurityIntegrationNotFound
	}

	delete(b.ax.CodeSecurityIntegrations, integrationARN)

	return nil
}

// GetCodeSecurityIntegration returns a code security integration.
func (b *InMemoryBackend) GetCodeSecurityIntegration(integrationARN string) (*CodeSecurityIntegration, error) {
	b.mu.RLock("GetCodeSecurityIntegration")
	defer b.mu.RUnlock()

	integ, ok := b.ax.CodeSecurityIntegrations[integrationARN]
	if !ok {
		return nil, ErrCodeSecurityIntegrationNotFound
	}

	cp := *integ

	return &cp, nil
}

// UpdateCodeSecurityIntegration updates a code security integration.
func (b *InMemoryBackend) UpdateCodeSecurityIntegration(
	integrationARN string,
	details map[string]any,
) (*CodeSecurityIntegration, error) {
	b.mu.Lock("UpdateCodeSecurityIntegration")
	defer b.mu.Unlock()

	integ, ok := b.ax.CodeSecurityIntegrations[integrationARN]
	if !ok {
		return nil, ErrCodeSecurityIntegrationNotFound
	}

	integ.UpdatedAt = time.Now().UTC()
	_ = details

	cp := *integ

	return &cp, nil
}

// ListCodeSecurityIntegrations returns all code security integrations.
func (b *InMemoryBackend) ListCodeSecurityIntegrations() ([]*CodeSecurityIntegration, error) {
	b.mu.RLock("ListCodeSecurityIntegrations")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.ax.CodeSecurityIntegrations)

	result := make([]*CodeSecurityIntegration, 0, len(arns))

	for _, a := range arns {
		cp := *b.ax.CodeSecurityIntegrations[a]
		result = append(result, &cp)
	}

	return result, nil
}

// --- Code Security Scan Configuration ---

// CreateCodeSecurityScanConfiguration creates a code security scan configuration.
func (b *InMemoryBackend) CreateCodeSecurityScanConfiguration(
	name string,
	scopeSettings map[string]any,
	periodicConfig map[string]any,
	tags map[string]string,
) (*CodeSecurityScanConfiguration, error) {
	b.mu.Lock("CreateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	cfgARN := b.buildCodeSecurityScanConfigARN()
	now := time.Now().UTC()
	cfg := &CodeSecurityScanConfiguration{
		Arn:                cfgARN,
		Name:               name,
		ScopeSettings:      scopeSettings,
		PeriodicScanConfig: periodicConfig,
		Status:             "ACTIVE",
		Tags:               tags,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	b.ax.CodeSecurityScanConfigs[cfgARN] = cfg

	return cfg, nil
}

// DeleteCodeSecurityScanConfiguration deletes a code security scan configuration.
func (b *InMemoryBackend) DeleteCodeSecurityScanConfiguration(scanConfigARN string) error {
	b.mu.Lock("DeleteCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.ax.CodeSecurityScanConfigs[scanConfigARN]; !ok {
		return ErrCodeSecurityScanConfigNotFound
	}

	delete(b.ax.CodeSecurityScanConfigs, scanConfigARN)
	delete(b.ax.ScanConfigAssociations, scanConfigARN)

	return nil
}

// GetCodeSecurityScanConfiguration returns a code security scan configuration.
func (b *InMemoryBackend) GetCodeSecurityScanConfiguration(
	scanConfigARN string,
) (*CodeSecurityScanConfiguration, error) {
	b.mu.RLock("GetCodeSecurityScanConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.ax.CodeSecurityScanConfigs[scanConfigARN]
	if !ok {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	cp := *cfg

	return &cp, nil
}

// UpdateCodeSecurityScanConfiguration updates a code security scan configuration.
func (b *InMemoryBackend) UpdateCodeSecurityScanConfiguration(
	scanConfigARN string,
	scopeSettings map[string]any,
	periodicConfig map[string]any,
) (*CodeSecurityScanConfiguration, error) {
	b.mu.Lock("UpdateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.ax.CodeSecurityScanConfigs[scanConfigARN]
	if !ok {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	if scopeSettings != nil {
		cfg.ScopeSettings = scopeSettings
	}

	if periodicConfig != nil {
		cfg.PeriodicScanConfig = periodicConfig
	}

	cfg.UpdatedAt = time.Now().UTC()
	cp := *cfg

	return &cp, nil
}

// ListCodeSecurityScanConfigurations returns all code security scan configurations.
func (b *InMemoryBackend) ListCodeSecurityScanConfigurations() ([]*CodeSecurityScanConfiguration, error) {
	b.mu.RLock("ListCodeSecurityScanConfigurations")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.ax.CodeSecurityScanConfigs)

	result := make([]*CodeSecurityScanConfiguration, 0, len(arns))

	for _, a := range arns {
		cp := *b.ax.CodeSecurityScanConfigs[a]
		result = append(result, &cp)
	}

	return result, nil
}

// BatchAssociateCodeSecurityScanConfiguration associates scan configs with resources.
func (b *InMemoryBackend) BatchAssociateCodeSecurityScanConfiguration(
	scanConfigARN string,
	resources []string,
) ([]map[string]any, error) {
	b.mu.Lock("BatchAssociateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.ax.CodeSecurityScanConfigs[scanConfigARN]; !ok {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	for _, resource := range resources {
		b.ax.ScanConfigAssociations[scanConfigARN] = append(
			b.ax.ScanConfigAssociations[scanConfigARN],
			&CodeSecurityScanConfigurationAssociation{
				ScanConfigurationArn: scanConfigARN,
				Resource:             resource,
				Status:               "ASSOCIATED",
			},
		)
	}

	return []map[string]any{}, nil
}

// BatchDisassociateCodeSecurityScanConfiguration removes scan config associations.
func (b *InMemoryBackend) BatchDisassociateCodeSecurityScanConfiguration(
	scanConfigARN string,
	resources []string,
) ([]map[string]any, error) {
	b.mu.Lock("BatchDisassociateCodeSecurityScanConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.ax.CodeSecurityScanConfigs[scanConfigARN]; !ok {
		return nil, ErrCodeSecurityScanConfigNotFound
	}

	removeSet := make(map[string]bool, len(resources))
	for _, r := range resources {
		removeSet[r] = true
	}

	existing := b.ax.ScanConfigAssociations[scanConfigARN]
	filtered := existing[:0]

	for _, assoc := range existing {
		if !removeSet[assoc.Resource] {
			filtered = append(filtered, assoc)
		}
	}

	b.ax.ScanConfigAssociations[scanConfigARN] = filtered

	return []map[string]any{}, nil
}

// ListCodeSecurityScanConfigurationAssociations returns associations for a scan config.
func (b *InMemoryBackend) ListCodeSecurityScanConfigurationAssociations(
	scanConfigARN string,
) ([]*CodeSecurityScanConfigurationAssociation, error) {
	b.mu.RLock("ListCodeSecurityScanConfigurationAssociations")
	defer b.mu.RUnlock()

	result := make([]*CodeSecurityScanConfigurationAssociation, 0, len(b.ax.ScanConfigAssociations[scanConfigARN]))

	for _, assoc := range b.ax.ScanConfigAssociations[scanConfigARN] {
		cp := *assoc
		result = append(result, &cp)
	}

	return result, nil
}

// StartCodeSecurityScan starts a code security scan.
func (b *InMemoryBackend) StartCodeSecurityScan(resourceID string) (map[string]any, error) {
	b.mu.Lock("StartCodeSecurityScan")
	defer b.mu.Unlock()

	scanID := uuid.New().String()
	scan := map[string]any{
		"scanId":     scanID,
		"resourceId": resourceID,
		keyStatus:    "IN_PROGRESS",
	}
	b.ax.CodeSecurityScans[scanID] = scan

	return map[string]any{"scanId": scanID}, nil
}

// GetCodeSecurityScan returns status of a code security scan.
func (b *InMemoryBackend) GetCodeSecurityScan(scanID string) (map[string]any, error) {
	b.mu.RLock("GetCodeSecurityScan")
	defer b.mu.RUnlock()

	scan, ok := b.ax.CodeSecurityScans[scanID]
	if !ok {
		return nil, fmt.Errorf("%w: scanId %q not found", ErrReportNotFound, scanID)
	}

	return scan, nil
}

// --- Findings Report ---

// CreateFindingsReport creates an async findings report.
func (b *InMemoryBackend) CreateFindingsReport(destination map[string]any) (*FindingsReport, error) {
	b.mu.Lock("CreateFindingsReport")
	defer b.mu.Unlock()

	reportID := b.buildReportARN()
	report := &FindingsReport{
		ReportID:    reportID,
		Status:      "SUCCEEDED",
		Destination: destination,
		CreatedAt:   time.Now().UTC(),
	}
	b.ax.FindingsReports[reportID] = report

	return report, nil
}

// CancelFindingsReport cancels a findings report.
func (b *InMemoryBackend) CancelFindingsReport(reportID string) error {
	b.mu.Lock("CancelFindingsReport")
	defer b.mu.Unlock()

	if _, ok := b.ax.FindingsReports[reportID]; !ok {
		return ErrReportNotFound
	}

	b.ax.FindingsReports[reportID].Status = "CANCELLED"

	return nil
}

// GetFindingsReportStatus returns the status of a findings report.
func (b *InMemoryBackend) GetFindingsReportStatus(reportID string) (*FindingsReport, error) {
	b.mu.RLock("GetFindingsReportStatus")
	defer b.mu.RUnlock()

	if reportID == "" {
		// Return the most recent report if no ID given.
		for _, r := range b.ax.FindingsReports {
			cp := *r

			return &cp, nil
		}

		return nil, ErrReportNotFound
	}

	r, ok := b.ax.FindingsReports[reportID]
	if !ok {
		return nil, ErrReportNotFound
	}

	cp := *r

	return &cp, nil
}

// --- SBOM Export ---

// CreateSbomExport creates an async SBOM export.
func (b *InMemoryBackend) CreateSbomExport(destination map[string]any) (*SbomExport, error) {
	b.mu.Lock("CreateSbomExport")
	defer b.mu.Unlock()

	reportID := b.buildReportARN()
	export := &SbomExport{
		ReportID:    reportID,
		Status:      "SUCCEEDED",
		Destination: destination,
		CreatedAt:   time.Now().UTC(),
	}
	b.ax.SbomExports[reportID] = export

	return export, nil
}

// CancelSbomExport cancels an SBOM export.
func (b *InMemoryBackend) CancelSbomExport(reportID string) error {
	b.mu.Lock("CancelSbomExport")
	defer b.mu.Unlock()

	if _, ok := b.ax.SbomExports[reportID]; !ok {
		return ErrSbomExportNotFound
	}

	b.ax.SbomExports[reportID].Status = "CANCELLED"

	return nil
}

// GetSbomExport returns the status of an SBOM export.
func (b *InMemoryBackend) GetSbomExport(reportID string) (*SbomExport, error) {
	b.mu.RLock("GetSbomExport")
	defer b.mu.RUnlock()

	e, ok := b.ax.SbomExports[reportID]
	if !ok {
		return nil, ErrSbomExportNotFound
	}

	cp := *e

	return &cp, nil
}

// --- Coverage ---

// ListCoverage returns a list of covered resources (stub — always empty).
func (b *InMemoryBackend) ListCoverage(_ map[string]any, _ int32, _ string) ([]*CoverageEntry, string, error) {
	return []*CoverageEntry{}, "", nil
}

// ListCoverageStatistics returns coverage statistics (stub).
func (b *InMemoryBackend) ListCoverageStatistics(_ map[string]any) (map[string]any, error) {
	return map[string]any{
		"countsByGroup": []any{},
		"totalCounts":   int64(0),
	}, nil
}

// --- Finding Aggregations ---

// ListFindingAggregations returns aggregated finding counts. When findings have
// been seeded it reports the real per-account severity breakdown; otherwise it
// returns an empty responses list (matching the prior empty-stub contract).
func (b *InMemoryBackend) ListFindingAggregations(aggregationType string, _ map[string]any) (map[string]any, error) {
	if aggregationType == "" {
		aggregationType = "ACCOUNT"
	}

	counts := b.FindingSeverityCounts()
	if len(counts) == 0 {
		return map[string]any{
			"aggregationType": aggregationType,
			"responses":       []any{},
		}, nil
	}

	var critical, high, medium, low, total int64
	for sev, n := range counts {
		total += n

		switch sev {
		case severityCritical:
			critical += n
		case severityHigh:
			high += n
		case severityMedium:
			medium += n
		case severityLow:
			low += n
		}
	}

	return map[string]any{
		"aggregationType": aggregationType,
		"responses": []map[string]any{
			{
				"accountAggregation": map[string]any{
					keyAccountID: b.accountID,
					"severityCounts": map[string]any{
						"all":      total,
						"critical": critical,
						"high":     high,
						"medium":   medium,
						"low":      low,
					},
				},
			},
		},
	}, nil
}

// --- Usage Totals ---

// ListUsageTotals returns usage totals (stub).
func (b *InMemoryBackend) ListUsageTotals(_ []string) ([]map[string]any, error) {
	return []map[string]any{
		{
			keyAccountID: b.accountID,
			keyStatus:    "ACTIVE",
			"usage":      []any{},
		},
	}, nil
}

// --- Account Permissions ---

// ListAccountPermissions returns account-level Inspector2 permissions (stub).
func (b *InMemoryBackend) ListAccountPermissions(_ string) ([]*AccountPermission, error) {
	return []*AccountPermission{}, nil
}

// --- Vulnerability Search ---

// SearchVulnerabilities returns matching vulnerabilities (stub).
func (b *InMemoryBackend) SearchVulnerabilities(_ map[string]any, _ string) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// --- Code Snippet / Finding Details ---

// BatchGetCodeSnippet returns code snippets for findings (stub).
func (b *InMemoryBackend) BatchGetCodeSnippet(_ []string) (map[string]any, error) {
	return map[string]any{
		"codeSnippetResults": []any{},
		"errors":             []any{},
	}, nil
}

// BatchGetFindingDetails returns finding details (stub).
func (b *InMemoryBackend) BatchGetFindingDetails(_ []map[string]any) (map[string]any, error) {
	return map[string]any{
		"findingDetails": []any{},
		"errors":         []any{},
	}, nil
}

// --- Free Trial Info ---

// BatchGetFreeTrialInfo returns free trial information for accounts.
func (b *InMemoryBackend) BatchGetFreeTrialInfo(accountIDs []string) (map[string]any, error) {
	accounts := make([]map[string]any, 0, len(accountIDs))

	for _, id := range accountIDs {
		accounts = append(accounts, map[string]any{
			"accountId": id,
			"freeTrialInfo": []map[string]any{
				{
					"end":     time.Now().UTC().AddDate(0, 0, 30).Format(time.RFC3339), //nolint:mnd // existing issue.
					"start":   time.Now().UTC().Format(time.RFC3339),
					keyStatus: "ACTIVE",
					"type":    "EC2",
				},
			},
		})
	}

	return map[string]any{
		"accounts":       accounts,
		"failedAccounts": []any{},
	}, nil
}

// --- Clusters for Image ---

// GetClustersForImage returns clusters associated with a container image (stub).
func (b *InMemoryBackend) GetClustersForImage(_ map[string]any) (map[string]any, error) {
	return map[string]any{
		"clusters": []any{},
	}, nil
}
