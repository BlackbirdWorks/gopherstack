package inspector2

import (
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Shared constants used across every operation family in this package.
const (
	inspector2Service = "inspector2"

	statusEnabled   = "ENABLED"
	statusDisabled  = "DISABLED"
	statusEnabling  = "ENABLING"
	statusDisabling = "DISABLING"

	// statusActive is the generic "ACTIVE" status value shared by CIS
	// sessions, code security integrations/scan configs, and usage/free-trial
	// reporting -- distinct domains that all happen to reuse the same AWS
	// status string.
	statusActive = "ACTIVE"

	// scanModeStatusSuccess is types.Ec2ScanModeStatusSuccess
	// (inspector2@v1.54.1 types/enums.go:1195) -- distinct from statusEnabled:
	// Ec2ScanModeState.ScanModeStatus has only SUCCESS/PENDING members, no
	// ENABLED.
	scanModeStatusSuccess = "SUCCESS"

	// ecrRescanDurationStatusSuccess is types.EcrRescanDurationStatusSuccess
	// (inspector2@v1.54.1 types/enums.go:1289-1303) -- distinct from
	// statusEnabled: EcrRescanDurationState.Status has only
	// SUCCESS/PENDING/FAILED members, no ENABLED.
	ecrRescanDurationStatusSuccess = "SUCCESS"
)

// InMemoryBackend is the in-memory implementation of Inspector2.
//
// Every map[string]*T resource collection is a *store.Table[T] registered on
// registry (see store_setup.go); tags, enabledTypes, and codeSecurityScans
// remain plain maps because their values are not *T (see store_setup.go's
// file doc comment for the full persistence audit).
type InMemoryBackend struct {
	appConfig                      any
	enabledTypes                   map[string]bool
	codeSecurityScanConfigs        *store.Table[CodeSecurityScanConfiguration]
	findings                       *store.Table[storedFinding]
	codeSecurityIntegrations       *store.Table[CodeSecurityIntegration]
	cisScanConfigs                 *store.Table[CisScanConfiguration]
	cisScans                       *store.Table[CisScan]
	cisScansByConfig               *store.Index[CisScan]
	sbomExports                    *store.Table[SbomExport]
	findingsReports                *store.Table[FindingsReport]
	vulnerabilities                *store.Table[Vulnerability]
	memberConfigs                  *store.Table[MemberConfiguration]
	members                        *store.Table[Member]
	registry                       *store.Registry
	encryptionKeys                 *store.Table[EncryptionKey]
	delegatedAdmins                *store.Table[DelegatedAdminAccount]
	cisSessions                    *store.Table[CisSession]
	filters                        *store.Table[Filter]
	memberEnabled                  map[string]map[string]bool
	scanConfigAssociations         *store.Table[CodeSecurityScanConfigurationAssociation]
	coverageEntries                *store.Table[CoverageEntry]
	memberEc2Status                *store.Table[MemberEc2DeepInspectionStatus]
	codeSnippets                   *store.Table[codeSnippet]
	connectors                     *store.Table[Connector]
	connectorsByAwsConfigArn       *store.Index[Connector]
	connectorScanConfigs           *store.Table[ConnectorScanConfiguration]
	tags                           map[string]map[string]string
	mu                             *lockmetrics.RWMutex
	codeSecurityScans              map[string]map[string]any
	scanConfigAssociationsByConfig *store.Index[CodeSecurityScanConfigurationAssociation]
	transitions                    map[string]time.Time
	clock                          func() time.Time
	config                         Configuration
	region                         string
	orgConfig                      OrgConfiguration
	accountID                      string
	ec2DeepConfig                  Ec2DeepInspectionConfig
	orgEc2Config                   OrgEc2DeepInspectionConfig
	lifecycleDelay                 time.Duration
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                lockmetrics.New("inspector2"),
		registry:          store.NewRegistry(),
		tags:              make(map[string]map[string]string),
		enabledTypes:      make(map[string]bool),
		transitions:       make(map[string]time.Time),
		memberEnabled:     make(map[string]map[string]bool),
		codeSecurityScans: make(map[string]map[string]any),
		config:            defaultConfiguration(),
		ec2DeepConfig:     defaultEc2DeepInspectionConfig(),
		accountID:         accountID,
		region:            region,
	}

	registerAllTables(b)

	return b
}

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// buildReportARN builds the identity used for findings reports and SBOM
// exports. Real AWS's reportId is a bare UUID rather than a full ARN, so this
// intentionally does not go through pkgs/arn.
func (b *InMemoryBackend) buildReportARN() string {
	return uuid.New().String()
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.resetRawState()
}
