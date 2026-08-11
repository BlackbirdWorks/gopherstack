package inspector2

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// Real ConnectorCloudProvider/EnablementStatus/ConnectorHealthStatus/
// ScopeState wire values (aws-sdk-go-v2/service/inspector2/types@v1.54.1
// enums.go), reproduced here as the subset this backend actually drives.
const (
	connectorProviderAzure = "AZURE"

	connectorEnablementPendingEnablement = "PENDING_ENABLEMENT"
	connectorEnablementPendingUpdate     = "PENDING_UPDATE"

	connectorHealthPendingAuthorization = "PENDING_AUTHORIZATION"

	connectorScopeStatePending = "PENDING"

	defaultConnectorsPageSize           = 100
	defaultConnectorScanConfigsPageSize = 50
)

func (b *InMemoryBackend) buildConnectorARN() string {
	return arn.Build(inspector2Service, b.region, b.accountID, "connector/"+uuid.New().String())
}

// autoInstallVMScannerOrDefault applies AzureProviderDetailCreate's
// documented default ("Specifies whether to automatically install the VM
// scanner on connected Azure resources. Defaults to true.") when the caller
// omits the field.
func autoInstallVMScannerOrDefault(v *bool) bool {
	if v == nil {
		return true
	}

	return *v
}

// pendingScope marks every scanning-type setting present in cfg as PENDING.
// This backend never validates a submitted scope against a live Azure
// tenant -- the connector itself never leaves PENDING_AUTHORIZATION (see
// CreateConnector's doc comment) -- so no scope setting can honestly report
// ACTIVE/ERROR/DISABLED.
func pendingScope(cfg *ConnectorScopeConfiguration) {
	if cfg == nil {
		return
	}

	for _, s := range []*ConnectorScopeSetting{cfg.VMScanning, cfg.ContainerImageScanning, cfg.ServerlessScanning} {
		if s != nil {
			s.State = connectorScopeStatePending
			s.StateReason = "Awaiting connector authorization."
		}
	}
}

// CreateConnector creates a new Azure connector.
//
// Real CreateConnector is asynchronous: Amazon Inspector begins provisioning
// the connector and its Azure AD app-consent (OAuth) authorization flow in
// the background, and CreateConnectorOutput returns only the new
// connector's ARN (confirmed via api_op_CreateConnector.go -- ConnectorArn
// is the output's only member). The connector only becomes healthy
// (Health.ConnectorStatus == CONNECTED) once an operator completes that
// external OAuth consent in the Azure portal; there is no Amazon Inspector
// API operation in the SDK that drives or observes that step
// (CreateConnector/UpdateConnector/DeleteConnector/ListConnectors/
// ListConnectorScanConfigurations/UpdateConnectorScanConfiguration are the
// entire connector-related SDK surface -- confirmed via `go doc
// .../inspector2 | grep -i connector`). This backend therefore creates the
// connector with EnablementStatus=PENDING_ENABLEMENT and
// Health.ConnectorStatus=PENDING_AUTHORIZATION and never auto-advances
// either: faking a transition to ENABLED/CONNECTED would silently claim to
// have completed an OAuth flow gopherstack has no way to perform, the same
// bug class the securityhub connector work in this campaign flagged.
func (b *InMemoryBackend) CreateConnector(
	name, description, provider string,
	tags map[string]string,
	awsConfigConnectorArn string,
	azureRegions []string,
	autoInstallVMScanner *bool,
	scopeConfig *ConnectorScopeConfiguration,
) (*Connector, error) {
	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if provider != connectorProviderAzure {
		return nil, fmt.Errorf("%w: provider must be AZURE", ErrValidation)
	}

	if awsConfigConnectorArn == "" {
		return nil, fmt.Errorf("%w: providerDetail.azure.awsConfigConnectorArn is required", ErrValidation)
	}

	if len(azureRegions) == 0 {
		return nil, fmt.Errorf("%w: providerDetail.azure.azureRegions is required", ErrValidation)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	pendingScope(scopeConfig)

	now := time.Now().UTC()
	connector := &Connector{
		ConnectorArn:          b.buildConnectorARN(),
		Name:                  name,
		Description:           description,
		Provider:              provider,
		AwsConfigConnectorArn: awsConfigConnectorArn,
		AzureRegions:          azureRegions,
		AutoInstallVMScanner:  autoInstallVMScannerOrDefault(autoInstallVMScanner),
		ScopeConfiguration:    scopeConfig,
		Tags:                  tags,
		EnablementStatus:      connectorEnablementPendingEnablement,
		Health: &ConnectorHealth{
			ConnectorStatus: connectorHealthPendingAuthorization,
			LastCheckedAt:   now,
			Message:         "Waiting for external Azure AD app-consent authorization to complete.",
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	b.connectors.Put(connector)

	cp := *connector

	return &cp, nil
}

// UpdateConnector updates an existing connector's description and/or Azure
// provider detail. Real UpdateConnectorOutput echoes only the connector's
// ARN (confirmed via api_op_UpdateConnector.go). description is nil when the
// caller omitted the field (real UpdateConnectorInput.Description is
// *string); a non-nil empty string is a valid explicit clear, matching the
// pointer-optionality of the real request shape. azureRegions/
// autoInstallVMScanner/scopeConfig replace the corresponding stored value
// only when the caller supplies them, matching AzureProviderDetailUpdate's
// all-optional members. EnablementStatus moves to PENDING_UPDATE, mirroring
// that a real update triggers Amazon Inspector to revalidate the new
// configuration -- and, like Create, this backend never auto-advances it
// back to ENABLED (see CreateConnector's doc comment for why).
func (b *InMemoryBackend) UpdateConnector(
	connectorArn string,
	description *string,
	azureRegions []string,
	autoInstallVMScanner *bool,
	scopeConfig *ConnectorScopeConfiguration,
) (*Connector, error) {
	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	connector, ok := b.connectors.Get(connectorArn)
	if !ok {
		return nil, ErrConnectorNotFound
	}

	if description != nil {
		connector.Description = *description
	}

	if azureRegions != nil {
		connector.AzureRegions = azureRegions
	}

	if autoInstallVMScanner != nil {
		connector.AutoInstallVMScanner = *autoInstallVMScanner
	}

	if scopeConfig != nil {
		pendingScope(scopeConfig)
		connector.ScopeConfiguration = scopeConfig
	}

	connector.EnablementStatus = connectorEnablementPendingUpdate
	connector.UpdatedAt = time.Now().UTC()

	cp := *connector

	return &cp, nil
}

// DeleteConnector deletes a connector.
//
// Real DeleteConnector is also asynchronous in principle (EnablementStatus
// has a PENDING_DELETION value), but the SDK exposes no follow-up read
// operation for a single connector (no GetConnector) through which a caller
// could ever observe an in-between state -- only ListConnectors, which would
// have to keep listing a deleted connector forever for "pending deletion" to
// be observable at all, since nothing in the connector SDK surface ever
// advances that state. Leaving it listed forever would misrepresent deletion
// as never completing, a worse dishonesty than treating the delete as
// complete once requested. This backend therefore removes the connector
// synchronously. Any ConnectorScanConfiguration keyed by the connector's
// AwsConfigConnectorArn is left untouched: it is real backend state (there is
// no evidence in the SDK that deleting one connector cascades to a scan
// configuration potentially still shared by other connectors on the same AWS
// Config connector ARN), not invented cleanup.
func (b *InMemoryBackend) DeleteConnector(connectorArn string) error {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	if !b.connectors.Delete(connectorArn) {
		return ErrConnectorNotFound
	}

	return nil
}

// ListConnectors lists connectors, optionally narrowed by provider,
// connector ARN, and/or AWS Config connector ARN -- the three
// ConnectorFilterCriteria facets this backend supports (see this file's
// package-level gaps note in PARITY.md for the ones it doesn't: accounts,
// meaningless in this single-account emulator, and connectorType, which has
// no corresponding field on the real Connector response shape to filter
// against at all). Matches are OR'd within a facet and AND'd across facets;
// every filter's real Comparison enum has exactly one value (EQUALS), so
// there is no other comparison semantic to emulate. Pagination uses the
// connector ARN as a stable cursor over the sorted result set, following
// ListFindings' precedent.
func (b *InMemoryBackend) ListConnectors(
	providers, connectorArns, awsConfigConnectorArns []string,
	maxResults int32, nextToken string,
) ([]*Connector, string, error) {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

	matched := make([]*Connector, 0, b.connectors.Len())

	b.connectors.Range(func(connector *Connector) bool {
		if len(providers) > 0 && !slices.Contains(providers, connector.Provider) {
			return true
		}

		if len(connectorArns) > 0 && !slices.Contains(connectorArns, connector.ConnectorArn) {
			return true
		}

		if len(awsConfigConnectorArns) > 0 &&
			!slices.Contains(awsConfigConnectorArns, connector.AwsConfigConnectorArn) {
			return true
		}

		cp := *connector
		matched = append(matched, &cp)

		return true
	})

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].ConnectorArn < matched[j].ConnectorArn
	})

	pageSize := int(maxResults)
	if pageSize <= 0 {
		pageSize = defaultConnectorsPageSize
	}

	start := 0

	if nextToken != "" {
		for i, connector := range matched {
			if connector.ConnectorArn == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+pageSize, len(matched))
	page := matched[start:end]

	next := ""
	if end < len(matched) {
		next = matched[end].ConnectorArn
	}

	return page, next, nil
}

// connectorScanConfigItemLocked builds the real ConnectorScanConfigurationItem
// shape: AwsConfigConnectorArn, the connector ARNs currently sharing it
// (derived live from the connectors table via connectorsByAwsConfigArn,
// since the real ConnectorArns member is a live join, not stored state), and
// the stored scan configuration. Callers must hold at least b.mu.RLock.
func (b *InMemoryBackend) connectorScanConfigItemLocked(
	cfg *ConnectorScanConfiguration,
) *ConnectorScanConfigurationItem {
	shared := b.connectorsByAwsConfigArn.Get(cfg.AwsConfigConnectorArn)
	connArns := make([]string, 0, len(shared))

	for _, connector := range shared {
		connArns = append(connArns, connector.ConnectorArn)
	}

	sort.Strings(connArns)

	cp := *cfg

	return &ConnectorScanConfigurationItem{
		AwsConfigConnectorArn: cfg.AwsConfigConnectorArn,
		ConnectorArns:         connArns,
		ScanConfiguration:     &cp,
	}
}

// ListConnectorScanConfigurations lists scan configurations that have been
// explicitly set via UpdateConnectorScanConfiguration, optionally narrowed to
// specific AWS Config connector ARNs. There is no
// CreateConnectorScanConfiguration operation in the real API, so a connector
// with no prior Update call simply has no entry here (not a zero-value one).
// Pagination follows ListConnectors' precedent, cursored on
// AwsConfigConnectorArn; real ListConnectorScanConfigurationsInput documents
// maxResults' valid range as 1-50, matching defaultConnectorScanConfigsPageSize.
func (b *InMemoryBackend) ListConnectorScanConfigurations(
	awsConfigConnectorArns []string, maxResults int32, nextToken string,
) ([]*ConnectorScanConfigurationItem, string, error) {
	b.mu.RLock("ListConnectorScanConfigurations")
	defer b.mu.RUnlock()

	matched := make([]*ConnectorScanConfigurationItem, 0, b.connectorScanConfigs.Len())

	b.connectorScanConfigs.Range(func(cfg *ConnectorScanConfiguration) bool {
		if len(awsConfigConnectorArns) > 0 && !slices.Contains(awsConfigConnectorArns, cfg.AwsConfigConnectorArn) {
			return true
		}

		matched = append(matched, b.connectorScanConfigItemLocked(cfg))

		return true
	})

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].AwsConfigConnectorArn < matched[j].AwsConfigConnectorArn
	})

	pageSize := int(maxResults)
	if pageSize <= 0 {
		pageSize = defaultConnectorScanConfigsPageSize
	}

	start := 0

	if nextToken != "" {
		for i, item := range matched {
			if item.AwsConfigConnectorArn == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+pageSize, len(matched))
	page := matched[start:end]

	next := ""
	if end < len(matched) {
		next = matched[end].AwsConfigConnectorArn
	}

	return page, next, nil
}

// UpdateConnectorScanConfiguration sets the scan configuration applied to
// resources discovered through every connector sharing awsConfigConnectorArn.
// There is no standalone "AWS Config connector" resource in Inspector2 to
// validate an unrecognized awsConfigConnectorArn against, so this backend
// requires at least one existing Connector carrying that
// AwsConfigConnectorArn, returning ErrConnectorScanConfigNotFound
// (ResourceNotFoundException) otherwise -- an arbitrary/unknown ARN is
// rejected rather than silently accepted, per this campaign's requirement
// that scan-config updates validate the connector actually exists.
func (b *InMemoryBackend) UpdateConnectorScanConfiguration(
	awsConfigConnectorArn string, containerImageScanning *ConnectorContainerImageScanConfig,
) error {
	b.mu.Lock("UpdateConnectorScanConfiguration")
	defer b.mu.Unlock()

	if len(b.connectorsByAwsConfigArn.Get(awsConfigConnectorArn)) == 0 {
		return ErrConnectorScanConfigNotFound
	}

	b.connectorScanConfigs.Put(&ConnectorScanConfiguration{
		AwsConfigConnectorArn:  awsConfigConnectorArn,
		ContainerImageScanning: containerImageScanning,
	})

	return nil
}
