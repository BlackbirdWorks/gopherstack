package inspector2

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/codebuild (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- Filter.Arn, storedFinding.FindingArn (via the
// embedded Finding), CodeSecurityIntegration.IntegrationArn,
// CisScanConfiguration.Arn, CisScan.ScanArn, SbomExport.ReportID,
// FindingsReport.ReportID, MemberEc2DeepInspectionStatus.AccountID,
// DelegatedAdminAccount.AccountID, CodeSecurityScanConfiguration.Arn,
// Member.AccountID, CisSession.ScanJobID, Connector.ConnectorArn,
// ConnectorScanConfiguration.AwsConfigConnectorArn -- so none of these tables
// need the "dirty" ephemeral-DTO registry treatment (cf. services/ses /
// services/codecommit): all are "clean" tables registered directly on
// b.registry, and persistence.go drives every one of them through
// b.registry.SnapshotAll() / RestoreAll().
//
// connectors additionally carries a "byAwsConfigArn" secondary [store.Index]
// (Connector.AwsConfigConnectorArn), following the cisScans "byConfig" index
// precedent: it backs both ListConnectorScanConfigurations' live
// ConnectorArns join and UpdateConnectorScanConfiguration's connector-exists
// validation (connectors.go).
//
// EncryptionKey has no single identity field, but its composite identity
// (ResourceType, ScanType) is already a pair of real fields on the value
// itself (not external state), so it is still a "clean" direct-register
// table -- encryptionKeyKeyFn simply joins the two fields, matching the
// "resourceType/scanType" composite key access sites already built by hand.
//
// CodeSecurityScanConfigurationAssociation was formerly a per-parent grouping
// (map[string][]*CodeSecurityScanConfigurationAssociation) keyed by
// scanConfigurationArn. It becomes a Table keyed by the composite
// "<scanConfigurationArn>/<resource>" identity, with a companion "byConfig"
// store.Index replacing the map's grouping-by-parent role.
//
// CisScan gains a "byConfig" store.Index (ScanConfigurationArn) replacing the
// former linear scan in findCisScan / DeleteCisScanConfiguration's scan
// cleanup loop.
//
// Left as plain maps (not store.Table-backed), persistence-audited in
// persistence.go:
//   - tags (map[string]map[string]string): values are plain string maps, not
//     *T.
//   - enabledTypes (map[string]bool): values are plain bools, not *T.
//   - codeSecurityScans (map[string]map[string]any): values are plain maps,
//     not *T.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func filterKeyFn(v *Filter) string { return v.Arn }

func storedFindingKeyFn(v *storedFinding) string { return v.FindingArn }

func codeSecurityIntegrationKeyFn(v *CodeSecurityIntegration) string { return v.IntegrationArn }

func cisScanConfigKeyFn(v *CisScanConfiguration) string { return v.Arn }

func cisScanKeyFn(v *CisScan) string          { return v.ScanArn }
func cisScanConfigARNKeyFn(v *CisScan) string { return v.ScanConfigurationArn }

func sbomExportKeyFn(v *SbomExport) string { return v.ReportID }

func findingsReportKeyFn(v *FindingsReport) string { return v.ReportID }

func memberEc2StatusKeyFn(v *MemberEc2DeepInspectionStatus) string { return v.AccountID }

func delegatedAdminKeyFn(v *DelegatedAdminAccount) string { return v.AccountID }

func codeSecurityScanConfigKeyFn(v *CodeSecurityScanConfiguration) string { return v.Arn }

// encryptionKeyKeyFn joins the composite identity used everywhere else in
// this backend (see e.g. GetEncryptionKey/UpdateEncryptionKey's local `key`
// variable) so lookups built by hand and Table.Put/Restore agree on the same
// key for the same value.
func encryptionKeyKeyFn(v *EncryptionKey) string { return v.ResourceType + "/" + v.ScanType }

func memberKeyFn(v *Member) string { return v.AccountID }

func cisSessionKeyFn(v *CisSession) string { return v.ScanJobID }

// scanConfigAssociationKeyFn builds the composite "<scanConfigurationArn>/<resource>"
// identity for a CodeSecurityScanConfigurationAssociation.
func scanConfigAssociationKeyFn(v *CodeSecurityScanConfigurationAssociation) string {
	return v.ScanConfigurationArn + "/" + v.Resource
}

func scanConfigAssociationConfigKeyFn(v *CodeSecurityScanConfigurationAssociation) string {
	return v.ScanConfigurationArn
}

// coverageEntryKeyFn builds the composite "<resourceId>/<scanType>" identity
// for a CoverageEntry -- a resource can be covered by more than one scan
// type (e.g. an EC2 instance covered by both EC2 and EC2_AGENTLESS), so
// resourceId alone is not unique.
func coverageEntryKeyFn(v *CoverageEntry) string { return v.ResourceID + "/" + v.ScanType }

func vulnerabilityKeyFn(v *Vulnerability) string { return v.ID }

func codeSnippetKeyFn(v *codeSnippet) string { return v.FindingArn }

func connectorKeyFn(v *Connector) string { return v.ConnectorArn }

// connectorAwsConfigArnKeyFn groups connectors by the AWS Config connector
// ARN they share, backing both ListConnectorScanConfigurations'
// ConnectorArns member and UpdateConnectorScanConfiguration's
// connector-exists validation (see connectors.go).
func connectorAwsConfigArnKeyFn(v *Connector) string { return v.AwsConfigConnectorArn }

func connectorScanConfigKeyFn(v *ConnectorScanConfiguration) string { return v.AwsConfigConnectorArn }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	b.filters = store.Register(b.registry, "filters", store.New(filterKeyFn))

	b.findings = store.Register(b.registry, "findings", store.New(storedFindingKeyFn))

	b.codeSecurityIntegrations = store.Register(
		b.registry, "codeSecurityIntegrations", store.New(codeSecurityIntegrationKeyFn),
	)

	b.cisScanConfigs = store.Register(b.registry, "cisScanConfigs", store.New(cisScanConfigKeyFn))

	b.cisScans = store.Register(b.registry, "cisScans", store.New(cisScanKeyFn))
	b.cisScansByConfig = b.cisScans.AddIndex("byConfig", cisScanConfigARNKeyFn)

	b.sbomExports = store.Register(b.registry, "sbomExports", store.New(sbomExportKeyFn))

	b.findingsReports = store.Register(b.registry, "findingsReports", store.New(findingsReportKeyFn))

	b.memberEc2Status = store.Register(b.registry, "memberEc2Status", store.New(memberEc2StatusKeyFn))

	b.delegatedAdmins = store.Register(b.registry, "delegatedAdmins", store.New(delegatedAdminKeyFn))

	b.codeSecurityScanConfigs = store.Register(
		b.registry, "codeSecurityScanConfigs", store.New(codeSecurityScanConfigKeyFn),
	)

	b.encryptionKeys = store.Register(b.registry, "encryptionKeys", store.New(encryptionKeyKeyFn))

	b.members = store.Register(b.registry, "members", store.New(memberKeyFn))

	b.cisSessions = store.Register(b.registry, "cisSessions", store.New(cisSessionKeyFn))

	b.scanConfigAssociations = store.Register(
		b.registry, "scanConfigAssociations", store.New(scanConfigAssociationKeyFn),
	)
	b.scanConfigAssociationsByConfig = b.scanConfigAssociations.AddIndex(
		"byConfig", scanConfigAssociationConfigKeyFn,
	)

	b.coverageEntries = store.Register(b.registry, "coverageEntries", store.New(coverageEntryKeyFn))

	b.vulnerabilities = store.Register(b.registry, "vulnerabilities", store.New(vulnerabilityKeyFn))

	b.codeSnippets = store.Register(b.registry, "codeSnippets", store.New(codeSnippetKeyFn))

	b.connectors = store.Register(b.registry, "connectors", store.New(connectorKeyFn))
	b.connectorsByAwsConfigArn = b.connectors.AddIndex("byAwsConfigArn", connectorAwsConfigArnKeyFn)

	b.connectorScanConfigs = store.Register(
		b.registry, "connectorScanConfigs", store.New(connectorScanConfigKeyFn),
	)
}
