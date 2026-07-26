package guardduty

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (and detector-nested map[string]map[string]*T) resource
// field on InMemoryBackend is replaced with a *store.Table[T], following the
// region-qualified-table pattern services/emr and services/codeartifact
// use, but qualified by detectorID instead of region (see detectorKey in
// backend.go).
//
// detectors, findings, and members already carry a real, wire-visible
// identity field (Detector.DetectorID; Finding.DetectorID + Finding.ID;
// Member.DetectorID + Member.AccountID), so each is a "clean" table --
// registered directly on b.registry, no DTO wrapper needed (see
// persistence.go). invitations, orgAdminAccounts, malwareScans, and
// malwareProtectionPlans are flat (not detector-nested) maps whose value
// types likewise carry a real identity field (InvitationID, AdminAccountID,
// ScanID, MalwareProtectionPlanID), so those four are also "clean".
//
// filters, ipSets, threatIntelSets, publishingDestinations,
// threatEntitySets, trustedEntitySets, and investigations have a DetectorID
// field, but it is hidden from the wire shape (json:"-"), so a direct
// b.registry.SnapshotAll would silently lose it on restore -- each of these
// seven is a "dirty" table (store.New only, deliberately NOT
// store.Register-ed onto b.registry) with persistence.go building an
// ephemeral DTO registry that carries DetectorID alongside the value.
//
// orgConfigs, adminAccounts, and malwareScanSettings were flat maps keyed
// directly by detectorID, but OrgConfig/AdminAccount/MalwareScanSettings
// have no identity field of their own (identity-less); each gained an
// unexported detectorID field purely for the table's key, which -- being
// unexported -- never round-trips through a direct json.Marshal either, so
// these three are "dirty" tables too.
//
// tags (map[string]string values, not *T) is deliberately NOT converted: it
// has no identity of its own to key a store.Table on. It remains a plain
// map, unchanged by this refactor (see persistence.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func detectorTableKeyFn(v *Detector) string { return v.DetectorID }

func filterTableKeyFn(v *Filter) string         { return detectorKey(v.DetectorID, v.Name) }
func filterDetectorIndexKeyFn(v *Filter) string { return v.DetectorID }

func findingTableKeyFn(v *Finding) string         { return detectorKey(v.DetectorID, v.ID) }
func findingDetectorIndexKeyFn(v *Finding) string { return v.DetectorID }

func ipSetTableKeyFn(v *IPSet) string         { return detectorKey(v.DetectorID, v.IPSetID) }
func ipSetDetectorIndexKeyFn(v *IPSet) string { return v.DetectorID }

func threatIntelSetTableKeyFn(v *ThreatIntelSet) string {
	return detectorKey(v.DetectorID, v.ThreatIntelSetID)
}
func threatIntelSetDetectorIndexKeyFn(v *ThreatIntelSet) string { return v.DetectorID }

func memberTableKeyFn(v *Member) string         { return detectorKey(v.DetectorID, v.AccountID) }
func memberDetectorIndexKeyFn(v *Member) string { return v.DetectorID }

func invitationTableKeyFn(v *Invitation) string { return v.InvitationID }

func orgAdminAccountTableKeyFn(v *OrgAdminAccount) string { return v.AdminAccountID }

func orgConfigTableKeyFn(v *OrgConfig) string { return v.detectorID }

func adminAccountTableKeyFn(v *AdminAccount) string { return v.detectorID }

func publishingDestinationTableKeyFn(v *PublishingDestination) string {
	return detectorKey(v.DetectorID, v.DestinationID)
}
func publishingDestinationDetectorIndexKeyFn(v *PublishingDestination) string { return v.DetectorID }

func malwareScanTableKeyFn(v *MalwareScan) string { return v.ScanID }

func malwareScanSettingsTableKeyFn(v *MalwareScanSettings) string { return v.detectorID }

func malwareProtectionPlanTableKeyFn(v *MalwareProtectionPlan) string {
	return v.MalwareProtectionPlanID
}

func threatEntitySetTableKeyFn(v *ThreatEntitySet) string {
	return detectorKey(v.DetectorID, v.ThreatEntitySetID)
}
func threatEntitySetDetectorIndexKeyFn(v *ThreatEntitySet) string { return v.DetectorID }

func trustedEntitySetTableKeyFn(v *TrustedEntitySet) string {
	return detectorKey(v.DetectorID, v.TrustedEntitySetID)
}
func trustedEntitySetDetectorIndexKeyFn(v *TrustedEntitySet) string { return v.DetectorID }

func investigationTableKeyFn(v *Investigation) string {
	return detectorKey(v.DetectorID, v.InvestigationID)
}
func investigationDetectorIndexKeyFn(v *Investigation) string { return v.DetectorID }

// registerAllTables registers every converted resource collection on
// b.registry (the "clean" tables) or builds it standalone (the "dirty"
// tables -- see the file doc comment above) exactly once. It must be called
// during construction only (immediately after b.registry is created -- see
// NewInMemoryBackend), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() plus
// the dirty tables' own Reset() calls instead (see InMemoryBackend.Reset in
// backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.detectors = store.Register(b.registry, "detectors", store.New(detectorTableKeyFn))

	b.filters = store.New(filterTableKeyFn)
	b.filtersByDetector = b.filters.AddIndex("byDetector", filterDetectorIndexKeyFn)

	b.findings = store.Register(b.registry, "findings", store.New(findingTableKeyFn))
	b.findingsByDetector = b.findings.AddIndex("byDetector", findingDetectorIndexKeyFn)

	b.ipSets = store.New(ipSetTableKeyFn)
	b.ipSetsByDetector = b.ipSets.AddIndex("byDetector", ipSetDetectorIndexKeyFn)

	b.threatIntelSets = store.New(threatIntelSetTableKeyFn)
	b.threatIntelSetsByDetector = b.threatIntelSets.AddIndex("byDetector", threatIntelSetDetectorIndexKeyFn)

	b.members = store.Register(b.registry, "members", store.New(memberTableKeyFn))
	b.membersByDetector = b.members.AddIndex("byDetector", memberDetectorIndexKeyFn)

	b.invitations = store.Register(b.registry, "invitations", store.New(invitationTableKeyFn))

	b.orgAdminAccounts = store.Register(b.registry, "orgAdminAccounts", store.New(orgAdminAccountTableKeyFn))

	b.orgConfigs = store.New(orgConfigTableKeyFn)

	b.adminAccounts = store.New(adminAccountTableKeyFn)

	b.publishingDestinations = store.New(publishingDestinationTableKeyFn)
	b.publishingDestinationsByDetector = b.publishingDestinations.AddIndex(
		"byDetector", publishingDestinationDetectorIndexKeyFn,
	)

	b.malwareScans = store.Register(b.registry, "malwareScans", store.New(malwareScanTableKeyFn))

	b.malwareScanSettings = store.New(malwareScanSettingsTableKeyFn)

	b.malwareProtectionPlans = store.Register(
		b.registry, "malwareProtectionPlans", store.New(malwareProtectionPlanTableKeyFn),
	)

	b.threatEntitySets = store.New(threatEntitySetTableKeyFn)
	b.threatEntitySetsByDetector = b.threatEntitySets.AddIndex("byDetector", threatEntitySetDetectorIndexKeyFn)

	b.trustedEntitySets = store.New(trustedEntitySetTableKeyFn)
	b.trustedEntitySetsByDetector = b.trustedEntitySets.AddIndex("byDetector", trustedEntitySetDetectorIndexKeyFn)

	b.investigations = store.New(investigationTableKeyFn)
	b.investigationsByDetector = b.investigations.AddIndex("byDetector", investigationDetectorIndexKeyFn)
}
