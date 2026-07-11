package directoryservice

// Code in this file supports Phase 3.3 of the datalayer refactor: sixteen
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*storedTrust inside regionState) are
// each replaced by a single flat *store.Table[T], keyed by the composite
// "region|id" string (see regionKey in backend.go), with a companion
// *store.Index grouping entries by region for per-region scans -- the same
// region-qualified-table pattern services/neptune, services/secretsmanager
// and services/cloudwatchlogs use.
//
// Six collections are deliberately NOT converted: store.Table requires a *V
// value with its own identity, but aliases (map[string]string), ipRoutes
// (map[string][]storedIpRoute), dirDataAccess/caEnrollment (map[string]bool)
// and dirSettings/updateInfoEntries (map[string][]*T) each hold either a bare
// scalar/slice with no key of its own, or a slice of multiple values sharing
// one directory key. They remain plain region-nested maps, unchanged by this
// refactor (see the *Store helpers in backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func directoryKeyFn(v *storedDirectory) string            { return regionKey(v.region, v.DirectoryID) }
func directoryRegionIndexKeyFn(v *storedDirectory) string { return v.region }

func snapshotKeyFn(v *storedSnapshot) string            { return regionKey(v.region, v.SnapshotID) }
func snapshotRegionIndexKeyFn(v *storedSnapshot) string { return v.region }

func dsRegionKeyFn(v *storedRegion) string {
	return regionKey(v.region, dsRegionID(v.DirectoryID, v.RegionName))
}
func dsRegionRegionIndexKeyFn(v *storedRegion) string { return v.region }

func schemaExtensionKeyFn(v *storedSchemaExtension) string            { return regionKey(v.region, v.ExtensionID) }
func schemaExtensionRegionIndexKeyFn(v *storedSchemaExtension) string { return v.region }

func conditionalForwarderKeyFn(v *storedConditionalForwarder) string {
	return regionKey(v.region, conditionalForwarderID(v.DirectoryID, v.RemoteDomainName))
}
func conditionalForwarderRegionIndexKeyFn(v *storedConditionalForwarder) string { return v.region }

func logSubscriptionKeyFn(v *storedLogSubscription) string {
	return regionKey(v.region, logSubscriptionID(v.DirectoryID, v.LogGroupName))
}
func logSubscriptionRegionIndexKeyFn(v *storedLogSubscription) string { return v.region }

func eventTopicKeyFn(v *storedEventTopic) string {
	return regionKey(v.region, eventTopicID(v.DirectoryID, v.TopicName))
}
func eventTopicRegionIndexKeyFn(v *storedEventTopic) string { return v.region }

func domainControllerKeyFn(v *storedDomainController) string {
	return regionKey(v.region, v.ControllerID)
}
func domainControllerRegionIndexKeyFn(v *storedDomainController) string { return v.region }

func trustKeyFn(v *storedTrust) string            { return regionKey(v.region, v.TrustID) }
func trustRegionIndexKeyFn(v *storedTrust) string { return v.region }

func sharedDirectoryKeyFn(v *storedSharedDirectory) string {
	return regionKey(v.region, v.SharedDirectoryID)
}
func sharedDirectoryRegionIndexKeyFn(v *storedSharedDirectory) string { return v.region }

func certificateKeyFn(v *storedCertificate) string            { return regionKey(v.region, v.CertificateID) }
func certificateRegionIndexKeyFn(v *storedCertificate) string { return v.region }

func ldapsSettingKeyFn(v *storedLDAPSSetting) string {
	return regionKey(v.region, ldapsSettingID(v.DirectoryID, v.LDAPSType))
}
func ldapsSettingRegionIndexKeyFn(v *storedLDAPSSetting) string { return v.region }

func clientAuthSettingKeyFn(v *storedClientAuthSetting) string {
	return regionKey(v.region, clientAuthSettingID(v.DirectoryID, v.AuthType))
}
func clientAuthSettingRegionIndexKeyFn(v *storedClientAuthSetting) string { return v.region }

func radiusSettingsKeyFn(v *storedRadiusSettings) string            { return regionKey(v.region, v.DirectoryID) }
func radiusSettingsRegionIndexKeyFn(v *storedRadiusSettings) string { return v.region }

// adAssessmentKeyFn reuses storedADAssessment's existing exported Region
// field (already populated with the AWS request region for the API's own
// ADAssessmentInfo.Region output) instead of adding a duplicate unexported
// field: it already carries exactly the value regionKey needs.
func adAssessmentKeyFn(v *storedADAssessment) string            { return regionKey(v.Region, v.AssessmentID) }
func adAssessmentRegionIndexKeyFn(v *storedADAssessment) string { return v.Region }

func hybridADUpdateKeyFn(v *storedHybridADUpdate) string            { return regionKey(v.region, v.RequestID) }
func hybridADUpdateRegionIndexKeyFn(v *storedHybridADUpdate) string { return v.region }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call and companion store.Table.AddIndex call to the concrete field and
// value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.directories = store.Register(b.registry, "directories", store.New(directoryKeyFn))
		b.directoriesByRegion = b.directories.AddIndex("byRegion", directoryRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.snapshots = store.Register(b.registry, "snapshots", store.New(snapshotKeyFn))
		b.snapshotsByRegion = b.snapshots.AddIndex("byRegion", snapshotRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.dsRegions = store.Register(b.registry, "dsRegions", store.New(dsRegionKeyFn))
		b.dsRegionsByRegion = b.dsRegions.AddIndex("byRegion", dsRegionRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.schemaExtensions = store.Register(b.registry, "schemaExtensions", store.New(schemaExtensionKeyFn))
		b.schemaExtensionsByRegion = b.schemaExtensions.AddIndex("byRegion", schemaExtensionRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.conditionalForwarders = store.Register(
			b.registry, "conditionalForwarders", store.New(conditionalForwarderKeyFn),
		)
		b.conditionalForwardersByRegion = b.conditionalForwarders.AddIndex(
			"byRegion", conditionalForwarderRegionIndexKeyFn,
		)
	},
	func(b *InMemoryBackend) {
		b.logSubscriptions = store.Register(b.registry, "logSubscriptions", store.New(logSubscriptionKeyFn))
		b.logSubscriptionsByRegion = b.logSubscriptions.AddIndex("byRegion", logSubscriptionRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.eventTopics = store.Register(b.registry, "eventTopics", store.New(eventTopicKeyFn))
		b.eventTopicsByRegion = b.eventTopics.AddIndex("byRegion", eventTopicRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.domainControllers = store.Register(b.registry, "domainControllers", store.New(domainControllerKeyFn))
		b.domainControllersByRegion = b.domainControllers.AddIndex("byRegion", domainControllerRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.trusts = store.Register(b.registry, "trusts", store.New(trustKeyFn))
		b.trustsByRegion = b.trusts.AddIndex("byRegion", trustRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.sharedDirectories = store.Register(b.registry, "sharedDirectories", store.New(sharedDirectoryKeyFn))
		b.sharedDirectoriesByRegion = b.sharedDirectories.AddIndex("byRegion", sharedDirectoryRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.certificates = store.Register(b.registry, "certificates", store.New(certificateKeyFn))
		b.certificatesByRegion = b.certificates.AddIndex("byRegion", certificateRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.ldapsSettings = store.Register(b.registry, "ldapsSettings", store.New(ldapsSettingKeyFn))
		b.ldapsSettingsByRegion = b.ldapsSettings.AddIndex("byRegion", ldapsSettingRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.clientAuthSettings = store.Register(b.registry, "clientAuthSettings", store.New(clientAuthSettingKeyFn))
		b.clientAuthSettingsByRegion = b.clientAuthSettings.AddIndex("byRegion", clientAuthSettingRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.radiusSettings = store.Register(b.registry, "radiusSettings", store.New(radiusSettingsKeyFn))
		b.radiusSettingsByRegion = b.radiusSettings.AddIndex("byRegion", radiusSettingsRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.adAssessments = store.Register(b.registry, "adAssessments", store.New(adAssessmentKeyFn))
		b.adAssessmentsByRegion = b.adAssessments.AddIndex("byRegion", adAssessmentRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.hybridADUpdates = store.Register(b.registry, "hybridADUpdates", store.New(hybridADUpdateKeyFn))
		b.hybridADUpdatesByRegion = b.hybridADUpdates.AddIndex("byRegion", hybridADUpdateRegionIndexKeyFn)
	},
}
