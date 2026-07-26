package directoryservice

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
//
// Directory Service resources are isolated per region: every backend operation
// resolves the caller's region from the request context and operates only on that
// region's nested store. Directories and all of their dependent resources (snapshots,
// trusts, certificates, conditional forwarders, etc.) are inherently single-region —
// their identifiers (d-..., s-..., t-..., c-...) carry no region component, so the
// region is always taken from the request context (falling back to the backend
// default). Cross-region references never occur and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// The following *ID helpers build the composite identifier used both as the
// suffix of a table's "region|id" primary key and as the ID carried by that
// table's persistence DTO (see persistence.go); they mirror the ad hoc
// "directoryID:extra" string keys the pre-conversion map-based backend used
// directly as its map keys.
func dsRegionID(directoryID, regionName string) string { return directoryID + ":" + regionName }
func conditionalForwarderID(directoryID, remoteDomainName string) string {
	return directoryID + ":" + remoteDomainName
}
func logSubscriptionID(directoryID, logGroupName string) string {
	return directoryID + ":" + logGroupName
}
func eventTopicID(directoryID, topicName string) string       { return directoryID + ":" + topicName }
func ldapsSettingID(directoryID, ldapsType string) string     { return directoryID + ":" + ldapsType }
func clientAuthSettingID(directoryID, authType string) string { return directoryID + ":" + authType }

// InMemoryBackend implements StorageBackend using in-memory maps, nested per region.
//
// Sixteen resource collections that were previously nested by region (outer
// key = region, e.g. map[string]map[string]*storedTrust) are now each a
// single flat *store.Table keyed by the composite "region|id" string (see
// regionKey above and store_setup.go), with a companion *store.Index grouping
// entries by region for per-region scans. aliases, ipRoutes, dirDataAccess,
// caEnrollment, dirSettings and updateInfoEntries remain raw region-nested
// maps: their values carry no identity of their own to key a store.Table by
// (see store_setup.go's doc comment for the full rationale).
type InMemoryBackend struct {
	registry *store.Registry

	directories         *store.Table[storedDirectory]
	directoriesByRegion *store.Index[storedDirectory]

	snapshots         *store.Table[storedSnapshot]
	snapshotsByRegion *store.Index[storedSnapshot]

	dsRegions         *store.Table[storedRegion]
	dsRegionsByRegion *store.Index[storedRegion]

	schemaExtensions         *store.Table[storedSchemaExtension]
	schemaExtensionsByRegion *store.Index[storedSchemaExtension]

	conditionalForwarders         *store.Table[storedConditionalForwarder]
	conditionalForwardersByRegion *store.Index[storedConditionalForwarder]

	logSubscriptions         *store.Table[storedLogSubscription]
	logSubscriptionsByRegion *store.Index[storedLogSubscription]

	eventTopics         *store.Table[storedEventTopic]
	eventTopicsByRegion *store.Index[storedEventTopic]

	domainControllers         *store.Table[storedDomainController]
	domainControllersByRegion *store.Index[storedDomainController]

	trusts         *store.Table[storedTrust]
	trustsByRegion *store.Index[storedTrust]

	sharedDirectories         *store.Table[storedSharedDirectory]
	sharedDirectoriesByRegion *store.Index[storedSharedDirectory]

	certificates         *store.Table[storedCertificate]
	certificatesByRegion *store.Index[storedCertificate]

	ldapsSettings         *store.Table[storedLDAPSSetting]
	ldapsSettingsByRegion *store.Index[storedLDAPSSetting]

	clientAuthSettings         *store.Table[storedClientAuthSetting]
	clientAuthSettingsByRegion *store.Index[storedClientAuthSetting]

	radiusSettings         *store.Table[storedRadiusSettings]
	radiusSettingsByRegion *store.Index[storedRadiusSettings]

	adAssessments         *store.Table[storedADAssessment]
	adAssessmentsByRegion *store.Index[storedADAssessment]

	hybridADUpdates         *store.Table[storedHybridADUpdate]
	hybridADUpdatesByRegion *store.Index[storedHybridADUpdate]

	// Raw region-nested maps (outer key = AWS region); see the doc comment
	// above for why these six were not converted to store.Table.
	aliases           map[string]map[string]string // region -> alias -> directoryID
	ipRoutes          map[string]map[string][]storedIpRoute
	dirDataAccess     map[string]map[string]bool
	caEnrollment      map[string]map[string]bool
	dirSettings       map[string]map[string][]*storedDirectorySetting
	updateInfoEntries map[string]map[string][]*storedUpdateInfo

	mu        *lockmetrics.RWMutex
	region    string
	accountID string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:          store.NewRegistry(),
		aliases:           make(map[string]map[string]string),
		ipRoutes:          make(map[string]map[string][]storedIpRoute),
		dirDataAccess:     make(map[string]map[string]bool),
		caEnrollment:      make(map[string]map[string]bool),
		dirSettings:       make(map[string]map[string][]*storedDirectorySetting),
		updateInfoEntries: make(map[string]map[string][]*storedUpdateInfo),
		mu:                lockmetrics.New("directoryservice"),
		accountID:         accountID,
		region:            region,
	}
	registerAllTables(b)

	return b
}

// The following accessor helpers replace the old lazy per-region map
// accessors (b.state(region).directories etc.) with store.Table / store.Index
// operations. Callers must still hold b.mu, exactly as before -- store.Table
// performs no locking of its own (see pkgs/store's package doc).

func (b *InMemoryBackend) directoryGet(region, id string) (*storedDirectory, bool) {
	return b.directories.Get(regionKey(region, id))
}

func (b *InMemoryBackend) directoryPut(v *storedDirectory) { b.directories.Put(v) }

func (b *InMemoryBackend) directoryDelete(region, id string) {
	b.directories.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) directoriesInRegion(region string) []*storedDirectory {
	return b.directoriesByRegion.Get(region)
}

func (b *InMemoryBackend) snapshotGet(region, id string) (*storedSnapshot, bool) {
	return b.snapshots.Get(regionKey(region, id))
}

func (b *InMemoryBackend) snapshotPut(v *storedSnapshot) { b.snapshots.Put(v) }

func (b *InMemoryBackend) snapshotDelete(region, id string) {
	b.snapshots.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) snapshotsInRegion(region string) []*storedSnapshot {
	return b.snapshotsByRegion.Get(region)
}

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu.

func (b *InMemoryBackend) aliasesStore(region string) map[string]string {
	if b.aliases[region] == nil {
		b.aliases[region] = make(map[string]string)
	}

	return b.aliases[region]
}

func (b *InMemoryBackend) ipRoutesStore(region string) map[string][]storedIpRoute {
	if b.ipRoutes[region] == nil {
		b.ipRoutes[region] = make(map[string][]storedIpRoute)
	}

	return b.ipRoutes[region]
}

// ipRoutesStoreRO returns the region-scoped ipRoutes map for region without
// mutating the outer map. Safe to call while holding only b.mu.RLock(): if
// the region has not been observed yet, it returns a fresh, unregistered,
// empty map instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) ipRoutesStoreRO(region string) map[string][]storedIpRoute {
	if v := b.ipRoutes[region]; v != nil {
		return v
	}

	return map[string][]storedIpRoute{}
}

func (b *InMemoryBackend) dirDataAccessStore(region string) map[string]bool {
	if b.dirDataAccess[region] == nil {
		b.dirDataAccess[region] = make(map[string]bool)
	}

	return b.dirDataAccess[region]
}

// dirDataAccessStoreRO returns the region-scoped dirDataAccess map for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty map instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) dirDataAccessStoreRO(region string) map[string]bool {
	if v := b.dirDataAccess[region]; v != nil {
		return v
	}

	return map[string]bool{}
}

func (b *InMemoryBackend) caEnrollmentStore(region string) map[string]bool {
	if b.caEnrollment[region] == nil {
		b.caEnrollment[region] = make(map[string]bool)
	}

	return b.caEnrollment[region]
}

// caEnrollmentStoreRO returns the region-scoped caEnrollment map for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty map instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) caEnrollmentStoreRO(region string) map[string]bool {
	if v := b.caEnrollment[region]; v != nil {
		return v
	}

	return map[string]bool{}
}

func (b *InMemoryBackend) dirSettingsStore(region string) map[string][]*storedDirectorySetting {
	if b.dirSettings[region] == nil {
		b.dirSettings[region] = make(map[string][]*storedDirectorySetting)
	}

	return b.dirSettings[region]
}

// dirSettingsStoreRO returns the region-scoped dirSettings map for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty map instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) dirSettingsStoreRO(region string) map[string][]*storedDirectorySetting {
	if v := b.dirSettings[region]; v != nil {
		return v
	}

	return map[string][]*storedDirectorySetting{}
}

func (b *InMemoryBackend) updateInfoEntriesStore(region string) map[string][]*storedUpdateInfo {
	if b.updateInfoEntries[region] == nil {
		b.updateInfoEntries[region] = make(map[string][]*storedUpdateInfo)
	}

	return b.updateInfoEntries[region]
}

// updateInfoEntriesStoreRO returns the region-scoped updateInfoEntries map
// for region without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty map instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) updateInfoEntriesStoreRO(region string) map[string][]*storedUpdateInfo {
	if v := b.updateInfoEntries[region]; v != nil {
		return v
	}

	return map[string][]*storedUpdateInfo{}
}

func (b *InMemoryBackend) newDirectoryID() string {
	return fmt.Sprintf("d-%s", uuid.NewString()[:10])
}

func (b *InMemoryBackend) newSnapshotID() string {
	return fmt.Sprintf("s-%s", uuid.NewString()[:10])
}

func (b *InMemoryBackend) defaultAlias(directoryID string) string {
	return directoryID
}

func (b *InMemoryBackend) defaultAccessURL(alias string) string {
	return fmt.Sprintf("%s.awsapps.com", alias)
}

// encodePageToken encodes an integer offset as an opaque base64 token.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken decodes a token produced by encodePageToken.
func decodePageToken(tok string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(b))
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetAllState()
}

// resetAllState clears every registered table and every raw region-nested
// map, returning the backend to the state NewInMemoryBackend leaves it in
// (minus accountID/region, which callers restore separately). Callers must
// hold b.mu.
func (b *InMemoryBackend) resetAllState() {
	b.registry.ResetAll()
	b.aliases = make(map[string]map[string]string)
	b.ipRoutes = make(map[string]map[string][]storedIpRoute)
	b.dirDataAccess = make(map[string]map[string]bool)
	b.caEnrollment = make(map[string]map[string]bool)
	b.dirSettings = make(map[string]map[string][]*storedDirectorySetting)
	b.updateInfoEntries = make(map[string]map[string][]*storedUpdateInfo)
}

func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return make(map[string]string)
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// The following accessor helpers mirror the ones above, one set per
// extended (Appendix A) resource table. Callers must hold b.mu.

func (b *InMemoryBackend) dsRegionGet(region, directoryID, regionName string) (*storedRegion, bool) {
	return b.dsRegions.Get(regionKey(region, dsRegionID(directoryID, regionName)))
}

func (b *InMemoryBackend) dsRegionPut(v *storedRegion) { b.dsRegions.Put(v) }

func (b *InMemoryBackend) dsRegionsInRegion(region string) []*storedRegion {
	return b.dsRegionsByRegion.Get(region)
}

func (b *InMemoryBackend) schemaExtensionGet(region, id string) (*storedSchemaExtension, bool) {
	return b.schemaExtensions.Get(regionKey(region, id))
}

func (b *InMemoryBackend) schemaExtensionPut(v *storedSchemaExtension) { b.schemaExtensions.Put(v) }

func (b *InMemoryBackend) schemaExtensionsInRegion(region string) []*storedSchemaExtension {
	return b.schemaExtensionsByRegion.Get(region)
}

func (b *InMemoryBackend) conditionalForwarderGet(
	region, directoryID, remoteDomainName string,
) (*storedConditionalForwarder, bool) {
	return b.conditionalForwarders.Get(regionKey(region, conditionalForwarderID(directoryID, remoteDomainName)))
}

func (b *InMemoryBackend) conditionalForwarderPut(v *storedConditionalForwarder) {
	b.conditionalForwarders.Put(v)
}

func (b *InMemoryBackend) conditionalForwarderDelete(region, directoryID, remoteDomainName string) {
	b.conditionalForwarders.Delete(regionKey(region, conditionalForwarderID(directoryID, remoteDomainName)))
}

func (b *InMemoryBackend) conditionalForwardersInRegion(region string) []*storedConditionalForwarder {
	return b.conditionalForwardersByRegion.Get(region)
}

func (b *InMemoryBackend) logSubscriptionGet(region, directoryID, logGroupName string) (*storedLogSubscription, bool) {
	return b.logSubscriptions.Get(regionKey(region, logSubscriptionID(directoryID, logGroupName)))
}

func (b *InMemoryBackend) logSubscriptionPut(v *storedLogSubscription) { b.logSubscriptions.Put(v) }

func (b *InMemoryBackend) logSubscriptionsInRegion(region string) []*storedLogSubscription {
	return b.logSubscriptionsByRegion.Get(region)
}

func (b *InMemoryBackend) eventTopicGet(region, directoryID, topicName string) (*storedEventTopic, bool) {
	return b.eventTopics.Get(regionKey(region, eventTopicID(directoryID, topicName)))
}

func (b *InMemoryBackend) eventTopicPut(v *storedEventTopic) { b.eventTopics.Put(v) }

func (b *InMemoryBackend) eventTopicDelete(region, directoryID, topicName string) {
	b.eventTopics.Delete(regionKey(region, eventTopicID(directoryID, topicName)))
}

func (b *InMemoryBackend) eventTopicsInRegion(region string) []*storedEventTopic {
	return b.eventTopicsByRegion.Get(region)
}

func (b *InMemoryBackend) domainControllerGet(region, id string) (*storedDomainController, bool) {
	return b.domainControllers.Get(regionKey(region, id))
}

func (b *InMemoryBackend) domainControllerPut(v *storedDomainController) { b.domainControllers.Put(v) }

func (b *InMemoryBackend) domainControllerDelete(region, id string) {
	b.domainControllers.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) domainControllersInRegion(region string) []*storedDomainController {
	return b.domainControllersByRegion.Get(region)
}

func (b *InMemoryBackend) trustGet(region, id string) (*storedTrust, bool) {
	return b.trusts.Get(regionKey(region, id))
}

func (b *InMemoryBackend) trustPut(v *storedTrust) { b.trusts.Put(v) }

func (b *InMemoryBackend) trustDelete(region, id string) { b.trusts.Delete(regionKey(region, id)) }

func (b *InMemoryBackend) trustsInRegion(region string) []*storedTrust {
	return b.trustsByRegion.Get(region)
}

func (b *InMemoryBackend) sharedDirectoryGet(region, id string) (*storedSharedDirectory, bool) {
	return b.sharedDirectories.Get(regionKey(region, id))
}

func (b *InMemoryBackend) sharedDirectoryPut(v *storedSharedDirectory) { b.sharedDirectories.Put(v) }

func (b *InMemoryBackend) sharedDirectoriesInRegion(region string) []*storedSharedDirectory {
	return b.sharedDirectoriesByRegion.Get(region)
}

func (b *InMemoryBackend) certificateGet(region, id string) (*storedCertificate, bool) {
	return b.certificates.Get(regionKey(region, id))
}

func (b *InMemoryBackend) certificatePut(v *storedCertificate) { b.certificates.Put(v) }

func (b *InMemoryBackend) certificateDelete(region, id string) {
	b.certificates.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) certificatesInRegion(region string) []*storedCertificate {
	return b.certificatesByRegion.Get(region)
}

func (b *InMemoryBackend) ldapsSettingGet(region, directoryID, ldapsType string) (*storedLDAPSSetting, bool) {
	return b.ldapsSettings.Get(regionKey(region, ldapsSettingID(directoryID, ldapsType)))
}

func (b *InMemoryBackend) ldapsSettingPut(v *storedLDAPSSetting) { b.ldapsSettings.Put(v) }

func (b *InMemoryBackend) ldapsSettingsInRegion(region string) []*storedLDAPSSetting {
	return b.ldapsSettingsByRegion.Get(region)
}

func (b *InMemoryBackend) clientAuthSettingGet(region, directoryID, authType string) (*storedClientAuthSetting, bool) {
	return b.clientAuthSettings.Get(regionKey(region, clientAuthSettingID(directoryID, authType)))
}

func (b *InMemoryBackend) clientAuthSettingPut(v *storedClientAuthSetting) {
	b.clientAuthSettings.Put(v)
}

func (b *InMemoryBackend) clientAuthSettingsInRegion(region string) []*storedClientAuthSetting {
	return b.clientAuthSettingsByRegion.Get(region)
}

func (b *InMemoryBackend) radiusSettingsGet(region, directoryID string) (*storedRadiusSettings, bool) {
	return b.radiusSettings.Get(regionKey(region, directoryID))
}

func (b *InMemoryBackend) radiusSettingsPut(v *storedRadiusSettings) { b.radiusSettings.Put(v) }

func (b *InMemoryBackend) radiusSettingsDelete(region, directoryID string) {
	b.radiusSettings.Delete(regionKey(region, directoryID))
}

func (b *InMemoryBackend) adAssessmentGet(region, id string) (*storedADAssessment, bool) {
	return b.adAssessments.Get(regionKey(region, id))
}

func (b *InMemoryBackend) adAssessmentPut(v *storedADAssessment) { b.adAssessments.Put(v) }

func (b *InMemoryBackend) adAssessmentDelete(region, id string) {
	b.adAssessments.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) adAssessmentsInRegion(region string) []*storedADAssessment {
	return b.adAssessmentsByRegion.Get(region)
}

func (b *InMemoryBackend) hybridADUpdatePut(v *storedHybridADUpdate) { b.hybridADUpdates.Put(v) }

func (b *InMemoryBackend) hybridADUpdatesInRegion(region string) []*storedHybridADUpdate {
	return b.hybridADUpdatesByRegion.Get(region)
}

// cascadeDeleteDirectory removes all resources that belong to directoryID
// from every table/raw map scoped to region. Must be called with the backend
// lock held.
func (b *InMemoryBackend) cascadeDeleteDirectory(region, directoryID string) {
	for _, snap := range slices.Clone(b.snapshotsInRegion(region)) {
		if snap.DirectoryID == directoryID {
			b.snapshotDelete(region, snap.SnapshotID)
		}
	}

	b.cascadeDeleteRawMaps(region, directoryID)
	b.cascadeDeleteCoreTables(region, directoryID)
	b.cascadeDeleteForwardingTables(region, directoryID)
	b.cascadeDeleteTrustTables(region, directoryID)
	b.cascadeDeleteSettingsTables(region, directoryID)
}

// cascadeDeleteRawMaps clears the six raw region-nested maps' entries for
// directoryID. Split out of cascadeDeleteDirectory to keep its cognitive
// complexity low.
func (b *InMemoryBackend) cascadeDeleteRawMaps(region, directoryID string) {
	delete(b.ipRoutesStore(region), directoryID)
	b.radiusSettingsDelete(region, directoryID)
	delete(b.dirDataAccessStore(region), directoryID)
	delete(b.caEnrollmentStore(region), directoryID)
	delete(b.dirSettingsStore(region), directoryID)
	delete(b.updateInfoEntriesStore(region), directoryID)
}

// cascadeDeleteCoreTables removes dsRegions/schemaExtensions entries owned by
// directoryID. Split out of cascadeDeleteDirectory to keep its cognitive
// complexity low.
func (b *InMemoryBackend) cascadeDeleteCoreTables(region, directoryID string) {
	for _, r := range slices.Clone(b.dsRegionsInRegion(region)) {
		if r.DirectoryID == directoryID {
			b.dsRegions.Delete(regionKey(region, dsRegionID(r.DirectoryID, r.RegionName)))
		}
	}
	for _, e := range slices.Clone(b.schemaExtensionsInRegion(region)) {
		if e.DirectoryID == directoryID {
			b.schemaExtensions.Delete(regionKey(region, e.ExtensionID))
		}
	}
}

// cascadeDeleteForwardingTables removes conditionalForwarders/logSubscriptions/
// eventTopics/domainControllers entries owned by directoryID. Split out of
// cascadeDeleteDirectory to keep its cognitive complexity low.
func (b *InMemoryBackend) cascadeDeleteForwardingTables(region, directoryID string) {
	for _, f := range slices.Clone(b.conditionalForwardersInRegion(region)) {
		if f.DirectoryID == directoryID {
			b.conditionalForwarderDelete(region, f.DirectoryID, f.RemoteDomainName)
		}
	}
	for _, s := range slices.Clone(b.logSubscriptionsInRegion(region)) {
		if s.DirectoryID == directoryID {
			b.logSubscriptions.Delete(regionKey(region, logSubscriptionID(s.DirectoryID, s.LogGroupName)))
		}
	}
	for _, t := range slices.Clone(b.eventTopicsInRegion(region)) {
		if t.DirectoryID == directoryID {
			b.eventTopicDelete(region, t.DirectoryID, t.TopicName)
		}
	}
	for _, dc := range slices.Clone(b.domainControllersInRegion(region)) {
		if dc.DirectoryID == directoryID {
			b.domainControllerDelete(region, dc.ControllerID)
		}
	}
}

// cascadeDeleteTrustTables removes trusts/sharedDirectories/certificates/
// ldapsSettings entries owned by directoryID. Split out of
// cascadeDeleteDirectory to keep its cognitive complexity low.
func (b *InMemoryBackend) cascadeDeleteTrustTables(region, directoryID string) {
	for _, t := range slices.Clone(b.trustsInRegion(region)) {
		if t.DirectoryID == directoryID {
			b.trustDelete(region, t.TrustID)
		}
	}
	for _, sd := range slices.Clone(b.sharedDirectoriesInRegion(region)) {
		if sd.OwnerDirectoryID == directoryID {
			b.sharedDirectories.Delete(regionKey(region, sd.SharedDirectoryID))
		}
	}
	for _, c := range slices.Clone(b.certificatesInRegion(region)) {
		if c.DirectoryID == directoryID {
			b.certificateDelete(region, c.CertificateID)
		}
	}
	for _, l := range slices.Clone(b.ldapsSettingsInRegion(region)) {
		if l.DirectoryID == directoryID {
			b.ldapsSettings.Delete(regionKey(region, ldapsSettingID(l.DirectoryID, l.LDAPSType)))
		}
	}
}

// cascadeDeleteSettingsTables removes clientAuthSettings/adAssessments/
// hybridADUpdates entries owned by directoryID. Split out of
// cascadeDeleteDirectory to keep its cognitive complexity low.
func (b *InMemoryBackend) cascadeDeleteSettingsTables(region, directoryID string) {
	for _, a := range slices.Clone(b.clientAuthSettingsInRegion(region)) {
		if a.DirectoryID == directoryID {
			b.clientAuthSettings.Delete(regionKey(region, clientAuthSettingID(a.DirectoryID, a.AuthType)))
		}
	}
	for _, a := range slices.Clone(b.adAssessmentsInRegion(region)) {
		if a.DirectoryID == directoryID {
			b.adAssessmentDelete(region, a.AssessmentID)
		}
	}
	for _, h := range slices.Clone(b.hybridADUpdatesInRegion(region)) {
		if h.DirectoryID == directoryID {
			b.hybridADUpdates.Delete(regionKey(region, h.RequestID))
		}
	}
}
