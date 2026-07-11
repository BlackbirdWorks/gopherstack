package emr

// Code in this file supports Phase 3.3 of the datalayer refactor: six
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*Cluster) are each replaced by a single
// flat *store.Table[T], keyed by the composite "region|id" string (see
// regionKey in backend.go), with a companion *store.Index grouping entries by
// region for per-region scans -- the region-qualified-table pattern
// services/neptune and services/mwaa use. BlockPublicAccessConfiguration and
// blockPublicAccessMeta are account-level (one per region, not per-resource),
// so each becomes a plain (non-composite-key) *store.Table keyed directly by
// region -- no secondary index is needed since the region itself is already
// the unique lookup key.
//
// arnIndex (map[string]map[string]string) is deliberately NOT converted:
// store.Table requires a *V value with its own identity, but each entry here
// is a bare string with no identifier of its own. It remains a plain
// region-nested map, unchanged by this refactor (see arnIndexStore in
// backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func clusterKeyFn(v *Cluster) string            { return regionKey(v.region, v.ID) }
func clusterRegionIndexKeyFn(v *Cluster) string { return v.region }

func securityConfigKeyFn(v *SecurityConfiguration) string            { return regionKey(v.region, v.Name) }
func securityConfigRegionIndexKeyFn(v *SecurityConfiguration) string { return v.region }

func studioKeyFn(v *Studio) string            { return regionKey(v.region, v.StudioID) }
func studioRegionIndexKeyFn(v *Studio) string { return v.region }

// studioSessionMappingKeyFn derives the per-region composite key from the
// same studioSessionKey components used by the old map key, qualified by
// region.
func studioSessionMappingKeyFn(v *StudioSessionMapping) string {
	return regionKey(v.region, studioSessionKey(v.StudioID, v.IdentityType, v.IdentityID, v.IdentityName))
}
func studioSessionMappingRegionIndexKeyFn(v *StudioSessionMapping) string { return v.region }

// persistentAppUIKeyFn has no companion region-index key function: unlike
// the other resource collections here, EMR's real API has no
// ListPersistentAppUIs operation, so nothing ever needs a per-region scan of
// this table (DescribePersistentAppUI and GetOnClusterPresignedURL both look
// up by ID via persistentAppUIGet, not by region).
func persistentAppUIKeyFn(v *PersistentAppUI) string { return regionKey(v.region, v.ID) }

func notebookExecutionKeyFn(v *NotebookExecution) string {
	return regionKey(v.region, v.NotebookExecutionID)
}
func notebookExecutionRegionIndexKeyFn(v *NotebookExecution) string { return v.region }

// blockPublicAccessKeyFn and blockPublicAccessMetaKeyFn key their tables
// directly by region: there is exactly one BlockPublicAccessConfiguration
// (and one blockPublicAccessMeta) per region, so region itself is the whole
// primary key -- no composite "region|id" is needed here, unlike the six
// per-resource tables above.
func blockPublicAccessKeyFn(v *BlockPublicAccessConfiguration) string { return v.region }
func blockPublicAccessMetaKeyFn(v *blockPublicAccessMeta) string      { return v.region }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))
	b.clustersByRegion = b.clusters.AddIndex("byRegion", clusterRegionIndexKeyFn)

	b.securityConfigs = store.Register(b.registry, "securityConfigs", store.New(securityConfigKeyFn))
	b.securityConfigsByRegion = b.securityConfigs.AddIndex("byRegion", securityConfigRegionIndexKeyFn)

	b.studios = store.Register(b.registry, "studios", store.New(studioKeyFn))
	b.studiosByRegion = b.studios.AddIndex("byRegion", studioRegionIndexKeyFn)

	b.studioSessionMappings = store.Register(
		b.registry, "studioSessionMappings", store.New(studioSessionMappingKeyFn),
	)
	b.studioSessionMappingsByRegion = b.studioSessionMappings.AddIndex(
		"byRegion", studioSessionMappingRegionIndexKeyFn,
	)

	b.persistentAppUIs = store.Register(b.registry, "persistentAppUIs", store.New(persistentAppUIKeyFn))

	b.notebookExecutions = store.Register(b.registry, "notebookExecutions", store.New(notebookExecutionKeyFn))
	b.notebookExecutionsByRegion = b.notebookExecutions.AddIndex("byRegion", notebookExecutionRegionIndexKeyFn)

	b.blockPublicAccess = store.Register(b.registry, "blockPublicAccess", store.New(blockPublicAccessKeyFn))
	b.blockPublicAccessMeta = store.Register(
		b.registry, "blockPublicAccessMeta", store.New(blockPublicAccessMetaKeyFn),
	)
}
