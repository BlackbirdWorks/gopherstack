package elasticbeanstalk

// Code in this file supports Phase 3.3 of the datalayer refactor: five
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*Application) are each replaced by a
// single flat *store.Table, keyed by the composite "region|id" string (see
// regionKey in backend.go) -- the same region-qualified-table pattern
// services/emr and services/codeartifact use. Application, ApplicationVersion,
// ConfigurationTemplate, and PlatformVersion each gain a hidden `region`
// field (see backend.go) since none of them carried an exported region field
// pre-refactor; Environment already had an exported Region field, so it needs
// no new field. Companion *store.Index instances replace the old hand-rolled
// per-region reverse-lookup maps (appARNIndex, envARNIndex, verARNIndex,
// envNameIndex, cnamIndex): each index key is itself a "region|X" composite
// so region-scoped isolation (e.g. an ARN created in one region must not
// resolve when queried from another, see TestEBTagRegionIsolation) is
// preserved exactly -- a plain global index keyed by ARN/name alone would
// have collapsed that isolation.
//
// managedActionHistory (map[region]map[envName][]*ManagedActionHistory),
// events (map[region][]*EventRecord), and envCounters (map[region]int) are
// deliberately NOT converted: store.Table requires a *V value with its own
// identity per key, but these are slice- or scalar-valued maps with no
// single-value-per-key shape to model as a Table. They remain plain
// region-nested maps, unchanged by this refactor.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func applicationKeyFn(v *Application) string            { return regionKey(v.region, v.ApplicationName) }
func applicationRegionIndexKeyFn(v *Application) string { return v.region }
func applicationARNIndexKeyFn(v *Application) string {
	return regionKey(v.region, v.ApplicationARN)
}

func environmentKeyFn(v *Environment) string {
	return regionKey(v.Region, envKey(v.ApplicationName, v.EnvironmentName))
}
func environmentRegionIndexKeyFn(v *Environment) string { return v.Region }
func environmentARNIndexKeyFn(v *Environment) string {
	return regionKey(v.Region, v.EnvironmentARN)
}
func environmentNameIndexKeyFn(v *Environment) string {
	return regionKey(v.Region, v.EnvironmentName)
}
func environmentCNAMEIndexKeyFn(v *Environment) string {
	return regionKey(v.Region, v.CNAME)
}

func appVersionKeyFn(v *ApplicationVersion) string {
	return regionKey(v.region, appVersionKey(v.ApplicationName, v.VersionLabel))
}
func appVersionRegionIndexKeyFn(v *ApplicationVersion) string { return v.region }
func appVersionARNIndexKeyFn(v *ApplicationVersion) string {
	return regionKey(v.region, v.ApplicationVersionARN)
}

func configTemplateKeyFn(v *ConfigurationTemplate) string {
	return regionKey(v.region, configTemplateKey(v.ApplicationName, v.TemplateName))
}
func configTemplateRegionIndexKeyFn(v *ConfigurationTemplate) string { return v.region }

func platformVersionKeyFn(v *PlatformVersion) string            { return regionKey(v.region, v.PlatformArn) }
func platformVersionRegionIndexKeyFn(v *PlatformVersion) string { return v.region }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	b.applicationsByRegion = b.applications.AddIndex("byRegion", applicationRegionIndexKeyFn)
	b.applicationsByARN = b.applications.AddIndex("byARN", applicationARNIndexKeyFn)

	b.environments = store.Register(b.registry, "environments", store.New(environmentKeyFn))
	b.environmentsByRegion = b.environments.AddIndex("byRegion", environmentRegionIndexKeyFn)
	b.environmentsByARN = b.environments.AddIndex("byARN", environmentARNIndexKeyFn)
	b.environmentsByName = b.environments.AddIndex("byName", environmentNameIndexKeyFn)
	b.environmentsByCNAME = b.environments.AddIndex("byCNAME", environmentCNAMEIndexKeyFn)

	b.appVersions = store.Register(b.registry, "appVersions", store.New(appVersionKeyFn))
	b.appVersionsByRegion = b.appVersions.AddIndex("byRegion", appVersionRegionIndexKeyFn)
	b.appVersionsByARN = b.appVersions.AddIndex("byARN", appVersionARNIndexKeyFn)

	b.configTemplates = store.Register(b.registry, "configTemplates", store.New(configTemplateKeyFn))
	b.configTemplatesByRegion = b.configTemplates.AddIndex("byRegion", configTemplateRegionIndexKeyFn)

	b.platformVersions = store.Register(b.registry, "platformVersions", store.New(platformVersionKeyFn))
	b.platformVersionsByRegion = b.platformVersions.AddIndex("byRegion", platformVersionRegionIndexKeyFn)
}
