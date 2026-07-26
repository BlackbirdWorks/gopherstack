package appconfig

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// resource-collection map on InMemoryBackend is replaced with a
// *store.Table[T] (plus companion *store.Index values where a per-parent
// scan or cascade delete needs one). See pkgs/store's package doc for the
// underlying primitive, and the InMemoryBackend doc comment (store.go) for
// why each table registers directly vs. on a composite key.
//
// applications, deploymentStrategies, extensions, and extensionAssociations
// are flat, top-level collections keyed by their own real identity field
// (ID) -- the services/sesv2 / services/dax / services/datasync
// clean-direct-register template. Each ByName *store.Index answers the
// per-collection name-uniqueness check in O(1), replacing the old
// applicationsByName/deploymentStrategiesByName/extensionsByName reverse
// maps.
//
// environments and configProfiles were nested two levels deep
// (applicationID -> own ID) but Environment.ID/ConfigurationProfile.ID are
// globally-random like every other ID in this backend, so both register
// directly on their own ID (opsworks/datasync precedent), with a byApp
// *store.Index (grouping by ApplicationID) replacing the per-application
// nesting, and a byAppName *store.Index (composite "applicationID|name" key)
// replacing the environmentsByName/configProfilesByName reverse maps.
//
// hostedConfigVersions and deployments were nested three levels deep
// (applicationID -> profileID/environmentID -> VersionNumber/
// DeploymentNumber). Those innermost keys are per-parent counters, not
// globally unique, so both flatten to a composite "applicationID|parentID|
// number" key (hcvKey/deploymentKey in store.go) with a byProfile/byEnv
// index for the per-parent List operations and a byApp index for
// DeleteApplication's cascade delete. hostedConfigVersions additionally
// carries a byLabel index (composite "applicationID|profileID|label")
// replacing the versionLabelIndex raw map: VersionLabel uniqueness is
// answered by the index instead of a parallel set.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func applicationKeyFn(v *Application) string { return v.ID }

func applicationNameIndexKeyFn(v *Application) string { return v.Name }

func environmentKeyFn(v *Environment) string { return v.ID }

func environmentAppIndexKeyFn(v *Environment) string { return v.ApplicationID }

func environmentAppNameIndexKeyFn(v *Environment) string {
	return appNameKey(v.ApplicationID, v.Name)
}

func configProfileKeyFn(v *ConfigurationProfile) string { return v.ID }

func configProfileAppIndexKeyFn(v *ConfigurationProfile) string { return v.ApplicationID }

func configProfileAppNameIndexKeyFn(v *ConfigurationProfile) string {
	return appNameKey(v.ApplicationID, v.Name)
}

func hostedConfigVersionKeyFn(v *HostedConfigurationVersion) string {
	return hcvKey(v.ApplicationID, v.ConfigurationProfileID, v.VersionNumber)
}

func hostedConfigVersionProfileIndexKeyFn(v *HostedConfigurationVersion) string {
	return appProfileKey(v.ApplicationID, v.ConfigurationProfileID)
}

func hostedConfigVersionAppIndexKeyFn(v *HostedConfigurationVersion) string {
	return v.ApplicationID
}

func hostedConfigVersionLabelIndexKeyFn(v *HostedConfigurationVersion) string {
	return hcvLabelKey(v.ApplicationID, v.ConfigurationProfileID, v.VersionLabel)
}

func deploymentStrategyKeyFn(v *DeploymentStrategy) string { return v.ID }

func deploymentStrategyNameIndexKeyFn(v *DeploymentStrategy) string { return v.Name }

func deploymentKeyFn(v *Deployment) string {
	return deploymentKey(v.ApplicationID, v.EnvironmentID, v.DeploymentNumber)
}

func deploymentEnvIndexKeyFn(v *Deployment) string {
	return appEnvKey(v.ApplicationID, v.EnvironmentID)
}

func deploymentAppIndexKeyFn(v *Deployment) string { return v.ApplicationID }

// extensionKeyFn keys each row on its composite (extensionID, versionNumber)
// identity -- extensions are versioned resources in real AWS AppConfig (see
// the InMemoryBackend doc comment in store.go), so unlike every other
// directly-registered table here, Extension.ID alone is not a unique key.
func extensionKeyFn(v *Extension) string { return extensionVersionKey(v.ID, v.VersionNumber) }

func extensionIDIndexKeyFn(v *Extension) string { return v.ID }

func extensionNameIndexKeyFn(v *Extension) string { return v.Name }

func extensionAssociationKeyFn(v *ExtensionAssociation) string { return v.ID }

func experimentDefinitionKeyFn(v *ExperimentDefinition) string { return v.ID }

func experimentDefinitionAppIndexKeyFn(v *ExperimentDefinition) string { return v.ApplicationID }

func experimentDefinitionAppNameIndexKeyFn(v *ExperimentDefinition) string {
	return appNameKey(v.ApplicationID, v.Name)
}

// experimentRunKeyFn keys each row on its composite (experimentDefinitionID,
// run) identity -- Run is a per-definition counter, not globally unique
// (see the InMemoryBackend doc comment in store.go).
func experimentRunKeyFn(v *ExperimentRun) string {
	return experimentRunKey(v.ExperimentDefinitionID, v.Run)
}

func experimentRunDefIndexKeyFn(v *ExperimentRun) string { return v.ExperimentDefinitionID }

// registerAllTables registers every store.Table-backed resource field
// exactly once, at construction time. It must be called during construction
// only (immediately after b.registry is created -- see NewInMemoryBackend),
// never on every reset -- store.Register panics on a duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	b.applicationsByName = b.applications.AddIndex("byName", applicationNameIndexKeyFn)

	b.environments = store.Register(b.registry, "environments", store.New(environmentKeyFn))
	b.environmentsByApp = b.environments.AddIndex("byApp", environmentAppIndexKeyFn)
	b.environmentsByAppName = b.environments.AddIndex("byAppName", environmentAppNameIndexKeyFn)

	b.configProfiles = store.Register(b.registry, "configProfiles", store.New(configProfileKeyFn))
	b.configProfilesByApp = b.configProfiles.AddIndex("byApp", configProfileAppIndexKeyFn)
	b.configProfilesByAppName = b.configProfiles.AddIndex("byAppName", configProfileAppNameIndexKeyFn)

	b.hostedConfigVersions = store.Register(
		b.registry, "hostedConfigVersions", store.New(hostedConfigVersionKeyFn),
	)
	b.hostedConfigVersionsByProfile = b.hostedConfigVersions.AddIndex(
		"byProfile", hostedConfigVersionProfileIndexKeyFn,
	)
	b.hostedConfigVersionsByApp = b.hostedConfigVersions.AddIndex(
		"byApp", hostedConfigVersionAppIndexKeyFn,
	)
	b.hostedConfigVersionsByLabel = b.hostedConfigVersions.AddIndex(
		"byLabel", hostedConfigVersionLabelIndexKeyFn,
	)

	b.deploymentStrategies = store.Register(
		b.registry, "deploymentStrategies", store.New(deploymentStrategyKeyFn),
	)
	b.deploymentStrategiesByName = b.deploymentStrategies.AddIndex(
		"byName", deploymentStrategyNameIndexKeyFn,
	)

	b.deployments = store.Register(b.registry, "deployments", store.New(deploymentKeyFn))
	b.deploymentsByEnv = b.deployments.AddIndex("byEnv", deploymentEnvIndexKeyFn)
	b.deploymentsByApp = b.deployments.AddIndex("byApp", deploymentAppIndexKeyFn)

	b.extensions = store.Register(b.registry, "extensions", store.New(extensionKeyFn))
	b.extensionsByID = b.extensions.AddIndex("byID", extensionIDIndexKeyFn)
	b.extensionsByName = b.extensions.AddIndex("byName", extensionNameIndexKeyFn)

	b.extensionAssociations = store.Register(
		b.registry, "extensionAssociations", store.New(extensionAssociationKeyFn),
	)

	b.experimentDefinitions = store.Register(
		b.registry, "experimentDefinitions", store.New(experimentDefinitionKeyFn),
	)
	b.experimentDefinitionsByApp = b.experimentDefinitions.AddIndex(
		"byApp", experimentDefinitionAppIndexKeyFn,
	)
	b.experimentDefinitionsByAppName = b.experimentDefinitions.AddIndex(
		"byAppName", experimentDefinitionAppNameIndexKeyFn,
	)

	b.experimentRuns = store.Register(b.registry, "experimentRuns", store.New(experimentRunKeyFn))
	b.experimentRunsByDef = b.experimentRuns.AddIndex("byDef", experimentRunDefIndexKeyFn)
}
