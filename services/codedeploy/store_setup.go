package codedeploy

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// backend's five *T resource maps become store.Table[T]s.
//
// deployments and deploymentConfigs carry a real, wire-visible identity
// field (DeploymentID / DeploymentConfigName respectively) and no live
// *tags.Tags field, so each is a "clean" table -- registered directly on
// b.registry, no DTO wrapper needed (see persistence.go).
//
// applications, deploymentGroups, and onPremisesInstances each carry a live
// *tags.Tags field marked json:"-" (see backend.go): registering them
// directly on b.registry would let registry.SnapshotAll silently drop tags,
// since Table.Snapshot's JSON marshal honours that json:"-" tag same as any
// other. So all three are "dirty" tables: store.New only, deliberately NOT
// store.Register-ed onto b.registry; persistence.go instead builds an
// ephemeral DTO registry for them (mirroring services/resourcegroups'
// groupSnapshot / services/codeartifact's regionalDTO technique), and
// InMemoryBackend.Reset (backend.go) resets each of the three explicitly.
//
// deploymentGroups was previously nested by application
// (map[string]map[string]*DeploymentGroup); DeploymentGroup already carries
// real ApplicationName and DeploymentGroupName wire fields, so it flattens
// to one store.Table keyed by the composite "appName/dgName" string (see
// dgKey in backend.go), with a companion *store.Index grouping entries by
// application for the per-application scans (ListDeploymentGroups,
// DeleteApplication's cascade, etc.) the nested map used to answer directly.
//
// githubTokens (map[string]struct{}, an identity-less set) is deliberately
// NOT converted: there is no *T value for store.Table to key on. It remains
// a plain map, unchanged by this refactor (see backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func applicationTableKeyFn(v *Application) string { return v.ApplicationName }

// dgKey returns the composite store.Table primary key for a deployment
// group, shared by the live deploymentGroups table and its persistence DTO.
func dgKey(appName, dgName string) string { return appName + "/" + dgName }

func deploymentGroupTableKeyFn(v *DeploymentGroup) string {
	return dgKey(v.ApplicationName, v.DeploymentGroupName)
}

func deploymentGroupAppIndexKeyFn(v *DeploymentGroup) string { return v.ApplicationName }

func deploymentTableKeyFn(v *Deployment) string { return v.DeploymentID }

func onPremisesInstanceTableKeyFn(v *OnPremisesInstance) string { return v.InstanceName }

func deploymentConfigTableKeyFn(v *DeploymentConfig) string { return v.DeploymentConfigName }

// registerAllTables registers every converted resource collection on
// b.registry (the "clean" tables: deployments, deploymentConfigs) or builds
// it standalone (the "dirty" tables: applications, deploymentGroups,
// onPremisesInstances -- see the file doc comment above) exactly once. It
// must be called during construction only (immediately after b.registry is
// created -- see NewInMemoryBackend), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() plus the dirty tables' own Reset() calls instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.applications = store.New(applicationTableKeyFn)

	b.deploymentGroups = store.New(deploymentGroupTableKeyFn)
	b.deploymentGroupsByApp = b.deploymentGroups.AddIndex("byApplication", deploymentGroupAppIndexKeyFn)

	b.onPremisesInstances = store.New(onPremisesInstanceTableKeyFn)

	b.deployments = store.Register(b.registry, "deployments", store.New(deploymentTableKeyFn))
	b.deploymentConfigs = store.Register(b.registry, "deploymentConfigs", store.New(deploymentConfigTableKeyFn))
}
