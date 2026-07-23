package ecs

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend that is a pure function of
// the stored value's own identity is registered exactly once, here, as a
// *store.Table[T] on b.registry. See pkgs/store's package doc and the ec2
// (commit 12e611a4) and sqs (commit 0f09d77c) conversions this follows.
//
// Resources that used to be nested map[string]map[string]*T (cluster ->
// resource-name -> value) are flattened into a single Table keyed by a
// composite "cluster/name" string, with a secondary store.Index grouping by
// cluster so the old "all X in cluster Y" access pattern (b.tasks[cluster],
// b.services[cluster], ...) still resolves in O(k).
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment above registerAllTables for the list and why.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// scopedKey builds a composite store.Table key for a resource whose identity
// is only unique within an owning scope (a cluster name for services/
// containerInstances/daemons, a service ARN for task sets). It mirrors the
// two-level map[scope]map[id]*T nesting it replaces.
func scopedKey(scope, id string) string { return scope + "/" + id }

// ---- primary key functions ----

func clustersKeyFn(v *Cluster) string                             { return v.ClusterName }
func capacityProvidersKeyFn(v *CapacityProvider) string           { return v.Name }
func accountSettingsKeyFn(v *AccountSetting) string               { return accountSettingKey(v.Name, v.PrincipalArn) }
func taskProtectionsKeyFn(v *TaskProtection) string               { return v.TaskArn }
func serviceDeploymentsKeyFn(v *ServiceDeployment) string         { return v.ServiceDeploymentArn }
func expressGatewayServicesKeyFn(v *ExpressGatewayService) string { return v.ServiceArn }
func daemonRevisionsKeyFn(v *DaemonRevision) string               { return v.DaemonRevisionArn }
func daemonDeploymentsKeyFn(v *DaemonDeployment) string           { return v.DaemonDeploymentArn }

func servicesKeyFn(v *Service) string             { return scopedKey(clusterKey(v.ClusterArn), v.ServiceName) }
func servicesClusterIndexKeyFn(v *Service) string { return clusterKey(v.ClusterArn) }

func tasksKeyFn(v *Task) string             { return v.TaskArn }
func tasksClusterIndexKeyFn(v *Task) string { return clusterKey(v.ClusterArn) }

func containerInstancesKeyFn(v *ContainerInstance) string {
	return scopedKey(clusterKey(v.ClusterArn), v.ContainerInstanceArn)
}
func containerInstancesClusterIndexKeyFn(v *ContainerInstance) string {
	return clusterKey(v.ClusterArn)
}

func taskSetsKeyFn(v *TaskSet) string             { return scopedKey(v.ServiceArn, v.TaskSetArn) }
func taskSetsServiceIndexKeyFn(v *TaskSet) string { return v.ServiceArn }

func daemonsKeyFn(v *Daemon) string             { return scopedKey(clusterKey(v.ClusterArn), v.DaemonName) }
func daemonsClusterIndexKeyFn(v *Daemon) string { return clusterKey(v.ClusterArn) }

// taskDefByArnKeyFn and daemonTaskDefByArnKeyFn back the two ARN-lookup cache
// tables that are NOT registered on b.registry (see registerAllTables) because
// they are derived caches rebuilt from the raw taskDefinitions/
// daemonTaskDefinitions maps, not independently persisted state.
func taskDefByArnKeyFn(v *TaskDefinition) string             { return v.TaskDefinitionArn }
func daemonTaskDefByArnKeyFn(v *DaemonTaskDefinition) string { return v.DaemonTaskDefinitionArn }

// registerAllTables registers every converted resource map on b.registry
// exactly once, and constructs the two unregistered ARN-cache tables. It must
// be called during construction only (immediately after b.registry is
// created), never on every Reset() -- store.Register panics on a duplicate
// name, so runtime resets go through registry.ResetAll() (plus the two
// unregistered caches' own .Reset()) instead; see InMemoryBackend.Reset in
// store.go.
//
// The following resource fields are deliberately left as plain maps (not
// registered here):
//   - taskDefinitions, daemonTaskDefinitions, daemonTaskDefs: value type is a
//     slice ([]*T), not a single *T -- store.Table wraps map[string]*V, so a
//     map[string][]*T container is out of scope for this conversion (matches
//     the ec2 precedent of leaving map[string][]*T fields such as
//     ipamPoolCidrs/spotFleetHistory/subnetCIDRAssociations raw). Ordering
//     within each family's revision slice is also load-bearing (latest
//     revision = last element) in a way a store.Index's Table.Restore-driven,
//     primary-key-sorted rebuild would not reliably preserve for a
//     multi-digit revision family.
//   - resourceTags: value type []Tag, same slice-shape exclusion.
//   - tasksByInstance: three levels of map keyed by bool, not a *T value at
//     all -- an internal reverse-index set, not a resource collection.
//   - serviceIndex: keyed by the svcRef struct, not a string, and its value is
//     bool, not *T.
//   - attributes: composite key (cluster, attributeKey(name, targetID))
//     requires the cluster, which is not stored on the Attribute value itself
//     -- matches the ec2 precedent for vpcCidrAssociations ("key composite
//     requires vpcID which is not stored on the value").
//   - lifecycle: value type taskLifecycle carries no identity field of its
//     own (no task ARN field); it is keyed externally by task ARN -- matches
//     the ec2 precedent for instanceIMDSOptions/vpcPeeringOptions-style
//     exclusions ("value type carries no identity field of its own").
//   - serviceRevisions, serviceRevisionsByArn: both are intentionally never
//     populated (see the comment on InMemoryBackend.serviceRevisions in
//     store.go -- this backend derives ServiceRevision snapshots on demand
//     instead), and serviceRevisions is additionally slice-valued.
func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clustersKeyFn))
	b.capacityProviders = store.Register(b.registry, "capacityProviders", store.New(capacityProvidersKeyFn))
	b.accountSettings = store.Register(b.registry, "accountSettings", store.New(accountSettingsKeyFn))
	b.taskProtections = store.Register(b.registry, "taskProtections", store.New(taskProtectionsKeyFn))
	b.serviceDeployments = store.Register(b.registry, "serviceDeployments", store.New(serviceDeploymentsKeyFn))
	b.expressGatewayServices = store.Register(
		b.registry, "expressGatewayServices", store.New(expressGatewayServicesKeyFn),
	)
	b.daemonRevisions = store.Register(b.registry, "daemonRevisions", store.New(daemonRevisionsKeyFn))
	b.daemonDeployments = store.Register(b.registry, "daemonDeployments", store.New(daemonDeploymentsKeyFn))

	b.services = store.Register(b.registry, "services", store.New(servicesKeyFn))
	b.servicesByCluster = b.services.AddIndex("servicesByCluster", servicesClusterIndexKeyFn)

	b.tasks = store.Register(b.registry, "tasks", store.New(tasksKeyFn))
	b.tasksByCluster = b.tasks.AddIndex("tasksByCluster", tasksClusterIndexKeyFn)

	b.containerInstances = store.Register(b.registry, "containerInstances", store.New(containerInstancesKeyFn))
	b.containerInstancesByCluster = b.containerInstances.AddIndex(
		"containerInstancesByCluster", containerInstancesClusterIndexKeyFn,
	)

	b.taskSets = store.Register(b.registry, "taskSets", store.New(taskSetsKeyFn))
	b.taskSetsByService = b.taskSets.AddIndex("taskSetsByService", taskSetsServiceIndexKeyFn)

	b.daemons = store.Register(b.registry, "daemons", store.New(daemonsKeyFn))
	b.daemonsByCluster = b.daemons.AddIndex("daemonsByCluster", daemonsClusterIndexKeyFn)

	// Unregistered derived-cache tables: reset/rebuilt manually, never part of
	// the persisted "tables" blob (see Reset/Restore in store.go/persistence.go).
	b.taskDefByArn = store.New(taskDefByArnKeyFn)
	b.daemonTaskDefByArn = store.New(daemonTaskDefByArnKeyFn)
}

// The helpers below replace the old "delete an entire per-cluster/per-service
// submap in one shot" idiom (e.g. delete(b.tasks, clusterName)) that the
// pre-conversion nested map[string]map[string]*T shape supported directly. A
// store.Index's Get returns a slice OWNED by the index -- it must not be
// mutated or ranged over while concurrently deleting from the backing Table
// (each Table.Delete calls idx.remove, which mutates that same backing
// slice) -- so every one of these snapshots the group into a private copy
// first via append(nil, ...). All must be called with the write lock held.

// tasksInClusterLocked returns a snapshot copy of every task in clusterName.
func (b *InMemoryBackend) tasksInClusterLocked(clusterName string) []*Task {
	return append([]*Task(nil), b.tasksByCluster.Get(clusterName)...)
}

// servicesInClusterLocked returns a snapshot copy of every service in clusterName.
func (b *InMemoryBackend) servicesInClusterLocked(clusterName string) []*Service {
	return append([]*Service(nil), b.servicesByCluster.Get(clusterName)...)
}

// containerInstancesInClusterLocked returns a snapshot copy of every container
// instance in clusterName.
func (b *InMemoryBackend) containerInstancesInClusterLocked(clusterName string) []*ContainerInstance {
	return append([]*ContainerInstance(nil), b.containerInstancesByCluster.Get(clusterName)...)
}

// taskSetsForServiceLocked returns a snapshot copy of every task set belonging
// to serviceArn.
func (b *InMemoryBackend) taskSetsForServiceLocked(serviceArn string) []*TaskSet {
	return append([]*TaskSet(nil), b.taskSetsByService.Get(serviceArn)...)
}

// daemonsInClusterLocked returns a snapshot copy of every daemon in clusterName.
func (b *InMemoryBackend) daemonsInClusterLocked(clusterName string) []*Daemon {
	return append([]*Daemon(nil), b.daemonsByCluster.Get(clusterName)...)
}

// deleteServicesForClusterLocked removes every service belonging to clusterName.
func (b *InMemoryBackend) deleteServicesForClusterLocked(clusterName string) {
	for _, s := range b.servicesInClusterLocked(clusterName) {
		b.services.Delete(servicesKeyFn(s))
		b.deleteResourceTagsLocked(s.ServiceArn)
	}
}

// deleteTasksForClusterLocked removes every task belonging to clusterName.
func (b *InMemoryBackend) deleteTasksForClusterLocked(clusterName string) {
	for _, t := range b.tasksInClusterLocked(clusterName) {
		b.tasks.Delete(tasksKeyFn(t))
	}
}

// deleteContainerInstancesForClusterLocked removes every container instance
// belonging to clusterName.
func (b *InMemoryBackend) deleteContainerInstancesForClusterLocked(clusterName string) {
	for _, ci := range b.containerInstancesInClusterLocked(clusterName) {
		b.containerInstances.Delete(containerInstancesKeyFn(ci))
		b.deleteResourceTagsLocked(ci.ContainerInstanceArn)
	}
}

// deleteTaskSetsForServiceLocked removes every task set belonging to serviceArn.
func (b *InMemoryBackend) deleteTaskSetsForServiceLocked(serviceArn string) {
	for _, ts := range b.taskSetsForServiceLocked(serviceArn) {
		b.taskSets.Delete(taskSetsKeyFn(ts))
		b.deleteResourceTagsLocked(ts.TaskSetArn)
	}
}
