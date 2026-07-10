package mediastore

// Code in this file supports Phase 3.3 of the datalayer refactor: converting
// InMemoryBackend's containers map to pkgs/store. See pkgs/store's package
// doc and the services/sqs pilot (commit 0f09d77c) / services/ec2 rollout
// (commit 12e611a4) for the general pattern, and services/elasticache for
// the region-nested-map variant this backend follows.
//
// containers is nested per-region (map[string]map[string]*Container -- outer
// key is region) because same-named containers in different regions must be
// fully isolated: MediaStore container names are unique only within a
// region, not globally, and every access is already region-scoped via
// regionFromContext. It converts to map[string]*store.Table[Container]
// (region -> Table) with a lazy per-region accessor (containersStore in
// backend.go) plus a non-creating read accessor (containerRegion, used by
// the RLock-held read paths so they never allocate a Table under a shared
// lock), matching services/elasticache: the set of regions is only known at
// runtime, so these per-region tables are NOT registered on a
// *store.Registry (Registry.SnapshotAll/RestoreAll require a fixed,
// construction-time-known table-name set). persistence.go instead
// snapshots/restores each per-region Table directly via
// Table.Snapshot()/Table.Restore().
//
// Container's per-container policy/CORS/lifecycle/metric-policy state
// (ContainerPolicy, CorsPolicy, LifecyclePolicy, MetricPolicy) already lives
// as fields on Container itself, not as separate maps keyed by container
// name, so there is nothing else in this backend to convert.
func containerKeyFn(v *Container) string { return v.Name }
