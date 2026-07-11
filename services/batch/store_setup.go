package batch

// Code in this file supports Phase 3.3 of the datalayer refactor: eight
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*Job) are each replaced by a single flat
// *store.Table[T], keyed by the composite "region|id" string (see regionKey
// in backend.go), with a companion *store.Index grouping entries by region
// for per-region scans -- the region-qualified-table pattern services/emr and
// services/mwaa use. jobs additionally carries a "byARN" index (replacing the
// old jobsByARN region-nested map) and a "byQueue" index keyed by
// (region, JobQueue) (replacing the old jobsByQueue region-nested map);
// unlike the byARN/byRegion indexes, byQueue keys off Job's own already
// -exported JobQueue field rather than a hidden one, since AWS Batch's real
// job shape already carries the owning queue name.
//
// jobDefRevisions (a per-region name -> revision-counter map, not a *T map)
// and the four dead-for-reads/non-regional maps (cesByARN, jqsByARN, crsByARN,
// ceToQueues) are deliberately NOT converted here -- see the InMemoryBackend
// doc comment in backend.go for why.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func computeEnvironmentKeyFn(v *ComputeEnvironment) string {
	return regionKey(v.region, v.ComputeEnvironmentName)
}
func computeEnvironmentRegionIndexKeyFn(v *ComputeEnvironment) string { return v.region }

func jobQueueKeyFn(v *JobQueue) string            { return regionKey(v.region, v.JobQueueName) }
func jobQueueRegionIndexKeyFn(v *JobQueue) string { return v.region }

func jobDefinitionKeyFn(v *JobDefinition) string            { return regionKey(v.region, v.JobDefinitionArn) }
func jobDefinitionRegionIndexKeyFn(v *JobDefinition) string { return v.region }

func jobKeyFn(v *Job) string            { return regionKey(v.region, v.JobID) }
func jobRegionIndexKeyFn(v *Job) string { return v.region }
func jobARNIndexKeyFn(v *Job) string    { return regionKey(v.region, v.JobARN) }
func jobQueueIndexKeyFn(v *Job) string  { return regionKey(v.region, v.JobQueue) }

func consumableResourceKeyFn(v *ConsumableResource) string {
	return regionKey(v.region, v.ConsumableResourceName)
}
func consumableResourceRegionIndexKeyFn(v *ConsumableResource) string { return v.region }

func schedulingPolicyKeyFn(v *SchedulingPolicy) string            { return regionKey(v.region, v.Arn) }
func schedulingPolicyRegionIndexKeyFn(v *SchedulingPolicy) string { return v.region }
func schedulingPolicyNameIndexKeyFn(v *SchedulingPolicy) string   { return regionKey(v.region, v.Name) }

func serviceEnvironmentKeyFn(v *ServiceEnvironment) string {
	return regionKey(v.region, v.ServiceEnvironmentName)
}
func serviceEnvironmentRegionIndexKeyFn(v *ServiceEnvironment) string { return v.region }

func serviceJobKeyFn(v *ServiceJob) string            { return regionKey(v.region, v.ServiceJobID) }
func serviceJobRegionIndexKeyFn(v *ServiceJob) string { return v.region }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.computeEnvironments = store.Register(b.registry, "computeEnvironments", store.New(computeEnvironmentKeyFn))
	b.computeEnvironmentsByRegion = b.computeEnvironments.AddIndex("byRegion", computeEnvironmentRegionIndexKeyFn)

	b.jobQueues = store.Register(b.registry, "jobQueues", store.New(jobQueueKeyFn))
	b.jobQueuesByRegion = b.jobQueues.AddIndex("byRegion", jobQueueRegionIndexKeyFn)

	b.jobDefinitions = store.Register(b.registry, "jobDefinitions", store.New(jobDefinitionKeyFn))
	b.jobDefinitionsByRegion = b.jobDefinitions.AddIndex("byRegion", jobDefinitionRegionIndexKeyFn)

	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
	b.jobsByRegion = b.jobs.AddIndex("byRegion", jobRegionIndexKeyFn)
	b.jobsByARN = b.jobs.AddIndex("byARN", jobARNIndexKeyFn)
	b.jobsByQueueIdx = b.jobs.AddIndex("byQueue", jobQueueIndexKeyFn)

	b.consumableResources = store.Register(b.registry, "consumableResources", store.New(consumableResourceKeyFn))
	b.consumableResourcesByRegion = b.consumableResources.AddIndex("byRegion", consumableResourceRegionIndexKeyFn)

	b.schedulingPolicies = store.Register(b.registry, "schedulingPolicies", store.New(schedulingPolicyKeyFn))
	b.schedulingPoliciesByRegion = b.schedulingPolicies.AddIndex("byRegion", schedulingPolicyRegionIndexKeyFn)
	b.schedulingPoliciesByName = b.schedulingPolicies.AddIndex("byName", schedulingPolicyNameIndexKeyFn)

	b.serviceEnvironments = store.Register(b.registry, "serviceEnvironments", store.New(serviceEnvironmentKeyFn))
	b.serviceEnvironmentsByRegion = b.serviceEnvironments.AddIndex("byRegion", serviceEnvironmentRegionIndexKeyFn)

	b.serviceJobs = store.Register(b.registry, "serviceJobs", store.New(serviceJobKeyFn))
	b.serviceJobsByRegion = b.serviceJobs.AddIndex("byRegion", serviceJobRegionIndexKeyFn)
}
