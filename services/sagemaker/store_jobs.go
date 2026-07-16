package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) processingJobsStore(r string) *store.Table[ProcessingJob] {
	if b.processingJobs[r] == nil {
		b.processingJobs[r] = store.Register(
			b.registry,
			"processingJobs:"+r,
			store.New(func(v *ProcessingJob) string { return v.ProcessingJobName }),
		)
	}

	return b.processingJobs[r]
}

// processingJobsStoreRO returns the region-scoped processingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) processingJobsStoreRO(r string) *store.Table[ProcessingJob] {
	if v := b.processingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *ProcessingJob) string { return v.ProcessingJobName })
}

func (b *InMemoryBackend) transformJobsStore(r string) *store.Table[TransformJob] {
	if b.transformJobs[r] == nil {
		b.transformJobs[r] = store.Register(
			b.registry,
			"transformJobs:"+r,
			store.New(func(v *TransformJob) string { return v.TransformJobName }),
		)
	}

	return b.transformJobs[r]
}

// transformJobsStoreRO returns the region-scoped transformJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) transformJobsStoreRO(r string) *store.Table[TransformJob] {
	if v := b.transformJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *TransformJob) string { return v.TransformJobName })
}

func (b *InMemoryBackend) edgePackagingJobsStore(r string) *store.Table[EdgePackagingJob] {
	if b.edgePackagingJobs[r] == nil {
		b.edgePackagingJobs[r] = store.Register(
			b.registry,
			"edgePackagingJobs:"+r,
			store.New(func(v *EdgePackagingJob) string { return v.EdgePackagingJobName }),
		)
	}

	return b.edgePackagingJobs[r]
}

// edgePackagingJobsStoreRO returns the region-scoped edgePackagingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) edgePackagingJobsStoreRO(r string) *store.Table[EdgePackagingJob] {
	if v := b.edgePackagingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *EdgePackagingJob) string { return v.EdgePackagingJobName })
}

func (b *InMemoryBackend) edgeDeploymentPlansStore(r string) *store.Table[EdgeDeploymentPlan] {
	if b.edgeDeploymentPlans[r] == nil {
		b.edgeDeploymentPlans[r] = store.Register(
			b.registry,
			"edgeDeploymentPlans:"+r,
			store.New(func(v *EdgeDeploymentPlan) string { return v.EdgeDeploymentPlanName }),
		)
	}

	return b.edgeDeploymentPlans[r]
}

// edgeDeploymentPlansStoreRO returns the region-scoped edgeDeploymentPlans table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) edgeDeploymentPlansStoreRO(r string) *store.Table[EdgeDeploymentPlan] {
	if v := b.edgeDeploymentPlans[r]; v != nil {
		return v
	}

	return store.New(func(v *EdgeDeploymentPlan) string { return v.EdgeDeploymentPlanName })
}

func (b *InMemoryBackend) inferenceRecommendationsJobsStore(r string) *store.Table[InferenceRecommendationsJob] {
	if b.inferenceRecommendationsJobs[r] == nil {
		b.inferenceRecommendationsJobs[r] = store.Register(
			b.registry,
			"inferenceRecommendationsJobs:"+r,
			store.New(func(v *InferenceRecommendationsJob) string { return v.JobName }),
		)
	}

	return b.inferenceRecommendationsJobs[r]
}

// inferenceRecommendationsJobsStoreRO returns the region-scoped
// inferenceRecommendationsJobs table for r without mutating the outer map.
// Safe to call while holding only b.mu.RLock(): if the region has not been
// observed yet, it returns a fresh, unregistered, empty view instead of
// lazily creating (and persisting) an entry.
func (b *InMemoryBackend) inferenceRecommendationsJobsStoreRO(r string) *store.Table[InferenceRecommendationsJob] {
	if v := b.inferenceRecommendationsJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *InferenceRecommendationsJob) string { return v.JobName })
}

func (b *InMemoryBackend) deviceFleetsStore(r string) *store.Table[DeviceFleet] {
	if b.deviceFleets[r] == nil {
		b.deviceFleets[r] = store.Register(
			b.registry,
			"deviceFleets:"+r,
			store.New(func(v *DeviceFleet) string { return v.DeviceFleetName }),
		)
	}

	return b.deviceFleets[r]
}

// deviceFleetsStoreRO returns the region-scoped deviceFleets table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) deviceFleetsStoreRO(r string) *store.Table[DeviceFleet] {
	if v := b.deviceFleets[r]; v != nil {
		return v
	}

	return store.New(func(v *DeviceFleet) string { return v.DeviceFleetName })
}

func (b *InMemoryBackend) devicesStore(r string) *store.Table[Device] {
	if b.devices[r] == nil {
		b.devices[r] = store.Register(b.registry, "devices:"+r, store.New(func(v *Device) string {
			return deviceKeyString(deviceKey{fleetName: v.DeviceFleetName, deviceName: v.DeviceName})
		}))
	}

	return b.devices[r]
}

// devicesStoreRO returns the region-scoped devices table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) devicesStoreRO(r string) *store.Table[Device] {
	if v := b.devices[r]; v != nil {
		return v
	}

	return store.New(func(v *Device) string {
		return deviceKeyString(deviceKey{fleetName: v.DeviceFleetName, deviceName: v.DeviceName})
	})
}

func (b *InMemoryBackend) inferenceComponentsStore(r string) *store.Table[InferenceComponent] {
	if b.inferenceComponents[r] == nil {
		b.inferenceComponents[r] = store.Register(
			b.registry,
			"inferenceComponents:"+r,
			store.New(func(v *InferenceComponent) string { return v.InferenceComponentName }),
		)
	}

	return b.inferenceComponents[r]
}

// inferenceComponentsStoreRO returns the region-scoped inferenceComponents table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) inferenceComponentsStoreRO(r string) *store.Table[InferenceComponent] {
	if v := b.inferenceComponents[r]; v != nil {
		return v
	}

	return store.New(func(v *InferenceComponent) string { return v.InferenceComponentName })
}

func (b *InMemoryBackend) clusterSchedulerConfigsStore(r string) *store.Table[ClusterSchedulerConfig] {
	if b.clusterSchedulerConfigs[r] == nil {
		b.clusterSchedulerConfigs[r] = store.Register(
			b.registry,
			"clusterSchedulerConfigs:"+r,
			store.New(func(v *ClusterSchedulerConfig) string { return v.ClusterSchedulerConfigName }),
		)
	}

	return b.clusterSchedulerConfigs[r]
}

// clusterSchedulerConfigsStoreRO returns the region-scoped clusterSchedulerConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) clusterSchedulerConfigsStoreRO(r string) *store.Table[ClusterSchedulerConfig] {
	if v := b.clusterSchedulerConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *ClusterSchedulerConfig) string { return v.ClusterSchedulerConfigName })
}

func (b *InMemoryBackend) computeQuotasStore(r string) *store.Table[ComputeQuota] {
	if b.computeQuotas[r] == nil {
		b.computeQuotas[r] = store.Register(
			b.registry,
			"computeQuotas:"+r,
			store.New(func(v *ComputeQuota) string { return v.ComputeQuotaName }),
		)
	}

	return b.computeQuotas[r]
}

// computeQuotasStoreRO returns the region-scoped computeQuotas table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) computeQuotasStoreRO(r string) *store.Table[ComputeQuota] {
	if v := b.computeQuotas[r]; v != nil {
		return v
	}

	return store.New(func(v *ComputeQuota) string { return v.ComputeQuotaName })
}
