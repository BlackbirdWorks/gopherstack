package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) inferenceExperimentsStore(r string) *store.Table[InferenceExperiment] {
	if b.inferenceExperiments[r] == nil {
		b.inferenceExperiments[r] = store.Register(
			b.registry,
			"inferenceExperiments:"+r,
			store.New(func(v *InferenceExperiment) string { return v.Name }),
		)
	}

	return b.inferenceExperiments[r]
}

// inferenceExperimentsStoreRO returns the region-scoped inferenceExperiments table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) inferenceExperimentsStoreRO(r string) *store.Table[InferenceExperiment] {
	if v := b.inferenceExperiments[r]; v != nil {
		return v
	}

	return store.New(func(v *InferenceExperiment) string { return v.Name })
}

func (b *InMemoryBackend) mlflowTrackingServersStore(r string) *store.Table[MlflowTrackingServer] {
	if b.mlflowTrackingServers[r] == nil {
		b.mlflowTrackingServers[r] = store.Register(
			b.registry,
			"mlflowTrackingServers:"+r,
			store.New(func(v *MlflowTrackingServer) string { return v.TrackingServerName }),
		)
	}

	return b.mlflowTrackingServers[r]
}

// mlflowTrackingServersStoreRO returns the region-scoped mlflowTrackingServers table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) mlflowTrackingServersStoreRO(r string) *store.Table[MlflowTrackingServer] {
	if v := b.mlflowTrackingServers[r]; v != nil {
		return v
	}

	return store.New(func(v *MlflowTrackingServer) string { return v.TrackingServerName })
}

func (b *InMemoryBackend) mlflowAppsStore(r string) *store.Table[MlflowApp] {
	if b.mlflowApps[r] == nil {
		b.mlflowApps[r] = store.Register(
			b.registry,
			"mlflowApps:"+r,
			store.New(func(v *MlflowApp) string { return v.Arn }),
		)
	}

	return b.mlflowApps[r]
}

// mlflowAppsStoreRO returns the region-scoped mlflowApps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) mlflowAppsStoreRO(r string) *store.Table[MlflowApp] {
	if v := b.mlflowApps[r]; v != nil {
		return v
	}

	return store.New(func(v *MlflowApp) string { return v.Arn })
}

func (b *InMemoryBackend) modelCardsStore(r string) *store.Table[ModelCard] {
	if b.modelCards[r] == nil {
		b.modelCards[r] = store.Register(
			b.registry,
			"modelCards:"+r,
			store.New(func(v *ModelCard) string { return v.ModelCardName }),
		)
	}

	return b.modelCards[r]
}

// modelCardsStoreRO returns the region-scoped modelCards table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelCardsStoreRO(r string) *store.Table[ModelCard] {
	if v := b.modelCards[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelCard) string { return v.ModelCardName })
}

func (b *InMemoryBackend) optimizationJobsStore(r string) *store.Table[OptimizationJob] {
	if b.optimizationJobs[r] == nil {
		b.optimizationJobs[r] = store.Register(
			b.registry,
			"optimizationJobs:"+r,
			store.New(func(v *OptimizationJob) string { return v.OptimizationJobName }),
		)
	}

	return b.optimizationJobs[r]
}

// optimizationJobsStoreRO returns the region-scoped optimizationJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) optimizationJobsStoreRO(r string) *store.Table[OptimizationJob] {
	if v := b.optimizationJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *OptimizationJob) string { return v.OptimizationJobName })
}

func (b *InMemoryBackend) studioLifecycleConfigsStore(r string) *store.Table[StudioLifecycleConfig] {
	if b.studioLifecycleConfigs[r] == nil {
		b.studioLifecycleConfigs[r] = store.Register(
			b.registry,
			"studioLifecycleConfigs:"+r,
			store.New(func(v *StudioLifecycleConfig) string { return v.StudioLifecycleConfigName }),
		)
	}

	return b.studioLifecycleConfigs[r]
}

// studioLifecycleConfigsStoreRO returns the region-scoped studioLifecycleConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) studioLifecycleConfigsStoreRO(r string) *store.Table[StudioLifecycleConfig] {
	if v := b.studioLifecycleConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *StudioLifecycleConfig) string { return v.StudioLifecycleConfigName })
}

func (b *InMemoryBackend) partnerAppsStore(r string) *store.Table[PartnerApp] {
	if b.partnerApps[r] == nil {
		b.partnerApps[r] = store.Register(
			b.registry,
			"partnerApps:"+r,
			store.New(func(v *PartnerApp) string { return v.Arn }),
		)
	}

	return b.partnerApps[r]
}

// partnerAppsStoreRO returns the region-scoped partnerApps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) partnerAppsStoreRO(r string) *store.Table[PartnerApp] {
	if v := b.partnerApps[r]; v != nil {
		return v
	}

	return store.New(func(v *PartnerApp) string { return v.Arn })
}

func (b *InMemoryBackend) trainingPlansStore(r string) *store.Table[TrainingPlan] {
	if b.trainingPlans[r] == nil {
		b.trainingPlans[r] = store.Register(
			b.registry,
			"trainingPlans:"+r,
			store.New(func(v *TrainingPlan) string { return v.TrainingPlanName }),
		)
	}

	return b.trainingPlans[r]
}

// trainingPlansStoreRO returns the region-scoped trainingPlans table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trainingPlansStoreRO(r string) *store.Table[TrainingPlan] {
	if v := b.trainingPlans[r]; v != nil {
		return v
	}

	return store.New(func(v *TrainingPlan) string { return v.TrainingPlanName })
}

func (b *InMemoryBackend) reservedCapacitiesStore(r string) *store.Table[ReservedCapacity] {
	if b.reservedCapacities[r] == nil {
		b.reservedCapacities[r] = store.Register(
			b.registry,
			"reservedCapacities:"+r,
			store.New(func(v *ReservedCapacity) string { return v.ReservedCapacityArn }),
		)
	}

	return b.reservedCapacities[r]
}

// reservedCapacitiesStoreRO returns the region-scoped reservedCapacities table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) reservedCapacitiesStoreRO(r string) *store.Table[ReservedCapacity] {
	if v := b.reservedCapacities[r]; v != nil {
		return v
	}

	return store.New(func(v *ReservedCapacity) string { return v.ReservedCapacityArn })
}

func (b *InMemoryBackend) trainingPlanExtensionOfferingsStore(r string) *store.Table[pendingTrainingPlanExtension] {
	if b.trainingPlanExtensionOfferings[r] == nil {
		b.trainingPlanExtensionOfferings[r] = store.Register(
			b.registry,
			"trainingPlanExtensionOfferings:"+r,
			store.New(func(v *pendingTrainingPlanExtension) string { return v.ID }),
		)
	}

	return b.trainingPlanExtensionOfferings[r]
}

func (b *InMemoryBackend) modelCardExportJobsStore(r string) *store.Table[ModelCardExportJob] {
	if b.modelCardExportJobs[r] == nil {
		b.modelCardExportJobs[r] = store.Register(
			b.registry,
			"modelCardExportJobs:"+r,
			store.New(func(v *ModelCardExportJob) string { return v.ModelCardExportJobArn }),
		)
	}

	return b.modelCardExportJobs[r]
}

// modelCardExportJobsStoreRO returns the region-scoped modelCardExportJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelCardExportJobsStoreRO(r string) *store.Table[ModelCardExportJob] {
	if v := b.modelCardExportJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelCardExportJob) string { return v.ModelCardExportJobArn })
}

// initTrainingPlanExtMaps (re)initialises the Training Plan / Reserved
// Capacity and ModelCard export job top-level maps. Shared by the
// constructor and Reset to keep both call sites short.
// initTrainingPlanExtMaps initialises the Training Plan / Reserved Capacity /
// ModelCard export job maps, plus the ModelPackageGroup policy / Servicecatalog
// portfolio / Pipeline version state introduced in a later de-stubbing round.
func (b *InMemoryBackend) initTrainingPlanExtMaps() {
	b.reservedCapacities = make(map[string]*store.Table[ReservedCapacity])
	b.trainingPlanExtensionOfferings = make(map[string]*store.Table[pendingTrainingPlanExtension])
	b.modelCardExportJobs = make(map[string]*store.Table[ModelCardExportJob])
	b.pipelineVersions = make(map[string]map[string][]*PipelineVersion)
	b.servicecatalogPortfolioEnabled = make(map[string]bool)
}
