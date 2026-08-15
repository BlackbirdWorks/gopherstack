package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) domainsStore(r string) *store.Table[Domain] {
	if b.domains[r] == nil {
		b.domains[r] = store.Register(b.registry, "domains:"+r, store.New(func(v *Domain) string { return v.DomainID }))
	}

	return b.domains[r]
}

// domainsStoreRO returns the region-scoped domains table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) domainsStoreRO(r string) *store.Table[Domain] {
	if v := b.domains[r]; v != nil {
		return v
	}

	return store.New(func(v *Domain) string { return v.DomainID })
}

func (b *InMemoryBackend) userProfilesStore(r string) *store.Table[UserProfile] {
	if b.userProfiles[r] == nil {
		b.userProfiles[r] = store.Register(b.registry, "userProfiles:"+r, store.New(func(v *UserProfile) string {
			return userProfileKeyString(userProfileKey{DomainID: v.DomainID, UserProfileName: v.UserProfileName})
		}))
	}

	return b.userProfiles[r]
}

// userProfilesStoreRO returns the region-scoped userProfiles table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) userProfilesStoreRO(r string) *store.Table[UserProfile] {
	if v := b.userProfiles[r]; v != nil {
		return v
	}

	return store.New(func(v *UserProfile) string {
		return userProfileKeyString(userProfileKey{DomainID: v.DomainID, UserProfileName: v.UserProfileName})
	})
}

func (b *InMemoryBackend) appsStore(r string) *store.Table[App] {
	if b.apps[r] == nil {
		b.apps[r] = store.Register(b.registry, "apps:"+r, store.New(func(v *App) string {
			return appKeyString(
				appKey{
					DomainID:        v.DomainID,
					UserProfileName: v.UserProfileName,
					SpaceName:       v.SpaceName,
					AppType:         v.AppType,
					AppName:         v.AppName,
				},
			)
		}))
	}

	return b.apps[r]
}

// appsStoreRO returns the region-scoped apps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) appsStoreRO(r string) *store.Table[App] {
	if v := b.apps[r]; v != nil {
		return v
	}

	return store.New(func(v *App) string {
		return appKeyString(
			appKey{
				DomainID:        v.DomainID,
				UserProfileName: v.UserProfileName,
				SpaceName:       v.SpaceName,
				AppType:         v.AppType,
				AppName:         v.AppName,
			},
		)
	})
}

func (b *InMemoryBackend) featureGroupsStore(r string) *store.Table[FeatureGroup] {
	if b.featureGroups[r] == nil {
		b.featureGroups[r] = store.Register(
			b.registry,
			"featureGroups:"+r,
			store.New(func(v *FeatureGroup) string { return v.FeatureGroupName }),
		)
	}

	return b.featureGroups[r]
}

// featureGroupsStoreRO returns the region-scoped featureGroups table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) featureGroupsStoreRO(r string) *store.Table[FeatureGroup] {
	if v := b.featureGroups[r]; v != nil {
		return v
	}

	return store.New(func(v *FeatureGroup) string { return v.FeatureGroupName })
}

func (b *InMemoryBackend) featureRecordsStore(r string) *store.Table[FeatureRecord] {
	if b.featureRecords[r] == nil {
		b.featureRecords[r] = store.Register(
			b.registry,
			"featureRecords:"+r,
			store.New(func(v *FeatureRecord) string { return v.Key }),
		)
	}

	return b.featureRecords[r]
}

// featureRecordsStoreRO returns the region-scoped featureRecords table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) featureRecordsStoreRO(r string) *store.Table[FeatureRecord] {
	if v := b.featureRecords[r]; v != nil {
		return v
	}

	return store.New(func(v *FeatureRecord) string { return v.Key })
}

func (b *InMemoryBackend) featureMetadataStore(r string) *store.Table[FeatureMetadata] {
	if b.featureMetadata[r] == nil {
		b.featureMetadata[r] = store.Register(
			b.registry,
			"featureMetadata:"+r,
			store.New(func(v *FeatureMetadata) string { return featureMetaKey(v.GroupName, v.FeatureName) }),
		)
	}

	return b.featureMetadata[r]
}

// featureMetadataStoreRO returns the region-scoped featureMetadata table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) featureMetadataStoreRO(r string) *store.Table[FeatureMetadata] {
	if v := b.featureMetadata[r]; v != nil {
		return v
	}

	return store.New(func(v *FeatureMetadata) string { return featureMetaKey(v.GroupName, v.FeatureName) })
}

func (b *InMemoryBackend) pipelinesStore(r string) *store.Table[Pipeline] {
	if b.pipelines[r] == nil {
		b.pipelines[r] = store.Register(
			b.registry,
			"pipelines:"+r,
			store.New(func(v *Pipeline) string { return v.PipelineName }),
		)
	}

	return b.pipelines[r]
}

// pipelinesStoreRO returns the region-scoped pipelines table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) pipelinesStoreRO(r string) *store.Table[Pipeline] {
	if v := b.pipelines[r]; v != nil {
		return v
	}

	return store.New(func(v *Pipeline) string { return v.PipelineName })
}

func (b *InMemoryBackend) pipelineExecutionsStore(r string) *store.Table[PipelineExecution] {
	if b.pipelineExecutions[r] == nil {
		b.pipelineExecutions[r] = store.Register(
			b.registry,
			"pipelineExecutions:"+r,
			store.New(func(v *PipelineExecution) string { return v.PipelineExecutionArn }),
		)
	}

	return b.pipelineExecutions[r]
}

// pipelineExecutionsStoreRO returns the region-scoped pipelineExecutions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) pipelineExecutionsStoreRO(r string) *store.Table[PipelineExecution] {
	if v := b.pipelineExecutions[r]; v != nil {
		return v
	}

	return store.New(func(v *PipelineExecution) string { return v.PipelineExecutionArn })
}

func (b *InMemoryBackend) pipelineExecStepsStore(r string) *store.Table[PipelineExecutionStep] {
	if b.pipelineExecSteps[r] == nil {
		b.pipelineExecSteps[r] = store.Register(
			b.registry,
			"pipelineExecSteps:"+r,
			store.New(
				func(v *PipelineExecutionStep) string { return pipelineExecutionStepsKey(v.ExecutionArn, v.StepName) },
			),
		)
	}

	return b.pipelineExecSteps[r]
}

// pipelineExecStepsStoreRO returns the region-scoped pipelineExecSteps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) pipelineExecStepsStoreRO(r string) *store.Table[PipelineExecutionStep] {
	if v := b.pipelineExecSteps[r]; v != nil {
		return v
	}

	return store.New(
		func(v *PipelineExecutionStep) string { return pipelineExecutionStepsKey(v.ExecutionArn, v.StepName) },
	)
}

func (b *InMemoryBackend) experimentsStore(r string) *store.Table[Experiment] {
	if b.experiments[r] == nil {
		b.experiments[r] = store.Register(
			b.registry,
			"experiments:"+r,
			store.New(func(v *Experiment) string { return v.ExperimentName }),
		)
	}

	return b.experiments[r]
}

// experimentsStoreRO returns the region-scoped experiments table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) experimentsStoreRO(r string) *store.Table[Experiment] {
	if v := b.experiments[r]; v != nil {
		return v
	}

	return store.New(func(v *Experiment) string { return v.ExperimentName })
}

func (b *InMemoryBackend) trialsStore(r string) *store.Table[Trial] {
	if b.trials[r] == nil {
		b.trials[r] = store.Register(b.registry, "trials:"+r, store.New(func(v *Trial) string { return v.TrialName }))
	}

	return b.trials[r]
}

// trialsStoreRO returns the region-scoped trials table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trialsStoreRO(r string) *store.Table[Trial] {
	if v := b.trials[r]; v != nil {
		return v
	}

	return store.New(func(v *Trial) string { return v.TrialName })
}

func (b *InMemoryBackend) trialComponentsStore(r string) *store.Table[TrialComponent] {
	if b.trialComponents[r] == nil {
		b.trialComponents[r] = store.Register(
			b.registry,
			"trialComponents:"+r,
			store.New(func(v *TrialComponent) string { return v.TrialComponentName }),
		)
	}

	return b.trialComponents[r]
}

// trialComponentsStoreRO returns the region-scoped trialComponents table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trialComponentsStoreRO(r string) *store.Table[TrialComponent] {
	if v := b.trialComponents[r]; v != nil {
		return v
	}

	return store.New(func(v *TrialComponent) string { return v.TrialComponentName })
}

func (b *InMemoryBackend) notebookLifecycleConfigsStore(r string) *store.Table[NotebookInstanceLifecycleConfig] {
	if b.notebookLifecycleConfigs[r] == nil {
		b.notebookLifecycleConfigs[r] = store.Register(
			b.registry,
			"notebookLifecycleConfigs:"+r,
			store.New(func(v *NotebookInstanceLifecycleConfig) string { return v.Name }),
		)
	}

	return b.notebookLifecycleConfigs[r]
}

// notebookLifecycleConfigsStoreRO returns the region-scoped notebookLifecycleConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) notebookLifecycleConfigsStoreRO(r string) *store.Table[NotebookInstanceLifecycleConfig] {
	if v := b.notebookLifecycleConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *NotebookInstanceLifecycleConfig) string { return v.Name })
}
