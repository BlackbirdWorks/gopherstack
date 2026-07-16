package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) dataQualityJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.dataQualityJobDefs[r] == nil {
		b.dataQualityJobDefs[r] = store.Register(
			b.registry,
			"dataQualityJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.dataQualityJobDefs[r]
}

// dataQualityJobDefsStoreRO returns the region-scoped dataQualityJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) dataQualityJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.dataQualityJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) modelBiasJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.modelBiasJobDefs[r] == nil {
		b.modelBiasJobDefs[r] = store.Register(
			b.registry,
			"modelBiasJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.modelBiasJobDefs[r]
}

// modelBiasJobDefsStoreRO returns the region-scoped modelBiasJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelBiasJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.modelBiasJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) modelQualityJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.modelQualityJobDefs[r] == nil {
		b.modelQualityJobDefs[r] = store.Register(
			b.registry,
			"modelQualityJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.modelQualityJobDefs[r]
}

// modelQualityJobDefsStoreRO returns the region-scoped modelQualityJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelQualityJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.modelQualityJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) modelExplainJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.modelExplainJobDefs[r] == nil {
		b.modelExplainJobDefs[r] = store.Register(
			b.registry,
			"modelExplainJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.modelExplainJobDefs[r]
}

// modelExplainJobDefsStoreRO returns the region-scoped modelExplainJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelExplainJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.modelExplainJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) monitoringExecutionsStore(r string) *store.Table[MonitoringExecution] {
	if b.monitoringExecutions[r] == nil {
		b.monitoringExecutions[r] = store.Register(
			b.registry,
			"monitoringExecutions:"+r,
			store.New(
				func(v *MonitoringExecution) string { return v.MonitoringScheduleName + "|" + v.ProcessingJobArn },
			),
		)
	}

	return b.monitoringExecutions[r]
}

// monitoringExecutionsStoreRO returns the region-scoped monitoringExecutions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) monitoringExecutionsStoreRO(r string) *store.Table[MonitoringExecution] {
	if v := b.monitoringExecutions[r]; v != nil {
		return v
	}

	return store.New(
		func(v *MonitoringExecution) string { return v.MonitoringScheduleName + "|" + v.ProcessingJobArn },
	)
}

func (b *InMemoryBackend) humanTaskUisStore(r string) *store.Table[HumanTaskUI] {
	if b.humanTaskUis[r] == nil {
		b.humanTaskUis[r] = store.Register(
			b.registry,
			"humanTaskUis:"+r,
			store.New(func(v *HumanTaskUI) string { return v.HumanTaskUIName }),
		)
	}

	return b.humanTaskUis[r]
}

// humanTaskUisStoreRO returns the region-scoped humanTaskUis table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) humanTaskUisStoreRO(r string) *store.Table[HumanTaskUI] {
	if v := b.humanTaskUis[r]; v != nil {
		return v
	}

	return store.New(func(v *HumanTaskUI) string { return v.HumanTaskUIName })
}

func (b *InMemoryBackend) workforcesStore(r string) *store.Table[Workforce] {
	if b.workforces[r] == nil {
		b.workforces[r] = store.Register(
			b.registry,
			"workforces:"+r,
			store.New(func(v *Workforce) string { return v.WorkforceName }),
		)
	}

	return b.workforces[r]
}

// workforcesStoreRO returns the region-scoped workforces table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) workforcesStoreRO(r string) *store.Table[Workforce] {
	if v := b.workforces[r]; v != nil {
		return v
	}

	return store.New(func(v *Workforce) string { return v.WorkforceName })
}

func (b *InMemoryBackend) flowDefinitionsStore(r string) *store.Table[FlowDefinition] {
	if b.flowDefinitions[r] == nil {
		b.flowDefinitions[r] = store.Register(
			b.registry,
			"flowDefinitions:"+r,
			store.New(func(v *FlowDefinition) string { return v.FlowDefinitionName }),
		)
	}

	return b.flowDefinitions[r]
}

// flowDefinitionsStoreRO returns the region-scoped flowDefinitions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) flowDefinitionsStoreRO(r string) *store.Table[FlowDefinition] {
	if v := b.flowDefinitions[r]; v != nil {
		return v
	}

	return store.New(func(v *FlowDefinition) string { return v.FlowDefinitionName })
}

func (b *InMemoryBackend) appImageConfigsStore(r string) *store.Table[AppImageConfig] {
	if b.appImageConfigs[r] == nil {
		b.appImageConfigs[r] = store.Register(
			b.registry,
			"appImageConfigs:"+r,
			store.New(func(v *AppImageConfig) string { return v.AppImageConfigName }),
		)
	}

	return b.appImageConfigs[r]
}

// appImageConfigsStoreRO returns the region-scoped appImageConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) appImageConfigsStoreRO(r string) *store.Table[AppImageConfig] {
	if v := b.appImageConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *AppImageConfig) string { return v.AppImageConfigName })
}
