package sagemaker

func (b *InMemoryBackend) modelARNIndexStore(r string) map[string]string {
	if b.modelARNIndex[r] == nil {
		b.modelARNIndex[r] = make(map[string]string)
	}

	return b.modelARNIndex[r]
}

// modelARNIndexStoreRO returns the region-scoped modelARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelARNIndexStoreRO(r string) map[string]string {
	if v := b.modelARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) endpointConfigARNIndexStore(r string) map[string]string {
	if b.endpointConfigARNIndex[r] == nil {
		b.endpointConfigARNIndex[r] = make(map[string]string)
	}

	return b.endpointConfigARNIndex[r]
}

// endpointConfigARNIndexStoreRO returns the region-scoped endpointConfigARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointConfigARNIndexStoreRO(r string) map[string]string {
	if v := b.endpointConfigARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) endpointARNIndexStore(r string) map[string]string {
	if b.endpointARNIndex[r] == nil {
		b.endpointARNIndex[r] = make(map[string]string)
	}

	return b.endpointARNIndex[r]
}

// endpointARNIndexStoreRO returns the region-scoped endpointARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointARNIndexStoreRO(r string) map[string]string {
	if v := b.endpointARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) trainingJobARNIndexStore(r string) map[string]string {
	if b.trainingJobARNIndex[r] == nil {
		b.trainingJobARNIndex[r] = make(map[string]string)
	}

	return b.trainingJobARNIndex[r]
}

// trainingJobARNIndexStoreRO returns the region-scoped trainingJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trainingJobARNIndexStoreRO(r string) map[string]string {
	if v := b.trainingJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) notebookARNIndexStore(r string) map[string]string {
	if b.notebookARNIndex[r] == nil {
		b.notebookARNIndex[r] = make(map[string]string)
	}

	return b.notebookARNIndex[r]
}

// notebookARNIndexStoreRO returns the region-scoped notebookARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) notebookARNIndexStoreRO(r string) map[string]string {
	if v := b.notebookARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) hpTuningJobARNIndexStore(r string) map[string]string {
	if b.hpTuningJobARNIndex[r] == nil {
		b.hpTuningJobARNIndex[r] = make(map[string]string)
	}

	return b.hpTuningJobARNIndex[r]
}

// hpTuningJobARNIndexStoreRO returns the region-scoped hpTuningJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) hpTuningJobARNIndexStoreRO(r string) map[string]string {
	if v := b.hpTuningJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) actionARNIndexStore(r string) map[string]string {
	if b.actionARNIndex[r] == nil {
		b.actionARNIndex[r] = make(map[string]string)
	}

	return b.actionARNIndex[r]
}

// actionARNIndexStoreRO returns the region-scoped actionARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) actionARNIndexStoreRO(r string) map[string]string {
	if v := b.actionARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) algorithmARNIndexStore(r string) map[string]string {
	if b.algorithmARNIndex[r] == nil {
		b.algorithmARNIndex[r] = make(map[string]string)
	}

	return b.algorithmARNIndex[r]
}

// algorithmARNIndexStoreRO returns the region-scoped algorithmARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) algorithmARNIndexStoreRO(r string) map[string]string {
	if v := b.algorithmARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) clusterARNIndexStore(r string) map[string]string {
	if b.clusterARNIndex[r] == nil {
		b.clusterARNIndex[r] = make(map[string]string)
	}

	return b.clusterARNIndex[r]
}

// clusterARNIndexStoreRO returns the region-scoped clusterARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) clusterARNIndexStoreRO(r string) map[string]string {
	if v := b.clusterARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) modelPackageARNIndexStore(r string) map[string]string {
	if b.modelPackageARNIndex[r] == nil {
		b.modelPackageARNIndex[r] = make(map[string]string)
	}

	return b.modelPackageARNIndex[r]
}

// modelPackageARNIndexStoreRO returns the region-scoped modelPackageARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackageARNIndexStoreRO(r string) map[string]string {
	if v := b.modelPackageARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) processingJobARNIndexStore(r string) map[string]string {
	if b.processingJobARNIndex[r] == nil {
		b.processingJobARNIndex[r] = make(map[string]string)
	}

	return b.processingJobARNIndex[r]
}

// processingJobARNIndexStoreRO returns the region-scoped processingJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) processingJobARNIndexStoreRO(r string) map[string]string {
	if v := b.processingJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) transformJobARNIndexStore(r string) map[string]string {
	if b.transformJobARNIndex[r] == nil {
		b.transformJobARNIndex[r] = make(map[string]string)
	}

	return b.transformJobARNIndex[r]
}

// transformJobARNIndexStoreRO returns the region-scoped transformJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) transformJobARNIndexStoreRO(r string) map[string]string {
	if v := b.transformJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) modelPackageGroupARNIndexStore(r string) map[string]string {
	if b.modelPackageGroupARNIndex[r] == nil {
		b.modelPackageGroupARNIndex[r] = make(map[string]string)
	}

	return b.modelPackageGroupARNIndex[r]
}

// modelPackageGroupARNIndexStoreRO returns the region-scoped modelPackageGroupARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackageGroupARNIndexStoreRO(r string) map[string]string {
	if v := b.modelPackageGroupARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) workteamARNIndexStore(r string) map[string]string {
	if b.workteamARNIndex[r] == nil {
		b.workteamARNIndex[r] = make(map[string]string)
	}

	return b.workteamARNIndex[r]
}

// workteamARNIndexStoreRO returns the region-scoped workteamARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) workteamARNIndexStoreRO(r string) map[string]string {
	if v := b.workteamARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) workforceARNIndexStore(r string) map[string]string {
	if b.workforceARNIndex[r] == nil {
		b.workforceARNIndex[r] = make(map[string]string)
	}

	return b.workforceARNIndex[r]
}

// workforceARNIndexStoreRO returns the region-scoped workforceARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) workforceARNIndexStoreRO(r string) map[string]string {
	if v := b.workforceARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}
