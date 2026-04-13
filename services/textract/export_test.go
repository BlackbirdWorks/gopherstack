package textract

// NewInMemoryBackendWithCap creates a backend with a custom job history cap for testing.
func NewInMemoryBackendWithCap(maxJobs int) *InMemoryBackend {
	b := NewInMemoryBackend("123456789012", "us-east-1")
	b.maxJobs = maxJobs

	return b
}

// JobCount returns the number of document jobs stored in the backend (for testing).
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	return len(b.jobs)
}

// ExpenseJobCount returns the number of expense jobs stored in the backend (for testing).
func ExpenseJobCount(b *InMemoryBackend) int {
	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	return len(b.expenseJobs)
}

// LendingJobCount returns the number of lending jobs stored in the backend (for testing).
func LendingJobCount(b *InMemoryBackend) int {
	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	return len(b.lendingJobs)
}

// AdapterCount returns the number of adapters stored in the backend (for testing).
func AdapterCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapter")
	defer b.mu.RUnlock()

	return len(b.adapters)
}

// AdapterVersionCount returns the number of adapter versions stored in the backend (for testing).
func AdapterVersionCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	return len(b.adapterVersions)
}

// HandlerOpsLen returns the number of operations in the handler's dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// AddAdapterInternal adds an adapter directly to the backend for test seeding.
func AddAdapterInternal(b *InMemoryBackend, a *Adapter) {
	b.mu.Lock("CreateAdapter")
	defer b.mu.Unlock()

	b.adapters[a.AdapterID] = cloneAdapter(a)
}

// AddAdapterVersionInternal adds an adapter version directly to the backend for test seeding.
func AddAdapterVersionInternal(b *InMemoryBackend, av *AdapterVersion) {
	b.mu.Lock("CreateAdapterVersion")
	defer b.mu.Unlock()

	b.adapterVersions[adapterVersionKey(av.AdapterID, av.AdapterVersion)] = cloneAdapterVersion(av)
}

// AddExpenseJobInternal adds an expense job directly to the backend for test seeding.
func AddExpenseJobInternal(b *InMemoryBackend, j *ExpenseJob) {
	b.mu.Lock("StartExpenseAnalysis")
	defer b.mu.Unlock()

	b.expenseJobs[j.JobID] = cloneExpenseJob(j)
}

// AddLendingJobInternal adds a lending job directly to the backend for test seeding.
func AddLendingJobInternal(b *InMemoryBackend, j *LendingJob) {
	b.mu.Lock("StartLendingAnalysis")
	defer b.mu.Unlock()

	b.lendingJobs[j.JobID] = cloneLendingJob(j)
}
