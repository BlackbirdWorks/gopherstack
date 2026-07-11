package textract

import "time"

// NewInMemoryBackendWithCap creates a backend with a custom job history cap for testing.
func NewInMemoryBackendWithCap(maxJobs int) *InMemoryBackend {
	b := NewInMemoryBackend("123456789012", "us-east-1")
	b.maxJobs = maxJobs

	return b
}

// NewInMemoryBackendSync creates a backend where async jobs complete synchronously (zero delay).
// Use this for tests that need immediate SUCCEEDED status without time.Sleep.
func NewInMemoryBackendSync(accountID, region string) *InMemoryBackend {
	b := NewInMemoryBackend(accountID, region)
	b.asyncJobDelay = 0

	return b
}

// SetBackendAsyncDelay sets the async job completion delay on a specific backend.
func SetBackendAsyncDelay(b *InMemoryBackend, d time.Duration) {
	b.asyncJobDelay = d
}

// JobCount returns the number of document jobs stored in the backend (for testing).
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	return b.jobs.Len()
}

// ExpenseJobCount returns the number of expense jobs stored in the backend (for testing).
func ExpenseJobCount(b *InMemoryBackend) int {
	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	return b.expenseJobs.Len()
}

// LendingJobCount returns the number of lending jobs stored in the backend (for testing).
func LendingJobCount(b *InMemoryBackend) int {
	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	return b.lendingJobs.Len()
}

// AdapterCount returns the number of adapters stored in the backend (for testing).
func AdapterCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapter")
	defer b.mu.RUnlock()

	return b.adapters.Len()
}

// AdapterVersionCount returns the number of adapter versions stored in the backend (for testing).
func AdapterVersionCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	return b.adapterVersions.Len()
}

// HandlerOpsLen returns the number of operations in the handler's dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// AddAdapterInternal adds an adapter directly to the backend for test seeding,
// always into the backend's default region (matching the pre-Phase-3.3
// behavior of always seeding via b.region).
func AddAdapterInternal(b *InMemoryBackend, a *Adapter) {
	b.mu.Lock("CreateAdapter")
	defer b.mu.Unlock()

	cp := cloneAdapter(a)
	cp.Region = b.region
	b.adapters.Put(cp)
}

// AddAdapterVersionInternal adds an adapter version directly to the backend for test seeding.
func AddAdapterVersionInternal(b *InMemoryBackend, av *AdapterVersion) {
	b.mu.Lock("CreateAdapterVersion")
	defer b.mu.Unlock()

	cp := cloneAdapterVersion(av)
	cp.Region = b.region
	b.adapterVersions.Put(cp)
}

// AddExpenseJobInternal adds an expense job directly to the backend for test seeding.
func AddExpenseJobInternal(b *InMemoryBackend, j *ExpenseJob) {
	b.mu.Lock("StartExpenseAnalysis")
	defer b.mu.Unlock()

	cp := cloneExpenseJob(j)
	cp.Region = b.region
	b.expenseJobs.Put(cp)
}

// AddLendingJobInternal adds a lending job directly to the backend for test seeding.
func AddLendingJobInternal(b *InMemoryBackend, j *LendingJob) {
	b.mu.Lock("StartLendingAnalysis")
	defer b.mu.Unlock()

	cp := cloneLendingJob(j)
	cp.Region = b.region
	b.lendingJobs.Put(cp)
}
