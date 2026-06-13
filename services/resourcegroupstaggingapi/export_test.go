package resourcegroupstaggingapi

// ProviderCount returns the number of registered resource providers (plain + filtered).
func ProviderCount(b *InMemoryBackend) int {
	b.mu.RLock("ProviderCount")
	defer b.mu.RUnlock()

	return len(b.providers) + len(b.filteredProviders)
}

// FilteredProviderCount returns the number of registered filtered resource providers.
func FilteredProviderCount(b *InMemoryBackend) int {
	b.mu.RLock("FilteredProviderCount")
	defer b.mu.RUnlock()

	return len(b.filteredProviders)
}

// HasCache returns whether the backend has a non-expired resource cache for its default region.
func HasCache(b *InMemoryBackend) bool {
	b.mu.RLock("HasCache")
	defer b.mu.RUnlock()

	return b.caches[b.defaultRegion] != nil
}

// TaggerCount returns the number of registered ARN taggers.
func TaggerCount(b *InMemoryBackend) int {
	b.mu.RLock("TaggerCount")
	defer b.mu.RUnlock()

	return len(b.taggers)
}

// UntaggerCount returns the number of registered ARN untaggers.
func UntaggerCount(b *InMemoryBackend) int {
	b.mu.RLock("UntaggerCount")
	defer b.mu.RUnlock()

	return len(b.untaggers)
}

// HasReportState returns whether the backend has a stored report creation state for its default region.
func HasReportState(b *InMemoryBackend) bool {
	b.mu.RLock("HasReportState")
	defer b.mu.RUnlock()

	return b.reportStates[b.defaultRegion] != nil
}

// ReportStatus returns the status string from the stored report state for the default region, or empty string.
func ReportStatus(b *InMemoryBackend) string {
	b.mu.RLock("ReportStatus")
	defer b.mu.RUnlock()

	state := b.reportStates[b.defaultRegion]
	if state == nil {
		return ""
	}

	return state.Status
}

// ReportS3Location returns the S3 location from the stored report state for the default region, or empty string.
func ReportS3Location(b *InMemoryBackend) string {
	b.mu.RLock("ReportS3Location")
	defer b.mu.RUnlock()

	state := b.reportStates[b.defaultRegion]
	if state == nil {
		return ""
	}

	return state.S3Location
}

// SetNowFunc replaces the backend's time provider with fn for deterministic testing.
func SetNowFunc(b *InMemoryBackend, fn func() string) {
	b.nowFunc = fn
}

// HandlerOpsLen returns the number of operations returned by GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddReportStateInternal seeds the backend with a specific report state for the default region.
func AddReportStateInternal(b *InMemoryBackend, status, s3Location, startDate string) {
	b.mu.Lock("AddReportStateInternal")
	defer b.mu.Unlock()

	b.reportStates[b.defaultRegion] = &reportCreationState{
		Status:     status,
		S3Location: s3Location,
		StartDate:  startDate,
	}
}
