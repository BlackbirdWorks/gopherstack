package resourcegroupstaggingapi

import "time"

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

	return b.reportStates.Has(b.defaultRegion)
}

// ReportStatus returns the status string from the stored report state for the default region, or empty string.
func ReportStatus(b *InMemoryBackend) string {
	b.mu.RLock("ReportStatus")
	defer b.mu.RUnlock()

	state, ok := b.reportStates.Get(b.defaultRegion)
	if !ok {
		return ""
	}

	return state.Status
}

// ReportS3Location returns the S3 location from the stored report state for the default region, or empty string.
func ReportS3Location(b *InMemoryBackend) string {
	b.mu.RLock("ReportS3Location")
	defer b.mu.RUnlock()

	state, ok := b.reportStates.Get(b.defaultRegion)
	if !ok {
		return ""
	}

	return state.S3Location
}

// SetNowFunc replaces the backend's time provider with fn for deterministic testing.
func SetNowFunc(b *InMemoryBackend, fn func() string) {
	b.nowFunc = fn
}

// SetClockFunc replaces the backend's clock with fn for deterministic time-based testing.
// Used to control RUNNING→SUCCEEDED report lifecycle transitions.
func SetClockFunc(b *InMemoryBackend, fn func() time.Time) {
	b.clockFunc = fn
}

// ReportRunningDuration returns the reportRunningDuration constant for use in tests.
func ReportRunningDuration() time.Duration { return reportRunningDuration }

// HandlerOpsLen returns the number of operations returned by GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddReportStateInternal seeds the backend with a specific report state for the default region.
func AddReportStateInternal(b *InMemoryBackend, status, s3Location, startDate string) {
	AddReportStateForRegion(b, b.defaultRegion, status, s3Location, startDate)
}

// AddReportStateForRegion seeds the backend with a specific report state for an arbitrary
// region, exercising the reportStates table's region-keyed identity directly (rather than
// through the request-context-derived region every production code path uses).
func AddReportStateForRegion(b *InMemoryBackend, region, status, s3Location, startDate string) {
	b.mu.Lock("AddReportStateForRegion")
	defer b.mu.Unlock()

	b.reportStates.Put(&reportCreationState{
		Region:     region,
		Status:     status,
		S3Location: s3Location,
		StartDate:  startDate,
	})
}

// ReportStateRegions returns the sorted list of regions that currently have a stored report
// creation state, for asserting the full contents of the reportStates table in tests.
func ReportStateRegions(b *InMemoryBackend) []string {
	b.mu.RLock("ReportStateRegions")
	defer b.mu.RUnlock()

	out := make([]string, 0, b.reportStates.Len())
	for _, s := range b.reportStates.Snapshot() {
		out = append(out, s.Region)
	}

	return out
}

// ReportStatusForRegion returns the status string from the stored report state for region,
// or empty string when no report state exists for that region.
func ReportStatusForRegion(b *InMemoryBackend, region string) string {
	b.mu.RLock("ReportStatusForRegion")
	defer b.mu.RUnlock()

	state, ok := b.reportStates.Get(region)
	if !ok {
		return ""
	}

	return state.Status
}
