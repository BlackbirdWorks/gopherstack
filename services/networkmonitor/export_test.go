package networkmonitor

import "context"

// ProbeInputForTest is a test alias for probeInput so external test packages can create probes.
type ProbeInputForTest = probeInput

// MonitorCount returns the number of monitors stored across all regions.
func MonitorCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.monitors.Len()
}

// HandlerOpsLen returns the number of operations the handler supports.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// WithRegion returns a context with the given region set.
func WithRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}
