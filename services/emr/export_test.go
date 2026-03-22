package emr

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultJanitorInterval

// DefaultTerminatedTTL exposes the package default terminated cluster TTL for testing.
const DefaultTerminatedTTL = defaultTerminatedTTL

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	return h.janitor.TaskTimeout
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	return h.janitor.Interval
}

// GetJanitorTerminatedTTL returns the TerminatedTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorTerminatedTTL() time.Duration {
	return h.janitor.TerminatedTTL
}

