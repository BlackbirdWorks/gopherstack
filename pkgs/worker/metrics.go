package worker

import "github.com/blackbirdworks/gopherstack/pkgs/telemetry"

// Metrics records background-worker telemetry. A Group is given one at
// construction (defaulting to a pkgs/telemetry-backed implementation) so the
// worker package owns task-status emission and tests can inject a fake.
type Metrics interface {
	// RecordTask records that a sweep ran with the given terminal status
	// ("success" or "panic"). Emitted automatically by the worker around every
	// sweep, so sweeps no longer record their own task status.
	RecordTask(service, component, status string)
	// RecordItems records how many items a sweep processed.
	RecordItems(service, component string, count int)
	// RecordQueueDepth records the current backlog a sweep observed.
	RecordQueueDepth(service, component string, depth int)
}

// telemetryMetrics forwards to pkgs/telemetry and is the default Metrics.
type telemetryMetrics struct{}

func (telemetryMetrics) RecordTask(service, component, status string) {
	telemetry.RecordWorkerTask(service, component, status)
}

func (telemetryMetrics) RecordItems(service, component string, count int) {
	telemetry.RecordWorkerItems(service, component, count)
}

func (telemetryMetrics) RecordQueueDepth(service, component string, depth int) {
	telemetry.RecordWorkerQueueDepth(service, component, depth)
}
