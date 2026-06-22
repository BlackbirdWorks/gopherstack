package worker

import "github.com/blackbirdworks/gopherstack/pkgs/telemetry"

// Metrics records background-worker telemetry the Group emits on its own behalf
// (currently the "panic" task recorded when a sweep/goroutine/timer panics).
// A Group is given one at construction, defaulting to a pkgs/telemetry-backed
// implementation; tests inject a fake to assert on it. Sweeps continue to emit
// their own success/items/depth telemetry, which is often finer-grained than the
// ticker that drives them.
type Metrics interface {
	RecordTask(service, component, status string)
}

// telemetryMetrics forwards to pkgs/telemetry and is the default Metrics.
type telemetryMetrics struct{}

func (telemetryMetrics) RecordTask(service, component, status string) {
	telemetry.RecordWorkerTask(service, component, status)
}
