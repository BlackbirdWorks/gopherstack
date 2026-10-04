package ec2

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/EC2 StatusCheckFailed* (InstanceId): UserGuide/viewing_metrics_with_cloudwatch.html.
// Utilisation metrics are not emitted: no instance runtime produces them.
const (
	ec2MetricNamespace        = "AWS/EC2"
	ec2UnitCount              = "Count"
	statusCheckMetricInterval = time.Minute
)

// SetMetricEmitter sets the emitter that publishes AWS/EC2 metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// EmitStatusCheckMetrics publishes StatusCheckFailed, StatusCheckFailed_Instance and
// StatusCheckFailed_System as 0 for every running instance: the emulator never fails a check.
func (b *InMemoryBackend) EmitStatusCheckMetrics() {
	if !b.metrics.Enabled() {
		return
	}

	b.mu.RLock("EmitStatusCheckMetrics")
	ids := make([]string, 0, b.instances.Len())

	for _, inst := range b.instances.All() {
		if inst.State == StateRunning {
			ids = append(ids, inst.ID)
		}
	}
	b.mu.RUnlock()

	for _, id := range ids {
		dim := cwmetric.Dimension{Name: "InstanceId", Value: id}

		for _, name := range []string{"StatusCheckFailed", "StatusCheckFailed_Instance", "StatusCheckFailed_System"} {
			b.metrics.Put(b.Region, ec2MetricNamespace, name, ec2UnitCount, 0, dim)
		}
	}
}
