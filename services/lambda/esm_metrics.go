package lambda

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// Event source mapping metrics per docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics-types.html:
// each group is opt-in via MetricsConfig and published to AWS/Lambda with the EventSourceMappingUUID dimension.
const (
	esmMetricGroupEventCount = "EventCount"
	esmMetricGroupErrorCount = "ErrorCount"
	esmMetricDimUUID         = "EventSourceMappingUUID"

	esmMetricPolled          = "PolledEventCount"
	esmMetricFilteredOut     = "FilteredOutEventCount"
	esmMetricInvoked         = "InvokedEventCount"
	esmMetricFailedInvoke    = "FailedInvokeEventCount"
	esmMetricDeleted         = "DeletedEventCount"
	esmMetricCommitted       = "CommittedEventCount"
	esmMetricPollingError    = "PollingErrorCount"
	esmMetricInvokeError     = "InvokeErrorCount"
	esmMetricCommitErrorName = "CommitErrorCount"
)

func esmMetricGroupEnabled(cfg *ESMMetricsConfig, group string) bool {
	return cfg != nil && slices.Contains(cfg.Metrics, group)
}

func (b *InMemoryBackend) emitESMMetric(cfg *ESMMetricsConfig, uuid, group, name string, value int) {
	if !esmMetricGroupEnabled(cfg, group) || !b.metrics.Enabled() {
		return
	}

	b.metrics.Put(b.region, lambdaMetricNamespace, name, lambdaUnitCount, float64(value),
		cwmetric.Dimension{Name: esmMetricDimUUID, Value: uuid})
}

// esmInvokeMetrics records one invocation attempt over n events.
func (p *EventSourcePoller) esmInvokeMetrics(m *EventSourceMapping, n, failed int) {
	p.lambdaBackend.emitESMMetric(m.MetricsConfig, m.UUID, esmMetricGroupEventCount, esmMetricInvoked, n)
	p.lambdaBackend.emitESMMetric(m.MetricsConfig, m.UUID, esmMetricGroupEventCount, esmMetricFailedInvoke, failed)
}

func (p *EventSourcePoller) esmCount(m *EventSourceMapping, name string, n int) {
	p.lambdaBackend.emitESMMetric(m.MetricsConfig, m.UUID, esmMetricGroupEventCount, name, n)
}
