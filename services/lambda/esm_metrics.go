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
	esmMetricGroupKafka      = "KafkaMetrics"
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
	esmMetricMaxOffsetLag    = "MaxOffsetLag"
	esmMetricSumOffsetLag    = "SumOffsetLag"
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

// offsetLags returns the largest and total lag over the partitions of recs, using each partition's last record.
func offsetLags(recs []KafkaRecord) (int64, int64, bool) {
	type part struct {
		topic string
		id    int32
	}

	last := map[part]KafkaRecord{}

	for _, r := range recs {
		k := part{r.Topic, r.Partition}
		if cur, ok := last[k]; !ok || r.Offset > cur.Offset {
			last[k] = r
		}
	}

	var maxLag, sum int64

	known := false

	for _, r := range last {
		if r.HighWatermark <= 0 {
			continue
		}

		known = true
		lag := max(r.HighWatermark-(r.Offset+1), 0)
		maxLag = max(maxLag, lag)
		sum += lag
	}

	return maxLag, sum, known
}
