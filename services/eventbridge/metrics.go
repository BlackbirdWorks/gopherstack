package eventbridge

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/Events rule metrics per docs.aws.amazon.com/eventbridge/latest/userguide/eb-monitoring.html.
const (
	ebMetricNamespace = "AWS/Events"
	ebUnitCount       = "Count"
	ebUnitBytes       = "Bytes"
	ebUnitMillis      = "Milliseconds"
)

// PutEvents records events in the event log, returns per-entry results and publishes the PutEvents* metrics.
func (b *InMemoryBackend) PutEvents(ctx context.Context, entries []EventEntry) ([]EventResultEntry, error) {
	start := time.Now()
	results, err := b.putEvents(ctx, entries)
	b.observePutEvents(ctx, entries, results, err, time.Since(start))

	return results, err
}

func (b *InMemoryBackend) observePutEvents(
	ctx context.Context, entries []EventEntry, results []EventResultEntry, err error, elapsed time.Duration,
) {
	if !b.metrics.Enabled() {
		return
	}

	region := getRegionFromContext(ctx, b.region)
	put := func(name, unit string, v float64) { b.metrics.Put(region, ebMetricNamespace, name, unit, v) }
	failedEntries := countFailedEntries(results)

	put("PutEventsApproximateCallCount", ebUnitCount, 1)

	if err != nil {
		put("PutEventsApproximateFailedCount", ebUnitCount, 1)
	} else {
		put("PutEventsApproximateSuccessCount", ebUnitCount, 1)
		put("PutEventsFailedEntriesCount", ebUnitCount, float64(failedEntries))
	}

	put("PutEventsEntriesCount", ebUnitCount, float64(len(entries)))
	put("PutEventsLatency", ebUnitMillis, float64(elapsed.Milliseconds()))

	if body, mErr := json.Marshal(entries); mErr == nil {
		put("PutEventsRequestSize", ebUnitBytes, float64(len(body)))
	}
}

// SetMetricEmitter sets the emitter that publishes AWS/Events metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// ruleObserver publishes the rule-dimensioned metrics of one matched rule; the zero value is inert.
type ruleObserver struct {
	sink   *cwmetric.Sink
	region string
	bus    string
	rule   string
}

func (o ruleObserver) put(name string) { o.putValue(name, ebUnitCount, 1) }

func (o ruleObserver) putValue(name, unit string, value float64) {
	if o.sink == nil || !o.sink.Enabled() {
		return
	}

	dims := []cwmetric.Dimension{{Name: "RuleName", Value: o.rule}}
	if o.bus != "" && o.bus != defaultEventBusName {
		dims = append(dims, cwmetric.Dimension{Name: "EventBusName", Value: o.bus})
	}

	o.sink.Put(o.region, ebMetricNamespace, name, unit, value, dims...)
}

func (o ruleObserver) matched() {
	o.put("MatchedEvents")
	o.put("TriggeredRules")
}

func (o ruleObserver) invoked()      { o.put("Invocations") }
func (o ruleObserver) failed()       { o.put("FailedInvocations") }
func (o ruleObserver) deadLettered() { o.put("DeadLetterInvocations") }

func (o ruleObserver) attempted()     { o.put("InvocationAttempts") }
func (o ruleObserver) succeeded()     { o.put("SuccessfulInvocationAttempts") }
func (o ruleObserver) retried()       { o.put("RetryInvocationAttempts") }
func (o ruleObserver) sentToDLQ()     { o.put("InvocationsSentToDlq") }
func (o ruleObserver) dlqSendFailed() { o.put("InvocationsFailedToBeSentToDlq") }

func (o ruleObserver) latency(name string, d time.Duration) {
	o.putValue(name, ebUnitMillis, float64(d.Milliseconds()))
}
