package eventbridge

import "github.com/blackbirdworks/gopherstack/pkgs/cwmetric"

// AWS/Events rule metrics per docs.aws.amazon.com/eventbridge/latest/userguide/eb-monitoring.html.
const (
	ebMetricNamespace = "AWS/Events"
	ebUnitCount       = "Count"
)

// SetMetricEmitter sets the emitter that publishes AWS/Events metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// ruleObserver publishes the rule-dimensioned metrics of one matched rule; the zero value is inert.
type ruleObserver struct {
	sink   *cwmetric.Sink
	region string
	bus    string
	rule   string
}

func (o ruleObserver) put(name string) {
	if o.sink == nil || !o.sink.Enabled() {
		return
	}

	dims := []cwmetric.Dimension{{Name: "RuleName", Value: o.rule}}
	if o.bus != "" && o.bus != defaultEventBusName {
		dims = append(dims, cwmetric.Dimension{Name: "EventBusName", Value: o.bus})
	}

	o.sink.Put(o.region, ebMetricNamespace, name, ebUnitCount, 1, dims...)
}

func (o ruleObserver) matched() {
	o.put("MatchedEvents")
	o.put("TriggeredRules")
}

func (o ruleObserver) invoked()      { o.put("Invocations") }
func (o ruleObserver) failed()       { o.put("FailedInvocations") }
func (o ruleObserver) deadLettered() { o.put("DeadLetterInvocations") }
