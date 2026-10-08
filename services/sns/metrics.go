package sns

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/SNS metrics per docs.aws.amazon.com/sns/latest/dg/sns-monitoring-using-cloudwatch.html (dimension TopicName).
const (
	snsMetricNamespace = "AWS/SNS"
	snsUnitCount       = "Count"
	snsUnitBytes       = "Bytes"
)

// filteredCounts tallies subscriptions a publish skipped, by filter-policy scope.
type filteredCounts struct {
	attributes   int
	noAttributes int
	body         int
}

func (f *filteredCounts) record(noAttributes, body bool) {
	switch {
	case body:
		f.body++
	case noAttributes:
		f.noAttributes++
	default:
		f.attributes++
	}
}

// SetMetricEmitter sets the emitter that publishes AWS/SNS metrics to CloudWatch.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// topicMetricPoint splits a topic ARN into its region and TopicName.
func topicMetricPoint(topicARN string) (string, string) {
	parts := strings.Split(topicARN, ":")
	const arnParts = 6

	if len(parts) < arnParts {
		return "", topicARN
	}

	return parts[3], parts[5]
}

func (b *InMemoryBackend) putTopicMetric(topicARN, name, unit string, v float64) {
	region, topic := topicMetricPoint(topicARN)
	b.metrics.Put(region, snsMetricNamespace, name, unit, v, cwmetric.Dimension{Name: "TopicName", Value: topic})
}

// emitPublishMetrics publishes publish-time metrics. SQS and email deliveries count as
// delivered here; HTTP, Lambda, Firehose, SMS and application report via emitDeliveryOutcome.
func (b *InMemoryBackend) emitPublishMetrics(topicARN, message string, t *publishTargets) {
	if !b.metrics.Enabled() {
		return
	}

	b.putTopicMetric(topicARN, "NumberOfMessagesPublished", snsUnitCount, 1)
	b.putTopicMetric(topicARN, "PublishSize", snsUnitBytes, float64(len(message)))

	delivered := len(t.emailDeliveries)

	for i := range t.subs {
		if t.subs[i].Protocol == protocolSQS {
			delivered++
		}
	}

	if delivered > 0 {
		b.putTopicMetric(topicARN, "NumberOfNotificationsDelivered", snsUnitCount, float64(delivered))
	}

	filtered := map[string]int{
		"NumberOfNotificationsFilteredOut":                     t.filteredOut.attributes,
		"NumberOfNotificationsFilteredOut-NoMessageAttributes": t.filteredOut.noAttributes,
		"NumberOfNotificationsFilteredOut-MessageBody":         t.filteredOut.body,
	}

	for name, n := range filtered {
		if n > 0 {
			b.putTopicMetric(topicARN, name, snsUnitCount, float64(n))
		}
	}
}

// emitDeliveryOutcome publishes the delivered/failed count for one asynchronous delivery.
func (b *InMemoryBackend) emitDeliveryOutcome(topicARN string, ok bool) {
	if !b.metrics.Enabled() {
		return
	}

	name := "NumberOfNotificationsFailed"
	if ok {
		name = "NumberOfNotificationsDelivered"
	}

	b.putTopicMetric(topicARN, name, snsUnitCount, 1)
}
