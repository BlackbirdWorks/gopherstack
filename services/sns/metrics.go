package sns

import (
	"encoding/json"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/SNS metrics per docs.aws.amazon.com/sns/latest/dg/sns-monitoring-using-cloudwatch.html (dimension TopicName).
const (
	snsMetricNamespace  = "AWS/SNS"
	snsUnitCount        = "Count"
	snsUnitBytes        = "Bytes"
	dataTypeStringArray = "String.Array"
)

// filteredCounts tallies subscriptions a publish skipped, by the AWS/SNS filtered-out metric that counts them.
type filteredCounts struct {
	total             int
	messageAttributes int
	noAttributes      int
	invalidAttributes int
	body              int
	invalidBody       int
}

type filterRejection int

const (
	rejectAttributes filterRejection = iota
	rejectNoAttributes
	rejectInvalidAttributes
	rejectBody
	rejectInvalidBody
)

func (f *filteredCounts) record(r filterRejection) {
	f.total++

	switch r {
	case rejectNoAttributes:
		f.noAttributes++
	case rejectInvalidAttributes:
		f.invalidAttributes++
	case rejectBody:
		f.body++
	case rejectInvalidBody:
		f.invalidBody++
	default:
		f.messageAttributes++
	}
}

// classifyAttributeRejection names why attrs failed an attribute-scope filter policy.
func classifyAttributeRejection(attrs map[string]MessageAttribute) filterRejection {
	if len(attrs) == 0 {
		return rejectNoAttributes
	}

	for _, a := range attrs {
		if a.DataType != dataTypeStringArray {
			continue
		}

		var elems []any
		if json.Unmarshal([]byte(a.StringValue), &elems) != nil {
			return rejectInvalidAttributes
		}
	}

	return rejectAttributes
}

// classifyBodyRejection names why message failed a body-scope filter policy.
func classifyBodyRejection(message string) filterRejection {
	var body map[string]json.RawMessage
	if json.Unmarshal([]byte(message), &body) != nil {
		return rejectInvalidBody
	}

	return rejectBody
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

// emitPublishMetrics publishes publish-time metrics. Email deliveries count as
// delivered here; SQS, HTTP, Lambda, Firehose, SMS and application report via emitDeliveryOutcome.
func (b *InMemoryBackend) emitPublishMetrics(topicARN, message string, t *publishTargets) {
	if !b.metrics.Enabled() {
		return
	}

	b.putTopicMetric(topicARN, "NumberOfMessagesPublished", snsUnitCount, 1)
	b.putTopicMetric(topicARN, "PublishSize", snsUnitBytes, float64(len(message)))

	delivered := len(t.emailDeliveries)

	if delivered > 0 {
		b.putTopicMetric(topicARN, "NumberOfNotificationsDelivered", snsUnitCount, float64(delivered))
	}

	filtered := map[string]int{
		"NumberOfNotificationsFilteredOut":                     t.filteredOut.total,
		"NumberOfNotificationsFilteredOut-MessageAttributes":   t.filteredOut.messageAttributes,
		"NumberOfNotificationsFilteredOut-NoMessageAttributes": t.filteredOut.noAttributes,
		"NumberOfNotificationsFilteredOut-InvalidAttributes":   t.filteredOut.invalidAttributes,
		"NumberOfNotificationsFilteredOut-MessageBody":         t.filteredOut.body,
		"NumberOfNotificationsFilteredOut-InvalidMessageBody":  t.filteredOut.invalidBody,
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
