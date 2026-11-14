package azurearm

import (
	"strconv"
	"strings"
)

const (
	acaScalerServiceBus   = "azure-servicebus"
	acaScalerStorageQueue = "azure-queue"
	acaDefaultQueueTarget = 5
	acaFieldMetadata      = "metadata"
)

// ACAQueueMeters reads the queue depths KEDA's azure-servicebus and azure-queue scalers poll.
type ACAQueueMeters interface {
	// ServiceBusMessages counts the active messages of a queue, or of a topic subscription when queue is empty.
	ServiceBusMessages(queue, topic, subscription string) (int, error)
	// StorageQueueMessages is the approximate message count of a Storage queue.
	StorageQueueMessages(queue string) (int, error)
}

// acaQueueRule is one queue-depth scale rule: the replicas (or executions) wanted are ceil(length / target).
type acaQueueRule struct {
	scaler, queue, topic, subscription string
	target                             int
}

func acaMetaString(md any, key string) string {
	m, ok := md.(map[string]any)
	if !ok {
		return ""
	}

	switch v := m[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}

	return ""
}

func acaMetaTarget(md any, key string) int {
	if n, err := strconv.Atoi(acaMetaString(md, key)); err == nil && n > 0 {
		return n
	}

	return acaDefaultQueueTarget
}

// acaQueueRuleOf turns a scaler type and its metadata into a rule; ok is false for scalers reading no local queue.
func acaQueueRuleOf(scaler string, md any) (acaQueueRule, bool) {
	switch strings.ToLower(scaler) {
	case acaScalerServiceBus:
		r := acaQueueRule{
			scaler: acaScalerServiceBus, queue: acaMetaString(md, "queueName"),
			topic: acaMetaString(md, "topicName"), subscription: acaMetaString(md, "subscriptionName"),
			target: acaMetaTarget(md, "messageCount"),
		}

		return r, r.queue != "" || (r.topic != "" && r.subscription != "")
	case acaScalerStorageQueue:
		r := acaQueueRule{
			scaler: acaScalerStorageQueue, queue: acaMetaString(md, "queueName"),
			target: acaMetaTarget(md, "queueLength"),
		}

		return r, r.queue != ""
	}

	return acaQueueRule{}, false
}

// acaAppQueueRules extracts the queue rules of an app's scale.rules: the azureQueue block and custom rules.
func acaAppQueueRules(rules []any) []acaQueueRule {
	var out []acaQueueRule

	for _, e := range rules {
		r := mapOf(e)

		if q := mapOf(r["azureQueue"]); q != nil {
			md := map[string]any{"queueName": q["queueName"], "queueLength": q["queueLength"]}
			if rule, ok := acaQueueRuleOf(acaScalerStorageQueue, md); ok {
				out = append(out, rule)
			}
		}

		if c := mapOf(r["custom"]); c != nil {
			if rule, ok := acaQueueRuleOf(strOf(c, fieldType), c[acaFieldMetadata]); ok {
				out = append(out, rule)
			}
		}
	}

	return out
}

// acaJobQueueRules extracts the queue rules of an event job's scale.rules.
func acaJobQueueRules(rules []any) []acaQueueRule {
	var out []acaQueueRule

	for _, e := range rules {
		r := mapOf(e)
		if rule, ok := acaQueueRuleOf(strOf(r, fieldType), r[acaFieldMetadata]); ok {
			out = append(out, rule)
		}
	}

	return out
}

// desired is KEDA's replica count for the rules: the largest ceil(length / target); a rule that cannot be read is
// skipped, and ok is false when none could be.
func acaDesiredFromQueues(m ACAQueueMeters, rules []acaQueueRule) (int, bool) {
	if m == nil {
		return 0, false
	}

	want, read := 0, false

	for _, r := range rules {
		var (
			n   int
			err error
		)

		if r.scaler == acaScalerServiceBus {
			n, err = m.ServiceBusMessages(r.queue, r.topic, r.subscription)
		} else {
			n, err = m.StorageQueueMessages(r.queue)
		}

		if err != nil {
			continue
		}

		read = true
		want = max(want, (n+r.target-1)/r.target)
	}

	return want, read
}
