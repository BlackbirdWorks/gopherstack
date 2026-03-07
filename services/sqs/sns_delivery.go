package sqs

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
)

// snsEnvelope is the JSON body delivered to an SQS queue from SNS.
type snsEnvelope struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Subject          string `json:"Subject,omitempty"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
}

// SubscribeToSNS registers a listener on the given SNS publish emitter so that
// every message published to an SNS topic with an "sqs" subscription is delivered
// to the matching in-memory queue.
//
// Delivery is synchronous and best-effort: per-message errors are silently dropped
// so that a missing queue does not block other subscribers.
func (b *InMemoryBackend) SubscribeToSNS(emitter events.EventEmitter[*events.SNSPublishedEvent]) {
	emitter.Subscribe(func(_ context.Context, ev *events.SNSPublishedEvent) error {
		for _, sub := range ev.Subscriptions {
			b.deliverSNSSubscription(ev, sub)
		}

		return nil
	})
}

// deliverSNSSubscription delivers a single SNS published event to an SQS subscription.
func (b *InMemoryBackend) deliverSNSSubscription(
	ev *events.SNSPublishedEvent,
	sub events.SNSSubscriptionSnapshot,
) {
	if sub.Protocol != "sqs" {
		return
	}

	if !matchesFilterPolicy(sub.FilterPolicy, ev.Attributes) {
		return
	}

	queueName := queueNameFromARN(sub.Endpoint)
	if queueName == "" {
		return
	}

	body, msgAttrs := buildDeliveryBody(ev, sub, queueName)

	input := &SendMessageInput{
		QueueURL:    "internal/" + queueName,
		MessageBody: body,
	}

	if len(msgAttrs) > 0 {
		input.MessageAttributes = msgAttrs
	}

	// Best-effort delivery: on failure, route to the dead-letter queue if configured.
	_, err := b.SendMessage(input)
	if err != nil && sub.RedrivePolicy != "" {
		b.deliverToDLQ(sub.RedrivePolicy, body, msgAttrs)
	}
}

// buildDeliveryBody returns the SQS message body and optional message attributes for the given subscription.
func buildDeliveryBody(
	ev *events.SNSPublishedEvent,
	sub events.SNSSubscriptionSnapshot,
	queueName string,
) (string, map[string]MessageAttributeValue) {
	if sub.RawMessageDelivery {
		return ev.Message, snsAttrsToSQSAttrs(ev.Attributes)
	}

	return buildSNSEnvelope(ev, queueName), nil
}

// deliverToDLQ sends the message body and attributes (exactly as attempted during the failed
// delivery) to the dead-letter queue specified in the redrive policy.
// The redrivePolicy JSON must have the form {"deadLetterTargetArn":"arn:aws:sqs:..."}.
func (b *InMemoryBackend) deliverToDLQ(
	redrivePolicy, body string,
	msgAttrs map[string]MessageAttributeValue,
) {
	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}

	if err := json.Unmarshal([]byte(redrivePolicy), &policy); err != nil {
		return
	}

	if policy.DeadLetterTargetArn == "" {
		return
	}

	dlqName := queueNameFromARN(policy.DeadLetterTargetArn)
	if dlqName == "" {
		return
	}

	input := &SendMessageInput{
		QueueURL:    "internal/" + dlqName,
		MessageBody: body,
	}

	if len(msgAttrs) > 0 {
		input.MessageAttributes = msgAttrs
	}

	_, _ = b.SendMessage(input)
}

// snsAttrsToSQSAttrs converts SNS message attribute snapshots to SQS MessageAttributeValues.
func snsAttrsToSQSAttrs(attrs map[string]events.SNSMessageAttributeSnapshot) map[string]MessageAttributeValue {
	if len(attrs) == 0 {
		return nil
	}

	result := make(map[string]MessageAttributeValue, len(attrs))

	for k, v := range attrs {
		result[k] = MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
		}
	}

	return result
}

// queueNameFromARN extracts the queue name from an SQS ARN or URL.
// ARN format:  arn:aws:sqs:<region>:<account>:<queue-name>
// URL format:  http://…/<account>/<queue-name>
func queueNameFromARN(endpoint string) string {
	parts := strings.Split(endpoint, ":")
	if len(parts) >= 6 && parts[0] == "arn" {
		return parts[len(parts)-1]
	}

	// Fall back to last path segment for URLs.
	segments := strings.Split(endpoint, "/")

	return segments[len(segments)-1]
}

// buildSNSEnvelope wraps the published message in the standard SNS notification JSON.
func buildSNSEnvelope(ev *events.SNSPublishedEvent, _ string) string {
	env := snsEnvelope{
		Type:             "Notification",
		MessageID:        ev.MessageID,
		TopicArn:         ev.TopicARN,
		Subject:          ev.Subject,
		Message:          ev.Message,
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		SignatureVersion: "1",
		Signature:        uuid.NewString(), // mock signature
	}

	b, err := json.Marshal(env)
	if err != nil {
		return ev.Message
	}

	return string(b)
}

// matchesFilterPolicy returns true when the message attributes satisfy the filter
// policy, or when no filter policy is set.
//
// Only "exact string match" (["value1","value2"]) policies are supported here.
// Full policy evaluation (prefix, numeric, anything-but, etc.) is handled by the
// richer filter package used in SNS Task 3; this implementation covers the common case.
func matchesFilterPolicy(policy string, attrs map[string]events.SNSMessageAttributeSnapshot) bool {
	if policy == "" {
		return true
	}

	var fp map[string][]string

	if err := json.Unmarshal([]byte(policy), &fp); err != nil {
		// If we can't parse the policy, allow delivery (fail-open).
		return true
	}

	for attrKey, allowedValues := range fp {
		attr, ok := attrs[attrKey]
		if !ok {
			return false
		}

		matched := slices.Contains(allowedValues, attr.StringValue)

		if !matched {
			return false
		}
	}

	return true
}
