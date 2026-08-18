package sqs

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
)

// snsMessageAttribute is a single message attribute in the SNS notification envelope.
type snsMessageAttribute struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// snsEnvelope is the JSON body delivered to an SQS queue from SNS.
type snsEnvelope struct {
	MessageAttributes map[string]snsMessageAttribute `json:"MessageAttributes,omitempty"`
	Type              string                         `json:"Type"`
	MessageID         string                         `json:"MessageId"`
	TopicArn          string                         `json:"TopicArn"`
	Subject           string                         `json:"Subject,omitempty"`
	Message           string                         `json:"Message"`
	Timestamp         string                         `json:"Timestamp"`
	SignatureVersion  string                         `json:"SignatureVersion"`
	Signature         string                         `json:"Signature"`
	SigningCertURL    string                         `json:"SigningCertURL"`
	UnsubscribeURL    string                         `json:"UnsubscribeURL"`
}

// SubscribeToSNS registers a listener on the given SNS publish emitter so that
// every message published to an SNS topic with an "sqs" subscription is delivered
// to the matching in-memory queue.
//
// Delivery is synchronous and best-effort: per-message errors are silently dropped
// so that a missing queue does not block other subscribers.
//
// If SubscribeToSNS has been called before, the previous subscription is replaced
// by unsubscribing the old listener before registering the new one, preventing
// stale listeners from accumulating in the emitter on repeated calls.
func (b *InMemoryBackend) SubscribeToSNS(emitter events.EventEmitter[*events.SNSPublishedEvent]) {
	unsubscribe := emitter.Subscribe(func(_ context.Context, ev *events.SNSPublishedEvent) error {
		for _, sub := range ev.Subscriptions {
			b.deliverSNSSubscription(ev, sub)
		}

		return nil
	})

	b.mu.Lock("SubscribeToSNS")
	defer b.mu.Unlock()

	if b.snsUnsubscribe != nil {
		b.snsUnsubscribe()
	}

	b.snsUnsubscribe = unsubscribe
}

// deliverSNSSubscription delivers a single SNS published event to an SQS subscription.
func (b *InMemoryBackend) deliverSNSSubscription(
	ev *events.SNSPublishedEvent,
	sub events.SNSSubscriptionSnapshot,
) {
	if sub.Protocol != "sqs" {
		return
	}

	queueRegion, queueName := parseQueueARNOrURL(sub.Endpoint)
	if queueName == "" {
		return
	}

	body, msgAttrs := buildDeliveryBody(ev, sub, queueName)

	input := &SendMessageInput{
		QueueURL:    "internal/" + queueName,
		MessageBody: body,
		Region:      queueRegion,
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

	dlqRegion, dlqName := parseQueueARNOrURL(policy.DeadLetterTargetArn)
	if dlqName == "" {
		return
	}

	input := &SendMessageInput{
		QueueURL:    "internal/" + dlqName,
		MessageBody: body,
		Region:      dlqRegion,
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

// arnRegionFieldCount is the number of colon-separated fields in a well-formed
// ARN up to and including the region field (arn:partition:service:region:...).
const arnRegionFieldCount = 4

// parseQueueARNOrURL extracts the region and queue name from an SQS ARN or URL,
// so that internal SNS->SQS fan-out and DLQ redirect deliveries reach a queue
// created in a non-default region instead of always falling back to the
// backend's default region (see gopherstack-qgh).
//
// ARN format:  arn:aws:sqs:<region>:<account>:<queue-name>  -> region is field 4
// URL format:  http://…/<account>/<queue-name>               -> region is unknown,
// so the empty string is returned and the caller's SendMessageInput.Region falls
// back to the backend's default region via effectiveRegion, matching prior behavior.
func parseQueueARNOrURL(endpoint string) (string, string) {
	parts := strings.Split(endpoint, ":")
	if len(parts) >= 6 && parts[0] == "arn" {
		return parts[arnRegionFieldCount-1], parts[len(parts)-1]
	}

	// Fall back to last path segment for URLs; region is not recoverable from a
	// bare queue URL in this codebase's URL scheme (scheme://host/account/name).
	segments := strings.Split(endpoint, "/")

	return "", segments[len(segments)-1]
}

// buildSNSEnvelope wraps the published message in the standard SNS notification JSON.
func buildSNSEnvelope(ev *events.SNSPublishedEvent, _ string) string {
	ts := ev.Timestamp
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	sig := ev.Signature
	if sig == "" {
		sig = uuid.NewString()
	}

	// SignatureVersion must reflect whichever hash (SHA1 for "1", SHA256 for "2")
	// actually produced Signature above; the SNS backend resolves and stamps this
	// per-topic (default "1", the real AWS default) via events.SNSPublishedEvent.
	// Fall back to "1" only for the defensive case of an event that predates this
	// field (e.g. constructed by a future caller that forgets to set it).
	sigVersion := ev.SignatureVersion
	if sigVersion == "" {
		sigVersion = "1"
	}

	env := snsEnvelope{
		Type:             "Notification",
		MessageID:        ev.MessageID,
		TopicArn:         ev.TopicARN,
		Subject:          ev.Subject,
		Message:          ev.Message,
		Timestamp:        ts,
		SignatureVersion: sigVersion,
		Signature:        sig,
		SigningCertURL:   ev.SigningCertURL,
		UnsubscribeURL:   "https://sns.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=" + ev.TopicARN,
	}

	if len(ev.Attributes) > 0 {
		env.MessageAttributes = make(map[string]snsMessageAttribute, len(ev.Attributes))
		for k, v := range ev.Attributes {
			env.MessageAttributes[k] = snsMessageAttribute{
				Type:  v.DataType,
				Value: v.StringValue,
			}
		}
	}

	b, err := json.Marshal(env)
	if err != nil {
		return ev.Message
	}

	return string(b)
}
