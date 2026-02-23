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
			if sub.Protocol != "sqs" {
				continue
			}

			if !matchesFilterPolicy(sub.FilterPolicy, ev.Attributes) {
				continue
			}

			queueName := queueNameFromARN(sub.Endpoint)
			if queueName == "" {
				continue
			}

			body := buildSNSEnvelope(ev, queueName)

			// Best-effort: ignore delivery errors (queue may not exist yet).
			_, _ = b.SendMessage(&SendMessageInput{
				QueueURL:    "internal/" + queueName,
				MessageBody: body,
			})
		}

		return nil
	})
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
