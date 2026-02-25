package eventbridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
)

// LambdaInvoker can invoke a Lambda function by name/ARN with a payload.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// SQSSender can send a message to an SQS queue by URL or ARN.
type SQSSender interface {
	SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error
}

// SNSPublisher can publish a message to an SNS topic by ARN.
type SNSPublisher interface {
	PublishToTopic(ctx context.Context, topicARN, message string) error
}

// DeliveryTargets holds optional service references for event fan-out.
type DeliveryTargets struct {
	Lambda LambdaInvoker
	SQS    SQSSender
	SNS    SNSPublisher
}

// deliverEvents fan-outs events to matching rule targets.
// It runs asynchronously and does not block PutEvents.
func (b *InMemoryBackend) deliverEvents(ctx context.Context, entries []EventEntry, targets DeliveryTargets) {
	b.mu.RLock()
	busRules := make(map[string]map[string]*Rule)
	busTargets := make(map[string]map[string]*Target)
	for busName, rules := range b.rules {
		busRules[busName] = rules
	}
	for key, t := range b.targets {
		busTargets[key] = t
	}
	b.mu.RUnlock()

	for _, entry := range entries {
		busName := entry.EventBusName
		if busName == "" {
			busName = defaultEventBusName
		}

		rules := busRules[busName]
		for _, rule := range rules {
			if rule.State != "ENABLED" {
				continue
			}

			if rule.EventPattern == "" {
				continue
			}

			// Build a normalized event envelope for pattern matching.
			eventEnvelope := buildEventEnvelope(entry)
			if !matchPattern(rule.EventPattern, eventEnvelope) {
				continue
			}

			// Deliver to all targets for this rule.
			key := b.targetKey(busName, rule.Name)
			for _, t := range busTargets[key] {
				deliverToTarget(ctx, b.logger, t, entry, targets)
			}
		}
	}
}

// buildEventEnvelope creates a JSON string representing the normalized event for pattern matching.
func buildEventEnvelope(entry EventEntry) string {
	envelope := map[string]any{
		"source":      entry.Source,
		"detail-type": entry.DetailType,
	}

	if entry.EventBusName != "" {
		envelope["event-bus-name"] = entry.EventBusName
	}

	if entry.Detail != "" {
		var detail map[string]any
		if err := json.Unmarshal([]byte(entry.Detail), &detail); err == nil {
			envelope["detail"] = detail
		} else {
			envelope["detail"] = entry.Detail
		}
	}

	b, _ := json.Marshal(envelope)

	return string(b)
}

// deliverToTarget delivers a single event to a single target.
func deliverToTarget(ctx context.Context, log *slog.Logger, target *Target, entry EventEntry, dt DeliveryTargets) {
	arn := target.Arn

	payload := buildPayload(target, entry)

	switch {
	case isLambdaARN(arn):
		if dt.Lambda == nil {
			return
		}
		_, _, err := dt.Lambda.InvokeFunction(ctx, arn, "Event", []byte(payload))
		if err != nil {
			log.WarnContext(ctx, "EventBridge failed to invoke Lambda target", "arn", arn, "error", err)
		}

	case isSQSARN(arn):
		if dt.SQS == nil {
			return
		}
		if err := dt.SQS.SendMessageToQueue(ctx, arn, payload); err != nil {
			log.WarnContext(ctx, "EventBridge failed to deliver to SQS target", "arn", arn, "error", err)
		}

	case isSNSARN(arn):
		if dt.SNS == nil {
			return
		}
		if err := dt.SNS.PublishToTopic(ctx, arn, payload); err != nil {
			log.WarnContext(ctx, "EventBridge failed to publish to SNS target", "arn", arn, "error", err)
		}

	default:
		log.WarnContext(ctx, "EventBridge: unsupported target ARN type", "arn", arn)
	}
}

// buildPayload constructs the message payload for a target.
// If the target has an Input override, that is used. Otherwise the full event is serialized.
func buildPayload(target *Target, entry EventEntry) string {
	if target.Input != "" {
		return target.Input
	}

	ev := map[string]any{
		"source":      entry.Source,
		"detail-type": entry.DetailType,
		"detail":      entry.Detail,
	}
	if entry.EventBusName != "" {
		ev["event-bus-name"] = entry.EventBusName
	}

	b, _ := json.Marshal(ev)

	return string(b)
}

// isLambdaARN returns true if the ARN identifies a Lambda function.
func isLambdaARN(arn string) bool {
	return strings.Contains(arn, ":lambda:") || strings.HasPrefix(arn, "arn:aws:lambda:")
}

// isSQSARN returns true if the ARN identifies an SQS queue.
func isSQSARN(arn string) bool {
	return strings.Contains(arn, ":sqs:") || strings.HasPrefix(arn, "arn:aws:sqs:")
}

// isSNSARN returns true if the ARN identifies an SNS topic.
func isSNSARN(arn string) bool {
	return strings.Contains(arn, ":sns:") || strings.HasPrefix(arn, "arn:aws:sns:")
}
