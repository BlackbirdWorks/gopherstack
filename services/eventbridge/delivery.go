package eventbridge

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/google/uuid"
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
	b.mu.RLock("deliverEvents")
	busRules := make(map[string]map[string]*Rule)
	busTargets := make(map[string]map[string]*Target)
	maps.Copy(busRules, b.rules)
	maps.Copy(busTargets, b.targets)
	accountID := b.accountID
	region := b.region
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
				deliverToTarget(ctx, t, entry, targets, accountID, region)
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

	if len(entry.Resources) > 0 {
		resources := make([]any, len(entry.Resources))
		for i, r := range entry.Resources {
			resources[i] = r
		}

		envelope["resources"] = resources
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
func deliverToTarget(
	ctx context.Context,
	target *Target,
	entry EventEntry,
	dt DeliveryTargets,
	accountID, region string,
) {
	arn := target.Arn
	log := logger.Load(ctx)

	payload := buildPayload(target, entry, accountID, region)

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
// Priority: Input override → InputPath → InputTransformer → full event envelope.
func buildPayload(target *Target, entry EventEntry, accountID, region string) string {
	if target.Input != "" {
		return target.Input
	}

	envelope := buildDeliveryEnvelope(entry, accountID, region)

	if target.InputPath != "" {
		return applyInputPath(target.InputPath, envelope)
	}

	if target.InputTransformer != nil {
		return applyInputTransformer(target.InputTransformer, envelope)
	}

	b, _ := json.Marshal(envelope)

	return string(b)
}

// buildDeliveryEnvelope creates the full AWS EventBridge event envelope used for delivery payloads.
// It includes id, version, time, account, region, source, detail-type, resources, and detail.
func buildDeliveryEnvelope(entry EventEntry, accountID, region string) map[string]any {
	eventTime := time.Now()
	if entry.Time != nil {
		eventTime = *entry.Time
	}

	var detail any
	if entry.Detail != "" {
		var d any
		if err := json.Unmarshal([]byte(entry.Detail), &d); err == nil {
			detail = d
		} else {
			detail = entry.Detail
		}
	}

	return map[string]any{
		"version":     "0",
		"id":          uuid.New().String(),
		"source":      entry.Source,
		"account":     accountID,
		"time":        eventTime.UTC().Format(time.RFC3339),
		"region":      region,
		"resources":   entry.Resources,
		"detail-type": entry.DetailType,
		"detail":      detail,
	}
}

// applyInputPath extracts a value from the event envelope using a simple JSONPath expression.
// Returns the JSON-serialized extracted value, or an empty string if extraction fails.
func applyInputPath(path string, envelope map[string]any) string {
	val := jsonPathExtract(path, envelope)
	if val == nil {
		return ""
	}

	b, _ := json.Marshal(val)

	return string(b)
}

// applyInputTransformer applies InputPathsMap variable extraction and InputTemplate substitution.
// Variables defined in InputPathsMap are extracted from the envelope and substituted into
// InputTemplate using <variableName> syntax.
func applyInputTransformer(t *InputTransformer, envelope map[string]any) string {
	vars := make(map[string]string, len(t.InputPathsMap))

	for varName, path := range t.InputPathsMap {
		val := jsonPathExtract(path, envelope)
		if val == nil {
			vars[varName] = ""

			continue
		}

		switch v := val.(type) {
		case string:
			vars[varName] = v
		default:
			b, _ := json.Marshal(v)
			vars[varName] = string(b)
		}
	}

	result := t.InputTemplate
	for varName, value := range vars {
		result = strings.ReplaceAll(result, "<"+varName+">", value)
	}

	return result
}

// jsonPathExtract resolves a simple dot-notation JSONPath expression (e.g. $.source, $.detail.key)
// against the given event envelope. Returns nil if the path cannot be resolved.
func jsonPathExtract(path string, data map[string]any) any {
	if path == "$" || path == "" {
		return data
	}

	if !strings.HasPrefix(path, "$.") {
		return nil
	}

	parts := strings.Split(path[2:], ".")
	var current any = data

	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}

		current, ok = m[part]
		if !ok {
			return nil
		}
	}

	return current
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
