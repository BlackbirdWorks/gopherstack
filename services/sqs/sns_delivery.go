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
	if b.snsUnsubscribe != nil {
		b.snsUnsubscribe()
	}
	b.snsUnsubscribe = unsubscribe
	b.mu.Unlock()
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
	ts := ev.Timestamp
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	sig := ev.Signature
	if sig == "" {
		sig = uuid.NewString()
	}

	env := snsEnvelope{
		Type:             "Notification",
		MessageID:        ev.MessageID,
		TopicArn:         ev.TopicARN,
		Subject:          ev.Subject,
		Message:          ev.Message,
		Timestamp:        ts,
		SignatureVersion: "1",
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

// matchesFilterPolicy returns true when the message attributes satisfy the filter
// policy, or when no filter policy is set.
//
// Supported operators (per AWS FilterPolicy docs):
//   - Exact string list:  ["val1","val2"]
//   - Prefix:             [{"prefix": "order-"}]
//   - Suffix:             [{"suffix": "-order"}]
//   - Equals-ignore-case: [{"equals-ignore-case": "VALUE"}]
//   - Numeric:            [{"numeric": ["=",100]}]  (=, !=, <, <=, >, >=, between)
//   - Anything-but:       [{"anything-but": ["v1","v2"]}] or {"anything-but":{"prefix":"p"}}
//   - Exists:             [{"exists": true}]
func matchesFilterPolicy(policy string, attrs map[string]events.SNSMessageAttributeSnapshot) bool {
	if policy == "" {
		return true
	}

	var fp map[string][]json.RawMessage

	if err := json.Unmarshal([]byte(policy), &fp); err != nil {
		// If we can't parse the policy, allow delivery (fail-open).
		return true
	}

	for attrKey, conditions := range fp {
		attr, attrExists := attrs[attrKey]
		if !matchesConditions(conditions, attr, attrExists) {
			return false
		}
	}

	return true
}

// matchesConditions evaluates an array of filter-policy conditions for one attribute.
// Returns true if ANY condition in the array matches (OR semantics within one attribute).
func matchesConditions(conditions []json.RawMessage, attr events.SNSMessageAttributeSnapshot, attrExists bool) bool {
	for _, raw := range conditions {
		if matchesCondition(raw, attr, attrExists) {
			return true
		}
	}

	return false
}

// matchesCondition evaluates a single filter-policy condition against an attribute.
func matchesCondition(raw json.RawMessage, attr events.SNSMessageAttributeSnapshot, attrExists bool) bool {
	// Try exact string match first (JSON string).
	var strVal string
	if json.Unmarshal(raw, &strVal) == nil {
		return attrExists && attr.StringValue == strVal
	}

	// Try operator object.
	var opMap map[string]json.RawMessage
	if json.Unmarshal(raw, &opMap) != nil {
		return false
	}

	if v, ok := opMap["exists"]; ok {
		var wantExists bool
		if json.Unmarshal(v, &wantExists) != nil {
			return false
		}

		return attrExists == wantExists
	}

	if !attrExists {
		return false
	}

	if v, ok := opMap["prefix"]; ok {
		var prefix string
		if json.Unmarshal(v, &prefix) != nil {
			return false
		}

		return strings.HasPrefix(attr.StringValue, prefix)
	}

	if v, ok := opMap["suffix"]; ok {
		var suffix string
		if json.Unmarshal(v, &suffix) != nil {
			return false
		}

		return strings.HasSuffix(attr.StringValue, suffix)
	}

	if v, ok := opMap["equals-ignore-case"]; ok {
		var target string
		if json.Unmarshal(v, &target) != nil {
			return false
		}

		return strings.EqualFold(attr.StringValue, target)
	}

	if v, ok := opMap["anything-but"]; ok {
		return matchesAnythingBut(v, attr)
	}

	if v, ok := opMap["numeric"]; ok {
		return matchesNumeric(v, attr.StringValue)
	}

	return false
}

// matchesAnythingBut handles the anything-but operator, which can be:
//   - a list of string values: {"anything-but": ["v1","v2"]}
//   - a prefix rule:           {"anything-but": {"prefix": "p"}}
func matchesAnythingBut(raw json.RawMessage, attr events.SNSMessageAttributeSnapshot) bool {
	// Try prefix form.
	var nested map[string]string
	if json.Unmarshal(raw, &nested) == nil {
		if prefix, ok := nested["prefix"]; ok {
			return !strings.HasPrefix(attr.StringValue, prefix)
		}
	}

	// Try list form.
	var list []string
	if json.Unmarshal(raw, &list) == nil {
		return !slices.Contains(list, attr.StringValue)
	}

	return false
}

// matchesNumeric evaluates a numeric filter condition against the attribute's
// string value. The condition is a JSON array:
// ["=", 100], ["!=", 100], ["<", 100], ["<=", 100], [">", 100], [">=", 100],
// [">", 0, "<", 100]  (between — treated as lower AND upper bounds).
func matchesNumeric(raw json.RawMessage, strValue string) bool {
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return false
	}

	var attrNum float64
	if err := json.Unmarshal([]byte(strValue), &attrNum); err != nil {
		return false
	}

	return evalNumericParts(parts, attrNum)
}

// evalNumericParts evaluates paired (operator, number) elements from a numeric
// filter array. Handles single-range ([op, n]) and between ([op1, n1, op2, n2]).
func evalNumericParts(parts []json.RawMessage, attrNum float64) bool {
	const pairSize = 2

	if len(parts) < pairSize || len(parts)%pairSize != 0 {
		return false
	}

	for i := 0; i+1 < len(parts); i += pairSize {
		var op string
		var num float64

		if json.Unmarshal(parts[i], &op) != nil {
			return false
		}

		if json.Unmarshal(parts[i+1], &num) != nil {
			return false
		}

		if !applyNumericOp(op, attrNum, num) {
			return false
		}
	}

	return true
}

// applyNumericOp applies a single numeric comparison operator.
func applyNumericOp(op string, attrNum, threshold float64) bool {
	switch op {
	case "=":
		return attrNum == threshold
	case "!=":
		return attrNum != threshold
	case "<":
		return attrNum < threshold
	case "<=":
		return attrNum <= threshold
	case ">":
		return attrNum > threshold
	case ">=":
		return attrNum >= threshold
	default:
		return false
	}
}
