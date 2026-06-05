package eventbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// inputPathsMapKeyRe validates InputPathsMap variable names per AWS spec.
var inputPathsMapKeyRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// LambdaInvoker can invoke a Lambda function by name/ARN with a payload.
type LambdaInvoker interface {
	InvokeFunction(
		ctx context.Context,
		name string,
		invocationType string,
		payload []byte,
	) ([]byte, int, error)
}

// SQSSender can send a message to an SQS queue by URL or ARN.
type SQSSender interface {
	SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error
}

// SNSPublisher can publish a message to an SNS topic by ARN.
type SNSPublisher interface {
	PublishToTopic(ctx context.Context, topicARN, message string) error
}

// KinesisFirehosePublisher can put records to a Kinesis Data Firehose delivery stream.
type KinesisFirehosePublisher interface {
	PutRecord(ctx context.Context, deliveryStreamARN, data string) error
}

// KinesisStreamPublisher can put records to a Kinesis Data Stream.
type KinesisStreamPublisher interface {
	PutRecord(ctx context.Context, streamARN, partitionKey, data string) error
}

// ECSTaskRunner can run an ECS task.
type ECSTaskRunner interface {
	RunTask(ctx context.Context, clusterARN string, payload []byte) error
}

// StepFunctionsExecutor can start a Step Functions state machine execution.
type StepFunctionsExecutor interface {
	// StartExecution starts an execution of the state machine identified by stateMachineARN.
	// The name may be empty (the backend will generate one). input is a JSON string.
	StartExecution(stateMachineARN, name, input string) error
}

// DeliveryTargets holds optional service references for event fan-out.
type DeliveryTargets struct {
	Lambda          LambdaInvoker
	SQS             SQSSender
	SNS             SNSPublisher
	KinesisFirehose KinesisFirehosePublisher
	KinesisStream   KinesisStreamPublisher
	ECS             ECSTaskRunner
	StepFunctions   StepFunctionsExecutor
}

// deliverEvents fan-outs events to matching rule targets.
// It runs asynchronously and does not block PutEvents.
func (b *InMemoryBackend) deliverEvents(
	ctx context.Context,
	entries []EventEntry,
	targets DeliveryTargets,
	timeout time.Duration,
) {
	b.mu.RLock("deliverEvents")
	// Deep copy rules and targets within the lock so concurrent mutations to the
	// inner maps (PutRule/DeleteRule/PutTargets/RemoveTargets) cannot race with
	// the iteration below.
	busRules := deepCopyBusRules(b.rules)
	busRuleIndex := deepCopyRuleIndex(b.ruleIndex, busRules)
	busTargets := deepCopyBusTargets(b.targets)
	accountID := b.accountID
	region := b.region
	b.mu.RUnlock()

	for _, entry := range entries {
		busName := entry.EventBusName
		if busName == "" {
			busName = defaultEventBusName
		}

		eventEnvelope := buildEventEnvelope(entry)
		rules := indexedRulesForEvent(busRuleIndex[busName], entry.Source, entry.DetailType)
		for _, rule := range rules {
			if rule.State != "ENABLED" {
				continue
			}

			if rule.EventPattern == "" {
				continue
			}

			if !matchCompiledPattern(rule.compiledPattern, eventEnvelope) {
				continue
			}

			// Build the delivery envelope once per matched rule so all targets
			// for this rule share the same event id, matching AWS behaviour.
			deliveryEnvelope := buildDeliveryEnvelope(entry, accountID, region)

			// Deliver to all targets for this rule. Each target gets its own
			// bounded context so a hung downstream service cannot block the
			// goroutine beyond the configured timeout.
			key := b.targetKey(busName, rule.Name)
			var wg sync.WaitGroup
			for _, t := range busTargets[key] {
				target := t
				wg.Go(func() {
					deliverToTargetBounded(ctx, target, deliveryEnvelope, targets, timeout)
				})
			}
			wg.Wait()
		}
	}
}

const (
	// defaultMaxRetryAttempts matches AWS EventBridge default retry attempts.
	defaultMaxRetryAttempts = 2
	// defaultMaxEventAgeSeconds matches AWS EventBridge default maximum event age (3600s = 1h).
	defaultMaxEventAgeSeconds = 3600
)

// deliverToTargetBounded delivers a single event to a single target, applying a per-call
// timeout when timeout > 0, with retry logic from target.RetryPolicy.
func deliverToTargetBounded(
	ctx context.Context,
	target *Target,
	envelope map[string]any,
	dt DeliveryTargets,
	timeout time.Duration,
) {
	maxAttempts := defaultMaxRetryAttempts
	maxAgeSeconds := defaultMaxEventAgeSeconds

	if target.RetryPolicy != nil {
		if target.RetryPolicy.MaximumRetryAttempts >= 0 {
			maxAttempts = target.RetryPolicy.MaximumRetryAttempts
		}
		if target.RetryPolicy.MaximumEventAgeInSeconds > 0 {
			maxAgeSeconds = target.RetryPolicy.MaximumEventAgeInSeconds
		}
	}

	eventAge := extractEventAge(envelope)

	for attempt := 0; attempt <= maxAttempts; attempt++ {
		if int(eventAge.Seconds()) > maxAgeSeconds {
			sendToDLQ(ctx, target, envelope, dt, "MaximumEventAgeExceeded")

			return
		}

		var delivErr bool
		if timeout <= 0 {
			delivErr = deliverToTarget(ctx, target, envelope, dt)
		} else {
			tCtx, cancel := context.WithTimeout(ctx, timeout)
			delivErr = deliverToTarget(tCtx, target, envelope, dt)
			cancel()
		}

		if !delivErr {
			return
		}

		if attempt == maxAttempts {
			sendToDLQ(ctx, target, envelope, dt, "DeliveryFailure")

			return
		}
	}
}

// extractEventAge returns the age of the event from the envelope's "time" field.
func extractEventAge(envelope map[string]any) time.Duration {
	timeVal, ok := envelope["time"].(string)
	if !ok {
		return 0
	}

	t, err := time.Parse(time.RFC3339, timeVal)
	if err != nil {
		return 0
	}

	age := time.Since(t)
	if age < 0 {
		return 0
	}

	return age
}

// sendToDLQ sends an event to the dead-letter queue if configured.
func sendToDLQ(
	ctx context.Context,
	target *Target,
	envelope map[string]any,
	dt DeliveryTargets,
	reason string,
) {
	if target.DeadLetterConfig == nil || target.DeadLetterConfig.Arn == "" {
		return
	}
	if dt.SQS == nil {
		return
	}

	log := logger.Load(ctx)
	payload, _ := json.Marshal(envelope)
	dlqARN := target.DeadLetterConfig.Arn

	if err := dt.SQS.SendMessageToQueue(ctx, dlqARN, string(payload)); err != nil {
		log.WarnContext(ctx, "EventBridge: failed to send event to DLQ",
			"dlq", dlqARN, "reason", reason, "error", err)
	}
}

// deepCopyBusRules returns a deep copy of the bus-to-rules map so that the
// caller can iterate it without holding any lock.
func deepCopyBusRules(src map[string]map[string]*Rule) map[string]map[string]*Rule {
	dst := make(map[string]map[string]*Rule, len(src))
	for bus, ruleMap := range src {
		cp := make(map[string]*Rule, len(ruleMap))
		for k, v := range ruleMap {
			r := *v
			cp[k] = &r
		}
		dst[bus] = cp
	}

	return dst
}

func deepCopyRuleIndex(
	src map[string]map[ruleIndexKey]map[string]*Rule,
	rules map[string]map[string]*Rule,
) map[string]map[ruleIndexKey]map[string]*Rule {
	dst := make(map[string]map[ruleIndexKey]map[string]*Rule, len(src))
	for bus, indexMap := range src {
		copiedBusRules := rules[bus]
		copiedIndexMap := make(map[ruleIndexKey]map[string]*Rule, len(indexMap))
		for indexKey, ruleMap := range indexMap {
			copiedRuleMap := make(map[string]*Rule, len(ruleMap))
			for name := range ruleMap {
				rule, exists := copiedBusRules[name]
				if exists {
					copiedRuleMap[name] = rule
				}
			}
			if len(copiedRuleMap) > 0 {
				copiedIndexMap[indexKey] = copiedRuleMap
			}
		}
		if len(copiedIndexMap) > 0 {
			dst[bus] = copiedIndexMap
		}
	}

	return dst
}

func indexedRulesForEvent(
	index map[ruleIndexKey]map[string]*Rule,
	source, detailType string,
) []*Rule {
	if len(index) == 0 {
		return nil
	}

	candidateKeys := []ruleIndexKey{
		{source: source, detailType: detailType},
		{source: source, detailType: ruleIndexAny},
		{source: ruleIndexAny, detailType: detailType},
		{source: ruleIndexAny, detailType: ruleIndexAny},
	}

	rulesByName := make(map[string]*Rule)
	for _, key := range candidateKeys {
		bucket := index[key]
		maps.Copy(rulesByName, bucket)
	}

	rules := make([]*Rule, 0, len(rulesByName))
	for _, rule := range rulesByName {
		rules = append(rules, rule)
	}

	return rules
}

// deepCopyBusTargets returns a deep copy of the target-key-to-targets map so
// that the caller can iterate it without holding any lock.
func deepCopyBusTargets(src map[string]map[string]*Target) map[string]map[string]*Target {
	dst := make(map[string]map[string]*Target, len(src))
	for key, targetMap := range src {
		cp := make(map[string]*Target, len(targetMap))
		for k, v := range targetMap {
			t := *v
			cp[k] = &t
		}
		dst[key] = cp
	}

	return dst
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
// Returns true if delivery failed (triggering retry/DLQ).
func deliverToTarget(
	ctx context.Context,
	target *Target,
	envelope map[string]any,
	dt DeliveryTargets,
) bool {
	targetARN := target.Arn
	payload := buildPayload(target, envelope)

	switch {
	case isLambdaARN(targetARN):
		return deliverToLambda(ctx, dt.Lambda, targetARN, payload)
	case isSQSARN(targetARN):
		return deliverToSQS(ctx, dt.SQS, targetARN, payload)
	case isSNSARN(targetARN):
		return deliverToSNS(ctx, dt.SNS, targetARN, payload)
	case isKinesisFirehoseARN(targetARN):
		return deliverToKinesisFirehose(ctx, dt.KinesisFirehose, targetARN, payload)
	case isKinesisStreamARN(targetARN):
		return deliverToKinesisStream(ctx, dt.KinesisStream, targetARN, payload)
	case isECSARN(targetARN):
		return deliverToECS(ctx, dt.ECS, targetARN, payload)
	case isStateMachineARN(targetARN):
		return deliverToStepFunctions(ctx, dt.StepFunctions, targetARN, payload)
	default:
		logger.Load(ctx).
			WarnContext(ctx, "EventBridge: unsupported target ARN type", "arn", targetARN)
	}

	return false
}

func deliverToLambda(ctx context.Context, svc LambdaInvoker, arn, payload string) bool {
	if svc == nil {
		return false
	}
	if _, _, err := svc.InvokeFunction(ctx, arn, "Event", []byte(payload)); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "EventBridge failed to invoke Lambda target", "arn", arn, "error", err)

		return true
	}

	return false
}

func deliverToSQS(ctx context.Context, svc SQSSender, arn, payload string) bool {
	if svc == nil {
		return false
	}
	if err := svc.SendMessageToQueue(ctx, arn, payload); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "EventBridge failed to deliver to SQS target", "arn", arn, "error", err)

		return true
	}

	return false
}

func deliverToSNS(ctx context.Context, svc SNSPublisher, arn, payload string) bool {
	if svc == nil {
		return false
	}
	if err := svc.PublishToTopic(ctx, arn, payload); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "EventBridge failed to publish to SNS target", "arn", arn, "error", err)

		return true
	}

	return false
}

func deliverToKinesisFirehose(
	ctx context.Context,
	svc KinesisFirehosePublisher,
	arn, payload string,
) bool {
	if svc == nil {
		return false
	}
	if err := svc.PutRecord(ctx, arn, payload); err != nil {
		logger.Load(ctx).WarnContext(ctx, "EventBridge failed to put record to Kinesis Firehose",
			"arn", arn, "error", err)

		return true
	}

	return false
}

func deliverToKinesisStream(
	ctx context.Context,
	svc KinesisStreamPublisher,
	arn, payload string,
) bool {
	if svc == nil {
		return false
	}
	partitionKey := uuid.New().String()
	if err := svc.PutRecord(ctx, arn, partitionKey, payload); err != nil {
		logger.Load(ctx).WarnContext(ctx, "EventBridge failed to put record to Kinesis Data Stream",
			"arn", arn, "error", err)

		return true
	}

	return false
}

func deliverToECS(ctx context.Context, svc ECSTaskRunner, arn, payload string) bool {
	if svc == nil {
		return false
	}
	if err := svc.RunTask(ctx, arn, []byte(payload)); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "EventBridge failed to run ECS task", "arn", arn, "error", err)

		return true
	}

	return false
}

// buildPayload constructs the message payload for a target from a pre-built event envelope.
// Priority: Input override → InputPath → InputTransformer → full event envelope.
func buildPayload(target *Target, envelope map[string]any) string {
	if target.Input != "" {
		return target.Input
	}

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

	resources := entry.Resources
	if resources == nil {
		resources = []string{}
	}

	return map[string]any{
		"version":     "0",
		"id":          uuid.New().String(),
		"source":      entry.Source,
		"account":     accountID,
		"time":        eventTime.UTC().Format(time.RFC3339),
		"region":      region,
		"resources":   resources,
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

// validateInputTransformer checks that all InputPathsMap keys satisfy the
// AWS constraint ([A-Za-z0-9_]+). Returns an error if any key is invalid.
func validateInputTransformer(t *InputTransformer) error {
	for key := range t.InputPathsMap {
		if !inputPathsMapKeyRe.MatchString(key) {
			return fmt.Errorf(
				"%w: InputPathsMap key %q must match [A-Za-z0-9_]+",
				ErrInvalidParameter,
				key,
			)
		}
	}

	return nil
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

// jsonPathExtract resolves a JSONPath expression against the given event envelope.
// Supports dot-notation ($.source, $.detail.key) and array indexing ($.detail.items[0]).
// Returns nil if the path cannot be resolved.
func jsonPathExtract(path string, data map[string]any) any {
	if path == "$" || path == "" {
		return data
	}

	if !strings.HasPrefix(path, "$.") {
		return nil
	}

	parts := splitJSONPathParts(path[2:])
	var current any = data

	for _, part := range parts {
		// Check for array index suffix, e.g. "items[0]".
		key, idx, hasIndex := parseArrayIndex(part)

		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}

		val, ok := m[key]
		if !ok {
			return nil
		}

		if !hasIndex {
			current = val

			continue
		}

		arr, ok := val.([]any)
		if !ok || idx < 0 || idx >= len(arr) {
			return nil
		}

		current = arr[idx]
	}

	return current
}

// splitJSONPathParts splits a dot-separated path into segments while preserving
// array index brackets as part of the segment (e.g. "items[0].name" → ["items[0]", "name"]).
func splitJSONPathParts(path string) []string {
	return strings.Split(path, ".")
}

const decimalBase = 10

// parseArrayIndex parses a path segment like "items[0]" into key="items", idx=0, hasIndex=true.
// Returns hasIndex=false when no bracket expression is found.
func parseArrayIndex(segment string) (string, int, bool) {
	open := strings.LastIndex(segment, "[")
	if open < 0 {
		return segment, 0, false
	}

	closeIdx := strings.Index(segment[open:], "]")
	if closeIdx < 0 {
		return segment, 0, false
	}

	indexStr := segment[open+1 : open+closeIdx]
	n := 0

	for _, ch := range indexStr {
		if ch < '0' || ch > '9' {
			return segment, 0, false
		}

		n = n*decimalBase + int(ch-'0')
	}

	return segment[:open], n, true
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

// isKinesisFirehoseARN returns true if the ARN identifies a Kinesis Data Firehose delivery stream.
func isKinesisFirehoseARN(arn string) bool {
	return strings.Contains(arn, ":firehose:")
}

// isKinesisStreamARN returns true if the ARN identifies a Kinesis Data Stream.
func isKinesisStreamARN(arn string) bool {
	return strings.Contains(arn, ":kinesis:") && strings.Contains(arn, ":stream/")
}

// isECSARN returns true if the ARN identifies an ECS cluster or task.
func isECSARN(arn string) bool {
	return strings.Contains(arn, ":ecs:")
}

// isStateMachineARN returns true if the ARN identifies a Step Functions state machine.
func isStateMachineARN(arn string) bool {
	return strings.Contains(arn, ":states:") && strings.Contains(arn, ":stateMachine:")
}

func deliverToStepFunctions(
	ctx context.Context,
	svc StepFunctionsExecutor,
	arn, payload string,
) bool {
	if svc == nil {
		return false
	}
	if err := svc.StartExecution(arn, "", payload); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "EventBridge failed to start Step Functions execution", "arn", arn, "error", err)

		return true
	}

	return false
}
