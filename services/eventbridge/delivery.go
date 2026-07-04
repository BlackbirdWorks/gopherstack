package eventbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
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
	CloudWatchLogs  CloudWatchLogsPublisher
	APIDestinations APIDestinationResolver
}

// APIDestinationResolver resolves an API-destination ARN to its concrete
// invocation config plus the connection credentials used to authenticate the
// outbound request, and throttles delivery to the destination's configured
// rate. It is implemented by the backend and consulted at delivery time.
type APIDestinationResolver interface {
	// ResolveAPIDestination returns the resolved destination, or false if the
	// ARN does not identify a known API destination.
	ResolveAPIDestination(destARN string) (*ResolvedAPIDestination, bool)
	// WaitAPIDestinationRateLimit blocks until the destination's configured
	// rate permits another request, or ctx is done. A non-positive rate is
	// unlimited.
	WaitAPIDestinationRateLimit(ctx context.Context, destARN string, ratePerSecond int)
}

// ResolvedAPIDestination is the flattened, delivery-ready view of an API
// destination and its associated connection auth.
type ResolvedAPIDestination struct {
	OAuth                 *ResolvedOAuth
	HTTPMethod            string
	Endpoint              string
	AuthType              string
	APIKeyName            string
	APIKeyValue           string
	BasicUsername         string
	BasicPassword         string
	HeaderParameters      []ConnectionHeaderParameter
	QueryStringParameters []ConnectionQueryStringParameter
	BodyParameters        []ConnectionBodyParameter
	RateLimitPerSecond    int
}

// ResolvedOAuth carries the OAuth client-credentials configuration used to mint
// a bearer token for an API destination.
type ResolvedOAuth struct {
	AuthorizationEndpoint string
	HTTPMethod            string
	ClientID              string
	ClientSecret          string
	HeaderParameters      []ConnectionHeaderParameter
	QueryStringParameters []ConnectionQueryStringParameter
	BodyParameters        []ConnectionBodyParameter
}

// CloudWatchLogsPublisher delivers an event to a CloudWatch Logs log group.
type CloudWatchLogsPublisher interface {
	PutLogEvents(ctx context.Context, logGroupName, logStreamName string, logEvents []any) error
}

// deliverScheduledRule delivers a scheduled-rule synthetic event directly to the
// rule's targets, bypassing pattern matching. On real AWS, scheduled rules invoke
// targets directly; they are NOT routed through event pattern matching.
func (b *InMemoryBackend) deliverScheduledRule(
	ctx context.Context,
	rule Rule,
	busName, region, detailType string,
) {
	const detail = `{"scheduled":true}`

	b.mu.Lock("deliverScheduledRule")
	storedTargets := b.targets[region][b.targetKey(busName, rule.Name)]
	snapped := snapshotTargets(storedTargets)
	accountID := b.accountID
	dt := *b.deliveryTargets
	timeout := b.deliveryTimeout
	// Log the event so diagnostic callers (GetEventLog) can observe it.
	eventID := uuid.NewString()
	b.eventLog = append(b.eventLog, EventLogEntry{
		ID:           eventID,
		Source:       "aws.events",
		DetailType:   detailType,
		Detail:       detail,
		EventBusName: busName,
		Time:         time.Now(),
	})
	if len(b.eventLog) > maxEventLogSize {
		b.eventLog = b.eventLog[len(b.eventLog)-maxEventLogSize:]
	}
	b.mu.Unlock()

	if len(snapped) == 0 {
		return
	}

	entry := EventEntry{
		Source:       "aws.events",
		DetailType:   detailType,
		Detail:       detail,
		EventBusName: busName,
	}
	envelope := buildDeliveryEnvelope(entry, accountID, region)

	var wg sync.WaitGroup
	for _, t := range snapped {
		target := t
		wg.Go(func() {
			deliverToTargetBounded(ctx, target, envelope, dt, timeout)
		})
	}
	wg.Wait()
}

// deliverEvents fan-outs events to matching rule targets.
// It runs asynchronously and does not block PutEvents.
func (b *InMemoryBackend) deliverEvents(
	ctx context.Context,
	region string,
	entries []EventEntry,
	targets DeliveryTargets,
	timeout time.Duration,
) {
	groups := b.buildDeliveryPlan(region, entries)

	// Deliver outside the lock. Targets within a rule run concurrently; each
	// gets its own bounded context so a hung downstream service cannot block
	// the goroutine beyond the configured timeout.
	for _, g := range groups {
		var wg sync.WaitGroup
		for _, t := range g.targets {
			target := t
			envelope := g.envelope
			wg.Go(func() {
				deliverToTargetBounded(ctx, target, envelope, targets, timeout)
			})
		}
		wg.Wait()
	}
}

// deliveryGroup is one matched rule's delivery work: a shared event envelope and
// the snapshot of targets to deliver it to.
type deliveryGroup struct {
	envelope map[string]any
	targets  []*Target
}

// buildDeliveryPlan matches each entry against its bus under the read lock and
// returns one delivery group per matched rule. Rather than deep-copying every
// bus's rules, index and targets on the hot path, it snapshots only the matched
// rules' targets, bounding per-PutEvents work to the buses the entries reference
// and the rules that matched. Snapshotting under the lock lets delivery run
// without racing concurrent mutations (PutRule/DeleteRule/PutTargets/RemoveTargets).
// Groups preserve the original rule-by-rule, entry-by-entry ordering.
func (b *InMemoryBackend) buildDeliveryPlan(region string, entries []EventEntry) []deliveryGroup {
	b.mu.RLock("buildDeliveryPlan")
	defer b.mu.RUnlock()

	accountID := b.accountID
	// Read directly without lazy-init: buildDeliveryPlan holds only RLock, so
	// calling ruleIndexStore/targetsStore (which write on nil) races with other
	// concurrent deliverEvents goroutines. Nil map reads are safe in Go.
	ruleIndex := b.ruleIndex[region]
	targetsStore := b.targets[region]

	var groups []deliveryGroup
	for _, entry := range entries {
		busName := entry.EventBusName
		if busName == "" {
			busName = defaultEventBusName
		}

		busKey := ebBusKey(busName)
		eventEnvelope := buildEventEnvelope(entry)
		for _, rule := range indexedRulesForEvent(ruleIndex[busKey], entry.Source, entry.DetailType) {
			if rule.State != "ENABLED" || rule.EventPattern == "" {
				continue
			}

			if !matchCompiledPattern(rule.compiledPattern, eventEnvelope) {
				continue
			}

			storedTargets := targetsStore[b.targetKey(busName, rule.Name)]
			if len(storedTargets) == 0 {
				continue
			}

			// Build the delivery envelope once per matched rule so all targets
			// for this rule share the same event id, matching AWS behaviour.
			groups = append(groups, deliveryGroup{
				envelope: buildDeliveryEnvelope(entry, accountID, region),
				targets:  snapshotTargets(storedTargets),
			})
		}
	}

	return groups
}

// snapshotTargets returns copies of the stored target structs so delivery cannot
// race a concurrent PutTargets/RemoveTargets mutating the stored values.
func snapshotTargets(stored map[string]*Target) []*Target {
	out := make([]*Target, 0, len(stored))
	for _, t := range stored {
		targetCopy := *t
		out = append(out, &targetCopy)
	}

	return out
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
	case isCloudWatchLogsARN(targetARN):
		return deliverToCloudWatchLogs(ctx, dt.CloudWatchLogs, targetARN, payload)
	case isAPIDestinationARN(targetARN):
		return deliverToAPIDestination(ctx, dt.APIDestinations, target, payload)
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
//
// Substitution is context-aware so the output is valid JSON: a placeholder that
// sits inside a JSON string literal (e.g. "<var>") receives the escaped scalar
// content of the value, while a placeholder in value position (e.g. {"k":<var>})
// receives the full JSON encoding of the value (adding quotes for strings). This
// matches AWS EventBridge, which produces valid JSON for both forms rather than
// splicing raw, unquoted strings into value positions.
func applyInputTransformer(t *InputTransformer, envelope map[string]any) string {
	vars := make(map[string]any, len(t.InputPathsMap))
	for varName, path := range t.InputPathsMap {
		vars[varName] = jsonPathExtract(path, envelope)
	}

	return substituteInputTemplate(t.InputTemplate, vars)
}

// substituteInputTemplate scans template byte-by-byte, tracking whether the
// cursor is inside a JSON string literal, and replaces every <name> placeholder
// whose name is present in vars using the appropriate rendering for its context.
func substituteInputTemplate(template string, vars map[string]any) string {
	var out strings.Builder
	out.Grow(len(template))

	inString := false
	escaped := false

	for i := 0; i < len(template); {
		c := template[i]

		if c == '<' && !escaped {
			if name, adv, ok := matchTemplatePlaceholder(template, i, vars); ok {
				if inString {
					out.WriteString(inputTransformerStringValue(vars[name]))
				} else {
					out.WriteString(inputTransformerValue(vars[name]))
				}
				i += adv

				continue
			}
		}

		out.WriteByte(c)
		switch {
		case escaped:
			escaped = false
		case inString && c == '\\':
			escaped = true
		case c == '"':
			inString = !inString
		}
		i++
	}

	return out.String()
}

// matchTemplatePlaceholder reports whether template[start:] begins with a
// <name> placeholder whose name is a key in vars. It returns the name and the
// number of bytes the placeholder spans (including the angle brackets).
func matchTemplatePlaceholder(template string, start int, vars map[string]any) (string, int, bool) {
	j := start + 1
	for j < len(template) {
		ch := template[j]
		if ch == '>' {
			name := template[start+1 : j]
			if name == "" {
				return "", 0, false
			}
			if _, ok := vars[name]; !ok {
				return "", 0, false
			}

			return name, j - start + 1, true
		}
		if !isPlaceholderNameByte(ch) {
			return "", 0, false
		}
		j++
	}

	return "", 0, false
}

func isPlaceholderNameByte(ch byte) bool {
	return ch >= 'A' && ch <= 'Z' ||
		ch >= 'a' && ch <= 'z' ||
		ch >= '0' && ch <= '9' ||
		ch == '_'
}

// inputTransformerStringValue renders v for a placeholder that sits inside a
// JSON string literal: the escaped scalar content, with no surrounding quotes.
// A missing value (nil) renders as the empty string.
func inputTransformerStringValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return jsonEscapeInner(val)
	default:
		encoded := marshalNoHTMLEscape(val)

		return jsonEscapeInner(string(encoded))
	}
}

// inputTransformerValue renders v for a placeholder in JSON value position: the
// full JSON encoding (strings gain surrounding quotes, objects/arrays/numbers
// are spliced verbatim). A missing value (nil) renders as an empty JSON string.
func inputTransformerValue(v any) string {
	if v == nil {
		return `""`
	}

	return string(marshalNoHTMLEscape(v))
}

// jsonEscapeInner returns the JSON-escaped form of s without the surrounding
// quotation marks, suitable for splicing into an existing string literal.
func jsonEscapeInner(s string) string {
	encoded := string(marshalNoHTMLEscape(s))
	encoded = strings.TrimPrefix(encoded, `"`)
	encoded = strings.TrimSuffix(encoded, `"`)

	return encoded
}

// marshalNoHTMLEscape JSON-encodes v without escaping <, >, and & so the output
// mirrors AWS, which does not HTML-escape input-transformer substitutions.
func marshalNoHTMLEscape(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil
	}

	return bytes.TrimRight(buf.Bytes(), "\n")
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

func isCloudWatchLogsARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:logs:")
}

func isAPIDestinationARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:events:") && strings.Contains(arn, ":api-destination/")
}

func deliverToCloudWatchLogs(ctx context.Context, svc CloudWatchLogsPublisher, arn, payload string) bool {
	if svc == nil {
		return false
	}
	parts := strings.Split(arn, ":")
	if len(parts) < 7 || parts[5] != "log-group" {
		return false
	}
	logGroupName := parts[6]

	err := svc.PutLogEvents(ctx, logGroupName, "EventBridge", []any{payload})

	return err != nil
}

const apiDestTimeout = 5 * time.Second

// deliverToAPIDestination performs a real HTTP invocation of an API destination:
// it resolves the destination's endpoint/method/rate-limit and the connection's
// auth, applies the connection's invocation HTTP parameters (headers, query
// string, and body merges), signs the request per the connection's
// authorization type, honours the destination's rate limit, and reports whether
// delivery failed (a transport error or a >=400 response), which drives
// retry/DLQ handling upstream.
func deliverToAPIDestination(
	ctx context.Context,
	resolver APIDestinationResolver,
	target *Target,
	payload string,
) bool {
	log := logger.Load(ctx)
	if resolver == nil {
		log.WarnContext(ctx, "EventBridge: no API-destination resolver configured", "arn", target.Arn)

		return true
	}

	dest, ok := resolver.ResolveAPIDestination(target.Arn)
	if !ok {
		log.WarnContext(ctx, "EventBridge: API destination not found", "arn", target.Arn)

		return true
	}

	// Throttle to the destination's configured invocation rate before sending.
	resolver.WaitAPIDestinationRateLimit(ctx, target.Arn, dest.RateLimitPerSecond)

	body := mergeBodyParameters(payload, dest.BodyParameters)

	method := dest.HTTPMethod
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(ctx, method, dest.Endpoint, strings.NewReader(body))
	if err != nil {
		log.WarnContext(ctx, "EventBridge: failed to build API-destination request", "arn", target.Arn, "error", err)

		return true
	}
	req.Header.Set("Content-Type", "application/json")

	applyConnectionHTTPParameters(req, dest.HeaderParameters, dest.QueryStringParameters)

	if authErr := applyAPIDestinationAuth(ctx, req, dest); authErr != nil {
		log.WarnContext(ctx, "EventBridge: failed to apply API-destination auth", "arn", target.Arn, "error", authErr)

		return true
	}

	client := &http.Client{Timeout: apiDestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		log.WarnContext(ctx, "EventBridge: API-destination request failed", "arn", target.Arn, "error", err)

		return true
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode >= http.StatusBadRequest
}

// mergeBodyParameters merges a connection's invocation body parameters into the
// event payload. When the payload is a JSON object the parameters are added as
// top-level keys (matching AWS, which merges connection body parameters into the
// request body); otherwise the payload is returned unchanged.
func mergeBodyParameters(payload string, params []ConnectionBodyParameter) string {
	if len(params) == 0 {
		return payload
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(payload), &obj); err != nil || obj == nil {
		return payload
	}

	for _, p := range params {
		obj[p.Key] = p.Value
	}

	merged, err := json.Marshal(obj)
	if err != nil {
		return payload
	}

	return string(merged)
}

// applyConnectionHTTPParameters adds a connection's invocation header and
// query-string parameters to the outbound request.
func applyConnectionHTTPParameters(
	req *http.Request,
	headers []ConnectionHeaderParameter,
	queries []ConnectionQueryStringParameter,
) {
	for _, h := range headers {
		req.Header.Set(h.Key, h.Value)
	}

	if len(queries) > 0 {
		q := req.URL.Query()
		for _, qp := range queries {
			q.Set(qp.Key, qp.Value)
		}
		req.URL.RawQuery = q.Encode()
	}
}

// applyAPIDestinationAuth signs the outbound request according to the resolved
// connection's authorization type.
func applyAPIDestinationAuth(ctx context.Context, req *http.Request, dest *ResolvedAPIDestination) error {
	switch dest.AuthType {
	case connectionAuthAPIKey:
		if dest.APIKeyName != "" {
			req.Header.Set(dest.APIKeyName, dest.APIKeyValue)
		}
	case connectionAuthBasic:
		req.SetBasicAuth(dest.BasicUsername, dest.BasicPassword)
	case connectionAuthOAuth:
		if dest.OAuth == nil {
			return nil
		}
		token, err := fetchOAuthToken(ctx, dest.OAuth)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return nil
}

// fetchOAuthToken performs an OAuth 2.0 client-credentials grant against the
// connection's authorization endpoint and returns the access token. Client
// credentials are sent via HTTP Basic auth and the grant_type in the form body,
// mirroring the common client-credentials flow AWS uses for OAuth connections.
func fetchOAuthToken(ctx context.Context, oauth *ResolvedOAuth) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	for _, bp := range oauth.BodyParameters {
		form.Set(bp.Key, bp.Value)
	}

	method := oauth.HTTPMethod
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(
		ctx, method, oauth.AuthorizationEndpoint, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(oauth.ClientID, oauth.ClientSecret)

	applyConnectionHTTPParameters(req, oauth.HeaderParameters, oauth.QueryStringParameters)

	client := &http.Client{Timeout: apiDestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOAuthResponseBytes))
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return "", fmt.Errorf("%w: OAuth token endpoint returned %d", ErrInvalidParameter, resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err = json.Unmarshal(data, &tokenResp); err != nil {
		return "", err
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("%w: OAuth token endpoint returned no access_token", ErrInvalidParameter)
	}

	return tokenResp.AccessToken, nil
}

const maxOAuthResponseBytes = 1 << 20
