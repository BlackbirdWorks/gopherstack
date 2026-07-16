package eventbridge

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

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
	var storedTargets *store.Table[Target]
	if regionTargets := b.targets[region]; regionTargets != nil {
		storedTargets = regionTargets[b.targetKey(busName, rule.Name)]
	}
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
			if storedTargets == nil || storedTargets.Len() == 0 {
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
// race a concurrent PutTargets/RemoveTargets mutating the stored values. A nil
// stored table (region/rule never touched) yields an empty, non-nil slice.
func snapshotTargets(stored *store.Table[Target]) []*Target {
	if stored == nil {
		return nil
	}

	all := stored.All()
	out := make([]*Target, 0, len(all))
	for _, t := range all {
		targetCopy := *t
		out = append(out, &targetCopy)
	}

	return out
}

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
