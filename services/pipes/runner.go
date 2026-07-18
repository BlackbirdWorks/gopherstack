package pipes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var (
	// ErrUnsupportedPipeTarget is returned when a pipe target ARN service is not
	// handled by the runner.
	ErrUnsupportedPipeTarget = errors.New("pipes: unsupported target ARN")
	// ErrUnsupportedPipeEnrichment is returned when a pipe enrichment ARN service
	// is not handled by the runner.
	ErrUnsupportedPipeEnrichment = errors.New("pipes: unsupported enrichment ARN")
	// ErrEnrichmentInvokerUnwired is returned when an enrichment ARN is recognized
	// but the corresponding invoker (Lambda/StepFunctions) has not been wired into
	// the runner. This surfaces a real misconfiguration instead of silently
	// dropping the event.
	ErrEnrichmentInvokerUnwired = errors.New("pipes: enrichment invoker not configured")
	// ErrTargetInvokerUnwired is returned when a target ARN service is recognized but
	// the corresponding invoker has not been wired into the runner.
	ErrTargetInvokerUnwired = errors.New("pipes: target invoker not configured")
)

const (
	pipeRunnerTickInterval = 1 * time.Second
	pipeDefaultBatchSize   = 10
	maxPipeWorkers         = 64

	// Pipes invocation type constants.
	invocationTypeFireAndForget   = "FIRE_AND_FORGET"
	invocationTypeRequestResponse = "REQUEST_RESPONSE"
)

// lambdaPipesInvocationType maps Pipes invocation types to Lambda invocation types.
func lambdaPipesInvocationType() map[string]string {
	return map[string]string{
		invocationTypeFireAndForget:   "Event",
		invocationTypeRequestResponse: "RequestResponse",
	}
}

// sfnPipesInvocationType maps Pipes invocation types to Step Functions invocation types.
func sfnPipesInvocationType() map[string]string {
	return map[string]string{
		invocationTypeFireAndForget:   invocationTypeFireAndForget,
		invocationTypeRequestResponse: invocationTypeRequestResponse,
	}
}

// SQSMessage is a single SQS message pulled by the pipe runner.
type SQSMessage struct {
	Attributes    map[string]string
	MessageID     string
	ReceiptHandle string
	Body          string
	MD5OfBody     string
}

// SQSReader reads and deletes SQS messages for a pipe source.
type SQSReader interface {
	ReceivePipeMessages(queueARN string, maxMessages int) ([]*SQSMessage, error)
	DeletePipeMessages(queueARN string, receiptHandles []string) error
}

// PipeLambdaInvoker invokes a Lambda function with a payload.
type PipeLambdaInvoker interface {
	InvokeFunction(
		ctx context.Context,
		name string,
		invocationType string,
		payload []byte,
	) ([]byte, int, error)
}

// PipeStepFunctionsStarter starts a StepFunctions state machine execution.
type PipeStepFunctionsStarter interface {
	StartExecution(stateMachineARN, name, input string) error
}

// SNSPublisher publishes a message to an SNS topic.
type SNSPublisher interface {
	PublishMessage(ctx context.Context, topicARN, message string) error
}

// SQSSender sends a message to an SQS queue.
type SQSSender interface {
	SendMessage(
		ctx context.Context,
		queueARN, body, groupID, dedupID string,
	) error
}

// PipeKinesisPutter puts a record into a Kinesis stream.
type PipeKinesisPutter interface {
	PutRecord(ctx context.Context, streamARN, partitionKey string, data []byte) error
}

// PipeEventBridgePutter puts events to an EventBridge event bus.
type PipeEventBridgePutter interface {
	PutEvents(ctx context.Context, eventBusARN string, events []map[string]any) error
}

// PipeCloudWatchLogsPutter puts log events to a CloudWatch Logs log group.
type PipeCloudWatchLogsPutter interface {
	PutLogEvents(
		ctx context.Context,
		logGroupARN, logStreamName string,
		messages []string,
	) error
}

// PipeFirehosePutter puts a record into a Kinesis Data Firehose delivery stream.
type PipeFirehosePutter interface {
	PutRecord(ctx context.Context, deliveryStreamARN string, data []byte) error
}

// Runner polls pipe sources and forwards records to pipe targets for RUNNING pipes.
type Runner struct {
	sqsReader SQSReader
	lambda    PipeLambdaInvoker
	sfn       PipeStepFunctionsStarter
	sns       SNSPublisher
	sqsSender SQSSender
	kinesis   PipeKinesisPutter
	eventBus  PipeEventBridgePutter
	cwLogs    PipeCloudWatchLogsPutter
	firehose  PipeFirehosePutter
	backend   *InMemoryBackend
	sem       chan struct{}
	done      chan struct{}
	doneMu    sync.RWMutex
	wg        sync.WaitGroup
	started   bool
}

func NewRunner(backend *InMemoryBackend) *Runner {
	return &Runner{
		backend: backend,
		sem:     make(chan struct{}, maxPipeWorkers),
		done:    make(chan struct{}),
	}
}

func (r *Runner) SetSQSReader(s SQSReader)                           { r.sqsReader = s }
func (r *Runner) SetLambdaInvoker(l PipeLambdaInvoker)               { r.lambda = l }
func (r *Runner) SetStepFunctionsStarter(s PipeStepFunctionsStarter) { r.sfn = s }
func (r *Runner) SetSNSPublisher(s SNSPublisher)                     { r.sns = s }
func (r *Runner) SetSQSSender(s SQSSender)                           { r.sqsSender = s }
func (r *Runner) SetKinesisPutter(k PipeKinesisPutter)               { r.kinesis = k }
func (r *Runner) SetEventBridgePutter(e PipeEventBridgePutter)       { r.eventBus = e }
func (r *Runner) SetCloudWatchLogsPutter(c PipeCloudWatchLogsPutter) { r.cwLogs = c }
func (r *Runner) SetFirehosePutter(f PipeFirehosePutter)             { r.firehose = f }

func (r *Runner) Start(ctx context.Context) {
	var done chan struct{}
	func() {
		r.doneMu.Lock()
		defer r.doneMu.Unlock()
		if r.started {
			return
		}
		r.started = true
		done = r.done
	}()
	if done == nil {
		return
	}

	r.wg.Go(func() {
		r.run(ctx)
	})
	go func() {
		r.wg.Wait()
		close(done)
	}()
}

// Wait blocks until all runner goroutines have exited, or ctx expires.
func (r *Runner) Wait(ctx context.Context) {
	var done chan struct{}
	func() {
		r.doneMu.RLock()
		defer r.doneMu.RUnlock()
		if !r.started {
			return
		}
		done = r.done
	}()
	if done == nil {
		return
	}

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (r *Runner) run(ctx context.Context) {
	ticker := time.NewTicker(pipeRunnerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.pollAllPipes(ctx)
		}
	}
}

func (r *Runner) pollAllPipes(ctx context.Context) {
	pipes := r.backend.allRunningPipes()

	for _, p := range pipes {
		select {
		case r.sem <- struct{}{}:
		default:
			continue
		}

		r.wg.Go(func() {
			defer func() { <-r.sem }()

			r.pollPipe(ctx, p)
		})
	}
}

func (r *Runner) pollPipe(ctx context.Context, p *Pipe) {
	if isSQSARN(p.Source) {
		r.pollSQSPipe(ctx, p)
	}
}

func isSQSARN(resourceARN string) bool {
	return strings.HasPrefix(resourceARN, "arn:aws:sqs:")
}

func (r *Runner) pollSQSPipe(ctx context.Context, p *Pipe) {
	if r.sqsReader == nil {
		return
	}

	batchSize := p.effectiveBatchSize()

	msgs, err := r.sqsReader.ReceivePipeMessages(p.Source, batchSize)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to receive SQS messages",
			"pipe", p.Name, "source", p.Source, "error", err)

		return
	}

	if len(msgs) == 0 {
		return
	}

	msgs = r.applyFilters(p, msgs)
	if len(msgs) == 0 {
		return
	}

	payload, err := json.Marshal(sqsPipeEvent{Records: buildSQSRecords(p, msgs)})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to marshal SQS records", "pipe", p.Name, "error", err)

		return
	}

	// Invoke enrichment if configured. Enriched payload replaces the default one.
	if p.Enrichment != "" {
		pipeCtx := context.WithValue(ctx, regionContextKey{}, p.Region)
		r.backend.RecordEnrichmentCall(pipeCtx, p.Name)

		enriched, enrichErr := r.invokeEnrichment(ctx, p, payload)
		if enrichErr != nil {
			logger.Load(ctx).WarnContext(ctx, "pipes: enrichment invocation failed",
				"pipe", p.Name, "enrichment", p.Enrichment, "error", enrichErr)
			r.handlePipeFailure(ctx, p, msgs, payload, enrichErr)

			return
		}
		if enriched != nil {
			payload = enriched
		}
	}

	receiptHandles, invokeErr := r.invokeTargetWithPayload(ctx, p, msgs, payload)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: target invocation failed",
			"pipe", p.Name, "target", p.Target, "error", invokeErr)
		r.handlePipeFailure(ctx, p, msgs, payload, invokeErr)

		return
	}

	if delErr := r.sqsReader.DeletePipeMessages(p.Source, receiptHandles); delErr != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to delete SQS messages",
			"pipe", p.Name, "source", p.Source, "error", delErr)
	}
}

// handlePipeFailure routes a failed batch to the configured dead-letter queue. When
// a DLQ is configured and the send succeeds, the source messages are deleted so they
// are not reprocessed in a tight loop. When no DLQ is configured, the messages are
// left in the source for the source service's own redelivery/visibility handling.
func (r *Runner) handlePipeFailure(
	ctx context.Context, p *Pipe, msgs []*SQSMessage, payload []byte, cause error,
) {
	if p.DeadLetterConfig == nil || p.DeadLetterConfig.Arn == "" {
		return
	}

	dlqARN := p.DeadLetterConfig.Arn
	if sendErr := r.sendToDLQ(ctx, dlqARN, payload); sendErr != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to send to DLQ",
			"pipe", p.Name, "dlq", dlqARN, "error", sendErr, "cause", cause)

		return
	}

	receiptHandles := make([]string, len(msgs))
	for i, m := range msgs {
		receiptHandles[i] = m.ReceiptHandle
	}

	if delErr := r.sqsReader.DeletePipeMessages(p.Source, receiptHandles); delErr != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to delete source messages after DLQ send",
			"pipe", p.Name, "source", p.Source, "error", delErr)
	}
}

// sendToDLQ delivers a failed payload to the pipe's dead-letter queue. Both SQS
// queue and SNS topic dead-letter targets are supported.
func (r *Runner) sendToDLQ(ctx context.Context, dlqARN string, payload []byte) error {
	switch {
	case strings.HasPrefix(dlqARN, "arn:aws:sqs:"):
		if r.sqsSender == nil {
			return fmt.Errorf("%w: sqs DLQ %q", ErrEnrichmentInvokerUnwired, dlqARN)
		}

		return r.sqsSender.SendMessage(ctx, dlqARN, string(payload), "", "")
	case strings.HasPrefix(dlqARN, "arn:aws:sns:"):
		if r.sns == nil {
			return fmt.Errorf("%w: sns DLQ %q", ErrEnrichmentInvokerUnwired, dlqARN)
		}

		return r.sns.PublishMessage(ctx, dlqARN, string(payload))
	}

	return fmt.Errorf("%w: dead-letter target %q", ErrUnsupportedPipeTarget, dlqARN)
}

// invokeEnrichment calls the enrichment endpoint and returns the enriched payload.
func (r *Runner) invokeEnrichment(ctx context.Context, p *Pipe, payload []byte) ([]byte, error) {
	enrichARN := p.Enrichment

	switch {
	case strings.HasPrefix(enrichARN, "arn:aws:lambda:"):
		if r.lambda == nil {
			return nil, fmt.Errorf("%w: lambda enrichment %q", ErrEnrichmentInvokerUnwired, enrichARN)
		}

		invocationType := "RequestResponse"
		if p.EnrichmentParameters != nil && p.EnrichmentParameters.InputTemplate != "" {
			payload = []byte(p.EnrichmentParameters.InputTemplate)
		}

		result, _, err := r.lambda.InvokeFunction(
			ctx,
			lambdaFunctionNameFromPipeARN(enrichARN),
			invocationType,
			payload,
		)
		if err != nil {
			return nil, err
		}

		return result, nil

	case strings.HasPrefix(enrichARN, "arn:aws:states:"):
		if r.sfn == nil {
			return nil, fmt.Errorf("%w: step functions enrichment %q", ErrEnrichmentInvokerUnwired, enrichARN)
		}

		input := string(payload)
		if p.EnrichmentParameters != nil && p.EnrichmentParameters.InputTemplate != "" {
			input = p.EnrichmentParameters.InputTemplate
		}

		return nil, r.sfn.StartExecution(enrichARN, "", input)
	}

	// Enrichment ARN type is not handled by this runner — surface a real error
	// rather than silently dropping the event.
	return nil, fmt.Errorf("%w %q for pipe %q", ErrUnsupportedPipeEnrichment, enrichARN, p.Name)
}

func (r *Runner) applyFilters(p *Pipe, msgs []*SQSMessage) []*SQSMessage {
	if p.SourceParameters == nil ||
		p.SourceParameters.FilterCriteria == nil ||
		len(p.SourceParameters.FilterCriteria.Filters) == 0 {
		return msgs
	}

	var out []*SQSMessage

	for _, m := range msgs {
		if matchesAnyFilter(m, p.SourceParameters.FilterCriteria.Filters) {
			out = append(out, m)
		}
	}

	return out
}

// invokeTargetWithPayload dispatches the pre-marshalled payload to the pipe's target.
// It returns receipt handles on success so the caller can delete the source messages.
func (r *Runner) invokeTargetWithPayload(
	ctx context.Context, p *Pipe, msgs []*SQSMessage, payload []byte,
) ([]string, error) {
	receiptHandles := make([]string, len(msgs))
	for i, m := range msgs {
		receiptHandles[i] = m.ReceiptHandle
	}

	switch {
	case strings.HasPrefix(p.Target, "arn:aws:lambda:"):
		return receiptHandles, r.invokeLambdaTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:states:"):
		return receiptHandles, r.invokeSFNTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:sns:"):
		return receiptHandles, r.invokeSNSTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:sqs:"):
		return receiptHandles, r.invokeSQSTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:kinesis:"):
		return receiptHandles, r.invokeKinesisTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:events:"):
		return receiptHandles, r.invokeEventBridgeTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:logs:"):
		return receiptHandles, r.invokeCloudWatchLogsTarget(ctx, p, payload)
	case strings.HasPrefix(p.Target, "arn:aws:firehose:"):
		return receiptHandles, r.invokeFirehoseTarget(ctx, p, payload)
	}

	return nil, fmt.Errorf("%w %q for pipe %q", ErrUnsupportedPipeTarget, p.Target, p.Name)
}

type sqsPipeEvent struct {
	Records []sqsPipeRecord `json:"Records"`
}

type sqsPipeRecord struct {
	Attributes     map[string]string `json:"attributes,omitempty"`
	MessageID      string            `json:"messageId"`
	ReceiptHandle  string            `json:"receiptHandle"`
	Body           string            `json:"body"`
	MD5OfBody      string            `json:"md5OfBody"`
	EventSource    string            `json:"eventSource"`
	EventSourceARN string            `json:"eventSourceARN"`
	AWSRegion      string            `json:"awsRegion"`
}

func buildSQSRecords(p *Pipe, msgs []*SQSMessage) []sqsPipeRecord {
	records := make([]sqsPipeRecord, len(msgs))

	for i, m := range msgs {
		records[i] = sqsPipeRecord{
			MessageID:      m.MessageID,
			ReceiptHandle:  m.ReceiptHandle,
			Body:           m.Body,
			Attributes:     m.Attributes,
			MD5OfBody:      m.MD5OfBody,
			EventSource:    "aws:sqs",
			EventSourceARN: p.Source,
			AWSRegion:      p.Region,
		}
	}

	return records
}

// applyInputTemplate returns the InputTemplate payload if configured, otherwise the
// pre-built default payload. This implements the Pipes TargetParameters.InputTemplate contract.
func applyInputTemplate(p *Pipe, defaultPayload []byte) []byte {
	if p.TargetParameters != nil && p.TargetParameters.InputTemplate != "" {
		return []byte(p.TargetParameters.InputTemplate)
	}

	return defaultPayload
}

func (r *Runner) invokeLambdaTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.lambda == nil {
		return fmt.Errorf("%w: lambda target %q", ErrTargetInvokerUnwired, p.Target)
	}

	// Map Pipes InvocationType to Lambda InvocationType.
	// Pipes: FIRE_AND_FORGET → Lambda: Event
	// Pipes: REQUEST_RESPONSE → Lambda: RequestResponse
	// Default (empty) → Event (async, matches AWS default behavior)
	invocationType := "Event"
	if p.TargetParameters != nil && p.TargetParameters.LambdaFunctionParameters != nil {
		it := p.TargetParameters.LambdaFunctionParameters.InvocationType
		if mapped, ok := lambdaPipesInvocationType()[it]; ok {
			invocationType = mapped
		} else if it != "" {
			invocationType = it
		}
	}

	payload = applyInputTemplate(p, payload)

	fnName := lambdaFunctionNameFromPipeARN(p.Target)
	if fnName == "" {
		fnName = p.Target
	}

	_, _, err := r.lambda.InvokeFunction(ctx, fnName, invocationType, payload)
	if err == nil {
		logger.Load(ctx).DebugContext(ctx, "pipes: invoked Lambda",
			"pipe", p.Name, "function", fnName)
	}

	return err
}

func (r *Runner) invokeSFNTarget(_ context.Context, p *Pipe, payload []byte) error {
	if r.sfn == nil {
		return fmt.Errorf("%w: step functions target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)
	invocationType := invocationTypeFireAndForget

	if p.TargetParameters != nil && p.TargetParameters.SFNStateMachineParameters != nil {
		it := p.TargetParameters.SFNStateMachineParameters.InvocationType
		if mapped, ok := sfnPipesInvocationType()[it]; ok {
			invocationType = mapped
		} else if it != "" {
			invocationType = it
		}
	}

	_ = invocationType // passed to StartExecution when API supports it

	return r.sfn.StartExecution(p.Target, "", string(payload))
}

func (r *Runner) invokeSNSTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.sns == nil {
		return fmt.Errorf("%w: sns target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)

	return r.sns.PublishMessage(ctx, p.Target, string(payload))
}

func (r *Runner) invokeSQSTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.sqsSender == nil {
		return fmt.Errorf("%w: sqs target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)
	var groupID, dedupID string

	if p.TargetParameters != nil && p.TargetParameters.SqsQueueParameters != nil {
		groupID = p.TargetParameters.SqsQueueParameters.MessageGroupID
		dedupID = p.TargetParameters.SqsQueueParameters.MessageDeduplicationID
	}

	return r.sqsSender.SendMessage(ctx, p.Target, string(payload), groupID, dedupID)
}

func (r *Runner) invokeKinesisTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.kinesis == nil {
		return fmt.Errorf("%w: kinesis target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)
	partitionKey := "default"

	if p.TargetParameters != nil && p.TargetParameters.KinesisStreamParameters != nil &&
		p.TargetParameters.KinesisStreamParameters.PartitionKey != "" {
		partitionKey = p.TargetParameters.KinesisStreamParameters.PartitionKey
	}

	return r.kinesis.PutRecord(ctx, p.Target, partitionKey, payload)
}

func (r *Runner) invokeEventBridgeTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.eventBus == nil {
		return fmt.Errorf("%w: eventbridge target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)
	evt := map[string]any{
		"Source":     "aws.pipes",
		"Detail":     string(payload),
		"DetailType": "Pipe Event",
	}

	if p.TargetParameters != nil && p.TargetParameters.EventBridgeEventBusParameters != nil {
		ebp := p.TargetParameters.EventBridgeEventBusParameters
		if ebp.Source != "" {
			evt["Source"] = ebp.Source
		}
		if ebp.DetailType != "" {
			evt["DetailType"] = ebp.DetailType
		}
	}

	return r.eventBus.PutEvents(ctx, p.Target, []map[string]any{evt})
}

func (r *Runner) invokeCloudWatchLogsTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.cwLogs == nil {
		return fmt.Errorf("%w: cloudwatch logs target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)
	logGroupARN := p.Target
	logStreamName := ""

	if p.TargetParameters != nil && p.TargetParameters.CloudWatchLogsParameters != nil {
		logStreamName = p.TargetParameters.CloudWatchLogsParameters.LogStreamName
	}

	return r.cwLogs.PutLogEvents(ctx, logGroupARN, logStreamName, []string{string(payload)})
}

func (r *Runner) invokeFirehoseTarget(ctx context.Context, p *Pipe, payload []byte) error {
	if r.firehose == nil {
		return fmt.Errorf("%w: firehose target %q", ErrTargetInvokerUnwired, p.Target)
	}

	payload = applyInputTemplate(p, payload)

	return r.firehose.PutRecord(ctx, p.Target, payload)
}

func lambdaFunctionNameFromPipeARN(arn string) string {
	const lambdaARNParts = 7
	parts := strings.SplitN(arn, ":", lambdaARNParts)

	if len(parts) < lambdaARNParts {
		return ""
	}

	return parts[lambdaARNParts-1]
}
