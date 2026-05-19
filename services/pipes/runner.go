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

var ErrUnsupportedPipeTarget = errors.New("pipes: unsupported target ARN")

const (
	pipeRunnerTickInterval = 1 * time.Second
	pipeDefaultBatchSize   = 10
	maxPipeWorkers         = 64
)

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

// Runner polls pipe sources and forwards records to pipe targets for RUNNING pipes.
type Runner struct {
	sqsReader SQSReader
	lambda    PipeLambdaInvoker
	sfn       PipeStepFunctionsStarter
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

func (r *Runner) Start(ctx context.Context) {
	r.doneMu.Lock()
	if r.started {
		r.doneMu.Unlock()

		return
	}
	r.started = true
	done := r.done
	r.doneMu.Unlock()

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
	r.doneMu.RLock()
	if !r.started {
		r.doneMu.RUnlock()

		return
	}
	done := r.done
	r.doneMu.RUnlock()

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
	res := r.backend.ListPipes(ListPipesFilter{CurrentState: stateRunning})

	for _, p := range res.Pipes {
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

	// Track enrichment invocation if enrichment is configured.
	if p.Enrichment != "" {
		r.backend.RecordEnrichmentCall(p.Name)
		logger.Load(ctx).DebugContext(ctx, "pipes: enrichment configured (tracked)",
			"pipe", p.Name, "enrichment", p.Enrichment, "messages", len(msgs))
	}

	receiptHandles, invokeErr := r.invokeTarget(ctx, p, msgs)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: target invocation failed",
			"pipe", p.Name, "target", p.Target, "error", invokeErr)

		return
	}

	if delErr := r.sqsReader.DeletePipeMessages(p.Source, receiptHandles); delErr != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to delete SQS messages",
			"pipe", p.Name, "source", p.Source, "error", delErr)
	}
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

func matchesAnyFilter(m *SQSMessage, filters []Filter) bool {
	for _, f := range filters {
		if f.Pattern == "" || strings.Contains(m.Body, f.Pattern) {
			return true
		}
	}

	return false
}

func (r *Runner) invokeTarget(ctx context.Context, p *Pipe, msgs []*SQSMessage) ([]string, error) {
	receiptHandles := make([]string, len(msgs))
	for i, m := range msgs {
		receiptHandles[i] = m.ReceiptHandle
	}

	switch {
	case strings.HasPrefix(p.Target, "arn:aws:lambda:"):
		return receiptHandles, r.invokeLambdaTarget(ctx, p, msgs)
	case strings.HasPrefix(p.Target, "arn:aws:states:"):
		return receiptHandles, r.invokeSFNTarget(ctx, p, msgs)
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

func (r *Runner) invokeLambdaTarget(ctx context.Context, p *Pipe, msgs []*SQSMessage) error {
	if r.lambda == nil {
		return nil
	}

	invocationType := "Event"
	if p.TargetParameters != nil &&
		p.TargetParameters.LambdaFunctionParameters != nil &&
		p.TargetParameters.LambdaFunctionParameters.InvocationType != "" {
		invocationType = p.TargetParameters.LambdaFunctionParameters.InvocationType
	}

	var payload []byte
	var err error

	if p.TargetParameters != nil && p.TargetParameters.InputTemplate != "" {
		payload = []byte(p.TargetParameters.InputTemplate)
	} else {
		payload, err = json.Marshal(sqsPipeEvent{Records: buildSQSRecords(p, msgs)})
		if err != nil {
			return err
		}
	}

	fnName := lambdaFunctionNameFromPipeARN(p.Target)
	if fnName == "" {
		fnName = p.Target
	}

	_, _, err = r.lambda.InvokeFunction(ctx, fnName, invocationType, payload)
	if err == nil {
		logger.Load(ctx).DebugContext(ctx, "pipes: invoked Lambda",
			"pipe", p.Name, "function", fnName, "messages", len(msgs))
	}

	return err
}

func (r *Runner) invokeSFNTarget(_ context.Context, p *Pipe, msgs []*SQSMessage) error {
	if r.sfn == nil {
		return nil
	}

	var inputStr string

	if p.TargetParameters != nil && p.TargetParameters.InputTemplate != "" {
		inputStr = p.TargetParameters.InputTemplate
	} else {
		payload, err := json.Marshal(sqsPipeEvent{Records: buildSQSRecords(p, msgs)})
		if err != nil {
			return err
		}

		inputStr = string(payload)
	}

	return r.sfn.StartExecution(p.Target, "", inputStr)
}

func lambdaFunctionNameFromPipeARN(arn string) string {
	const lambdaARNParts = 7
	parts := strings.SplitN(arn, ":", lambdaARNParts)

	if len(parts) < lambdaARNParts {
		return ""
	}

	return parts[lambdaARNParts-1]
}
