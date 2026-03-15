package pipes

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// pipeRunnerTickInterval is how often the runner polls sources for new records.
	pipeRunnerTickInterval = 1 * time.Second
	// pipeDefaultBatchSize is the default number of messages/records read per poll cycle.
	pipeDefaultBatchSize = 10
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
	// ReceivePipeMessages pulls up to maxMessages from the queue identified by queueARN.
	ReceivePipeMessages(queueARN string, maxMessages int) ([]*SQSMessage, error)
	// DeletePipeMessages removes the messages identified by receiptHandles from the queue.
	DeletePipeMessages(queueARN string, receiptHandles []string) error
}

// PipeLambdaInvoker invokes a Lambda function with a payload.
type PipeLambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// PipeStepFunctionsStarter starts a StepFunctions state machine execution.
type PipeStepFunctionsStarter interface {
	StartExecution(stateMachineARN, name, input string) error
}

// Runner polls pipe sources and forwards records to pipe targets for RUNNING pipes.
type Runner struct {
	backend   *InMemoryBackend
	sqsReader SQSReader
	lambda    PipeLambdaInvoker
	sfn       PipeStepFunctionsStarter
}

// NewRunner creates a new pipe Runner for the given backend.
func NewRunner(backend *InMemoryBackend) *Runner {
	return &Runner{backend: backend}
}

// SetSQSReader configures the SQS reader for SQS pipe sources.
func (r *Runner) SetSQSReader(s SQSReader) { r.sqsReader = s }

// SetLambdaInvoker configures the Lambda invoker for Lambda pipe targets.
func (r *Runner) SetLambdaInvoker(l PipeLambdaInvoker) { r.lambda = l }

// SetStepFunctionsStarter configures the StepFunctions starter for SFN pipe targets.
func (r *Runner) SetStepFunctionsStarter(s PipeStepFunctionsStarter) { r.sfn = s }

// Start runs the pipe runner as a background goroutine.
// It returns immediately; the goroutine stops when ctx is cancelled.
func (r *Runner) Start(ctx context.Context) {
	go r.run(ctx)
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
	pipes := r.backend.ListPipes()

	for _, p := range pipes {
		if p.CurrentState != stateRunning {
			continue
		}

		r.pollPipe(ctx, p)
	}
}

func (r *Runner) pollPipe(ctx context.Context, p *Pipe) {
	if isSQSARN(p.Source) {
		r.pollSQSPipe(ctx, p)
	}
}

// isSQSARN reports whether the given ARN identifies an SQS queue.
func isSQSARN(resourceARN string) bool {
	return strings.HasPrefix(resourceARN, "arn:aws:sqs:")
}

// pollSQSPipe polls the SQS source of a pipe and forwards messages to the target.
func (r *Runner) pollSQSPipe(ctx context.Context, p *Pipe) {
	if r.sqsReader == nil {
		return
	}

	msgs, err := r.sqsReader.ReceivePipeMessages(p.Source, pipeDefaultBatchSize)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to receive SQS messages",
			"pipe", p.Name, "source", p.Source, "error", err)

		return
	}

	if len(msgs) == 0 {
		return
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

// invokeTarget forwards the SQS messages to the pipe's target and returns the receipt handles.
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

	logger.Load(ctx).WarnContext(ctx, "pipes: unsupported target ARN", "pipe", p.Name, "target", p.Target)

	return receiptHandles, nil
}

// sqsPipeEvent is the Lambda event format for SQS pipe sources.
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

func (r *Runner) invokeLambdaTarget(ctx context.Context, p *Pipe, msgs []*SQSMessage) error {
	if r.lambda == nil {
		return nil
	}

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

	payload, err := json.Marshal(sqsPipeEvent{Records: records})
	if err != nil {
		return err
	}

	fnName := lambdaFunctionNameFromPipeARN(p.Target)
	if fnName == "" {
		fnName = p.Target
	}

	_, _, err = r.lambda.InvokeFunction(ctx, fnName, "Event", payload)
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

	payload, err := json.Marshal(sqsPipeEvent{Records: func() []sqsPipeRecord {
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
	}()})
	if err != nil {
		return err
	}

	return r.sfn.StartExecution(p.Target, "", string(payload))
}

// lambdaFunctionNameFromPipeARN extracts the function name from a Lambda ARN.
func lambdaFunctionNameFromPipeARN(arn string) string {
	const lambdaARNParts = 7
	parts := strings.SplitN(arn, ":", lambdaARNParts)

	if len(parts) < lambdaARNParts {
		return ""
	}

	return parts[lambdaARNParts-1]
}
