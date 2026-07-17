package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// runnerTickInterval is how often the runner polls for due schedules.
const runnerTickInterval = 1 * time.Second

// LambdaInvoker can invoke a Lambda function by name with a payload.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// SQSSender can send a message to an SQS queue by ARN.
type SQSSender interface {
	SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error
}

// SQSFIFOSender can send a message to a FIFO SQS queue with a MessageGroupId.
type SQSFIFOSender interface {
	SendMessageToFIFOQueue(ctx context.Context, queueARN, messageBody, messageGroupID string) error
}

// EventBusPutter can put events onto an EventBridge bus.
type EventBusPutter interface {
	PutSchedulerEvent(ctx context.Context, busARN, source, detailType, detail string) error
}

// KinesisRecordPutter can put a record onto a Kinesis stream.
type KinesisRecordPutter interface {
	PutSchedulerRecord(ctx context.Context, streamARN, partitionKey string, data []byte) error
}

// SageMakerPipelineStarter can start a SageMaker pipeline execution.
type SageMakerPipelineStarter interface {
	StartPipelineExecution(ctx context.Context, pipelineARN string, params map[string]string) error
}

// ECSTaskRunner can run an ECS task.
type ECSTaskRunner interface {
	RunSchedulerTask(ctx context.Context, taskDefARN, launchType string, taskCount int) error
}

// SNSPublisher can publish a message to an SNS topic by ARN.
type SNSPublisher interface {
	PublishToTopic(ctx context.Context, topicARN, message string) error
}

// StepFunctionsStarter can start a StepFunctions state machine execution.
type StepFunctionsStarter interface {
	StartExecution(stateMachineARN, name, input string) error
}

// Runner evaluates schedule expressions and invokes targets when due.
type Runner struct {
	backend     StorageBackend
	lambda      LambdaInvoker
	sqs         SQSSender
	sqsFIFO     SQSFIFOSender
	sns         SNSPublisher
	sfn         StepFunctionsStarter
	eventBus    EventBusPutter
	kinesis     KinesisRecordPutter
	sageMaker   SageMakerPipelineStarter
	ecsRunner   ECSTaskRunner
	lastFiredAt map[string]time.Time
	// cronCache caches parsed cron fields keyed by expression string to avoid re-parsing on every poll.
	cronCache map[string]*cronFields
	mu        sync.Mutex
	cacheMu   sync.RWMutex
}

// NewRunner creates a new Runner for the given scheduler backend.
func NewRunner(backend StorageBackend) *Runner {
	return &Runner{
		backend:     backend,
		lastFiredAt: make(map[string]time.Time),
		cronCache:   make(map[string]*cronFields),
	}
}

// SetLambdaInvoker configures the Lambda invoker for schedule targets.
func (r *Runner) SetLambdaInvoker(l LambdaInvoker) { r.lambda = l }

// SetSQSSender configures the SQS sender for schedule targets.
func (r *Runner) SetSQSSender(s SQSSender) { r.sqs = s }

// SetSNSPublisher configures the SNS publisher for schedule targets.
func (r *Runner) SetSNSPublisher(p SNSPublisher) { r.sns = p }

// SetStepFunctionsStarter configures the StepFunctions starter for schedule targets.
func (r *Runner) SetStepFunctionsStarter(s StepFunctionsStarter) { r.sfn = s }

// SetSQSFIFOSender configures the FIFO SQS sender for schedule targets.
func (r *Runner) SetSQSFIFOSender(s SQSFIFOSender) { r.sqsFIFO = s }

// SetEventBusPutter configures the EventBridge bus sender for schedule targets.
func (r *Runner) SetEventBusPutter(p EventBusPutter) { r.eventBus = p }

// SetKinesisRecordPutter configures the Kinesis record putter for schedule targets.
func (r *Runner) SetKinesisRecordPutter(k KinesisRecordPutter) { r.kinesis = k }

// SetSageMakerPipelineStarter configures the SageMaker pipeline starter for schedule targets.
func (r *Runner) SetSageMakerPipelineStarter(s SageMakerPipelineStarter) { r.sageMaker = s }

// SetECSTaskRunner configures the ECS task runner for schedule targets.
func (r *Runner) SetECSTaskRunner(e ECSTaskRunner) { r.ecsRunner = e }

// Start runs the scheduler as a background goroutine.
// It returns immediately; the goroutine stops when ctx is cancelled.
func (r *Runner) Start(ctx context.Context) {
	go r.run(ctx)
}

func (r *Runner) run(ctx context.Context) {
	ticker := time.NewTicker(runnerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.checkAndFireSchedules(ctx, now)
		}
	}
}

func (r *Runner) checkAndFireSchedules(ctx context.Context, now time.Time) {
	schedules, _ := r.backend.ListSchedules(ctx, "", "", "", "", 0)

	activeKeys := make(map[string]struct{}, len(schedules))
	activeExprs := make(map[string]struct{}, len(schedules))

	for _, s := range schedules {
		key := scheduleKey(s.GroupName, s.Name)
		activeKeys[key] = struct{}{}
		activeExprs[strings.TrimSpace(s.ScheduleExpression)] = struct{}{}

		if s.State != "ENABLED" {
			continue
		}

		if r.isDue(s, now) {
			r.mu.Lock()
			r.lastFiredAt[key] = now
			r.mu.Unlock()

			r.invokeTarget(ctx, s, now)
		}
	}

	// Sweep lastFiredAt entries for schedules that no longer exist to prevent unbounded growth.
	r.mu.Lock()
	for key := range r.lastFiredAt {
		if _, ok := activeKeys[key]; !ok {
			delete(r.lastFiredAt, key)
		}
	}
	r.mu.Unlock()

	// Sweep cronCache entries for expressions that no schedule references anymore.
	// Without this the cache would grow unbounded as schedules churn through unique
	// cron expressions across the runner's lifetime.
	r.cacheMu.Lock()
	for expr := range r.cronCache {
		if _, ok := activeExprs[expr]; !ok {
			delete(r.cronCache, expr)
		}
	}
	r.cacheMu.Unlock()
}

// isDue reports whether the schedule s should fire at time now.
func (r *Runner) isDue(s *Schedule, now time.Time) bool {
	expr := strings.TrimSpace(s.ScheduleExpression)
	key := scheduleKey(s.GroupName, s.Name)

	if strings.HasPrefix(expr, "rate(") {
		return r.isDueRate(key, expr, now)
	}

	if strings.HasPrefix(expr, "cron(") {
		return r.isDueCron(key, expr, now)
	}

	return false
}

// isDueRate returns true when the rate interval has elapsed since the last firing.
func (r *Runner) isDueRate(key, expr string, now time.Time) bool {
	interval, err := parseRateExpression(expr)
	if err != nil || interval <= 0 {
		return false
	}

	r.mu.Lock()
	last, ok := r.lastFiredAt[key]
	r.mu.Unlock()

	if !ok {
		return true
	}

	return now.Sub(last) >= interval
}

// cachedParseCron returns the parsed cron fields for expr, using the cache.
func (r *Runner) cachedParseCron(expr string) (*cronFields, error) {
	r.cacheMu.RLock()
	if cf, ok := r.cronCache[expr]; ok {
		r.cacheMu.RUnlock()

		return cf, nil
	}
	r.cacheMu.RUnlock()

	cf, err := parseCronExpression(expr)
	if err != nil {
		return nil, err
	}

	r.cacheMu.Lock()
	r.cronCache[expr] = cf
	r.cacheMu.Unlock()

	return cf, nil
}

// isDueCron returns true when now matches all fields of the cron expression.
func (r *Runner) isDueCron(key, expr string, now time.Time) bool {
	fields, err := r.cachedParseCron(expr)
	if err != nil {
		return false
	}

	if !matchesCron(now, fields) {
		return false
	}

	// Prevent double-firing within the same minute.
	r.mu.Lock()
	last, fired := r.lastFiredAt[key]
	r.mu.Unlock()

	if fired && last.Year() == now.Year() && last.YearDay() == now.YearDay() &&
		last.Hour() == now.Hour() && last.Minute() == now.Minute() {
		return false
	}

	return true
}

// invokeTarget dispatches the schedule's target based on its ARN prefix, with retries and DLQ.
// scheduledAt is recorded for logging; event-age deadline is measured from the first invocation attempt.
func (r *Runner) invokeTarget(ctx context.Context, s *Schedule, _ time.Time) {
	log := logger.Load(ctx)

	maxAttempts, maxAge := retryPolicyParams(s.Target.RetryPolicy)
	payload := targetPayload(s)
	startedAt := time.Now()

	var invokeErr error

	for attempt := 0; attempt <= maxAttempts; attempt++ {
		// Check event age deadline (only after first attempt has been made).
		if attempt > 0 && maxAge > 0 && time.Since(startedAt) > maxAge {
			log.WarnContext(ctx, "scheduler: event age exceeded, dropping", "schedule", s.Name, "attempt", attempt)
			r.sendToDLQ(ctx, s, payload, log)

			return
		}

		if attempt > 0 {
			backoff := retryBackoff(attempt)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}

		invokeErr = r.dispatchTarget(ctx, s, payload, log)
		if invokeErr == nil {
			r.handleActionAfterCompletion(ctx, s, log)

			return
		}

		log.WarnContext(
			ctx,
			"scheduler: target invocation failed",
			"schedule",
			s.Name,
			"attempt",
			attempt,
			"error",
			invokeErr,
		)
	}

	// All retries exhausted.
	r.sendToDLQ(ctx, s, payload, log)
}

// retryPolicyParams extracts retry limits from the policy, applying AWS defaults when nil.
func retryPolicyParams(rp *RetryPolicy) (int, time.Duration) {
	const defaultMaxAttempts = 185
	const defaultMaxAgeSecs = 86400

	if rp == nil {
		return defaultMaxAttempts, time.Duration(defaultMaxAgeSecs) * time.Second
	}

	attempts := rp.MaximumRetryAttempts

	var age time.Duration
	if rp.MaximumEventAgeInSeconds > 0 {
		age = time.Duration(rp.MaximumEventAgeInSeconds) * time.Second
	} else {
		age = time.Duration(defaultMaxAgeSecs) * time.Second
	}

	return attempts, age
}

// retryBackoff returns the sleep duration for the given attempt (exponential, capped at 15s).
func retryBackoff(attempt int) time.Duration {
	const base = 200 * time.Millisecond
	const maxBackoff = 15 * time.Second
	const maxShift = 6

	d := base * (1 << min(attempt-1, maxShift))
	if d > maxBackoff {
		return maxBackoff
	}

	return d
}

// dispatchTarget routes the payload to the correct underlying service.
func (r *Runner) dispatchTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	targetARN := s.Target.ARN

	switch {
	case strings.HasPrefix(targetARN, "arn:aws:lambda:"):
		return r.invokeLambdaTarget(ctx, s, payload, log)
	case strings.HasPrefix(targetARN, "arn:aws:sqs:"):
		return r.invokeSQSTarget(ctx, s, payload, log)
	case strings.HasPrefix(targetARN, "arn:aws:sns:"):
		return r.invokeSNSTarget(ctx, s, payload, log)
	case strings.HasPrefix(targetARN, "arn:aws:states:"):
		return r.invokeSFNTarget(ctx, s, payload, log)
	case strings.HasPrefix(targetARN, "arn:aws:events:"):
		return r.invokeEventBusTarget(ctx, s, payload, log)
	case strings.HasPrefix(targetARN, "arn:aws:kinesis:"):
		return r.invokeKinesisTarget(ctx, s, payload, log)
	case strings.HasPrefix(targetARN, "arn:aws:sagemaker:"):
		return r.invokeSageMakerTarget(ctx, s, log)
	case strings.HasPrefix(targetARN, "arn:aws:ecs:"):
		return r.invokeECSTarget(ctx, s, log)
	default:
		log.WarnContext(ctx, "scheduler: unsupported target ARN", "target", targetARN, "schedule", s.Name)

		return nil
	}
}

// sendToDLQ sends the payload to the dead-letter SQS queue if configured.
func (r *Runner) sendToDLQ(ctx context.Context, s *Schedule, payload []byte, log loggerIface) {
	if s.Target.DeadLetterConfig == nil || s.Target.DeadLetterConfig.Arn == "" {
		return
	}

	if r.sqs == nil {
		log.WarnContext(ctx, "scheduler: DLQ configured but no SQS sender", "schedule", s.Name)

		return
	}

	if err := r.sqs.SendMessageToQueue(ctx, s.Target.DeadLetterConfig.Arn, string(payload)); err != nil {
		log.WarnContext(
			ctx,
			"scheduler: DLQ send failed",
			"dlq",
			s.Target.DeadLetterConfig.Arn,
			"schedule",
			s.Name,
			"error",
			err,
		)
	} else {
		log.DebugContext(ctx, "scheduler: sent to DLQ", "dlq", s.Target.DeadLetterConfig.Arn, "schedule", s.Name)
	}
}

// handleActionAfterCompletion fires the after-completion action when the schedule has run.
// For one-shot at() schedules or upon explicit DELETE/NONE setting.
func (r *Runner) handleActionAfterCompletion(ctx context.Context, s *Schedule, log loggerIface) {
	action := s.ActionAfterCompletion
	if action == "" {
		return
	}

	switch strings.ToUpper(action) {
	case "DELETE":
		delCtx := ctx
		if s.Region != "" {
			delCtx = context.WithValue(ctx, regionContextKey{}, s.Region)
		}
		if err := r.backend.DeleteSchedule(delCtx, s.Name, s.GroupName); err != nil {
			log.WarnContext(ctx, "scheduler: ActionAfterCompletion=DELETE failed", "schedule", s.Name, "error", err)
		} else {
			log.DebugContext(ctx, "scheduler: deleted schedule after completion", "schedule", s.Name)
		}
	case "NONE":
		// NONE is the default; no state change required.
	}
}

// schedulerEventPayload is the default event sent by EventBridge Scheduler to a target.
type schedulerEventPayload struct {
	ScheduleARN  string `json:"schedule-arn"`
	ScheduledAt  string `json:"scheduledTime"`
	ScheduleName string `json:"schedule-name"`
}

func buildSchedulerPayload(s *Schedule) []byte {
	p := schedulerEventPayload{
		ScheduleARN:  s.ARN,
		ScheduledAt:  time.Now().UTC().Format(time.RFC3339),
		ScheduleName: s.Name,
	}

	b, _ := json.Marshal(p)

	return b
}

// targetPayload returns the payload to send to a schedule target.
// When the target has a custom Input set, it is used verbatim (AWS behaviour).
// Otherwise the default EventBridge Scheduler event is built.
func targetPayload(s *Schedule) []byte {
	if s.Target.Input != "" {
		return []byte(s.Target.Input)
	}

	return buildSchedulerPayload(s)
}

type loggerIface interface {
	WarnContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

func (r *Runner) invokeLambdaTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	if r.lambda == nil {
		return nil
	}

	fnName := lambdaFunctionNameFromARN(s.Target.ARN)
	if fnName == "" {
		fnName = s.Target.ARN
	}

	if _, _, err := r.lambda.InvokeFunction(ctx, fnName, "Event", payload); err != nil {
		log.WarnContext(
			ctx,
			"scheduler: Lambda invocation failed",
			"function",
			fnName,
			"schedule",
			s.Name,
			"error",
			err,
		)

		return err
	}

	log.DebugContext(ctx, "scheduler: invoked Lambda", "function", fnName, "schedule", s.Name)

	return nil
}

func (r *Runner) invokeSQSTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	if r.sqs == nil {
		return nil
	}

	// FIFO queue requires MessageGroupId.
	if s.Target.SqsParameters != nil && s.Target.SqsParameters.MessageGroupID != "" && r.sqsFIFO != nil {
		if err := r.sqsFIFO.SendMessageToFIFOQueue(
			ctx,
			s.Target.ARN,
			string(payload),
			s.Target.SqsParameters.MessageGroupID,
		); err != nil {
			log.WarnContext(
				ctx,
				"scheduler: SQS FIFO send failed",
				"queue",
				s.Target.ARN,
				"schedule",
				s.Name,
				"error",
				err,
			)

			return err
		}

		log.DebugContext(ctx, "scheduler: sent SQS FIFO message", "queue", s.Target.ARN, "schedule", s.Name)

		return nil
	}

	if err := r.sqs.SendMessageToQueue(ctx, s.Target.ARN, string(payload)); err != nil {
		log.WarnContext(ctx, "scheduler: SQS send failed", "queue", s.Target.ARN, "schedule", s.Name, "error", err)

		return err
	}

	log.DebugContext(ctx, "scheduler: sent SQS message", "queue", s.Target.ARN, "schedule", s.Name)

	return nil
}

func (r *Runner) invokeSNSTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	if r.sns == nil {
		return nil
	}

	if err := r.sns.PublishToTopic(ctx, s.Target.ARN, string(payload)); err != nil {
		log.WarnContext(ctx, "scheduler: SNS publish failed", "topic", s.Target.ARN, "schedule", s.Name, "error", err)

		return err
	}

	log.DebugContext(ctx, "scheduler: published SNS notification", "topic", s.Target.ARN, "schedule", s.Name)

	return nil
}

func (r *Runner) invokeSFNTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	if r.sfn == nil {
		return nil
	}

	if err := r.sfn.StartExecution(s.Target.ARN, "", string(payload)); err != nil {
		log.WarnContext(
			ctx,
			"scheduler: StepFunctions start failed",
			"stateMachine",
			s.Target.ARN,
			"schedule",
			s.Name,
			"error",
			err,
		)

		return err
	}

	log.DebugContext(
		ctx,
		"scheduler: started StepFunctions execution",
		"stateMachine",
		s.Target.ARN,
		"schedule",
		s.Name,
	)

	return nil
}

func (r *Runner) invokeEventBusTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	if r.eventBus == nil {
		log.DebugContext(ctx, "scheduler: EventBridge bus target (no invoker)", "bus", s.Target.ARN, "schedule", s.Name)

		return nil
	}

	var source, detailType string

	if s.Target.EventBridgeParameters != nil {
		source = s.Target.EventBridgeParameters.Source
		detailType = s.Target.EventBridgeParameters.DetailType
	}

	if err := r.eventBus.PutSchedulerEvent(ctx, s.Target.ARN, source, detailType, string(payload)); err != nil {
		log.WarnContext(
			ctx,
			"scheduler: EventBridge PutEvents failed",
			"bus",
			s.Target.ARN,
			"schedule",
			s.Name,
			"error",
			err,
		)

		return err
	}

	log.DebugContext(ctx, "scheduler: put EventBridge event", "bus", s.Target.ARN, "schedule", s.Name)

	return nil
}

func (r *Runner) invokeKinesisTarget(ctx context.Context, s *Schedule, payload []byte, log loggerIface) error {
	if r.kinesis == nil {
		log.DebugContext(ctx, "scheduler: Kinesis target (no invoker)", "stream", s.Target.ARN, "schedule", s.Name)

		return nil
	}

	partitionKey := ""
	if s.Target.KinesisParameters != nil {
		partitionKey = s.Target.KinesisParameters.PartitionKey
	}

	if err := r.kinesis.PutSchedulerRecord(ctx, s.Target.ARN, partitionKey, payload); err != nil {
		log.WarnContext(
			ctx,
			"scheduler: Kinesis PutRecord failed",
			"stream",
			s.Target.ARN,
			"schedule",
			s.Name,
			"error",
			err,
		)

		return err
	}

	log.DebugContext(ctx, "scheduler: put Kinesis record", "stream", s.Target.ARN, "schedule", s.Name)

	return nil
}

func (r *Runner) invokeSageMakerTarget(ctx context.Context, s *Schedule, log loggerIface) error {
	if r.sageMaker == nil {
		log.DebugContext(
			ctx,
			"scheduler: SageMaker pipeline target (no invoker)",
			"pipeline",
			s.Target.ARN,
			"schedule",
			s.Name,
		)

		return nil
	}

	params := map[string]string{}

	if s.Target.SageMakerPipelineParameters != nil {
		for _, p := range s.Target.SageMakerPipelineParameters.PipelineParameterList {
			params[p.Name] = p.Value
		}
	}

	if err := r.sageMaker.StartPipelineExecution(ctx, s.Target.ARN, params); err != nil {
		log.WarnContext(
			ctx,
			"scheduler: SageMaker StartPipelineExecution failed",
			"pipeline",
			s.Target.ARN,
			"schedule",
			s.Name,
			"error",
			err,
		)

		return err
	}

	log.DebugContext(ctx, "scheduler: started SageMaker pipeline", "pipeline", s.Target.ARN, "schedule", s.Name)

	return nil
}

func (r *Runner) invokeECSTarget(ctx context.Context, s *Schedule, log loggerIface) error {
	if r.ecsRunner == nil {
		log.DebugContext(ctx, "scheduler: ECS RunTask target (no invoker)", "arn", s.Target.ARN, "schedule", s.Name)

		return nil
	}

	var taskDefARN, launchType string

	var taskCount int

	if s.Target.EcsParameters != nil {
		taskDefARN = s.Target.EcsParameters.TaskDefinitionArn
		launchType = s.Target.EcsParameters.LaunchType
		taskCount = s.Target.EcsParameters.TaskCount
	}

	if taskDefARN == "" {
		taskDefARN = s.Target.ARN
	}

	if taskCount == 0 {
		taskCount = 1
	}

	if err := r.ecsRunner.RunSchedulerTask(ctx, taskDefARN, launchType, taskCount); err != nil {
		log.WarnContext(ctx, "scheduler: ECS RunTask failed", "taskDef", taskDefARN, "schedule", s.Name, "error", err)

		return err
	}

	log.DebugContext(ctx, "scheduler: ran ECS task", "taskDef", taskDefARN, "schedule", s.Name)

	return nil
}

// lambdaFunctionNameFromARN extracts the function name from a Lambda ARN.
// Example: arn:aws:lambda:us-east-1:000000000000:function:my-func → my-func.
func lambdaFunctionNameFromARN(arn string) string {
	const lambdaARNParts = 7
	parts := strings.SplitN(arn, ":", lambdaARNParts)

	if len(parts) < lambdaARNParts {
		return ""
	}

	return parts[lambdaARNParts-1]
}
