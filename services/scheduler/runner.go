package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// runnerTickInterval is how often the runner polls for due schedules.
	runnerTickInterval = 1 * time.Second
)

var (
	// ErrInvalidRateExpression is returned for malformed rate() expressions.
	ErrInvalidRateExpression = errors.New("invalid rate expression")
	// ErrInvalidRateValue is returned when the numeric value in a rate() expression is not valid.
	ErrInvalidRateValue = errors.New("invalid rate value")
	// ErrUnknownRateUnit is returned when the unit in a rate() expression is not recognised.
	ErrUnknownRateUnit = errors.New("unknown rate unit")
	// ErrInvalidCronExpression is returned for malformed cron() expressions.
	ErrInvalidCronExpression = errors.New("invalid cron expression")
)

// LambdaInvoker can invoke a Lambda function by name with a payload.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// SQSSender can send a message to an SQS queue by ARN.
type SQSSender interface {
	SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error
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
	backend     *InMemoryBackend
	lambda      LambdaInvoker
	sqs         SQSSender
	sns         SNSPublisher
	sfn         StepFunctionsStarter
	lastFiredAt map[string]time.Time
	mu          sync.Mutex
}

// NewRunner creates a new Runner for the given scheduler backend.
func NewRunner(backend *InMemoryBackend) *Runner {
	return &Runner{
		backend:     backend,
		lastFiredAt: make(map[string]time.Time),
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
	schedules := r.backend.ListSchedules()

	for _, s := range schedules {
		if s.State != "ENABLED" {
			continue
		}

		if r.isDue(s, now) {
			r.mu.Lock()
			r.lastFiredAt[s.Name] = now
			r.mu.Unlock()

			r.invokeTarget(ctx, s)
		}
	}
}

// isDue reports whether the schedule s should fire at time now.
func (r *Runner) isDue(s *Schedule, now time.Time) bool {
	expr := strings.TrimSpace(s.ScheduleExpression)

	if strings.HasPrefix(expr, "rate(") {
		return r.isDueRate(s.Name, expr, now)
	}

	if strings.HasPrefix(expr, "cron(") {
		return r.isDueCron(s.Name, expr, now)
	}

	return false
}

// isDueRate returns true when the rate interval has elapsed since the last firing.
func (r *Runner) isDueRate(name, expr string, now time.Time) bool {
	interval, err := parseRateExpression(expr)
	if err != nil || interval <= 0 {
		return false
	}

	r.mu.Lock()
	last, ok := r.lastFiredAt[name]
	r.mu.Unlock()

	if !ok {
		return true
	}

	return now.Sub(last) >= interval
}

// isDueCron returns true when now matches all fields of the cron expression.
func (r *Runner) isDueCron(name, expr string, now time.Time) bool {
	fields, err := parseCronExpression(expr)
	if err != nil {
		return false
	}

	if !matchesCron(now, fields) {
		return false
	}

	// Prevent double-firing within the same minute.
	r.mu.Lock()
	last, fired := r.lastFiredAt[name]
	r.mu.Unlock()

	if fired && last.Year() == now.Year() && last.YearDay() == now.YearDay() &&
		last.Hour() == now.Hour() && last.Minute() == now.Minute() {
		return false
	}

	return true
}

// invokeTarget dispatches the schedule's target based on its ARN prefix.
func (r *Runner) invokeTarget(ctx context.Context, s *Schedule) {
	targetARN := s.Target.ARN
	log := logger.Load(ctx)

	switch {
	case strings.HasPrefix(targetARN, "arn:aws:lambda:"):
		r.invokeLambdaTarget(ctx, s, log)
	case strings.HasPrefix(targetARN, "arn:aws:sqs:"):
		r.invokeSQSTarget(ctx, s, log)
	case strings.HasPrefix(targetARN, "arn:aws:sns:"):
		r.invokeSNSTarget(ctx, s, log)
	case strings.HasPrefix(targetARN, "arn:aws:states:"):
		r.invokeSFNTarget(ctx, s, log)
	default:
		log.WarnContext(ctx, "scheduler: unsupported target ARN", "target", targetARN, "schedule", s.Name)
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

type loggerIface interface {
	WarnContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

func (r *Runner) invokeLambdaTarget(ctx context.Context, s *Schedule, log loggerIface) {
	if r.lambda == nil {
		return
	}

	fnName := lambdaFunctionNameFromARN(s.Target.ARN)
	if fnName == "" {
		fnName = s.Target.ARN
	}

	payload := buildSchedulerPayload(s)

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
	} else {
		log.DebugContext(ctx, "scheduler: invoked Lambda", "function", fnName, "schedule", s.Name)
	}
}

func (r *Runner) invokeSQSTarget(ctx context.Context, s *Schedule, log loggerIface) {
	if r.sqs == nil {
		return
	}

	payload := string(buildSchedulerPayload(s))

	if err := r.sqs.SendMessageToQueue(ctx, s.Target.ARN, payload); err != nil {
		log.WarnContext(ctx, "scheduler: SQS send failed", "queue", s.Target.ARN, "schedule", s.Name, "error", err)
	} else {
		log.DebugContext(ctx, "scheduler: sent SQS message", "queue", s.Target.ARN, "schedule", s.Name)
	}
}

func (r *Runner) invokeSNSTarget(ctx context.Context, s *Schedule, log loggerIface) {
	if r.sns == nil {
		return
	}

	payload := string(buildSchedulerPayload(s))

	if err := r.sns.PublishToTopic(ctx, s.Target.ARN, payload); err != nil {
		log.WarnContext(ctx, "scheduler: SNS publish failed", "topic", s.Target.ARN, "schedule", s.Name, "error", err)
	} else {
		log.DebugContext(ctx, "scheduler: published SNS notification", "topic", s.Target.ARN, "schedule", s.Name)
	}
}

func (r *Runner) invokeSFNTarget(ctx context.Context, s *Schedule, log loggerIface) {
	if r.sfn == nil {
		return
	}

	payload := string(buildSchedulerPayload(s))

	if err := r.sfn.StartExecution(s.Target.ARN, "", payload); err != nil {
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
	} else {
		log.DebugContext(
			ctx,
			"scheduler: started StepFunctions execution",
			"stateMachine",
			s.Target.ARN,
			"schedule",
			s.Name,
		)
	}
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

// parseRateExpression parses an AWS EventBridge rate expression.
// Supported units: minutes, hours, days (and singular forms).
// Non-standard unit "seconds" is also supported for local testing.
func parseRateExpression(expr string) (time.Duration, error) {
	const rateFieldCount = 2 // rate(N unit) always has exactly two whitespace-separated fields

	inner := strings.TrimSuffix(strings.TrimPrefix(expr, "rate("), ")")
	inner = strings.TrimSpace(inner)

	parts := strings.Fields(inner)
	if len(parts) != rateFieldCount {
		return 0, fmt.Errorf("%w: %q", ErrInvalidRateExpression, expr)
	}

	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidRateValue, parts[0])
	}

	unit := strings.ToLower(parts[1])

	switch unit {
	case "second", "seconds":
		return time.Duration(n) * time.Second, nil
	case "minute", "minutes":
		return time.Duration(n) * time.Minute, nil
	case "hour", "hours":
		return time.Duration(n) * time.Hour, nil
	case "day", "days":
		return time.Duration(n) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("%w: %q", ErrUnknownRateUnit, parts[1])
}

// cronFields holds the parsed fields of a 6-field AWS EventBridge cron expression.
// AWS cron format: cron(Minutes Hours Day-of-month Month Day-of-week Year).
type cronFields struct {
	minutes    string
	hours      string
	dayOfMonth string
	month      string
	dayOfWeek  string
	year       string
}

// parseCronExpression parses an AWS EventBridge 6-field cron expression.
func parseCronExpression(expr string) (*cronFields, error) {
	inner := strings.TrimSuffix(strings.TrimPrefix(expr, "cron("), ")")
	inner = strings.TrimSpace(inner)

	parts := strings.Fields(inner)

	const cronFieldCount = 6
	if len(parts) != cronFieldCount {
		return nil, fmt.Errorf("%w: must have 6 fields, got %d: %q", ErrInvalidCronExpression, len(parts), expr)
	}

	return &cronFields{
		minutes:    parts[0],
		hours:      parts[1],
		dayOfMonth: parts[2],
		month:      parts[3],
		dayOfWeek:  parts[4],
		year:       parts[5],
	}, nil
}

// matchesCron returns true if t matches the given cron fields.
// Only supports wildcard (*), don't-care (?), single integers, and comma-separated lists.
func matchesCron(t time.Time, cf *cronFields) bool {
	return matchesCronField(cf.minutes, t.Minute()) &&
		matchesCronField(cf.hours, t.Hour()) &&
		matchesCronField(cf.dayOfMonth, t.Day()) &&
		matchesCronField(cf.month, int(t.Month())) &&
		matchesCronField(cf.dayOfWeek, dayOfWeekAWS(t.Weekday())) &&
		matchesCronField(cf.year, t.Year())
}

// dayOfWeekAWS converts Go's [time.Weekday] to the AWS cron day-of-week (1=Sunday, 7=Saturday).
func dayOfWeekAWS(wd time.Weekday) int {
	return int(wd) + 1
}

// matchesCronField checks whether a cron field pattern matches a numeric value.
// Supports: * (any), ? (any), single integers, and comma-separated values.
func matchesCronField(field string, value int) bool {
	if field == "*" || field == "?" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		part = strings.TrimSpace(part)
		if n, err := strconv.Atoi(part); err == nil && n == value {
			return true
		}
	}

	return false
}
