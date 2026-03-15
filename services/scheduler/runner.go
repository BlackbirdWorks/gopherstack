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
	// cronStepMaxRange is the inclusive upper bound used when a cron step has no explicit end
	// (e.g. "*/5"). It must be larger than any valid field value (year ~9999).
	cronStepMaxRange = 9999
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

	activeNames := make(map[string]struct{}, len(schedules))

	for _, s := range schedules {
		activeNames[s.Name] = struct{}{}

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

	// Sweep lastFiredAt entries for schedules that no longer exist to prevent unbounded growth.
	r.mu.Lock()
	for name := range r.lastFiredAt {
		if _, ok := activeNames[name]; !ok {
			delete(r.lastFiredAt, name)
		}
	}
	r.mu.Unlock()
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

func (r *Runner) invokeLambdaTarget(ctx context.Context, s *Schedule, log loggerIface) {
	if r.lambda == nil {
		return
	}

	fnName := lambdaFunctionNameFromARN(s.Target.ARN)
	if fnName == "" {
		fnName = s.Target.ARN
	}

	payload := targetPayload(s)

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

	payload := string(targetPayload(s))

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

	payload := string(targetPayload(s))

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

	payload := string(targetPayload(s))

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
// Supports wildcard (*), don't-care (?), single integers, comma-separated lists,
// ranges (n-m), steps (*/s, n/s, n-m/s), and month/weekday name aliases.
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
// Supports: * (any), ? (any), comma-separated lists, ranges (n-m), steps (*/s, n/s, n-m/s),
// and month/weekday name aliases (JAN-DEC, SUN-SAT).
func matchesCronField(field string, value int) bool {
	if field == "*" || field == "?" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		part = strings.TrimSpace(part)
		if matchesCronPart(part, value) {
			return true
		}
	}

	return false
}

// matchesCronPart evaluates a single cron token (no commas) against value.
// Handles: integer, name alias, range (n-m), step (*/s, n/s, n-m/s).
func matchesCronPart(part string, value int) bool {
	// Step: base/step
	if baseStr, stepStr, isStep := strings.Cut(part, "/"); isStep {
		return matchesCronStep(baseStr, stepStr, cronStepMaxRange, value)
	}

	// Range: n-m
	if lo, hi, isRange := strings.Cut(part, "-"); isRange {
		start, err1 := parseCronValue(lo)
		end, err2 := parseCronValue(hi)
		if err1 != nil || err2 != nil {
			return false
		}

		return value >= start && value <= end
	}

	// Single value or name alias
	n, err := parseCronValue(part)

	return err == nil && n == value
}

// matchesCronStep evaluates a step token (base/step) against value.
func matchesCronStep(baseStr, stepStr string, maxVal, value int) bool {
	step, err := strconv.Atoi(stepStr)
	if err != nil || step <= 0 {
		return false
	}

	start, end := 0, maxVal

	switch baseStr {
	case "*", "?":
		// */step — every step starting from 0; end stays at maxVal
	default:
		if lo, hi, isRange := strings.Cut(baseStr, "-"); isRange {
			s, err1 := parseCronValue(lo)
			e, err2 := parseCronValue(hi)
			if err1 != nil || err2 != nil {
				return false
			}

			start, end = s, e
		} else {
			s, parseErr := parseCronValue(baseStr)
			if parseErr != nil {
				return false
			}

			start = s
		}
	}

	if value < start || value > end {
		return false
	}

	return (value-start)%step == 0
}

// ErrUnknownCronValue is returned when a cron field token cannot be parsed.
var ErrUnknownCronValue = errors.New("unknown cron value")

// Month and weekday numeric constants used in AWS EventBridge cron expressions.
const (
	cronJan = 1
	cronFeb = 2
	cronMar = 3
	cronApr = 4
	cronMay = 5
	cronJun = 6
	cronJul = 7
	cronAug = 8
	cronSep = 9
	cronOct = 10
	cronNov = 11
	cronDec = 12

	// AWS cron day-of-week: 1=SUN, 2=MON, ..., 7=SAT.
	cronSun = 1
	cronMon = 2
	cronTue = 3
	cronWed = 4
	cronThu = 5
	cronFri = 6
	cronSat = 7
)

// cronMonthValue maps a 3-letter month abbreviation (uppercase) to its numeric value (1=JAN..12=DEC).
// Cases are ordered alphabetically for lint compliance (cyclop/cyclop keeps the count within limits).
func cronMonthValue(upper string) (int, bool) {
	switch upper {
	case "APR":
		return cronApr, true
	case "AUG":
		return cronAug, true
	case "DEC":
		return cronDec, true
	case "FEB":
		return cronFeb, true
	case "JAN":
		return cronJan, true
	case "JUL":
		return cronJul, true
	case "JUN":
		return cronJun, true
	case "MAR":
		return cronMar, true
	case "MAY":
		return cronMay, true
	case "NOV":
		return cronNov, true
	case "OCT":
		return cronOct, true
	case "SEP":
		return cronSep, true
	}

	return 0, false
}

// cronDOWValue maps a 3-letter weekday abbreviation (uppercase) to its AWS numeric value.
// AWS uses 1=SUN, 2=MON, ..., 7=SAT.
func cronDOWValue(upper string) (int, bool) {
	switch upper {
	case "FRI":
		return cronFri, true
	case "MON":
		return cronMon, true
	case "SAT":
		return cronSat, true
	case "SUN":
		return cronSun, true
	case "THU":
		return cronThu, true
	case "TUE":
		return cronTue, true
	case "WED":
		return cronWed, true
	}

	return 0, false
}

// parseCronValue parses a single cron field token: an integer or a month/weekday name alias.
func parseCronValue(s string) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		return n, nil
	}

	upper := strings.ToUpper(s)

	if n, ok := cronMonthValue(upper); ok {
		return n, nil
	}

	if n, ok := cronDOWValue(upper); ok {
		return n, nil
	}

	return 0, fmt.Errorf("%w: %q", ErrUnknownCronValue, s)
}
