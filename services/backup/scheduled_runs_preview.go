package backup

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
)

// ErrInvalidScheduleExpression is returned when a plan rule's
// ScheduleExpression cannot be parsed as an AWS cron(...) expression.
var ErrInvalidScheduleExpression = errors.New("invalid schedule expression")

const (
	// defaultRuleScheduleExpression is what real AWS Backup uses "when no CRON
	// expression is provided" (api_op_CreateBackupPlan.go's Rule.ScheduleExpression doc).
	defaultRuleScheduleExpression = "cron(0 5 ? * * *)"

	// maxScheduledRunsPreviewResults matches GetBackupPlanInput.MaxScheduledRunsPreview's
	// documented "Valid range is 0-10" (api_op_GetBackupPlan.go).
	maxScheduledRunsPreviewResults = 10

	cronFieldCount     = 6 // minute hour day-of-month month day-of-week year
	cronScanYearsAhead = 2 // forward scan limit, matches other cron previews in this repo

	ruleExecutionTypeSnapshots              = "SNAPSHOTS"
	ruleExecutionTypeContinuousAndSnapshots = "CONTINUOUS_AND_SNAPSHOTS"
)

// ScheduledPlanExecution is a computed preview of one future rule execution
// -- never persisted, derived fresh from the plan's rules on every
// GetBackupPlan call, matching types.ScheduledPlanExecutionMember.
type ScheduledPlanExecution struct {
	ExecutionTime     time.Time
	RuleExecutionType string
	RuleID            string
}

// planCronExpression is a parsed AWS Backup cron(min hour dom month dow year)
// schedule expression -- the same 6-field EventBridge-style dialect
// api_op_CreateBackupPlan.go's ScheduleExpression doc links to.
type planCronExpression struct {
	minute     string
	hour       string
	dayOfMonth string
	month      string
	dayOfWeek  string
	year       string
}

func parsePlanCronExpression(expr string) (*planCronExpression, error) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "cron(") || !strings.HasSuffix(expr, ")") {
		return nil, fmt.Errorf("%w: %q", ErrInvalidScheduleExpression, expr)
	}

	fields := strings.Fields(expr[len("cron(") : len(expr)-1])
	if len(fields) != cronFieldCount {
		return nil, fmt.Errorf(
			"%w: requires %d fields, got %d: %q",
			ErrInvalidScheduleExpression, cronFieldCount, len(fields), expr,
		)
	}

	return &planCronExpression{
		minute: fields[0], hour: fields[1], dayOfMonth: fields[2],
		month: fields[3], dayOfWeek: fields[4], year: fields[5],
	}, nil
}

// nextAfter returns the next time strictly after t that matches, or the zero
// Time if nothing matches within the scan window.
func (c *planCronExpression) nextAfter(t time.Time) time.Time {
	candidate := t.Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(cronScanYearsAhead, 0, 0)

	for candidate.Before(limit) {
		if c.matches(candidate) {
			return candidate
		}

		candidate = candidate.Add(time.Minute)
	}

	return time.Time{}
}

func (c *planCronExpression) matches(t time.Time) bool {
	if !awscron.MatchField(awscron.FieldMinute, c.minute, t.Minute(), awscron.MinuteMin, awscron.MinuteMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldHour, c.hour, t.Hour(), awscron.HourMin, awscron.HourMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldMonth, c.month, int(t.Month()), awscron.MonthMin, awscron.MonthMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldYear, c.year, t.Year(), awscron.YearMin, awscron.YearMax) {
		return false
	}

	return awscron.MatchDayFields(c.dayOfMonth, c.dayOfWeek, t)
}

// ruleLocation resolves a rule's ScheduleExpressionTimezone ("By default,
// ScheduleExpressions are in UTC" -- api_op_CreateBackupPlan.go), falling
// back to UTC for an empty or unrecognized zone rather than erroring: a
// schedule preview is best-effort information, not a validated mutation.
func ruleLocation(r Rule) *time.Location {
	if r.ScheduleExpressionTimezone == "" {
		return time.UTC
	}

	if loc, err := time.LoadLocation(r.ScheduleExpressionTimezone); err == nil {
		return loc
	}

	return time.UTC
}

func ruleExecutionType(r Rule) string {
	if r.EnableContinuousBackup {
		return ruleExecutionTypeContinuousAndSnapshots
	}

	return ruleExecutionTypeSnapshots
}

// ScheduledRunsPreview computes up to maxResults upcoming executions across
// every rule in the plan, merged and sorted by time -- GetBackupPlanOutput's
// documented behavior for MaxScheduledRunsPreview > 0. A rule with an
// unparseable ScheduleExpression is skipped rather than aborting the whole
// preview (real AWS validates ScheduleExpression at CreateBackupPlan/
// UpdateBackupPlan time; this backend does not model that validation, so a
// malformed expression can only be reached by a caller bypassing it).
func ScheduledRunsPreview(plan *Plan, maxResults int, now time.Time) []ScheduledPlanExecution {
	if maxResults <= 0 {
		return nil
	}

	if maxResults > maxScheduledRunsPreviewResults {
		maxResults = maxScheduledRunsPreviewResults
	}

	var out []ScheduledPlanExecution

	for _, r := range plan.Rules {
		expr := r.ScheduleExpression
		if expr == "" {
			expr = defaultRuleScheduleExpression
		}

		ce, err := parsePlanCronExpression(expr)
		if err != nil {
			continue
		}

		loc := ruleLocation(r)
		t := now.In(loc)

		for range maxResults {
			t = ce.nextAfter(t)
			if t.IsZero() {
				break
			}

			out = append(out, ScheduledPlanExecution{
				ExecutionTime:     t.UTC(),
				RuleExecutionType: ruleExecutionType(r),
				RuleID:            r.RuleID,
			})
		}
	}

	slices.SortFunc(out, func(a, b ScheduledPlanExecution) int {
		return a.ExecutionTime.Compare(b.ExecutionTime)
	})

	if len(out) > maxResults {
		out = out[:maxResults]
	}

	return out
}
