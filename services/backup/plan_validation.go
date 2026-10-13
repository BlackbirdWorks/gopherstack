package backup

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	minStartWindowMinutes       = 60
	minCompletionWindowMinutes  = 60
	minColdStorageRetentionDays = 90
)

var planNamePattern = regexp.MustCompile(`^[a-zA-Z0-9\-_.]{1,50}$`)

func validatePlanName(kind, name string) error {
	if !planNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: %s %q is invalid: must be 1-50 characters from [a-zA-Z0-9-_.]",
			ErrValidation, kind, name,
		)
	}

	return nil
}

func validateScheduleExpression(expr string) error {
	if _, err := parsePlanCronExpression(expr); err != nil {
		return fmt.Errorf("%w: ScheduleExpression is invalid: %s", ErrValidation, err.Error())
	}

	return nil
}

func validateRuleSettings(r Rule) error {
	if err := validatePlanName("RuleName", r.RuleName); err != nil {
		return err
	}
	if r.ScheduleExpression != "" {
		if err := validateScheduleExpression(r.ScheduleExpression); err != nil {
			return err
		}
	}
	if r.StartWindowMinutes != 0 && r.StartWindowMinutes < minStartWindowMinutes {
		return fmt.Errorf("%w: rule %q: StartWindowMinutes must be at least %d",
			ErrValidation, r.RuleName, minStartWindowMinutes)
	}
	if r.CompletionWindowMinutes != 0 && r.CompletionWindowMinutes < minCompletionWindowMinutes {
		return fmt.Errorf("%w: rule %q: CompletionWindowMinutes must be at least %d",
			ErrValidation, r.RuleName, minCompletionWindowMinutes)
	}

	return validateRuleLifecycle(r)
}

func validateRuleLifecycle(r Rule) error {
	lc := r.Lifecycle
	if lc == nil || lc.DeleteAfterDays <= 0 || lc.MoveToColdStorageAfterDays <= 0 {
		return nil
	}
	if lc.DeleteAfterDays <= lc.MoveToColdStorageAfterDays {
		return fmt.Errorf(
			"%w: rule %q: DeleteAfterDays must be greater than MoveToColdStorageAfterDays",
			ErrValidation, r.RuleName,
		)
	}
	if lc.DeleteAfterDays-lc.MoveToColdStorageAfterDays < minColdStorageRetentionDays {
		return fmt.Errorf(
			"%w: rule %q: DeleteAfterDays must be at least %d days greater than MoveToColdStorageAfterDays",
			ErrValidation, r.RuleName, minColdStorageRetentionDays,
		)
	}

	return nil
}

const minArnSegments = 6

func validateArnValue(field, v string) error {
	if !strings.HasPrefix(v, "arn:") || len(strings.SplitN(v, ":", minArnSegments+1)) < minArnSegments {
		return fmt.Errorf("%w: %s %q is not a valid ARN", ErrValidation, field, v)
	}

	return nil
}
