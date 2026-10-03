package rds

import (
	"fmt"
	"strings"
	"time"
)

const (
	automationModeFull      = "full"
	automationModeAllPaused = "all-paused"

	minResumeAutomationMinutes = 60
	maxResumeAutomationMinutes = 1440
)

// resolveAutomation validates AutomationMode/ResumeFullAutomationModeMinutes for ModifyDBInstance.
func resolveAutomation(inst *DBInstance, opts DBInstanceOptions) (string, time.Time, error) {
	if opts.AutomationMode == "" && !opts.ResumeFullAutomationModeMinutesSet {
		return inst.AutomationMode, inst.ResumeFullAutomationModeTime, nil
	}
	if !strings.HasPrefix(inst.Engine, "custom-") {
		return "", time.Time{}, fmt.Errorf(
			"%w: AutomationMode applies only to RDS Custom DB instances", ErrInvalidParameterCombination,
		)
	}

	mode := opts.AutomationMode
	if mode == "" {
		mode = inst.AutomationMode
	}
	switch mode {
	case automationModeFull:
		if opts.ResumeFullAutomationModeMinutesSet {
			return "", time.Time{}, fmt.Errorf(
				"%w: ResumeFullAutomationModeMinutes requires AutomationMode all-paused",
				ErrInvalidParameterCombination,
			)
		}

		return mode, time.Time{}, nil
	case automationModeAllPaused:
	default:
		return "", time.Time{}, fmt.Errorf("%w: invalid AutomationMode %q", ErrInvalidParameter, mode)
	}

	minutes := minResumeAutomationMinutes
	if opts.ResumeFullAutomationModeMinutesSet {
		minutes = opts.ResumeFullAutomationModeMinutes
	}
	if minutes < minResumeAutomationMinutes || minutes > maxResumeAutomationMinutes {
		return "", time.Time{}, fmt.Errorf(
			"%w: ResumeFullAutomationModeMinutes must be between %d and %d",
			ErrInvalidParameter, minResumeAutomationMinutes, maxResumeAutomationMinutes,
		)
	}

	return mode, time.Now().UTC().Add(time.Duration(minutes) * time.Minute), nil
}

// resumeExpiredAutomationLocked returns paused-automation instances to full once their window ends.
func (b *InMemoryBackend) resumeExpiredAutomationLocked(now time.Time) {
	for _, inst := range b.instances.All() {
		if inst.AutomationMode == automationModeAllPaused && !inst.ResumeFullAutomationModeTime.IsZero() &&
			now.After(inst.ResumeFullAutomationModeTime) {
			inst.AutomationMode = automationModeFull
			inst.ResumeFullAutomationModeTime = time.Time{}
		}
	}
}
