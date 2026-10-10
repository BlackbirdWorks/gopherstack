package dax

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	minutesPerDay        = hoursPerDay * minutesPerHour
	minutesPerWeek       = daysPerWeek * minutesPerDay
	minMaintenanceWindow = 60
	arnPartCount         = 6
	windowEdgeParts      = 3
	daysPerWeek          = 7
)

// SetLifecycleDelay overrides how long a cluster or node stays in a transitional status; zero keeps the 1s default.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.transitionDelayOverride = d
}

// transitionDelay returns the effective dwell time. Caller must hold b.mu.
func (b *InMemoryBackend) transitionDelay() time.Duration {
	if b.transitionDelayOverride > 0 {
		return b.transitionDelayOverride
	}

	return clusterTransitionDelay
}

// parseWindowEdge parses ddd:hh24:mi into minutes since Sunday 00:00.
func parseWindowEdge(edge string) (int, bool) {
	parts := strings.Split(strings.ToLower(edge), ":")
	if len(parts) != windowEdgeParts {
		return 0, false
	}

	day := slices.Index(maintenanceWindowDays, parts[0])
	hour, hourErr := strconv.Atoi(parts[1])
	minute, minuteErr := strconv.Atoi(parts[2])

	if day < 0 || hourErr != nil || minuteErr != nil || len(parts[1]) != 2 || len(parts[2]) != 2 ||
		hour < 0 || hour >= hoursPerDay || minute < 0 || minute >= minutesPerHour {
		return 0, false
	}

	return day*minutesPerDay + hour*minutesPerHour + minute, true
}

// validateMaintenanceWindow checks the ddd:hh24:mi-ddd:hh24:mi format and the 60-minute minimum.
func validateMaintenanceWindow(window string) error {
	startEdge, endEdge, found := strings.Cut(window, "-")
	start, startOK := parseWindowEdge(startEdge)
	end, endOK := parseWindowEdge(endEdge)

	if !found || !startOK || !endOK {
		return fmt.Errorf(
			"%w: PreferredMaintenanceWindow %q must use the format ddd:hh24:mi-ddd:hh24:mi",
			ErrInvalidParameterValue, window,
		)
	}

	if (end-start+minutesPerWeek)%minutesPerWeek < minMaintenanceWindow {
		return fmt.Errorf(
			"%w: PreferredMaintenanceWindow must span at least %d minutes",
			ErrInvalidParameterValue, minMaintenanceWindow,
		)
	}

	return nil
}

// validateRoleARN rejects an IamRoleArn that is not a well-formed IAM role ARN.
func validateRoleARN(roleARN string) error {
	parts := strings.SplitN(roleARN, ":", arnPartCount)
	if len(parts) != arnPartCount || parts[0] != "arn" || parts[2] != "iam" || !strings.HasPrefix(parts[5], "role/") {
		return fmt.Errorf("%w: IamRoleArn %q is not a valid IAM role ARN", ErrInvalidARN, roleARN)
	}

	return nil
}

func validateRoleAndWindow(input *CreateClusterInput) error {
	if input.IamRoleArn == "" {
		return fmt.Errorf("%w: IamRoleArn is required", ErrInvalidParameterValue)
	}

	if err := validateRoleARN(input.IamRoleArn); err != nil {
		return err
	}

	if input.PreferredMaintenanceWindow == "" {
		return nil
	}

	return validateMaintenanceWindow(input.PreferredMaintenanceWindow)
}

func validateUpdateInput(input *UpdateClusterInput) error {
	if input.ClusterName == "" {
		return fmt.Errorf("%w: ClusterName is required", ErrInvalidParameterValue)
	}

	if input.PreferredMaintenanceWindow == nil {
		return nil
	}

	return validateMaintenanceWindow(*input.PreferredMaintenanceWindow)
}
