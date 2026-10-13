package pinpoint

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

func campaignFrequencies() []string {
	return []string{"ONCE", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "EVENT", "IN_APP_EVENT"}
}

func journeyStates() []string {
	return []string{"DRAFT", "ACTIVE", "COMPLETED", "CANCELLED", "CLOSED", "PAUSED"}
}

func jobFormats() []string { return []string{"CSV", "JSON"} }

const campaignStartImmediate = "IMMEDIATE"

func badRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

// validateCampaignSchedule checks Schedule.Frequency and Schedule.StartTime (IMMEDIATE or ISO 8601).
func validateCampaignSchedule(schedule map[string]any) error {
	if freq, ok := schedule["Frequency"].(string); ok && freq != "" && !slices.Contains(campaignFrequencies(), freq) {
		return badRequest("Schedule.Frequency %q must be one of %s", freq, strings.Join(campaignFrequencies(), ", "))
	}

	start, ok := schedule["StartTime"].(string)
	if !ok || start == "" || start == campaignStartImmediate {
		return nil
	}

	if _, err := time.Parse(time.RFC3339, start); err != nil {
		return badRequest("Schedule.StartTime %q must be IMMEDIATE or an ISO 8601 timestamp", start)
	}

	return nil
}

func validateJourneyStateName(state string) error {
	if state != "" && !slices.Contains(journeyStates(), state) {
		return badRequest("State %q must be one of %s", state, strings.Join(journeyStates(), ", "))
	}

	return nil
}

// validateJobLocation checks an S3 location and, when format is non-empty, the import Format enum.
func validateJobLocation(field, location, format string) error {
	if !strings.HasPrefix(location, "s3://") {
		return badRequest("%s %q must start with s3://", field, location)
	}

	if format != "" && !slices.Contains(jobFormats(), format) {
		return badRequest("Format %q must be one of %s", format, strings.Join(jobFormats(), ", "))
	}

	return nil
}
