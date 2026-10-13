package rolesanywhere

import (
	"fmt"
	"regexp"
)

const (
	maxNameLen              = 255
	minDurationSeconds      = 900
	maxDurationSeconds      = 43200
	maxRoleArns             = 250
	maxManagedPolicyArns    = 50
	maxManagedPolicyArnLen  = 200
	maxCrlDataBytes         = 300000
	maxNotificationSettings = 50
	maxNotificationDays     = 360
)

var (
	resourceNameRe   = regexp.MustCompile(`^[ a-zA-Z0-9_-]*$`)
	roleArnRe        = regexp.MustCompile(`^arn:aws(-[^:]+)?:iam(:.*){2}(:role.*)$`)
	trustAnchorArnRe = regexp.MustCompile(`^arn:aws(-[^:]+)?:rolesanywhere(:.*){2}(:trust-anchor.*)$`)
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

// validateResourceName applies the ResourceName shape (1-255, [ a-zA-Z0-9-_]*)
// from botocore rolesanywhere service-2.json.
func validateResourceName(name string) error {
	if name == "" || len(name) > maxNameLen || !resourceNameRe.MatchString(name) {
		return invalid("name must be 1-%d characters of letters, digits, spaces, '-' or '_'", maxNameLen)
	}

	return nil
}

func validateDuration(seconds *int32) error {
	if seconds != nil && (*seconds < minDurationSeconds || *seconds > maxDurationSeconds) {
		return invalid("durationSeconds must be between %d and %d", minDurationSeconds, maxDurationSeconds)
	}

	return nil
}

func validateProfileLists(roleArns, managedPolicyArns []string) error {
	if len(roleArns) > maxRoleArns {
		return invalid("roleArns must contain at most %d entries", maxRoleArns)
	}

	for _, r := range roleArns {
		if len(r) > 1011 || !roleArnRe.MatchString(r) {
			return invalid("roleArn %q is not a valid IAM role ARN", r)
		}
	}

	if len(managedPolicyArns) > maxManagedPolicyArns {
		return invalid("managedPolicyArns must contain at most %d entries", maxManagedPolicyArns)
	}

	for _, m := range managedPolicyArns {
		if m == "" || len(m) > maxManagedPolicyArnLen {
			return invalid("managedPolicyArns entries must be 1-%d characters", maxManagedPolicyArnLen)
		}
	}

	return nil
}

func validateCrlInput(crlData []byte, trustAnchorArn string) error {
	if len(crlData) > maxCrlDataBytes {
		return invalid("crlData must not exceed %d bytes", maxCrlDataBytes)
	}

	if trustAnchorArn != "" && (len(trustAnchorArn) > 1011 || !trustAnchorArnRe.MatchString(trustAnchorArn)) {
		return invalid("trustAnchorArn %q is not a valid trust anchor ARN", trustAnchorArn)
	}

	return nil
}

func validateNotificationSettings(settings []NotificationSetting) error {
	if len(settings) > maxNotificationSettings {
		return invalid("notificationSettings must contain at most %d entries", maxNotificationSettings)
	}

	for _, s := range settings {
		if s.Event != "CA_CERTIFICATE_EXPIRY" && s.Event != "END_ENTITY_CERTIFICATE_EXPIRY" {
			return invalid("notification event %q is not a recognized value", s.Event)
		}

		if s.Channel != "" && s.Channel != "ALL" {
			return invalid("notification channel %q is not a recognized value", s.Channel)
		}

		if s.Threshold != nil && (*s.Threshold < 1 || *s.Threshold > maxNotificationDays) {
			return invalid("notification threshold must be between 1 and %d", maxNotificationDays)
		}
	}

	return nil
}
