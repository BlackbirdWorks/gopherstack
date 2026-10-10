package autoscaling

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var quotedName = regexp.MustCompile(`"([^"]*)"`)

func firstQuoted(msg string) string {
	if m := quotedName.FindStringSubmatch(msg); m != nil {
		return m[1]
	}

	return ""
}

// opErrorMessage returns the client-facing message for a backend error: AWS wording for the
// common not-found / already-exists cases, otherwise the detail without the sentinel's code prefix.
func opErrorMessage(err error) string {
	msg := err.Error()

	switch {
	case errors.Is(err, ErrInvalidNextToken):
		return "The NextToken value is invalid."
	case errors.Is(err, ErrGroupNotFound):
		return "AutoScalingGroup name not found - null"
	case errors.Is(err, ErrGroupAlreadyExists):
		return fmt.Sprintf(
			"AutoScalingGroup by this name already exists - A group with the name %s already exists",
			firstQuoted(msg))
	case errors.Is(err, ErrLaunchConfigurationAlreadyExists):
		return fmt.Sprintf(
			"Launch configuration by this name already exists - A launch configuration already exists with the name %s",
			firstQuoted(msg))
	case errors.Is(err, ErrLaunchConfigurationNotFound):
		return fmt.Sprintf(
			"Launch configuration name not found - A launch configuration with the name: %s does not exist",
			firstQuoted(msg))
	}

	for _, m := range errorMappings() {
		if rest, ok := strings.CutPrefix(msg, m.sentinel.Error()+": "); ok {
			return rest
		}
	}

	return msg
}

// errMsgf wraps sentinel with a client-facing message that may end in punctuation.
func errMsgf(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, fmt.Sprintf(format, args...))
}
