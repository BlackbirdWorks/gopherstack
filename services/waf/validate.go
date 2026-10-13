package waf

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

const maxNameLen = 128

var metricNamePattern = regexp.MustCompile(`^[0-9A-Za-z]+$`)

// validateCreateNames enforces the documented Name (1-128 chars) and MetricName
// (alphanumeric, 1-128 chars, not the reserved "All"/"Default_Action") constraints.
// Empty values are skipped: the SDK validators already require both members, and many in-repo
// callers omit them.
func validateCreateNames(name, metricName string) error {
	if n := utf8.RuneCountInString(name); n > maxNameLen {
		return fmt.Errorf("%w: Name must be at most %d characters", ErrInvalidParameter, maxNameLen)
	}

	if metricName == "" {
		return nil
	}

	if utf8.RuneCountInString(metricName) > maxNameLen || !metricNamePattern.MatchString(metricName) {
		return fmt.Errorf(
			"%w: MetricName must be 1-%d alphanumeric characters without whitespace",
			ErrInvalidParameter, maxNameLen,
		)
	}

	if metricName == "All" || metricName == "Default_Action" {
		return fmt.Errorf("%w: MetricName %q is reserved", ErrInvalidParameter, metricName)
	}

	return nil
}

// wireMessage drops the leading wire code from err and falls back to the documented message
// for errors that carry nothing else.
func wireMessage(code string, err error) string {
	msg := strings.TrimPrefix(err.Error(), code)
	msg = strings.TrimPrefix(msg, ": ")

	if msg == "" {
		if def := defaultMessage(code); def != "" {
			return def
		}

		return code
	}

	return msg
}

func defaultMessage(code string) string {
	return map[string]string{
		errStaleData:        "The input token is no longer current.",
		errResourceNotFound: "The referenced item does not exist.",
		errReferencedItem:   "The operation failed because you tried to delete an object that is still in use.",
		errNonEmptyEntity:   "The operation failed because you tried to delete an object that isn't empty.",
		errInvalidOperation: "The operation failed because there was nothing to do.",
	}[code]
}
