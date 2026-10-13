package swf

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	maxNameLength    = 256
	maxVersionLength = 64
)

// validateName applies SWF's shared identifier rules: 1..maxLen chars, no leading/trailing
// whitespace, none of ':' '/' '|' or control characters, and not the literal "arn".
func validateName(field, value string, maxLen int) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrValidation, field)
	}

	if utf8.RuneCountInString(value) > maxLen {
		return fmt.Errorf("%w: %s must be at most %d characters", ErrValidation, field, maxLen)
	}

	if value == "arn" || strings.TrimSpace(value) != value || strings.ContainsAny(value, ":/|") ||
		strings.IndexFunc(value, isControlRune) >= 0 {
		return fmt.Errorf(
			"%w: %s %q must not start or end with whitespace, contain ':', '/', '|' or control characters, or be 'arn'",
			ErrValidation, field, value,
		)
	}

	return nil
}

func isControlRune(r rune) bool {
	return r <= 0x1f || (r >= 0x7f && r <= 0x9f)
}
