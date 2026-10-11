package elbv2

import (
	"fmt"
	"slices"
	"strings"
)

const (
	condHostHeader   = "host-header"
	condPathPattern  = "path-pattern"
	condHTTPHeader   = "http-header"
	condHTTPMethod   = "http-request-method"
	condQueryString  = "query-string"
	condSourceIP     = "source-ip"
	maxConditionLen  = 128
	pathPatternChars = "_-.$/~\"'@:+&()*?%"
)

var ruleConditionFields = []string{ //nolint:gochecknoglobals // static lookup table
	condHostHeader, condPathPattern, condHTTPHeader, condHTTPMethod, condQueryString, condSourceIP,
}

// validateRuleConditions checks each condition names a known Field and carries the values that
// Field requires (types.RuleCondition).
func validateRuleConditions(conds []Condition) error {
	for _, c := range conds {
		if c.Field == "" {
			continue
		}

		if !slices.Contains(ruleConditionFields, c.Field) {
			return fmt.Errorf("%w: Condition field '%s' is not recognized", ErrInvalidParameter, c.Field)
		}

		if err := validateConditionValues(c); err != nil {
			return err
		}
	}

	return nil
}

func validateConditionValues(c Condition) error {
	switch c.Field {
	case condHostHeader, condPathPattern:
		return validateHostOrPath(c)
	case condHTTPHeader:
		if c.HTTPHeaderName == "" {
			return fmt.Errorf("%w: HttpHeaderName is required for an http-header condition", ErrInvalidParameter)
		}

		return requireValues(c, len(c.Values)+len(c.RegexValues))
	case condHTTPMethod:
		return requireValues(c, len(c.Values))
	case condQueryString:
		return requireValues(c, len(c.QueryStringPairs))
	}

	return nil
}

func requireValues(c Condition, n int) error {
	if n == 0 {
		return fmt.Errorf("%w: A %s condition requires at least one value", ErrInvalidParameter, c.Field)
	}

	return nil
}

func validateHostOrPath(c Condition) error {
	if err := requireValues(c, len(c.Values)+len(c.RegexValues)); err != nil {
		return err
	}

	for _, v := range c.Values {
		if len(v) > maxConditionLen {
			return fmt.Errorf("%w: Condition value '%s' is too long", ErrInvalidParameter, v)
		}

		if c.Field == condPathPattern && !validPathPattern(v) {
			return fmt.Errorf("%w: Condition value '%s' contains characters that are not valid", ErrInvalidParameter, v)
		}
	}

	return nil
}

// validPathPattern reports whether v uses only the characters PathPatternConditionConfig allows.
func validPathPattern(v string) bool {
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune(pathPatternChars, r):
		default:
			return false
		}
	}

	return true
}
