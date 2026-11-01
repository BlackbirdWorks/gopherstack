package eventbridge

import (
	"fmt"
	"net"
	"slices"
	"strings"
)

const (
	maxNumericRuleTerms     = 4
	matcherPrefix           = "prefix"
	matcherSuffix           = "suffix"
	matcherEqualsIgnoreCase = "equals-ignore-case"
	matcherWildcard         = "wildcard"
	matcherNumeric          = "numeric"
)

var numericOperators = []string{"=", "<", "<=", ">", ">="} //nolint:gochecknoglobals // static lookup table

// validateMatcherValue checks the value shape of a single matcher key such as
// {"prefix": ...} or {"numeric": [...]} so malformed patterns fail at PutRule time.
func validateMatcherValue(field, key string, val any) error {
	switch key {
	case matcherPrefix, matcherSuffix:
		return validateStringOrIgnoreCase(field, key, val)
	case matcherEqualsIgnoreCase:
		return requireString(field, key, val)
	case "exists":
		if _, ok := val.(bool); !ok {
			return reasonErr("exists match pattern must be either true or false.")
		}
	case "cidr":
		return validateCIDRMatcher(val)
	case matcherWildcard:
		return validateWildcardMatcher(val)
	case matcherNumeric:
		return validateNumericMatcher(val)
	}

	return nil
}

func requireString(_, key string, val any) error {
	if _, ok := val.(string); !ok {
		return fmt.Errorf("%w: %s match pattern must be a string", ErrInvalidParameter, key)
	}

	return nil
}

func validateStringOrIgnoreCase(field, key string, val any) error {
	if obj, ok := val.(map[string]any); ok {
		ic, hasIC := obj[matcherEqualsIgnoreCase]
		if len(obj) != 1 || !hasIC {
			return fmt.Errorf(
				"%w: %s match pattern must be a string or an equals-ignore-case object",
				ErrInvalidParameter,
				key,
			)
		}

		return requireString(field, key, ic)
	}

	return requireString(field, key, val)
}

func validateCIDRMatcher(val any) error {
	s, ok := val.(string)
	if !ok {
		return fmt.Errorf("%w: cidr match pattern must be a string", ErrInvalidParameter)
	}

	if _, _, err := net.ParseCIDR(s); err != nil && net.ParseIP(s) == nil {
		return fmt.Errorf("%w: Malformed CIDR, one or more octets not valid", ErrInvalidParameter)
	}

	return nil
}

func validateWildcardMatcher(val any) error {
	s, ok := val.(string)
	if !ok {
		return fmt.Errorf("%w: wildcard match pattern must be a string", ErrInvalidParameter)
	}

	if pos := strings.Index(s, "**"); pos >= 0 {
		return fmt.Errorf("%w: Consecutive wildcard characters at pos %d", ErrInvalidParameter, pos)
	}

	return nil
}

// validateNumericMatcher accepts one or two (operator, number) pairs; two pairs must form a
// range with a lower bound (> or >=) followed by an upper bound (< or <=).
func validateNumericMatcher(val any) error {
	rules, ok := val.([]any)
	if !ok || len(rules) == 0 || len(rules)%2 != 0 || len(rules) > maxNumericRuleTerms {
		return fmt.Errorf("%w: Bad numeric range operator", ErrInvalidParameter)
	}

	for i := 0; i < len(rules); i += 2 {
		op, opOK := rules[i].(string)
		if !opOK || !slices.Contains(numericOperators, op) {
			return fmt.Errorf("%w: Unrecognized numeric range operator: %v", ErrInvalidParameter, rules[i])
		}

		if _, numOK := rules[i+1].(float64); !numOK {
			return fmt.Errorf("%w: Value of %s must be numeric", ErrInvalidParameter, op)
		}
	}

	if len(rules) == maxNumericRuleTerms {
		lower, _ := rules[0].(string)
		upper, _ := rules[2].(string)

		if (lower != ">" && lower != ">=") || (upper != "<" && upper != "<=") {
			return fmt.Errorf("%w: Bad numeric range operator: %s", ErrInvalidParameter, lower)
		}
	}

	return nil
}
