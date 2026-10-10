package route53

import (
	"fmt"
	"strings"
)

const (
	maxDomainNameLength = 253
	maxDNSLabelLength   = 63
)

// validateDomainName applies Route 53's InvalidDomainName rules to a hosted zone name.
func validateDomainName(name string) error {
	trimmed := strings.TrimSuffix(name, ".")
	if trimmed == "" || len(trimmed) > maxDomainNameLength {
		return fmt.Errorf("%w: %q is not a valid DNS name", ErrInvalidDomainName, name)
	}

	for label := range strings.SplitSeq(trimmed, ".") {
		if !validDNSLabel(label) {
			return fmt.Errorf("%w: %q is not a valid DNS name", ErrInvalidDomainName, name)
		}
	}

	return nil
}

func validDNSLabel(label string) bool {
	if label == "" || len(label) > maxDNSLabelLength {
		return false
	}

	for _, c := range label {
		valid := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '*'
		if !valid {
			return false
		}
	}

	return true
}
