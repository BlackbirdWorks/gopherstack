package sts

import "regexp"

// wellFormedAccessKeyRe matches any well-formed AWS access key regardless of type prefix.
// Format: known 4-char prefix + 16 uppercase alphanumeric characters.
var wellFormedAccessKeyRe = regexp.MustCompile(`^(AKIA|ASIA|AIDA|AROA|AGPA|ANPA|ANVA|APKA|ASCA)[A-Z0-9]{16}$`)

// isWellFormedAccessKey reports whether the given key matches the well-formed AWS access key format:
// a known 4-char prefix followed by exactly 16 uppercase alphanumeric characters.
func isWellFormedAccessKey(key string) bool {
	return wellFormedAccessKeyRe.MatchString(key)
}
