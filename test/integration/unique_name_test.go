package integration_test

import "github.com/google/uuid"

// shortUniqueName returns prefix plus an 8-char random suffix, safe for strict name validators.
func shortUniqueName(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}
