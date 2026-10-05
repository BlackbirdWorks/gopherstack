package ec2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWildcardMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, pattern, value string
		want                 bool
	}{
		{"exact", "abc", "abc", true},
		{"exact_miss", "abc", "abd", false},
		{"case_sensitive", "abc", "ABC", false},
		{"star_suffix", "i-0*", "i-0abc", true},
		{"star_prefix", "*-prod", "web-prod", true},
		{"star_middle", "a*c", "abbbc", true},
		{"star_empty", "a*", "a", true},
		{"star_only", "*", "anything", true},
		{"question_one", "a?c", "abc", true},
		{"question_not_zero", "a?c", "ac", false},
		{"question_not_two", "a?c", "abbc", false},
		{"escaped_star", `a\*c`, "a*c", true},
		{"escaped_star_miss", `a\*c`, "abc", false},
		{"backtrack", "*ab*cd", "xxabyabcd", true},
		{"unicode", "h?llo", "héllo", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, wildcardMatch(tt.pattern, tt.value))
		})
	}
}
