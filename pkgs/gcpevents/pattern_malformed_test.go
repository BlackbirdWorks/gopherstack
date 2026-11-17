package gcpevents_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
)

func TestMatchFilterMalformedInput(t *testing.T) {
	t.Parallel()

	stars := strings.Repeat("a*", 200) + "b"
	long := strings.Repeat("a", 5000)

	tests := []struct {
		name, pattern, value string
		want                 bool
	}{
		{"unclosed brace", "users/{id", "users/x", false},
		{"empty capture", "users/{}", "users/x", false},
		{"capture without name", "users/{=**}", "users/x", false},
		{"capture with other glob", "users/{id=a}", "users/x", false},
		{"nested braces", "users/{{id}}", "users/x", false},
		{"regex metacharacters", "(a+)+$", "(a+)+$", true},
		{"regex metacharacters not regex", "(a+)+$", "aaaa", false},
		{"many stars on long value", stars, long, false},
		{"empty pattern", "", "", true},
		{"empty pattern non-empty value", "", "x", false},
		{"leading slash", "/a", "/a", true},
		{"trailing slash", "a/", "a", false},
		{"embedded newline", "a\nb", "a\nb", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, gcpevents.MatchFilter(gcpevents.OperatorPathPattern, tt.pattern, tt.value))
		})
	}
}
