package ociqueue_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociqueue"
)

func TestCompileFilter(t *testing.T) {
	t.Parallel()

	attrs := map[string]string{"name": "Bob", "age": "40", "location": "Chicago", "bad-key": "x"}

	tests := []struct {
		name   string
		filter string
		want   bool
	}{
		{"empty", "", true},
		{"blank", "   ", true},
		{"exists", ":name", true},
		{"missing", ":region", false},
		{"not exists", "NOT :region", true},
		{"str eq", `:name = "Bob"`, true},
		{"str ne", `:name != "Bob"`, false},
		{"str ne other", `:name != "Al"`, true},
		{"prefix", `hasPrefix(:name, "Bo")`, true},
		{"suffix", `hasSuffix(:name, "ob")`, true},
		{"not prefix", `NOT hasPrefix(:name, "Bo")`, false},
		{"num eq", ":age = 40", true},
		{"num gt", ":age > 40", false},
		{"num ge", ":age >= 40", true},
		{"num lt", ":age < 41", true},
		{"num le", ":age <= 39", false},
		{"num ne", ":age != 40", false},
		{"num on string", ":name > 3", false},
		{"num ne on string", ":name != 3", true},
		{"missing num", ":height > 1", false},
		{"and not", `:name = "Bob" AND NOT :age`, false},
		{"paren", `:name = "Bob" AND NOT (:age > 40 OR :location = "Chicago")`, false},
		{"precedence", `:region OR :name = "Bob" AND :age = 1`, false},
		{"or", `:region OR :age = 40`, true},
		{"quoted key", `:"bad-key" = "x"`, true},
		{"hyphen key", `:bad-key = "x"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := oci.MatchFilter(tt.filter, attrs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompileFilterErrors(t *testing.T) {
	t.Parallel()

	for name, f := range map[string]string{
		"dangling and": ":a AND",
		"open paren":   "(:a",
		"lowercase":    ":a and :b",
		"bad key":      ":1a",
		"bang":         ":a ! 1",
		"unterminated": `:a = "x`,
		"str gt":       `:a > "x"`,
		"no literal":   ":a =",
		"prefix num":   "hasPrefix(:a, 1)",
		"too long":     ":a = \"" + strings.Repeat("x", 300) + "\"",
		"trailing":     ":a :b",
		"bare literal": "40",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := oci.MatchFilter(f, nil)
			require.Error(t, err)
		})
	}
}
