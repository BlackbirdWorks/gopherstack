package strs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

func TestEscapeSingleQuotes(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, want string }{
		{"plain", "abc", "abc"},
		{"quote", "a'b", "a''b"},
		{"only quotes", "''", "''''"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := strs.EscapeSingleQuotes(tt.in); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEscapeDoubleQuotes(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, want string }{
		{"plain", "abc", "abc"},
		{"quote", `a"b`, `a\"b`},
		{"backslash", `a\b`, `a\\b`},
		{"both", `\"`, `\\\"`},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := strs.EscapeDoubleQuotes(tt.in); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
