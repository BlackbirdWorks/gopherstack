package gcppubsub_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

func TestFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs  map[string]string
		name   string
		filter string
		want   bool
	}{
		{map[string]string{"a": "x"}, "empty matches all", "", true},
		{map[string]string{"a": "x"}, "equals hit", `attributes.a = "x"`, true},
		{map[string]string{"a": "y"}, "equals miss", `attributes.a = "x"`, false},
		{nil, "equals absent", `attributes.a = "x"`, false},
		{map[string]string{"a": "y"}, "not equals hit", `attributes.a != "x"`, true},
		{map[string]string{"a": "x"}, "not equals miss", `attributes.a != "x"`, false},
		{nil, "not equals absent", `attributes.a != "x"`, true},
		{map[string]string{"a": ""}, "has hit", `attributes:a`, true},
		{nil, "has miss", `attributes:a`, false},
		{map[string]string{"a": "foobar"}, "prefix hit", `hasPrefix(attributes.a, "foo")`, true},
		{map[string]string{"a": "barfoo"}, "prefix miss", `hasPrefix(attributes.a, "foo")`, false},
		{map[string]string{"a": "1", "b": "2"}, "and", `attributes.a = "1" AND attributes.b = "2"`, true},
		{map[string]string{"a": "1"}, "and miss", `attributes.a = "1" AND attributes.b = "2"`, false},
		{map[string]string{"b": "2"}, "or", `attributes.a = "1" OR attributes.b = "2"`, true},
		{map[string]string{"a": "1"}, "not", `NOT attributes:b`, true},
		{
			map[string]string{"a": "1", "b": "2"},
			"precedence",
			`attributes.a = "9" AND attributes.b = "9" OR attributes.a = "1"`,
			true,
		},
		{
			map[string]string{"a": "1", "c": "3"},
			"parens",
			`attributes.a = "1" AND (attributes.b = "2" OR attributes.c = "3")`,
			true,
		},
		{map[string]string{"my-attr": "v"}, "hyphen name", `attributes.my-attr = "v"`, true},
		{map[string]string{"my attr": "v"}, "quoted name", `attributes."my attr" = "v"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eval, err := gcppubsub.CompileFilter(tt.filter)
			require.NoError(t, err)
			assert.Equal(t, tt.want, eval(tt.attrs))
		})
	}
}

func TestFilterInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, filter string }{
		{"bare word", `foo`},
		{"unterminated", `attributes.a = "x`},
		{"missing value", `attributes.a =`},
		{"unquoted value", `attributes.a = x`},
		{"dangling and", `attributes:a AND`},
		{"unbalanced paren", `(attributes:a`},
		{"bad prefix arg", `hasPrefix(attributes:a, "x")`},
		{"too long", string(make([]byte, 300))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gcppubsub.CompileFilter(tt.filter)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}
