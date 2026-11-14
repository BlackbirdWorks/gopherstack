package gremlin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		script  string
		wantErr string
		want    []string
	}{
		{"to_upper", `g.V('marko').values('name').toUpper()`, "", []string{"MARKO"}},
		{"to_lower", `g.inject('AbC').toLower()`, "", []string{"abc"}},
		{"trim", `g.inject('  a b  ').trim()`, "", []string{"a b"}},
		{"ltrim", `g.inject('  a ').lTrim()`, "", []string{"a "}},
		{"rtrim", `g.inject('  a ').rTrim()`, "", []string{"  a"}},
		{"length", `g.V('marko').values('name').length()`, "", []string{"5"}},
		{"as_string_number", `g.V('marko').values('age').asString()`, "", []string{"29"}},
		{"as_string_float", `g.inject(1.5).asString()`, "", []string{"1.5"}},
		{"split", `g.inject('a,b,c').split(',')`, "", []string{"[a,b,c]"}},
		{"concat", `g.V('marko').values('name').concat('-x', '-y')`, "", []string{"marko-x-y"}},
		{"concat_traversal", `g.V('marko').values('name').concat(__.constant('!'))`, "", []string{"marko!"}},
		{"replace", `g.inject('banana').replace('an', 'AN')`, "", []string{"bANANa"}},
		{"substring_start", `g.inject('abcdef').substring(2)`, "", []string{"cdef"}},
		{"substring_range", `g.inject('abcdef').substring(1, 3)`, "", []string{"bcd"}},
		{"substring_out_of_range", `g.inject('abc').substring(9)`, "", []string{""}},
		{"list_elements", `g.inject(['a','b']).toUpper()`, "", []string{"[A,B]"}},
		{"non_string_rejected", `g.V('marko').values('age').toUpper()`, "must be a String", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalAll(t, seeded(t), tt.script)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
