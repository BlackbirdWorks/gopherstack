package athena

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisteredFuncNamesArePrefixed(t *testing.T) {
	t.Parallel()

	names := registeredFuncNames()
	require.NotEmpty(t, names)

	seen := map[string]bool{}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, strings.HasPrefix(name, funcPrefix), "%s lacks the %s prefix", name, funcPrefix)
		})

		assert.False(t, seen[name], "duplicate %s", name)

		seen[name] = true
	}
}

func TestSharedFuncNamesRouteToPrefixed(t *testing.T) {
	t.Parallel()

	cases := []struct{ in, want string }{
		{"to_milliseconds", "athena_to_milliseconds"},
		{"parse_duration", "athena_parse_duration"},
		{"array_agg", "athena_array_agg"},
		{"minute", "athena_minute"},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			got, ok := athenaFunc(tc.in)
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEmbeddedTranslationKeepsOwnerFunctions(t *testing.T) {
	t.Parallel()

	none := func([]string) resolvedRef { return resolvedRef{} }

	cases := []struct {
		name     string
		want     string
		embedded bool
	}{
		{"athena", "SELECT athena_to_milliseconds(x), athena_reverse(y)", false},
		{"embedded", "SELECT to_milliseconds(x), athena_reverse(y)", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, _, err := translateSQLFor("SELECT to_milliseconds(x), reverse(y)", none, tc.embedded)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
