package azurearm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicMoreFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		expr string
	}{
		{name: "decodeBase64", expr: "@decodeBase64('aGVsbG8=')", want: "hello"},
		{
			name: "decodeUriComponent", want: "https://contoso.com",
			expr: "@decodeUriComponent('https%3A%2F%2Fcontoso.com')",
		},
		{
			name: "encodeUriComponent", want: "https%3A%2F%2Fcontoso.com",
			expr: "@encodeUriComponent('https://contoso.com')",
		},
		{name: "dataUri", expr: "@dataUri('hello')", want: "data:text/plain;charset=utf-8;base64,aGVsbG8="},
		{
			name: "dataUriToString", want: "hello",
			expr: "@dataUriToString('data:text/plain;charset=utf-8;base64,aGVsbG8=')",
		},
		{name: "dataUriToString plain", expr: "@dataUriToString('data:,a%20b')", want: "a b"},
		{name: "dayOfMonth", expr: "@dayOfMonth('2018-03-15T13:27:36Z')", want: int64(15)},
		{name: "dayOfWeek", expr: "@dayOfWeek('2018-03-15T13:27:36Z')", want: int64(4)},
		{name: "dayOfYear", expr: "@dayOfYear('2018-03-15T13:27:36Z')", want: int64(74)},
		{name: "getFutureTime", expr: "@getFutureTime(5, 'Day', 'yyyy-MM-dd')", want: "2025-09-30"},
		{name: "getPastTime", expr: "@getPastTime(5, 'Month', 'yyyy-MM-dd')", want: "2025-04-25"},
		{name: "rand single", expr: "@rand(7, 8)", want: int64(7)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicMoreFunctionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "rand empty range", expr: "@rand(5, 5)"},
		{name: "rand non int", expr: "@rand('a', 5)"},
		{name: "data uri invalid", expr: "@dataUriToString('hello')"},
		{name: "future arity", expr: "@getFutureTime(1)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalLogicTemplate(tc.expr, logicTestCtx())
			require.Error(t, err)
		})
	}
}

func TestLogicRandRange(t *testing.T) {
	t.Parallel()

	for range 50 {
		got, err := evalLogicTemplate("@rand(-3, 3)", logicTestCtx())
		require.NoError(t, err)

		n, ok := got.(int64)
		require.True(t, ok)
		assert.GreaterOrEqual(t, n, int64(-3))
		assert.Less(t, n, int64(3))
	}
}
