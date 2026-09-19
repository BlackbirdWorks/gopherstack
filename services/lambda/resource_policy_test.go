package lambda_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendResourcePolicy_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *lambda.InMemoryBackend)
		name string
	}{
		{
			name: "get unknown function",
			run: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()

				_, err := b.GetResourcePolicy("arn:aws:lambda:us-east-1:000000000000:function:missing")
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
		{
			name: "put empty policy",
			run: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()

				_, err := b.PutResourcePolicy("arn:aws:lambda:us-east-1:000000000000:function:missing", "", "")
				require.ErrorIs(t, err, lambda.ErrInvalidParameterValue)
			},
		},
		{
			name: "put unknown function",
			run: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()

				_, err := b.PutResourcePolicy("arn:aws:lambda:us-east-1:000000000000:function:missing", "{}", "")
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
		{
			name: "delete unknown function",
			run: func(t *testing.T, b *lambda.InMemoryBackend) {
				t.Helper()

				err := b.DeleteResourcePolicy("arn:aws:lambda:us-east-1:000000000000:function:missing", "")
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newInMemoryHandler(t)
			_ = h
			tc.run(t, b)
		})
	}
}

func TestBackendResourcePolicy_QualifierScoped(t *testing.T) {
	t.Parallel()

	h, b := newInMemoryHandler(t)
	_ = h

	require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "qual-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:qual-fn",
		PackageType:  "Zip",
	}))

	unqualified, err := b.PutResourcePolicy(
		"arn:aws:lambda:us-east-1:000000000000:function:qual-fn", `{"a":1}`, "",
	)
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, unqualified.Policy)

	// A qualified target (nonexistent version/alias) must be rejected, not
	// silently accepted as the unqualified target.
	_, err = b.PutResourcePolicy(
		"arn:aws:lambda:us-east-1:000000000000:function:qual-fn:v1", `{"b":2}`, "",
	)
	require.ErrorIs(t, err, lambda.ErrVersionNotFound)

	// The unqualified policy must be untouched by the rejected qualified attempt.
	got, err := b.GetResourcePolicy("arn:aws:lambda:us-east-1:000000000000:function:qual-fn")
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, got.Policy)
}
