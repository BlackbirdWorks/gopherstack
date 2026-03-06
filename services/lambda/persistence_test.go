package lambda_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLambdaBackend() *lambda.InMemoryBackend {
	return lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
}

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *lambda.InMemoryBackend) string
		verify func(t *testing.T, b *lambda.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *lambda.InMemoryBackend) string {
				fn := &lambda.FunctionConfiguration{
					FunctionName: "test-fn",
					Runtime:      "python3.9",
					Role:         "arn:aws:iam::000000000000:role/test",
					Handler:      "index.handler",
				}
				err := b.CreateFunction(fn)
				if err != nil {
					return ""
				}

				return fn.FunctionName
			},
			verify: func(t *testing.T, b *lambda.InMemoryBackend, id string) {
				t.Helper()

				fn, err := b.GetFunction(id)
				require.NoError(t, err)
				assert.Equal(t, id, fn.FunctionName)
				assert.Equal(t, "python3.9", fn.Runtime)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *lambda.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *lambda.InMemoryBackend, _ string) {
				t.Helper()

				fns := b.ListFunctions("", 0)
				assert.Empty(t, fns.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := newLambdaBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := newLambdaBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := newLambdaBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
