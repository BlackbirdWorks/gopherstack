package cloudformation_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudformation.InMemoryBackend) string
		verify func(t *testing.T, b *cloudformation.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *cloudformation.InMemoryBackend) string {
				stack, err := b.CreateStack(
					context.Background(),
					"test-stack",
					`{"AWSTemplateFormatVersion":"2010-09-09"}`,
					nil,
					nil,
				)
				if err != nil {
					return ""
				}

				return stack.StackName
			},
			verify: func(t *testing.T, b *cloudformation.InMemoryBackend, id string) {
				t.Helper()

				stack, err := b.DescribeStack(id)
				require.NoError(t, err)
				assert.Equal(t, id, stack.StackName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *cloudformation.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *cloudformation.InMemoryBackend, _ string) {
				t.Helper()

				stacks, err := b.ListStacks(nil, "")
				require.NoError(t, err)
				assert.Empty(t, stacks.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudformation.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := cloudformation.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudformation.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
