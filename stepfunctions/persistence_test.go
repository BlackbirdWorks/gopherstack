package stepfunctions_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/stepfunctions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *stepfunctions.InMemoryBackend) string
		verify func(t *testing.T, b *stepfunctions.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *stepfunctions.InMemoryBackend) string {
				sm, err := b.CreateStateMachine(
					"test-sm",
					`{"Comment":"test","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
					"arn:aws:iam::000000000000:role/test",
					"STANDARD",
				)
				if err != nil {
					return ""
				}

				return sm.StateMachineArn
			},
			verify: func(t *testing.T, b *stepfunctions.InMemoryBackend, id string) {
				t.Helper()

				sm, err := b.DescribeStateMachine(id)
				require.NoError(t, err)
				assert.Equal(t, "test-sm", sm.Name)
				assert.Equal(t, id, sm.StateMachineArn)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *stepfunctions.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *stepfunctions.InMemoryBackend, _ string) {
				t.Helper()

				sms, _, err := b.ListStateMachines("", 0)
				require.NoError(t, err)
				assert.Empty(t, sms)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
