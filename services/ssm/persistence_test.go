package ssm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ssm.InMemoryBackend) string
		verify func(t *testing.T, b *ssm.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *ssm.InMemoryBackend) string {
				_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
					Name:  "/test/param",
					Value: "my-value",
					Type:  "String",
				})
				if err != nil {
					return ""
				}

				return "/test/param"
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: id})
				require.NoError(t, err)
				assert.Equal(t, id, out.Parameter.Name)
				assert.Equal(t, "my-value", out.Parameter.Value)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ssm.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ssm.InMemoryBackend, _ string) {
				t.Helper()

				params := b.ListAll(context.TODO())
				assert.Empty(t, params)
			},
		},
		{
			name: "patch_operation_state_survives_round_trip",
			setup: func(b *ssm.InMemoryBackend) string {
				_, err := b.SendCommand(context.TODO(), &ssm.SendCommandInput{
					DocumentName: "AWS-RunPatchBaseline",
					InstanceIDs:  []string{"i-persist"},
					Parameters:   map[string][]string{"Operation": {"Scan"}},
				})
				if err != nil {
					return ""
				}

				return "i-persist"
			},
			verify: func(t *testing.T, b *ssm.InMemoryBackend, id string) {
				t.Helper()

				states, err := b.DescribeInstancePatchStates(
					context.TODO(),
					&ssm.DescribeInstancePatchStatesInput{InstanceIDs: []string{id}},
				)
				require.NoError(t, err)
				require.Len(t, states.InstancePatchStates, 1)
				assert.Equal(t, id, states.InstancePatchStates[0].InstanceID)

				patches, err := b.DescribeAvailablePatches(
					context.TODO(),
					&ssm.DescribeAvailablePatchesInput{},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, patches.Patches, "the seeded available-patches catalog must survive restore")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ssm.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ssm.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
