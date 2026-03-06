package ssm_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				_, err := b.PutParameter(&ssm.PutParameterInput{
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

				out, err := b.GetParameter(&ssm.GetParameterInput{Name: id})
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

				params := b.ListAll()
				assert.Empty(t, params)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ssm.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := ssm.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
