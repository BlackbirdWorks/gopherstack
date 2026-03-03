package apigateway_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *apigateway.InMemoryBackend) string
		verify func(t *testing.T, b *apigateway.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *apigateway.InMemoryBackend) string {
				api, err := b.CreateRestAPI("test-api", "test", nil)
				if err != nil {
					return ""
				}

				return api.ID
			},
			verify: func(t *testing.T, b *apigateway.InMemoryBackend, id string) {
				t.Helper()

				api, err := b.GetRestAPI(id)
				require.NoError(t, err)
				assert.Equal(t, "test-api", api.Name)
				assert.Equal(t, id, api.ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *apigateway.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *apigateway.InMemoryBackend, _ string) {
				t.Helper()

				apis, _, err := b.GetRestAPIs(0, "")
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := apigateway.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := apigateway.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
