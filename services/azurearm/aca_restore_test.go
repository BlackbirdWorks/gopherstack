package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACA_RestoreStartsMinReplicas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		min  int32
		want int
	}{
		{name: "min zero", min: 0, want: 0},
		{name: "min two", min: 2, want: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			envID := *s.mustEnv(t, "rg", "env1").ID

			s.mustApp(t, "rg", "persist", envID, acaApp{
				extIng: true,
				scale:  &armappcontainers.Scale{MinReplicas: new(tc.min), MaxReplicas: new(int32(5))},
			})

			snap := s.handler.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			other := newACAStack(t)
			require.NoError(t, other.handler.Restore(t.Context(), snap))
			assert.Equal(t, tc.want, other.rt.live())
		})
	}
}
