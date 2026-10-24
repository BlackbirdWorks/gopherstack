package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	rumbackend "github.com/blackbirdworks/gopherstack/services/rum"
)

func TestInitializeServices_RUMTagsWiring(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)

	h, ok := byName["RUM"].(*rumbackend.Handler)
	require.True(t, ok)

	_, err := h.Backend.CreateAppMonitor(t.Context(), rumbackend.CreateMonitorInput{
		Name: "mon", Domain: "example.com", Tags: map[string]string{"Team": "emu"},
	})
	require.NoError(t, err)

	monitorARN := "arn:aws:rum:us-east-1:000000000000:appmonitor/mon"

	runRGTWiringCases(t, rgtH, []rgtWiringCase{
		{
			name:         "app monitor",
			arn:          monitorARN,
			resourceType: "rum:appmonitor",
			listTags: func(ctx context.Context) (map[string]string, error) {
				return h.Backend.ListTagsForResource(ctx, monitorARN)
			},
		},
	})
}
