package ocicontainerinstances_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svc "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
)

func TestStartWorkerBindsEphemeralPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
	}{
		{name: "port zero", port: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := svc.NewHandler(svc.NewInMemoryBackend())
			h.Port = tt.port
			require.NoError(t, h.StartWorker(t.Context()))
			t.Cleanup(func() { h.Shutdown(t.Context()) })

			assert.Positive(t, h.Port)
			assert.NotEqual(t, svc.DefaultPort, h.Port)

			busy := svc.NewHandler(svc.NewInMemoryBackend())
			busy.Port = h.Port
			require.Error(t, busy.StartWorker(t.Context()))
		})
	}
}
