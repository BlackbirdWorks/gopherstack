package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	gcpidtkbackend "github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
)

func TestReserveFixedServicePorts_GCPIdentityToolkit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       int
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", gcpidtkbackend.DefaultPort, 8000, 9000, true},
		{"port outside pool range", gcpidtkbackend.DefaultPort, 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{GCPIdentityToolkit: gcpidtkbackend.Settings{Port: tt.port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(tt.port))
		})
	}
}
