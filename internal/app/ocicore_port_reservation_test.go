package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestReserveFixedServicePorts_OCICore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", ocicorebackend.DefaultPort, true},
		{"port outside pool range", ocicorebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{OCICore: ocicorebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
