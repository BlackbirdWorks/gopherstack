package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpworkflowsbackend "github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

func TestReserveFixedServicePorts_GCPWorkflows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpworkflowsbackend.DefaultPort, true},
		{"port outside pool range", gcpworkflowsbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPWorkflows: gcpworkflowsbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
