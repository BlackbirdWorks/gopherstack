package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

func TestReserveFixedServicePorts_GCPFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       int
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"grpc port inside pool range", gcpfunctionsbackend.DefaultPort, 8000, 9000, true},
		{"grpc port outside pool range", gcpfunctionsbackend.DefaultPort, 10000, 10100, false},
		{"ingress port inside pool range", gcpfunctionsbackend.DefaultIngressPort, 8000, 9000, true},
		{"ingress port outside pool range", gcpfunctionsbackend.DefaultIngressPort, 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{GCPFunctions: gcpfunctionsbackend.Settings{
				Port: gcpfunctionsbackend.DefaultPort, IngressPort: gcpfunctionsbackend.DefaultIngressPort,
			}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(tt.port))
		})
	}
}
