package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	ocistreamingbackend "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

func TestReserveFixedServicePorts_OCIStreaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", ocistreamingbackend.DefaultPort, true},
		{"port outside pool range", ocistreamingbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{OCIStreaming: ocistreamingbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
