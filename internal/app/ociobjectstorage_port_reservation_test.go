package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	ociobjectstoragebackend "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestReserveFixedServicePorts_OCIObjectStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", ociobjectstoragebackend.DefaultPort, true},
		{"port outside pool range", ociobjectstoragebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{OCIObjectStorage: ociobjectstoragebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
