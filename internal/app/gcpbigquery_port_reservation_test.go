package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpbigquerybackend "github.com/blackbirdworks/gopherstack/services/gcpbigquery"
)

func TestReserveFixedServicePorts_GCPBigQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantTaken bool
	}{
		{"port inside pool range", true},
		{"port outside pool range", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, gcpbigquerybackend.DefaultPort)

			cli := CLI{GCPBigQuery: gcpbigquerybackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
