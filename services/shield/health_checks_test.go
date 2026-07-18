package shield_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestBackend_AssociateHealthCheck tests backend health check association.
func TestBackend_AssociateHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*shield.InMemoryBackend) string
		name           string
		healthCheckARN string
		wantErr        bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-001",
		},
		{
			name: "idempotent",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2")
				require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc-002"))

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-002",
		},
		{
			name: "protection not found",
			setup: func(_ *shield.InMemoryBackend) string {
				return "no-such-id"
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-001",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(b)

			err := b.AssociateHealthCheck(id, tt.healthCheckARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_DisassociateHealthCheck tests health check disassociation.
func TestInMemoryBackend_DisassociateHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*shield.InMemoryBackend) string
		healthCheckARN string
		name           string
		wantErr        bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
				require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc-1"))

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-1",
		},
		{
			name: "health_check_not_associated",
			setup: func(b *shield.InMemoryBackend) string {
				p := b.AddProtectionInternal("p2", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-2")

				return p.ID
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-2",
			wantErr:        true,
		},
		{
			name: "protection_not_found",
			setup: func(*shield.InMemoryBackend) string {
				return "no-such-id"
			},
			healthCheckARN: "arn:aws:route53:::healthcheck/hc-3",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(b)

			err := b.DisassociateHealthCheck(id, tt.healthCheckARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
