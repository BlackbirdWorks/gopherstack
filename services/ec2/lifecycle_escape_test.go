package ec2_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// Run under -race: the reconciler writes State while Create returns objects.
func TestBackendReturnsDoNotAliasReconciledState(t *testing.T) {
	t.Parallel()

	const iterations = 200

	tests := []struct {
		create func(t *testing.T, b *ec2.InMemoryBackend) string
		name   string
	}{
		{
			name: "nat_gateway",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				addr, err := b.AllocateAddress()
				require.NoError(t, err)

				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID, nil)
				require.NoError(t, err)

				_, err = b.AssignPrivateNatGatewayAddress(ngw.ID, 1, nil)
				require.NoError(t, err)

				for _, d := range b.DescribeNatGateways([]string{ngw.ID}) {
					_ = d.State
				}

				return ngw.State
			},
		},
		{
			name: "instance",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				insts, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)

				return insts[0].State.Name
			},
		},
		{
			name: "volume",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				b.SetVolumeCreateDelay(time.Nanosecond)

				vol, err := b.CreateVolume("", "gp2", 8, "")
				require.NoError(t, err)

				return vol.State
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var wg sync.WaitGroup

			wg.Go(func() {
				for range iterations {
					b.TickLifecycleForTest()
				}
			})

			for range iterations {
				require.NotEmpty(t, tt.create(t, b))
			}

			wg.Wait()
		})
	}
}
