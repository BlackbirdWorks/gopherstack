package directconnect

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBgpFailoverTest_SurvivesCreationTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantVif  string
		wantPeer string
		stop     bool
	}{
		{name: "testing_holds_past_creation_hop", wantVif: VifStateTesting, wantPeer: BGPStatusDown},
		{name: "stop_restores_available", stop: true, wantVif: VifStateAvailable, wantPeer: BGPStatusUp},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := NewInMemoryBackend(context.Background(), "000000000000", "us-east-1")
				defer b.Close()

				conn, err := b.CreateConnection(&createConnectionRequest{
					Bandwidth: "1Gbps", ConnectionName: "c", Location: "EqDC2",
				})
				require.NoError(t, err)

				vif, err := b.CreatePrivateVirtualInterface(
					conn.ConnectionID,
					&newPrivateVifWire{VirtualInterfaceName: "v", Vlan: 500},
				)
				require.NoError(t, err)

				_, err = b.CreateBGPPeer(vif.VirtualInterfaceID, &newBGPPeerWire{Asn: 65001})
				require.NoError(t, err)

				dur := int32(60)
				_, err = b.StartBgpFailoverTest(vif.VirtualInterfaceID, nil, &dur)
				require.NoError(t, err)

				time.Sleep(2 * asyncTransitionDelay)
				synctest.Wait()

				if tt.stop {
					_, err = b.StopBgpFailoverTest(vif.VirtualInterfaceID)
					require.NoError(t, err)
				}

				got := b.DescribeVirtualInterfaces("", vif.VirtualInterfaceID)
				require.Len(t, got, 1)
				require.Equal(t, tt.wantVif, got[0].VirtualInterfaceState)
				require.Equal(t, tt.wantPeer, got[0].BgpPeers[0].BgpStatus)
			})
		})
	}
}
