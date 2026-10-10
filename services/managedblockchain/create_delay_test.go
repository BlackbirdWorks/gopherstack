package managedblockchain_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func TestCreateDelay_StatusProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		want     string
		delay    time.Duration
		elapsed  time.Duration
		notReady bool
	}{
		{name: "default", want: "AVAILABLE"},
		{name: "creating", delay: time.Hour, elapsed: time.Minute, want: "CREATING", notReady: true},
		{name: "available", delay: time.Hour, elapsed: 2 * time.Hour, want: "AVAILABLE", notReady: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := managedblockchain.NewInMemoryBackend()
				b.SetCreateDelay(tt.delay)

				network, member, err := b.CreateNetwork(
					testRegion, testAccountID, "net", "", "", "", "mem", "",
					nil, nil, nil, "", "admin", "",
				)
				require.NoError(t, err)

				node, nodeErr := b.CreateNode(
					testRegion, testAccountID, network.ID, member.ID, "bc.t3.small", "us-east-1a", "", nil,
				)
				if tt.notReady {
					require.ErrorIs(t, nodeErr, managedblockchain.ErrResourceNotReady)
					require.ErrorIs(t, b.DeleteMember(network.ID, member.ID), managedblockchain.ErrResourceNotReady)
				} else {
					require.NoError(t, nodeErr)
				}

				time.Sleep(tt.elapsed)

				if node == nil && tt.want == "AVAILABLE" {
					node, nodeErr = b.CreateNode(
						testRegion, testAccountID, network.ID, member.ID, "bc.t3.small", "us-east-1a", "", nil,
					)
					require.NoError(t, nodeErr)
				}

				gotNet, err := b.GetNetwork(network.ID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, gotNet.Status)

				gotMem, err := b.GetMember(network.ID, member.ID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, gotMem.Status)

				if tt.delay == 0 {
					gotNode, getErr := b.GetNode(network.ID, member.ID, node.ID)
					require.NoError(t, getErr)
					assert.Equal(t, "AVAILABLE", gotNode.Status)
				}
			})
		})
	}
}
