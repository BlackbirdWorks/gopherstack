package medialive_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

type fakeVPCNetwork struct {
	enis map[string]bool
	azs  map[string]string
	mu   sync.Mutex
	next int
}

func (f *fakeVPCNetwork) SubnetAZ(id string) (string, bool) {
	az, ok := f.azs[id]

	return az, ok
}

func (f *fakeVPCNetwork) CreateNetworkInterface(subnetID, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.next++
	id := fmt.Sprintf("eni-%s-%d", subnetID, f.next)
	f.enis[id] = true

	return id, nil
}

func (f *fakeVPCNetwork) DeleteNetworkInterface(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.enis, id)
}

func TestChannelVpcNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subnets []string
		wantAZs []string
	}{
		{name: "two_subnets", subnets: []string{"subnet-a", "subnet-b"}, wantAZs: []string{"us-east-1a", "us-east-1b"}},
		{name: "unknown_subnet", subnets: []string{"subnet-a", "subnet-x"}, wantAZs: []string{"us-east-1a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := medialive.NewInMemoryBackend("000000000000", "us-east-1")
			f := &fakeVPCNetwork{
				enis: map[string]bool{},
				azs:  map[string]string{"subnet-a": "us-east-1a", "subnet-b": "us-east-1b"},
			}
			b.SetVPCNetwork(f)

			ch, err := b.CreateChannel("c", "STANDARD", "arn:aws:iam::000000000000:role/r",
				medialive.ChannelAnywhereSettings{},
				medialive.ChannelCreateExtras{Vpc: medialive.ChannelVpcSettings{SubnetIDs: tt.subnets}}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAZs, ch.Vpc.AvailabilityZones)
			assert.Len(t, ch.Vpc.NetworkInterfaceIDs, len(tt.wantAZs))
			assert.Len(t, f.enis, len(tt.wantAZs))

			_, err = b.DeleteChannel(ch.ID)
			require.NoError(t, err)
			assert.Empty(t, f.enis)
		})
	}
}
