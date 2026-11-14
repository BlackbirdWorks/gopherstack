package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestAssignIpv6AddressesFromSubnetCIDR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cidr       string
		wantPrefix string
	}{
		{name: "subnet cidr", cidr: "2600:1f18:1234:ab00::/64", wantPrefix: "2600:1f18:1234:ab00:"},
		{name: "no cidr", wantPrefix: "2001:db8:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.cidr != "" {
				_, err := b.AssociateSubnetCidrBlock("subnet-default", tt.cidr)
				require.NoError(t, err)
			}

			ni, err := b.CreateNetworkInterface("subnet-default", "v6")
			require.NoError(t, err)

			addrs, err := b.AssignIpv6Addresses(ni.ID, 2)
			require.NoError(t, err)
			require.Len(t, addrs, 2)

			for _, a := range addrs {
				assert.Contains(t, a, tt.wantPrefix)
			}
		})
	}
}
