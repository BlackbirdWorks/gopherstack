package neptune_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

type fakeSubnets map[string]struct {
	vpc  string
	dual bool
}

func (f fakeSubnets) SubnetNetwork(id string) (string, bool, bool) {
	s, ok := f[id]

	return s.vpc, s.dual, ok
}

func TestSubnetNetwork(t *testing.T) {
	t.Parallel()

	subnets := fakeSubnets{
		"subnet-v4a":   {vpc: "vpc-1"},
		"subnet-dualA": {vpc: "vpc-2", dual: true},
		"subnet-dualB": {vpc: "vpc-2", dual: true},
	}

	tests := []struct {
		name      string
		subnetIDs []string
		wantVpc   string
		wantTypes []string
		dualErr   bool
	}{
		{
			name:      "ipv4_only",
			subnetIDs: []string{"subnet-v4a"},
			wantVpc:   "vpc-1",
			wantTypes: []string{"IPV4"},
			dualErr:   true,
		},
		{
			name:      "mixed",
			subnetIDs: []string{"subnet-v4a", "subnet-dualA"},
			wantVpc:   "vpc-1",
			wantTypes: []string{"IPV4"},
			dualErr:   true,
		},
		{
			name:      "dual",
			subnetIDs: []string{"subnet-dualA", "subnet-dualB"},
			wantVpc:   "vpc-2",
			wantTypes: []string{"IPV4", "DUAL"},
		},
		{name: "unknown_subnet", subnetIDs: []string{"subnet-nope"}, wantTypes: []string{"IPV4"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetSubnetResolver(subnets)

			sg, err := b.CreateDBSubnetGroup(t.Context(), "sg", "d", "", tt.subnetIDs)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVpc, sg.VpcID)
			assert.Equal(t, tt.wantTypes, sg.SupportedNetworkTypes)

			got, err := b.DescribeDBSubnetGroups(t.Context(), "sg")
			require.NoError(t, err)
			assert.Equal(t, tt.wantTypes, got[0].SupportedNetworkTypes)

			_, err = b.CreateDBCluster(t.Context(), "dual-c", "", 0, neptune.DBClusterCreateOptions{
				NetworkType: "DUAL", DBSubnetGroupName: "sg",
			})
			if tt.dualErr {
				require.ErrorIs(t, err, neptune.ErrNetworkTypeNotSupported)

				return
			}

			require.NoError(t, err)

			if tt.wantVpc == "" {
				return
			}

			snap, err := b.CreateDBClusterSnapshot(t.Context(), "snap", "dual-c")
			require.NoError(t, err)
			assert.Equal(t, tt.wantVpc, snap.VpcID)
		})
	}
}
