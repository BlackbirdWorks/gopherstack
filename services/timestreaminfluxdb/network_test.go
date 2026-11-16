package timestreaminfluxdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type vpcResolver struct {
	subnets map[string]string
	groups  map[string]string
}

func (v vpcResolver) SubnetCIDR(_, id string) (string, bool) {
	_, ok := v.subnets[id]

	return "10.0.0.0/24", ok
}

func (v vpcResolver) SubnetVPC(_, id string) (string, bool) {
	vpc, ok := v.subnets[id]

	return vpc, ok
}

func (v vpcResolver) SecurityGroupVPC(_, id string) (string, bool) {
	vpc, ok := v.groups[id]

	return vpc, ok
}

func TestNetworkMembership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subnets []string
		groups  []string
		wantErr bool
	}{
		{name: "same vpc", subnets: []string{"subnet-aaa111", "subnet-bbb222"}, groups: []string{"sg-bbb222"}},
		{
			name:    "subnets in two vpcs",
			subnets: []string{"subnet-aaa111", "subnet-ccc333"},
			groups:  []string{"sg-bbb222"},
			wantErr: true,
		},
		{name: "group in other vpc", subnets: []string{"subnet-aaa111"}, groups: []string{"sg-other11"}, wantErr: true},
		{name: "unknown group", subnets: []string{"subnet-aaa111"}, groups: []string{"sg-missing1"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetSubnetResolver(vpcResolver{
				subnets: map[string]string{
					"subnet-aaa111": "vpc-1",
					"subnet-bbb222": "vpc-1",
					"subnet-ccc333": "vpc-2",
				},
				groups: map[string]string{"sg-bbb222": "vpc-1", "sg-other11": "vpc-2"},
			})

			in := instanceInput("net")
			in.VpcSubnetIds, in.VpcSecurityGroupIds = tt.subnets, tt.groups

			_, err := f.client.CreateDbInstance(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, "ValidationException", apiCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}
