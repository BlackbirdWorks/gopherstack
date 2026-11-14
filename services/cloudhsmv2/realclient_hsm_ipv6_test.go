package cloudhsmv2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ipv6Net struct{ *fakeNet }

func (n ipv6Net) AssignENIIPv6(_, subnetID, _ string) (string, error) {
	if subnetID != "subnet-a" {
		return "", nil
	}

	return "2600:1f18:1234:ab00::1", nil
}

func TestRealClient_HsmEniIPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		netType types.NetworkType
		zone    string
		wantV6  string
	}{
		{name: "dualstack", netType: types.NetworkTypeDualstack, zone: "us-east-1a", wantV6: "2600:1f18:1234:ab00::1"},
		{name: "dualstack no ipv6 subnet", netType: types.NetworkTypeDualstack, zone: "us-east-1b"},
		{name: "ipv4", netType: types.NetworkTypeIpv4, zone: "us-east-1a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetNetwork(ipv6Net{f.net})

			c, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{
				HsmType: aws.String("hsm1.medium"), NetworkType: tt.netType,
				SubnetIds: []string{"subnet-a", "subnet-b"},
			})
			require.NoError(t, err)
			f.clock.advance(time.Second)

			h, err := f.client.CreateHsm(t.Context(), &sdk.CreateHsmInput{
				ClusterId: c.Cluster.ClusterId, AvailabilityZone: aws.String(tt.zone),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantV6, aws.ToString(h.Hsm.EniIpV6))

			got := f.describeOne(t, aws.ToString(c.Cluster.ClusterId))
			require.Len(t, got.Hsms, 1)
			assert.Equal(t, tt.wantV6, aws.ToString(got.Hsms[0].EniIpV6))
		})
	}
}
