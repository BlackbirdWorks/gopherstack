package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_DescribeMacHostsFilters covers DescribeMacHosts, which
// previously ignored Filters entirely. MacHost itself carries neither
// availability-zone nor instance-type on the wire, so these filters are
// cross-referenced against the underlying Dedicated Host allocated via
// AllocateHosts.
func TestRealClient_DescribeMacHostsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	hostA, err := client.AllocateHosts(t.Context(), &ec2sdk.AllocateHostsInput{
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceType:     aws.String("mac2.metal"),
		Quantity:         aws.Int32(1),
	})
	require.NoError(t, err)
	hostB, err := client.AllocateHosts(t.Context(), &ec2sdk.AllocateHostsInput{
		AvailabilityZone: aws.String("us-east-1b"),
		InstanceType:     aws.String("mac1.metal"),
		Quantity:         aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, hostA.HostIds, 1)
	require.Len(t, hostB.HostIds, 1)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "availability-zone",
			filters: []types.Filter{{Name: aws.String("availability-zone"), Values: []string{"us-east-1a"}}},
			want:    []string{hostA.HostIds[0]},
		},
		{
			name:    "instance-type",
			filters: []types.Filter{{Name: aws.String("instance-type"), Values: []string{"mac1.metal"}}},
			want:    []string{hostB.HostIds[0]},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("instance-type"), Values: []string{"mac2-m2.metal"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeMacHosts(t.Context(), &ec2sdk.DescribeMacHostsInput{Filters: tt.filters})
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.MacHosts))
			for _, mh := range out.MacHosts {
				got = append(got, aws.ToString(mh.HostId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
