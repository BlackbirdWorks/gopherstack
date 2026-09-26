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

// TestAllocateIpamPoolCidr_PreviewNextCidr covers gopherstack's fix for
// AllocateIpamPoolCidrInput.PreviewNextCidr (aws-sdk-go-v2/service/ec2
// api_op_AllocateIpamPoolCidr.go:96, serializers.go:68952-68954): the emulator
// previously ignored the flag and always recorded a real allocation, which
// consumed pool space and leaked into GetIpamPoolAllocations. A preview call
// must compute the next CIDR without any side effect.
func TestAllocateIpamPoolCidr_PreviewNextCidr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		preview bool
	}{
		{name: "preview_true_no_allocation_recorded", preview: true},
		{name: "preview_false_allocation_recorded", preview: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			h := ec2.NewHandler(backend)
			client := newTestEC2Client(t, h)

			ipamOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
			require.NoError(t, err)
			scopeID := aws.ToString(ipamOut.Ipam.PrivateDefaultScopeId)

			poolOut, err := client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
				IpamScopeId:   aws.String(scopeID),
				AddressFamily: types.AddressFamilyIpv4,
			})
			require.NoError(t, err)
			poolID := aws.ToString(poolOut.IpamPool.IpamPoolId)

			_, err = client.ProvisionIpamPoolCidr(t.Context(), &ec2sdk.ProvisionIpamPoolCidrInput{
				IpamPoolId: aws.String(poolID),
				Cidr:       aws.String("10.30.0.0/16"),
			})
			require.NoError(t, err)

			allocOut, err := client.AllocateIpamPoolCidr(t.Context(), &ec2sdk.AllocateIpamPoolCidrInput{
				IpamPoolId:      aws.String(poolID),
				NetmaskLength:   aws.Int32(24),
				PreviewNextCidr: aws.Bool(tt.preview),
			})
			require.NoError(t, err)
			require.NotNil(t, allocOut.IpamPoolAllocation)
			assert.Contains(t, aws.ToString(allocOut.IpamPoolAllocation.Cidr), "/24")

			getAllocOut, err := client.GetIpamPoolAllocations(t.Context(), &ec2sdk.GetIpamPoolAllocationsInput{
				IpamPoolId: aws.String(poolID),
			})
			require.NoError(t, err)

			if tt.preview {
				assert.Empty(t, getAllocOut.IpamPoolAllocations, "a preview allocation must not be recorded")
			} else {
				assert.Len(t, getAllocOut.IpamPoolAllocations, 1, "a real allocation must be recorded")
			}
		})
	}
}
