package ec2_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputValidation_ErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *ec2sdk.Client) error
		name     string
		wantCode string
	}{
		{
			name:     "vpc cidr too large",
			wantCode: "InvalidVpcRange",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/8")})

				return err
			},
		},
		{
			name:     "vpc cidr too small",
			wantCode: "InvalidVpcRange",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/30")})

				return err
			},
		},
		{
			name:     "vpc cidr malformed",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/8x")})

				return err
			},
		},
		{
			name:     "describe vpc malformed id",
			wantCode: "InvalidVpcID.Malformed",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{VpcIds: []string{"bogus"}})

				return err
			},
		},
		{
			name:     "describe vpc unknown id",
			wantCode: "InvalidVpcID.NotFound",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{
					VpcIds: []string{"vpc-0123456789abcdef0"},
				})

				return err
			},
		},
		{
			name:     "describe instance malformed id",
			wantCode: "InvalidInstanceID.Malformed",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
					InstanceIds: []string{"bogus"},
				})

				return err
			},
		},
		{
			name:     "describe instance unknown id",
			wantCode: "InvalidInstanceID.NotFound",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
					InstanceIds: []string{"i-0123456789abcdef0"},
				})

				return err
			},
		},
		{
			name:     "run instances malformed ami",
			wantCode: "InvalidAMIID.Malformed",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
					ImageId: aws.String("bogus"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
				})

				return err
			},
		},
		{
			name:     "run instances bad type",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
					ImageId: aws.String("ami-12345678"), InstanceType: "bogus.type",
					MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
				})

				return err
			},
		},
		{
			name:     "volume size zero",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
					AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(0),
				})

				return err
			},
		},
		{
			name:     "volume bad type",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
					AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8), VolumeType: "gp9",
				})

				return err
			},
		},
		{
			name:     "volume st1 below minimum",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
					AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8), VolumeType: types.VolumeTypeSt1,
				})

				return err
			},
		},
		{
			name:     "security group reserved prefix",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
					GroupName: aws.String("sg-mine"), Description: aws.String("d"),
				})

				return err
			},
		},
		{
			name:     "security group bad description",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
					GroupName: aws.String("ok"), Description: aws.String("bad§desc"),
				})

				return err
			},
		},
		{
			name:     "subnet bad prefix",
			wantCode: "InvalidSubnet.Range",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				vpc, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
				require.NoError(t, err)

				_, err = c.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
					VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.0.0/32"),
				})

				return err
			},
		},
		{
			name:     "subnet malformed zone",
			wantCode: "InvalidZone.NotFound",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				vpc, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
				require.NoError(t, err)

				_, err = c.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
					VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24"),
					AvailabilityZone: aws.String("nonsense"),
				})

				return err
			},
		},
		{
			name:     "volume malformed zone",
			wantCode: "InvalidZone.NotFound",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
					AvailabilityZone: aws.String("nonsense"), Size: aws.Int32(8),
				})

				return err
			},
		},
		{
			name:     "tag reserved prefix",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				vpc, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
				require.NoError(t, err)

				_, err = c.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
					Resources: []string{*vpc.Vpc.VpcId},
					Tags:      []types.Tag{{Key: aws.String("aws:x"), Value: aws.String("y")}},
				})

				return err
			},
		},
		{
			name:     "tag value too long",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				vpc, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
				require.NoError(t, err)

				_, err = c.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
					Resources: []string{*vpc.Vpc.VpcId},
					Tags:      []types.Tag{{Key: aws.String("k"), Value: aws.String(strings.Repeat("v", 257))}},
				})

				return err
			},
		},
		{
			name:     "run instances unknown key pair",
			wantCode: "InvalidKeyPair.NotFound",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
					ImageId: aws.String("ami-12345678"), InstanceType: types.InstanceTypeT3Micro,
					KeyName: aws.String("nokey"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
				})

				return err
			},
		},
		{
			name:     "allocate address bad domain",
			wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *ec2sdk.Client) error {
				t.Helper()

				_, err := c.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{Domain: "bogus"})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)

			err := tc.call(t, client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestInputValidation_DescribeInstancesNoEmptyReservation(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
		Filters: []types.Filter{{Name: aws.String("instance-state-name"), Values: []string{"running"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Reservations)
}

func TestNatGateway_LifecycleStates(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	addr, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{Domain: types.DomainTypeVpc})
	require.NoError(t, err)

	created, err := client.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		SubnetId: aws.String("subnet-default"), AllocationId: addr.AllocationId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.NatGatewayStatePending, created.NatGateway.State)

	b.TickLifecycleForTest()

	desc, err := client.DescribeNatGateways(t.Context(), &ec2sdk.DescribeNatGatewaysInput{
		NatGatewayIds: []string{*created.NatGateway.NatGatewayId},
	})
	require.NoError(t, err)
	require.Len(t, desc.NatGateways, 1)
	assert.Equal(t, types.NatGatewayStateAvailable, desc.NatGateways[0].State)
}

func TestErrorMessage_NoCodePrefix(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	_, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{VpcIds: []string{"vpc-0123456789abcdef0"}})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "The vpc ID 'vpc-0123456789abcdef0' does not exist", apiErr.ErrorMessage())

	_, err = client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/8")})
	require.ErrorAs(t, err, &apiErr)
	assert.NotContains(t, apiErr.ErrorMessage(), "InvalidVpcRange:")
}
