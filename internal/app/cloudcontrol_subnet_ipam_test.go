package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudControlSubnetIpamPool(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	c := ec2.NewFromConfig(fx.cfg)

	vpc, err := c.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String("10.50.0.0/16")})
	require.NoError(t, err)

	ipam, err := c.CreateIpam(t.Context(), &ec2.CreateIpamInput{
		OperatingRegions: []ec2types.AddIpamOperatingRegion{{RegionName: aws.String(fx.cfg.Region)}},
	})
	require.NoError(t, err)

	pool, err := c.CreateIpamPool(t.Context(), &ec2.CreateIpamPoolInput{
		IpamScopeId:   ipam.Ipam.PrivateDefaultScopeId,
		AddressFamily: ec2types.AddressFamilyIpv4,
		Locale:        aws.String(fx.cfg.Region),
	})
	require.NoError(t, err)

	_, err = c.ProvisionIpamPoolCidr(t.Context(), &ec2.ProvisionIpamPoolCidrInput{
		IpamPoolId: pool.IpamPool.IpamPoolId, Cidr: aws.String("10.50.0.0/16"),
	})
	require.NoError(t, err)

	cc := cloudcontrol.NewFromConfig(fx.cfg)
	created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
		TypeName: aws.String("AWS::EC2::Subnet"),
		DesiredState: aws.String(`{"VpcId":"` + aws.ToString(vpc.Vpc.VpcId) + `","Ipv4IpamPoolId":"` +
			aws.ToString(pool.IpamPool.IpamPoolId) + `","Ipv4NetmaskLength":24}`),
	})
	require.NoError(t, err)

	subnets, err := c.DescribeSubnets(t.Context(), &ec2.DescribeSubnetsInput{
		SubnetIds: []string{aws.ToString(created.ProgressEvent.Identifier)},
	})
	require.NoError(t, err)
	require.Len(t, subnets.Subnets, 1)
	assert.Regexp(t, `^10\.50\.\d+\.0/24$`, aws.ToString(subnets.Subnets[0].CidrBlock))
}
