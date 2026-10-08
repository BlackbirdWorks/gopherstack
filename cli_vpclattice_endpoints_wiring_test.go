package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice"
	latticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVPCLatticeEndpointAssociationsFromEC2(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	lattice := vpclattice.NewFromConfig(fx.cfg)
	ec2c := ec2.NewFromConfig(fx.cfg)

	vpc, err := ec2c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)

	vpcID := aws.ToString(vpc.Vpc.VpcId)

	sn, err := lattice.CreateServiceNetwork(ctx, &vpclattice.CreateServiceNetworkInput{Name: aws.String("sn-ep")})
	require.NoError(t, err)

	rc, err := lattice.CreateResourceConfiguration(ctx, &vpclattice.CreateResourceConfigurationInput{
		Name: aws.String("rc-ep"), Type: latticetypes.ResourceConfigurationTypeArn,
		ResourceConfigurationDefinition: &latticetypes.ResourceConfigurationDefinitionMemberArnResource{
			Value: latticetypes.ArnResource{Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb")},
		},
	})
	require.NoError(t, err)

	snEndpoint, err := ec2c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{
		VpcId: aws.String(vpcID), VpcEndpointType: ec2types.VpcEndpointTypeServiceNetwork, ServiceNetworkArn: sn.Arn,
	})
	require.NoError(t, err)

	rcEndpoint, err := ec2c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{
		VpcId: aws.String(vpcID), VpcEndpointType: ec2types.VpcEndpointTypeResource, ResourceConfigurationArn: rc.Arn,
	})
	require.NoError(t, err)

	snAssocs, err := lattice.ListServiceNetworkVpcEndpointAssociations(
		ctx, &vpclattice.ListServiceNetworkVpcEndpointAssociationsInput{ServiceNetworkIdentifier: sn.Id},
	)
	require.NoError(t, err)
	require.Len(t, snAssocs.Items, 1)
	assert.Equal(t, aws.ToString(snEndpoint.VpcEndpoint.VpcEndpointId), aws.ToString(snAssocs.Items[0].VpcEndpointId))
	assert.Equal(t, vpcID, aws.ToString(snAssocs.Items[0].VpcId))

	reAssocs, err := lattice.ListResourceEndpointAssociations(
		ctx, &vpclattice.ListResourceEndpointAssociationsInput{ResourceConfigurationIdentifier: rc.Id},
	)
	require.NoError(t, err)
	require.Len(t, reAssocs.Items, 1)
	assert.Equal(t, aws.ToString(rcEndpoint.VpcEndpoint.VpcEndpointId), aws.ToString(reAssocs.Items[0].VpcEndpointId))

	_, err = lattice.DeleteResourceEndpointAssociation(ctx, &vpclattice.DeleteResourceEndpointAssociationInput{
		ResourceEndpointAssociationIdentifier: reAssocs.Items[0].Id,
	})
	require.NoError(t, err)

	reAssocs, err = lattice.ListResourceEndpointAssociations(
		ctx, &vpclattice.ListResourceEndpointAssociationsInput{ResourceConfigurationIdentifier: rc.Id},
	)
	require.NoError(t, err)
	assert.Empty(t, reAssocs.Items)
}
