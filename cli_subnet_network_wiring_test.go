package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/medialive"
	medialivetypes "github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type subnetFixture struct {
	fx       *sfnFixture
	vpcID    string
	v4Subnet string
	dualNet  string
}

func newSubnetFixture(t *testing.T) subnetFixture {
	t.Helper()

	fx := newSFNFixture(t)
	c := ec2.NewFromConfig(fx.cfg)

	vpc, err := c.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String("10.9.0.0/16")})
	require.NoError(t, err)

	vpcID := aws.ToString(vpc.Vpc.VpcId)
	mk := func(cidr, az string) string {
		out, serr := c.CreateSubnet(t.Context(), &ec2.CreateSubnetInput{
			VpcId: aws.String(vpcID), CidrBlock: aws.String(cidr), AvailabilityZone: aws.String(az),
		})
		require.NoError(t, serr)

		return aws.ToString(out.Subnet.SubnetId)
	}

	v4 := mk("10.9.1.0/24", "us-east-1a")
	dual := mk("10.9.2.0/24", "us-east-1b")

	_, err = c.AssociateSubnetCidrBlock(t.Context(), &ec2.AssociateSubnetCidrBlockInput{
		SubnetId: aws.String(dual), Ipv6CidrBlock: aws.String("2600:1f18:1::/64"),
	})
	require.NoError(t, err)

	return subnetFixture{fx: fx, vpcID: vpcID, v4Subnet: v4, dualNet: dual}
}

func TestSubnetNetworkWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, s subnetFixture)
		name string
	}{
		{name: "docdb", run: func(t *testing.T, s subnetFixture) {
			t.Helper()

			c := docdb.NewFromConfig(s.fx.cfg)
			out, err := c.CreateDBSubnetGroup(t.Context(), &docdb.CreateDBSubnetGroupInput{
				DBSubnetGroupName: aws.String("dd-sg"), DBSubnetGroupDescription: aws.String("d"),
				SubnetIds: []string{s.v4Subnet, s.dualNet},
			})
			require.NoError(t, err)
			assert.Equal(t, s.vpcID, aws.ToString(out.DBSubnetGroup.VpcId))
			assert.Equal(t, []string{"IPV4"}, out.DBSubnetGroup.SupportedNetworkTypes)

			_, err = c.CreateDBCluster(t.Context(), &docdb.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("dd-c"), Engine: aws.String("docdb"), MasterUsername: aws.String("a"),
				MasterUserPassword: aws.String("password123"), DBSubnetGroupName: aws.String("dd-sg"),
				NetworkType: aws.String("DUAL"),
			})
			require.ErrorContains(t, err, "NetworkTypeNotSupported")

			snapCluster, err := c.CreateDBCluster(t.Context(), &docdb.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("dd-c2"), Engine: aws.String("docdb"), MasterUsername: aws.String("a"),
				MasterUserPassword: aws.String("password123"), DBSubnetGroupName: aws.String("dd-sg"),
			})
			require.NoError(t, err)

			snap, err := c.CreateDBClusterSnapshot(t.Context(), &docdb.CreateDBClusterSnapshotInput{
				DBClusterIdentifier:         snapCluster.DBCluster.DBClusterIdentifier,
				DBClusterSnapshotIdentifier: aws.String("dd-snap"),
			})
			require.NoError(t, err)
			assert.Equal(t, s.vpcID, aws.ToString(snap.DBClusterSnapshot.VpcId))
		}},
		{name: "neptune", run: func(t *testing.T, s subnetFixture) {
			t.Helper()

			c := neptune.NewFromConfig(s.fx.cfg)
			out, err := c.CreateDBSubnetGroup(t.Context(), &neptune.CreateDBSubnetGroupInput{
				DBSubnetGroupName: aws.String("np-sg"), DBSubnetGroupDescription: aws.String("d"),
				SubnetIds: []string{s.v4Subnet, s.dualNet},
			})
			require.NoError(t, err)
			assert.Equal(t, s.vpcID, aws.ToString(out.DBSubnetGroup.VpcId))
			assert.Equal(t, []string{"IPV4"}, out.DBSubnetGroup.SupportedNetworkTypes)

			_, err = c.CreateDBCluster(t.Context(), &neptune.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("np-c"), Engine: aws.String("neptune"),
				DBSubnetGroupName: aws.String("np-sg"), NetworkType: aws.String("DUAL"),
			})
			require.ErrorContains(t, err, "NetworkTypeNotSupported")
		}},
		{name: "medialive", run: func(t *testing.T, s subnetFixture) {
			t.Helper()

			c := medialive.NewFromConfig(s.fx.cfg)
			out, err := c.CreateChannel(t.Context(), &medialive.CreateChannelInput{
				Name: aws.String("vpc-ch"), ChannelClass: medialivetypes.ChannelClassStandard,
				Vpc: &medialivetypes.VpcOutputSettings{SubnetIds: []string{s.v4Subnet, s.dualNet}},
			})
			require.NoError(t, err)
			require.NotNil(t, out.Channel.Vpc)
			assert.Equal(t, []string{"us-east-1a", "us-east-1b"}, out.Channel.Vpc.AvailabilityZones)
			require.Len(t, out.Channel.Vpc.NetworkInterfaceIds, 2)

			enis, err := ec2.NewFromConfig(s.fx.cfg).DescribeNetworkInterfaces(
				t.Context(),
				&ec2.DescribeNetworkInterfacesInput{NetworkInterfaceIds: out.Channel.Vpc.NetworkInterfaceIds},
			)
			require.NoError(t, err)
			assert.Len(t, enis.NetworkInterfaces, 2)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newSubnetFixture(t))
		})
	}
}
