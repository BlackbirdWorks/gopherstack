package app

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationProvisionsCloudHSMCluster(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	hsm := cloudhsmv2.NewFromConfig(fx.cfg)
	ec2Client := ec2.NewFromConfig(fx.cfg)

	vpc, err := ec2Client.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String("10.9.0.0/16")})
	require.NoError(t, err)

	subnet, err := ec2Client.CreateSubnet(t.Context(), &ec2.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.9.1.0/24"), AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Cluster": map[string]any{
				"Type": "AWS::CloudHSM::Cluster",
				"Properties": map[string]any{
					"HsmType": "hsm1.medium", "SubnetIds": []string{aws.ToString(subnet.Subnet.SubnetId)},
					"Tags": []any{map[string]any{"Key": "env", "Value": "cfn"}},
				},
			},
		},
		"Outputs": map[string]any{
			"Id": map[string]any{"Value": map[string]any{"Ref": "Cluster"}},
		},
	})
	require.NoError(t, err)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("hsm-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	stack := waitStack(t, cfn, "hsm-stack")
	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))
	require.Len(t, stack.Outputs, 1)

	clusterID := aws.ToString(stack.Outputs[0].OutputValue)

	got, err := hsm.DescribeClusters(t.Context(), &cloudhsmv2.DescribeClustersInput{
		Filters: map[string][]string{"clusterIds": {clusterID}},
	})
	require.NoError(t, err)
	require.Len(t, got.Clusters, 1)
	assert.Equal(t, aws.ToString(vpc.Vpc.VpcId), aws.ToString(got.Clusters[0].VpcId))

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("hsm-stack")})
	require.NoError(t, err)

	after, err := hsm.DescribeClusters(t.Context(), &cloudhsmv2.DescribeClustersInput{
		Filters: map[string][]string{"clusterIds": {clusterID}},
	})
	require.NoError(t, err)
	assert.Empty(t, after.Clusters)
}
