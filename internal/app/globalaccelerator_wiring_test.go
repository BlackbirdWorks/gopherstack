package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitStack(t *testing.T, cfn *cloudformation.Client, name string) cfntypes.Stack {
	t.Helper()

	var stack cfntypes.Stack

	require.Eventually(t, func() bool {
		out, err := cfn.DescribeStacks(t.Context(), &cloudformation.DescribeStacksInput{StackName: aws.String(name)})
		if err != nil || len(out.Stacks) == 0 {
			return false
		}

		stack = out.Stacks[0]

		return stack.StackStatus != cfntypes.StackStatusCreateInProgress &&
			stack.StackStatus != cfntypes.StackStatusUpdateInProgress
	}, 30*time.Second, 50*time.Millisecond)

	return stack
}

func TestCloudFormationProvisionsGlobalAccelerator(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Acc": map[string]any{
				"Type": "AWS::GlobalAccelerator::Accelerator",
				"Properties": map[string]any{
					"Name": "cfn-acc", "IpAddressType": "DUAL_STACK",
					"Tags": []any{map[string]any{"Key": "env", "Value": "cfn"}},
				},
			},
			"Lis": map[string]any{
				"Type": "AWS::GlobalAccelerator::Listener",
				"Properties": map[string]any{
					"AcceleratorArn": map[string]any{"Ref": "Acc"}, "Protocol": "TCP",
					"PortRanges": []any{map[string]any{"FromPort": 80, "ToPort": 80}},
				},
			},
			"Grp": map[string]any{
				"Type": "AWS::GlobalAccelerator::EndpointGroup",
				"Properties": map[string]any{
					"ListenerArn":         map[string]any{"Ref": "Lis"},
					"EndpointGroupRegion": "us-east-1",
				},
			},
			"Att": map[string]any{
				"Type":       "AWS::GlobalAccelerator::CrossAccountAttachment",
				"Properties": map[string]any{"Name": "cfn-att", "Principals": []any{"111122223333"}},
			},
		},
		"Outputs": map[string]any{
			"AccArn": map[string]any{"Value": map[string]any{"Ref": "Acc"}},
			"Dns":    map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Acc", "DnsName"}}},
			"DualDns": map[string]any{
				"Value": map[string]any{"Fn::GetAtt": []string{"Acc", "DualStackDnsName"}},
			},
			"LisArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Lis", "ListenerArn"}}},
			"GrpArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Grp", "EndpointGroupArn"}}},
			"AttArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Att", "AttachmentArn"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	ga := globalaccelerator.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("ga-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	stack := waitStack(t, cfn, "ga-stack")
	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))

	outputs := map[string]string{}
	for _, o := range stack.Outputs {
		outputs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	assert.Contains(t, outputs["AccArn"], ":accelerator/")
	assert.Contains(t, outputs["Dns"], ".awsglobalaccelerator.com")
	assert.Contains(t, outputs["DualDns"], ".dualstack.awsglobalaccelerator.com")
	assert.Contains(t, outputs["LisArn"], outputs["AccArn"]+"/listener/")
	assert.Contains(t, outputs["GrpArn"], outputs["LisArn"]+"/endpoint-group/")
	assert.Contains(t, outputs["AttArn"], ":attachment/")

	desc, err := ga.DescribeAccelerator(t.Context(), &globalaccelerator.DescribeAcceleratorInput{
		AcceleratorArn: aws.String(outputs["AccArn"]),
	})
	require.NoError(t, err)
	assert.Equal(t, "cfn-acc", aws.ToString(desc.Accelerator.Name))

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("ga-stack")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, lerr := ga.ListAccelerators(t.Context(), &globalaccelerator.ListAcceleratorsInput{})

		return lerr == nil && len(out.Accelerators) == 0
	}, 30*time.Second, 50*time.Millisecond)
}

func TestGlobalAcceleratorTaggingAPI(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ga := globalaccelerator.NewFromConfig(fx.cfg)
	rgt := resourcegroupstaggingapi.NewFromConfig(fx.cfg)

	acc, err := ga.CreateAccelerator(t.Context(), &globalaccelerator.CreateAcceleratorInput{
		Name: aws.String("tagged"),
		Tags: []gatypes.Tag{{Key: aws.String("team"), Value: aws.String("edge")}},
	})
	require.NoError(t, err)

	arn := aws.ToString(acc.Accelerator.AcceleratorArn)

	out, err := rgt.GetResources(t.Context(), &resourcegroupstaggingapi.GetResourcesInput{
		ResourceTypeFilters: []string{"globalaccelerator:accelerator"},
	})
	require.NoError(t, err)
	require.Len(t, out.ResourceTagMappingList, 1)
	assert.Equal(t, arn, aws.ToString(out.ResourceTagMappingList[0].ResourceARN))

	_, err = rgt.TagResources(t.Context(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{arn}, Tags: map[string]string{"extra": "1"},
	})
	require.NoError(t, err)

	tags, err := ga.ListTagsForResource(
		t.Context(), &globalaccelerator.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
	)
	require.NoError(t, err)
	assert.Len(t, tags.Tags, 2)
}

func TestGlobalAcceleratorEndpointChecks(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ga := globalaccelerator.NewFromConfig(fx.cfg)
	ec2c := ec2.NewFromConfig(fx.cfg)
	elb := elasticloadbalancingv2.NewFromConfig(fx.cfg)

	run, err := ec2c.RunInstances(t.Context(), &ec2.RunInstancesInput{
		ImageId: aws.String("ami-12345678"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)

	eip, err := ec2c.AllocateAddress(t.Context(), &ec2.AllocateAddressInput{Domain: ec2types.DomainTypeVpc})
	require.NoError(t, err)

	lb, err := elb.CreateLoadBalancer(t.Context(), &elasticloadbalancingv2.CreateLoadBalancerInput{
		Name: aws.String("ga-nlb"), Type: elbtypes.LoadBalancerTypeEnumNetwork,
	})
	require.NoError(t, err)

	acc, err := ga.CreateAccelerator(t.Context(), &globalaccelerator.CreateAcceleratorInput{Name: aws.String("chk")})
	require.NoError(t, err)

	lis, err := ga.CreateListener(t.Context(), &globalaccelerator.CreateListenerInput{
		AcceleratorArn: acc.Accelerator.AcceleratorArn, Protocol: gatypes.ProtocolTcp,
		PortRanges: []gatypes.PortRange{{FromPort: aws.Int32(80), ToPort: aws.Int32(80)}},
	})
	require.NoError(t, err)

	grp, err := ga.CreateEndpointGroup(t.Context(), &globalaccelerator.CreateEndpointGroupInput{
		ListenerArn: lis.Listener.ListenerArn, EndpointGroupRegion: aws.String("us-east-1"),
	})
	require.NoError(t, err)

	good := []string{
		aws.ToString(run.Instances[0].InstanceId),
		aws.ToString(eip.AllocationId),
		aws.ToString(lb.LoadBalancers[0].LoadBalancerArn),
	}
	bad := []string{
		"i-0123456789abcdef0", "eipalloc-0123456789abcdef0",
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nope/0123456789abcdef",
	}

	for _, id := range bad {
		_, err = ga.AddEndpoints(t.Context(), &globalaccelerator.AddEndpointsInput{
			EndpointGroupArn:       grp.EndpointGroup.EndpointGroupArn,
			EndpointConfigurations: []gatypes.EndpointConfiguration{{EndpointId: aws.String(id)}},
		})
		require.ErrorContains(t, err, "InvalidArgumentException", id)
	}

	for _, id := range good {
		_, err = ga.AddEndpoints(t.Context(), &globalaccelerator.AddEndpointsInput{
			EndpointGroupArn:       grp.EndpointGroup.EndpointGroupArn,
			EndpointConfigurations: []gatypes.EndpointConfiguration{{EndpointId: aws.String(id)}},
		})
		require.NoError(t, err, id)
	}
}
