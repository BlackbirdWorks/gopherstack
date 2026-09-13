package elb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestRealClient_AttributesAndNetworkConfig drives elb's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_AttributesAndNetworkConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "availability zones and attributes and account limits", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBClient(t, elb.NewHandler(elb.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
				LoadBalancerName:  aws.String("s15-classic-lb"),
				AvailabilityZones: []string{"us-east-1a"},
				Listeners: []types.Listener{
					{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
				},
			})
			require.NoError(t, err)

			enableOut, err := client.EnableAvailabilityZonesForLoadBalancer(
				ctx, &elbsdk.EnableAvailabilityZonesForLoadBalancerInput{
					LoadBalancerName:  aws.String("s15-classic-lb"),
					AvailabilityZones: []string{"us-east-1b"},
				},
			)
			require.NoError(t, err)
			assert.Contains(t, enableOut.AvailabilityZones, "us-east-1a")
			assert.Contains(t, enableOut.AvailabilityZones, "us-east-1b")

			disableOut, err := client.DisableAvailabilityZonesForLoadBalancer(
				ctx, &elbsdk.DisableAvailabilityZonesForLoadBalancerInput{
					LoadBalancerName:  aws.String("s15-classic-lb"),
					AvailabilityZones: []string{"us-east-1b"},
				},
			)
			require.NoError(t, err)
			assert.Contains(t, disableOut.AvailabilityZones, "us-east-1a")
			assert.NotContains(t, disableOut.AvailabilityZones, "us-east-1b")

			_, err = client.ModifyLoadBalancerAttributes(ctx, &elbsdk.ModifyLoadBalancerAttributesInput{
				LoadBalancerName: aws.String("s15-classic-lb"),
				LoadBalancerAttributes: &types.LoadBalancerAttributes{
					CrossZoneLoadBalancing: &types.CrossZoneLoadBalancing{Enabled: true},
					ConnectionDraining:     &types.ConnectionDraining{Enabled: true, Timeout: aws.Int32(120)},
				},
			})
			require.NoError(t, err)

			attrOut, err := client.DescribeLoadBalancerAttributes(ctx, &elbsdk.DescribeLoadBalancerAttributesInput{
				LoadBalancerName: aws.String("s15-classic-lb"),
			})
			require.NoError(t, err)
			require.NotNil(t, attrOut.LoadBalancerAttributes.CrossZoneLoadBalancing)
			assert.True(t, attrOut.LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled)
			require.NotNil(t, attrOut.LoadBalancerAttributes.ConnectionDraining)
			assert.True(t, attrOut.LoadBalancerAttributes.ConnectionDraining.Enabled)
			assert.EqualValues(t, 120, aws.ToInt32(attrOut.LoadBalancerAttributes.ConnectionDraining.Timeout))

			limitsOut, err := client.DescribeAccountLimits(ctx, &elbsdk.DescribeAccountLimitsInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, limitsOut.Limits)
		}},
		{name: "vpc load balancer subnets and security groups", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBClient(t, elb.NewHandler(elb.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
				LoadBalancerName: aws.String("s15-vpc-lb"),
				Subnets:          []string{"subnet-0123456789abcdef0"},
				Listeners: []types.Listener{
					{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
				},
			})
			require.NoError(t, err)

			sgOut, err := client.ApplySecurityGroupsToLoadBalancer(ctx, &elbsdk.ApplySecurityGroupsToLoadBalancerInput{
				LoadBalancerName: aws.String("s15-vpc-lb"),
				SecurityGroups:   []string{"sg-0123456789abcdef0"},
			})
			require.NoError(t, err)
			assert.Contains(t, sgOut.SecurityGroups, "sg-0123456789abcdef0")

			_, err = client.AttachLoadBalancerToSubnets(ctx, &elbsdk.AttachLoadBalancerToSubnetsInput{
				LoadBalancerName: aws.String("s15-vpc-lb"),
				Subnets:          []string{"subnet-0abcdef0123456789"},
			})
			require.NoError(t, err)

			detachOut, err := client.DetachLoadBalancerFromSubnets(ctx, &elbsdk.DetachLoadBalancerFromSubnetsInput{
				LoadBalancerName: aws.String("s15-vpc-lb"),
				Subnets:          []string{"subnet-0abcdef0123456789"},
			})
			require.NoError(t, err)
			assert.Contains(t, detachOut.Subnets, "subnet-0123456789abcdef0")
			assert.NotContains(t, detachOut.Subnets, "subnet-0abcdef0123456789")
		}},
		{name: "describe load balancer policies", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBClient(t, elb.NewHandler(elb.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
				LoadBalancerName:  aws.String("s15-policy-lb"),
				AvailabilityZones: []string{"us-east-1a"},
				Listeners: []types.Listener{
					{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateLoadBalancerPolicy(ctx, &elbsdk.CreateLoadBalancerPolicyInput{
				LoadBalancerName: aws.String("s15-policy-lb"),
				PolicyName:       aws.String("s15-generic-pol"),
				PolicyTypeName:   aws.String("ProxyProtocolPolicyType"),
				PolicyAttributes: []types.PolicyAttribute{
					{AttributeName: aws.String("ProxyProtocol"), AttributeValue: aws.String("true")},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeLoadBalancerPolicies(ctx, &elbsdk.DescribeLoadBalancerPoliciesInput{
				LoadBalancerName: aws.String("s15-policy-lb"),
				PolicyNames:      []string{"s15-generic-pol"},
			})
			require.NoError(t, err)
			require.Len(t, descOut.PolicyDescriptions, 1)
			assert.Equal(t, "s15-generic-pol", aws.ToString(descOut.PolicyDescriptions[0].PolicyName))
			assert.Equal(t, "ProxyProtocolPolicyType", aws.ToString(descOut.PolicyDescriptions[0].PolicyTypeName))
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
