package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ELBClassic_Lifecycle exercises the Classic ELB (v1) API end-to-end
// via the AWS SDK v2: create LB with a listener, register instances, configure a
// health check, set attributes, manage tags, and delete. This catches form/XML
// parity regressions in the elasticloadbalancing v1 handler.
func TestIntegration_ELBClassic_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createELBClient(t)
	ctx := t.Context()

	const (
		lbName = "it-elb-classic"
		instA  = "i-0a0a0a0a0a0a0a0a0"
		instB  = "i-0b0b0b0b0b0b0b0b0"
	)

	// CreateLoadBalancer with a single HTTP listener.
	createOut, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName: aws.String(lbName),
		Listeners: []elbtypes.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
		AvailabilityZones: []string{"us-east-1a"},
		Tags: []elbtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.DNSName))

	t.Cleanup(func() {
		_, _ = client.DeleteLoadBalancer(ctx, &elbsdk.DeleteLoadBalancerInput{
			LoadBalancerName: aws.String(lbName),
		})
	})

	// DescribeLoadBalancers should return the newly created LB.
	descOut, err := client.DescribeLoadBalancers(ctx, &elbsdk.DescribeLoadBalancersInput{
		LoadBalancerNames: []string{lbName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.LoadBalancerDescriptions, 1)
	assert.Equal(t, lbName, aws.ToString(descOut.LoadBalancerDescriptions[0].LoadBalancerName))
	require.Len(t, descOut.LoadBalancerDescriptions[0].ListenerDescriptions, 1)
	assert.EqualValues(t, 80, descOut.LoadBalancerDescriptions[0].ListenerDescriptions[0].Listener.LoadBalancerPort)

	// RegisterInstancesWithLoadBalancer.
	_, err = client.RegisterInstancesWithLoadBalancer(ctx, &elbsdk.RegisterInstancesWithLoadBalancerInput{
		LoadBalancerName: aws.String(lbName),
		Instances: []elbtypes.Instance{
			{InstanceId: aws.String(instA)},
			{InstanceId: aws.String(instB)},
		},
	})
	require.NoError(t, err)

	// DescribeInstanceHealth should report both instances.
	healthOut, err := client.DescribeInstanceHealth(ctx, &elbsdk.DescribeInstanceHealthInput{
		LoadBalancerName: aws.String(lbName),
	})
	require.NoError(t, err)
	assert.Len(t, healthOut.InstanceStates, 2)

	// DeregisterInstancesFromLoadBalancer.
	_, err = client.DeregisterInstancesFromLoadBalancer(ctx, &elbsdk.DeregisterInstancesFromLoadBalancerInput{
		LoadBalancerName: aws.String(lbName),
		Instances:        []elbtypes.Instance{{InstanceId: aws.String(instB)}},
	})
	require.NoError(t, err)

	healthOut2, err := client.DescribeInstanceHealth(ctx, &elbsdk.DescribeInstanceHealthInput{
		LoadBalancerName: aws.String(lbName),
	})
	require.NoError(t, err)
	assert.Len(t, healthOut2.InstanceStates, 1)

	// ConfigureHealthCheck.
	hcOut, err := client.ConfigureHealthCheck(ctx, &elbsdk.ConfigureHealthCheckInput{
		LoadBalancerName: aws.String(lbName),
		HealthCheck: &elbtypes.HealthCheck{
			Target:             aws.String("HTTP:8080/health"),
			Interval:           aws.Int32(30),
			Timeout:            aws.Int32(5),
			HealthyThreshold:   aws.Int32(2),
			UnhealthyThreshold: aws.Int32(3),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, hcOut.HealthCheck)
	assert.Equal(t, "HTTP:8080/health", aws.ToString(hcOut.HealthCheck.Target))

	// AddTags / DescribeTags / RemoveTags.
	_, err = client.AddTags(ctx, &elbsdk.AddTagsInput{
		LoadBalancerNames: []string{lbName},
		Tags: []elbtypes.Tag{
			{Key: aws.String("owner"), Value: aws.String("integration")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.DescribeTags(ctx, &elbsdk.DescribeTagsInput{
		LoadBalancerNames: []string{lbName},
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagDescriptions, 1)

	got := make(map[string]string)
	for _, kv := range tagsOut.TagDescriptions[0].Tags {
		got[aws.ToString(kv.Key)] = aws.ToString(kv.Value)
	}
	assert.Equal(t, "integration", got["owner"])
	assert.Equal(t, "test", got["env"])

	_, err = client.RemoveTags(ctx, &elbsdk.RemoveTagsInput{
		LoadBalancerNames: []string{lbName},
		Tags:              []elbtypes.TagKeyOnly{{Key: aws.String("owner")}},
	})
	require.NoError(t, err)

	// CreateLoadBalancerListeners (add a second listener) then DeleteLoadBalancerListeners.
	_, err = client.CreateLoadBalancerListeners(ctx, &elbsdk.CreateLoadBalancerListenersInput{
		LoadBalancerName: aws.String(lbName),
		Listeners: []elbtypes.Listener{
			{Protocol: aws.String("TCP"), LoadBalancerPort: 8081, InstancePort: aws.Int32(8081)},
		},
	})
	require.NoError(t, err)

	desc2, err := client.DescribeLoadBalancers(ctx, &elbsdk.DescribeLoadBalancersInput{
		LoadBalancerNames: []string{lbName},
	})
	require.NoError(t, err)
	require.Len(t, desc2.LoadBalancerDescriptions, 1)
	assert.Len(t, desc2.LoadBalancerDescriptions[0].ListenerDescriptions, 2)

	_, err = client.DeleteLoadBalancerListeners(ctx, &elbsdk.DeleteLoadBalancerListenersInput{
		LoadBalancerName:  aws.String(lbName),
		LoadBalancerPorts: []int32{8081},
	})
	require.NoError(t, err)
}
