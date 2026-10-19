package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createGlobalAcceleratorClient(t *testing.T) *gasdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return gasdk.NewFromConfig(cfg, func(o *gasdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_GlobalAccelerator_Lifecycle drives accelerator, listener, endpoint group and tags
// through the real SDK client against the container.
func TestIntegration_GlobalAccelerator_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createGlobalAcceleratorClient(t)
	ctx := t.Context()

	created, err := client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
		Name:          aws.String("it-accelerator"),
		IpAddressType: gatypes.IpAddressTypeDualStack,
		Tags:          []gatypes.Tag{{Key: aws.String("env"), Value: aws.String("it")}},
	})
	require.NoError(t, err)

	arn := created.Accelerator.AcceleratorArn
	assert.Len(t, created.Accelerator.IpSets, 2)

	listener, err := client.CreateListener(ctx, &gasdk.CreateListenerInput{
		AcceleratorArn: arn,
		Protocol:       gatypes.ProtocolTcp,
		PortRanges:     []gatypes.PortRange{{FromPort: aws.Int32(80), ToPort: aws.Int32(80)}},
	})
	require.NoError(t, err)

	group, err := client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn:         listener.Listener.ListenerArn,
		EndpointGroupRegion: aws.String("us-west-2"),
		EndpointConfigurations: []gatypes.EndpointConfiguration{
			{EndpointId: aws.String("i-0123456789abcdef0"), Weight: aws.Int32(100)},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteEndpointGroup(cleanupCtx, &gasdk.DeleteEndpointGroupInput{
			EndpointGroupArn: group.EndpointGroup.EndpointGroupArn,
		})
		_, _ = client.DeleteListener(cleanupCtx, &gasdk.DeleteListenerInput{ListenerArn: listener.Listener.ListenerArn})
		_, _ = client.UpdateAccelerator(
			cleanupCtx,
			&gasdk.UpdateAcceleratorInput{AcceleratorArn: arn, Enabled: aws.Bool(false)},
		)
		_, _ = client.DeleteAccelerator(cleanupCtx, &gasdk.DeleteAcceleratorInput{AcceleratorArn: arn})
	})

	require.Eventually(t, func() bool {
		out, derr := client.DescribeAccelerator(ctx, &gasdk.DescribeAcceleratorInput{AcceleratorArn: arn})

		return derr == nil && out.Accelerator.Status == gatypes.AcceleratorStatusDeployed
	}, 15*time.Second, 100*time.Millisecond)

	tags, err := client.ListTagsForResource(ctx, &gasdk.ListTagsForResourceInput{ResourceArn: arn})
	require.NoError(t, err)
	require.Len(t, tags.Tags, 1)

	_, err = client.DeleteAccelerator(ctx, &gasdk.DeleteAcceleratorInput{AcceleratorArn: arn})
	require.Error(t, err)
	assert.True(t, hasAPIErrorCode(err, "AcceleratorNotDisabledException"))

	_, err = client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: listener.Listener.ListenerArn, EndpointGroupRegion: aws.String("us-west-2"),
	})
	require.Error(t, err)
	assert.True(t, hasAPIErrorCode(err, "EndpointGroupAlreadyExistsException"))
}
