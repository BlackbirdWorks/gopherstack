package globalaccelerator_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/globalaccelerator"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	std := newStandardAccelerator(t, f)
	listener := newListener(t, f, std)

	group, err := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn:         listener,
		EndpointGroupRegion: aws.String("us-west-2"),
		EndpointConfigurations: []gatypes.EndpointConfiguration{
			{EndpointId: aws.String("i-0abc"), Weight: aws.Int32(7)},
		},
		PortOverrides: []gatypes.PortOverride{{ListenerPort: aws.Int32(8080), EndpointPort: aws.Int32(80)}},
	})
	require.NoError(t, err)

	_, err = f.client.TagResource(
		ctx,
		&gasdk.TagResourceInput{ResourceArn: std, Tags: []gatypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}}},
	)
	require.NoError(t, err)

	custom := newCustomStack(t, f, portRange(10000, 20000))
	_, err = f.client.AddCustomRoutingEndpoints(ctx, &gasdk.AddCustomRoutingEndpointsInput{
		EndpointGroupArn:       custom.groupARN,
		EndpointConfigurations: []gatypes.CustomRoutingEndpointConfiguration{{EndpointId: aws.String("subnet-aaa")}},
	})
	require.NoError(t, err)
	_, err = f.client.AllowCustomRoutingTraffic(ctx, &gasdk.AllowCustomRoutingTrafficInput{
		EndpointGroupArn: custom.groupARN, EndpointId: aws.String("subnet-aaa"),
		DestinationAddresses: []string{"10.0.0.9"}, DestinationPorts: []int32{81},
	})
	require.NoError(t, err)

	att, err := f.client.CreateCrossAccountAttachment(ctx, &gasdk.CreateCrossAccountAttachmentInput{
		Name: aws.String("att"), Principals: []string{awsmeta.DefaultAccount},
	})
	require.NoError(t, err)

	_, err = f.client.ProvisionByoipCidr(ctx, &gasdk.ProvisionByoipCidrInput{
		Cidr: aws.String("203.0.113.0/24"),
		CidrAuthorizationContext: &gatypes.CidrAuthorizationContext{
			Message:   aws.String("m"),
			Signature: aws.String("s"),
		},
	})
	require.NoError(t, err)

	snapshot := f.backend.Snapshot(ctx)
	require.NotEmpty(t, snapshot)

	restored := globalaccelerator.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(ctx, snapshot))
	assert.Equal(t, testAccount, restored.AccountID())
	assert.Equal(t, "us-west-2", restored.Region())
	assert.Equal(t, f.backend.AcceleratorCount(), restored.AcceleratorCount())

	r := newFixtureFromBackend(t, restored, f.clock)

	std2, err := r.client.DescribeAccelerator(ctx, &gasdk.DescribeAcceleratorInput{AcceleratorArn: std})
	require.NoError(t, err)
	assert.Equal(t, "routing", aws.ToString(std2.Accelerator.Name))

	tags, err := r.client.ListTagsForResource(ctx, &gasdk.ListTagsForResourceInput{ResourceArn: std})
	require.NoError(t, err)
	require.Len(t, tags.Tags, 1)

	eg, err := r.client.DescribeEndpointGroup(
		ctx,
		&gasdk.DescribeEndpointGroupInput{EndpointGroupArn: group.EndpointGroup.EndpointGroupArn},
	)
	require.NoError(t, err)
	require.Len(t, eg.EndpointGroup.EndpointDescriptions, 1)
	assert.EqualValues(t, 7, aws.ToInt32(eg.EndpointGroup.EndpointDescriptions[0].Weight))
	require.Len(t, eg.EndpointGroup.PortOverrides, 1)

	maps, err := r.client.ListCustomRoutingPortMappingsByDestination(
		ctx,
		&gasdk.ListCustomRoutingPortMappingsByDestinationInput{
			EndpointId: aws.String("subnet-aaa"), DestinationAddress: aws.String("10.0.0.9"),
		},
	)
	require.NoError(t, err)
	require.Len(t, maps.DestinationPortMappings, 2)
	assert.Equal(
		t,
		gatypes.CustomRoutingDestinationTrafficStateDeny,
		maps.DestinationPortMappings[0].DestinationTrafficState,
	)
	assert.Equal(
		t,
		gatypes.CustomRoutingDestinationTrafficStateAllow,
		maps.DestinationPortMappings[1].DestinationTrafficState,
	)

	_, err = r.client.DescribeCrossAccountAttachment(ctx, &gasdk.DescribeCrossAccountAttachmentInput{
		AttachmentArn: att.CrossAccountAttachment.AttachmentArn,
	})
	require.NoError(t, err)

	byoip, err := r.client.ListByoipCidrs(ctx, &gasdk.ListByoipCidrsInput{})
	require.NoError(t, err)
	require.Len(t, byoip.ByoipCidrs, 1)
	assert.Equal(t, gatypes.ByoipCidrStatePendingProvisioning, byoip.ByoipCidrs[0].State)

	r.clock.advance(time.Minute)

	byoip, err = r.client.ListByoipCidrs(ctx, &gasdk.ListByoipCidrsInput{})
	require.NoError(t, err)
	assert.Equal(t, gatypes.ByoipCidrStateReady, byoip.ByoipCidrs[0].State)

	next, err := r.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{Name: aws.String("after")})
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"75.2.0.5", "75.2.0.6"},
		next.Accelerator.IpSets[0].IpAddresses,
		"restored addresses stay allocated",
	)

	list, err := r.client.ListAccelerators(ctx, &gasdk.ListAcceleratorsInput{})
	require.NoError(t, err)
	assert.Equal(
		t,
		"after",
		aws.ToString(list.Accelerators[len(list.Accelerators)-1].Name),
		"creation order survives restore",
	)
}

func TestPersistence_IncompatibleSnapshotIsDiscarded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		data string
	}{
		{"older_version", `{"version":0,"tables":{}}`},
		{"newer_version", `{"version":99,"tables":{}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			_ = newStandardAccelerator(t, f)

			require.NoError(t, f.backend.Restore(t.Context(), []byte(tc.data)))
			assert.Zero(t, f.backend.AcceleratorCount())
		})
	}
}

func TestPersistence_ResetClearsEverything(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	_ = newStandardAccelerator(t, f)

	h := globalaccelerator.NewHandler(f.backend)
	h.Reset()

	assert.Zero(t, f.backend.AcceleratorCount())
}
