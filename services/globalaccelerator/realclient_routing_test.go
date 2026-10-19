package globalaccelerator_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStandardAccelerator(t *testing.T, f *fixture) *string {
	t.Helper()

	out, err := f.client.CreateAccelerator(t.Context(), &gasdk.CreateAcceleratorInput{Name: aws.String("routing")})
	require.NoError(t, err)

	return out.Accelerator.AcceleratorArn
}

func portRange(from, to int32) gatypes.PortRange {
	return gatypes.PortRange{FromPort: aws.Int32(from), ToPort: aws.Int32(to)}
}

func TestRealClient_ListenersAndEndpointGroups(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testListenerLifecycle, "listener_lifecycle"},
		{testListenerPortConflicts, "listener_port_conflicts"},
		{testEndpointGroupLifecycle, "endpoint_group_lifecycle"},
		{testEndpointGroupValidation, "endpoint_group_validation"},
		{testEndpointMembership, "endpoint_membership"},
		{testDeleteGuards, "delete_guards"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testListenerLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	accARN := newStandardAccelerator(t, f)

	created, err := f.client.CreateListener(ctx, &gasdk.CreateListenerInput{
		AcceleratorArn: accARN, Protocol: gatypes.ProtocolTcp,
		PortRanges: []gatypes.PortRange{portRange(80, 80), portRange(443, 450)},
	})
	require.NoError(t, err)

	l := created.Listener
	assert.Regexp(t, `/listener/[0-9a-f]{8}$`, aws.ToString(l.ListenerArn))
	assert.Equal(t, gatypes.ClientAffinityNone, l.ClientAffinity)
	assert.Equal(t, gatypes.ProtocolTcp, l.Protocol)
	assert.Len(t, l.PortRanges, 2)

	upd, err := f.client.UpdateListener(ctx, &gasdk.UpdateListenerInput{
		ListenerArn: l.ListenerArn, ClientAffinity: gatypes.ClientAffinitySourceIp, Protocol: gatypes.ProtocolUdp,
		PortRanges: []gatypes.PortRange{portRange(53, 53)},
	})
	require.NoError(t, err)
	assert.Equal(t, gatypes.ClientAffinitySourceIp, upd.Listener.ClientAffinity)
	assert.Equal(t, gatypes.ProtocolUdp, upd.Listener.Protocol)
	require.Len(t, upd.Listener.PortRanges, 1)

	desc, err := f.client.DescribeListener(ctx, &gasdk.DescribeListenerInput{ListenerArn: l.ListenerArn})
	require.NoError(t, err)
	assert.EqualValues(t, 53, aws.ToInt32(desc.Listener.PortRanges[0].FromPort))

	list, err := f.client.ListListeners(ctx, &gasdk.ListListenersInput{AcceleratorArn: accARN})
	require.NoError(t, err)
	require.Len(t, list.Listeners, 1)

	_, err = f.client.DeleteListener(ctx, &gasdk.DeleteListenerInput{ListenerArn: l.ListenerArn})
	require.NoError(t, err)

	_, err = f.client.DescribeListener(ctx, &gasdk.DescribeListenerInput{ListenerArn: l.ListenerArn})
	assert.Equal(t, "ListenerNotFoundException", apiCode(err))

	_, err = f.client.ListListeners(
		ctx,
		&gasdk.ListListenersInput{AcceleratorArn: aws.String("arn:aws:globalaccelerator::123456789012:accelerator/x")},
	)
	assert.Equal(t, "AcceleratorNotFoundException", apiCode(err))
}

func testListenerPortConflicts(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	accARN := newStandardAccelerator(t, f)

	_, err := f.client.CreateListener(ctx, &gasdk.CreateListenerInput{
		AcceleratorArn: accARN, Protocol: gatypes.ProtocolTcp, PortRanges: []gatypes.PortRange{portRange(100, 200)},
	})
	require.NoError(t, err)

	cases := []struct {
		name     string
		code     string
		protocol gatypes.Protocol
		ranges   []gatypes.PortRange
	}{
		{
			"overlap_same_protocol",
			"InvalidPortRangeException",
			gatypes.ProtocolTcp,
			[]gatypes.PortRange{portRange(150, 250)},
		},
		{"overlap_other_protocol_ok", "", gatypes.ProtocolUdp, []gatypes.PortRange{portRange(150, 250)}},
		{"inverted_range", "InvalidPortRangeException", gatypes.ProtocolTcp, []gatypes.PortRange{portRange(300, 299)}},
		{
			"self_overlap",
			"InvalidPortRangeException",
			gatypes.ProtocolTcp,
			[]gatypes.PortRange{portRange(400, 410), portRange(405, 420)},
		},
		{"port_zero", "InvalidPortRangeException", gatypes.ProtocolTcp, []gatypes.PortRange{portRange(0, 10)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := f.client.CreateListener(ctx, &gasdk.CreateListenerInput{
				AcceleratorArn: accARN, Protocol: tc.protocol, PortRanges: tc.ranges,
			})
			assert.Equal(t, tc.code, apiCode(cerr))
		})
	}
}

func newListener(t *testing.T, f *fixture, accARN *string) *string {
	t.Helper()

	out, err := f.client.CreateListener(t.Context(), &gasdk.CreateListenerInput{
		AcceleratorArn: accARN, Protocol: gatypes.ProtocolTcp, PortRanges: []gatypes.PortRange{portRange(8080, 8090)},
	})
	require.NoError(t, err)

	return out.Listener.ListenerArn
}

func testEndpointGroupLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	listenerARN := newListener(t, f, newStandardAccelerator(t, f))
	alb := "arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/web/50dc6c495c0c9188"

	created, err := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: listenerARN, EndpointGroupRegion: aws.String("us-west-2"),
		EndpointConfigurations: []gatypes.EndpointConfiguration{{EndpointId: aws.String(alb), Weight: aws.Int32(50)}},
		PortOverrides:          []gatypes.PortOverride{{ListenerPort: aws.Int32(8080), EndpointPort: aws.Int32(80)}},
	})
	require.NoError(t, err)

	g := created.EndpointGroup
	assert.Regexp(t, `/endpoint-group/[0-9a-f]{12}$`, aws.ToString(g.EndpointGroupArn))
	assert.Equal(t, "us-west-2", aws.ToString(g.EndpointGroupRegion))
	assert.EqualValues(t, 30, aws.ToInt32(g.HealthCheckIntervalSeconds))
	assert.EqualValues(t, 3, aws.ToInt32(g.ThresholdCount))
	assert.EqualValues(t, 8080, aws.ToInt32(g.HealthCheckPort))
	assert.Equal(t, gatypes.HealthCheckProtocolTcp, g.HealthCheckProtocol)
	assert.InDelta(t, 100, aws.ToFloat32(g.TrafficDialPercentage), 0.001)
	require.Len(t, g.EndpointDescriptions, 1)
	assert.Equal(t, gatypes.HealthStateInitial, g.EndpointDescriptions[0].HealthState)
	assert.True(t, aws.ToBool(g.EndpointDescriptions[0].ClientIPPreservationEnabled), "ALB default")
	assert.EqualValues(t, 50, aws.ToInt32(g.EndpointDescriptions[0].Weight))
	require.Len(t, g.PortOverrides, 1)

	f.clock.advance(time.Minute)

	got, err := f.client.DescribeEndpointGroup(
		ctx,
		&gasdk.DescribeEndpointGroupInput{EndpointGroupArn: g.EndpointGroupArn},
	)
	require.NoError(t, err)
	assert.Equal(t, gatypes.HealthStateHealthy, got.EndpointGroup.EndpointDescriptions[0].HealthState)

	upd, err := f.client.UpdateEndpointGroup(ctx, &gasdk.UpdateEndpointGroupInput{
		EndpointGroupArn: g.EndpointGroupArn, HealthCheckProtocol: gatypes.HealthCheckProtocolHttp,
		HealthCheckPath: aws.String("/ping"), HealthCheckIntervalSeconds: aws.Int32(10),
		ThresholdCount: aws.Int32(5), TrafficDialPercentage: aws.Float32(25),
	})
	require.NoError(t, err)
	assert.Equal(t, "/ping", aws.ToString(upd.EndpointGroup.HealthCheckPath))
	assert.EqualValues(t, 10, aws.ToInt32(upd.EndpointGroup.HealthCheckIntervalSeconds))
	assert.InDelta(t, 25, aws.ToFloat32(upd.EndpointGroup.TrafficDialPercentage), 0.001)
	require.Len(t, upd.EndpointGroup.EndpointDescriptions, 1, "unset endpoint list leaves members alone")
	assert.Equal(t, gatypes.HealthStateHealthy, upd.EndpointGroup.EndpointDescriptions[0].HealthState)

	cleared, err := f.client.UpdateEndpointGroup(ctx, &gasdk.UpdateEndpointGroupInput{
		EndpointGroupArn: g.EndpointGroupArn, EndpointConfigurations: []gatypes.EndpointConfiguration{},
	})
	require.NoError(t, err)
	assert.Empty(t, cleared.EndpointGroup.EndpointDescriptions)

	list, err := f.client.ListEndpointGroups(ctx, &gasdk.ListEndpointGroupsInput{ListenerArn: listenerARN})
	require.NoError(t, err)
	require.Len(t, list.EndpointGroups, 1)

	_, err = f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: listenerARN, EndpointGroupRegion: aws.String("us-west-2"),
	})
	assert.Equal(t, "EndpointGroupAlreadyExistsException", apiCode(err))

	other, err := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: listenerARN, EndpointGroupRegion: aws.String("eu-west-1"),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteEndpointGroup(
		ctx,
		&gasdk.DeleteEndpointGroupInput{EndpointGroupArn: other.EndpointGroup.EndpointGroupArn},
	)
	require.NoError(t, err)

	_, err = f.client.DescribeEndpointGroup(
		ctx,
		&gasdk.DescribeEndpointGroupInput{EndpointGroupArn: other.EndpointGroup.EndpointGroupArn},
	)
	assert.Equal(t, "EndpointGroupNotFoundException", apiCode(err))
}

func testEndpointGroupValidation(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	listenerARN := newListener(t, f, newStandardAccelerator(t, f))

	cases := []struct {
		in   gasdk.CreateEndpointGroupInput
		name string
		code string
	}{
		{
			gasdk.CreateEndpointGroupInput{EndpointGroupRegion: aws.String("nowhere")},
			"bad_region",
			"InvalidArgumentException",
		},
		{
			gasdk.CreateEndpointGroupInput{
				EndpointGroupRegion:        aws.String("us-east-1"),
				HealthCheckIntervalSeconds: aws.Int32(20),
			},
			"bad_interval",
			"InvalidArgumentException",
		},
		{
			gasdk.CreateEndpointGroupInput{
				EndpointGroupRegion:   aws.String("us-east-1"),
				TrafficDialPercentage: aws.Float32(120),
			},
			"bad_dial",
			"InvalidArgumentException",
		},
		{
			gasdk.CreateEndpointGroupInput{EndpointGroupRegion: aws.String("us-east-1"), ThresholdCount: aws.Int32(11)},
			"bad_threshold",
			"InvalidArgumentException",
		},
		{gasdk.CreateEndpointGroupInput{
			EndpointGroupRegion: aws.String("us-east-1"),
			PortOverrides:       []gatypes.PortOverride{{ListenerPort: aws.Int32(9999), EndpointPort: aws.Int32(80)}},
		}, "override_outside_listener", "InvalidArgumentException"},
		{gasdk.CreateEndpointGroupInput{
			EndpointGroupRegion:    aws.String("us-east-1"),
			EndpointConfigurations: []gatypes.EndpointConfiguration{{EndpointId: aws.String("not-an-endpoint")}},
		}, "bad_endpoint_id", "InvalidArgumentException"},
		{
			gasdk.CreateEndpointGroupInput{
				EndpointGroupRegion: aws.String("us-east-1"),
				EndpointConfigurations: []gatypes.EndpointConfiguration{
					{EndpointId: aws.String("i-1"), Weight: aws.Int32(256)},
				},
			},
			"bad_weight",
			"InvalidArgumentException",
		},
		{
			gasdk.CreateEndpointGroupInput{
				EndpointGroupRegion: aws.String("us-east-1"),
				EndpointConfigurations: []gatypes.EndpointConfiguration{
					{
						EndpointId:    aws.String("i-1"),
						AttachmentArn: aws.String("arn:aws:globalaccelerator::123456789012:attachment/x"),
					},
				},
			},
			"missing_attachment",
			"AttachmentNotFoundException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := tc.in
			in.ListenerArn = listenerARN
			_, err := f.client.CreateEndpointGroup(ctx, &in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}

	_, err := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: aws.String(*listenerARN + "x"), EndpointGroupRegion: aws.String("us-east-1"),
	})
	assert.Equal(t, "ListenerNotFoundException", apiCode(err))
}

func testEndpointMembership(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	listenerARN := newListener(t, f, newStandardAccelerator(t, f))

	g, err := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: listenerARN, EndpointGroupRegion: aws.String("us-west-2"),
	})
	require.NoError(t, err)

	arn := g.EndpointGroup.EndpointGroupArn

	added, err := f.client.AddEndpoints(ctx, &gasdk.AddEndpointsInput{
		EndpointGroupArn: arn,
		EndpointConfigurations: []gatypes.EndpointConfiguration{
			{
				EndpointId: aws.String("i-0123456789abcdef0"),
			},
			{EndpointId: aws.String("eipalloc-0abc"), Weight: aws.Int32(0)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(arn), aws.ToString(added.EndpointGroupArn))
	require.Len(t, added.EndpointDescriptions, 2)
	assert.EqualValues(t, 128, aws.ToInt32(added.EndpointDescriptions[0].Weight))
	assert.EqualValues(t, 0, aws.ToInt32(added.EndpointDescriptions[1].Weight))

	_, err = f.client.AddEndpoints(ctx, &gasdk.AddEndpointsInput{
		EndpointGroupArn:       arn,
		EndpointConfigurations: []gatypes.EndpointConfiguration{{EndpointId: aws.String("i-0123456789abcdef0")}},
	})
	assert.Equal(t, "EndpointAlreadyExistsException", apiCode(err))

	_, err = f.client.RemoveEndpoints(ctx, &gasdk.RemoveEndpointsInput{
		EndpointGroupArn:    arn,
		EndpointIdentifiers: []gatypes.EndpointIdentifier{{EndpointId: aws.String("i-missing")}},
	})
	assert.Equal(t, "EndpointNotFoundException", apiCode(err))

	_, err = f.client.RemoveEndpoints(ctx, &gasdk.RemoveEndpointsInput{
		EndpointGroupArn:    arn,
		EndpointIdentifiers: []gatypes.EndpointIdentifier{{EndpointId: aws.String("i-0123456789abcdef0")}},
	})
	require.NoError(t, err)

	got, err := f.client.DescribeEndpointGroup(ctx, &gasdk.DescribeEndpointGroupInput{EndpointGroupArn: arn})
	require.NoError(t, err)
	require.Len(t, got.EndpointGroup.EndpointDescriptions, 1)
	assert.Equal(t, "eipalloc-0abc", aws.ToString(got.EndpointGroup.EndpointDescriptions[0].EndpointId))

	more := make([]gatypes.EndpointConfiguration, 10)
	for i := range more {
		more[i] = gatypes.EndpointConfiguration{EndpointId: aws.String("i-" + string(rune('a'+i)))}
	}

	_, err = f.client.AddEndpoints(ctx, &gasdk.AddEndpointsInput{EndpointGroupArn: arn, EndpointConfigurations: more})
	assert.Equal(t, "LimitExceededException", apiCode(err))
}

func testDeleteGuards(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	accARN := newStandardAccelerator(t, f)
	listenerARN := newListener(t, f, accARN)

	g, err := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
		ListenerArn: listenerARN, EndpointGroupRegion: aws.String("us-west-2"),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteListener(ctx, &gasdk.DeleteListenerInput{ListenerArn: listenerARN})
	assert.Equal(t, "AssociatedEndpointGroupFoundException", apiCode(err))

	_, err = f.client.UpdateAccelerator(
		ctx,
		&gasdk.UpdateAcceleratorInput{AcceleratorArn: accARN, Enabled: aws.Bool(false)},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteAccelerator(ctx, &gasdk.DeleteAcceleratorInput{AcceleratorArn: accARN})
	assert.Equal(t, "AssociatedListenerFoundException", apiCode(err))

	_, err = f.client.DeleteEndpointGroup(
		ctx,
		&gasdk.DeleteEndpointGroupInput{EndpointGroupArn: g.EndpointGroup.EndpointGroupArn},
	)
	require.NoError(t, err)
	_, err = f.client.DeleteListener(ctx, &gasdk.DeleteListenerInput{ListenerArn: listenerARN})
	require.NoError(t, err)
	_, err = f.client.DeleteAccelerator(ctx, &gasdk.DeleteAcceleratorInput{AcceleratorArn: accARN})
	require.NoError(t, err)
	assert.Zero(t, f.backend.AcceleratorCount())
}
