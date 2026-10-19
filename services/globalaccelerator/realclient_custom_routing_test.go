package globalaccelerator_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type customStack struct {
	accARN, listenerARN, groupARN *string
}

func newCustomStack(t *testing.T, f *fixture, listenerRange gatypes.PortRange) customStack {
	t.Helper()

	ctx := t.Context()

	acc, err := f.client.CreateCustomRoutingAccelerator(
		ctx,
		&gasdk.CreateCustomRoutingAcceleratorInput{Name: aws.String("custom")},
	)
	require.NoError(t, err)

	l, err := f.client.CreateCustomRoutingListener(ctx, &gasdk.CreateCustomRoutingListenerInput{
		AcceleratorArn: acc.Accelerator.AcceleratorArn, PortRanges: []gatypes.PortRange{listenerRange},
	})
	require.NoError(t, err)

	g, err := f.client.CreateCustomRoutingEndpointGroup(ctx, &gasdk.CreateCustomRoutingEndpointGroupInput{
		ListenerArn: l.Listener.ListenerArn, EndpointGroupRegion: aws.String("us-west-2"),
		DestinationConfigurations: []gatypes.CustomRoutingDestinationConfiguration{
			{
				FromPort:  aws.Int32(80),
				ToPort:    aws.Int32(81),
				Protocols: []gatypes.CustomRoutingProtocol{gatypes.CustomRoutingProtocolTcp},
			},
		},
	})
	require.NoError(t, err)

	return customStack{
		accARN:      acc.Accelerator.AcceleratorArn,
		listenerARN: l.Listener.ListenerArn,
		groupARN:    g.EndpointGroup.EndpointGroupArn,
	}
}

func TestRealClient_CustomRouting(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testCustomRoutingLifecycle, "lifecycle"},
		{testCustomRoutingPortMappings, "port_mappings"},
		{testCustomRoutingTraffic, "traffic"},
		{testCustomRoutingValidation, "validation"},
		{testCustomRoutingForeignGroup, "foreign_group"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testCustomRoutingLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	s := newCustomStack(t, f, portRange(10000, 20000))

	acc, err := f.client.DescribeCustomRoutingAccelerator(
		ctx,
		&gasdk.DescribeCustomRoutingAcceleratorInput{AcceleratorArn: s.accARN},
	)
	require.NoError(t, err)
	assert.Equal(t, gatypes.IpAddressTypeIpv4, acc.Accelerator.IpAddressType)
	assert.Len(t, acc.Accelerator.IpSets[0].IpAddresses, 2)

	upd, err := f.client.UpdateCustomRoutingAccelerator(ctx, &gasdk.UpdateCustomRoutingAcceleratorInput{
		AcceleratorArn: s.accARN, Name: aws.String("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(upd.Accelerator.Name))

	attrs, err := f.client.UpdateCustomRoutingAcceleratorAttributes(
		ctx,
		&gasdk.UpdateCustomRoutingAcceleratorAttributesInput{
			AcceleratorArn: s.accARN, FlowLogsEnabled: aws.Bool(true),
			FlowLogsS3Bucket: aws.String("b"), FlowLogsS3Prefix: aws.String("p/"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(attrs.AcceleratorAttributes.FlowLogsEnabled))

	got, err := f.client.DescribeCustomRoutingAcceleratorAttributes(
		ctx,
		&gasdk.DescribeCustomRoutingAcceleratorAttributesInput{AcceleratorArn: s.accARN},
	)
	require.NoError(t, err)
	assert.Equal(t, "b", aws.ToString(got.AcceleratorAttributes.FlowLogsS3Bucket))

	ls, err := f.client.ListCustomRoutingListeners(
		ctx,
		&gasdk.ListCustomRoutingListenersInput{AcceleratorArn: s.accARN},
	)
	require.NoError(t, err)
	require.Len(t, ls.Listeners, 1)

	ul, err := f.client.UpdateCustomRoutingListener(ctx, &gasdk.UpdateCustomRoutingListenerInput{
		ListenerArn: s.listenerARN, PortRanges: []gatypes.PortRange{portRange(30000, 30100)},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 30000, aws.ToInt32(ul.Listener.PortRanges[0].FromPort))

	_, err = f.client.DescribeListener(ctx, &gasdk.DescribeListenerInput{ListenerArn: s.listenerARN})
	assert.Equal(t, "ListenerNotFoundException", apiCode(err), "standard calls do not see custom routing listeners")

	groups, err := f.client.ListCustomRoutingEndpointGroups(
		ctx,
		&gasdk.ListCustomRoutingEndpointGroupsInput{ListenerArn: s.listenerARN},
	)
	require.NoError(t, err)
	require.Len(t, groups.EndpointGroups, 1)
	require.Len(t, groups.EndpointGroups[0].DestinationDescriptions, 1)
	assert.Equal(
		t,
		[]gatypes.Protocol{gatypes.ProtocolTcp},
		groups.EndpointGroups[0].DestinationDescriptions[0].Protocols,
	)

	_, err = f.client.CreateCustomRoutingEndpointGroup(ctx, &gasdk.CreateCustomRoutingEndpointGroupInput{
		ListenerArn: s.listenerARN, EndpointGroupRegion: aws.String("us-west-2"),
		DestinationConfigurations: []gatypes.CustomRoutingDestinationConfiguration{
			{
				FromPort:  aws.Int32(1),
				ToPort:    aws.Int32(2),
				Protocols: []gatypes.CustomRoutingProtocol{gatypes.CustomRoutingProtocolUdp},
			},
		},
	})
	assert.Equal(t, "EndpointGroupAlreadyExistsException", apiCode(err))

	_, err = f.client.DeleteCustomRoutingListener(
		ctx,
		&gasdk.DeleteCustomRoutingListenerInput{ListenerArn: s.listenerARN},
	)
	assert.Equal(t, "AssociatedEndpointGroupFoundException", apiCode(err))

	_, err = f.client.DeleteCustomRoutingEndpointGroup(
		ctx,
		&gasdk.DeleteCustomRoutingEndpointGroupInput{EndpointGroupArn: s.groupARN},
	)
	require.NoError(t, err)
	_, err = f.client.DeleteCustomRoutingListener(
		ctx,
		&gasdk.DeleteCustomRoutingListenerInput{ListenerArn: s.listenerARN},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteCustomRoutingAccelerator(
		ctx,
		&gasdk.DeleteCustomRoutingAcceleratorInput{AcceleratorArn: s.accARN},
	)
	assert.Equal(t, "AcceleratorNotDisabledException", apiCode(err))

	_, err = f.client.UpdateCustomRoutingAccelerator(
		ctx,
		&gasdk.UpdateCustomRoutingAcceleratorInput{AcceleratorArn: s.accARN, Enabled: aws.Bool(false)},
	)
	require.NoError(t, err)
	_, err = f.client.DeleteCustomRoutingAccelerator(
		ctx,
		&gasdk.DeleteCustomRoutingAcceleratorInput{AcceleratorArn: s.accARN},
	)
	require.NoError(t, err)

	list, err := f.client.ListCustomRoutingAccelerators(ctx, &gasdk.ListCustomRoutingAcceleratorsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Accelerators)
}

func testCustomRoutingPortMappings(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	s := newCustomStack(t, f, portRange(10000, 20000))

	added, err := f.client.AddCustomRoutingEndpoints(ctx, &gasdk.AddCustomRoutingEndpointsInput{
		EndpointGroupArn:       s.groupARN,
		EndpointConfigurations: []gatypes.CustomRoutingEndpointConfiguration{{EndpointId: aws.String("subnet-aaa")}},
	})
	require.NoError(t, err)
	require.Len(t, added.EndpointDescriptions, 1)
	assert.Equal(t, "subnet-aaa", aws.ToString(added.EndpointDescriptions[0].EndpointId))

	// 10.0.0.0/28 has 16-5=11 usable hosts (10.0.0.4 - 10.0.0.14) x ports 80-81 = 22 sockets.
	var all []gatypes.PortMapping

	var token *string

	pages := 0

	for {
		out, lerr := f.client.ListCustomRoutingPortMappings(ctx, &gasdk.ListCustomRoutingPortMappingsInput{
			AcceleratorArn: s.accARN, MaxResults: aws.Int32(10), NextToken: token,
		})
		require.NoError(t, lerr)

		pages++

		all = append(all, out.PortMappings...)

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	assert.Equal(t, 3, pages)
	require.Len(t, all, 22)
	assert.EqualValues(t, 10000, aws.ToInt32(all[0].AcceleratorPort))
	assert.Equal(t, "10.0.0.4", aws.ToString(all[0].DestinationSocketAddress.IpAddress))
	assert.EqualValues(t, 80, aws.ToInt32(all[0].DestinationSocketAddress.Port))
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateDeny, all[0].DestinationTrafficState)
	assert.EqualValues(t, 81, aws.ToInt32(all[1].DestinationSocketAddress.Port))
	assert.Equal(t, "10.0.0.14", aws.ToString(all[21].DestinationSocketAddress.IpAddress))
	assert.EqualValues(t, 10021, aws.ToInt32(all[21].AcceleratorPort))
	assert.Equal(t, "subnet-aaa", aws.ToString(all[0].EndpointId))
	assert.Equal(t, []gatypes.CustomRoutingProtocol{gatypes.CustomRoutingProtocolTcp}, all[0].Protocols)

	byGroup, err := f.client.ListCustomRoutingPortMappings(ctx, &gasdk.ListCustomRoutingPortMappingsInput{
		AcceleratorArn: s.accARN, EndpointGroupArn: s.groupARN, MaxResults: aws.Int32(100),
	})
	require.NoError(t, err)
	assert.Len(t, byGroup.PortMappings, 22)

	dest, err := f.client.ListCustomRoutingPortMappingsByDestination(
		ctx,
		&gasdk.ListCustomRoutingPortMappingsByDestinationInput{
			EndpointId: aws.String("subnet-aaa"), DestinationAddress: aws.String("10.0.0.5"),
		},
	)
	require.NoError(t, err)
	require.Len(t, dest.DestinationPortMappings, 2)
	m := dest.DestinationPortMappings[0]
	assert.EqualValues(t, 10002, aws.ToInt32(m.AcceleratorSocketAddresses[0].Port))
	assert.Len(t, m.AcceleratorSocketAddresses, 2)
	assert.Equal(t, aws.ToString(s.accARN), aws.ToString(m.AcceleratorArn))
	assert.Equal(t, "us-west-2", aws.ToString(m.EndpointGroupRegion))
	assert.Equal(t, gatypes.IpAddressTypeIpv4, m.IpAddressType)

	_, err = f.client.ListCustomRoutingPortMappingsByDestination(
		ctx,
		&gasdk.ListCustomRoutingPortMappingsByDestinationInput{
			EndpointId: aws.String("subnet-aaa"), DestinationAddress: aws.String("10.9.9.9"),
		},
	)
	assert.Equal(t, "EndpointNotFoundException", apiCode(err))

	_, err = f.client.RemoveCustomRoutingEndpoints(ctx, &gasdk.RemoveCustomRoutingEndpointsInput{
		EndpointGroupArn: s.groupARN, EndpointIds: []string{"subnet-aaa"},
	})
	require.NoError(t, err)

	empty, err := f.client.ListCustomRoutingPortMappings(
		ctx,
		&gasdk.ListCustomRoutingPortMappingsInput{AcceleratorArn: s.accARN},
	)
	require.NoError(t, err)
	assert.Empty(t, empty.PortMappings)
}

func testCustomRoutingTraffic(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	s := newCustomStack(t, f, portRange(10000, 20000))

	_, err := f.client.AddCustomRoutingEndpoints(ctx, &gasdk.AddCustomRoutingEndpointsInput{
		EndpointGroupArn:       s.groupARN,
		EndpointConfigurations: []gatypes.CustomRoutingEndpointConfiguration{{EndpointId: aws.String("subnet-aaa")}},
	})
	require.NoError(t, err)

	state := func(ip string, port int32) gatypes.CustomRoutingDestinationTrafficState {
		out, derr := f.client.ListCustomRoutingPortMappingsByDestination(
			ctx,
			&gasdk.ListCustomRoutingPortMappingsByDestinationInput{
				EndpointId: aws.String("subnet-aaa"), DestinationAddress: aws.String(ip),
			},
		)
		require.NoError(t, derr)

		for _, m := range out.DestinationPortMappings {
			if aws.ToInt32(m.DestinationSocketAddress.Port) == port {
				return m.DestinationTrafficState
			}
		}

		t.Fatalf("no mapping for %s:%d", ip, port)

		return ""
	}

	_, err = f.client.AllowCustomRoutingTraffic(ctx, &gasdk.AllowCustomRoutingTrafficInput{
		EndpointGroupArn: s.groupARN, EndpointId: aws.String("subnet-aaa"),
		DestinationAddresses: []string{"10.0.0.6"}, DestinationPorts: []int32{80},
	})
	require.NoError(t, err)
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateAllow, state("10.0.0.6", 80))
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateDeny, state("10.0.0.6", 81))
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateDeny, state("10.0.0.7", 80))

	_, err = f.client.AllowCustomRoutingTraffic(ctx, &gasdk.AllowCustomRoutingTrafficInput{
		EndpointGroupArn: s.groupARN, EndpointId: aws.String("subnet-aaa"), AllowAllTrafficToEndpoint: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateAllow, state("10.0.0.7", 81))

	_, err = f.client.DenyCustomRoutingTraffic(ctx, &gasdk.DenyCustomRoutingTrafficInput{
		EndpointGroupArn: s.groupARN, EndpointId: aws.String("subnet-aaa"), DestinationAddresses: []string{"10.0.0.7"},
	})
	require.NoError(t, err)
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateDeny, state("10.0.0.7", 81))
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateAllow, state("10.0.0.8", 81))

	_, err = f.client.DenyCustomRoutingTraffic(ctx, &gasdk.DenyCustomRoutingTrafficInput{
		EndpointGroupArn: s.groupARN, EndpointId: aws.String("subnet-aaa"), DenyAllTrafficToEndpoint: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, gatypes.CustomRoutingDestinationTrafficStateDeny, state("10.0.0.8", 81))

	cases := []struct {
		name string
		code string
		in   gasdk.AllowCustomRoutingTrafficInput
	}{
		{
			name: "unknown_endpoint", code: "EndpointNotFoundException",
			in: gasdk.AllowCustomRoutingTrafficInput{
				EndpointId: aws.String("subnet-zzz"), AllowAllTrafficToEndpoint: aws.Bool(true),
			},
		},
		{
			name: "address_outside_subnet", code: "InvalidArgumentException",
			in: gasdk.AllowCustomRoutingTrafficInput{
				EndpointId: aws.String("subnet-aaa"), DestinationAddresses: []string{"192.168.0.1"},
			},
		},
		{
			name: "port_outside_destinations", code: "InvalidPortRangeException",
			in: gasdk.AllowCustomRoutingTrafficInput{
				EndpointId: aws.String("subnet-aaa"), DestinationPorts: []int32{9999},
			},
		},
		{
			name: "nothing_specified", code: "InvalidArgumentException",
			in: gasdk.AllowCustomRoutingTrafficInput{EndpointId: aws.String("subnet-aaa")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := tc.in
			in.EndpointGroupArn = s.groupARN
			_, aerr := f.client.AllowCustomRoutingTraffic(ctx, &in)
			assert.Equal(t, tc.code, apiCode(aerr))
		})
	}
}

func testCustomRoutingValidation(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	s := newCustomStack(t, f, portRange(10000, 10010))

	_, err := f.client.AddCustomRoutingEndpoints(ctx, &gasdk.AddCustomRoutingEndpointsInput{
		EndpointGroupArn:       s.groupARN,
		EndpointConfigurations: []gatypes.CustomRoutingEndpointConfiguration{{EndpointId: aws.String("subnet-aaa")}},
	})
	assert.Equal(t, "LimitExceededException", apiCode(err), "22 destination sockets do not fit in 11 listener ports")

	cases := []struct {
		name string
		id   string
		code string
	}{
		{"unknown_subnet", "subnet-nope", "InvalidArgumentException"},
		{"not_a_subnet", "i-123", "InvalidArgumentException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, aerr := f.client.AddCustomRoutingEndpoints(ctx, &gasdk.AddCustomRoutingEndpointsInput{
				EndpointGroupArn:       s.groupARN,
				EndpointConfigurations: []gatypes.CustomRoutingEndpointConfiguration{{EndpointId: aws.String(tc.id)}},
			})
			assert.Equal(t, tc.code, apiCode(aerr))
		})
	}

	_, err = f.client.RemoveCustomRoutingEndpoints(ctx, &gasdk.RemoveCustomRoutingEndpointsInput{
		EndpointGroupArn: s.groupARN, EndpointIds: []string{"subnet-aaa"},
	})
	assert.Equal(t, "EndpointNotFoundException", apiCode(err))

	_, err = f.client.CreateCustomRoutingEndpointGroup(ctx, &gasdk.CreateCustomRoutingEndpointGroupInput{
		ListenerArn: s.listenerARN, EndpointGroupRegion: aws.String("eu-west-1"),
		DestinationConfigurations: []gatypes.CustomRoutingDestinationConfiguration{
			{
				FromPort:  aws.Int32(1),
				ToPort:    aws.Int32(10),
				Protocols: []gatypes.CustomRoutingProtocol{gatypes.CustomRoutingProtocolTcp},
			},
			{
				FromPort:  aws.Int32(5),
				ToPort:    aws.Int32(20),
				Protocols: []gatypes.CustomRoutingProtocol{gatypes.CustomRoutingProtocolUdp},
			},
		},
	})
	assert.Equal(t, "InvalidPortRangeException", apiCode(err))
}

func testCustomRoutingForeignGroup(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	a := newCustomStack(t, f, portRange(10000, 20000))
	b := newCustomStack(t, f, portRange(10000, 20000))

	_, err := f.client.ListCustomRoutingPortMappings(ctx, &gasdk.ListCustomRoutingPortMappingsInput{
		AcceleratorArn: a.accARN, EndpointGroupArn: b.groupARN,
	})
	assert.Equal(t, "EndpointGroupNotFoundException", apiCode(err))
}
