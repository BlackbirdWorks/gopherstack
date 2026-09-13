package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_CoreResources covers medialive's core resource op families
// (gopherstack-n3zi): channels, inputs, input security groups, multiplexes/
// programs, schedules, reservations/offerings, signal maps, event bridge
// rule templates, cloudwatch alarm templates, nodes/clusters/networks,
// input devices, batch ops, and tags. Each case creates real state through
// the typed aws-sdk-go-v2 medialive client and asserts decoded response
// values.
func TestRealClient_CoreResources(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *medialivesdk.Client)
		name string
	}{
		{testChannelsLifecycleExtraRealClient, "channels_lifecycle_extra"},
		{testAccountConfigurationRealClient, "account_configuration"},
		{testChannelPlacementGroupsExtraRealClient, "channel_placement_groups_extra"},
		{testClustersExtraRealClient, "clusters_extra"},
		{testNetworksExtraRealClient, "networks_extra"},
		{testNodesExtraRealClient, "nodes_extra"},
		{testInputDevicesExtraRealClient, "input_devices_extra"},
		{testInputSecurityGroupsExtraRealClient, "input_security_groups_extra"},
		{testInputsExtraRealClient, "inputs_extra"},
		{testMultiplexesExtraRealClient, "multiplexes_extra"},
		{testMultiplexProgramsExtraRealClient, "multiplex_programs_extra"},
		{testReservationsExtraRealClient, "reservations_extra"},
		{testSignalMapsExtraRealClient, "signal_maps_extra"},
		{testSdiSourcesExtraRealClient, "sdi_sources_extra"},
		{testSchedulesExtraRealClient, "schedules_extra"},
		{testTagsExtraRealClient, "tags_extra"},
		{testBatchExtraRealClient, "batch_extra"},
		{testEventBridgeRuleTemplatesExtraRealClient, "event_bridge_rule_templates_extra"},
		{testCloudWatchAlarmTemplatesExtraRealClient, "cloudwatch_alarm_templates_extra"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaLiveClient(t, newTestHandler(t))
			tc.run(t, client)
		})
	}
}

// testChannelsLifecycleExtraRealClient covers DeleteChannel, ListChannels,
// StartChannel, StopChannel, RestartChannelPipelines, UpdateChannelClass,
// ListAlerts, ListVersions.
func testChannelsLifecycleExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateChannel(ctx, &medialivesdk.CreateChannelInput{
		Name:            aws.String("slice6-channel"),
		ChannelClass:    types.ChannelClassStandard,
		EncoderSettings: minimalValidEncoderSettings(),
	})
	require.NoError(t, err)

	list, err := client.ListChannels(ctx, &medialivesdk.ListChannelsInput{})
	require.NoError(t, err)

	found := false

	for _, c := range list.Channels {
		if aws.ToString(c.Id) == aws.ToString(created.Channel.Id) {
			found = true
		}
	}

	assert.True(t, found)

	startOut, err := client.StartChannel(ctx, &medialivesdk.StartChannelInput{
		ChannelId: created.Channel.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChannelStateStarting, startOut.State)

	alerts, err := client.ListAlerts(ctx, &medialivesdk.ListAlertsInput{
		ChannelId: created.Channel.Id,
	})
	require.NoError(t, err)
	assert.NotNil(t, alerts.Alerts)

	restarted, err := client.RestartChannelPipelines(ctx, &medialivesdk.RestartChannelPipelinesInput{
		ChannelId: created.Channel.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Channel.Id), aws.ToString(restarted.Id))

	stopOut, err := client.StopChannel(ctx, &medialivesdk.StopChannelInput{
		ChannelId: created.Channel.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChannelStateStopping, stopOut.State)

	classOut, err := client.UpdateChannelClass(ctx, &medialivesdk.UpdateChannelClassInput{
		ChannelId:    created.Channel.Id,
		ChannelClass: types.ChannelClassSinglePipeline,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChannelClassSinglePipeline, classOut.Channel.ChannelClass)

	versions, err := client.ListVersions(ctx, &medialivesdk.ListVersionsInput{})
	require.NoError(t, err)
	assert.NotNil(t, versions.Versions)

	_, err = client.DeleteChannel(ctx, &medialivesdk.DeleteChannelInput{
		ChannelId: created.Channel.Id,
	})
	require.NoError(t, err)
}

// testAccountConfigurationRealClient covers DescribeAccountConfiguration,
// UpdateAccountConfiguration.
func testAccountConfigurationRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.DescribeAccountConfiguration(
		ctx, &medialivesdk.DescribeAccountConfigurationInput{},
	)
	require.NoError(t, err)

	updated, err := client.UpdateAccountConfiguration(
		ctx,
		&medialivesdk.UpdateAccountConfigurationInput{
			AccountConfiguration: &types.AccountConfiguration{
				KmsKeyId: aws.String("arn:aws:kms:us-east-1:000000000000:key/slice6-key"),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, updated.AccountConfiguration)
	assert.Equal(
		t,
		"arn:aws:kms:us-east-1:000000000000:key/slice6-key",
		aws.ToString(updated.AccountConfiguration.KmsKeyId),
	)

	got, err := client.DescribeAccountConfiguration(
		ctx, &medialivesdk.DescribeAccountConfigurationInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, got.AccountConfiguration)
	assert.Equal(
		t,
		"arn:aws:kms:us-east-1:000000000000:key/slice6-key",
		aws.ToString(got.AccountConfiguration.KmsKeyId),
	)
}

// testChannelPlacementGroupsExtraRealClient covers
// DescribeChannelPlacementGroup, UpdateChannelPlacementGroup,
// ListChannelPlacementGroups, DeleteChannelPlacementGroup.
func testChannelPlacementGroupsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &medialivesdk.CreateClusterInput{
		Name: aws.String("slice6-cluster-cpg"),
	})
	require.NoError(t, err)

	created, err := client.CreateChannelPlacementGroup(
		ctx,
		&medialivesdk.CreateChannelPlacementGroupInput{
			ClusterId: cluster.Id,
			Name:      aws.String("slice6-cpg"),
		},
	)
	require.NoError(t, err)

	got, err := client.DescribeChannelPlacementGroup(
		ctx,
		&medialivesdk.DescribeChannelPlacementGroupInput{
			ClusterId:               cluster.Id,
			ChannelPlacementGroupId: created.Id,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-cpg", aws.ToString(got.Name))

	updated, err := client.UpdateChannelPlacementGroup(
		ctx,
		&medialivesdk.UpdateChannelPlacementGroupInput{
			ClusterId:               cluster.Id,
			ChannelPlacementGroupId: created.Id,
			Name:                    aws.String("slice6-cpg-renamed"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-cpg-renamed", aws.ToString(updated.Name))

	list, err := client.ListChannelPlacementGroups(
		ctx, &medialivesdk.ListChannelPlacementGroupsInput{ClusterId: cluster.Id},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, list.ChannelPlacementGroups)

	_, err = client.DeleteChannelPlacementGroup(
		ctx,
		&medialivesdk.DeleteChannelPlacementGroupInput{
			ClusterId:               cluster.Id,
			ChannelPlacementGroupId: created.Id,
		},
	)
	require.NoError(t, err)
}

// testClustersExtraRealClient covers DescribeCluster, UpdateCluster,
// ListClusters, DeleteCluster.
func testClustersExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &medialivesdk.CreateClusterInput{
		Name: aws.String("slice6-cluster"),
	})
	require.NoError(t, err)

	got, err := client.DescribeCluster(ctx, &medialivesdk.DescribeClusterInput{
		ClusterId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-cluster", aws.ToString(got.Name))

	updated, err := client.UpdateCluster(ctx, &medialivesdk.UpdateClusterInput{
		ClusterId: created.Id,
		Name:      aws.String("slice6-cluster-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-cluster-renamed", aws.ToString(updated.Name))

	list, err := client.ListClusters(ctx, &medialivesdk.ListClustersInput{})
	require.NoError(t, err)

	found := false

	for _, c := range list.Clusters {
		if aws.ToString(c.Id) == aws.ToString(created.Id) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.DeleteCluster(ctx, &medialivesdk.DeleteClusterInput{
		ClusterId: created.Id,
	})
	require.NoError(t, err)
}

// testNetworksExtraRealClient covers DescribeNetwork, UpdateNetwork,
// ListNetworks, DeleteNetwork.
func testNetworksExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateNetwork(ctx, &medialivesdk.CreateNetworkInput{
		Name: aws.String("slice6-network"),
	})
	require.NoError(t, err)

	got, err := client.DescribeNetwork(ctx, &medialivesdk.DescribeNetworkInput{
		NetworkId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-network", aws.ToString(got.Name))

	updated, err := client.UpdateNetwork(ctx, &medialivesdk.UpdateNetworkInput{
		NetworkId: created.Id,
		Name:      aws.String("slice6-network-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-network-renamed", aws.ToString(updated.Name))

	list, err := client.ListNetworks(ctx, &medialivesdk.ListNetworksInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.Networks)

	_, err = client.DeleteNetwork(ctx, &medialivesdk.DeleteNetworkInput{
		NetworkId: created.Id,
	})
	require.NoError(t, err)
}

// testNodesExtraRealClient covers DescribeNode, UpdateNode, UpdateNodeState,
// ListNodes, CreateNodeRegistrationScript, DeleteNode.
func testNodesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &medialivesdk.CreateClusterInput{
		Name: aws.String("slice6-cluster-node"),
	})
	require.NoError(t, err)

	created, err := client.CreateNode(ctx, &medialivesdk.CreateNodeInput{
		ClusterId: cluster.Id,
		Name:      aws.String("slice6-node"),
	})
	require.NoError(t, err)

	got, err := client.DescribeNode(ctx, &medialivesdk.DescribeNodeInput{
		ClusterId: cluster.Id,
		NodeId:    created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-node", aws.ToString(got.Name))

	updated, err := client.UpdateNode(ctx, &medialivesdk.UpdateNodeInput{
		ClusterId: cluster.Id,
		NodeId:    created.Id,
		Name:      aws.String("slice6-node-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-node-renamed", aws.ToString(updated.Name))

	stateOut, err := client.UpdateNodeState(ctx, &medialivesdk.UpdateNodeStateInput{
		ClusterId: cluster.Id,
		NodeId:    created.Id,
		State:     types.UpdateNodeStateShapeActive,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(stateOut.Id))

	list, err := client.ListNodes(ctx, &medialivesdk.ListNodesInput{ClusterId: cluster.Id})
	require.NoError(t, err)
	assert.NotEmpty(t, list.Nodes)

	script, err := client.CreateNodeRegistrationScript(
		ctx,
		&medialivesdk.CreateNodeRegistrationScriptInput{
			ClusterId: cluster.Id,
			Id:        created.Id,
			Name:      created.Name,
			Role:      types.NodeRoleActive,
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(script.NodeRegistrationScript))

	_, err = client.DeleteNode(ctx, &medialivesdk.DeleteNodeInput{
		ClusterId: cluster.Id,
		NodeId:    created.Id,
	})
	require.NoError(t, err)
}

// testInputDevicesExtraRealClient covers ClaimDevice, ListInputDevices,
// DescribeInputDevice, UpdateInputDevice, RebootInputDevice,
// TransferInputDevice, ListInputDeviceTransfers, CancelInputDeviceTransfer,
// AcceptInputDeviceTransfer, RejectInputDeviceTransfer, StartInputDevice,
// StopInputDevice, StartInputDeviceMaintenanceWindow.
func testInputDevicesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	claimed, err := client.ClaimDevice(ctx, &medialivesdk.ClaimDeviceInput{
		Id: aws.String("slice6-device-0001"),
	})
	require.NoError(t, err)
	_ = claimed

	list, err := client.ListInputDevices(ctx, &medialivesdk.ListInputDevicesInput{})
	require.NoError(t, err)

	found := false

	for _, d := range list.InputDevices {
		if aws.ToString(d.Id) == "slice6-device-0001" {
			found = true
		}
	}

	assert.True(t, found)

	got, err := client.DescribeInputDevice(ctx, &medialivesdk.DescribeInputDeviceInput{
		InputDeviceId: aws.String("slice6-device-0001"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-device-0001", aws.ToString(got.Id))

	updated, err := client.UpdateInputDevice(ctx, &medialivesdk.UpdateInputDeviceInput{
		InputDeviceId: aws.String("slice6-device-0001"),
		Name:          aws.String("slice6-device-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-device-renamed", aws.ToString(updated.Name))

	_, err = client.RebootInputDevice(ctx, &medialivesdk.RebootInputDeviceInput{
		InputDeviceId: aws.String("slice6-device-0001"),
	})
	require.NoError(t, err)

	_, err = client.TransferInputDevice(ctx, &medialivesdk.TransferInputDeviceInput{
		InputDeviceId:    aws.String("slice6-device-0001"),
		TargetCustomerId: aws.String("999999999999"),
	})
	require.NoError(t, err)

	transfers, err := client.ListInputDeviceTransfers(
		ctx, &medialivesdk.ListInputDeviceTransfersInput{TransferType: aws.String("OUTGOING")},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, transfers.InputDeviceTransfers)

	_, err = client.CancelInputDeviceTransfer(
		ctx,
		&medialivesdk.CancelInputDeviceTransferInput{InputDeviceId: aws.String("slice6-device-0001")},
	)
	require.NoError(t, err)

	claimed2, err := client.ClaimDevice(ctx, &medialivesdk.ClaimDeviceInput{
		Id: aws.String("slice6-device-0002"),
	})
	require.NoError(t, err)
	_ = claimed2

	_, err = client.TransferInputDevice(ctx, &medialivesdk.TransferInputDeviceInput{
		InputDeviceId:    aws.String("slice6-device-0002"),
		TargetCustomerId: aws.String("999999999999"),
	})
	require.NoError(t, err)

	_, err = client.AcceptInputDeviceTransfer(
		ctx,
		&medialivesdk.AcceptInputDeviceTransferInput{InputDeviceId: aws.String("slice6-device-0002")},
	)
	require.NoError(t, err)

	claimed3, err := client.ClaimDevice(ctx, &medialivesdk.ClaimDeviceInput{
		Id: aws.String("slice6-device-0003"),
	})
	require.NoError(t, err)
	_ = claimed3

	_, err = client.TransferInputDevice(ctx, &medialivesdk.TransferInputDeviceInput{
		InputDeviceId:    aws.String("slice6-device-0003"),
		TargetCustomerId: aws.String("999999999999"),
	})
	require.NoError(t, err)

	_, err = client.RejectInputDeviceTransfer(
		ctx,
		&medialivesdk.RejectInputDeviceTransferInput{InputDeviceId: aws.String("slice6-device-0003")},
	)
	require.NoError(t, err)

	_, err = client.StartInputDevice(ctx, &medialivesdk.StartInputDeviceInput{
		InputDeviceId: aws.String("slice6-device-0001"),
	})
	require.NoError(t, err)

	_, err = client.StopInputDevice(ctx, &medialivesdk.StopInputDeviceInput{
		InputDeviceId: aws.String("slice6-device-0001"),
	})
	require.NoError(t, err)

	_, err = client.StartInputDeviceMaintenanceWindow(
		ctx,
		&medialivesdk.StartInputDeviceMaintenanceWindowInput{InputDeviceId: aws.String("slice6-device-0001")},
	)
	require.NoError(t, err)
}

// testInputSecurityGroupsExtraRealClient covers UpdateInputSecurityGroup.
func testInputSecurityGroupsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateInputSecurityGroup(
		ctx,
		&medialivesdk.CreateInputSecurityGroupInput{
			WhitelistRules: []types.InputWhitelistRuleCidr{{Cidr: aws.String("10.0.0.0/24")}},
		},
	)
	require.NoError(t, err)

	updated, err := client.UpdateInputSecurityGroup(
		ctx,
		&medialivesdk.UpdateInputSecurityGroupInput{
			InputSecurityGroupId: created.SecurityGroup.Id,
			WhitelistRules:       []types.InputWhitelistRuleCidr{{Cidr: aws.String("192.168.0.0/16")}},
		},
	)
	require.NoError(t, err)
	require.Len(t, updated.SecurityGroup.WhitelistRules, 1)
	assert.Equal(t, "192.168.0.0/16", aws.ToString(updated.SecurityGroup.WhitelistRules[0].Cidr))
}

// testInputsExtraRealClient covers UpdateInput, ListInputs, CreatePartnerInput.
func testInputsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateInput(ctx, &medialivesdk.CreateInputInput{
		Name: aws.String("slice6-input"),
		Type: types.InputTypeRtmpPush,
		Destinations: []types.InputDestinationRequest{
			{StreamName: aws.String("live/slice6")},
		},
	})
	require.NoError(t, err)

	updated, err := client.UpdateInput(ctx, &medialivesdk.UpdateInputInput{
		InputId: created.Input.Id,
		Name:    aws.String("slice6-input-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-input-renamed", aws.ToString(updated.Input.Name))

	list, err := client.ListInputs(ctx, &medialivesdk.ListInputsInput{})
	require.NoError(t, err)

	found := false

	for _, in := range list.Inputs {
		if aws.ToString(in.Id) == aws.ToString(created.Input.Id) {
			found = true
		}
	}

	assert.True(t, found)

	partner, err := client.CreatePartnerInput(ctx, &medialivesdk.CreatePartnerInputInput{
		InputId: created.Input.Id,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(partner.Input.Id))
}

// testMultiplexesExtraRealClient covers DescribeMultiplex, UpdateMultiplex,
// StartMultiplex, ListMultiplexAlerts, StopMultiplex, DeleteMultiplex.
func testMultiplexesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateMultiplex(ctx, &medialivesdk.CreateMultiplexInput{
		Name:              aws.String("slice6-multiplex"),
		AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
		MultiplexSettings: &types.MultiplexSettings{
			TransportStreamBitrate: aws.Int32(12345678),
			TransportStreamId:      aws.Int32(1),
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeMultiplex(ctx, &medialivesdk.DescribeMultiplexInput{
		MultiplexId: created.Multiplex.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-multiplex", aws.ToString(got.Name))

	updated, err := client.UpdateMultiplex(ctx, &medialivesdk.UpdateMultiplexInput{
		MultiplexId: created.Multiplex.Id,
		Name:        aws.String("slice6-multiplex-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-multiplex-renamed", aws.ToString(updated.Multiplex.Name))

	startOut, err := client.StartMultiplex(ctx, &medialivesdk.StartMultiplexInput{
		MultiplexId: created.Multiplex.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateStarting, startOut.State)

	alerts, err := client.ListMultiplexAlerts(ctx, &medialivesdk.ListMultiplexAlertsInput{
		MultiplexId: created.Multiplex.Id,
	})
	require.NoError(t, err)
	assert.NotNil(t, alerts.Alerts)

	stopOut, err := client.StopMultiplex(ctx, &medialivesdk.StopMultiplexInput{
		MultiplexId: created.Multiplex.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateStopping, stopOut.State)

	_, err = client.DeleteMultiplex(ctx, &medialivesdk.DeleteMultiplexInput{
		MultiplexId: created.Multiplex.Id,
	})
	require.NoError(t, err)
}

// testMultiplexProgramsExtraRealClient covers CreateMultiplexProgram,
// DescribeMultiplexProgram, UpdateMultiplexProgram, ListMultiplexPrograms,
// DeleteMultiplexProgram.
func testMultiplexProgramsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	mux, err := client.CreateMultiplex(ctx, &medialivesdk.CreateMultiplexInput{
		Name:              aws.String("slice6-multiplex-programs"),
		AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
		MultiplexSettings: &types.MultiplexSettings{
			TransportStreamBitrate: aws.Int32(12345678),
			TransportStreamId:      aws.Int32(1),
		},
	})
	require.NoError(t, err)

	created, err := client.CreateMultiplexProgram(ctx, &medialivesdk.CreateMultiplexProgramInput{
		MultiplexId: mux.Multiplex.Id,
		ProgramName: aws.String("slice6-program"),
		MultiplexProgramSettings: &types.MultiplexProgramSettings{
			ProgramNumber: aws.Int32(1),
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeMultiplexProgram(ctx, &medialivesdk.DescribeMultiplexProgramInput{
		MultiplexId: mux.Multiplex.Id,
		ProgramName: aws.String("slice6-program"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-program", aws.ToString(got.ProgramName))

	updated, err := client.UpdateMultiplexProgram(ctx, &medialivesdk.UpdateMultiplexProgramInput{
		MultiplexId: mux.Multiplex.Id,
		ProgramName: aws.String("slice6-program"),
		MultiplexProgramSettings: &types.MultiplexProgramSettings{
			ProgramNumber: aws.Int32(2),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MultiplexProgram.MultiplexProgramSettings)
	assert.Equal(t, int32(2), aws.ToInt32(updated.MultiplexProgram.MultiplexProgramSettings.ProgramNumber))

	list, err := client.ListMultiplexPrograms(ctx, &medialivesdk.ListMultiplexProgramsInput{
		MultiplexId: mux.Multiplex.Id,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, list.MultiplexPrograms)

	_, err = client.DeleteMultiplexProgram(ctx, &medialivesdk.DeleteMultiplexProgramInput{
		MultiplexId: mux.Multiplex.Id,
		ProgramName: aws.String("slice6-program"),
	})
	require.NoError(t, err)

	_ = created
}

// testReservationsExtraRealClient covers ListOfferings and UpdateReservation.
// DeleteReservation is not exercised here: real AWS (and this backend,
// reservations.go's effectiveState) only allows deleting an EXPIRED
// reservation, and PurchaseOffering's term length comes from the offering
// catalog with no test-only time-travel hook to fast-forward it.
func testReservationsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	offerings, err := client.ListOfferings(ctx, &medialivesdk.ListOfferingsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, offerings.Offerings)

	offeringID := offerings.Offerings[0].OfferingId

	purchased, err := client.PurchaseOffering(ctx, &medialivesdk.PurchaseOfferingInput{
		OfferingId: offeringID,
		Count:      aws.Int32(1),
		Name:       aws.String("slice6-reservation"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateReservation(ctx, &medialivesdk.UpdateReservationInput{
		ReservationId: purchased.Reservation.ReservationId,
		Name:          aws.String("slice6-reservation-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-reservation-renamed", aws.ToString(updated.Reservation.Name))
}

// testSignalMapsExtraRealClient covers ListSignalMaps, DeleteSignalMap.
func testSignalMapsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateSignalMap(ctx, &medialivesdk.CreateSignalMapInput{
		Name:                   aws.String("slice6-signal-map"),
		DiscoveryEntryPointArn: aws.String("arn:aws:mediaconnect:us-east-1:000000000000:flow:1:f"),
	})
	require.NoError(t, err)

	list, err := client.ListSignalMaps(ctx, &medialivesdk.ListSignalMapsInput{})
	require.NoError(t, err)

	found := false

	for _, sm := range list.SignalMaps {
		if aws.ToString(sm.Id) == aws.ToString(created.Id) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.DeleteSignalMap(ctx, &medialivesdk.DeleteSignalMapInput{
		Identifier: created.Id,
	})
	require.NoError(t, err)
}

// testSdiSourcesExtraRealClient covers DescribeSdiSource, UpdateSdiSource,
// ListSdiSources, DeleteSdiSource.
func testSdiSourcesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateSdiSource(ctx, &medialivesdk.CreateSdiSourceInput{
		Name: aws.String("slice6-sdi-source"),
	})
	require.NoError(t, err)

	got, err := client.DescribeSdiSource(ctx, &medialivesdk.DescribeSdiSourceInput{
		SdiSourceId: created.SdiSource.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-sdi-source", aws.ToString(got.SdiSource.Name))

	updated, err := client.UpdateSdiSource(ctx, &medialivesdk.UpdateSdiSourceInput{
		SdiSourceId: created.SdiSource.Id,
		Name:        aws.String("slice6-sdi-source-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-sdi-source-renamed", aws.ToString(updated.SdiSource.Name))

	list, err := client.ListSdiSources(ctx, &medialivesdk.ListSdiSourcesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.SdiSources)

	_, err = client.DeleteSdiSource(ctx, &medialivesdk.DeleteSdiSourceInput{
		SdiSourceId: created.SdiSource.Id,
	})
	require.NoError(t, err)
}

// testSchedulesExtraRealClient covers DeleteSchedule.
func testSchedulesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateChannel(ctx, &medialivesdk.CreateChannelInput{
		Name:            aws.String("slice6-schedule-channel"),
		ChannelClass:    types.ChannelClassStandard,
		EncoderSettings: minimalValidEncoderSettings(),
	})
	require.NoError(t, err)

	_, err = client.BatchUpdateSchedule(ctx, &medialivesdk.BatchUpdateScheduleInput{
		ChannelId: created.Channel.Id,
		Creates: &types.BatchScheduleActionCreateRequest{
			ScheduleActions: []types.ScheduleAction{
				{
					ActionName: aws.String("slice6-action"),
					ScheduleActionStartSettings: &types.ScheduleActionStartSettings{
						ImmediateModeScheduleActionStartSettings: &types.ImmediateModeScheduleActionStartSettings{},
					},
					ScheduleActionSettings: &types.ScheduleActionSettings{
						InputSwitchSettings: &types.InputSwitchScheduleActionSettings{
							InputAttachmentNameReference: aws.String("input1"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteSchedule(ctx, &medialivesdk.DeleteScheduleInput{
		ChannelId: created.Channel.Id,
	})
	require.NoError(t, err)
}

// testTagsExtraRealClient covers CreateTags, DeleteTags.
func testTagsExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &medialivesdk.CreateClusterInput{
		Name: aws.String("slice6-tag-cluster"),
	})
	require.NoError(t, err)

	_, err = client.CreateTags(ctx, &medialivesdk.CreateTagsInput{
		ResourceArn: cluster.Arn,
		Tags:        map[string]string{"env": "slice6"},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(ctx, &medialivesdk.ListTagsForResourceInput{
		ResourceArn: cluster.Arn,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6", tagsOut.Tags["env"])

	_, err = client.DeleteTags(ctx, &medialivesdk.DeleteTagsInput{
		ResourceArn: cluster.Arn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	tagsOut, err = client.ListTagsForResource(ctx, &medialivesdk.ListTagsForResourceInput{
		ResourceArn: cluster.Arn,
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut.Tags)
}

// testBatchExtraRealClient covers BatchStart, BatchStop, BatchDelete.
func testBatchExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateChannel(ctx, &medialivesdk.CreateChannelInput{
		Name:            aws.String("slice6-batch-channel"),
		ChannelClass:    types.ChannelClassStandard,
		EncoderSettings: minimalValidEncoderSettings(),
	})
	require.NoError(t, err)

	startOut, err := client.BatchStart(ctx, &medialivesdk.BatchStartInput{
		ChannelIds: []string{aws.ToString(created.Channel.Id)},
	})
	require.NoError(t, err)
	require.Len(t, startOut.Successful, 1)

	stopOut, err := client.BatchStop(ctx, &medialivesdk.BatchStopInput{
		ChannelIds: []string{aws.ToString(created.Channel.Id)},
	})
	require.NoError(t, err)
	require.Len(t, stopOut.Successful, 1)

	deleteOut, err := client.BatchDelete(ctx, &medialivesdk.BatchDeleteInput{
		ChannelIds: []string{aws.ToString(created.Channel.Id)},
	})
	require.NoError(t, err)
	require.Len(t, deleteOut.Successful, 1)
}

// testEventBridgeRuleTemplatesExtraRealClient covers
// GetEventBridgeRuleTemplateGroup, ListEventBridgeRuleTemplateGroups,
// UpdateEventBridgeRuleTemplateGroup, DeleteEventBridgeRuleTemplateGroup,
// GetEventBridgeRuleTemplate, ListEventBridgeRuleTemplates,
// UpdateEventBridgeRuleTemplate, DeleteEventBridgeRuleTemplate.
func testEventBridgeRuleTemplatesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	group, err := client.CreateEventBridgeRuleTemplateGroup(
		ctx,
		&medialivesdk.CreateEventBridgeRuleTemplateGroupInput{Name: aws.String("slice6-ebrt-group")},
	)
	require.NoError(t, err)

	gotGroup, err := client.GetEventBridgeRuleTemplateGroup(
		ctx, &medialivesdk.GetEventBridgeRuleTemplateGroupInput{Identifier: group.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-ebrt-group", aws.ToString(gotGroup.Name))

	listGroups, err := client.ListEventBridgeRuleTemplateGroups(
		ctx, &medialivesdk.ListEventBridgeRuleTemplateGroupsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listGroups.EventBridgeRuleTemplateGroups)

	updatedGroup, err := client.UpdateEventBridgeRuleTemplateGroup(
		ctx,
		&medialivesdk.UpdateEventBridgeRuleTemplateGroupInput{
			Identifier:  group.Id,
			Description: aws.String("slice6 updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6 updated", aws.ToString(updatedGroup.Description))

	template, err := client.CreateEventBridgeRuleTemplate(
		ctx,
		&medialivesdk.CreateEventBridgeRuleTemplateInput{
			GroupIdentifier: group.Id,
			Name:            aws.String("slice6-ebrt"),
			EventType:       types.EventBridgeRuleTemplateEventTypeMedialiveChannelAlert,
		},
	)
	require.NoError(t, err)

	gotTemplate, err := client.GetEventBridgeRuleTemplate(
		ctx, &medialivesdk.GetEventBridgeRuleTemplateInput{Identifier: template.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-ebrt", aws.ToString(gotTemplate.Name))

	listTemplates, err := client.ListEventBridgeRuleTemplates(
		ctx, &medialivesdk.ListEventBridgeRuleTemplatesInput{GroupIdentifier: group.Id},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listTemplates.EventBridgeRuleTemplates)

	updatedTemplate, err := client.UpdateEventBridgeRuleTemplate(
		ctx,
		&medialivesdk.UpdateEventBridgeRuleTemplateInput{
			Identifier:  template.Id,
			Description: aws.String("slice6 template updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6 template updated", aws.ToString(updatedTemplate.Description))

	_, err = client.DeleteEventBridgeRuleTemplate(
		ctx, &medialivesdk.DeleteEventBridgeRuleTemplateInput{Identifier: template.Id},
	)
	require.NoError(t, err)

	_, err = client.DeleteEventBridgeRuleTemplateGroup(
		ctx, &medialivesdk.DeleteEventBridgeRuleTemplateGroupInput{Identifier: group.Id},
	)
	require.NoError(t, err)
}

// testCloudWatchAlarmTemplatesExtraRealClient covers
// GetCloudWatchAlarmTemplateGroup, ListCloudWatchAlarmTemplateGroups,
// UpdateCloudWatchAlarmTemplateGroup, DeleteCloudWatchAlarmTemplateGroup,
// GetCloudWatchAlarmTemplate, ListCloudWatchAlarmTemplates,
// UpdateCloudWatchAlarmTemplate, DeleteCloudWatchAlarmTemplate.
func testCloudWatchAlarmTemplatesExtraRealClient(t *testing.T, client *medialivesdk.Client) {
	t.Helper()
	ctx := t.Context()

	group, err := client.CreateCloudWatchAlarmTemplateGroup(
		ctx,
		&medialivesdk.CreateCloudWatchAlarmTemplateGroupInput{Name: aws.String("slice6-cw-group")},
	)
	require.NoError(t, err)

	gotGroup, err := client.GetCloudWatchAlarmTemplateGroup(
		ctx, &medialivesdk.GetCloudWatchAlarmTemplateGroupInput{Identifier: group.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-cw-group", aws.ToString(gotGroup.Name))

	listGroups, err := client.ListCloudWatchAlarmTemplateGroups(
		ctx, &medialivesdk.ListCloudWatchAlarmTemplateGroupsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listGroups.CloudWatchAlarmTemplateGroups)

	updatedGroup, err := client.UpdateCloudWatchAlarmTemplateGroup(
		ctx,
		&medialivesdk.UpdateCloudWatchAlarmTemplateGroupInput{
			Identifier:  group.Id,
			Description: aws.String("slice6 cw updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6 cw updated", aws.ToString(updatedGroup.Description))

	template, err := client.CreateCloudWatchAlarmTemplate(
		ctx,
		&medialivesdk.CreateCloudWatchAlarmTemplateInput{
			GroupIdentifier:    group.Id,
			Name:               aws.String("slice6-cw-template"),
			ComparisonOperator: types.CloudWatchAlarmTemplateComparisonOperatorGreaterThanThreshold,
			EvaluationPeriods:  aws.Int32(1),
			MetricName:         aws.String("4xxErrors"),
			Period:             aws.Int32(60),
			Statistic:          types.CloudWatchAlarmTemplateStatisticAverage,
			TargetResourceType: types.CloudWatchAlarmTemplateTargetResourceTypeMedialiveChannel,
			Threshold:          aws.Float64(1),
			TreatMissingData:   types.CloudWatchAlarmTemplateTreatMissingDataNotBreaching,
		},
	)
	require.NoError(t, err)

	gotTemplate, err := client.GetCloudWatchAlarmTemplate(
		ctx, &medialivesdk.GetCloudWatchAlarmTemplateInput{Identifier: template.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-cw-template", aws.ToString(gotTemplate.Name))

	listTemplates, err := client.ListCloudWatchAlarmTemplates(
		ctx, &medialivesdk.ListCloudWatchAlarmTemplatesInput{GroupIdentifier: group.Id},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listTemplates.CloudWatchAlarmTemplates)

	updatedTemplate, err := client.UpdateCloudWatchAlarmTemplate(
		ctx,
		&medialivesdk.UpdateCloudWatchAlarmTemplateInput{
			Identifier:  template.Id,
			Description: aws.String("slice6 cw template updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6 cw template updated", aws.ToString(updatedTemplate.Description))

	_, err = client.DeleteCloudWatchAlarmTemplate(
		ctx, &medialivesdk.DeleteCloudWatchAlarmTemplateInput{Identifier: template.Id},
	)
	require.NoError(t, err)

	_, err = client.DeleteCloudWatchAlarmTemplateGroup(
		ctx, &medialivesdk.DeleteCloudWatchAlarmTemplateGroupInput{Identifier: group.Id},
	)
	require.NoError(t, err)
}
