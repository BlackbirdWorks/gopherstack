package iotwireless_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	types "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_DevicesGatewaysAndConfig covers iotwireless's highest-priority
// typed-client-uncovered op families (gopherstack-n3zi): wireless
// devices/gateways, device profiles, destinations, multicast groups, FUOTA
// tasks, network analyzer, positioning, wireless gateway task definitions,
// event configurations, log levels, metrics, partner accounts, wireless
// device import tasks, and tags. Each subtest creates real state through the
// typed aws-sdk-go-v2 iotwireless client and asserts decoded response values.
func TestRealClient_DevicesGatewaysAndConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *iotwirelesssdk.Client)
		name string
	}{
		{testDestinationsRealClient, "destinations"},
		{testDeviceProfilesRealClient, "device_profiles"},
		{testWirelessGatewaysRealClient, "wireless_gateways"},
		{testWirelessGatewayCertificateRealClient, "wireless_gateway_certificate"},
		{testWirelessDevicesMiscRealClient, "wireless_devices_misc"},
		{testWirelessGatewayTasksRealClient, "wireless_gateway_tasks"},
		{testMulticastGroupsExtraRealClient, "multicast_groups_extra"},
		{testFuotaTasksExtraRealClient, "fuota_tasks_extra"},
		{testNetworkAnalyzerRealClient, "network_analyzer"},
		{testPositioningExtraRealClient, "positioning_extra"},
		{testPartnerAccountsRealClient, "partner_accounts"},
		{testEventConfigurationsExtraRealClient, "event_configurations_extra"},
		{testLogLevelsRealClient, "log_levels"},
		{testMetricsRealClient, "metrics"},
		{testTagsRealClient, "tags"},
		{testWirelessDeviceImportTasksRealClient, "wireless_device_import_tasks"},
		{testServiceEndpointRealClient, "service_endpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestIoTWirelessRegistryServer(t)
			client := newTestIoTWirelessSDKClient(t, srv.URL)
			tc.run(t, client)
		})
	}
}

// testDestinationsRealClient covers CreateDestination, GetDestination,
// ListDestinations, UpdateDestination, DeleteDestination.
func testDestinationsRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateDestination(ctx, &iotwirelesssdk.CreateDestinationInput{
		Name:           aws.String("slice6-destination"),
		Expression:     aws.String("iotwireless_rule"),
		ExpressionType: types.ExpressionTypeRuleName,
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/dest-role"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-destination", aws.ToString(created.Name))

	got, err := client.GetDestination(ctx, &iotwirelesssdk.GetDestinationInput{
		Name: aws.String("slice6-destination"),
	})
	require.NoError(t, err)
	assert.Equal(t, "iotwireless_rule", aws.ToString(got.Expression))

	list, err := client.ListDestinations(ctx, &iotwirelesssdk.ListDestinationsInput{})
	require.NoError(t, err)

	found := false

	for _, d := range list.DestinationList {
		if aws.ToString(d.Name) == "slice6-destination" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateDestination(ctx, &iotwirelesssdk.UpdateDestinationInput{
		Name:       aws.String("slice6-destination"),
		Expression: aws.String("iotwireless_rule_v2"),
	})
	require.NoError(t, err)

	got, err = client.GetDestination(ctx, &iotwirelesssdk.GetDestinationInput{
		Name: aws.String("slice6-destination"),
	})
	require.NoError(t, err)
	assert.Equal(t, "iotwireless_rule_v2", aws.ToString(got.Expression))

	_, err = client.DeleteDestination(ctx, &iotwirelesssdk.DeleteDestinationInput{
		Name: aws.String("slice6-destination"),
	})
	require.NoError(t, err)

	_, err = client.GetDestination(ctx, &iotwirelesssdk.GetDestinationInput{
		Name: aws.String("slice6-destination"),
	})
	require.Error(t, err)
}

// testDeviceProfilesRealClient covers CreateDeviceProfile, GetDeviceProfile,
// ListDeviceProfiles, DeleteDeviceProfile.
func testDeviceProfilesRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateDeviceProfile(ctx, &iotwirelesssdk.CreateDeviceProfileInput{
		Name: aws.String("slice6-device-profile"),
		LoRaWAN: &types.LoRaWANDeviceProfile{
			MacVersion: aws.String("1.0.3"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetDeviceProfile(ctx, &iotwirelesssdk.GetDeviceProfileInput{
		Id: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-device-profile", aws.ToString(got.Name))

	list, err := client.ListDeviceProfiles(ctx, &iotwirelesssdk.ListDeviceProfilesInput{})
	require.NoError(t, err)

	found := false

	for _, dp := range list.DeviceProfileList {
		if aws.ToString(dp.Id) == aws.ToString(created.Id) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.DeleteDeviceProfile(ctx, &iotwirelesssdk.DeleteDeviceProfileInput{
		Id: created.Id,
	})
	require.NoError(t, err)

	_, err = client.GetDeviceProfile(ctx, &iotwirelesssdk.GetDeviceProfileInput{
		Id: created.Id,
	})
	require.Error(t, err)
}

// testWirelessGatewaysRealClient covers CreateWirelessGateway,
// GetWirelessGateway, ListWirelessGateways, UpdateWirelessGateway,
// AssociateWirelessGatewayWithThing, DisassociateWirelessGatewayFromThing,
// GetWirelessGatewayFirmwareInformation, GetWirelessGatewayStatistics,
// DeleteWirelessGateway.
func testWirelessGatewaysRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateWirelessGateway(ctx, &iotwirelesssdk.CreateWirelessGatewayInput{
		Name: aws.String("slice6-gateway"),
		LoRaWAN: &types.LoRaWANGateway{
			GatewayEui: aws.String("AAAA0000BBBB1111"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetWirelessGateway(ctx, &iotwirelesssdk.GetWirelessGatewayInput{
		Identifier:     created.Id,
		IdentifierType: types.WirelessGatewayIdTypeWirelessGatewayId,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-gateway", aws.ToString(got.Name))

	list, err := client.ListWirelessGateways(ctx, &iotwirelesssdk.ListWirelessGatewaysInput{})
	require.NoError(t, err)

	found := false

	for _, gw := range list.WirelessGatewayList {
		if aws.ToString(gw.Id) == aws.ToString(created.Id) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateWirelessGateway(ctx, &iotwirelesssdk.UpdateWirelessGatewayInput{
		Id:   created.Id,
		Name: aws.String("slice6-gateway-renamed"),
	})
	require.NoError(t, err)

	got, err = client.GetWirelessGateway(ctx, &iotwirelesssdk.GetWirelessGatewayInput{
		Identifier:     created.Id,
		IdentifierType: types.WirelessGatewayIdTypeWirelessGatewayId,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-gateway-renamed", aws.ToString(got.Name))

	_, err = client.AssociateWirelessGatewayWithThing(
		ctx,
		&iotwirelesssdk.AssociateWirelessGatewayWithThingInput{
			Id:       created.Id,
			ThingArn: aws.String("arn:aws:iot:us-east-1:000000000000:thing/gw-thing"),
		},
	)
	require.NoError(t, err)

	got, err = client.GetWirelessGateway(ctx, &iotwirelesssdk.GetWirelessGatewayInput{
		Identifier:     created.Id,
		IdentifierType: types.WirelessGatewayIdTypeWirelessGatewayId,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:thing/gw-thing", aws.ToString(got.ThingArn))

	fw, err := client.GetWirelessGatewayFirmwareInformation(
		ctx,
		&iotwirelesssdk.GetWirelessGatewayFirmwareInformationInput{Id: created.Id},
	)
	require.NoError(t, err)
	require.NotNil(t, fw.LoRaWAN)

	stats, err := client.GetWirelessGatewayStatistics(
		ctx,
		&iotwirelesssdk.GetWirelessGatewayStatisticsInput{WirelessGatewayId: created.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(stats.WirelessGatewayId))

	_, err = client.DisassociateWirelessGatewayFromThing(
		ctx,
		&iotwirelesssdk.DisassociateWirelessGatewayFromThingInput{Id: created.Id},
	)
	require.NoError(t, err)

	got, err = client.GetWirelessGateway(ctx, &iotwirelesssdk.GetWirelessGatewayInput{
		Identifier:     created.Id,
		IdentifierType: types.WirelessGatewayIdTypeWirelessGatewayId,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.ThingArn))

	_, err = client.DeleteWirelessGateway(ctx, &iotwirelesssdk.DeleteWirelessGatewayInput{
		Id: created.Id,
	})
	require.NoError(t, err)
}

// testWirelessGatewayCertificateRealClient covers
// AssociateWirelessGatewayWithCertificate, GetWirelessGatewayCertificate,
// DisassociateWirelessGatewayFromCertificate.
func testWirelessGatewayCertificateRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	gw, err := client.CreateWirelessGateway(ctx, &iotwirelesssdk.CreateWirelessGatewayInput{
		Name:    aws.String("slice6-gateway-cert"),
		LoRaWAN: &types.LoRaWANGateway{GatewayEui: aws.String("CCCC0000DDDD1111")},
	})
	require.NoError(t, err)

	assoc, err := client.AssociateWirelessGatewayWithCertificate(
		ctx,
		&iotwirelesssdk.AssociateWirelessGatewayWithCertificateInput{
			Id:               gw.Id,
			IotCertificateId: aws.String("cert-0001"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "cert-0001", aws.ToString(assoc.IotCertificateId))

	got, err := client.GetWirelessGatewayCertificate(
		ctx,
		&iotwirelesssdk.GetWirelessGatewayCertificateInput{Id: gw.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "cert-0001", aws.ToString(got.IotCertificateId))

	_, err = client.DisassociateWirelessGatewayFromCertificate(
		ctx,
		&iotwirelesssdk.DisassociateWirelessGatewayFromCertificateInput{Id: gw.Id},
	)
	require.NoError(t, err)
}

// testWirelessDevicesMiscRealClient covers AssociateWirelessDeviceWithThing,
// DisassociateWirelessDeviceFromThing, UpdateWirelessDevice,
// GetWirelessDeviceStatistics, SendDataToWirelessDevice,
// ListQueuedMessages, DeleteQueuedMessages, TestWirelessDevice,
// DeregisterWirelessDevice.
func testWirelessDevicesMiscRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	dev, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("slice6-device"),
		Type:            types.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("slice6-dest"),
	})
	require.NoError(t, err)

	_, err = client.AssociateWirelessDeviceWithThing(
		ctx,
		&iotwirelesssdk.AssociateWirelessDeviceWithThingInput{
			Id:       dev.Id,
			ThingArn: aws.String("arn:aws:iot:us-east-1:000000000000:thing/dev-thing"),
		},
	)
	require.NoError(t, err)

	got, err := client.GetWirelessDevice(ctx, &iotwirelesssdk.GetWirelessDeviceInput{
		Identifier:     dev.Id,
		IdentifierType: types.WirelessDeviceIdTypeWirelessDeviceId,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:thing/dev-thing", aws.ToString(got.ThingArn))

	_, err = client.UpdateWirelessDevice(ctx, &iotwirelesssdk.UpdateWirelessDeviceInput{
		Id:   dev.Id,
		Name: aws.String("slice6-device-renamed"),
	})
	require.NoError(t, err)

	got, err = client.GetWirelessDevice(ctx, &iotwirelesssdk.GetWirelessDeviceInput{
		Identifier:     dev.Id,
		IdentifierType: types.WirelessDeviceIdTypeWirelessDeviceId,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-device-renamed", aws.ToString(got.Name))

	stats, err := client.GetWirelessDeviceStatistics(
		ctx,
		&iotwirelesssdk.GetWirelessDeviceStatisticsInput{WirelessDeviceId: dev.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(dev.Id), aws.ToString(stats.WirelessDeviceId))

	sendOut, err := client.SendDataToWirelessDevice(ctx, &iotwirelesssdk.SendDataToWirelessDeviceInput{
		Id:           dev.Id,
		PayloadData:  aws.String("AQIDBA=="),
		TransmitMode: aws.Int32(0),
		WirelessMetadata: &types.WirelessMetadata{
			LoRaWAN: &types.LoRaWANSendDataToDevice{FPort: aws.Int32(1)},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(sendOut.MessageId))

	msgs, err := client.ListQueuedMessages(ctx, &iotwirelesssdk.ListQueuedMessagesInput{
		Id: dev.Id,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, msgs.DownlinkQueueMessagesList)

	_, err = client.DeleteQueuedMessages(ctx, &iotwirelesssdk.DeleteQueuedMessagesInput{
		Id:        dev.Id,
		MessageId: aws.String("*"),
	})
	require.NoError(t, err)

	msgs, err = client.ListQueuedMessages(ctx, &iotwirelesssdk.ListQueuedMessagesInput{
		Id: dev.Id,
	})
	require.NoError(t, err)
	assert.Empty(t, msgs.DownlinkQueueMessagesList)

	testOut, err := client.TestWirelessDevice(ctx, &iotwirelesssdk.TestWirelessDeviceInput{
		Id: dev.Id,
	})
	require.NoError(t, err)
	assert.NotNil(t, testOut.Result)

	_, err = client.DisassociateWirelessDeviceFromThing(
		ctx,
		&iotwirelesssdk.DisassociateWirelessDeviceFromThingInput{Id: dev.Id},
	)
	require.NoError(t, err)

	_, err = client.DeregisterWirelessDevice(ctx, &iotwirelesssdk.DeregisterWirelessDeviceInput{
		Identifier: dev.Id,
	})
	require.NoError(t, err)

	_, err = client.GetWirelessDevice(ctx, &iotwirelesssdk.GetWirelessDeviceInput{
		Identifier:     dev.Id,
		IdentifierType: types.WirelessDeviceIdTypeWirelessDeviceId,
	})
	require.Error(t, err)
}

// testWirelessGatewayTasksRealClient covers CreateWirelessGatewayTask,
// GetWirelessGatewayTask, DeleteWirelessGatewayTask,
// CreateWirelessGatewayTaskDefinition, GetWirelessGatewayTaskDefinition,
// ListWirelessGatewayTaskDefinitions, DeleteWirelessGatewayTaskDefinition.
func testWirelessGatewayTasksRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	gw, err := client.CreateWirelessGateway(ctx, &iotwirelesssdk.CreateWirelessGatewayInput{
		Name:    aws.String("slice6-gateway-task"),
		LoRaWAN: &types.LoRaWANGateway{GatewayEui: aws.String("EEEE0000FFFF1111")},
	})
	require.NoError(t, err)

	taskDef, err := client.CreateWirelessGatewayTaskDefinition(
		ctx,
		&iotwirelesssdk.CreateWirelessGatewayTaskDefinitionInput{
			Name:            aws.String("slice6-task-def"),
			AutoCreateTasks: false,
			Update: &types.UpdateWirelessGatewayTaskCreate{
				UpdateDataSource: aws.String("s3://bucket/firmware.bin"),
				UpdateDataRole:   aws.String("arn:aws:iam::000000000000:role/update"),
			},
		},
	)
	require.NoError(t, err)

	gotDef, err := client.GetWirelessGatewayTaskDefinition(
		ctx,
		&iotwirelesssdk.GetWirelessGatewayTaskDefinitionInput{Id: taskDef.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-task-def", aws.ToString(gotDef.Name))

	listDef, err := client.ListWirelessGatewayTaskDefinitions(
		ctx, &iotwirelesssdk.ListWirelessGatewayTaskDefinitionsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listDef.TaskDefinitions)

	_, err = client.CreateWirelessGatewayTask(ctx, &iotwirelesssdk.CreateWirelessGatewayTaskInput{
		Id:                              gw.Id,
		WirelessGatewayTaskDefinitionId: taskDef.Id,
	})
	require.NoError(t, err)

	gotTask, err := client.GetWirelessGatewayTask(
		ctx, &iotwirelesssdk.GetWirelessGatewayTaskInput{Id: gw.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(taskDef.Id), aws.ToString(gotTask.WirelessGatewayTaskDefinitionId))

	_, err = client.DeleteWirelessGatewayTask(
		ctx, &iotwirelesssdk.DeleteWirelessGatewayTaskInput{Id: gw.Id},
	)
	require.NoError(t, err)

	_, err = client.DeleteWirelessGatewayTaskDefinition(
		ctx, &iotwirelesssdk.DeleteWirelessGatewayTaskDefinitionInput{Id: taskDef.Id},
	)
	require.NoError(t, err)
}

// testMulticastGroupsExtraRealClient covers GetMulticastGroup,
// ListMulticastGroups, UpdateMulticastGroup,
// DisassociateWirelessDeviceFromMulticastGroup,
// StartBulkAssociateWirelessDeviceWithMulticastGroup,
// StartBulkDisassociateWirelessDeviceFromMulticastGroup,
// StartMulticastGroupSession, GetMulticastGroupSession,
// CancelMulticastGroupSession, SendDataToMulticastGroup,
// DeleteMulticastGroup.
func testMulticastGroupsExtraRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	mg, err := client.CreateMulticastGroup(ctx, &iotwirelesssdk.CreateMulticastGroupInput{
		Name: aws.String("slice6-mg"),
		LoRaWAN: &types.LoRaWANMulticast{
			RfRegion: types.SupportedRfRegionUs915,
		},
	})
	require.NoError(t, err)

	got, err := client.GetMulticastGroup(ctx, &iotwirelesssdk.GetMulticastGroupInput{Id: mg.Id})
	require.NoError(t, err)
	assert.Equal(t, "slice6-mg", aws.ToString(got.Name))

	list, err := client.ListMulticastGroups(ctx, &iotwirelesssdk.ListMulticastGroupsInput{})
	require.NoError(t, err)

	found := false

	for _, g := range list.MulticastGroupList {
		if aws.ToString(g.Id) == aws.ToString(mg.Id) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateMulticastGroup(ctx, &iotwirelesssdk.UpdateMulticastGroupInput{
		Id:   mg.Id,
		Name: aws.String("slice6-mg-renamed"),
	})
	require.NoError(t, err)

	got, err = client.GetMulticastGroup(ctx, &iotwirelesssdk.GetMulticastGroupInput{Id: mg.Id})
	require.NoError(t, err)
	assert.Equal(t, "slice6-mg-renamed", aws.ToString(got.Name))

	dev, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("slice6-mg-device"),
		Type:            types.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("slice6-dest"),
	})
	require.NoError(t, err)

	_, err = client.AssociateWirelessDeviceWithMulticastGroup(
		ctx,
		&iotwirelesssdk.AssociateWirelessDeviceWithMulticastGroupInput{
			Id:               mg.Id,
			WirelessDeviceId: dev.Id,
		},
	)
	require.NoError(t, err)

	_, err = client.DisassociateWirelessDeviceFromMulticastGroup(
		ctx,
		&iotwirelesssdk.DisassociateWirelessDeviceFromMulticastGroupInput{
			Id:               mg.Id,
			WirelessDeviceId: dev.Id,
		},
	)
	require.NoError(t, err)

	_, err = client.StartBulkAssociateWirelessDeviceWithMulticastGroup(
		ctx,
		&iotwirelesssdk.StartBulkAssociateWirelessDeviceWithMulticastGroupInput{
			Id:          mg.Id,
			QueryString: aws.String("*"),
		},
	)
	require.NoError(t, err)

	_, err = client.StartBulkDisassociateWirelessDeviceFromMulticastGroup(
		ctx,
		&iotwirelesssdk.StartBulkDisassociateWirelessDeviceFromMulticastGroupInput{
			Id:          mg.Id,
			QueryString: aws.String("*"),
		},
	)
	require.NoError(t, err)

	_, err = client.StartMulticastGroupSession(ctx, &iotwirelesssdk.StartMulticastGroupSessionInput{
		Id: mg.Id,
		LoRaWAN: &types.LoRaWANMulticastSession{
			DlDr: aws.Int32(1),
		},
	})
	require.NoError(t, err)

	session, err := client.GetMulticastGroupSession(
		ctx, &iotwirelesssdk.GetMulticastGroupSessionInput{Id: mg.Id},
	)
	require.NoError(t, err)
	require.NotNil(t, session.LoRaWAN)
	assert.NotNil(t, session.LoRaWAN.SessionStartTime)

	_, err = client.CancelMulticastGroupSession(
		ctx, &iotwirelesssdk.CancelMulticastGroupSessionInput{Id: mg.Id},
	)
	require.NoError(t, err)

	sendOut, err := client.SendDataToMulticastGroup(ctx, &iotwirelesssdk.SendDataToMulticastGroupInput{
		Id:          mg.Id,
		PayloadData: aws.String("AQIDBA=="),
		WirelessMetadata: &types.MulticastWirelessMetadata{
			LoRaWAN: &types.LoRaWANMulticastMetadata{FPort: aws.Int32(1)},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(sendOut.MessageId))

	_, err = client.DeleteMulticastGroup(ctx, &iotwirelesssdk.DeleteMulticastGroupInput{Id: mg.Id})
	require.NoError(t, err)
}

// testFuotaTasksExtraRealClient covers GetFuotaTask, ListFuotaTasks,
// UpdateFuotaTask, DisassociateWirelessDeviceFromFuotaTask,
// DisassociateMulticastGroupFromFuotaTask, StartFuotaTask, DeleteFuotaTask.
func testFuotaTasksExtraRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	ft, err := client.CreateFuotaTask(ctx, &iotwirelesssdk.CreateFuotaTaskInput{
		Name:                aws.String("slice6-fuota"),
		FirmwareUpdateImage: aws.String("s3://bucket/fw.bin"),
		FirmwareUpdateRole:  aws.String("arn:aws:iam::000000000000:role/fuota-role"),
	})
	require.NoError(t, err)

	got, err := client.GetFuotaTask(ctx, &iotwirelesssdk.GetFuotaTaskInput{Id: ft.Id})
	require.NoError(t, err)
	assert.Equal(t, "slice6-fuota", aws.ToString(got.Name))

	list, err := client.ListFuotaTasks(ctx, &iotwirelesssdk.ListFuotaTasksInput{})
	require.NoError(t, err)

	found := false

	for _, f := range list.FuotaTaskList {
		if aws.ToString(f.Id) == aws.ToString(ft.Id) {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateFuotaTask(ctx, &iotwirelesssdk.UpdateFuotaTaskInput{
		Id:   ft.Id,
		Name: aws.String("slice6-fuota-renamed"),
	})
	require.NoError(t, err)

	got, err = client.GetFuotaTask(ctx, &iotwirelesssdk.GetFuotaTaskInput{Id: ft.Id})
	require.NoError(t, err)
	assert.Equal(t, "slice6-fuota-renamed", aws.ToString(got.Name))

	dev, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("slice6-fuota-device"),
		Type:            types.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("slice6-dest"),
	})
	require.NoError(t, err)

	_, err = client.AssociateWirelessDeviceWithFuotaTask(
		ctx,
		&iotwirelesssdk.AssociateWirelessDeviceWithFuotaTaskInput{Id: ft.Id, WirelessDeviceId: dev.Id},
	)
	require.NoError(t, err)

	_, err = client.DisassociateWirelessDeviceFromFuotaTask(
		ctx,
		&iotwirelesssdk.DisassociateWirelessDeviceFromFuotaTaskInput{Id: ft.Id, WirelessDeviceId: dev.Id},
	)
	require.NoError(t, err)

	mg, err := client.CreateMulticastGroup(ctx, &iotwirelesssdk.CreateMulticastGroupInput{
		Name:    aws.String("slice6-fuota-mg"),
		LoRaWAN: &types.LoRaWANMulticast{RfRegion: types.SupportedRfRegionUs915},
	})
	require.NoError(t, err)

	_, err = client.AssociateMulticastGroupWithFuotaTask(
		ctx,
		&iotwirelesssdk.AssociateMulticastGroupWithFuotaTaskInput{Id: ft.Id, MulticastGroupId: mg.Id},
	)
	require.NoError(t, err)

	_, err = client.DisassociateMulticastGroupFromFuotaTask(
		ctx,
		&iotwirelesssdk.DisassociateMulticastGroupFromFuotaTaskInput{Id: ft.Id, MulticastGroupId: mg.Id},
	)
	require.NoError(t, err)

	_, err = client.StartFuotaTask(ctx, &iotwirelesssdk.StartFuotaTaskInput{
		Id: ft.Id,
		LoRaWAN: &types.LoRaWANStartFuotaTask{
			StartTime: aws.Time(time.Now().UTC()),
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteFuotaTask(ctx, &iotwirelesssdk.DeleteFuotaTaskInput{Id: ft.Id})
	require.NoError(t, err)
}

// testNetworkAnalyzerRealClient covers CreateNetworkAnalyzerConfiguration,
// GetNetworkAnalyzerConfiguration, ListNetworkAnalyzerConfigurations,
// UpdateNetworkAnalyzerConfiguration, DeleteNetworkAnalyzerConfiguration.
func testNetworkAnalyzerRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateNetworkAnalyzerConfiguration(
		ctx,
		&iotwirelesssdk.CreateNetworkAnalyzerConfigurationInput{
			Name: aws.String("slice6-nac"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-nac", aws.ToString(created.Name))

	got, err := client.GetNetworkAnalyzerConfiguration(
		ctx,
		&iotwirelesssdk.GetNetworkAnalyzerConfigurationInput{ConfigurationName: aws.String("slice6-nac")},
	)
	require.NoError(t, err)
	assert.Empty(t, got.WirelessDevices)

	list, err := client.ListNetworkAnalyzerConfigurations(
		ctx, &iotwirelesssdk.ListNetworkAnalyzerConfigurationsInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, list.NetworkAnalyzerConfigurationList)

	_, err = client.UpdateNetworkAnalyzerConfiguration(
		ctx,
		&iotwirelesssdk.UpdateNetworkAnalyzerConfigurationInput{
			ConfigurationName: aws.String("slice6-nac"),
			WirelessDevicesToAdd: []string{
				"dev-1",
			},
		},
	)
	require.NoError(t, err)

	got, err = client.GetNetworkAnalyzerConfiguration(
		ctx,
		&iotwirelesssdk.GetNetworkAnalyzerConfigurationInput{ConfigurationName: aws.String("slice6-nac")},
	)
	require.NoError(t, err)
	assert.Contains(t, got.WirelessDevices, "dev-1")

	_, err = client.DeleteNetworkAnalyzerConfiguration(
		ctx,
		&iotwirelesssdk.DeleteNetworkAnalyzerConfigurationInput{ConfigurationName: aws.String("slice6-nac")},
	)
	require.NoError(t, err)
}

// testPositioningExtraRealClient covers PutPositionConfiguration,
// GetPositionConfiguration, ListPositionConfigurations, GetPositionEstimate.
func testPositioningExtraRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	dev, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("slice6-position-device"),
		Type:            types.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("slice6-dest"),
	})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	_, err = client.PutPositionConfiguration(ctx, &iotwirelesssdk.PutPositionConfigurationInput{
		ResourceIdentifier: dev.Id,
		ResourceType:       types.PositionResourceTypeWirelessDevice,
		Destination:        aws.String("slice6-position-dest"),
		Solvers: &types.PositionSolverConfigurations{
			SemtechGnss: &types.SemtechGnssConfiguration{
				Status: types.PositionConfigurationStatusEnabled,
				Fec:    types.PositionConfigurationFecRose,
			},
		},
	})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	got, err := client.GetPositionConfiguration(ctx, &iotwirelesssdk.GetPositionConfigurationInput{
		ResourceIdentifier: dev.Id,
		ResourceType:       types.PositionResourceTypeWirelessDevice,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice6-position-dest", aws.ToString(got.Destination))
	require.NotNil(t, got.Solvers)
	require.NotNil(t, got.Solvers.SemtechGnss)
	assert.Equal(t, types.PositionConfigurationStatusEnabled, got.Solvers.SemtechGnss.Status)

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	list, err := client.ListPositionConfigurations(ctx, &iotwirelesssdk.ListPositionConfigurationsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.PositionConfigurationList)

	estimate, err := client.GetPositionEstimate(ctx, &iotwirelesssdk.GetPositionEstimateInput{
		Ip: &types.Ip{IpAddress: aws.String("192.0.2.1")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, estimate.GeoJsonPayload)
}

// testPartnerAccountsRealClient covers AssociateAwsAccountWithPartnerAccount,
// GetPartnerAccount, ListPartnerAccounts, UpdatePartnerAccount,
// DisassociateAwsAccountFromPartnerAccount.
func testPartnerAccountsRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	assoc, err := client.AssociateAwsAccountWithPartnerAccount(
		ctx,
		&iotwirelesssdk.AssociateAwsAccountWithPartnerAccountInput{
			Sidewalk: &types.SidewalkAccountInfo{AmazonId: aws.String("amzn-slice6")},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(assoc.Arn))

	got, err := client.GetPartnerAccount(ctx, &iotwirelesssdk.GetPartnerAccountInput{
		PartnerAccountId: aws.String("amzn-slice6"),
		PartnerType:      types.PartnerTypeSidewalk,
	})
	require.NoError(t, err)
	assert.True(t, got.AccountLinked)

	list, err := client.ListPartnerAccounts(ctx, &iotwirelesssdk.ListPartnerAccountsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.Sidewalk)

	_, err = client.UpdatePartnerAccount(ctx, &iotwirelesssdk.UpdatePartnerAccountInput{
		PartnerAccountId: aws.String("amzn-slice6"),
		PartnerType:      types.PartnerTypeSidewalk,
		Sidewalk:         &types.SidewalkUpdateAccount{},
	})
	require.NoError(t, err)

	_, err = client.DisassociateAwsAccountFromPartnerAccount(
		ctx,
		&iotwirelesssdk.DisassociateAwsAccountFromPartnerAccountInput{
			PartnerAccountId: aws.String("amzn-slice6"),
			PartnerType:      types.PartnerTypeSidewalk,
		},
	)
	require.NoError(t, err)

	got, err = client.GetPartnerAccount(ctx, &iotwirelesssdk.GetPartnerAccountInput{
		PartnerAccountId: aws.String("amzn-slice6"),
		PartnerType:      types.PartnerTypeSidewalk,
	})
	require.NoError(t, err)
	assert.False(t, got.AccountLinked)
}

// testEventConfigurationsExtraRealClient covers GetResourceEventConfiguration,
// UpdateResourceEventConfiguration, ListEventConfigurations.
func testEventConfigurationsExtraRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	dev, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("slice6-event-device"),
		Type:            types.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("slice6-dest"),
	})
	require.NoError(t, err)

	_, err = client.UpdateResourceEventConfiguration(
		ctx,
		&iotwirelesssdk.UpdateResourceEventConfigurationInput{
			Identifier:     dev.Id,
			IdentifierType: types.IdentifierTypeWirelessDeviceId,
			Join: &types.JoinEventConfiguration{
				WirelessDeviceIdEventTopic: types.EventNotificationTopicStatusEnabled,
			},
		},
	)
	require.NoError(t, err)

	got, err := client.GetResourceEventConfiguration(
		ctx,
		&iotwirelesssdk.GetResourceEventConfigurationInput{
			Identifier:     dev.Id,
			IdentifierType: types.IdentifierTypeWirelessDeviceId,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, got.Join)

	list, err := client.ListEventConfigurations(ctx, &iotwirelesssdk.ListEventConfigurationsInput{
		ResourceType: types.EventNotificationResourceTypeWirelessDevice,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, list.EventConfigurationsList)
}

// testLogLevelsRealClient covers GetLogLevelsByResourceTypes,
// UpdateLogLevelsByResourceTypes, GetResourceLogLevel, PutResourceLogLevel,
// ResetResourceLogLevel, ResetAllResourceLogLevels.
func testLogLevelsRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.UpdateLogLevelsByResourceTypes(
		ctx,
		&iotwirelesssdk.UpdateLogLevelsByResourceTypesInput{DefaultLogLevel: types.LogLevelInfo},
	)
	require.NoError(t, err)

	got, err := client.GetLogLevelsByResourceTypes(
		ctx, &iotwirelesssdk.GetLogLevelsByResourceTypesInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, types.LogLevelInfo, got.DefaultLogLevel)

	_, err = client.PutResourceLogLevel(ctx, &iotwirelesssdk.PutResourceLogLevelInput{
		ResourceIdentifier: aws.String("slice6-log-resource"),
		ResourceType:       aws.String("WirelessDevice"),
		LogLevel:           types.LogLevelError,
	})
	require.NoError(t, err)

	logLevel, err := client.GetResourceLogLevel(ctx, &iotwirelesssdk.GetResourceLogLevelInput{
		ResourceIdentifier: aws.String("slice6-log-resource"),
		ResourceType:       aws.String("WirelessDevice"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.LogLevelError, logLevel.LogLevel)

	_, err = client.ResetResourceLogLevel(ctx, &iotwirelesssdk.ResetResourceLogLevelInput{
		ResourceIdentifier: aws.String("slice6-log-resource"),
		ResourceType:       aws.String("WirelessDevice"),
	})
	require.NoError(t, err)

	_, err = client.ResetAllResourceLogLevels(ctx, &iotwirelesssdk.ResetAllResourceLogLevelsInput{})
	require.NoError(t, err)
}

// testMetricsRealClient covers GetMetricConfiguration,
// UpdateMetricConfiguration, GetMetrics.
func testMetricsRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.UpdateMetricConfiguration(ctx, &iotwirelesssdk.UpdateMetricConfigurationInput{
		SummaryMetric: &types.SummaryMetricConfiguration{
			Status: types.SummaryMetricConfigurationStatusEnabled,
		},
	})
	require.NoError(t, err)

	got, err := client.GetMetricConfiguration(ctx, &iotwirelesssdk.GetMetricConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, got.SummaryMetric)
	assert.Equal(t, types.SummaryMetricConfigurationStatusEnabled, got.SummaryMetric.Status)

	metrics, err := client.GetMetrics(ctx, &iotwirelesssdk.GetMetricsInput{
		SummaryMetricQueries: []types.SummaryMetricQuery{
			{QueryId: aws.String("q1"), MetricName: types.MetricNameDeviceUplinkCount},
		},
	})
	require.NoError(t, err)
	require.Len(t, metrics.SummaryMetricQueryResults, 1)
	assert.Equal(t, "q1", aws.ToString(metrics.SummaryMetricQueryResults[0].QueryId))
}

// testTagsRealClient covers TagResource, UntagResource, ListTagsForResource.
func testTagsRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	dest, err := client.CreateDestination(ctx, &iotwirelesssdk.CreateDestinationInput{
		Name:           aws.String("slice6-tag-dest"),
		Expression:     aws.String("rule"),
		ExpressionType: types.ExpressionTypeRuleName,
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/dest-role"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &iotwirelesssdk.TagResourceInput{
		ResourceArn: dest.Arn,
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("slice6")}},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(ctx, &iotwirelesssdk.ListTagsForResourceInput{
		ResourceArn: dest.Arn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))

	_, err = client.UntagResource(ctx, &iotwirelesssdk.UntagResourceInput{
		ResourceArn: dest.Arn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	tagsOut, err = client.ListTagsForResource(ctx, &iotwirelesssdk.ListTagsForResourceInput{
		ResourceArn: dest.Arn,
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut.Tags)
}

// testWirelessDeviceImportTasksRealClient covers StartWirelessDeviceImportTask,
// StartSingleWirelessDeviceImportTask, GetWirelessDeviceImportTask,
// ListWirelessDeviceImportTasks, ListDevicesForWirelessDeviceImportTask,
// UpdateWirelessDeviceImportTask, DeleteWirelessDeviceImportTask.
func testWirelessDeviceImportTasksRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	task, err := client.StartWirelessDeviceImportTask(
		ctx,
		&iotwirelesssdk.StartWirelessDeviceImportTaskInput{
			DestinationName: aws.String("slice6-import-dest"),
			Sidewalk: &types.SidewalkStartImportInfo{
				DeviceCreationFile: aws.String("s3://bucket/devices.csv"),
			},
		},
	)
	require.NoError(t, err)

	got, err := client.GetWirelessDeviceImportTask(
		ctx, &iotwirelesssdk.GetWirelessDeviceImportTaskInput{Id: task.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-import-dest", aws.ToString(got.DestinationName))

	list, err := client.ListWirelessDeviceImportTasks(
		ctx, &iotwirelesssdk.ListWirelessDeviceImportTasksInput{},
	)
	require.NoError(t, err)

	found := false

	for _, it := range list.WirelessDeviceImportTaskList {
		if aws.ToString(it.Id) == aws.ToString(task.Id) {
			found = true
		}
	}

	assert.True(t, found)

	devices, err := client.ListDevicesForWirelessDeviceImportTask(
		ctx, &iotwirelesssdk.ListDevicesForWirelessDeviceImportTaskInput{Id: task.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice6-import-dest", aws.ToString(devices.DestinationName))

	_, err = client.UpdateWirelessDeviceImportTask(
		ctx,
		&iotwirelesssdk.UpdateWirelessDeviceImportTaskInput{
			Id: task.Id,
			Sidewalk: &types.SidewalkUpdateImportInfo{
				DeviceCreationFile: aws.String("s3://bucket/more-devices.csv"),
			},
		},
	)
	require.NoError(t, err)

	single, err := client.StartSingleWirelessDeviceImportTask(
		ctx,
		&iotwirelesssdk.StartSingleWirelessDeviceImportTaskInput{
			DestinationName: aws.String("slice6-import-dest"),
			Sidewalk:        &types.SidewalkSingleStartImportInfo{SidewalkManufacturingSn: aws.String("sn-0001")},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(single.Id))

	_, err = client.DeleteWirelessDeviceImportTask(
		ctx, &iotwirelesssdk.DeleteWirelessDeviceImportTaskInput{Id: task.Id},
	)
	require.NoError(t, err)
}

// testServiceEndpointRealClient covers GetServiceEndpoint.
func testServiceEndpointRealClient(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	out, err := client.GetServiceEndpoint(ctx, &iotwirelesssdk.GetServiceEndpointInput{
		ServiceType: types.WirelessGatewayServiceTypeCups,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.ServiceEndpoint))
	assert.Equal(t, types.WirelessGatewayServiceTypeCups, out.ServiceType)
}
