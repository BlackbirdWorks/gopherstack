package iotwireless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	iotwirelesstypes "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/stretchr/testify/require"
)

// TestUpdateVerbOps_RouteThroughRealSDKClient drives UpdatePosition,
// UpdateResourcePosition, and UpdateEventConfigurationByResourceTypes
// through the real router (newTestIoTWirelessRegistryServer, shared with
// routing_associate_test.go). All three bind PATCH in iotwireless@v1.59.4
// (serializers.go:8146, :8927, :9159) while routing previously matched
// PUT or POST, so a real client's request never reached the handler.
func TestUpdateVerbOps_RouteThroughRealSDKClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *iotwirelesssdk.Client)
		name string
	}{
		{assertUpdatePosition, "update_position"},
		{assertUpdateResourcePosition, "update_resource_position"},
		{assertUpdateEventConfigurationByResourceTypes, "update_event_configuration_by_resource_types"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestIoTWirelessRegistryServer(t)
			client := newTestIoTWirelessSDKClient(t, srv.URL)
			tt.run(t, client)
		})
	}
}

// assertUpdatePosition drives PATCH /positions/{ResourceIdentifier}
// (serializers.go:8924) through the router.
func assertUpdatePosition(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	devOut, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("dev-update-position"),
		Type:            iotwirelesstypes.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("route-dest"),
	})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	_, err = client.UpdatePosition(ctx, &iotwirelesssdk.UpdatePositionInput{
		ResourceIdentifier: devOut.Id,
		ResourceType:       iotwirelesstypes.PositionResourceTypeWirelessDevice,
		Position:           []float32{47.6, -122.3, 100.0},
	})
	require.NoError(t, err, "UpdatePosition must route through the real PATCH path")

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	getOut, err := client.GetPosition(ctx, &iotwirelesssdk.GetPositionInput{
		ResourceIdentifier: devOut.Id,
		ResourceType:       iotwirelesstypes.PositionResourceTypeWirelessDevice,
	})
	require.NoError(t, err)
	require.Equal(t, []float32{47.6, -122.3, 100.0}, getOut.Position)
}

// assertUpdateResourcePosition drives PATCH
// /resource-positions/{ResourceIdentifier} (serializers.go:9156) through the
// router.
func assertUpdateResourcePosition(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	devOut, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("dev-update-resource-position"),
		Type:            iotwirelesstypes.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("route-dest"),
	})
	require.NoError(t, err)

	payload := []byte(`{"type":"Point","coordinates":[47.6,-122.3]}`)

	_, err = client.UpdateResourcePosition(ctx, &iotwirelesssdk.UpdateResourcePositionInput{
		ResourceIdentifier: devOut.Id,
		ResourceType:       iotwirelesstypes.PositionResourceTypeWirelessDevice,
		GeoJsonPayload:     payload,
	})
	require.NoError(t, err, "UpdateResourcePosition must route through the real PATCH path")

	getOut, err := client.GetResourcePosition(ctx, &iotwirelesssdk.GetResourcePositionInput{
		ResourceIdentifier: devOut.Id,
		ResourceType:       iotwirelesstypes.PositionResourceTypeWirelessDevice,
	})
	require.NoError(t, err)
	require.Equal(t, payload, getOut.GeoJsonPayload)
}

// assertUpdateEventConfigurationByResourceTypes drives PATCH
// /event-configurations-resource-types (serializers.go:8143) through the
// router.
func assertUpdateEventConfigurationByResourceTypes(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.UpdateEventConfigurationByResourceTypes(
		ctx,
		&iotwirelesssdk.UpdateEventConfigurationByResourceTypesInput{
			DeviceRegistrationState: &iotwirelesstypes.DeviceRegistrationStateResourceTypeEventConfiguration{
				Sidewalk: &iotwirelesstypes.SidewalkResourceTypeEventConfiguration{
					WirelessDeviceEventTopic: iotwirelesstypes.EventNotificationTopicStatusEnabled,
				},
			},
		},
	)
	require.NoError(
		t,
		err,
		"UpdateEventConfigurationByResourceTypes must route through the real PATCH path",
	)

	getOut, err := client.GetEventConfigurationByResourceTypes(
		ctx,
		&iotwirelesssdk.GetEventConfigurationByResourceTypesInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, getOut.DeviceRegistrationState)
	require.NotNil(t, getOut.DeviceRegistrationState.Sidewalk)
	require.Equal(
		t,
		iotwirelesstypes.EventNotificationTopicStatusEnabled,
		getOut.DeviceRegistrationState.Sidewalk.WirelessDeviceEventTopic,
	)
}
