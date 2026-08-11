package iotwireless_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	iotwirelesstypes "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// newTestIoTWirelessRegistryServer wires the Handler through service.Registry
// and service.NewServiceRouter exactly as production does (provider.go),
// so requests are dispatched by RouteMatcher/ExtractOperation rather than
// calling Handler().Handler() directly. AssociateWirelessDeviceWithFuotaTask,
// AssociateMulticastGroupWithFuotaTask, and AssociateWirelessDeviceWithMulticastGroup
// bind PUT to singular sub-paths ("multicast-group", "wireless-device") that
// a hand-typed httptest path can silently get wrong in the same way the
// routing code did; only a real aws-sdk-go-v2 client generates the path AWS
// actually sends.
func newTestIoTWirelessRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	backend := iotwireless.NewInMemoryBackend()
	handler := iotwireless.NewHandler(backend)
	handler.AccountID = testAccountID
	handler.DefaultRegion = testRegion
	require.NoError(t, registry.Register(handler))

	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func newTestIoTWirelessSDKClient(t *testing.T, baseURL string) *iotwirelesssdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return iotwirelesssdk.NewFromConfig(cfg, func(o *iotwirelesssdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

func TestAssociateOps_RouteThroughRealSDKClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *iotwirelesssdk.Client)
		name string
	}{
		{assertAssociateWirelessDeviceWithFuotaTask, "wireless_device_with_fuota_task"},
		{assertAssociateMulticastGroupWithFuotaTask, "multicast_group_with_fuota_task"},
		{assertAssociateWirelessDeviceWithMulticastGroup, "wireless_device_with_multicast_group"},
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

// assertAssociateWirelessDeviceWithFuotaTask drives PUT
// /fuota-tasks/{Id}/wireless-device (serializers.go:234) through the router.
func assertAssociateWirelessDeviceWithFuotaTask(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	ftOut, err := client.CreateFuotaTask(ctx, &iotwirelesssdk.CreateFuotaTaskInput{
		FirmwareUpdateImage: aws.String("s3://bucket/fw.bin"),
		FirmwareUpdateRole:  aws.String("arn:aws:iam::000000000000:role/r"),
	})
	require.NoError(t, err)

	devOut, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("dev-route-fuota"),
		Type:            iotwirelesstypes.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("route-dest"),
	})
	require.NoError(t, err)

	_, err = client.AssociateWirelessDeviceWithFuotaTask(
		ctx,
		&iotwirelesssdk.AssociateWirelessDeviceWithFuotaTaskInput{
			Id:               ftOut.Id,
			WirelessDeviceId: devOut.Id,
		},
	)
	require.NoError(
		t,
		err,
		"AssociateWirelessDeviceWithFuotaTask must route through the real singular PUT path",
	)

	listOut, err := client.ListWirelessDevices(ctx, &iotwirelesssdk.ListWirelessDevicesInput{
		FuotaTaskId: ftOut.Id,
	})
	require.NoError(t, err)
	if assert.Len(t, listOut.WirelessDeviceList, 1) {
		assert.Equal(t, aws.ToString(devOut.Id), aws.ToString(listOut.WirelessDeviceList[0].Id))
	}
}

// assertAssociateMulticastGroupWithFuotaTask drives PUT
// /fuota-tasks/{Id}/multicast-group (serializers.go:140) through the router.
func assertAssociateMulticastGroupWithFuotaTask(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	ftOut, err := client.CreateFuotaTask(ctx, &iotwirelesssdk.CreateFuotaTaskInput{
		FirmwareUpdateImage: aws.String("s3://bucket/fw.bin"),
		FirmwareUpdateRole:  aws.String("arn:aws:iam::000000000000:role/r"),
	})
	require.NoError(t, err)

	mgOut, err := client.CreateMulticastGroup(ctx, &iotwirelesssdk.CreateMulticastGroupInput{
		Name:    aws.String("mg-route-fuota"),
		LoRaWAN: &iotwirelesstypes.LoRaWANMulticast{},
	})
	require.NoError(t, err)

	_, err = client.AssociateMulticastGroupWithFuotaTask(
		ctx,
		&iotwirelesssdk.AssociateMulticastGroupWithFuotaTaskInput{
			Id:               ftOut.Id,
			MulticastGroupId: mgOut.Id,
		},
	)
	require.NoError(
		t,
		err,
		"AssociateMulticastGroupWithFuotaTask must route through the real singular PUT path",
	)

	listOut, err := client.ListMulticastGroupsByFuotaTask(
		ctx,
		&iotwirelesssdk.ListMulticastGroupsByFuotaTaskInput{
			Id: ftOut.Id,
		},
	)
	require.NoError(t, err)
	if assert.Len(t, listOut.MulticastGroupList, 1) {
		assert.Equal(t, aws.ToString(mgOut.Id), aws.ToString(listOut.MulticastGroupList[0].Id))
	}
}

// assertAssociateWirelessDeviceWithMulticastGroup drives PUT
// /multicast-groups/{Id}/wireless-device (serializers.go:328) through the router.
func assertAssociateWirelessDeviceWithMulticastGroup(t *testing.T, client *iotwirelesssdk.Client) {
	t.Helper()
	ctx := t.Context()

	mgOut, err := client.CreateMulticastGroup(ctx, &iotwirelesssdk.CreateMulticastGroupInput{
		Name:    aws.String("mg-route-device"),
		LoRaWAN: &iotwirelesstypes.LoRaWANMulticast{},
	})
	require.NoError(t, err)

	devOut, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
		Name:            aws.String("dev-route-mg"),
		Type:            iotwirelesstypes.WirelessDeviceTypeLoRaWAN,
		DestinationName: aws.String("route-dest"),
	})
	require.NoError(t, err)

	_, err = client.AssociateWirelessDeviceWithMulticastGroup(
		ctx,
		&iotwirelesssdk.AssociateWirelessDeviceWithMulticastGroupInput{
			Id:               mgOut.Id,
			WirelessDeviceId: devOut.Id,
		},
	)
	require.NoError(
		t,
		err,
		"AssociateWirelessDeviceWithMulticastGroup must route through the real singular PUT path",
	)

	listOut, err := client.ListWirelessDevices(ctx, &iotwirelesssdk.ListWirelessDevicesInput{
		MulticastGroupId: mgOut.Id,
	})
	require.NoError(t, err)
	if assert.Len(t, listOut.WirelessDeviceList, 1) {
		assert.Equal(t, aws.ToString(devOut.Id), aws.ToString(listOut.WirelessDeviceList[0].Id))
	}
}
