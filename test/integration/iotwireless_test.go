//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createIoTWirelessClient returns an IoT Wireless client pointed at the shared test container.
func createIoTWirelessClient(t *testing.T) *iotwirelesssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return iotwirelesssdk.NewFromConfig(cfg, func(o *iotwirelesssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_IoTWireless_ServiceProfileLifecycle tests the full service profile CRUD lifecycle.
func TestIntegration_IoTWireless_ServiceProfileLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
	}{
		{
			name:        "full_lifecycle",
			profileName: "integration-test-profile-" + t.Name(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createIoTWirelessClient(t)

			// Create service profile.
			createOut, err := client.CreateServiceProfile(ctx, &iotwirelesssdk.CreateServiceProfileInput{
				Name: aws.String(tt.profileName),
			})
			require.NoError(t, err, "CreateServiceProfile should succeed")
			require.NotEmpty(t, aws.ToString(createOut.Id))
			profileID := aws.ToString(createOut.Id)

			// List service profiles — should contain the created one.
			listOut, err := client.ListServiceProfiles(ctx, &iotwirelesssdk.ListServiceProfilesInput{})
			require.NoError(t, err, "ListServiceProfiles should succeed")

			found := false
			for _, sp := range listOut.ServiceProfileList {
				if aws.ToString(sp.Id) == profileID {
					found = true

					break
				}
			}

			assert.True(t, found, "created service profile should appear in list")

			// Delete service profile.
			_, err = client.DeleteServiceProfile(ctx, &iotwirelesssdk.DeleteServiceProfileInput{
				Id: aws.String(profileID),
			})
			require.NoError(t, err, "DeleteServiceProfile should succeed")

			// List again — should be gone.
			listOut2, err := client.ListServiceProfiles(ctx, &iotwirelesssdk.ListServiceProfilesInput{})
			require.NoError(t, err, "ListServiceProfiles after delete should succeed")

			for _, sp := range listOut2.ServiceProfileList {
				assert.NotEqual(t, profileID, aws.ToString(sp.Id), "deleted profile should not appear in list")
			}
		})
	}
}

// TestIntegration_IoTWireless_WirelessDevice tests wireless device CRUD lifecycle.
func TestIntegration_IoTWireless_WirelessDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceName string
		devType    iotwirelesssdk.WirelessDeviceType
	}{
		{
			name:       "lorawan_device",
			deviceName: "integration-device-lorawan-" + t.Name(),
			devType:    iotwirelesssdk.WirelessDeviceTypeLoRaWAN,
		},
		{
			name:       "sidewalk_device",
			deviceName: "integration-device-sidewalk-" + t.Name(),
			devType:    iotwirelesssdk.WirelessDeviceTypeSidewalk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createIoTWirelessClient(t)

			// Create wireless device.
			createOut, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
				Name:            aws.String(tt.deviceName),
				Type:            tt.devType,
				DestinationName: aws.String("integration-dest"),
			})
			require.NoError(t, err, "CreateWirelessDevice should succeed")
			require.NotEmpty(t, aws.ToString(createOut.Id))
			deviceID := aws.ToString(createOut.Id)

			// Get wireless device.
			getOut, err := client.GetWirelessDevice(ctx, &iotwirelesssdk.GetWirelessDeviceInput{
				Identifier:     aws.String(deviceID),
				IdentifierType: iotwirelesssdk.WirelessDeviceIdTypeWirelessDeviceId,
			})
			require.NoError(t, err, "GetWirelessDevice should succeed")
			assert.Equal(t, tt.deviceName, aws.ToString(getOut.Name))

			// List wireless devices.
			listOut, err := client.ListWirelessDevices(ctx, &iotwirelesssdk.ListWirelessDevicesInput{})
			require.NoError(t, err, "ListWirelessDevices should succeed")

			found := false
			for _, d := range listOut.WirelessDeviceList {
				if aws.ToString(d.Id) == deviceID {
					found = true

					break
				}
			}

			assert.True(t, found, "created device should appear in list")

			// Delete wireless device.
			_, err = client.DeleteWirelessDevice(ctx, &iotwirelesssdk.DeleteWirelessDeviceInput{
				Id: aws.String(deviceID),
			})
			require.NoError(t, err, "DeleteWirelessDevice should succeed")
		})
	}
}
