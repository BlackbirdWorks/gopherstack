package iotwireless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	types "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_DeleteQueuedMessagesByID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deleteByID bool
		wantLeft   int
	}{
		{name: "one_message", deleteByID: true, wantLeft: 1},
		{name: "wildcard_clears_all", wantLeft: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTWirelessSDKClient(t, newTestIoTWirelessRegistryServer(t).URL)
			ctx := t.Context()

			dev, err := client.CreateWirelessDevice(ctx, &iotwirelesssdk.CreateWirelessDeviceInput{
				Name: aws.String("d"), Type: types.WirelessDeviceTypeLoRaWAN, DestinationName: aws.String("dest"),
			})
			require.NoError(t, err)

			ids := make([]string, 0, 2)

			for range 2 {
				sent, sendErr := client.SendDataToWirelessDevice(ctx, &iotwirelesssdk.SendDataToWirelessDeviceInput{
					Id: dev.Id, PayloadData: aws.String("AQIDBA=="), TransmitMode: aws.Int32(0),
					WirelessMetadata: &types.WirelessMetadata{
						LoRaWAN: &types.LoRaWANSendDataToDevice{FPort: aws.Int32(1)},
					},
				})
				require.NoError(t, sendErr)

				ids = append(ids, aws.ToString(sent.MessageId))
			}

			target := "*"
			if tt.deleteByID {
				target = ids[0]
			}

			_, err = client.DeleteQueuedMessages(ctx, &iotwirelesssdk.DeleteQueuedMessagesInput{
				Id: dev.Id, MessageId: aws.String(target),
			})
			require.NoError(t, err)

			left, err := client.ListQueuedMessages(ctx, &iotwirelesssdk.ListQueuedMessagesInput{Id: dev.Id})
			require.NoError(t, err)
			require.Len(t, left.DownlinkQueueMessagesList, tt.wantLeft)

			if tt.deleteByID {
				assert.Equal(t, ids[1], aws.ToString(left.DownlinkQueueMessagesList[0].MessageId))
			}
		})
	}
}

func TestSDK_ListTaskDefinitionsByType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   types.WirelessGatewayTaskDefinitionType
		wantDefs int
	}{
		{name: "no_filter", wantDefs: 1},
		{name: "update", filter: types.WirelessGatewayTaskDefinitionTypeUpdate, wantDefs: 1},
		{name: "unknown_type", filter: "OTHER", wantDefs: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTWirelessSDKClient(t, newTestIoTWirelessRegistryServer(t).URL)
			ctx := t.Context()

			_, err := client.CreateWirelessGatewayTaskDefinition(
				ctx,
				&iotwirelesssdk.CreateWirelessGatewayTaskDefinitionInput{
					Name: aws.String("def"),
					Update: &types.UpdateWirelessGatewayTaskCreate{
						UpdateDataSource: aws.String("s3://bucket/firmware.bin"),
						UpdateDataRole:   aws.String("arn:aws:iam::000000000000:role/update"),
					},
				},
			)
			require.NoError(t, err)

			out, err := client.ListWirelessGatewayTaskDefinitions(
				ctx,
				&iotwirelesssdk.ListWirelessGatewayTaskDefinitionsInput{
					TaskDefinitionType: tt.filter,
				},
			)
			require.NoError(t, err)
			assert.Len(t, out.TaskDefinitions, tt.wantDefs)
		})
	}
}
