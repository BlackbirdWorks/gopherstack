package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestInMemoryBackend_WirelessDeviceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deviceName  string
		devType     string
		destination string
		description string
		wantErr     bool
	}{
		{
			name:        "create_and_get",
			deviceName:  "device-1",
			devType:     "LoRaWAN",
			destination: "dest-1",
			description: "test device",
		},
		{
			name:    "get_nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			if tt.wantErr {
				_, err := bk.GetWirelessDevice(testAccountID, testRegion, "no-such-id")
				require.Error(t, err)

				return
			}

			d, err := bk.CreateWirelessDevice(
				testAccountID,
				testRegion,
				tt.deviceName,
				tt.devType,
				tt.destination,
				tt.description,
				"",
				nil,
				nil,
				map[string]string{"env": "test"},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.deviceName, d.Name)
			assert.Equal(t, tt.devType, d.Type)
			assert.NotEmpty(t, d.ID)
			assert.NotEmpty(t, d.ARN)
			assert.Equal(t, "test", d.Tags["env"])

			got, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
			require.NoError(t, err)
			assert.Equal(t, d.ID, got.ID)
			assert.Equal(t, tt.deviceName, got.Name)

			err = bk.DeleteWirelessDevice(testAccountID, testRegion, d.ID)
			require.NoError(t, err)

			_, err = bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_WirelessDevice_DeleteNotFound(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	err := bk.DeleteWirelessDevice(testAccountID, testRegion, "no-such-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrDeviceNotFound)
}

func TestInMemoryBackend_ListWirelessDevices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deviceNames []string
		wantCount   int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name:        "multiple",
			deviceNames: []string{"d1", "d2", "d3"},
			wantCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			for _, name := range tt.deviceNames {
				_, err := bk.CreateWirelessDevice(testAccountID, testRegion, name, "LoRaWAN", "", "", "", nil, nil, nil)
				require.NoError(t, err)
			}

			devices := bk.ListWirelessDevices(testAccountID, testRegion)
			assert.Len(t, devices, tt.wantCount)
		})
	}
}

// TestInMemoryBackend_SortedListWirelessDevices verifies deterministic sort order.
func TestInMemoryBackend_SortedListWirelessDevices(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"zebra", "alpha", "mango"} {
		_, err := b.CreateWirelessDevice(testAccountID, testRegion, name, "LoRaWAN", "", "", "", nil, nil, nil)
		require.NoError(t, err)
	}

	devices := b.ListWirelessDevices(testAccountID, testRegion)
	require.Len(t, devices, 3)
	assert.Equal(t, "alpha", devices[0].Name)
	assert.Equal(t, "mango", devices[1].Name)
	assert.Equal(t, "zebra", devices[2].Name)
}
