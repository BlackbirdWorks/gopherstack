package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// listWirelessDevicesNames drives a real HTTP GET against /wireless-devices
// with the given raw query string, so query-string parsing bugs (not just
// backend-filter bugs) get caught.
func listWirelessDevicesNames(t *testing.T, h *iotwireless.Handler, rawQuery string) []string {
	t.Helper()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices"+rawQuery, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		WirelessDeviceList []struct {
			Name string `json:"Name"`
		} `json:"WirelessDeviceList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	names := make([]string, 0, len(resp.WirelessDeviceList))
	for _, d := range resp.WirelessDeviceList {
		names = append(names, d.Name)
	}

	return names
}

func TestHandler_ListWirelessDevices_Filters(t *testing.T) {
	t.Parallel()

	dpA, dpB := "dp-a", "dp-b"
	spA := "sp-a"
	sidewalkDP := "sidewalk-dp"

	h := newTestHandlerHTTP()

	devA, err := h.Backend.CreateWirelessDevice(
		testAccountID, testRegion, "dev-a", "LoRaWAN", "dest-a", "", "",
		&iotwireless.LoRaWANDevice{DeviceProfileID: &dpA, ServiceProfileID: &spA}, nil, nil,
	)
	require.NoError(t, err)

	devB, err := h.Backend.CreateWirelessDevice(
		testAccountID, testRegion, "dev-b", "LoRaWAN", "dest-b", "", "",
		&iotwireless.LoRaWANDevice{DeviceProfileID: &dpB}, nil, nil,
	)
	require.NoError(t, err)

	_, err = h.Backend.CreateWirelessDevice(
		testAccountID, testRegion, "dev-sidewalk", "Sidewalk", "dest-a", "", "",
		nil, &iotwireless.SidewalkCreateWirelessDevice{DeviceProfileID: &sidewalkDP}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, h.Backend.AssociateWirelessDeviceWithFuotaTask("ft-1", devA.ID))
	require.NoError(t, h.Backend.AssociateWirelessDeviceWithMulticastGroup("mg-1", devB.ID))

	tests := []struct {
		name      string
		rawQuery  string
		wantNames []string
	}{
		{
			name:      "no_filter_returns_all",
			rawQuery:  "",
			wantNames: []string{"dev-a", "dev-b", "dev-sidewalk"},
		},
		{
			name:      "destination_name",
			rawQuery:  "?destinationName=dest-a",
			wantNames: []string{"dev-a", "dev-sidewalk"},
		},
		{
			name:      "device_profile_id_lorawan",
			rawQuery:  "?deviceProfileId=dp-a",
			wantNames: []string{"dev-a"},
		},
		{
			name:      "device_profile_id_sidewalk",
			rawQuery:  "?deviceProfileId=sidewalk-dp",
			wantNames: []string{"dev-sidewalk"},
		},
		{
			name:      "service_profile_id",
			rawQuery:  "?serviceProfileId=sp-a",
			wantNames: []string{"dev-a"},
		},
		{
			name:      "wireless_device_type",
			rawQuery:  "?wirelessDeviceType=Sidewalk",
			wantNames: []string{"dev-sidewalk"},
		},
		{
			name:      "fuota_task_id",
			rawQuery:  "?fuotaTaskId=ft-1",
			wantNames: []string{"dev-a"},
		},
		{
			name:      "multicast_group_id",
			rawQuery:  "?multicastGroupId=mg-1",
			wantNames: []string{"dev-b"},
		},
		{
			name:      "empty_filter_value_is_ignored",
			rawQuery:  "?destinationName=",
			wantNames: []string{"dev-a", "dev-b", "dev-sidewalk"},
		},
		{
			name:      "unmatched_value_returns_empty",
			rawQuery:  "?destinationName=no-such-destination",
			wantNames: []string{},
		},
		{
			name:      "two_filters_combine_with_and",
			rawQuery:  "?destinationName=dest-a&wirelessDeviceType=LoRaWAN",
			wantNames: []string{"dev-a"},
		},
		{
			name:      "two_filters_combine_with_and_no_match",
			rawQuery:  "?destinationName=dest-b&wirelessDeviceType=Sidewalk",
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := listWirelessDevicesNames(t, h, tt.rawQuery)
			assert.ElementsMatch(t, tt.wantNames, got)
		})
	}
}
