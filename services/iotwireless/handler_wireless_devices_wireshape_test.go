package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_CreateGetWirelessDevice_LoRaWANSidewalkWireShape drives a real
// HTTP CreateWirelessDevice/GetWirelessDevice round trip with a deeply
// nested LoRaWANDevice/SidewalkCreateWirelessDevice request body, verifying
// the exact wire field names/casing typed in lorawan_types.go (AbpV1_0X,
// SessionKeys, AppSKey, ...) survive JSON marshal/unmarshal through the
// actual HTTP path -- not just a Go-struct-to-Go-struct backend call.
func TestHandler_CreateGetWirelessDevice_LoRaWANSidewalkWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	body := `{
		"Name":"dev1","Type":"LoRaWAN","DestinationName":"dest1",
		"LoRaWAN":{
			"DeviceProfileId":"dp-1",
			"AbpV1_0_x":{"DevAddr":"addr-1","FCntStart":5,"SessionKeys":{"AppSKey":"ask-1","NwkSKey":"nsk-1"}}
		},
		"Sidewalk":{"DeviceProfileId":"sw-dp-1","SidewalkManufacturingSn":"smsn-1"}
	}`

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+createResp.ID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var got struct {
		Sidewalk struct {
			DeviceProfileID         string `json:"DeviceProfileId"`
			SidewalkManufacturingSn string `json:"SidewalkManufacturingSn"`
		} `json:"Sidewalk"`
		LoRaWAN struct {
			DeviceProfileID string `json:"DeviceProfileId"`
			AbpV1_0X        struct {
				SessionKeys struct {
					AppSKey string `json:"AppSKey"`
					NwkSKey string `json:"NwkSKey"`
				} `json:"SessionKeys"`
				DevAddr   string `json:"DevAddr"`
				FCntStart int32  `json:"FCntStart"`
			} `json:"AbpV1_0_x"`
		} `json:"LoRaWAN"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

	assert.Equal(t, "dp-1", got.LoRaWAN.DeviceProfileID)
	assert.Equal(t, "addr-1", got.LoRaWAN.AbpV1_0X.DevAddr)
	assert.Equal(t, int32(5), got.LoRaWAN.AbpV1_0X.FCntStart)
	assert.Equal(t, "ask-1", got.LoRaWAN.AbpV1_0X.SessionKeys.AppSKey)
	assert.Equal(t, "nsk-1", got.LoRaWAN.AbpV1_0X.SessionKeys.NwkSKey)
	assert.Equal(t, "sw-dp-1", got.Sidewalk.DeviceProfileID)
	assert.Equal(t, "smsn-1", got.Sidewalk.SidewalkManufacturingSn)
}

// TestHandler_NetworkAnalyzerConfig_TraceContentWireShape verifies
// TraceContent's LogLevel/MulticastFrameInfo/WirelessDeviceFrameInfo fields
// round-trip through Create/Get, and that Update replaces TraceContent
// wholesale rather than merging (see UpdateNetworkAnalyzerConfig's doc
// comment).
func TestHandler_NetworkAnalyzerConfig_TraceContentWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	body := `{"Name":"nc1","TraceContent":{"LogLevel":"INFO","MulticastFrameInfo":"ENABLED"}}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/network-analyzer-configurations", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	type traceContent struct {
		LogLevel                string `json:"LogLevel"`
		MulticastFrameInfo      string `json:"MulticastFrameInfo"`
		WirelessDeviceFrameInfo string `json:"WirelessDeviceFrameInfo"`
	}

	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations/nc1", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var got struct {
		TraceContent traceContent `json:"TraceContent"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, "INFO", got.TraceContent.LogLevel)
	assert.Equal(t, "ENABLED", got.TraceContent.MulticastFrameInfo)
	assert.Empty(t, got.TraceContent.WirelessDeviceFrameInfo)

	// Update supplies only WirelessDeviceFrameInfo -- since TraceContent's
	// own fields aren't optional pointers, this replaces the whole object.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/network-analyzer-configurations/nc1",
		`{"TraceContent":{"WirelessDeviceFrameInfo":"ENABLED"}}`)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations/nc1", "")
	var got2 struct {
		TraceContent traceContent `json:"TraceContent"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got2))
	assert.Empty(t, got2.TraceContent.LogLevel, "update replaces TraceContent wholesale")
	assert.Equal(t, "ENABLED", got2.TraceContent.WirelessDeviceFrameInfo)
}
