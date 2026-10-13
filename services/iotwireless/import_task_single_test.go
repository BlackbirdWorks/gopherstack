package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleImportTaskRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantPositioning string
		wantSidewalkDst string
		wantTagValue    string
	}{
		{
			name: "details_and_tags",
			body: `{"DestinationName":"d1","DeviceName":"dev","Positioning":"Enabled",` +
				`"Sidewalk":{"SidewalkManufacturingSn":"sn","Positioning":{"DestinationName":"pos"}},` +
				`"Tags":[{"Key":"k","Value":"v"}]}`,
			wantPositioning: "Enabled",
			wantSidewalkDst: "pos",
			wantTagValue:    "v",
		},
		{
			name: "minimal",
			body: `{"DestinationName":"d1"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_single_device_import_task", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var start map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &start))

			id, _ := start["Id"].(string)
			arn, _ := start["Arn"].(string)

			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, "d1", got["DestinationName"])
			assert.Equal(t, tt.wantPositioning, orEmpty(got["Positioning"]))

			sidewalk, _ := got["Sidewalk"].(map[string]any)
			pos, _ := sidewalk["Positioning"].(map[string]any)
			assert.Equal(t, tt.wantSidewalkDst, orEmpty(pos["DestinationName"]))

			rec = doIoTWRequest(t, h, http.MethodGet, "/tags?resourceArn="+url.QueryEscape(arn), "")
			require.Equal(t, http.StatusOK, rec.Code)

			var tagResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagResp))

			if tt.wantTagValue == "" {
				assert.Empty(t, tagResp.Tags)
			} else {
				require.Len(t, tagResp.Tags, 1)
				assert.Equal(t, tt.wantTagValue, tagResp.Tags[0].Value)
			}

			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless_device_import_task/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func orEmpty(v any) string {
	s, _ := v.(string)

	return s
}

func TestWirelessDeviceTypeParamValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "list_bad_type",
			method:     http.MethodGet,
			path:       "/wireless-devices/x/data?WirelessDeviceType=Bogus",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_bad_type",
			method:     http.MethodDelete,
			path:       "/wireless-devices/x/data?messageId=m&WirelessDeviceType=Bogus",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "deregister_bad_type",
			method:     http.MethodPatch,
			path:       "/wireless-devices/x/deregister?WirelessDeviceType=Bogus",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list_valid_type",
			method:     http.MethodGet,
			path:       "/wireless-devices/x/data?WirelessDeviceType=LoRaWAN",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, tt.method, tt.path, "")
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
