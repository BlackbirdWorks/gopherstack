package iotwireless_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListWirelessDevices_Pagination locks in real cursor
// pagination for List* operations: maxResults/nextToken are honored rather
// than silently accepted-and-ignored (see PARITY.md gap: "List* ops always
// return a full single page with no NextToken").
func TestHandler_ListWirelessDevices_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	const total = 5
	for i := range total {
		rec := doIoTWRequest(t, h, "POST", "/wireless-devices",
			`{"Name":"dev-`+strconv.Itoa(i)+`","Type":"LoRaWAN"}`)
		require.Equal(t, 201, rec.Code)
	}

	type listResp struct {
		NextToken          string `json:"NextToken"`
		WirelessDeviceList []struct {
			ID string `json:"Id"`
		} `json:"WirelessDeviceList"`
	}

	seen := map[string]bool{}
	token := ""

	for page := 0; ; page++ {
		require.Lessf(t, page, total+1, "pagination must terminate within %d pages", total)

		path := "/wireless-devices?maxResults=2"
		if token != "" {
			path += "&nextToken=" + token
		}

		rec := doIoTWRequest(t, h, "GET", path, "")
		require.Equal(t, 200, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.WirelessDeviceList), 2, "page size must respect maxResults")

		for _, d := range resp.WirelessDeviceList {
			assert.False(t, seen[d.ID], "no device should be returned twice across pages")
			seen[d.ID] = true
		}

		if resp.NextToken == "" {
			break
		}

		token = resp.NextToken
	}

	assert.Len(t, seen, total, "every device must be reachable across pages")
}
