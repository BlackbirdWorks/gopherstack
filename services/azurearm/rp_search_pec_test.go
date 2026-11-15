package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchPrivateEndpointConnections(t *testing.T) {
	t.Parallel()

	e := newPEEnv(t)
	e.handler.Registry.SetSearch(newFakeSearchPlane())

	svcID := netProv("Microsoft.Search", "searchServices", "srch1")
	code, _ := e.rawJSON(t, http.MethodPut, svcID+"?api-version=2025-05-01",
		`{"location":"westus","sku":{"name":"basic"}}`)
	require.Less(t, code, 300)

	pe, err := e.endpoint(t, "pe1", true, svcID, "searchService")
	require.NoError(t, err)

	state := pe.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState
	assert.Equal(t, "Pending", *state.Status)

	_, list := e.rawJSON(t, http.MethodGet, svcID+"/privateEndpointConnections?api-version=2025-05-01", "")
	items, _ := list["value"].([]any)
	require.Len(t, items, 1)

	item, _ := items[0].(map[string]any)
	name, _ := item["name"].(string)

	code, _ = e.rawJSON(t, http.MethodPut, svcID+"/privateEndpointConnections/"+name+"?api-version=2025-05-01",
		`{"properties":{"privateLinkServiceConnectionState":{"status":"Approved","description":"ok"}}}`)
	require.Equal(t, http.StatusOK, code)

	_, body := e.rawJSON(t, http.MethodGet, svcID+"?api-version=2025-05-01", "")
	props, _ := body["properties"].(map[string]any)
	pecs, _ := props["privateEndpointConnections"].([]any)
	require.Len(t, pecs, 1, "the service body lists its connections")
}
