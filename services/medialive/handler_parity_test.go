package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	return resp
}

// --- Network tests ---

func TestNetwork_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/networks", map[string]any{
		"name":    "net-1",
		"ipPools": []map[string]any{{"cidr": "10.0.0.0/16"}},
		"routes":  []map[string]any{{"cidr": "0.0.0.0/0", "gateway": "10.0.0.1"}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	id := created["id"].(string)
	assert.Equal(t, "ACTIVE", created["state"])
	assert.NotEmpty(t, created["ipPools"])

	rec = doRequest(t, h, http.MethodGet, "/prod/networks/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, "/prod/networks/"+id, map[string]any{"name": "net-1-upd"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "net-1-upd", decodeBody(t, rec.Body.Bytes())["name"])

	rec = doRequest(t, h, http.MethodGet, "/prod/networks", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody(t, rec.Body.Bytes())["networks"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, "/prod/networks/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/networks/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestNetwork_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name: "create without name", method: http.MethodPost, path: "/prod/networks",
			body: map[string]any{}, wantCode: http.StatusBadRequest,
		},
		{
			name: "describe missing", method: http.MethodGet,
			path: "/prod/networks/missing", wantCode: http.StatusNotFound,
		},
		{
			name: "update missing", method: http.MethodPut, path: "/prod/networks/missing",
			body: map[string]any{"name": "x"}, wantCode: http.StatusNotFound,
		},
		{
			name: "delete missing", method: http.MethodDelete,
			path: "/prod/networks/missing", wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- SdiSource tests ---

func TestSdiSource_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/sdiSources", map[string]any{
		"name": "sdi-1", "type": "SINGLE", "mode": "QUADRANT",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())["sdiSource"].(map[string]any)
	id := created["id"].(string)
	assert.Equal(t, "IDLE", created["state"])

	rec = doRequest(t, h, http.MethodGet, "/prod/sdiSources/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, "/prod/sdiSources/"+id, map[string]any{"name": "sdi-upd"})
	require.Equal(t, http.StatusOK, rec.Code)
	got := decodeBody(t, rec.Body.Bytes())["sdiSource"].(map[string]any)
	assert.Equal(t, "sdi-upd", got["name"])

	rec = doRequest(t, h, http.MethodGet, "/prod/sdiSources", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody(t, rec.Body.Bytes())["sdiSources"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, "/prod/sdiSources/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/sdiSources/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSdiSource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name: "create without name", method: http.MethodPost, path: "/prod/sdiSources",
			body: map[string]any{}, wantCode: http.StatusBadRequest,
		},
		{
			name: "describe missing", method: http.MethodGet,
			path: "/prod/sdiSources/missing", wantCode: http.StatusNotFound,
		},
		{
			name: "delete missing", method: http.MethodDelete,
			path: "/prod/sdiSources/missing", wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- ChannelPlacementGroup tests ---

func TestChannelPlacementGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{"name": "clu-1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	clusterID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	base := "/prod/clusters/" + clusterID + "/channelplacementgroups"
	rec = doRequest(t, h, http.MethodPost, base, map[string]any{
		"name": "cpg-1", "nodes": []string{"node-a"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	groupID := created["id"].(string)
	assert.Equal(t, "UNASSIGNED", created["state"])
	assert.Equal(t, clusterID, created["clusterId"])

	rec = doRequest(t, h, http.MethodGet, base+"/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, base+"/"+groupID, map[string]any{"name": "cpg-upd"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "cpg-upd", decodeBody(t, rec.Body.Bytes())["name"])

	rec = doRequest(t, h, http.MethodGet, base, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody(t, rec.Body.Bytes())["channelPlacementGroups"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, base+"/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, base+"/"+groupID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestChannelPlacementGroup_ClusterNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name: "create under missing cluster", method: http.MethodPost,
			path: "/prod/clusters/missing/channelplacementgroups",
			body: map[string]any{"name": "x"}, wantCode: http.StatusNotFound,
		},
		{
			name: "list under missing cluster", method: http.MethodGet,
			path: "/prod/clusters/missing/channelplacementgroups", wantCode: http.StatusNotFound,
		},
		{
			name: "describe missing group", method: http.MethodGet,
			path: "/prod/clusters/missing/channelplacementgroups/g1", wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- Account configuration tests ---

func TestAccountConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/prod/accountConfiguration", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	cfg := decodeBody(t, rec.Body.Bytes())["accountConfiguration"].(map[string]any)
	assert.Empty(t, cfg["kmsKeyId"])

	rec = doRequest(t, h, http.MethodPut, "/prod/accountConfiguration", map[string]any{
		"accountConfiguration": map[string]any{"kmsKeyId": "key-123"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	cfg = decodeBody(t, rec.Body.Bytes())["accountConfiguration"].(map[string]any)
	assert.Equal(t, "key-123", cfg["kmsKeyId"])

	rec = doRequest(t, h, http.MethodGet, "/prod/accountConfiguration", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	cfg = decodeBody(t, rec.Body.Bytes())["accountConfiguration"].(map[string]any)
	assert.Equal(t, "key-123", cfg["kmsKeyId"])
}

// --- Schedule tests ---

func TestSchedule_DescribeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Seed an action via BatchUpdateSchedule.
	rec := doRequest(t, h, http.MethodPut, "/prod/channels/"+channelID+"/schedule", map[string]any{
		"creates": map[string]any{
			"scheduleActions": []map[string]any{
				{"actionName": "a1", "scheduleActionSettings": map[string]any{}},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID+"/schedule", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["scheduleActions"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/channels/"+channelID+"/schedule", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/missing/schedule", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Alerts and versions tests ---

func TestAlertsAndVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID+"/alerts", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["alerts"])

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/missing/alerts", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	versions := decodeBody(t, rec.Body.Bytes())["versions"].([]any)
	require.NotEmpty(t, versions)
	first := versions[0].(map[string]any)
	assert.NotEmpty(t, first["version"])
	_, hasExpiration := first["expirationDate"]
	assert.False(t, hasExpiration, "empty expirationDate must be omitted, not emitted as \"\"")
}

func TestMultiplexAlerts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/multiplexes", map[string]any{
		"name":              "mux-1",
		"availabilityZones": []string{"us-east-1a", "us-east-1b"},
		"multiplexSettings": map[string]any{
			"transportStreamBitrate": 1000000,
			"transportStreamId":      1,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	mux := created["multiplex"].(map[string]any)
	muxID := mux["id"].(string)

	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+muxID+"/alerts", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["alerts"])

	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/missing/alerts", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Channel lifecycle extras tests ---

func TestChannelLifecycleExtras(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/prod/channels/"+channelID+"/channelClass",
		map[string]any{
			"channelClass": "SINGLE_PIPELINE",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	ch := decodeBody(t, rec.Body.Bytes())["channel"].(map[string]any)
	assert.Equal(t, "SINGLE_PIPELINE", ch["channelClass"])

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/channels/"+channelID+"/restartChannelPipelines",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID+"/thumbnails", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["ThumbnailDetails"])

	rec = doRequest(t, h, http.MethodPut, "/prod/channels/missing/channelClass", map[string]any{
		"channelClass": "STANDARD",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- InputDevice lifecycle extras tests ---

func TestInputDeviceLifecycleExtras(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/claimDevice",
		map[string]any{"Id": "hd-device-1"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	for _, action := range []string{"start", "stop", "startInputDeviceMaintenanceWindow"} {
		rec = doRequest(t, h, http.MethodPost, "/prod/inputDevices/hd-device-1/"+action, nil)
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doRequest(t, h, http.MethodGet, "/prod/inputDevices/hd-device-1/thumbnailData", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/prod/inputDevices/missing/start", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/inputDevices/missing/thumbnailData", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- StartDeleteMonitorDeployment + PartnerInput tests ---

func TestStartDeleteMonitorDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{"name": "sm-1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	id := created["id"].(string)
	assert.NotEmpty(t, created["createdAt"])
	assert.NotEmpty(t, created["modifiedAt"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/"+id+"/monitor-deployment", nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "DELETING", decodeBody(t, rec.Body.Bytes())["monitorDeploymentStatus"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/missing/monitor-deployment", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreatePartnerInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/inputs", map[string]any{
		"name": "primary", "type": "UDP_PUSH",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	inputID := decodeBody(t, rec.Body.Bytes())["input"].(map[string]any)["id"].(string)

	rec = doRequest(t, h, http.MethodPost, "/prod/inputs/"+inputID+"/partners", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)
	partner := decodeBody(t, rec.Body.Bytes())["input"].(map[string]any)
	assert.NotEmpty(t, partner["id"])
	assert.NotEqual(t, inputID, partner["id"])

	rec = doRequest(t, h, http.MethodPost, "/prod/inputs/missing/partners", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
