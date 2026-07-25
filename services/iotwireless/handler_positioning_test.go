package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Position(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Get position (empty): no data ever submitted for this resource.
	rec := doIoTWRequest(t, h, http.MethodGet, "/positions/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	pos, ok := getResp["Position"].([]any)
	require.True(t, ok)
	assert.Empty(t, pos)
	assert.NotContains(
		t,
		getResp,
		"Accuracy",
		"Accuracy must be absent when no position data exists",
	)

	// Update position
	rec = doIoTWRequest(t, h, http.MethodPut, "/positions/resource-123",
		`{"Position":[47.6,-122.3,100.0]}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get position must now reflect the real, previously-submitted
	// coordinates, not a fabricated empty response.
	rec = doIoTWRequest(t, h, http.MethodGet, "/positions/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	pos, ok = getResp["Position"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{47.6, -122.3, 100.0}, pos)

	// Accuracy is an object ({HorizontalAccuracy, VerticalAccuracy}), not a
	// bare scalar -- confirmed against types.Accuracy. Both sub-fields 0.0
	// signal position data is available, per AWS docs.
	accuracy, ok := getResp["Accuracy"].(map[string]any)
	require.True(t, ok, "Accuracy must be an object, not a scalar")
	assert.InDelta(t, 0.0, accuracy["HorizontalAccuracy"], 0.0001)
	assert.InDelta(t, 0.0, accuracy["VerticalAccuracy"], 0.0001)

	// Get position configuration (never set): correct empty shape.
	rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getCfgResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCfgResp))
	assert.NotContains(t, getCfgResp, "Destination")

	// Put position configuration with a real ResourceType and Destination.
	rec = doIoTWRequest(t, h, http.MethodPut,
		"/position-configurations/resource-123?resourceType=WirelessDevice",
		`{"Destination":"my-position-dest","Solvers":{"SemtechGnss":{"Status":"Enabled"}}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get position configuration must now reflect the stored destination.
	rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCfgResp))
	assert.Equal(t, "my-position-dest", getCfgResp["Destination"])
	solvers, ok := getCfgResp["Solvers"].(map[string]any)
	require.True(t, ok)
	semtech, ok := solvers["SemtechGnss"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Semtech", semtech["Provider"])
	assert.Equal(t, "GNSS", semtech["Type"])
	assert.Equal(t, "Enabled", semtech["Status"])

	// List position configurations, filtered by the matching resource type,
	// must reflect the stored entry rather than a hardcoded empty list.
	rec = doIoTWRequest(
		t,
		h,
		http.MethodGet,
		"/position-configurations?resourceType=WirelessDevice",
		"",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok := listResp["PositionConfigurationList"].([]any)
	require.True(t, ok)
	require.Len(t, configs, 1)
	assert.Equal(t, "resource-123", configs[0].(map[string]any)["ResourceIdentifier"])

	// Filtering by a non-matching resource type must exclude the entry.
	rec = doIoTWRequest(
		t,
		h,
		http.MethodGet,
		"/position-configurations?resourceType=WirelessGateway",
		"",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok = listResp["PositionConfigurationList"].([]any)
	require.True(t, ok)
	assert.Empty(t, configs)
}

func TestHandler_ResourcePosition_UpdateAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
	}{
		{name: "device_resource", resourceID: "dev-pos-001"},
		{name: "gateway_resource", resourceID: "gw-pos-001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Update resource position.
			rec := doIoTWRequest(t, h, http.MethodPut, "/resource-positions/"+tt.resourceID,
				`{"GeoJsonPayload":"eyJ0eXBlIjoiUG9pbnQifQ=="}`)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get resource position.
			rec = doIoTWRequest(t, h, http.MethodGet, "/resource-positions/"+tt.resourceID, "")
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_GetPositionEstimate_ReturnsOK(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodPost, "/position-estimate", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PositionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		body       string
		wantKey    string
	}{
		{
			name:       "gnss_solver",
			resourceID: "loc-aaa",
			body:       `{"Destination":"dest-1","Solvers":{"SemtechGnss":{"Status":"Enabled"}}}`,
			wantKey:    "Solvers",
		},
		{
			name:       "combined_solver",
			resourceID: "loc-bbb",
			body:       `{"Destination":"dest-2","Solvers":{"SemtechGnss":{"Status":"Enabled"}}}`,
			wantKey:    "Solvers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Put position config (PUT /position-configurations/{id}).
			rec := doIoTWRequest(t, h, http.MethodPut,
				"/position-configurations/"+tt.resourceID, tt.body)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// Get it back.
			rec = doIoTWRequest(t, h, http.MethodGet,
				"/position-configurations/"+tt.resourceID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp[tt.wantKey]
			assert.True(t, ok, "expected key %q in position config response", tt.wantKey)

			// List — must include this resource.
			rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			cfgs, ok := listResp["PositionConfigurationList"].([]any)
			require.True(t, ok)
			require.Len(t, cfgs, 1)
			cfg0 := cfgs[0].(map[string]any)
			assert.Equal(t, tt.resourceID, cfg0["ResourceIdentifier"])
		})
	}
}
