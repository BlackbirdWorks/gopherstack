package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetWirelessGatewayTaskDefinition_NotFound verifies that requesting a
// gateway task definition that was never created returns a real
// ResourceNotFoundException (HTTP 404) rather than a fabricated stub body.
func TestGetWirelessGatewayTaskDefinition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions/does-not-exist", "")
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	msg, _ := resp["Message"].(string)
	assert.Contains(t, msg, "ResourceNotFoundException")
	assert.Contains(t, msg, "task definition")

	// A stub response would have returned these placeholder fields; make
	// sure they are genuinely absent from the error body.
	assert.NotContains(t, resp, "Arn")
	assert.NotContains(t, resp, "Name")
}

// TestGetWirelessGatewayTaskDefinition_Found verifies the happy path still
// returns the real, previously-registered task definition.
func TestGetWirelessGatewayTaskDefinition_Found(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateway-task-definitions",
		`{"Name":"real-taskdef","AutoCreateTasks":true}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id, _ := created["Id"].(string)
	require.NotEmpty(t, id)

	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions/"+id, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, "real-taskdef", got["Name"])
	assert.Equal(t, true, got["AutoCreateTasks"])
	assert.NotEmpty(t, got["Arn"])
}
