package iotwireless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GatewayTaskLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create gateway
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var gwResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gwResp))
	gwID := gwResp["Id"].(string)

	// Create task definition
	rec = doIoTWRequest(t, h, http.MethodPost, "/wireless-gateway-task-definitions",
		`{"Name":"taskdef1","AutoCreateTasks":false}`)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var defResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &defResp))
	defID := defResp["Id"].(string)
	assert.NotEmpty(t, defID)

	// Create task on gateway
	rec = doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways/"+gwID+"/tasks",
		fmt.Sprintf(`{"WirelessGatewayTaskDefinitionId":%q}`, defID))
	assert.Equal(t, http.StatusCreated, rec.Code)

	var taskResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &taskResp))
	assert.Equal(t, "PENDING", taskResp["Status"])

	// Get task
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/tasks", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getTaskResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getTaskResp))
	assert.Equal(t, "PENDING", getTaskResp["Status"])

	// List task definitions
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get task definition
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions/"+defID, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete task
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateways/"+gwID+"/tasks", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get task after deletion must be a real 404, not a fabricated PENDING
	// placeholder.
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/tasks", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var notFoundResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &notFoundResp))
	msg, _ := notFoundResp["Message"].(string)
	assert.Contains(t, msg, "ResourceNotFoundException")
	assert.NotContains(t, notFoundResp, "Status", "a stub response would fabricate a Status field")

	// Delete task definition
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateway-task-definitions/"+defID, "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestHandler_GetWirelessGatewayTask_NeverCreated verifies that a gateway
// which never had a task returns a real 404 rather than a fabricated PENDING
// task, for a gateway ID that was never associated with any task at all.
func TestHandler_GetWirelessGatewayTask_NeverCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/never-had-a-task/tasks", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListWirelessGatewayTaskDefinitions_ReturnsRealData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		defNames []string
		defCount int
	}{
		{
			name:     "empty_list",
			defCount: 0,
			defNames: []string{},
		},
		{
			name:     "single_definition",
			defCount: 1,
			defNames: []string{"task-def-1"},
		},
		{
			name:     "multiple_definitions",
			defCount: 3,
			defNames: []string{"def-alpha", "def-beta", "def-gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			createdIDs := make([]string, 0, tt.defCount)

			for _, name := range tt.defNames {
				body := `{"Name":"` + name + `","AutoCreateTasks":true}`
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateway-task-definitions", body)
				require.Equal(t, http.StatusCreated, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				id, _ := createResp["Id"].(string)
				require.NotEmpty(t, id)
				createdIDs = append(createdIDs, id)
			}

			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			defs, ok := resp["TaskDefinitions"].([]any)
			require.True(t, ok, "TaskDefinitions should be array")
			assert.Len(t, defs, tt.defCount)

			// Verify each created definition appears in the list.
			foundIDs := make(map[string]bool)
			for _, d := range defs {
				defMap, isMap := d.(map[string]any)
				require.True(t, isMap)
				id, _ := defMap["Id"].(string)
				foundIDs[id] = true
			}

			for _, id := range createdIDs {
				assert.True(t, foundIDs[id], "definition %s should appear in list", id)
			}
		})
	}
}

func TestHandler_GatewayTaskDefinitionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		defName         string
		autoCreateTasks bool
	}{
		{
			name:            "auto_create_true",
			defName:         "gw-task-def-auto",
			autoCreateTasks: true,
		},
		{
			name:            "auto_create_false",
			defName:         "gw-task-def-manual",
			autoCreateTasks: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			autoStr := "false"
			if tt.autoCreateTasks {
				autoStr = "true"
			}

			// Create.
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateway-task-definitions",
				`{"Name":"`+tt.defName+`","AutoCreateTasks":`+autoStr+`}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id := createResp["Id"].(string)
			arn := createResp["Arn"].(string)
			require.NotEmpty(t, id)
			assert.Contains(t, arn, "WirelessGatewayTaskDefinition")

			// Get.
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.defName, getResp["Name"])

			// Delete.
			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateway-task-definitions/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// List after delete should be empty.
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			defs := listResp["TaskDefinitions"].([]any)
			assert.Empty(t, defs)
		})
	}
}

func TestHandler_GatewayTaskDefinitionNotFound_Simple(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// GET /wireless-gateway-task-definitions/{id} with unknown ID must 404.
	rec := doIoTWRequest(t, h, http.MethodGet,
		"/wireless-gateway-task-definitions/no-such-id", "")
	assert.Equal(t, http.StatusNotFound, rec.Code, "unknown task definition must return 404")
}

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
