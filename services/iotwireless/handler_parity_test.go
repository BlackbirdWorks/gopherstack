package iotwireless_test

// handler_parity_test.go — AWS-accuracy audit tests covering parity gaps addressed in go-f2mk.
// All tests are table-driven and parallel.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// ============================================================
// Import Task — create, get, update, delete, list
// ============================================================

func TestParity_StartWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantDestination string
		wantStatus      int
	}{
		{
			name:            "with_destination",
			body:            `{"DestinationName":"dest-a"}`,
			wantStatus:      http.StatusCreated,
			wantDestination: "dest-a",
		},
		{
			name:       "empty_destination",
			body:       `{}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:            "full_body",
			body:            `{"DestinationName":"my-dest","Sidewalk":{}}`,
			wantStatus:      http.StatusCreated,
			wantDestination: "my-dest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arn, _ := resp["Arn"].(string)
			id, _ := resp["Id"].(string)
			assert.NotEmpty(t, arn)
			assert.NotEmpty(t, id)
			assert.Contains(t, arn, "ImportTask")
		})
	}
}

func TestParity_GetWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDest  string
		id         string
		wantStatus int
		wantFound  bool
	}{
		{
			name:       "existing_task",
			setupDest:  "my-dest",
			wantStatus: http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "not_found",
			id:         "nonexistent-id",
			wantStatus: http.StatusNotFound,
			wantFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			taskID := tt.id
			if tt.setupDest != "" {
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task",
					`{"DestinationName":"`+tt.setupDest+`"}`)
				require.Equal(t, http.StatusCreated, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				taskID = createResp["Id"].(string)
			}

			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+taskID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFound {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.setupDest, resp["DestinationName"])
				assert.Equal(t, "Initialized", resp["Status"])
			}
		})
	}
}

func TestParity_DeleteWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupTask  bool
		wantStatus int
	}{
		{
			name:       "existing_task",
			setupTask:  true,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			setupTask:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			taskID := "nonexistent-task-id"
			if tt.setupTask {
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task", `{"DestinationName":"d"}`)
				require.Equal(t, http.StatusCreated, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				taskID = createResp["Id"].(string)
			}

			rec := doIoTWRequest(t, h, http.MethodDelete, "/wireless_device_import_task/"+taskID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.setupTask {
				// Get after delete returns 404.
				rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+taskID, "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}

func TestParity_UpdateWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialDest string
		updatedDest string
		wantNewDest string
		wantStatus  int
	}{
		{
			name:        "update_destination",
			initialDest: "dest-old",
			updatedDest: "dest-new",
			wantStatus:  http.StatusNoContent,
			wantNewDest: "dest-new",
		},
		{
			name:        "empty_update_preserves",
			initialDest: "dest-initial",
			updatedDest: "",
			wantStatus:  http.StatusNoContent,
			wantNewDest: "dest-initial",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task",
				`{"DestinationName":"`+tt.initialDest+`"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			taskID := createResp["Id"].(string)

			var body string
			if tt.updatedDest != "" {
				body = `{"DestinationName":"` + tt.updatedDest + `"}`
			} else {
				body = `{}`
			}

			rec = doIoTWRequest(t, h, http.MethodPatch, "/wireless_device_import_task/"+taskID, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify via Get.
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+taskID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.wantNewDest, getResp["DestinationName"])
		})
	}
}

func TestParity_ListWirelessDeviceImportTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		taskCount int
	}{
		{name: "empty", taskCount: 0},
		{name: "one_task", taskCount: 1},
		{name: "multiple_tasks", taskCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			for i := range tt.taskCount {
				body := `{"DestinationName":"dest-` + string(rune('a'+i)) + `"}`
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task", body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_tasks", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			list, ok := resp["WirelessDeviceImportTaskList"].([]any)
			require.True(t, ok, "WirelessDeviceImportTaskList should be array")
			assert.Len(t, list, tt.taskCount)
		})
	}
}

func TestParity_StartSingleWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "basic_create",
			body:       `{"DestinationName":"d1"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "no_destination",
			body:       `{}`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_single_device_import_task", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arn, _ := resp["Arn"].(string)
			devID, _ := resp["WirelessDeviceId"].(string)
			assert.NotEmpty(t, arn)
			assert.NotEmpty(t, devID)
			assert.Contains(t, arn, "ImportTask")
		})
	}
}

func TestParity_ListDevicesForWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	list, ok := resp["ImportedWirelessDeviceList"].([]any)
	require.True(t, ok, "ImportedWirelessDeviceList should be array")
	assert.Empty(t, list)
}

// ============================================================
// sendData — UUID messageID uniqueness
// ============================================================

func TestParity_SendDataToWirelessDevice_UniqueMessageID(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	dev := setupWirelessDevice(t, h, "test-device", "LoRaWAN", "dest")

	seen := make(map[string]bool)

	for range 5 {
		rec := doIoTWRequest(t, h, http.MethodPost,
			"/wireless-devices/"+dev+"/data",
			`{"PayloadData":"aGVsbG8=","WirelessMetadata":{}}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		msgID, _ := resp["MessageId"].(string)
		assert.NotEmpty(t, msgID, "MessageId should not be empty")
		assert.False(t, seen[msgID], "MessageId should be unique: %s", msgID)
		seen[msgID] = true
	}
}

func TestParity_SendDataToMulticastGroup_UniqueMessageID(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a multicast group first.
	rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"mg-send"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	mgID := createResp["Id"].(string)

	seen := make(map[string]bool)

	for range 5 {
		rec = doIoTWRequest(t, h, http.MethodPost,
			"/multicast-groups/"+mgID+"/data",
			`{"PayloadData":"aGVsbG8="}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		msgID, _ := resp["MessageId"].(string)
		assert.NotEmpty(t, msgID)
		assert.False(t, seen[msgID], "MessageId should be unique: %s", msgID)
		seen[msgID] = true
	}
}

// ============================================================
// GatewayTaskDefinition list — returns real stored data
// ============================================================

func TestParity_ListWirelessGatewayTaskDefinitions_ReturnsRealData(t *testing.T) {
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
				defMap, ok := d.(map[string]any)
				require.True(t, ok)
				id, _ := defMap["Id"].(string)
				foundIDs[id] = true
			}

			for _, id := range createdIDs {
				assert.True(t, foundIDs[id], "definition %s should appear in list", id)
			}
		})
	}
}

func TestParity_GatewayTaskDefinitionCRUD(t *testing.T) {
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

// ============================================================
// Persistence — snapshot/restore covers multicast groups, network analyzer configs, import tasks
// ============================================================

func TestParity_Snapshot_IncludesMulticastGroups(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	mg1, err := b.CreateMulticastGroup(
		testAccountID,
		testRegion,
		"mg-snap-1",
		"desc1",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	mg2, err := b.CreateMulticastGroup(testAccountID, testRegion, "mg-snap-2", "desc2", nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	got1, err := b2.GetMulticastGroup(testAccountID, testRegion, mg1.ID)
	require.NoError(t, err)
	assert.Equal(t, mg1.Name, got1.Name)
	assert.Equal(t, "desc1", got1.Description)
	assert.Equal(t, "test", got1.Tags["env"])

	got2, err := b2.GetMulticastGroup(testAccountID, testRegion, mg2.ID)
	require.NoError(t, err)
	assert.Equal(t, mg2.Name, got2.Name)
}

func TestParity_Snapshot_IncludesNetworkAnalyzerConfigs(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	nc, err := b.CreateNetworkAnalyzerConfig(
		testAccountID, testRegion,
		"nc-snap-1", "my network analyzer",
		[]string{"dev-1", "dev-2"},
		[]string{"gw-1"},
		map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	got, err := b2.GetNetworkAnalyzerConfig(testAccountID, testRegion, nc.Name)
	require.NoError(t, err)
	assert.Equal(t, nc.Name, got.Name)
	assert.Equal(t, "my network analyzer", got.Description)
	assert.Equal(t, []string{"dev-1", "dev-2"}, got.WirelessDevices)
	assert.Equal(t, []string{"gw-1"}, got.WirelessGateways)
	assert.Equal(t, "prod", got.Tags["env"])
}

func TestParity_Snapshot_IncludesImportTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	task, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "snap-dest")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	got, err := b2.GetWirelessDeviceImportTask(task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, "snap-dest", got.DestinationName)
	assert.Equal(t, "Initialized", got.Status)
}

func TestParity_Snapshot_IncludesLogLevels(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	require.NoError(t, b.UpdateLogLevelsByResourceTypes("ERROR"))
	require.NoError(t, b.PutResourceLogLevel("res-001", "DEBUG"))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, "ERROR", b2.GetLogLevelsByResourceTypes())
	assert.Equal(t, "DEBUG", b2.GetResourceLogLevel("res-001"))
}

func TestParity_SnapshotRestore_FullRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	// Populate a variety of resource types.
	dev, err := b.CreateWirelessDevice(
		testAccountID, testRegion, "snap-dev", "LoRaWAN", "dest", "desc", map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	gw, err := b.CreateWirelessGateway(testAccountID, testRegion, "snap-gw", "a gateway", nil)
	require.NoError(t, err)

	mg, err := b.CreateMulticastGroup(testAccountID, testRegion, "snap-mg", "", nil)
	require.NoError(t, err)

	nc, err := b.CreateNetworkAnalyzerConfig(testAccountID, testRegion, "snap-nc", "", nil, nil, nil)
	require.NoError(t, err)

	it, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "snap-dest")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	// Verify all resources survived.
	gotDev, err := b2.GetWirelessDevice(testAccountID, testRegion, dev.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-dev", gotDev.Name)
	assert.Equal(t, "v", gotDev.Tags["k"])

	gotGW, err := b2.GetWirelessGateway(testAccountID, testRegion, gw.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-gw", gotGW.Name)

	gotMG, err := b2.GetMulticastGroup(testAccountID, testRegion, mg.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-mg", gotMG.Name)

	gotNC, err := b2.GetNetworkAnalyzerConfig(testAccountID, testRegion, nc.Name)
	require.NoError(t, err)
	assert.Equal(t, "snap-nc", gotNC.Name)

	gotIT, err := b2.GetWirelessDeviceImportTask(it.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-dest", gotIT.DestinationName)
}

// ============================================================
// Import task backend — error propagation
// ============================================================

func TestParity_ImportTaskBackend_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runTest func(*testing.T, *iotwireless.InMemoryBackend)
		name    string
	}{
		{
			name: "get_not_found",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				_, err := b.GetWirelessDeviceImportTask("no-such-id")
				require.Error(t, err)
				assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)
			},
		},
		{
			name: "delete_not_found",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				err := b.DeleteWirelessDeviceImportTask("no-such-id")
				require.Error(t, err)
				assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)
			},
		},
		{
			name: "update_not_found",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				err := b.UpdateWirelessDeviceImportTask("no-such-id", "dest")
				require.Error(t, err)
				assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			tt.runTest(t, b)
		})
	}
}

func TestParity_ImportTaskBackend_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination string
	}{
		{name: "lorawan_dest", destination: "lorawan-destination"},
		{name: "sidewalk_dest", destination: "sidewalk-destination"},
		{name: "empty_dest", destination: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()

			// Start.
			task, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, tt.destination)
			require.NoError(t, err)
			assert.NotEmpty(t, task.ID)
			assert.NotEmpty(t, task.ARN)
			assert.Equal(t, tt.destination, task.DestinationName)
			assert.Equal(t, "Initialized", task.Status)

			// Get.
			got, err := b.GetWirelessDeviceImportTask(task.ID)
			require.NoError(t, err)
			assert.Equal(t, task.ID, got.ID)
			assert.Equal(t, tt.destination, got.DestinationName)

			// List.
			tasks := b.ListWirelessDeviceImportTasks()
			assert.Len(t, tasks, 1)

			// Update.
			err = b.UpdateWirelessDeviceImportTask(task.ID, "updated-dest")
			require.NoError(t, err)

			got, err = b.GetWirelessDeviceImportTask(task.ID)
			require.NoError(t, err)
			assert.Equal(t, "updated-dest", got.DestinationName)

			// Delete.
			err = b.DeleteWirelessDeviceImportTask(task.ID)
			require.NoError(t, err)

			// Verify deleted.
			_, err = b.GetWirelessDeviceImportTask(task.ID)
			require.Error(t, err)
			assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)

			// List should be empty.
			tasks = b.ListWirelessDeviceImportTasks()
			assert.Empty(t, tasks)
		})
	}
}

func TestParity_ImportTaskARN_ContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	task, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "d")
	require.NoError(t, err)

	assert.Contains(t, task.ARN, testRegion)
	assert.Contains(t, task.ARN, testAccountID)
	assert.Contains(t, task.ARN, "ImportTask")
}

func TestParity_SingleImportTaskARN_ContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	task, err := b.StartSingleWirelessDeviceImportTask(testAccountID, testRegion, "d")
	require.NoError(t, err)

	assert.Contains(t, task.ARN, testRegion)
	assert.Contains(t, task.ARN, testAccountID)
	assert.NotEmpty(t, task.WirelessDeviceID)
}

// ============================================================
// Multicast group operations
// ============================================================

func TestParity_MulticastGroup_BulkOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "bulk_associate",
			method: http.MethodPatch,
			path:   "/multicast-groups/{id}/bulk",
		},
		{
			name:   "bulk_disassociate",
			method: http.MethodPost,
			path:   "/multicast-groups/{id}/bulk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Create a multicast group.
			rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"mg-bulk"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			mgID := createResp["Id"].(string)

			path := "/multicast-groups/" + mgID + "/bulk"
			rec = doIoTWRequest(t, h, tt.method, path, `{}`)
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestParity_MulticastGroup_SortedList(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"mg-z", "mg-a", "mg-m"} {
		_, err := b.CreateMulticastGroup(testAccountID, testRegion, name, "", nil)
		require.NoError(t, err)
	}

	groups := b.ListMulticastGroups(testAccountID, testRegion)
	require.Len(t, groups, 3)
	assert.Equal(t, "mg-a", groups[0].Name)
	assert.Equal(t, "mg-m", groups[1].Name)
	assert.Equal(t, "mg-z", groups[2].Name)
}

// ============================================================
// Network Analyzer Config operations
// ============================================================

func TestParity_NetworkAnalyzerConfig_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		configName       string
		description      string
		wirelessDevices  []string
		wirelessGateways []string
	}{
		{
			name:             "full_config",
			configName:       "nc-full",
			description:      "Full network analyzer config",
			wirelessDevices:  []string{"dev-1", "dev-2"},
			wirelessGateways: []string{"gw-1"},
		},
		{
			name:             "minimal_config",
			configName:       "nc-minimal",
			description:      "",
			wirelessDevices:  nil,
			wirelessGateways: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()

			nc, err := b.CreateNetworkAnalyzerConfig(
				testAccountID, testRegion,
				tt.configName, tt.description,
				tt.wirelessDevices, tt.wirelessGateways,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.configName, nc.Name)
			assert.NotEmpty(t, nc.ARN)

			got, err := b.GetNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.NoError(t, err)
			assert.Equal(t, tt.description, got.Description)

			if tt.wirelessDevices != nil {
				assert.Equal(t, tt.wirelessDevices, got.WirelessDevices)
			}

			// Update.
			err = b.UpdateNetworkAnalyzerConfig(
				testAccountID, testRegion,
				tt.configName, "updated desc",
				[]string{"new-dev"}, []string{"new-gw"},
			)
			require.NoError(t, err)

			updated, err := b.GetNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.NoError(t, err)
			assert.Equal(t, "updated desc", updated.Description)
			assert.Equal(t, []string{"new-dev"}, updated.WirelessDevices)

			// Delete.
			err = b.DeleteNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.NoError(t, err)

			_, err = b.GetNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.Error(t, err)
			assert.ErrorIs(t, err, iotwireless.ErrNetworkAnalyzerConfigNotFound)
		})
	}
}

// ============================================================
// Position operations (resource positions backed by real state)
// ============================================================

func TestParity_ResourcePosition_UpdateAndGet(t *testing.T) {
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

// ============================================================
// Misc operations — metric config, metrics
// ============================================================

func TestParity_MetricConfiguration_UpdateAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get_metric_config"},
		{name: "update_metric_config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			switch tt.name {
			case "get_metric_config":
				rec := doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
				assert.Equal(t, http.StatusOK, rec.Code)
			case "update_metric_config":
				rec := doIoTWRequest(t, h, http.MethodPut, "/metric-configuration", `{}`)
				assert.Equal(t, http.StatusNoContent, rec.Code)
			}
		})
	}
}

func TestParity_GetMetrics_ReturnsEmptyResults(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/metrics", `{"SummaryMetricQueries":[]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["SummaryMetricQueryResults"].([]any)
	require.True(t, ok)
	assert.Empty(t, results)
}

func TestParity_GetPositionEstimate_ReturnsOK(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodPost, "/position-estimate", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ============================================================
// Reset — clears import tasks and all new state
// ============================================================

func TestParity_Reset_ClearsImportTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	_, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "d1")
	require.NoError(t, err)

	_, err = b.StartWirelessDeviceImportTask(testAccountID, testRegion, "d2")
	require.NoError(t, err)

	assert.Len(t, b.ListWirelessDeviceImportTasks(), 2)

	b.Reset()

	assert.Empty(t, b.ListWirelessDeviceImportTasks())
}

func TestParity_Reset_ClearsMulticastGroups(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	_, err := b.CreateMulticastGroup(testAccountID, testRegion, "mg1", "", nil)
	require.NoError(t, err)

	b.Reset()

	groups := b.ListMulticastGroups(testAccountID, testRegion)
	assert.Empty(t, groups)
}

func TestParity_Reset_ClearsNetworkAnalyzerConfigs(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	_, err := b.CreateNetworkAnalyzerConfig(testAccountID, testRegion, "nc1", "", nil, nil, nil)
	require.NoError(t, err)

	b.Reset()

	configs := b.ListNetworkAnalyzerConfigs(testAccountID, testRegion)
	assert.Empty(t, configs)
}

// ============================================================
// Helpers
// ============================================================

// setupWirelessDevice creates a wireless device and returns its ID.
func setupWirelessDevice(t *testing.T, h *iotwireless.Handler, name, devType, dest string) string {
	t.Helper()

	body := `{"Name":"` + name + `","Type":"` + devType + `","DestinationName":"` + dest + `"}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	id, _ := resp["Id"].(string)
	require.NotEmpty(t, id)

	return id
}
