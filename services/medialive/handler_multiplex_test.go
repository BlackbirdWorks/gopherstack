package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func createTestMultiplex(t *testing.T, h *medialive.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/multiplexes", map[string]any{
		"name":              "test-multiplex",
		"availabilityZones": []any{"us-east-1a", "us-east-1b"},
		"multiplexSettings": map[string]any{
			"transportStreamBitrate": 1000000,
			"transportStreamId":      1,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	m := resp["multiplex"].(map[string]any)

	return m["id"].(string)
}

func TestMultiplex_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "create returns multiplex with ARN and IDLE state",
			body: map[string]any{
				"name":              "my-multiplex",
				"availabilityZones": []any{"us-east-1a"},
				"multiplexSettings": map[string]any{
					"transportStreamBitrate": 2000000,
					"transportStreamId":      5,
				},
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				m := resp["multiplex"].(map[string]any)
				assert.Contains(t, m["arn"], "arn:aws:medialive:us-east-1:000000000000:multiplex:")
				assert.Equal(t, "IDLE", m["state"])
				assert.NotEmpty(t, m["id"])
				assert.Equal(t, "my-multiplex", m["name"])
				assert.InDelta(t, 0, m["pipelinesRunningCount"], 0)
				assert.InDelta(t, 0, m["programCount"], 0)
				settings := m["multiplexSettings"].(map[string]any)
				assert.Equal(t, 2000000, int(settings["transportStreamBitrate"].(float64)))
				assert.Equal(t, 5, int(settings["transportStreamId"].(float64)))
			},
		},
		{
			name:     "create missing Name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/multiplexes", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMultiplex_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	assert.Equal(t, 1, medialive.MultiplexCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-multiplex", descResp["name"])
	assert.Equal(t, "IDLE", descResp["state"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/prod/multiplexes/"+multiplexID, map[string]any{
		"name": "updated-multiplex",
		"multiplexSettings": map[string]any{
			"transportStreamBitrate": 3000000,
			"transportStreamId":      2,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	m := updateResp["multiplex"].(map[string]any)
	assert.Equal(t, "updated-multiplex", m["name"])
	settings := m["multiplexSettings"].(map[string]any)
	assert.Equal(t, 3000000, int(settings["transportStreamBitrate"].(float64)))

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["multiplexes"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/multiplexes/"+multiplexID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, medialive.MultiplexCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMultiplex_StartStop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	// Start
	rec := doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Equal(t, "STARTING", startResp["state"])

	// Start again returns conflict
	rec = doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/start", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Stop
	rec = doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "STOPPING", stopResp["state"])

	// Stop again returns conflict
	rec = doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/stop", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestMultiplex_DeleteRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)
	doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/start", nil)

	rec := doRequest(t, h, http.MethodDelete, "/prod/multiplexes/"+multiplexID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestMultiplex_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/prod/multiplexes/notexist"},
		{"update unknown returns 404", http.MethodPut, "/prod/multiplexes/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/prod/multiplexes/notexist"},
		{"start unknown returns 404", http.MethodPost, "/prod/multiplexes/notexist/start"},
		{"stop unknown returns 404", http.MethodPost, "/prod/multiplexes/notexist/stop"},
		{
			"list programs unknown returns 404",
			http.MethodGet,
			"/prod/multiplexes/notexist/programs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}

	t.Run("create program unknown multiplex returns 404", func(t *testing.T) {
		t.Parallel()
		rec := doRequest(
			t,
			h,
			http.MethodPost,
			"/prod/multiplexes/notexist/programs",
			map[string]any{
				"programName": "prog-1",
				"multiplexProgramSettings": map[string]any{
					"programNumber": 1,
				},
			},
		)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestMultiplex_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/multiplexes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["multiplexes"])
}

func TestMultiplexProgram_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	// Create program
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/multiplexes/"+multiplexID+"/programs",
		map[string]any{
			"programName": "prog-1",
			"multiplexProgramSettings": map[string]any{
				"programNumber":            1,
				"preferredChannelPipeline": "CURRENTLY_ACTIVE",
				"serviceDescriptor": map[string]any{
					"providerName": "MyProvider",
					"serviceName":  "MyService",
				},
			},
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	prog := createResp["multiplexProgram"].(map[string]any)
	assert.Equal(t, "prog-1", prog["programName"])
	settings := prog["multiplexProgramSettings"].(map[string]any)
	assert.Equal(t, 1, int(settings["programNumber"].(float64)))
	assert.Equal(t, "CURRENTLY_ACTIVE", settings["preferredChannelPipeline"])
	sd := settings["serviceDescriptor"].(map[string]any)
	assert.Equal(t, "MyProvider", sd["providerName"])
	assert.Equal(t, "MyService", sd["serviceName"])

	assert.Equal(
		t,
		1,
		medialive.MultiplexProgramCount(h.Backend.(*medialive.InMemoryBackend), multiplexID),
	)

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID+"/programs/prog-1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "prog-1", descResp["programName"])

	// Update
	rec = doRequest(
		t,
		h,
		http.MethodPut,
		"/prod/multiplexes/"+multiplexID+"/programs/prog-1",
		map[string]any{
			"multiplexProgramSettings": map[string]any{
				"programNumber":            2,
				"preferredChannelPipeline": "PIPELINE_0",
				"serviceDescriptor": map[string]any{
					"providerName": "UpdatedProvider",
					"serviceName":  "UpdatedService",
				},
			},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	updatedProg := updateResp["multiplexProgram"].(map[string]any)
	updatedSettings := updatedProg["multiplexProgramSettings"].(map[string]any)
	assert.Equal(t, 2, int(updatedSettings["programNumber"].(float64)))
	assert.Equal(t, "PIPELINE_0", updatedSettings["preferredChannelPipeline"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID+"/programs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["multiplexPrograms"], 1)

	// Delete
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/prod/multiplexes/"+multiplexID+"/programs/prog-1",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(
		t,
		0,
		medialive.MultiplexProgramCount(h.Backend.(*medialive.InMemoryBackend), multiplexID),
	)

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID+"/programs/prog-1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMultiplexProgram_CreateConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	body := map[string]any{
		"programName": "prog-1",
		"multiplexProgramSettings": map[string]any{
			"programNumber": 1,
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/programs", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create again with same name → conflict
	rec = doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/programs", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestMultiplexProgram_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/multiplexes/"+multiplexID+"/programs",
		map[string]any{
			"multiplexProgramSettings": map[string]any{"programNumber": 1},
		},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMultiplexProgram_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			"describe unknown program returns 404",
			http.MethodGet,
			"/prod/multiplexes/" + multiplexID + "/programs/noexist",
		},
		{
			"update unknown program returns 404",
			http.MethodPut,
			"/prod/multiplexes/" + multiplexID + "/programs/noexist",
		},
		{
			"delete unknown program returns 404",
			http.MethodDelete,
			"/prod/multiplexes/" + multiplexID + "/programs/noexist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestMultiplexProgram_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	multiplexID := createTestMultiplex(t, h)

	rec := doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID+"/programs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["multiplexPrograms"])
}
