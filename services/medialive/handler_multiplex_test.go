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
		"Name":              "test-multiplex",
		"AvailabilityZones": []any{"us-east-1a", "us-east-1b"},
		"MultiplexSettings": map[string]any{
			"TransportStreamBitrate": 1000000,
			"TransportStreamId":      1,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	m := resp["Multiplex"].(map[string]any)

	return m["Id"].(string)
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
				"Name":              "my-multiplex",
				"AvailabilityZones": []any{"us-east-1a"},
				"MultiplexSettings": map[string]any{
					"TransportStreamBitrate": 2000000,
					"TransportStreamId":      5,
				},
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				m := resp["Multiplex"].(map[string]any)
				assert.Contains(t, m["Arn"], "arn:aws:medialive:us-east-1:000000000000:multiplex:")
				assert.Equal(t, "IDLE", m["State"])
				assert.NotEmpty(t, m["Id"])
				assert.Equal(t, "my-multiplex", m["Name"])
				settings := m["MultiplexSettings"].(map[string]any)
				assert.Equal(t, 2000000, int(settings["TransportStreamBitrate"].(float64)))
				assert.Equal(t, 5, int(settings["TransportStreamId"].(float64)))
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
	assert.Equal(t, "test-multiplex", descResp["Name"])
	assert.Equal(t, "IDLE", descResp["State"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/prod/multiplexes/"+multiplexID, map[string]any{
		"Name": "updated-multiplex",
		"MultiplexSettings": map[string]any{
			"TransportStreamBitrate": 3000000,
			"TransportStreamId":      2,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	m := updateResp["Multiplex"].(map[string]any)
	assert.Equal(t, "updated-multiplex", m["Name"])
	settings := m["MultiplexSettings"].(map[string]any)
	assert.Equal(t, 3000000, int(settings["TransportStreamBitrate"].(float64)))

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Multiplexes"], 1)

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
	assert.Equal(t, "RUNNING", startResp["State"])

	// Start again returns conflict
	rec = doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/start", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Stop
	rec = doRequest(t, h, http.MethodPost, "/prod/multiplexes/"+multiplexID+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "IDLE", stopResp["State"])

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
				"ProgramName": "prog-1",
				"MultiplexProgramSettings": map[string]any{
					"ProgramNumber": 1,
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
	assert.Empty(t, resp["Multiplexes"])
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
			"ProgramName": "prog-1",
			"MultiplexProgramSettings": map[string]any{
				"ProgramNumber":            1,
				"PreferredChannelPipeline": "CURRENTLY_ACTIVE",
				"ServiceDescriptor": map[string]any{
					"ProviderName": "MyProvider",
					"ServiceName":  "MyService",
				},
			},
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	prog := createResp["MultiplexProgram"].(map[string]any)
	assert.Equal(t, "prog-1", prog["ProgramName"])
	settings := prog["MultiplexProgramSettings"].(map[string]any)
	assert.Equal(t, 1, int(settings["ProgramNumber"].(float64)))
	assert.Equal(t, "CURRENTLY_ACTIVE", settings["PreferredChannelPipeline"])
	sd := settings["ServiceDescriptor"].(map[string]any)
	assert.Equal(t, "MyProvider", sd["ProviderName"])
	assert.Equal(t, "MyService", sd["ServiceName"])

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
	assert.Equal(t, "prog-1", descResp["ProgramName"])

	// Update
	rec = doRequest(
		t,
		h,
		http.MethodPut,
		"/prod/multiplexes/"+multiplexID+"/programs/prog-1",
		map[string]any{
			"MultiplexProgramSettings": map[string]any{
				"ProgramNumber":            2,
				"PreferredChannelPipeline": "PIPELINE_0",
				"ServiceDescriptor": map[string]any{
					"ProviderName": "UpdatedProvider",
					"ServiceName":  "UpdatedService",
				},
			},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	updatedProg := updateResp["MultiplexProgram"].(map[string]any)
	updatedSettings := updatedProg["MultiplexProgramSettings"].(map[string]any)
	assert.Equal(t, 2, int(updatedSettings["ProgramNumber"].(float64)))
	assert.Equal(t, "PIPELINE_0", updatedSettings["PreferredChannelPipeline"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/multiplexes/"+multiplexID+"/programs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["MultiplexPrograms"], 1)

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
		"ProgramName": "prog-1",
		"MultiplexProgramSettings": map[string]any{
			"ProgramNumber": 1,
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
			"MultiplexProgramSettings": map[string]any{"ProgramNumber": 1},
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
	assert.Empty(t, resp["MultiplexPrograms"])
}
