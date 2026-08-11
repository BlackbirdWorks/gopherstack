package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProgram_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestChannel(t, h)
	createTestSourceLocationAndVodSource(t, h)

	// create program
	createBody := testScheduleConfigBody(1_700_000_000_000)
	createBody["SourceLocationName"] = "sl1"
	createBody["VodSourceName"] = "vs1"
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1/program/prog1", createBody)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, "prog1", created["ProgramName"])
	assert.Equal(t, "ch1", created["ChannelName"])
	assert.Contains(t, created["Arn"], ":mediatailor:")

	// describe program
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1/program/prog1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &described))
	assert.Equal(t, "prog1", described["ProgramName"])
	assert.Equal(t, "sl1", described["SourceLocationName"])

	// update program
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/program/prog1", map[string]any{
		"ScheduleConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// get channel schedule
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1/schedule", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var schedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &schedResp))
	items, _ := schedResp["Items"].([]any)
	assert.Len(t, items, 1)
	item0, _ := items[0].(map[string]any)
	assert.Equal(t, "prog1", item0["ProgramName"])

	// delete program
	rec = doRequest(t, h, http.MethodDelete, "/channel/ch1/program/prog1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// describe after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1/program/prog1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestProgram_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:   "create under missing channel returns 404",
			method: http.MethodPost,
			path:   "/channel/nope/program/prog1",
			body: func() map[string]any {
				body := testScheduleConfigBody(1_700_000_000_000)
				body["SourceLocationName"] = "sl1"

				return body
			}(),
			wantCode: http.StatusNotFound,
		},
		{
			name:     "describe missing program returns 404",
			method:   http.MethodGet,
			path:     "/channel/ch1/program/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing program returns 404",
			method:   http.MethodDelete,
			path:     "/channel/ch1/program/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestChannel(t, h)

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandleUpdateProgram_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/channel/nonexistent/program/prog1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetChannelSchedule_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/channel/nonexistent/schedule", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetChannelSchedule_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocationAndVodSource(t, h)

	// Create channel
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
		"PlaybackMode": "LINEAR",
	})

	// Create program in channel
	progBody := testScheduleConfigBody(1_700_000_000_000)
	progBody["SourceLocationName"] = "sl1"
	progBody["VodSourceName"] = "vs1"
	createRec := doRequest(t, h, http.MethodPost, "/channel/ch1/program/prog1", progBody)
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	rec := doRequest(t, h, http.MethodGet, "/channel/ch1/schedule", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var schedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &schedResp))
	items, _ := schedResp["Items"].([]any)
	require.Len(t, items, 1)
}
