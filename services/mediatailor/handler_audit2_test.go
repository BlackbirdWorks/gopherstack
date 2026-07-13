package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func createTestChannel(t *testing.T, h *mediatailor.Handler, name string) { //nolint:unparam // existing issue.
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/channel/"+name, map[string]any{"PlaybackMode": "LOOP"})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createTestPlaybackConfig(t *testing.T, h *mediatailor.Handler, name string) { //nolint:unparam // existing issue.
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  name,
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- LiveSource tests --- //nolint:godot // existing issue.
func TestAudit2_LiveSource_Create(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		setup    func(t *testing.T, h *mediatailor.Handler)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create live source returns ARN and configs",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()
				createTestSourceLocation(t, h)
			},
			path: "/sourceLocation/sl1/liveSource/ls1",
			body: map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "sg1", "Type": "HLS"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "ls1", resp["LiveSourceName"])
				assert.Equal(t, "sl1", resp["SourceLocationName"])
				assert.Contains(t, resp["Arn"], ":mediatailor:")
				cfgs, ok := resp["HttpPackageConfigurations"].([]any)
				require.True(t, ok)
				assert.Len(t, cfgs, 1)
			},
		},
		{
			name:     "create live source under missing source location returns 404",
			setup:    func(_ *testing.T, _ *mediatailor.Handler) {},
			path:     "/sourceLocation/nope/liveSource/ls1",
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
		{
			name: "create duplicate live source returns 409",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()
				createTestSourceLocation(t, h)
				doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/ls1", map[string]any{})
			},
			path:     "/sourceLocation/sl1/liveSource/ls1",
			body:     map[string]any{},
			wantCode: http.StatusConflict,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			tc.setup(t, h)

			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}

func TestAudit2_LiveSource_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	// create
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/ls1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "sg1", "Type": "HLS"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// describe
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSource/ls1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ls1", resp["LiveSourceName"])

	// update
	rec = doRequest(t, h, http.MethodPut, "/sourceLocation/sl1/liveSource/ls1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/dash", "SourceGroup": "sg2", "Type": "DASH_ISO"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	cfgs, _ := updated["HttpPackageConfigurations"].([]any)
	require.Len(t, cfgs, 1)
	cfg0, _ := cfgs[0].(map[string]any)
	assert.Equal(t, "/dash", cfg0["Path"])

	// list
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSources", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items, _ := listResp["Items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/sourceLocation/sl1/liveSource/ls1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// describe after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSource/ls1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
func TestAudit2_LiveSource_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "describe missing live source returns 404",
			method:   http.MethodGet,
			path:     "/sourceLocation/sl1/liveSource/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update missing live source returns 404",
			method:   http.MethodPut,
			path:     "/sourceLocation/sl1/liveSource/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing live source returns 404",
			method:   http.MethodDelete,
			path:     "/sourceLocation/sl1/liveSource/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			createTestSourceLocation(t, h)

			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- PrefetchSchedule tests --- //nolint:godot // existing issue.
func TestAudit2_PrefetchSchedule_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		name     string
		wantCode int
	}{
		{
			name:     "create prefetch schedule",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "sched1", resp["Name"])
				assert.Equal(t, "pc1", resp["PlaybackConfigurationName"])
				assert.Contains(t, resp["Arn"], ":mediatailor:")
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h2 := newTestHandler(t)
			createTestPlaybackConfig(t, h2, "pc1")

			rec := doRequest(t, h2, http.MethodPost, "/prefetchSchedule/pc1/sched1", nil)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}

	// get
	doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/sched1", nil)
	rec := doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1/sched1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "sched1", resp["Name"])

	// list
	rec = doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items, _ := listResp["Items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/prefetchSchedule/pc1/sched1", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1/sched1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
func TestAudit2_PrefetchSchedule_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "create under missing playback config returns 404",
			method:   http.MethodPost,
			path:     "/prefetchSchedule/nope/sched1",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "get missing schedule returns 404",
			method:   http.MethodGet,
			path:     "/prefetchSchedule/pc1/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing schedule returns 404",
			method:   http.MethodDelete,
			path:     "/prefetchSchedule/pc1/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			createTestPlaybackConfig(t, h, "pc1")

			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- Program tests --- //nolint:godot // existing issue.
func TestAudit2_Program_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	createTestChannel(t, h, "ch1")

	// create program
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1/program/prog1", map[string]any{
		"SourceLocationName": "sl1",
		"VodSourceName":      "vs1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

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
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/program/prog1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

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
func TestAudit2_Program_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "create under missing channel returns 404",
			method:   http.MethodPost,
			path:     "/channel/nope/program/prog1",
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

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			createTestChannel(t, h, "ch1")

			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- ChannelPolicy tests --- //nolint:godot // existing issue.
func TestAudit2_ChannelPolicy_FullCycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	createTestChannel(t, h, "ch1")

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		method   string
		path     string
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "put channel policy returns 200",
			method:   http.MethodPut,
			path:     "/channel/ch1/policy",
			body:     map[string]any{"Policy": `{"Version":"2012-10-17","Statement":[]}`},
			wantCode: http.StatusOK,
		},
		{
			name:     "get channel policy returns policy string",
			method:   http.MethodGet,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["Policy"], "2012-10-17")
			},
		},
		{
			name:     "delete channel policy returns 200",
			method:   http.MethodDelete,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusOK,
		},
		{
			name:     "get after delete returns 404",
			method:   http.MethodGet,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}
func TestAudit2_ChannelPolicy_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		method   string
		path     string
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "put policy on missing channel returns 404",
			method:   http.MethodPut,
			path:     "/channel/nope/policy",
			body:     map[string]any{"Policy": `{}`},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "get policy on missing channel returns 404",
			method:   http.MethodGet,
			path:     "/channel/nope/policy",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete policy on channel with no policy returns 200",
			method:   http.MethodDelete,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			createTestChannel(t, h, "ch1")

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// --- Function tests --- //nolint:godot // existing issue.
func TestAudit2_Function_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		method   string
		path     string
		body     any
		name     string
		wantCode int
	}{
		{
			name:   "put function returns id type arn",
			method: http.MethodPut,
			path:   "/function/fn1",
			body: map[string]any{
				"FunctionType": "CUSTOM_OUTPUT",
				"Description":  "my function",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "fn1", resp["FunctionId"])
				assert.Equal(t, "CUSTOM_OUTPUT", resp["FunctionType"])
				assert.Contains(t, resp["Arn"], ":mediatailor:")
				assert.Equal(t, "my function", resp["Description"])
			},
		},
		{
			name:     "get function returns details",
			method:   http.MethodGet,
			path:     "/function/fn1",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "fn1", resp["FunctionId"])
				assert.Equal(t, "my function", resp["Description"])
			},
		},
		{
			name:     "list functions returns one item",
			method:   http.MethodGet,
			path:     "/functions",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items, _ := resp["Items"].([]any)
				assert.Len(t, items, 1)
			},
		},
		{
			name:     "delete function returns 204",
			method:   http.MethodDelete,
			path:     "/function/fn1",
			wantCode: http.StatusNoContent,
		},
		{
			name:     "get after delete returns 404",
			method:   http.MethodGet,
			path:     "/function/fn1",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}
func TestAudit2_Function_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "get missing function returns 404",
			method:   http.MethodGet,
			path:     "/function/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing function returns 404",
			method:   http.MethodDelete,
			path:     "/function/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
func TestAudit2_Function_MissingType(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/function/fn1", map[string]any{
		"Description": "no type",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ListAlerts tests --- //nolint:godot // existing issue.
func TestAudit2_ListAlerts(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/alerts", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.NotNil(t, items)
}

// --- ConfigureLogs tests --- //nolint:godot // existing issue.
func TestAudit2_ConfigureLogsForChannel(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "configure logs returns channel name and log types",
			body: map[string]any{
				"ChannelName": "ch1",
				"LogTypes":    []any{"AS_RUN"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "ch1", resp["ChannelName"])
				logTypes, _ := resp["LogTypes"].([]any)
				assert.Len(t, logTypes, 1)
			},
		},
		{
			name: "configure logs for missing channel returns 404",
			body: map[string]any{
				"ChannelName": "nope",
				"LogTypes":    []any{},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			createTestChannel(t, h, "ch1")

			rec := doRequest(t, h, http.MethodPut, "/configureLogs/channel", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}
func TestAudit2_ConfigureLogsForPlaybackConfiguration(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "configure logs returns config name and percent",
			body: map[string]any{
				"PlaybackConfigurationName": "pc1",
				"PercentEnabled":            float64(60),
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "pc1", resp["PlaybackConfigurationName"])
				assert.InDelta(t, float64(60), resp["PercentEnabled"], 0.0001)
			},
		},
		{
			name: "configure logs for missing config returns 404",
			body: map[string]any{
				"PlaybackConfigurationName": "nope",
				"PercentEnabled":            float64(0),
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			createTestPlaybackConfig(t, h, "pc1")

			rec := doRequest(t, h, http.MethodPut, "/configureLogs/playbackConfiguration", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}
