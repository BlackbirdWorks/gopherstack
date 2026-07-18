package scheduler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func newTestSchedulerHandler(t *testing.T) *scheduler.Handler {
	t.Helper()

	return scheduler.NewHandler(scheduler.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doSchedulerRequest(t *testing.T, h *scheduler.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSScheduler."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doInvalidSchedulerRequest sends a request with invalid JSON body.
func doInvalidSchedulerRequest(t *testing.T, h *scheduler.Handler, action string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSScheduler."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doRESTRequest sends a REST request to the scheduler handler and returns the recorder.
func doRESTRequest(
	t *testing.T,
	h *scheduler.Handler,
	method, path string,
	body any,
	query map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if len(query) > 0 {
		q := req.URL.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// wireTagsBody builds the []{"Key":...,"Value":...} shape EventBridge Scheduler
// uses on the wire for resource-level tags (CreateScheduleGroup.Tags,
// TagResource.Tags, ListTagsForResource.Tags), from a convenience Go map.
func wireTagsBody(kv map[string]string) []map[string]string {
	out := make([]map[string]string, 0, len(kv))
	for k, v := range kv {
		out = append(out, map[string]string{"Key": k, "Value": v})
	}

	return out
}

// wireTagsToMap converts a decoded []{"Key":...,"Value":...} JSON response (as
// produced by json.Unmarshal into map[string]any) back into a plain Go map for
// easy test assertions.
func wireTagsToMap(t *testing.T, raw any) map[string]string {
	t.Helper()

	out := map[string]string{}

	list, ok := raw.([]any)
	if !ok {
		return out
	}

	for _, item := range list {
		entry, entryOK := item.(map[string]any)
		require.True(t, entryOK)
		key, _ := entry["Key"].(string)
		value, _ := entry["Value"].(string)
		out[key] = value
	}

	return out
}

func TestSchedulerHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	assert.Equal(t, "Scheduler", h.Name())
}

func TestSchedulerHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateSchedule")
	assert.Contains(t, ops, "GetSchedule")
	assert.Contains(t, ops, "ListSchedules")
	assert.Contains(t, ops, "DeleteSchedule")
	assert.Contains(t, ops, "UpdateSchedule")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "CreateScheduleGroup")
	assert.Contains(t, ops, "DeleteScheduleGroup")
	assert.Contains(t, ops, "GetScheduleGroup")
	assert.Contains(t, ops, "ListScheduleGroups")
	assert.Contains(t, ops, "UntagResource")
}

// TestSchedulerHandler_OpsLen verifies the internal dispatch table's size both
// tracks GetSupportedOperations() dynamically and matches the known op count.
func TestSchedulerHandler_OpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
		useOps  bool
	}{
		{name: "matches_supported_operations_length", useOps: true},
		{name: "equals_known_op_count", wantLen: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			want := tt.wantLen
			if tt.useOps {
				want = len(h.GetSupportedOperations())
			}
			assert.Equal(t, want, scheduler.HandlerOpsLen(h))
		})
	}
}

func TestSchedulerHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestSchedulerHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{name: "Match", target: "AWSScheduler.CreateSchedule", wantMatch: true},
		{name: "NoMatch", target: "Firehose_20150804.CreateDeliveryStream", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestSchedulerHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSScheduler.CreateSchedule")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateSchedule", h.ExtractOperation(c))

	// No target → "Unknown"
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))
}

func TestSchedulerHandler_ExtractOperationRESTPaths(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	tests := []struct {
		method  string
		path    string
		wantOp  string
		wantRes string
	}{
		{http.MethodGet, "/schedules", "ListSchedules", ""},
		{http.MethodGet, "/schedules/", "ListSchedules", ""},
		{http.MethodPost, "/schedules/my-sched", "CreateSchedule", "my-sched"},
		{http.MethodGet, "/schedules/my-sched", "GetSchedule", "my-sched"},
		{http.MethodDelete, "/schedules/my-sched", "DeleteSchedule", "my-sched"},
		{http.MethodPut, "/schedules/my-sched", "UpdateSchedule", "my-sched"},
		{http.MethodGet, "/schedule-groups", "ListScheduleGroups", ""},
		{http.MethodPost, "/schedule-groups/grp", "CreateScheduleGroup", "grp"},
		{http.MethodGet, "/schedule-groups/grp", "GetScheduleGroup", "grp"},
		{http.MethodDelete, "/schedule-groups/grp", "DeleteScheduleGroup", "grp"},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_"+tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestSchedulerHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Name":"my-schedule"}`))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-schedule", h.ExtractResource(c))
}

func TestSchedulerHandler_ErrorStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "GetSchedule_NotFound",
			action:   "GetSchedule",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteSchedule_NotFound",
			action:   "DeleteSchedule",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateSchedule_NotFound",
			action: "UpdateSchedule",
			body: map[string]any{
				"Name":               "nonexistent",
				"ScheduleExpression": "rate(1 minute)",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "TagResource_NotFound",
			action: "TagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent",
				"Tags":        wireTagsBody(map[string]string{"env": "test"}),
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListTagsForResource_NotFound",
			action: "ListTagsForResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetScheduleGroup_NotFound",
			action:   "GetScheduleGroup",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteScheduleGroup_NotFound",
			action:   "DeleteScheduleGroup",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteScheduleGroup_Default",
			action:   "DeleteScheduleGroup",
			body:     map[string]any{"Name": "default"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "UntagResource_NotFound",
			action: "UntagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nope",
				"TagKeys":     []string{"k"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doSchedulerRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSchedulerHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{name: "CreateSchedule", action: "CreateSchedule", wantCode: http.StatusBadRequest},
		{name: "GetSchedule", action: "GetSchedule", wantCode: http.StatusBadRequest},
		{name: "DeleteSchedule", action: "DeleteSchedule", wantCode: http.StatusBadRequest},
		{name: "UpdateSchedule", action: "UpdateSchedule", wantCode: http.StatusBadRequest},
		{name: "TagResource", action: "TagResource", wantCode: http.StatusBadRequest},
		{name: "ListTagsForResource", action: "ListTagsForResource", wantCode: http.StatusBadRequest},
		{name: "CreateScheduleGroup", action: "CreateScheduleGroup", wantCode: http.StatusBadRequest},
		{name: "DeleteScheduleGroup", action: "DeleteScheduleGroup", wantCode: http.StatusBadRequest},
		{name: "GetScheduleGroup", action: "GetScheduleGroup", wantCode: http.StatusBadRequest},
		{name: "UntagResource", action: "UntagResource", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)
			rec := doInvalidSchedulerRequest(t, h, tt.action)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestSchedulerHandler_Routing verifies the RouteMatcher accepts both the JSON
// protocol (X-Amz-Target) and REST-style paths.
func TestSchedulerHandler_Routing(t *testing.T) {
	t.Parallel()

	h := scheduler.NewHandler(scheduler.NewInMemoryBackend("000000000000", "us-east-1"))

	assert.Equal(t, "Scheduler", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		path      string
		target    string
		wantMatch bool
	}{
		{"target match", "/", "AWSScheduler.ListSchedules", true},
		{"rest path match", "/schedules", "", true},
		{"no match", "/other", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

// TestSchedulerHandler_RESTPath exercises a full REST create-then-get round trip
// through the raw echo.HandlerFunc, without going through doRESTRequest.
func TestSchedulerHandler_RESTPath(t *testing.T) {
	t.Parallel()

	h := scheduler.NewHandler(scheduler.NewInMemoryBackend("000000000000", "us-east-1"))

	e := echo.New()

	// Create via REST POST /schedules
	target := `"Target":{"Arn":"arn:aws:lambda:us-east-1:000000000000:function:test",` +
		`"RoleArn":"arn:aws:iam::000000000000:role/test"}`
	body := `{"Name":"rest-sched","ScheduleExpression":"rate(1 minute)",` + target +
		`,"FlexibleTimeWindow":{"Mode":"OFF"},"State":"ENABLED"}`
	req := httptest.NewRequest(http.MethodPost, "/schedules/rest-sched", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/schedules/rest-sched")

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get via REST GET /schedules/{name}
	req2 := httptest.NewRequest(http.MethodGet, "/schedules/rest-sched", nil)
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	c2.SetPath("/schedules/rest-sched")

	err = h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestSchedulerHandler_REST_UnknownPath verifies an unsupported REST method/path
// combination returns 404 rather than falling through to the JSON dispatcher.
func TestSchedulerHandler_REST_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)
	e := echo.New()

	// PUT on /schedule-groups/{name} is not a valid REST operation → 404.
	req := httptest.NewRequest(http.MethodPut, "/schedule-groups/my-group", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
