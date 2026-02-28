package scheduler_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/scheduler"
)

func newTestSchedulerHandler(t *testing.T) *scheduler.Handler {
	t.Helper()

	return scheduler.NewHandler(scheduler.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
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

func TestSchedulerHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				assert.Equal(t, "Scheduler", h.Name())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "CreateSchedule")
				assert.Contains(t, ops, "GetSchedule")
				assert.Contains(t, ops, "ListSchedules")
				assert.Contains(t, ops, "DeleteSchedule")
				assert.Contains(t, ops, "UpdateSchedule")
				assert.Contains(t, ops, "TagResource")
				assert.Contains(t, ops, "ListTagsForResource")
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				assert.Equal(t, 100, h.MatchPriority())
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				matcher := h.RouteMatcher()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "AWSScheduler.CreateSchedule")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.True(t, matcher(c))
			},
		},
		{
			name: "RouteMatcher_NoMatch",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				matcher := h.RouteMatcher()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "Firehose_20150804.CreateDeliveryStream")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.False(t, matcher(c))
			},
		},
		{
			name: "ExtractOperation",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "ExtractResource",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Name":"my-schedule"}`))
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "my-schedule", h.ExtractResource(c))
			},
		},
		{
			name: "CreateSchedule",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target": map[string]string{
						"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
						"RoleArn": "arn:aws:iam::000000000000:role/my-role",
					},
					"FlexibleTimeWindow": map[string]any{
						"Mode": "OFF",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ScheduleArn"], "arn:aws:scheduler:")
			},
		},
		{
			name: "CreateSchedule_AlreadyExists",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target":             map[string]string{"Arn": "arn:aws:lambda:::fn", "RoleArn": "arn:aws:iam:::role"},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})

				rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target":             map[string]string{"Arn": "arn:aws:lambda:::fn", "RoleArn": "arn:aws:iam:::role"},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "GetSchedule",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target":             map[string]string{"Arn": "arn:aws:lambda:::fn", "RoleArn": "arn:aws:iam:::role"},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})

				rec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "my-schedule"})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-schedule", resp["Name"])
				assert.Equal(t, "rate(5 minutes)", resp["ScheduleExpression"])
				assert.Contains(t, resp, "Target")
				assert.Contains(t, resp, "FlexibleTimeWindow")
			},
		},
		{
			name: "GetSchedule_NotFound",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "nonexistent"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "ListSchedules",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "s1",
					"ScheduleExpression": "rate(1 minute)",
					"Target": map[string]string{
						"Arn":     "arn:a",
						"RoleArn": "arn:r",
					},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "s2",
					"ScheduleExpression": "rate(2 minutes)",
					"Target": map[string]string{
						"Arn":     "arn:a",
						"RoleArn": "arn:r",
					},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})

				rec := doSchedulerRequest(t, h, "ListSchedules", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Schedules")
				schedules, ok := resp["Schedules"].([]any)
				require.True(t, ok)
				assert.Len(t, schedules, 2)
			},
		},
		{
			name: "DeleteSchedule",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target": map[string]string{
						"Arn":     "arn:a",
						"RoleArn": "arn:r",
					},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})

				rec := doSchedulerRequest(t, h, "DeleteSchedule", map[string]any{"Name": "my-schedule"})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify deleted
				rec2 := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "my-schedule"})
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "DeleteSchedule_NotFound",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "DeleteSchedule", map[string]any{"Name": "nonexistent"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "UpdateSchedule",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target": map[string]string{
						"Arn":     "arn:a",
						"RoleArn": "arn:r",
					},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})

				rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(10 minutes)",
					"Target":             map[string]string{"Arn": "arn:a2", "RoleArn": "arn:r2"},
					"State":              "DISABLED",
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ScheduleArn"], "arn:aws:scheduler:")

				// Verify the update
				getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "my-schedule"})
				var getResp map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
				assert.Equal(t, "rate(10 minutes)", getResp["ScheduleExpression"])
				assert.Equal(t, "DISABLED", getResp["State"])
			},
		},
		{
			name: "UpdateSchedule_NotFound",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
					"Name":               "nonexistent",
					"ScheduleExpression": "rate(1 minute)",
					"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "TagResource",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				// Create a schedule and get its ARN
				createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target": map[string]string{
						"Arn":     "arn:a",
						"RoleArn": "arn:r",
					},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				var createResp map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				arn := createResp["ScheduleArn"]

				rec := doSchedulerRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        map[string]string{"env": "test", "team": "platform"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "TagResource_NotFound",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent",
					"Tags":        map[string]string{"env": "test"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "ListTagsForResource",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				// Create schedule and tag it
				createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "my-schedule",
					"ScheduleExpression": "rate(5 minutes)",
					"Target": map[string]string{
						"Arn":     "arn:a",
						"RoleArn": "arn:r",
					},
					"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				})
				var createResp map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				arn := createResp["ScheduleArn"]

				doSchedulerRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        map[string]string{"env": "prod"},
				})

				rec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Tags")
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
			},
		},
		{
			name: "ListTagsForResource_NotFound",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "UnknownAction",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "UnknownAction", nil)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "Provider",
			run: func(t *testing.T) {
				p := &scheduler.Provider{}
				assert.Equal(t, "Scheduler", p.Name())
			},
		},
		{
			name: "Provider_Init",
			run: func(t *testing.T) {
				p := &scheduler.Provider{}
				ctx := &service.AppContext{Logger: slog.Default()}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				assert.NotNil(t, svc)
				assert.Equal(t, "Scheduler", svc.Name())
			},
		},
		{
			name: "CreateSchedule_InvalidJSON",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				rec := doInvalidSchedulerRequest(t, h, "CreateSchedule")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "GetSchedule_InvalidJSON",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				rec := doInvalidSchedulerRequest(t, h, "GetSchedule")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "DeleteSchedule_InvalidJSON",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				rec := doInvalidSchedulerRequest(t, h, "DeleteSchedule")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "UpdateSchedule_InvalidJSON",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				rec := doInvalidSchedulerRequest(t, h, "UpdateSchedule")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "TagResource_InvalidJSON",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				rec := doInvalidSchedulerRequest(t, h, "TagResource")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "ListTagsForResource_InvalidJSON",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)
				rec := doInvalidSchedulerRequest(t, h, "ListTagsForResource")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "CreateSchedule_DefaultState",
			run: func(t *testing.T) {
				// When State is omitted, it should default to ENABLED.
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "no-state-schedule",
					"ScheduleExpression": "rate(1 hour)",
					"Target": map[string]string{
						"Arn":     "arn:aws:lambda:us-east-1:0:function:f",
						"RoleArn": "arn:aws:iam::0:role/r",
					},
					"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
					// State intentionally omitted
				})
				require.Equal(t, http.StatusOK, rec.Code)

				getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "no-state-schedule"})
				require.Equal(t, http.StatusOK, getRec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
				assert.Equal(t, "ENABLED", resp["State"])
			},
		},
		{
			name: "CreateSchedule_CronExpression",
			run: func(t *testing.T) {
				h := newTestSchedulerHandler(t)

				rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
					"Name":               "cron-schedule",
					"ScheduleExpression": "cron(0 12 * * ? *)",
					"Target": map[string]string{
						"Arn":     "arn:aws:lambda:us-east-1:0:function:f",
						"RoleArn": "arn:aws:iam::0:role/r",
					},
					"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
					"State":              "ENABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
