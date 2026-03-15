package scheduler_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *scheduler.InMemoryBackend) string
		verify func(t *testing.T, b *scheduler.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *scheduler.InMemoryBackend) string {
				sched, err := b.CreateSchedule(
					"test-schedule",
					"rate(1 minute)",
					scheduler.Target{
						ARN:     "arn:aws:lambda:us-east-1:000000000000:function:test",
						RoleARN: "arn:aws:iam::000000000000:role/test",
					},
					"ENABLED",
					scheduler.FlexibleTimeWindow{Mode: "OFF"},
				)
				if err != nil {
					return ""
				}

				return sched.Name
			},
			verify: func(t *testing.T, b *scheduler.InMemoryBackend, id string) {
				t.Helper()

				sched, err := b.GetSchedule(id)
				require.NoError(t, err)
				assert.Equal(t, id, sched.Name)
				assert.Equal(t, "rate(1 minute)", sched.ScheduleExpression)
			},
		},
		{
			name: "restore_rebuilds_arn_index",
			setup: func(b *scheduler.InMemoryBackend) string {
				sched, err := b.CreateSchedule(
					"idx-schedule",
					"rate(5 minutes)",
					scheduler.Target{
						ARN:     "arn:aws:lambda:us-east-1:000000000000:function:idx",
						RoleARN: "arn:aws:iam::000000000000:role/idx",
					},
					"ENABLED",
					scheduler.FlexibleTimeWindow{Mode: "OFF"},
				)
				if err != nil {
					return ""
				}

				return sched.ARN
			},
			verify: func(t *testing.T, b *scheduler.InMemoryBackend, resourceARN string) {
				t.Helper()

				// TagResource uses the scheduleARNIndex; must succeed after restore.
				err := b.TagResource(resourceARN, map[string]string{"env": "test"})
				require.NoError(t, err)

				kv, err := b.ListTagsForResource(resourceARN)
				require.NoError(t, err)
				assert.Equal(t, "test", kv["env"])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *scheduler.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *scheduler.InMemoryBackend, _ string) {
				t.Helper()

				schedules := b.ListSchedules()
				assert.Empty(t, schedules)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestSchedulerHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	h := scheduler.NewHandler(backend)

	_, err := backend.CreateSchedule(
		"snap-schedule",
		"rate(5 minutes)",
		scheduler.Target{
			ARN:     "arn:aws:lambda:us-east-1:000000000000:function:test",
			RoleARN: "arn:aws:iam::000000000000:role/test",
		},
		"ENABLED",
		scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := scheduler.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	schedules := fresh.ListSchedules()
	assert.Len(t, schedules, 1)
}

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
