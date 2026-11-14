package azurearm_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recurrenceDef(rec la) la {
	return logicDef(la{"tick": la{"type": "Recurrence", "recurrence": rec}}, la{
		"Mark": compose("@utcNow('yyyy-MM-ddTHH:mm')", nil),
	})
}

func (s *logicStack) runCount(t *testing.T, name string) int {
	t.Helper()

	n := 0

	for p := s.factory.NewWorkflowRunsClient().NewListPager(logicRG, name, nil); p.More(); {
		page, err := p.NextPage(t.Context())
		require.NoError(t, err)

		n += len(page.Value)
	}

	return n
}

func TestLogicRecurrenceOnClock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rec     la
		name    string
		advance time.Duration
		want    int
	}{
		{
			name: "every minute from start", advance: 3 * time.Minute, want: 4,
			rec: la{"frequency": "Minute", "interval": 1, "startTime": "2026-01-01T00:00:00Z"},
		},
		{
			name: "every five minutes", advance: 12 * time.Minute, want: 3,
			rec: la{"frequency": "Minute", "interval": 5, "startTime": "2026-01-01T00:00:00Z"},
		},
		{
			name: "future start waits", advance: 30 * time.Minute, want: 1,
			rec: la{"frequency": "Hour", "interval": 1, "startTime": "2026-01-01T00:30:00Z"},
		},
		{
			name: "hourly with minutes schedule", advance: 2 * time.Hour, want: 4,
			rec: la{
				"frequency": "Hour", "interval": 1, "startTime": "2026-01-01T00:00:00Z",
				"schedule": la{"minutes": []any{15, 45}},
			},
		},
		{
			name: "weekly on monday", advance: 4*24*time.Hour + 10*time.Hour, want: 1,
			rec: la{
				"frequency": "Week", "interval": 1, "startTime": "2026-01-01T00:00:00Z",
				"schedule": la{"weekDays": []any{"Monday"}, "hours": []any{9}, "minutes": []any{30}},
			},
		},
		{
			name: "daily at hours", advance: 48 * time.Hour, want: 4,
			rec: la{
				"frequency": "Day", "interval": 1, "startTime": "2026-01-01T00:00:00Z",
				"schedule": la{"hours": []any{6, 18}, "minutes": []any{0}},
			},
		},
		{
			name: "monthly on the 15th", advance: 45 * 24 * time.Hour, want: 2,
			rec: la{
				"frequency": "Month", "interval": 1, "startTime": "2026-01-01T00:00:00Z",
				"schedule": la{"monthDays": []any{15}, "hours": []any{0}, "minutes": []any{0}},
			},
		},
		{
			name:    "end time stops firing",
			advance: 10 * time.Minute,
			want:    3,
			rec: la{
				"frequency": "Minute",
				"interval":  1,
				"startTime": "2026-01-01T00:00:00Z",
				"endTime":   "2026-01-01T00:02:00Z",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "ticker", recurrenceDef(tc.rec))

			s.clock.Advance(0)
			s.clock.Advance(tc.advance)
			s.waitRuns(t)

			assert.Equal(t, tc.want, s.runCount(t, "ticker"))

			run := s.latestRun(t, "ticker")
			assert.Equal(t, armlogic.WorkflowStatusSucceeded, *run.Properties.Status)
			assert.Equal(t, "tick", *run.Properties.Trigger.Name)
		})
	}
}

func TestLogicRecurrenceLifecycle(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "ticker", recurrenceDef(la{"frequency": "Minute", "interval": 1, "startTime": "2026-01-01T00:00:00Z"}))

	tc := s.factory.NewWorkflowTriggersClient()
	wc := s.factory.NewWorkflowsClient()

	trig, err := tc.Get(t.Context(), logicRG, "ticker", "tick", nil)
	require.NoError(t, err)
	require.NotNil(t, trig.Properties.NextExecutionTime)
	assert.Equal(t, armlogic.RecurrenceFrequencyMinute, *trig.Properties.Recurrence.Frequency)

	s.clock.Advance(0)
	s.clock.Advance(2 * time.Minute)
	s.waitRuns(t)
	require.Equal(t, 3, s.runCount(t, "ticker"))

	trig, err = tc.Get(t.Context(), logicRG, "ticker", "tick", nil)
	require.NoError(t, err)
	assert.Equal(t, s.clock.Now(), trig.Properties.LastExecutionTime.UTC())
	assert.Equal(t, s.clock.Now().Add(time.Minute), trig.Properties.NextExecutionTime.UTC())

	hist, err := s.factory.NewWorkflowTriggerHistoriesClient().
		NewListPager(logicRG, "ticker", "tick", nil).
		NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, hist.Value, 3)
	assert.Equal(t, s.clock.Now(), hist.Value[0].Properties.StartTime.UTC())

	_, err = wc.Disable(t.Context(), logicRG, "ticker", nil)
	require.NoError(t, err)

	s.clock.Advance(10 * time.Minute)
	s.waitRuns(t)
	assert.Equal(t, 3, s.runCount(t, "ticker"), "disabled workflows do not fire")

	trig, err = tc.Get(t.Context(), logicRG, "ticker", "tick", nil)
	require.NoError(t, err)
	assert.Nil(t, trig.Properties.NextExecutionTime)

	_, err = wc.Enable(t.Context(), logicRG, "ticker", nil)
	require.NoError(t, err)

	s.clock.Advance(0)
	s.clock.Advance(time.Minute)
	s.waitRuns(t)
	assert.Equal(t, 5, s.runCount(t, "ticker"))

	_, err = tc.Run(t.Context(), logicRG, "ticker", "tick", nil)
	require.NoError(t, err)
	s.waitRuns(t)
	assert.Equal(t, 6, s.runCount(t, "ticker"))

	_, err = tc.Reset(t.Context(), logicRG, "ticker", "tick", nil)
	require.NoError(t, err)

	_, err = wc.Delete(t.Context(), logicRG, "ticker", nil)
	require.NoError(t, err)

	s.clock.Advance(time.Hour)
	assert.Zero(t, s.clock.pending(false), "deleting the workflow stops its timer")
}

func TestLogicRecurrenceWithoutStartFiresImmediately(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "ticker", recurrenceDef(la{"frequency": "Hour", "interval": 1}))

	s.clock.Advance(0)
	s.waitRuns(t)
	assert.Equal(t, 1, s.runCount(t, "ticker"))

	s.clock.Advance(59 * time.Minute)
	s.waitRuns(t)
	assert.Equal(t, 1, s.runCount(t, "ticker"))

	s.clock.Advance(time.Minute)
	s.waitRuns(t)
	assert.Equal(t, 2, s.runCount(t, "ticker"))
}

func TestLogicTriggerOperations(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "req", greetingDef())
	s.create(t, "rec", recurrenceDef(la{"frequency": "Day", "interval": 1, "startTime": "2030-01-01T00:00:00Z"}))

	tc := s.factory.NewWorkflowTriggersClient()

	_, err := tc.Run(t.Context(), logicRG, "req", "manual", nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidTriggerType", sdkErrCode(err))

	_, err = tc.Reset(t.Context(), logicRG, "req", "manual", nil)
	require.Error(t, err)

	_, err = tc.Get(t.Context(), logicRG, "req", "missing", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	_, err = tc.GetSchemaJSON(t.Context(), logicRG, "rec", "tick", nil)
	require.Error(t, err)

	_, err = tc.ListCallbackURL(t.Context(), logicRG, "rec", "tick", nil)
	require.Error(t, err)

	list, err := tc.NewListPager(logicRG, "rec", nil).NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, list.Value, 1)
	assert.Equal(t, "tick", *list.Value[0].Name)

	// setState copies the state of another workflow's trigger.
	_, err = s.factory.NewWorkflowsClient().Disable(t.Context(), logicRG, "req", nil)
	require.NoError(t, err)

	prefix := "subscriptions/" + aciSub + "/resourceGroups/" + logicRG + "/providers/Microsoft.Logic/workflows/"
	src := prefix + "rec/triggers/tick"
	_, err = tc.SetState(t.Context(), logicRG, "req", "manual",
		armlogic.SetTriggerStateActionDefinition{Source: &armlogic.WorkflowTriggerReference{ID: &src}}, nil)
	require.NoError(t, err)

	got, err := s.factory.NewWorkflowsClient().Get(t.Context(), logicRG, "req", nil)
	require.NoError(t, err)
	assert.Equal(t, armlogic.WorkflowStateDisabled, *got.Properties.State)

	missing := prefix + "none/triggers/x"
	_, err = tc.SetState(t.Context(), logicRG, "req", "manual",
		armlogic.SetTriggerStateActionDefinition{Source: &armlogic.WorkflowTriggerReference{ID: &missing}}, nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestLogicRequestTriggerMethodAndPath(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "paths", logicDef(la{"manual": la{
		"type": "Request", "kind": "Http",
		"inputs": la{"method": "PUT", "relativePath": "orders/{id}"},
	}}, la{
		"R": la{"type": "Response", "runAfter": la{}, "inputs": la{
			"statusCode": 200, "body": "@triggerOutputs()['relativePathParameters']['id']",
		}},
	}))

	cb, err := s.factory.NewWorkflowTriggersClient().ListCallbackURL(t.Context(), logicRG, "paths", "manual", nil)
	require.NoError(t, err)
	assert.Equal(t, "PUT", *cb.Method)
	assert.Equal(t, "orders/{id}", *cb.RelativePath)
	require.Len(t, cb.RelativePathParameters, 1)
	assert.Equal(t, "id", *cb.RelativePathParameters[0])

	do := func(method, url string) (int, string) {
		req, rerr := http.NewRequestWithContext(t.Context(), method, url, nil)
		require.NoError(t, rerr)

		resp, rerr := s.client.Do(req)
		require.NoError(t, rerr)

		defer resp.Body.Close()

		buf := make([]byte, 128)
		n, _ := resp.Body.Read(buf)

		return resp.StatusCode, string(buf[:n])
	}

	base := *cb.BasePath
	q := "?" + (*cb.Value)[len(*cb.BasePath)+1:]

	assert.Equal(t, base+q, *cb.Value)

	code, body := do(http.MethodPut, base[:len(base)-len("orders/{id}")]+"orders/42"+q)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "42", body)

	code, _ = do(http.MethodGet, base[:len(base)-len("orders/{id}")]+"orders/42"+q)
	assert.Equal(t, http.StatusMethodNotAllowed, code)

	code, _ = do(http.MethodPut, base[:len(base)-len("orders/{id}")]+"other/42/extra"+q)
	assert.Equal(t, http.StatusNotFound, code)

	s.waitRuns(t)
	assert.Equal(t, 1, s.runCount(t, "paths"))
}
