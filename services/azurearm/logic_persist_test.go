package azurearm_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	a := newLogicStack(t)
	a.create(t, "greeter", greetingDef())
	a.create(t, "waiter", logicDef(requestTrigger(nil), la{
		"Pause": la{"type": "Wait", "runAfter": la{}, "inputs": la{"interval": la{"count": 1, "unit": "Hour"}}},
	}))
	a.create(t, "ticker", recurrenceDef(la{"frequency": "Minute", "interval": 1, "startTime": "2026-01-01T00:00:00Z"}))

	oldURL := a.callback(t, "greeter")
	code, _, _ := a.post(t, oldURL, `{"name":"bob"}`)
	require.Equal(t, http.StatusOK, code)

	code, _, _ = a.post(t, a.callback(t, "waiter"), "{}")
	require.Equal(t, http.StatusAccepted, code)
	a.clock.waitTimers(t, 1)

	snap := a.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b := newLogicStack(t)
	require.NoError(t, b.handler.Restore(t.Context(), snap))

	wf, err := b.factory.NewWorkflowsClient().Get(t.Context(), logicRG, "greeter", nil)
	require.NoError(t, err)
	assert.NotNil(t, wf.Properties.Definition)

	run := b.latestRun(t, "greeter")
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *run.Properties.Status)

	acts := b.actions(t, "greeter")
	assert.Equal(t, "hello bob", b.content(t, acts["Compose"].Properties.OutputsLink))

	wrun := b.latestRun(t, "waiter")
	assert.Equal(
		t,
		armlogic.WorkflowStatusCancelled,
		*wrun.Properties.Status,
		"in-flight runs do not survive a restore",
	)
	assert.Equal(t, "ServiceRestarted", *wrun.Properties.Code)

	// access keys persist, so a URL issued before the snapshot keeps working (modulo the listener address).
	moved := b.url + oldURL[len(a.url):]
	assert.Equal(t, moved, b.callback(t, "greeter"))

	code, raw, _ := b.post(t, moved, `{"name":"carol"}`)
	require.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"greeting":"hello carol"}`, string(raw))

	b.clock.Advance(0)
	b.clock.Advance(2 * time.Minute)
	b.waitRuns(t)
	assert.GreaterOrEqual(t, b.runCount(t, "ticker"), 2, "recurrence timers are re-armed on restore")

	vp, err := b.factory.NewWorkflowVersionsClient().NewListPager(logicRG, "greeter", nil).NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, vp.Value, 1)
}

func TestLogicResetAndGroupDelete(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "greeter", greetingDef())
	s.create(t, "ticker", recurrenceDef(la{"frequency": "Minute", "interval": 1, "startTime": "2026-01-01T00:00:00Z"}))

	s.handler.Reset()

	for _, name := range []string{"greeter", "ticker"} {
		_, err := s.factory.NewWorkflowsClient().Get(t.Context(), logicRG, name, nil)
		assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
	}

	s.clock.Advance(time.Hour)
	assert.Zero(t, s.clock.pending(false))
}
