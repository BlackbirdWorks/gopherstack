package glue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// setupWorkflowRunFixture creates a job, crawler, workflow and an ON_DEMAND
// entry trigger (no Predicate) wiring both into the workflow -- the "start
// trigger" a real workflow run fires per AWS's workflow docs.
func setupWorkflowRunFixture(t *testing.T, h *glue.Handler) {
	t.Helper()

	dispatchNewOp(t, h, "CreateJob", map[string]any{
		"Name": "statsjob", "Role": "role1", "Command": map[string]any{"Name": "glueetl"},
	})
	dispatchNewOp(t, h, "CreateCrawler", map[string]any{
		"Name": "statscrawler", "Role": "role1",
		"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/data/"}}},
	})
	dispatchNewOp(t, h, "CreateWorkflow", map[string]any{"Name": "statswf"})
	dispatchNewOp(t, h, "CreateTrigger", map[string]any{
		"Name": "statswf-trigger", "Type": "ON_DEMAND", "WorkflowName": "statswf",
		"Actions": []map[string]any{
			{"JobName": "statsjob"},
			{"CrawlerName": "statscrawler"},
		},
	})
}

// TestStartWorkflowRun_LinksFiredActions proves StartWorkflowRun now actually
// fires the workflow's entry trigger and links the resulting job run/crawl to
// the new WorkflowRun: before this change StartWorkflowRun only wrote a
// bookkeeping record and started nothing, so Statistics would always have
// read TotalActions == 0.
func TestStartWorkflowRun_LinksFiredActions(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)
	setupWorkflowRunFixture(t, h)

	startOut := dispatchNewOp(t, h, "StartWorkflowRun", map[string]any{"Name": "statswf"})
	runID, _ := startOut["RunId"].(string)
	require.NotEmpty(t, runID)

	// Immediately after firing, the crawl is RUNNING and the job run is still
	// STARTING (jobTransitionDelay hasn't elapsed) -- GetWorkflowRun does not
	// itself advance state (see comment on it), so this is deterministic.
	runOut := dispatchNewOp(t, h, "GetWorkflowRun", map[string]any{"Name": "statswf", "RunId": runID})
	stats, ok := runOut["Run"].(map[string]any)["Statistics"].(map[string]any)
	require.True(t, ok, "Statistics must be present")
	assert.Equal(t, 2, statInt(t, stats, "TotalActions"))
	assert.Equal(t, 1, statInt(t, stats, "RunningActions"), "only the crawl has reached RUNNING yet")
	assert.NotContains(t, stats, "SucceededActions")

	// TriggerName is JobRun's real wire field; WorkflowRunID is internal-only
	// and must never reach the wire under either casing.
	jobRunsOut := dispatchNewOp(t, h, "GetJobRuns", map[string]any{"JobName": "statsjob"})
	jobRuns, ok := jobRunsOut["JobRuns"].([]any)
	require.True(t, ok)
	require.Len(t, jobRuns, 1)
	jr, ok := jobRuns[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "statswf-trigger", jr["TriggerName"])
	assert.NotContains(t, jr, "WorkflowRunId")
	assert.NotContains(t, jr, "workflowRunId")

	// Let both actions reach a terminal state and re-check.
	glue.AdvanceStatesForTest(backend, time.Now().Add(time.Hour))

	runOut = dispatchNewOp(t, h, "GetWorkflowRun", map[string]any{"Name": "statswf", "RunId": runID})
	stats, ok = runOut["Run"].(map[string]any)["Statistics"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 2, statInt(t, stats, "TotalActions"))
	assert.Equal(t, 2, statInt(t, stats, "SucceededActions"), "job SUCCEEDED + crawl COMPLETED")
	assert.NotContains(t, stats, "RunningActions")
	assert.NotContains(t, stats, "FailedActions")
}

// statInt extracts an integer-valued Statistics field decoded from JSON (as
// float64) into an int, so callers can use exact integer assertions instead
// of a float-tolerance comparison for what are always whole action counts.
func statInt(t *testing.T, stats map[string]any, key string) int {
	t.Helper()

	v, ok := stats[key].(float64)
	require.True(t, ok, "stats[%q] missing or not a number", key)

	return int(v)
}

// TestStartTrigger_DoesNotLinkWorkflowRun verifies that firing a trigger
// directly via StartTrigger -- bypassing StartWorkflowRun -- still sets the
// real TriggerName field but creates no WorkflowRun, so there is nothing to
// link the started job run to. Fabricating a run here would misattribute the
// action to a run that never happened.
func TestStartTrigger_DoesNotLinkWorkflowRun(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)
	setupWorkflowRunFixture(t, h)

	dispatchNewOp(t, h, "StartTrigger", map[string]any{"Name": "statswf-trigger"})

	runsOut := dispatchNewOp(t, h, "GetWorkflowRuns", map[string]any{"Name": "statswf"})
	runs, ok := runsOut["Runs"].([]any)
	require.True(t, ok)
	assert.Empty(t, runs, "StartTrigger must not fabricate a workflow run")

	jobRunsOut := dispatchNewOp(t, h, "GetJobRuns", map[string]any{"JobName": "statsjob"})
	jobRuns, ok := jobRunsOut["JobRuns"].([]any)
	require.True(t, ok)
	require.Len(t, jobRuns, 1)
	jr, ok := jobRuns[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "statswf-trigger", jr["TriggerName"])
}

// TestWorkflowRunStatistics_CrawlerStopped verifies StoppedActions comes from
// a crawl actually reaching CrawlHistoryEntry.State == "STOPPED" -- the only
// terminal state this backend's crawler reconciler ever assigns beyond
// RUNNING/COMPLETED (see finishCrawlHistoryLocked in crawlers.go).
func TestWorkflowRunStatistics_CrawlerStopped(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)

	dispatchNewOp(t, h, "CreateCrawler", map[string]any{
		"Name": "stopcrawler", "Role": "role1",
		"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/data/"}}},
	})
	dispatchNewOp(t, h, "CreateWorkflow", map[string]any{"Name": "stopwf"})
	dispatchNewOp(t, h, "CreateTrigger", map[string]any{
		"Name": "stopwf-trigger", "Type": "ON_DEMAND", "WorkflowName": "stopwf",
		"Actions": []map[string]any{{"CrawlerName": "stopcrawler"}},
	})

	startOut := dispatchNewOp(t, h, "StartWorkflowRun", map[string]any{"Name": "stopwf"})
	runID, _ := startOut["RunId"].(string)
	require.NotEmpty(t, runID)

	dispatchNewOp(t, h, "StopCrawler", map[string]any{"Name": "stopcrawler"})
	glue.AdvanceStatesForTest(backend, time.Now().Add(time.Hour))

	runOut := dispatchNewOp(t, h, "GetWorkflowRun", map[string]any{"Name": "stopwf", "RunId": runID})
	stats, ok := runOut["Run"].(map[string]any)["Statistics"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 1, statInt(t, stats, "TotalActions"))
	assert.Equal(t, 1, statInt(t, stats, "StoppedActions"))
	assert.NotContains(t, stats, "SucceededActions")
}

// TestWorkflowRunStatistics_PersistsAcrossRestart proves the internal
// WorkflowRunID link on JobRun/CrawlHistoryEntry survives a Snapshot/Restore
// round trip. Statistics itself is never persisted (computed live), so a
// correct non-zero count after Restore is only possible if the link fields
// -- the thing this change actually persists -- round-tripped through JSON.
// The exact bug class checked for here (a field that round-trips in memory
// but vanishes on restore) was found in sagemaker's cluster config fields.
func TestWorkflowRunStatistics_PersistsAcrossRestart(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)
	setupWorkflowRunFixture(t, h)

	startOut := dispatchNewOp(t, h, "StartWorkflowRun", map[string]any{"Name": "statswf"})
	runID, _ := startOut["RunId"].(string)
	require.NotEmpty(t, runID)

	glue.AdvanceStatesForTest(backend, time.Now().Add(time.Hour))

	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := glue.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, restored.Restore(t.Context(), snap))

	run, err := restored.GetWorkflowRun("statswf", runID)
	require.NoError(t, err)
	require.NotNil(t, run.Statistics)
	assert.Equal(t, 2, run.Statistics.TotalActions)
	assert.Equal(t, 2, run.Statistics.SucceededActions)
}
