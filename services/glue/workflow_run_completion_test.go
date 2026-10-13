package glue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestWorkflowRun_CompletesWhenActionsFinish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		advance    time.Duration
		wantDone   bool
		wantRerun  bool
	}{
		{name: "actions in flight", advance: 0, wantStatus: "RUNNING", wantRerun: false},
		{name: "actions finished", advance: time.Hour, wantStatus: "COMPLETED", wantDone: true, wantRerun: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			h := glue.NewHandler(backend)
			setupWorkflowRunFixture(t, h)
			dispatchNewOp(t, h, "UpdateWorkflow", map[string]any{"Name": "statswf", "MaxConcurrentRuns": 1})

			out := dispatchNewOp(t, h, "StartWorkflowRun", map[string]any{"Name": "statswf"})
			runID, _ := out["RunId"].(string)
			require.NotEmpty(t, runID)

			if tt.advance > 0 {
				glue.AdvanceStatesForTest(backend, time.Now().Add(tt.advance))
			}

			got, err := backend.GetWorkflowRun("statswf", runID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantDone, got.CompletedOn > 0)

			_, err = backend.StartWorkflowRun("statswf")
			assert.Equal(t, tt.wantRerun, err == nil)
		})
	}
}
