package gcpworkflows_test

import (
	"testing"

	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutionsSystemLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		arg       string
		wantState string
		wantEnd   string
		wantSev   string
		wantExec  executionspb.Execution_State
	}{
		{
			name: "success", arg: `{"k":1}`, wantState: "SUCCEEDED", wantSev: "INFO", wantEnd: "success",
			wantExec: executionspb.Execution_SUCCEEDED,
			source:   "main:\n  params: [args]\n  steps:\n    - r:\n        return: ${args.k}",
		},
		{
			name: "failure", wantState: "FAILED", wantSev: "ERROR", wantEnd: "failure",
			wantExec: executionspb.Execution_FAILED,
			source:   "main:\n  steps:\n    - boom:\n        raise: \"bad\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sink := &captureSink{}
			e.handler.Backend.SetLogSink(sink)

			ex := e.run(t, "syslog", tt.source, tt.arg)
			require.Equal(t, tt.wantExec, ex.GetState())
			e.handler.Backend.Close()

			sink.mu.Lock()
			defer sink.mu.Unlock()

			require.Len(t, sink.system, 2)

			start, end := sink.system[0], sink.system[1]
			assert.Equal(t,
				"projects/test-project/logs/workflows.googleapis.com%2Fexecutions_system", start.GetLogName())
			assert.Equal(t, "workflows.googleapis.com/Workflow", start.GetResource().GetType())
			assert.NotEmpty(t, start.GetLabels()["workflows.googleapis.com/execution_id"])

			sp := start.GetJsonPayload().AsMap()
			assert.Equal(t, "ACTIVE", sp["state"])
			assert.Contains(t, sp, "start")
			assert.NotEmpty(t, sp["message"])

			if tt.arg != "" {
				assert.JSONEq(t, tt.arg, sp["start"].(map[string]any)["argument"].(string))
			}

			ep := end.GetJsonPayload().AsMap()
			assert.Equal(t, tt.wantState, ep["state"])
			assert.Contains(t, ep, tt.wantEnd)
			assert.Equal(t, tt.wantSev, end.GetSeverity().String())
			assert.Equal(t, ex.GetEndTime().AsTime(), end.GetTimestamp().AsTime())
		})
	}
}

func TestExecutionsSystemLogCancelled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sink := &captureSink{}
	e.handler.Backend.SetLogSink(sink)
	e.clock.holdSleeps()

	w := e.createWorkflow(t, "cancelme",
		"main:\n  steps:\n    - nap:\n        call: sys.sleep\n        args: {seconds: 100}")
	ex := e.execute(t, w.GetName(), "")
	<-e.clock.sleeping

	_, err := e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: ex.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Close()

	sink.mu.Lock()
	defer sink.mu.Unlock()

	require.Len(t, sink.system, 2)
	assert.Equal(t, "CANCELLED", sink.system[1].GetJsonPayload().AsMap()["state"])
}
