package gcpworkflows_test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRESTConnectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svc     string
		step    string
		wantReq string
	}{
		{
			name: "tasks queue create", svc: "cloudtasks",
			step: `call: googleapis.cloudtasks.v2.projects.locations.queues.create
        args: {parent: "projects/p/locations/l", body: {name: "projects/p/locations/l/queues/q"}}
        result: out`,
			wantReq: "POST /v2/projects/p/locations/l/queues",
		},
		{
			name: "tasks run", svc: "cloudtasks",
			step: `call: googleapis.cloudtasks.v2.projects.locations.queues.tasks.run
        args: {name: "projects/p/locations/l/queues/q/tasks/t"}
        result: out`,
			wantReq: "POST /v2/projects/p/locations/l/queues/q/tasks/t:run",
		},
		{
			name: "build get", svc: "cloudbuild",
			step: `call: googleapis.cloudbuild.v1.projects.builds.get
        args: {projectId: p, id: b1}
        result: out`,
			wantReq: "GET /v1/projects/p/builds/b1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true}`))
			})

			e := newEnv(t)
			e.handler.Backend.SetEndpoints(endpoints{tt.svc: rec.srv.URL})

			src := "main:\n  steps:\n    - c:\n        " + tt.step + "\n    - r:\n        return: ${out}"
			got := e.run(t, "rest_conn", src, "")
			require.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
			assert.JSONEq(t, `{"ok":true}`, got.GetResult())

			reqs := rec.requests()
			require.Len(t, reqs, 1)
			assert.True(t, strings.HasPrefix(reqs[0], tt.wantReq), reqs[0])
		})
	}
}

const buildStep = `
main:
  steps:
    - b:
        call: googleapis.cloudbuild.v1.projects.builds.create
        args:
          projectId: p
          body: {}
%s
        result: out
    - r:
        return: ${out}
`

func TestLROConnectorPolling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     string
		opBody     string
		wantResult string
		wantSleeps []time.Duration
		finishAt   int32
		wantState  executionspb.Execution_State
	}{
		{
			name: "polls with backoff", finishAt: 3, opBody: `"response":{"id":"b1"}`,
			wantResult: `{"id":"b1"}`, wantState: executionspb.Execution_SUCCEEDED,
			wantSleeps: []time.Duration{time.Second, 1250 * time.Millisecond},
		},
		{
			name: "custom policy", finishAt: 2, opBody: `"response":{"id":"b1"}`,
			params:     "          connector_params: {polling_policy: {initial_delay: 4, multiplier: 2, max_delay: 5}}",
			wantResult: `{"id":"b1"}`, wantState: executionspb.Execution_SUCCEEDED,
			wantSleeps: []time.Duration{4 * time.Second},
		},
		{
			name: "operation error", finishAt: 2, opBody: `"error":{"code":3,"message":"build broke"}`,
			wantState: executionspb.Execution_FAILED, wantSleeps: []time.Duration{time.Second},
		},
		{
			name: "skip polling", finishAt: 9, opBody: `"response":{}`,
			params:     "          connector_params: {skip_polling: true}",
			wantResult: `{"done":false,"name":"projects/p/operations/op1"}`,
			wantState:  executionspb.Execution_SUCCEEDED,
		},
		{
			name: "timeout", finishAt: 1000, opBody: `"response":{}`,
			params:    "          connector_params: {timeout: 2, polling_policy: {initial_delay: 1, multiplier: 1}}",
			wantState: executionspb.Execution_FAILED, wantSleeps: []time.Duration{time.Second, time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var polls atomic.Int32

			rec := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				if r.Method == http.MethodPost {
					_, _ = w.Write([]byte(`{"name":"projects/p/operations/op1","done":false}`))

					return
				}

				if polls.Add(1) >= tt.finishAt-1 {
					_, _ = w.Write([]byte(`{"name":"projects/p/operations/op1","done":true,` + tt.opBody + `}`))

					return
				}

				_, _ = w.Write([]byte(`{"name":"projects/p/operations/op1","done":false}`))
			})

			e := newEnv(t)
			e.handler.Backend.SetEndpoints(endpoints{"cloudbuild": rec.srv.URL})

			src := strings.Replace(buildStep, "%s", tt.params, 1)
			got := e.run(t, "lro_conn", src, "")
			require.Equal(t, tt.wantState, got.GetState(), got.GetError().GetPayload())

			if tt.wantResult != "" {
				assert.JSONEq(t, tt.wantResult, got.GetResult())
			}

			assert.Equal(t, tt.wantSleeps, e.clock.slept())
		})
	}
}

func TestExecutionsConnectorTargetsOwnListener(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	child := e.createWorkflow(t, "child", "main:\n  steps:\n    - r:\n        return: 7")

	got := e.run(t, "parent_wf", `
main:
  steps:
    - c:
        call: googleapis.workflowexecutions.v1.projects.locations.workflows.executions.create
        args:
          parent: `+child.GetName()+`
          body: {}
        result: out
    - r:
        return: ${out.name}
`, "")
	require.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
	assert.Contains(t, got.GetResult(), child.GetName()+"/executions/")
}
