package gcpworkflows_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const callLogSource = `
main:
  steps:
    - a:
        call: add
        args: {x: 1, headers: {Authorization: "Bearer s3cret"}}
        result: r
    - t:
        try:
          steps:
            - boom:
                call: bad
        except:
          as: e
          steps:
            - ok:
                assign: [{handled: true}]
    - done:
        return: ${r}
add:
  params: [x, headers]
  steps:
    - s:
        return: ${x + 1}
bad:
  steps:
    - r:
        raise: "oops"
`

func TestCallLogging(t *testing.T) {
	t.Parallel()

	const (
		begun   = "BEGUN"
		ok      = "SUCCEEDED"
		raised  = "EXCEPTION_RAISED"
		handled = "EXCEPTION_HANDLED"
	)

	tests := []struct {
		name  string
		want  []string
		level workflowspb.Workflow_CallLogLevel
	}{
		{"all calls", []string{begun, ok, begun, raised, handled}, workflowspb.Workflow_LOG_ALL_CALLS},
		{"errors only", []string{raised}, workflowspb.Workflow_LOG_ERRORS_ONLY},
		{"none", []string{}, workflowspb.Workflow_LOG_NONE},
		{"unspecified", []string{}, workflowspb.Workflow_CALL_LOG_LEVEL_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sink := &captureSink{}
			e.handler.Backend.SetLogSink(sink)

			w := e.createWorkflow(t, "calls", callLogSource, func(w *workflowspb.Workflow) {
				w.CallLogLevel = tt.level
			})
			ex := e.await(t, e.execute(t, w.GetName(), "").GetName())
			require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState(), ex.GetError().GetPayload())

			sink.mu.Lock()
			defer sink.mu.Unlock()

			got := make([]string, 0, len(sink.entries))

			for _, en := range sink.entries {
				assert.Equal(t, "projects/test-project/logs/workflows.googleapis.com%2Fengine_call", en.GetLogName())
				assert.Equal(t, "workflows.googleapis.com/Workflow", en.GetResource().GetType())

				p := en.GetJsonPayload().AsMap()
				got = append(got, p["state"].(string))

				assert.NotContains(t, en.GetJsonPayload().String(), "s3cret")

				if p["state"] == raised {
					assert.Equal(t, "ERROR", en.GetSeverity().String())
					assert.Equal(t, "bad", p["callee"])
					assert.Equal(t, "boom", p["step"])
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExecutionCallLevelOverridesWorkflow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sink := &captureSink{}
	e.handler.Backend.SetLogSink(sink)

	w := e.createWorkflow(t, "override", callLogSource, func(w *workflowspb.Workflow) {
		w.CallLogLevel = workflowspb.Workflow_LOG_ALL_CALLS
	})

	ex, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{
		Parent:    w.GetName(),
		Execution: &executionspb.Execution{CallLogLevel: executionspb.Execution_LOG_NONE},
	})
	require.NoError(t, err)
	e.await(t, ex.GetName())

	sink.mu.Lock()
	defer sink.mu.Unlock()

	assert.Empty(t, sink.entries)
}

const redactSource = `
main:
  steps:
    - deep:
        assign: [{d: {Authorization: "Bearer deepsecret"}}]
    - nest:
        for:
          value: i
          range: [1, 40]
          steps:
            - wrap:
                assign: [{d: {n: '${d}'}}]
    - a:
        call: add
        args:
          x: 1
          headers:
            Cookie: "sid=cookiesecret"
            X-Goog-Api-Key: apikeysecret
            Proxy-Authorization: "Basic proxysecret"
            Keep: visible
          deep: ${d}
add:
  params: [x, headers, deep]
  steps:
    - s:
        return: ${x}
`

func TestCallLogRedactsCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sink := &captureSink{}
	e.handler.Backend.SetLogSink(sink)

	w := e.createWorkflow(t, "redact", redactSource, func(w *workflowspb.Workflow) {
		w.CallLogLevel = workflowspb.Workflow_LOG_ALL_CALLS
	})
	ex := e.await(t, e.execute(t, w.GetName(), "").GetName())
	require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState(), ex.GetError().GetPayload())

	sink.mu.Lock()
	defer sink.mu.Unlock()

	require.NotEmpty(t, sink.entries)

	var sb strings.Builder
	for _, en := range sink.entries {
		sb.WriteString(en.GetJsonPayload().String())
	}

	all := sb.String()

	for _, secret := range []string{"cookiesecret", "apikeysecret", "proxysecret", "deepsecret"} {
		assert.NotContains(t, all, secret)
	}

	assert.Contains(t, all, "visible")
}
