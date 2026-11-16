package gcpworkflows_test

import (
	"testing"

	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const childSource = `
main:
  params: [x]
  steps:
    - chk:
        switch:
          - condition: ${x < 0}
            raise: "negative"
    - r:
        return: ${x * 2}
`

func TestExperimentalExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "run returns the result",
			body: `
        call: experimental.executions.run
        args: {workflow_id: child, argument: 21}
        result: out`,
			want: `{"out":42}`,
		},
		{
			name: "map returns results in order",
			body: `
        call: experimental.executions.map
        args: {workflow_id: child, arguments: [1, 2, 3]}
        result: out`,
			want: `{"out":[2,4,6]}`,
		},
		{
			name: "failed child raises ExecutionError",
			body: `
        try:
          call: experimental.executions.run
          args: {workflow_id: child, argument: -1}
          result: out
        except:
          as: e
          steps:
            - h:
                assign: [{out: "${e.tags[0]}"}]`,
			want: `{"out":"ExecutionError"}`,
		},
		{
			name: "missing workflow raises ExecutionError",
			body: `
        try:
          call: experimental.executions.run
          args: {workflow_id: ghost}
          result: out
        except:
          as: e
          steps:
            - h:
                assign: [{out: "${e.tags[0]}"}]`,
			want: `{"out":"ExecutionError"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.clock.holdSleeps()
			e.createWorkflow(t, "child", childSource)

			src := "main:\n  steps:\n    - init:\n        assign: [{out: null}]\n    - go:" + tt.body +
				"\n    - done:\n        return:\n          out: ${out}\n"

			ex := e.run(t, "parent", src, "")
			require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState(), ex.GetError().GetPayload())
			assert.JSONEq(t, tt.want, ex.GetResult())
		})
	}
}

func TestHTTPToEmulatedGoogleHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		service string
		wantHit string
	}{
		{
			"pubsub", "https://pubsub.googleapis.com/v1/projects/p/topics/t",
			"pubsub", "GET /v1/projects/p/topics/t auth=",
		},
		{
			"secret manager", "https://secretmanager.googleapis.com/v1/projects/p/secrets/s",
			"secretmanager", "GET /v1/projects/p/secrets/s auth=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			rec := newRecorder(t, nil)
			e.handler.Backend.SetEndpoints(endpoints{tt.service: rec.srv.URL})

			src := "main:\n  steps:\n    - g:\n        call: http.get\n        args: {url: " + tt.url +
				"}\n        result: r\n    - d:\n        return: ${r.code}\n"
			ex := e.run(t, "caller", src, "")
			require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState(), ex.GetError().GetPayload())
			assert.Equal(t, []string{tt.wantHit}, rec.requests())
		})
	}
}
