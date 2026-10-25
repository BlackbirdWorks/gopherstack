package gcpworkflows_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

func TestExecutionResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		arg        string
		wantResult string
		wantErr    string
		wantStep   string
		wantState  executionspb.Execution_State
	}{
		{
			name: "argument in result out", source: hello, arg: `{"name":"gopher"}`,
			wantState: executionspb.Execution_SUCCEEDED, wantResult: `"hello gopher"`,
		},
		{
			name:      "structured result",
			source:    "main:\n  steps:\n    - r:\n        return: {n: 1, l: [1.5, null, true]}\n",
			wantState: executionspb.Execution_SUCCEEDED, wantResult: `{"l":[1.5,null,true],"n":1}`,
		},
		{
			name:      "no return yields null",
			source:    "main:\n  steps:\n    - a:\n        assign: [{x: 1}]\n",
			wantState: executionspb.Execution_SUCCEEDED, wantResult: `null`,
		},
		{
			name: "uncaught raise fails the execution",
			source: "main:\n  steps:\n    - ok:\n        assign: [{x: 1}]\n" +
				"    - boom:\n        raise: {code: 7, message: bad}\n",
			wantState: executionspb.Execution_FAILED,
			wantErr:   `{"code":7,"message":"bad"}`,
			wantStep:  "boom",
		},
		{
			name:      "runtime error carries tags",
			source:    "main:\n  steps:\n    - div:\n        return: '${1 / 0}'\n",
			wantState: executionspb.Execution_FAILED, wantErr: `ZeroDivisionError`, wantStep: "div",
		},
		{
			name:      "raise in a subworkflow reports its routine",
			source:    "main:\n  steps:\n    - c:\n        call: inner\ninner:\n  steps:\n    - fail:\n        raise: oops\n",
			wantState: executionspb.Execution_FAILED,
			wantErr:   `"oops"`,
			wantStep:  "fail",
		},
	}

	e := newEnv(t)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.run(t, "exec_"+string(rune('a'+i)), tt.source, tt.arg)

			assert.Equal(t, tt.wantState, got.GetState())
			assert.True(t, got.GetStartTime().IsValid())
			assert.True(t, got.GetEndTime().IsValid())
			assert.NotNil(t, got.GetDuration())
			assert.Nil(t, got.GetStatus())

			if tt.wantState == executionspb.Execution_SUCCEEDED {
				assert.JSONEq(t, tt.wantResult, got.GetResult())
				assert.Nil(t, got.GetError())

				return
			}

			assert.Empty(t, got.GetResult())
			assert.Contains(t, got.GetError().GetPayload(), tt.wantErr)
			assert.Contains(t, got.GetError().GetContext(), `in step "`+tt.wantStep+`"`)
			require.NotEmpty(t, got.GetError().GetStackTrace().GetElements())
			assert.Equal(t, tt.wantStep, got.GetError().GetStackTrace().GetElements()[0].GetStep())
		})
	}
}

func TestCreateExecutionShapeAndErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "shape_exec", hello, func(w *workflowspb.Workflow) {
		w.Labels = map[string]string{"from": "workflow", "both": "wf"}
		w.CallLogLevel = workflowspb.Workflow_LOG_ALL_CALLS
	})

	ex, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{
		Parent: w.GetName(),
		Execution: &executionspb.Execution{
			Argument: `{"name":"x"}`, Labels: map[string]string{"both": "exec"},
		},
	})
	require.NoError(t, err)

	assert.Regexp(t, `^`+w.GetName()+`/executions/[0-9a-f-]{36}$`, ex.GetName())
	assert.Equal(t, executionspb.Execution_ACTIVE, ex.GetState())
	assert.Equal(t, w.GetRevisionId(), ex.GetWorkflowRevisionId())
	assert.JSONEq(t, `{"name":"x"}`, ex.GetArgument())
	assert.Equal(t, map[string]string{"from": "workflow", "both": "exec"}, ex.GetLabels())
	assert.Equal(t, executionspb.Execution_LOG_ALL_CALLS, ex.GetCallLogLevel())

	e.await(t, ex.GetName())

	tests := []struct {
		name   string
		parent string
		arg    string
		want   codes.Code
	}{
		{"bad parent", "nope", "", codes.InvalidArgument},
		{"missing workflow", parent + "/workflows/ghost", "", codes.NotFound},
		{"argument is not json", w.GetName(), "{", codes.InvalidArgument},
		{"argument too large", w.GetName(), `"` + string(make([]byte, 40000)) + `"`, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{
				Parent: tt.parent, Execution: &executionspb.Execution{Argument: tt.arg},
			})
			assert.Equal(t, tt.want, code(cerr), "%v", cerr)
		})
	}
}

func TestExecutionRunsTheLatestRevision(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "rev_exec", "main:\n  steps:\n    - r:\n        return: one\n")
	first := e.await(t, e.execute(t, w.GetName(), "").GetName())

	op, err := e.wf.UpdateWorkflow(t.Context(), &workflowspb.UpdateWorkflowRequest{
		Workflow: &workflowspb.Workflow{
			Name: w.GetName(),
			SourceCode: &workflowspb.Workflow_SourceContents{
				SourceContents: "main:\n  steps:\n    - r:\n        return: two\n",
			},
		},
	})
	require.NoError(t, err)

	updated, err := op.Wait(t.Context())
	require.NoError(t, err)

	second := e.await(t, e.execute(t, w.GetName(), "").GetName())

	assert.Equal(t, `"one"`, first.GetResult())
	assert.Equal(t, w.GetRevisionId(), first.GetWorkflowRevisionId())
	assert.Equal(t, `"two"`, second.GetResult())
	assert.Equal(t, updated.GetRevisionId(), second.GetWorkflowRevisionId())
}

func TestCancelExecution(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.clock.holdSleeps()

	w := e.createWorkflow(t, "cancel_me", `
main:
  steps:
    - before:
        assign: [{x: 1}]
    - nap:
        call: sys.sleep
        args: {seconds: 3600}
    - after:
        return: done
`)
	ex := e.execute(t, w.GetName(), "")

	<-e.clock.sleeping

	active, err := e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: ex.GetName()})
	require.NoError(t, err)
	assert.Equal(t, executionspb.Execution_ACTIVE, active.GetState())
	require.Len(t, active.GetStatus().GetCurrentSteps(), 1)
	assert.Equal(t, "nap", active.GetStatus().GetCurrentSteps()[0].GetStep())
	assert.Equal(t, "main", active.GetStatus().GetCurrentSteps()[0].GetRoutine())

	cancelled, err := e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: ex.GetName()})
	require.NoError(t, err)
	assert.Equal(t, executionspb.Execution_CANCELLED, cancelled.GetState())
	assert.True(t, cancelled.GetEndTime().IsValid())
	assert.NotNil(t, cancelled.GetError())

	final := e.await(t, ex.GetName())
	assert.Equal(t, executionspb.Execution_CANCELLED, final.GetState())
	assert.Empty(t, final.GetResult(), "the runner stopped at the sleep")

	_, err = e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: ex.GetName()})
	assert.Equal(t, codes.FailedPrecondition, code(err))

	_, err = e.ex.CancelExecution(
		t.Context(),
		&executionspb.CancelExecutionRequest{Name: parent + "/workflows/x/executions/ghost"},
	)
	assert.Equal(t, codes.NotFound, code(err))
}

func TestSleepAndRetryUseTheInjectedClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	got := e.run(t, "clocked", `
main:
  steps:
    - init:
        assign: [{n: 0}]
    - nap:
        call: sys.sleep
        args: {seconds: 90}
    - t:
        try:
          steps:
            - bump:
                assign: [{n: "${n + 1}"}]
            - maybe:
                switch:
                  - condition: ${n < 3}
                    raise: {code: 503, tags: ["HttpError"]}
        retry:
          predicate: ${http.default_retry_predicate}
          max_retries: 5
          backoff: {initial_delay: 2, max_delay: 60, multiplier: 3}
    - r:
        return: ${n}
`, "")

	assert.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState())
	assert.Equal(t, "3", got.GetResult())
	assert.Equal(t, []time.Duration{90 * time.Second, 2 * time.Second, 6 * time.Second}, e.clock.slept())
}

func TestGetAndListExecutionViews(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "views", hello)

	names := make([]string, 0, 3)

	for _, arg := range []string{`{"name":"a"}`, `{"name":"b"}`, `{"name":"c"}`} {
		ex := e.execute(t, w.GetName(), arg)
		e.await(t, ex.GetName())
		names = append(names, ex.GetName())
		e.clock.mu.Lock()
		e.clock.now = e.clock.now.Add(time.Minute)
		e.clock.mu.Unlock()
	}

	basic, err := e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{
		Name: names[0], View: executionspb.ExecutionView_BASIC,
	})
	require.NoError(t, err)
	assert.Empty(t, basic.GetArgument())
	assert.Empty(t, basic.GetResult())
	assert.Equal(t, executionspb.Execution_SUCCEEDED, basic.GetState())
	assert.NotEmpty(t, basic.GetWorkflowRevisionId())

	full, err := e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: names[0]})
	require.NoError(t, err)
	assert.Equal(t, `"hello a"`, full.GetResult())

	collect := func(req *executionspb.ListExecutionsRequest) []*executionspb.Execution {
		it := e.ex.ListExecutions(t.Context(), req)

		var out []*executionspb.Execution

		for {
			ex, nerr := it.Next()
			if errors.Is(nerr, iterator.Done) {
				return out
			}

			require.NoError(t, nerr)
			out = append(out, ex)
		}
	}

	list := collect(&executionspb.ListExecutionsRequest{Parent: w.GetName(), PageSize: 2})
	require.Len(t, list, 3)
	assert.Equal(t, names[2], list[0].GetName(), "newest first by default")
	assert.Empty(t, list[0].GetResult(), "list defaults to the basic view")

	full3 := collect(&executionspb.ListExecutionsRequest{Parent: w.GetName(), View: executionspb.ExecutionView_FULL})
	assert.Equal(t, `"hello c"`, full3[0].GetResult())

	asc := collect(&executionspb.ListExecutionsRequest{Parent: w.GetName(), OrderBy: "startTime"})
	assert.Equal(t, names[0], asc[0].GetName())

	byState := collect(&executionspb.ListExecutionsRequest{Parent: w.GetName(), Filter: `state="FAILED"`})
	assert.Empty(t, byState)

	byState = collect(
		&executionspb.ListExecutionsRequest{Parent: w.GetName(), Filter: `state="SUCCEEDED" AND executionID!="zzz"`},
	)
	assert.Len(t, byState, 3)

	after := collect(&executionspb.ListExecutionsRequest{
		Parent: w.GetName(), Filter: `startTime>"` + list[1].GetStartTime().AsTime().Format(time.RFC3339Nano) + `"`,
	})
	require.Len(t, after, 1)
	assert.Equal(t, names[2], after[0].GetName())

	it := e.ex.ListExecutions(
		t.Context(),
		&executionspb.ListExecutionsRequest{Parent: w.GetName(), Filter: `bogus="1"`},
	)
	_, err = it.Next()
	assert.Equal(t, codes.InvalidArgument, code(err))

	it = e.ex.ListExecutions(t.Context(), &executionspb.ListExecutionsRequest{Parent: parent + "/workflows/ghost"})
	_, err = it.Next()
	assert.Equal(t, codes.NotFound, code(err))
}

func TestDeleteWorkflowRemovesExecutions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "gone", hello)
	ex := e.execute(t, w.GetName(), `{"name":"x"}`)
	e.await(t, ex.GetName())

	op, err := e.wf.DeleteWorkflow(t.Context(), &workflowspb.DeleteWorkflowRequest{Name: w.GetName()})
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	_, err = e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: ex.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestBuiltInEnvironment(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "envwf", `
main:
  steps:
    - r:
        return:
          project: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          location: ${sys.get_env("GOOGLE_CLOUD_LOCATION")}
          workflow: ${sys.get_env("GOOGLE_CLOUD_WORKFLOW_ID")}
          revision: ${sys.get_env("GOOGLE_CLOUD_WORKFLOW_REVISION_ID")}
          execution: ${sys.get_env("GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID")}
          account: ${sys.get_env("GOOGLE_CLOUD_SERVICE_ACCOUNT_NAME")}
          number: ${sys.get_env("GOOGLE_CLOUD_PROJECT_NUMBER")}
          mine: ${sys.get_env("MODE")}
          missing: ${sys.get_env("NOPE", "fallback")}
`, func(w *workflowspb.Workflow) {
		w.UserEnvVars = map[string]string{"MODE": "test"}
		w.ServiceAccount = "me@test-project.iam.gserviceaccount.com"
	})
	ex := e.await(t, e.execute(t, w.GetName(), "").GetName())

	assert.JSONEq(t, `{"project":"test-project","location":"us-central1","workflow":"envwf",`+
		`"revision":"`+w.GetRevisionId()+`","execution":"`+ex.GetName()[len(w.GetName())+len("/executions/"):]+`",`+
		`"account":"me@test-project.iam.gserviceaccount.com","number":"`+projectNumberOf(ex)+
		`","mine":"test","missing":"fallback"}`,
		ex.GetResult())
}
