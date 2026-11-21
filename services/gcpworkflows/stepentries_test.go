package gcpworkflows_test

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"testing"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	wfexec "google.golang.org/api/workflowexecutions/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

const stepSource = `
main:
  steps:
    - init:
        assign: [{total: 0}]
    - go:
        call: helper
        args: {n: 5}
        result: out
    - guarded:
        try:
          steps:
            - boom:
                raise: "bad"
        except:
          as: e
          steps:
            - handled:
                assign: [{caught: true}]
    - done:
        return: ${out}
helper:
  params: [n]
  steps:
    - double:
        return: ${n * 2}
`

func (e *env) stepsService(t *testing.T) *wfexec.Service {
	t.Helper()

	svc, err := wfexec.NewService(t.Context(),
		option.WithEndpoint("http://"+e.handler.Addr()), option.WithoutAuthentication())
	require.NoError(t, err)

	return svc
}

func (e *env) runSteps(t *testing.T, level workflowspb.ExecutionHistoryLevel) string {
	t.Helper()

	w := e.createWorkflow(t, "steps", stepSource, func(w *workflowspb.Workflow) { w.ExecutionHistoryLevel = level })
	ex := e.await(t, e.execute(t, w.GetName(), "").GetName())
	require.Equal(t, executionspb.Execution_SUCCEEDED, ex.GetState(), ex.GetError().GetPayload())

	return ex.GetName()
}

func stepNames(entries []*wfexec.StepEntry) []string {
	out := make([]string, len(entries))
	for i, en := range entries {
		out[i] = en.Routine + "." + en.Step
	}

	return out
}

func TestStepEntriesList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		orderBy   string
		want      []string
		wantTotal int64
	}{
		{
			name: "all in entry order",
			want: []string{
				"main.init", "main.go", "helper.double", "main.guarded", "main.boom", "main.handled", "main.done",
			},
			wantTotal: 7,
		},
		{name: "failed", filter: `state="STATE_FAILED"`, want: []string{"main.boom"}, wantTotal: 1},
		{
			name: "routine", filter: `routine="helper"`, want: []string{"helper.double"}, wantTotal: 1,
		},
		{
			name: "step type", filter: `stepType="STEP_ASSIGN"`, wantTotal: 2,
			want: []string{"main.init", "main.handled"},
		},
		{
			name: "entry id range", filter: `entryId>=6`, want: []string{"main.handled", "main.done"}, wantTotal: 2,
		},
		{
			name: "descending", orderBy: "entryId desc", wantTotal: 7,
			want: []string{
				"main.done", "main.handled", "main.boom", "main.guarded", "helper.double", "main.go", "main.init",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			name := e.runSteps(t, workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_BASIC)

			call := e.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries.List(name)
			if tt.filter != "" {
				call = call.Filter(tt.filter)
			}

			if tt.orderBy != "" {
				call = call.OrderBy(tt.orderBy)
			}

			got, err := call.Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, tt.want, stepNames(got.StepEntries))
			assert.Equal(t, tt.wantTotal, got.TotalSize)
		})
	}
}

func TestStepEntryContents(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	name := e.runSteps(t, workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_DETAILED)
	svc := e.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries

	list, err := svc.List(name).View("EXECUTION_ENTRY_VIEW_DETAILED").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, list.StepEntries, 7)

	byStep := map[string]*wfexec.StepEntry{}
	for _, en := range list.StepEntries {
		byStep[en.Routine+"."+en.Step] = en
	}

	call, helper, guarded, boom := byStep["main.go"], byStep["helper.double"], byStep["main.guarded"], byStep["main.boom"]

	assert.Equal(t, name+"/stepEntries/"+"2", call.Name)
	assert.Equal(t, "STEP_SUBWORKFLOW_CALL", call.StepType)
	assert.Equal(t, "STATE_SUCCEEDED", call.State)
	assert.Equal(t, call.EntryId, helper.NavigationInfo.Parent)
	assert.Equal(t, []int64{helper.EntryId}, []int64(call.NavigationInfo.Children))
	assert.Equal(t, byStep["main.init"].EntryId, call.NavigationInfo.Previous)
	assert.Equal(t, guarded.EntryId, call.NavigationInfo.Next)
	assert.Equal(t, "STEP_TRY_RETRY_EXCEPT", guarded.StepType)
	assert.Equal(t, "STATE_SUCCEEDED", guarded.State)

	assert.Equal(t, "STATE_FAILED", boom.State)
	require.NotNil(t, boom.Exception)
	assert.JSONEq(t, `"bad"`, boom.Exception.Payload)

	assert.NotEmpty(t, call.CreateTime)
	assert.GreaterOrEqual(t, call.UpdateTime, call.CreateTime)

	require.NotNil(t, byStep["main.done"].VariableData)

	vars := map[string]any{}
	require.NoError(t, json.Unmarshal(byStep["main.done"].VariableData.Variables, &vars))
	assert.EqualValues(t, 10, vars["out"])
	assert.Contains(t, vars, "total")

	one, err := svc.Get(call.Name).View("EXECUTION_ENTRY_VIEW_DETAILED").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, call.EntryId, one.EntryId)

	basic, err := svc.Get(call.Name).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Nil(t, basic.VariableData)
}

func TestStepEntriesBasicLevelOmitsVariables(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	name := e.runSteps(t, workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_BASIC)

	list, err := e.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries.List(name).
		View("EXECUTION_ENTRY_VIEW_DETAILED").Context(t.Context()).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.StepEntries)

	for _, en := range list.StepEntries {
		assert.Nil(t, en.VariableData)
	}
}

func TestStepEntriesPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	name := e.runSteps(t, workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_BASIC)
	svc := e.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries

	var ids []int64

	token := ""

	for range 10 {
		got, err := svc.List(name).PageSize(3).PageToken(token).Skip(0).Context(t.Context()).Do()
		require.NoError(t, err)
		assert.EqualValues(t, 7, got.TotalSize)

		for _, en := range got.StepEntries {
			ids = append(ids, en.EntryId)
		}

		if token = got.NextPageToken; token == "" {
			break
		}
	}

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7}, ids)

	skipped, err := svc.List(name).Skip(5).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, []string{"main.handled", "main.done"}, stepNames(skipped.StepEntries))
}

func TestStepEntriesErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(svc *wfexec.ProjectsLocationsWorkflowsExecutionsStepEntriesService, name string) error
		name string
		want int
	}{
		{
			name: "unknown execution",
			call: func(svc *wfexec.ProjectsLocationsWorkflowsExecutionsStepEntriesService, _ string) error {
				_, err := svc.List(parent + "/workflows/nope/executions/x").Do()

				return err
			},
			want: http.StatusNotFound,
		},
		{
			name: "unknown entry",
			call: func(svc *wfexec.ProjectsLocationsWorkflowsExecutionsStepEntriesService, name string) error {
				_, err := svc.Get(name + "/stepEntries/9999").Do()

				return err
			},
			want: http.StatusNotFound,
		},
		{
			name: "bad filter",
			call: func(svc *wfexec.ProjectsLocationsWorkflowsExecutionsStepEntriesService, name string) error {
				_, err := svc.List(name).Filter("nosuch=1").Do()

				return err
			},
			want: http.StatusBadRequest,
		},
		{
			name: "bad order",
			call: func(svc *wfexec.ProjectsLocationsWorkflowsExecutionsStepEntriesService, name string) error {
				_, err := svc.List(name).OrderBy("nosuch").Do()

				return err
			},
			want: http.StatusBadRequest,
		},
		{
			name: "bad token",
			call: func(svc *wfexec.ProjectsLocationsWorkflowsExecutionsStepEntriesService, name string) error {
				_, err := svc.List(name).PageToken("!!").Do()

				return err
			},
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			name := e.runSteps(t, workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_BASIC)

			err := tt.call(e.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries, name)

			var gerr *googleapi.Error

			require.ErrorAs(t, err, &gerr)
			assert.Equal(t, tt.want, gerr.Code)
		})
	}
}

func TestStepEntriesSurviveRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	name := src.runSteps(t, workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_DETAILED)
	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries.List(name).
		View("EXECUTION_ENTRY_VIEW_DETAILED").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Len(t, got.StepEntries, 7)
	assert.NotNil(t, got.StepEntries[6].VariableData)
}

func TestStepEntriesCancelled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.clock.holdSleeps()

	w := e.createWorkflow(t, "sleepy", `
main:
  steps:
    - nap:
        call: sys.sleep
        args: {seconds: 100}
`)
	ex := e.execute(t, w.GetName(), "")
	<-e.clock.sleeping

	_, err := e.ex.CancelExecution(t.Context(), &executionspb.CancelExecutionRequest{Name: ex.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Close()

	got, err := e.stepsService(t).Projects.Locations.Workflows.Executions.StepEntries.List(ex.GetName()).
		Context(context.WithoutCancel(t.Context())).Do()
	require.NoError(t, err)
	require.Len(t, got.StepEntries, 1)
	assert.Equal(t, "STATE_CANCELLED", got.StepEntries[0].State)
}

func TestIsStepEntriesPath(t *testing.T) {
	t.Parallel()

	const base = "/v1/projects/p/locations/l/workflows/w/executions/e"

	tests := []struct {
		path string
		want bool
	}{
		{base + "/stepEntries", true},
		{base + "/stepEntries/12", true},
		{base, false},
		{base + "/callbacks/tok", false},
		{base + "/stepEntries/1/x", false},
		{"/v1/projects/p/locations/l/workflows/w/stepEntries", false},
		{"/v2/projects/p/locations/l/workflows/w/executions/e/stepEntries", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, gcpworkflows.IsStepEntriesPath(tt.path))
		})
	}
}
