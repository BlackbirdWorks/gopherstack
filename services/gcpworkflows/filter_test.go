package gcpworkflows_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

func TestListWorkflowsFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createWorkflow(t, "alpha", hello, func(w *workflowspb.Workflow) {
		w.Description = "nightly import"
		w.Labels = map[string]string{"team": "data"}
	})
	e.createWorkflow(t, "beta", hello, func(w *workflowspb.Workflow) {
		w.Description = "billing sync"
		w.Labels = map[string]string{"team": "finance"}
	})
	e.createWorkflow(t, "gamma", hello)

	tests := []struct {
		name   string
		filter string
		want   []string
		code   codes.Code
	}{
		{name: "state", filter: `state="ACTIVE"`, want: []string{"alpha", "beta", "gamma"}},
		{name: "state none", filter: `state="UNAVAILABLE"`, want: nil},
		{name: "description contains", filter: `description:billing`, want: []string{"beta"}},
		{name: "label", filter: `labels.team="data"`, want: []string{"alpha"}},
		{name: "label present", filter: `labels.team:*`, want: []string{"alpha", "beta"}},
		{name: "or", filter: `labels.team="data" OR labels.team="finance"`, want: []string{"alpha", "beta"}},
		{name: "not", filter: `NOT labels.team:*`, want: []string{"gamma"}},
		{name: "minus", filter: `-labels.team:*`, want: []string{"gamma"}},
		{
			name:   "parens",
			filter: `(labels.team="data" OR description:billing) AND state="ACTIVE"`,
			want:   []string{"alpha", "beta"},
		},
		{name: "implicit and", filter: `state="ACTIVE" labels.team="finance"`, want: []string{"beta"}},
		{name: "wildcard", filter: `description="nightly*"`, want: []string{"alpha"}},
		{name: "created after", filter: `createTime>"2000-01-01"`, want: []string{"alpha", "beta", "gamma"}},
		{name: "created before", filter: `createTime<"2000-01-01"`, want: nil},
		{name: "bad field", filter: `bogus="x"`, code: codes.InvalidArgument},
		{name: "bad time", filter: `createTime>"yesterday"`, code: codes.InvalidArgument},
		{name: "unbalanced", filter: `(state="ACTIVE"`, code: codes.InvalidArgument},
		{name: "dangling", filter: `state=`, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.wf.ListWorkflows(t.Context(), &workflowspb.ListWorkflowsRequest{
				Parent: parent, Filter: tt.filter, OrderBy: "name",
			})

			var got []string

			for {
				w, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				if err != nil {
					assert.Equal(t, tt.code, code(err))

					return
				}

				got = append(got, lastName(w.GetName()))
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, got)
		})
	}
}

func lastName(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}

	return s
}

func TestListExecutionsFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "runs", hello)

	names := make([]string, 0, 2)

	for _, lbl := range []string{"a", "b"} {
		ex, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{
			Parent: w.GetName(), Execution: &executionspb.Execution{Labels: map[string]string{"k": lbl}},
		})
		require.NoError(t, err)

		e.await(t, ex.GetName())

		names = append(names, ex.GetName())
	}

	tests := []struct {
		name   string
		filter string
		want   int
		code   codes.Code
	}{
		{name: "or", filter: `label.k="a" OR label.k="b"`, want: 2},
		{name: "labels alias", filter: `labels.k="a"`, want: 1},
		{name: "not", filter: `NOT label.k="a"`, want: 1},
		{name: "parens", filter: `(label.k="a" OR label.k="b") AND state!="ACTIVE"`, want: 2},
		{name: "duration", filter: `duration>="0s"`, want: 2},
		{name: "duration none", filter: `duration>"1000s"`, want: 0},
		{name: "end time", filter: `endTime>"2000-01-01"`, want: 2},
		{name: "step name", filter: `stepName:*`, want: 0},
		{name: "bad state", filter: `state="NOPE"`, code: codes.InvalidArgument},
		{name: "bad duration", filter: `duration>"soon"`, code: codes.InvalidArgument},
		{name: "enum order", filter: `state>"A"`, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.ex.ListExecutions(t.Context(), &executionspb.ListExecutionsRequest{
				Parent: w.GetName(), Filter: tt.filter,
			})

			got := 0

			for {
				_, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				if err != nil {
					assert.Equal(t, tt.code, code(err))

					return
				}

				got++
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, got)
		})
	}

	require.Len(t, names, 2)
}
