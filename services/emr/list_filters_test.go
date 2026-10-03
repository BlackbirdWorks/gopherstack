package emr_test

import (
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newListFiltersClient(t *testing.T) *emrsdk.Client {
	t.Helper()

	return newTestEMRClient(t, newTestHandler(t))
}

func runListFiltersCluster(t *testing.T, c *emrsdk.Client, name string) string {
	t.Helper()

	out, err := c.RunJobFlow(t.Context(), &emrsdk.RunJobFlowInput{
		Name:      awssdk.String(name),
		Instances: &emrtypes.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: awssdk.Bool(true)},
	})
	require.NoError(t, err)

	return awssdk.ToString(out.JobFlowId)
}

// TestListClusters_StateFilter pins api_op_ListClusters.go:13 ("all clusters visible
// to this account"): terminated clusters appear unless ClusterStates narrows.
func TestListClusters_StateFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []emrtypes.ClusterState
		want   []string
	}{
		{name: "no_filter_includes_terminated", want: []string{"alive", "gone"}},
		{
			name:   "terminated_only",
			states: []emrtypes.ClusterState{emrtypes.ClusterStateTerminated},
			want:   []string{"gone"},
		},
		{name: "waiting_only", states: []emrtypes.ClusterState{emrtypes.ClusterStateWaiting}, want: []string{"alive"}},
		{
			name:   "union_of_states",
			states: []emrtypes.ClusterState{emrtypes.ClusterStateWaiting, emrtypes.ClusterStateTerminated},
			want:   []string{"alive", "gone"},
		},
		{name: "unmatched_state", states: []emrtypes.ClusterState{emrtypes.ClusterStateRunning}, want: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newListFiltersClient(t)
			runListFiltersCluster(t, c, "alive")
			gone := runListFiltersCluster(t, c, "gone")

			_, err := c.TerminateJobFlows(t.Context(), &emrsdk.TerminateJobFlowsInput{JobFlowIds: []string{gone}})
			require.NoError(t, err)

			out, err := c.ListClusters(t.Context(), &emrsdk.ListClustersInput{ClusterStates: tc.states})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Clusters))
			for _, cl := range out.Clusters {
				got = append(got, awssdk.ToString(cl.Name))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

// TestListSteps_Order pins api_op_ListSteps.go:13: reverse order unless
// stepIds or StepStates is specified.
func TestListSteps_Order(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []emrtypes.StepState
		want   []string
		ids    bool
	}{
		{name: "default_reversed", want: []string{"s2", "s1"}},
		{name: "step_ids_not_reversed", ids: true, want: []string{"s1", "s2"}},
		{
			name:   "step_states_not_reversed",
			states: []emrtypes.StepState{emrtypes.StepStatePending},
			want:   []string{"s1", "s2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newListFiltersClient(t)
			cluster := runListFiltersCluster(t, c, "steps")

			added, err := c.AddJobFlowSteps(t.Context(), &emrsdk.AddJobFlowStepsInput{
				JobFlowId: awssdk.String(cluster),
				Steps: []emrtypes.StepConfig{
					{
						Name:          awssdk.String("s1"),
						HadoopJarStep: &emrtypes.HadoopJarStepConfig{Jar: awssdk.String("a.jar")},
					},
					{
						Name:          awssdk.String("s2"),
						HadoopJarStep: &emrtypes.HadoopJarStepConfig{Jar: awssdk.String("b.jar")},
					},
				},
			})
			require.NoError(t, err)

			in := &emrsdk.ListStepsInput{ClusterId: awssdk.String(cluster), StepStates: tc.states}
			if tc.ids {
				in.StepIds = added.StepIds
			}

			out, err := c.ListSteps(t.Context(), in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Steps))
			for _, s := range out.Steps {
				got = append(got, awssdk.ToString(s.Name))
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDescribeJobFlows_States pins the JobFlowExecutionState values and the
// no-parameter default of api_op_DescribeJobFlows.go:17-28.
func TestDescribeJobFlows_States(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   map[string]string
		name   string
		states []emrtypes.JobFlowExecutionState
	}{
		{name: "no_params_recent_and_active", want: map[string]string{"alive": "WAITING", "gone": "COMPLETED"}},
		{
			name:   "completed",
			states: []emrtypes.JobFlowExecutionState{emrtypes.JobFlowExecutionStateCompleted},
			want:   map[string]string{"gone": "COMPLETED"},
		},
		{
			name:   "waiting",
			states: []emrtypes.JobFlowExecutionState{emrtypes.JobFlowExecutionStateWaiting},
			want:   map[string]string{"alive": "WAITING"},
		},
		{
			name:   "failed_matches_nothing",
			states: []emrtypes.JobFlowExecutionState{emrtypes.JobFlowExecutionStateFailed},
			want:   map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newListFiltersClient(t)
			runListFiltersCluster(t, c, "alive")
			gone := runListFiltersCluster(t, c, "gone")

			_, err := c.TerminateJobFlows(t.Context(), &emrsdk.TerminateJobFlowsInput{JobFlowIds: []string{gone}})
			require.NoError(t, err)

			//nolint:staticcheck // SA1019: DescribeJobFlows is deprecated but still real on the wire
			out, err := c.DescribeJobFlows(t.Context(), &emrsdk.DescribeJobFlowsInput{JobFlowStates: tc.states})
			require.NoError(t, err)

			got := map[string]string{}
			for _, jf := range out.JobFlows {
				got[awssdk.ToString(jf.Name)] = string(jf.ExecutionStatusDetail.State)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
