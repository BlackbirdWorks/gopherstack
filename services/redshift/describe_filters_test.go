package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestDescribeEvents_StartEndTime covers StartTime/EndTime (api_op_DescribeEvents.go:42-48,104-110).
func TestDescribeEvents_StartEndTime(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name  string
		start *time.Time
		end   *time.Time
		want  []string
	}{
		{name: "open", want: []string{"e-10", "e-30", "e-50"}},
		{name: "start_only", start: aws.Time(now.Add(-40 * time.Minute)), want: []string{"e-10", "e-30"}},
		{name: "end_only", end: aws.Time(now.Add(-20 * time.Minute)), want: []string{"e-30", "e-50"}},
		{
			name: "window", start: aws.Time(now.Add(-40 * time.Minute)), end: aws.Time(now.Add(-20 * time.Minute)),
			want: []string{"e-30"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newPaginationBackendAndClient(t)

			for _, m := range []int{10, 30, 50} {
				backend.AddEventInternal(&redshift.Event{
					EventID: "e-" + string(rune('0'+m/10)) + "0", Date: now.Add(-time.Duration(m) * time.Minute),
					SourceIdentifier: "src", SourceType: "cluster", Message: "m",
				})
			}

			out, err := client.DescribeEvents(t.Context(), &redshiftsdk.DescribeEventsInput{
				StartTime: tt.start, EndTime: tt.end,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Events))
			for _, e := range out.Events {
				got = append(got, aws.ToString(e.EventId))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestDescribeScheduledActions_Filters covers the cluster-identifier and iam-role filters (types/enums.go:519-520).
func TestDescribeScheduledActions_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []types.ScheduledActionFilter
		want    []string
	}{
		{name: "none", want: []string{"act-a", "act-b"}},
		{
			name: "cluster", want: []string{"act-b"},
			filters: []types.ScheduledActionFilter{
				{Name: types.ScheduledActionFilterNameClusterIdentifier, Values: []string{"cl-2"}},
			},
		},
		{
			name: "role_miss", want: []string{},
			filters: []types.ScheduledActionFilter{
				{Name: types.ScheduledActionFilterNameIamRole, Values: []string{"arn:aws:iam::1:role/none"}},
			},
		},
		{
			name: "role_and_cluster", want: []string{"act-a"},
			filters: []types.ScheduledActionFilter{
				{Name: types.ScheduledActionFilterNameIamRole, Values: []string{"arn:aws:iam::1:role/R"}},
				{Name: types.ScheduledActionFilterNameClusterIdentifier, Values: []string{"cl-1", "cl-9"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newPaginationBackendAndClient(t)

			for name, cl := range map[string]string{"act-a": "cl-1", "act-b": "cl-2"} {
				role := "arn:aws:iam::1:role/R"
				if name == "act-b" {
					role = "arn:aws:iam::1:role/Other"
				}

				_, err := backend.CreateScheduledAction(
					name,
					"cron(0 12 * * ? *)",
					role,
					"",
					&redshift.ScheduledActionTarget{
						PauseCluster: &redshift.PauseClusterAction{ClusterIdentifier: cl},
					},
					nil,
				)
				require.NoError(t, err)
			}

			out, err := client.DescribeScheduledActions(t.Context(), &redshiftsdk.DescribeScheduledActionsInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.ScheduledActions))
			for _, a := range out.ScheduledActions {
				got = append(got, aws.ToString(a.ScheduledActionName))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestDescribeNodeConfigurationOptions_ModeAndUtilizationFilters covers the Mode and
// EstimatedDiskUtilizationPercent filters (types/types.go:1374-1391).
func TestDescribeNodeConfigurationOptions_ModeAndUtilizationFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []types.NodeConfigurationOptionsFilter
		wantMin int
		wantMax int
	}{
		{name: "none", wantMin: 3, wantMax: 3},
		{
			name: "mode_elastic", wantMin: 3, wantMax: 3,
			filters: []types.NodeConfigurationOptionsFilter{
				{
					Name:     types.NodeConfigurationOptionsFilterNameMode,
					Operator: types.OperatorTypeIn,
					Values:   []string{"elastic"},
				},
			},
		},
		{
			name: "mode_classic_miss", wantMin: 0, wantMax: 0,
			filters: []types.NodeConfigurationOptionsFilter{
				{
					Name:     types.NodeConfigurationOptionsFilterNameMode,
					Operator: types.OperatorTypeIn,
					Values:   []string{"classic"},
				},
			},
		},
		{
			name: "utilization_miss", wantMin: 0, wantMax: 0,
			filters: []types.NodeConfigurationOptionsFilter{
				{
					Name:     types.NodeConfigurationOptionsFilterNameEstimatedDiskUtilizationPercent,
					Operator: types.OperatorTypeGt, Values: []string{"1000"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newPaginationBackendAndClient(t)

			out, err := client.DescribeNodeConfigurationOptions(
				t.Context(),
				&redshiftsdk.DescribeNodeConfigurationOptionsInput{
					ActionType: types.ActionTypeResizeCluster, Filters: tt.filters,
				},
			)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(out.NodeConfigurationOptionList), tt.wantMin)
			assert.LessOrEqual(t, len(out.NodeConfigurationOptionList), tt.wantMax)
		})
	}
}
