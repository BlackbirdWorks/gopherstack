package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeInstanceProperties_Filters covers gopherstack-tz6z:
// DescribeInstanceProperties previously ignored FiltersWithOperator and
// InstancePropertyFilterList entirely (pagination itself is covered by
// TestDescribeInstanceProperties_Pagination in pagination_cursor_fixes_test.go).
func TestDescribeInstanceProperties_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		build   func(a, b string) *ssmsdk.DescribeInstancePropertiesInput
		wantIdx []int // indices into [a, b] expected in the result
	}{
		{
			name: "filter_list_value_set",
			build: func(a, _ string) *ssmsdk.DescribeInstancePropertiesInput {
				return &ssmsdk.DescribeInstancePropertiesInput{
					InstancePropertyFilterList: []ssmtypes.InstancePropertyFilter{
						{Key: ssmtypes.InstancePropertyFilterKeyInstanceIds, ValueSet: []string{a}},
					},
				}
			},
			wantIdx: []int{0},
		},
		{
			name: "string_filter_equal",
			build: func(a, _ string) *ssmsdk.DescribeInstancePropertiesInput {
				return &ssmsdk.DescribeInstancePropertiesInput{
					FiltersWithOperator: []ssmtypes.InstancePropertyStringFilter{
						{
							Key:      aws.String("ActivationIds"),
							Operator: ssmtypes.InstancePropertyFilterOperatorEqual,
							Values:   []string{a},
						},
					},
				}
			},
			wantIdx: []int{0},
		},
		{
			name: "string_filter_not_equal",
			build: func(a, _ string) *ssmsdk.DescribeInstancePropertiesInput {
				return &ssmsdk.DescribeInstancePropertiesInput{
					FiltersWithOperator: []ssmtypes.InstancePropertyStringFilter{
						{
							Key:      aws.String("ActivationIds"),
							Operator: ssmtypes.InstancePropertyFilterOperatorNotEqual,
							Values:   []string{a},
						},
					},
				}
			},
			wantIdx: []int{1},
		},
		{
			name: "untracked_key_matches_everything",
			build: func(_, _ string) *ssmsdk.DescribeInstancePropertiesInput {
				return &ssmsdk.DescribeInstancePropertiesInput{
					InstancePropertyFilterList: []ssmtypes.InstancePropertyFilter{
						{
							Key:      ssmtypes.InstancePropertyFilterKeyIamRole,
							ValueSet: []string{"arn:aws:iam::000000000000:role/whatever"},
						},
					},
				}
			},
			wantIdx: []int{0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))
			ctx := t.Context()

			outA, err := client.CreateActivation(ctx, &ssmsdk.CreateActivationInput{IamRole: aws.String("role")})
			require.NoError(t, err)
			outB, err := client.CreateActivation(ctx, &ssmsdk.CreateActivationInput{IamRole: aws.String("role")})
			require.NoError(t, err)

			ids := []string{aws.ToString(outA.ActivationId), aws.ToString(outB.ActivationId)}

			got, err := client.DescribeInstanceProperties(ctx, tt.build(ids[0], ids[1]))
			require.NoError(t, err)

			var want []string
			for _, i := range tt.wantIdx {
				want = append(want, ids[i])
			}

			var gotIDs []string
			for _, p := range got.InstanceProperties {
				gotIDs = append(gotIDs, aws.ToString(p.InstanceId))
			}

			assert.ElementsMatch(t, want, gotIDs)
		})
	}
}

// newMWTestClient creates a maintenance window and returns its ID alongside
// a ready client, for the Targets/Tasks/ExecutionTasks filter tests below.
func newMWTestClient(t *testing.T) (*ssmsdk.Client, string) {
	t.Helper()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:                     aws.String("mw-filters"),
		Schedule:                 aws.String("cron(0 0 ? * * *)"),
		Duration:                 aws.Int32(2),
		Cutoff:                   1,
		AllowUnassociatedTargets: false,
	})
	require.NoError(t, err)

	return client, aws.ToString(win.WindowId)
}

// TestDescribeMaintenanceWindowTargets_Filters covers gopherstack-tz6z:
// DescribeMaintenanceWindowTargets ignored Filters entirely.
func TestDescribeMaintenanceWindowTargets_Filters(t *testing.T) {
	t.Parallel()

	client, windowID := newMWTestClient(t)
	ctx := t.Context()

	t1, setupErr := client.RegisterTargetWithMaintenanceWindow(ctx, &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
		WindowId:         aws.String(windowID),
		ResourceType:     ssmtypes.MaintenanceWindowResourceTypeInstance,
		Targets:          []ssmtypes.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-1"}}},
		OwnerInformation: aws.String("team-a"),
	})
	require.NoError(t, setupErr)

	t2, setupErr := client.RegisterTargetWithMaintenanceWindow(ctx, &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
		WindowId:         aws.String(windowID),
		ResourceType:     ssmtypes.MaintenanceWindowResourceTypeResourceGroup,
		Targets:          []ssmtypes.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-2"}}},
		OwnerInformation: aws.String("team-b"),
	})
	require.NoError(t, setupErr)

	t.Run("filter_by_type_narrows", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTargets(ctx, &ssmsdk.DescribeMaintenanceWindowTargetsInput{
			WindowId: aws.String(windowID),
			Filters:  []ssmtypes.MaintenanceWindowFilter{{Key: aws.String("Type"), Values: []string{"INSTANCE"}}},
		})
		require.NoError(t, err)
		require.Len(t, got.Targets, 1)
		assert.Equal(t, aws.ToString(t1.WindowTargetId), aws.ToString(got.Targets[0].WindowTargetId))
	})

	t.Run("filter_by_owner_information_narrows", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTargets(ctx, &ssmsdk.DescribeMaintenanceWindowTargetsInput{
			WindowId: aws.String(windowID),
			Filters: []ssmtypes.MaintenanceWindowFilter{
				{Key: aws.String("OwnerInformation"), Values: []string{"team-b"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, got.Targets, 1)
		assert.Equal(t, aws.ToString(t2.WindowTargetId), aws.ToString(got.Targets[0].WindowTargetId))
	})

	t.Run("filter_by_window_target_id_narrows", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTargets(ctx, &ssmsdk.DescribeMaintenanceWindowTargetsInput{
			WindowId: aws.String(windowID),
			Filters: []ssmtypes.MaintenanceWindowFilter{
				{Key: aws.String("WindowTargetId"), Values: []string{aws.ToString(t1.WindowTargetId)}},
			},
		})
		require.NoError(t, err)
		require.Len(t, got.Targets, 1)
		assert.Equal(t, aws.ToString(t1.WindowTargetId), aws.ToString(got.Targets[0].WindowTargetId))
	})

	t.Run("no_match_returns_empty", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTargets(ctx, &ssmsdk.DescribeMaintenanceWindowTargetsInput{
			WindowId: aws.String(windowID),
			Filters:  []ssmtypes.MaintenanceWindowFilter{{Key: aws.String("Type"), Values: []string{"NOPE"}}},
		})
		require.NoError(t, err)
		assert.Empty(t, got.Targets)
	})
}

// TestDescribeMaintenanceWindowTasks_Filters covers gopherstack-tz6z:
// DescribeMaintenanceWindowTasks ignored Filters entirely.
func TestDescribeMaintenanceWindowTasks_Filters(t *testing.T) {
	t.Parallel()

	client, windowID := newMWTestClient(t)
	ctx := t.Context()

	task1, setupErr := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: aws.String(windowID),
		TaskArn:  aws.String("AWS-RunShellScript"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
		Priority: aws.Int32(1),
	})
	require.NoError(t, setupErr)

	task2, setupErr := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: aws.String(windowID),
		TaskArn:  aws.String("AWS-RunPatchBaseline"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeAutomation,
		Priority: aws.Int32(2),
	})
	require.NoError(t, setupErr)

	t.Run("filter_by_task_type_narrows", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
			WindowId: aws.String(windowID),
			Filters:  []ssmtypes.MaintenanceWindowFilter{{Key: aws.String("TaskType"), Values: []string{"AUTOMATION"}}},
		})
		require.NoError(t, err)
		require.Len(t, got.Tasks, 1)
		assert.Equal(t, aws.ToString(task2.WindowTaskId), aws.ToString(got.Tasks[0].WindowTaskId))
	})

	t.Run("filter_by_task_arn_narrows", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
			WindowId: aws.String(windowID),
			Filters: []ssmtypes.MaintenanceWindowFilter{
				{Key: aws.String("TaskArn"), Values: []string{"AWS-RunShellScript"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, got.Tasks, 1)
		assert.Equal(t, aws.ToString(task1.WindowTaskId), aws.ToString(got.Tasks[0].WindowTaskId))
	})

	t.Run("filter_by_priority_narrows", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
			WindowId: aws.String(windowID),
			Filters:  []ssmtypes.MaintenanceWindowFilter{{Key: aws.String("Priority"), Values: []string{"2"}}},
		})
		require.NoError(t, err)
		require.Len(t, got.Tasks, 1)
		assert.Equal(t, aws.ToString(task2.WindowTaskId), aws.ToString(got.Tasks[0].WindowTaskId))
	})

	t.Run("filter_and_pagination_combine", func(t *testing.T) {
		t.Parallel()

		first, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
			WindowId:   aws.String(windowID),
			MaxResults: aws.Int32(1),
			Filters: []ssmtypes.MaintenanceWindowFilter{
				{Key: aws.String("TaskType"), Values: []string{"RUN_COMMAND", "AUTOMATION"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, first.Tasks, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
			WindowId:   aws.String(windowID),
			MaxResults: aws.Int32(1),
			NextToken:  first.NextToken,
			Filters: []ssmtypes.MaintenanceWindowFilter{
				{Key: aws.String("TaskType"), Values: []string{"RUN_COMMAND", "AUTOMATION"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, second.Tasks, 1)
		assert.Nil(t, second.NextToken)

		assert.ElementsMatch(t,
			[]string{aws.ToString(task1.WindowTaskId), aws.ToString(task2.WindowTaskId)},
			[]string{aws.ToString(first.Tasks[0].WindowTaskId), aws.ToString(second.Tasks[0].WindowTaskId)},
		)
	})
}

// TestDescribeMaintenanceWindowExecutionTasks_Filters covers gopherstack-tz6z:
// DescribeMaintenanceWindowExecutionTasks ignored Filters entirely.
func TestDescribeMaintenanceWindowExecutionTasks_Filters(t *testing.T) {
	t.Parallel()

	client, windowID := newMWTestClient(t)
	ctx := t.Context()

	_, setupErr := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: aws.String(windowID),
		TaskArn:  aws.String("AWS-RunShellScript"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
		Priority: aws.Int32(1),
	})
	require.NoError(t, setupErr)

	execs, setupErr := client.DescribeMaintenanceWindowExecutions(ctx, &ssmsdk.DescribeMaintenanceWindowExecutionsInput{
		WindowId: aws.String(windowID),
	})
	require.NoError(t, setupErr)
	require.NotEmpty(t, execs.WindowExecutions)
	execID := aws.ToString(execs.WindowExecutions[0].WindowExecutionId)

	t.Run("status_match_returns_task", func(t *testing.T) {
		t.Parallel()

		// Real AWS's MaintenanceWindowExecutionStatus enum value is "SUCCESS"
		// (enums.go:1223); this backend's commandStatusSuccess constant emits
		// "Success" for every op that shares it, a pre-existing wire-case bug
		// out of scope for this filter fix (gopherstack-tz6z) -- tracked
		// separately (bd: ssm status casing).
		got, err := client.DescribeMaintenanceWindowExecutionTasks(
			ctx,
			&ssmsdk.DescribeMaintenanceWindowExecutionTasksInput{
				WindowExecutionId: aws.String(execID),
				Filters: []ssmtypes.MaintenanceWindowFilter{
					{Key: aws.String("STATUS"), Values: []string{"Success"}},
				},
			},
		)
		require.NoError(t, err)
		assert.Len(t, got.WindowExecutionTaskIdentities, 1)
	})

	t.Run("status_mismatch_excludes_task", func(t *testing.T) {
		t.Parallel()

		got, err := client.DescribeMaintenanceWindowExecutionTasks(
			ctx,
			&ssmsdk.DescribeMaintenanceWindowExecutionTasksInput{
				WindowExecutionId: aws.String(execID),
				Filters: []ssmtypes.MaintenanceWindowFilter{
					{Key: aws.String("STATUS"), Values: []string{"FAILED"}},
				},
			},
		)
		require.NoError(t, err)
		assert.Empty(t, got.WindowExecutionTaskIdentities)
	})
}
