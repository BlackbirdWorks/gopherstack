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

func TestClientTokenIdempotency_RealClient(t *testing.T) {
	t.Parallel()

	// each op returns the created ID for (token, variant); variant changes a parameter.
	type opFn func(t *testing.T, c *ssmsdk.Client, token string, variant bool) (string, error)

	tests := []struct {
		run  opFn
		name string
	}{
		{
			name: "create_maintenance_window",
			run: func(t *testing.T, c *ssmsdk.Client, token string, variant bool) (string, error) {
				t.Helper()

				name := "mw"
				if variant {
					name = "mw-other"
				}

				out, err := c.CreateMaintenanceWindow(t.Context(), &ssmsdk.CreateMaintenanceWindowInput{
					ClientToken: aws.String(token), Name: aws.String(name),
					Schedule: aws.String("rate(7 days)"), Duration: aws.Int32(2), Cutoff: 1,
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.WindowId), nil
			},
		},
		{
			name: "create_patch_baseline",
			run: func(t *testing.T, c *ssmsdk.Client, token string, variant bool) (string, error) {
				t.Helper()

				name := "pb"
				if variant {
					name = "pb-other"
				}

				out, err := c.CreatePatchBaseline(t.Context(), &ssmsdk.CreatePatchBaselineInput{
					ClientToken: aws.String(token), Name: aws.String(name),
					OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2,
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.BaselineId), nil
			},
		},
		{
			name: "start_automation_execution",
			run: func(t *testing.T, c *ssmsdk.Client, token string, variant bool) (string, error) {
				t.Helper()

				params := map[string][]string{"k": {"v"}}
				if variant {
					params = map[string][]string{"k": {"other"}}
				}

				out, err := c.StartAutomationExecution(t.Context(), &ssmsdk.StartAutomationExecutionInput{
					ClientToken: aws.String(
						token,
					), DocumentName: aws.String("AWS-RestartEC2Instance"), Parameters: params,
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.AutomationExecutionId), nil
			},
		},
		{
			name: "register_target_and_task",
			run: func(t *testing.T, c *ssmsdk.Client, token string, variant bool) (string, error) {
				t.Helper()

				win, err := c.CreateMaintenanceWindow(t.Context(), &ssmsdk.CreateMaintenanceWindowInput{
					ClientToken: aws.String("33333333-3333-3333-3333-333333333333"), Name: aws.String("w"),
					Schedule: aws.String("rate(7 days)"), Duration: aws.Int32(2), Cutoff: 1,
				})
				if err != nil {
					return "", err
				}

				owner := "o"
				if variant {
					owner = "o2"
				}

				tgt, err := c.RegisterTargetWithMaintenanceWindow(
					t.Context(),
					&ssmsdk.RegisterTargetWithMaintenanceWindowInput{
						ClientToken: aws.String(token), WindowId: win.WindowId, OwnerInformation: aws.String(owner),
						ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
						Targets:      []ssmtypes.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-1"}}},
					},
				)
				if err != nil {
					return "", err
				}

				task, err := c.RegisterTaskWithMaintenanceWindow(
					t.Context(),
					&ssmsdk.RegisterTaskWithMaintenanceWindowInput{
						ClientToken: aws.String(
							token,
						), WindowId: win.WindowId, TaskArn: aws.String("AWS-RunShellScript"),
						TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand, Name: aws.String(owner),
					},
				)
				if err != nil {
					return "", err
				}

				return aws.ToString(tgt.WindowTargetId) + "/" + aws.ToString(task.WindowTaskId), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))

			first, err := tt.run(t, client, "11111111-1111-1111-1111-111111111111", false)
			require.NoError(t, err)

			replay, err := tt.run(t, client, "11111111-1111-1111-1111-111111111111", false)
			require.NoError(t, err)
			assert.Equal(t, first, replay)

			fresh, err := tt.run(t, client, "22222222-2222-2222-2222-222222222222", false)
			require.NoError(t, err)
			assert.NotEqual(t, first, fresh)

			_, err = tt.run(t, client, "11111111-1111-1111-1111-111111111111", true)
			require.Error(t, err)

			var mismatch *ssmtypes.IdempotentParameterMismatch

			require.ErrorAs(t, err, &mismatch)

			restored := ssm.NewInMemoryBackend()
			require.NoError(t, restored.Restore(t.Context(), backend.Snapshot(t.Context())))

			again, err := tt.run(
				t,
				newTestSSMClient(t, ssm.NewHandler(restored)),
				"11111111-1111-1111-1111-111111111111",
				false,
			)
			require.NoError(t, err)
			assert.Equal(t, first, again)
		})
	}
}
