package ssm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestUpdateAssociation_ReplacesOmittedFields_RealClient covers
// api_op_UpdateAssociation.go: an omitted optional field must be nulled, not merged.
func TestUpdateAssociation_ReplacesOmittedFields_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
		Name:            aws.String("AWS-RunShellScript"),
		AssociationName: aws.String("original-name"),
		DocumentVersion: aws.String("1"),
		Targets: []ssmtypes.Target{
			{Key: aws.String("tag:Env"), Values: []string{"prod"}},
		},
		Parameters:     map[string][]string{"commands": {"echo hi"}},
		MaxConcurrency: aws.String("50%"),
	})
	require.NoError(t, err)

	assocID := created.AssociationDescription.AssociationId

	updated, err := client.UpdateAssociation(ctx, &ssmsdk.UpdateAssociationInput{
		AssociationId:      assocID,
		ComplianceSeverity: ssmtypes.AssociationComplianceSeverityCritical,
	})
	require.NoError(t, err)

	desc := updated.AssociationDescription
	assert.Equal(t, ssmtypes.AssociationComplianceSeverityCritical, desc.ComplianceSeverity)
	assert.Nil(t, desc.AssociationName, "AssociationName omitted from the update must be nulled")
	assert.Nil(t, desc.DocumentVersion, "DocumentVersion omitted from the update must be nulled")
	assert.Empty(t, desc.Targets, "Targets omitted from the update must be nulled")
	assert.Empty(t, desc.Parameters, "Parameters omitted from the update must be nulled")
	assert.Nil(t, desc.MaxConcurrency, "MaxConcurrency omitted from the update must be nulled")
}

// TestUpdatePatchBaseline_Replace_RealClient covers UpdatePatchBaseline's
// Replace parameter (api_op_UpdatePatchBaseline.go): true nulls omitted fields, false merges.
func TestUpdatePatchBaseline_Replace_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update func(baselineID *string) *ssmsdk.UpdatePatchBaselineInput
		check  func(t *testing.T, updated *ssmsdk.UpdatePatchBaselineOutput, err error)
		name   string
	}{
		{
			name: "replace_true_requires_name",
			update: func(baselineID *string) *ssmsdk.UpdatePatchBaselineInput {
				return &ssmsdk.UpdatePatchBaselineInput{BaselineId: baselineID, Replace: aws.Bool(true)}
			},
			check: func(t *testing.T, _ *ssmsdk.UpdatePatchBaselineOutput, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
		{
			name: "replace_true_nulls_omitted_fields",
			update: func(baselineID *string) *ssmsdk.UpdatePatchBaselineInput {
				return &ssmsdk.UpdatePatchBaselineInput{
					BaselineId: baselineID,
					Name:       aws.String("replace-semantics-baseline"),
					Replace:    aws.Bool(true),
				}
			},
			check: func(t *testing.T, updated *ssmsdk.UpdatePatchBaselineOutput, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.Empty(t, updated.Description, "Description omitted under Replace=true must be nulled")
				assert.Empty(t, updated.ApprovedPatches, "ApprovedPatches omitted under Replace=true must be nulled")
			},
		},
		{
			name: "replace_false_merges_omitted_fields",
			update: func(baselineID *string) *ssmsdk.UpdatePatchBaselineInput {
				return &ssmsdk.UpdatePatchBaselineInput{BaselineId: baselineID}
			},
			check: func(t *testing.T, updated *ssmsdk.UpdatePatchBaselineOutput, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.Equal(t, "original description", aws.ToString(updated.Description))
				assert.Equal(t, []string{"KB123456"}, updated.ApprovedPatches)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))
			ctx := t.Context()

			created, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
				Name:            aws.String("replace-semantics-baseline"),
				OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2,
				Description:     aws.String("original description"),
				ApprovedPatches: []string{"KB123456"},
			})
			require.NoError(t, err)

			updated, err := client.UpdatePatchBaseline(ctx, tc.update(created.BaselineId))
			tc.check(t, updated, err)
		})
	}
}

// TestUpdateMaintenanceWindowTarget_Replace_RealClient: Replace=true requires fields and
// nulls omitted ones; false merges (api_op_UpdateMaintenanceWindowTarget.go).
func TestUpdateMaintenanceWindowTarget_Replace_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update func(windowID, targetID *string) *ssmsdk.UpdateMaintenanceWindowTargetInput
		check  func(t *testing.T, updated *ssmsdk.UpdateMaintenanceWindowTargetOutput, err error)
		name   string
	}{
		{
			name: "replace_true_requires_targets",
			update: func(windowID, targetID *string) *ssmsdk.UpdateMaintenanceWindowTargetInput {
				return &ssmsdk.UpdateMaintenanceWindowTargetInput{
					WindowId: windowID, WindowTargetId: targetID, Replace: aws.Bool(true),
				}
			},
			check: func(t *testing.T, _ *ssmsdk.UpdateMaintenanceWindowTargetOutput, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
		{
			name: "replace_true_nulls_omitted_fields",
			update: func(windowID, targetID *string) *ssmsdk.UpdateMaintenanceWindowTargetInput {
				return &ssmsdk.UpdateMaintenanceWindowTargetInput{
					WindowId:       windowID,
					WindowTargetId: targetID,
					Targets: []ssmtypes.Target{
						{Key: aws.String("InstanceIds"), Values: []string{"i-2222222222222222"}},
					},
					Replace: aws.Bool(true),
				}
			},
			check: func(t *testing.T, updated *ssmsdk.UpdateMaintenanceWindowTargetOutput, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.Empty(t, updated.Name, "Name omitted under Replace=true must be nulled")
				assert.Empty(t, updated.OwnerInformation, "OwnerInformation omitted under Replace=true must be nulled")
			},
		},
		{
			name: "replace_false_merges_omitted_fields",
			update: func(windowID, targetID *string) *ssmsdk.UpdateMaintenanceWindowTargetInput {
				return &ssmsdk.UpdateMaintenanceWindowTargetInput{WindowId: windowID, WindowTargetId: targetID}
			},
			check: func(t *testing.T, updated *ssmsdk.UpdateMaintenanceWindowTargetOutput, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.Equal(t, "original-name", aws.ToString(updated.Name))
				assert.Equal(t, "original-owner", aws.ToString(updated.OwnerInformation))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))
			ctx := t.Context()

			mw, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
				Name:     aws.String("replace-semantics-window"),
				Schedule: aws.String("cron(0 9 ? * MON *)"),
				Duration: aws.Int32(2),
				Cutoff:   1,
			})
			require.NoError(t, err)

			registerInput := &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
				WindowId:     mw.WindowId,
				ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
				Targets: []ssmtypes.Target{
					{Key: aws.String("InstanceIds"), Values: []string{"i-1111111111111111"}},
				},
				Name:             aws.String("original-name"),
				OwnerInformation: aws.String("original-owner"),
			}

			target, err := client.RegisterTargetWithMaintenanceWindow(ctx, registerInput)
			require.NoError(t, err)

			updated, err := client.UpdateMaintenanceWindowTarget(ctx, tc.update(mw.WindowId, target.WindowTargetId))
			tc.check(t, updated, err)
		})
	}
}

// TestUpdateMaintenanceWindowTask_Replace_RealClient: Replace=true requires fields and
// nulls omitted ones; false merges (api_op_UpdateMaintenanceWindowTask.go).
func TestUpdateMaintenanceWindowTask_Replace_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update func(windowID, taskID *string) *ssmsdk.UpdateMaintenanceWindowTaskInput
		check  func(t *testing.T, updated *ssmsdk.UpdateMaintenanceWindowTaskOutput, err error)
		name   string
	}{
		{
			name: "replace_true_requires_task_arn",
			update: func(windowID, taskID *string) *ssmsdk.UpdateMaintenanceWindowTaskInput {
				return &ssmsdk.UpdateMaintenanceWindowTaskInput{
					WindowId: windowID, WindowTaskId: taskID, Replace: aws.Bool(true),
				}
			},
			check: func(t *testing.T, _ *ssmsdk.UpdateMaintenanceWindowTaskOutput, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
		{
			name: "replace_true_nulls_omitted_fields",
			update: func(windowID, taskID *string) *ssmsdk.UpdateMaintenanceWindowTaskInput {
				return &ssmsdk.UpdateMaintenanceWindowTaskInput{
					WindowId:     windowID,
					WindowTaskId: taskID,
					TaskArn:      aws.String("AWS-RunShellScript"),
					Replace:      aws.Bool(true),
				}
			},
			check: func(t *testing.T, updated *ssmsdk.UpdateMaintenanceWindowTaskOutput, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.Empty(t, updated.Name, "Name omitted under Replace=true must be nulled")
				assert.Empty(t, updated.ServiceRoleArn, "ServiceRoleArn omitted under Replace=true must be nulled")
				assert.Empty(t, updated.Priority, "Priority omitted under Replace=true must be nulled")
			},
		},
		{
			name: "replace_false_merges_omitted_fields",
			update: func(windowID, taskID *string) *ssmsdk.UpdateMaintenanceWindowTaskInput {
				return &ssmsdk.UpdateMaintenanceWindowTaskInput{WindowId: windowID, WindowTaskId: taskID}
			},
			check: func(t *testing.T, updated *ssmsdk.UpdateMaintenanceWindowTaskOutput, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.Equal(t, "original-name", aws.ToString(updated.Name))
				assert.Equal(t, "arn:aws:iam::123456789012:role/OriginalRole", aws.ToString(updated.ServiceRoleArn))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))
			ctx := t.Context()

			mw, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
				Name:     aws.String("replace-semantics-window"),
				Schedule: aws.String("cron(0 9 ? * MON *)"),
				Duration: aws.Int32(2),
				Cutoff:   1,
			})
			require.NoError(t, err)

			task, err := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
				WindowId:       mw.WindowId,
				TaskArn:        aws.String("AWS-RunPowerShellScript"),
				TaskType:       ssmtypes.MaintenanceWindowTaskTypeRunCommand,
				Name:           aws.String("original-name"),
				ServiceRoleArn: aws.String("arn:aws:iam::123456789012:role/OriginalRole"),
			})
			require.NoError(t, err)

			updated, err := client.UpdateMaintenanceWindowTask(ctx, tc.update(mw.WindowId, task.WindowTaskId))
			tc.check(t, updated, err)
		})
	}
}

// TestJanitor_SweepsExpiredCommandHistory_RealClient covers the janitor's
// command-history sweep, which retains a terminal command independently of ExpiresAfter.
func TestJanitor_SweepsExpiredCommandHistory_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend().WithCommandHistoryRetention(10 * time.Millisecond)
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	sent, sendErr := client.SendCommand(ctx, &ssmsdk.SendCommandInput{
		DocumentName: aws.String("AWS-RunShellScript"),
		InstanceIds:  []string{"i-1111"},
	})
	require.NoError(t, sendErr)

	cmdID := sent.Command.CommandId

	j := ssm.NewJanitor(backend, time.Minute)

	require.Eventually(t, func() bool {
		j.SweepOnce(ctx)

		listOut, listErr := client.ListCommands(ctx, &ssmsdk.ListCommandsInput{CommandId: cmdID})
		require.NoError(t, listErr)

		return len(listOut.Commands) == 0
	}, 2*time.Second, 5*time.Millisecond)

	invOut, invErr := client.ListCommandInvocations(ctx, &ssmsdk.ListCommandInvocationsInput{CommandId: cmdID})
	require.NoError(t, invErr)
	assert.Empty(t, invOut.CommandInvocations)
}
