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
