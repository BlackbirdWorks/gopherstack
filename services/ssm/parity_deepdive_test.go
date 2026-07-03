package ssm_test

// parity_deepdive_test.go — tests for the SSM deep-dive parity work:
//   1. Run Command output rendering + observable InProgress window.
//   2. Automation execution advancing through steps to a terminal status.
//   3. Stable association execution records.
//   4. Session eviction / store bounding.
//   5. Patch/inventory real-state backing.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// 1. Run Command output
// ---------------------------------------------------------------------------

func TestSendCommand_OutputRendering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		doc        string
		wantStdout string
		wantStderr string
		wantStatus string
		commands   []string
	}{
		{
			name:       "shell_echo_single",
			doc:        "AWS-RunShellScript",
			commands:   []string{"echo hello"},
			wantStdout: "hello\n",
			wantStatus: "Success",
		},
		{
			name:       "shell_echo_quoted_and_plain",
			doc:        "AWS-RunShellScript",
			commands:   []string{"echo 'a b'", "echo c"},
			wantStdout: "a b\nc\n",
			wantStatus: "Success",
		},
		{
			name:       "shell_multiline_command",
			doc:        "AWS-RunShellScript",
			commands:   []string{"echo one\necho two"},
			wantStdout: "one\ntwo\n",
			wantStatus: "Success",
		},
		{
			name:       "shell_nonzero_exit_fails",
			doc:        "AWS-RunShellScript",
			commands:   []string{"echo before", "exit 3"},
			wantStdout: "before\n",
			wantStderr: "exit status 3",
			wantStatus: "Failed",
		},
		{
			name:       "powershell_write_host",
			doc:        "AWS-RunPowerShellScript",
			commands:   []string{"Write-Host hi"},
			wantStdout: "hi\n",
			wantStatus: "Success",
		},
		{
			name:       "powershell_echo_alias",
			doc:        "AWS-RunPowerShellScript",
			commands:   []string{"echo pow"},
			wantStdout: "pow\n",
			wantStatus: "Success",
		},
		{
			name:       "unknown_document_empty_output",
			doc:        "My-Custom-Doc",
			commands:   []string{"whatever"},
			wantStdout: "",
			wantStatus: "Success",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.TODO()

			// SendCommand validates the document exists (AWS-accurate). Register
			// any non-default document used by the case first.
			if tt.doc != "AWS-RunShellScript" && tt.doc != "AWS-RunPowerShellScript" {
				_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{
					Name:         tt.doc,
					Content:      `{"schemaVersion":"2.2","mainSteps":[]}`,
					DocumentType: "Command",
				})
				require.NoError(t, err)
			}

			out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
				DocumentName: tt.doc,
				InstanceIDs:  []string{"i-0123456789abcdef0"},
				Parameters:   map[string][]string{"commands": tt.commands},
			})
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, out.Command.Status,
				"command status must reflect script outcome")

			inv, err := b.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
				CommandID:  out.Command.CommandID,
				InstanceID: "i-0123456789abcdef0",
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantStdout, inv.StandardOutputContent,
				"stdout must be the rendered command output")
			assert.Equal(t, tt.wantStatus, inv.Status)

			if tt.wantStderr == "" {
				assert.Empty(t, inv.StandardErrorContent)
			} else {
				assert.Contains(t, inv.StandardErrorContent, tt.wantStderr)
			}
		})
	}
}

func TestSendCommand_InProgressObservable(t *testing.T) {
	t.Parallel()

	// A long exec delay keeps the command InProgress until forced complete,
	// making the InProgress window (and hidden output) observable.
	b := ssm.NewInMemoryBackend().WithCommandExecDelay(time.Hour)
	ctx := context.TODO()

	out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-1"},
		Parameters:   map[string][]string{"commands": {"echo hi"}},
	})
	require.NoError(t, err)
	require.Equal(t, "InProgress", out.Command.Status,
		"with an exec delay the command must be observable as InProgress")

	inv, err := b.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
		CommandID:  out.Command.CommandID,
		InstanceID: "i-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "InProgress", inv.Status)
	assert.Empty(t, inv.StandardOutputContent, "output must be hidden while InProgress")

	// Force completion (as elapsed time would) and confirm output is revealed.
	b.ForceCompleteCommands()

	inv, err = b.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
		CommandID:  out.Command.CommandID,
		InstanceID: "i-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "Success", inv.Status)
	assert.Equal(t, "hi\n", inv.StandardOutputContent)
}

func TestSendCommand_InProgressCompletesAfterDelay(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend().WithCommandExecDelay(60 * time.Millisecond)
	ctx := context.TODO()

	out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-1"},
		Parameters:   map[string][]string{"commands": {"echo done"}},
	})
	require.NoError(t, err)
	require.Equal(t, "InProgress", out.Command.Status)

	require.Eventually(t, func() bool {
		inv, gErr := b.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
			CommandID:  out.Command.CommandID,
			InstanceID: "i-1",
		})

		return gErr == nil && inv.Status == "Success" && inv.StandardOutputContent == "done\n"
	}, 2*time.Second, 10*time.Millisecond,
		"command must complete and reveal output once the delay elapses")
}

// ---------------------------------------------------------------------------
// 2. Automation execution
// ---------------------------------------------------------------------------

func TestStartAutomationExecution_CompletesWithSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		docContent string
		wantSteps  []string
	}{
		{
			name:       "synthetic_step_for_unknown_doc",
			docContent: "",
			wantSteps:  []string{"AWS-Doc"},
		},
		{
			name: "steps_extracted_from_document",
			docContent: `{"schemaVersion":"0.3","mainSteps":[` +
				`{"name":"stepA","action":"aws:runInstances"},` +
				`{"name":"stepB","action":"aws:sleep"}]}`,
			wantSteps: []string{"stepA", "stepB"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.TODO()

			docName := "AWS-Doc"
			if tt.docContent != "" {
				_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{
					Name:         docName,
					Content:      tt.docContent,
					DocumentType: "Automation",
				})
				require.NoError(t, err)
			}

			start, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
				DocumentName: docName,
			})
			require.NoError(t, err)

			got, err := b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: start.AutomationExecutionID,
			})
			require.NoError(t, err)

			exec := got.AutomationExecution
			require.NotNil(t, exec)
			assert.Equal(t, "Success", exec.Status, "automation must reach a terminal Success status")
			require.NotNil(t, exec.EndTime, "terminal execution must have an EndTime")
			require.Len(t, exec.Steps, len(tt.wantSteps))

			for i, want := range tt.wantSteps {
				assert.Equal(t, want, exec.Steps[i].StepName)
				assert.Equal(t, "Success", exec.Steps[i].StepStatus,
					"each step must be advanced to Success")
			}

			// DescribeAutomationStepExecutions returns the populated steps too.
			steps, err := b.DescribeAutomationStepExecutions(
				ctx,
				&ssm.DescribeAutomationStepExecutionsInput{
					AutomationExecutionID: start.AutomationExecutionID,
				},
			)
			require.NoError(t, err)
			assert.Len(t, steps.StepExecutions, len(tt.wantSteps))
		})
	}
}

func TestStartAutomationExecution_InProgressObservable(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend().WithAutomationExecDelay(time.Hour)
	ctx := context.TODO()

	start, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
		DocumentName: "AWS-DoSomething",
	})
	require.NoError(t, err)

	got, err := b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
		AutomationExecutionID: start.AutomationExecutionID,
	})
	require.NoError(t, err)
	assert.Equal(t, "InProgress", got.AutomationExecution.Status,
		"with an exec delay the automation must be observable as InProgress")

	b.ForceCompleteAutomations()

	got, err = b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
		AutomationExecutionID: start.AutomationExecutionID,
	})
	require.NoError(t, err)
	assert.Equal(t, "Success", got.AutomationExecution.Status)
}

// ---------------------------------------------------------------------------
// 3. Association executions
// ---------------------------------------------------------------------------

func TestAssociationExecutions_Stable(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	create, err := b.CreateAssociation(ctx, &ssm.CreateAssociationInput{
		Name:       "AWS-RunShellScript",
		InstanceID: "i-0123456789abcdef0",
	})
	require.NoError(t, err)
	assocID := create.AssociationDescription.AssociationID

	first, err := b.DescribeAssociationExecutions(ctx, &ssm.DescribeAssociationExecutionsInput{
		AssociationID: assocID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.AssociationExecutions)
	execID := first.AssociationExecutions[0].ExecutionID
	require.NotEmpty(t, execID)

	// A second call must return the SAME execution ID (not a fresh UUID).
	second, err := b.DescribeAssociationExecutions(ctx, &ssm.DescribeAssociationExecutionsInput{
		AssociationID: assocID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, second.AssociationExecutions)
	assert.Equal(t, execID, second.AssociationExecutions[0].ExecutionID,
		"association execution ID must be stable across calls")

	// Targets for that execution must be stable and reference the instance.
	targetsA, err := b.DescribeAssociationExecutionTargets(
		ctx,
		&ssm.DescribeAssociationExecutionTargetsInput{
			AssociationID: assocID,
			ExecutionID:   execID,
		},
	)
	require.NoError(t, err)
	require.Len(t, targetsA.AssociationExecutionTargets, 1)
	assert.Equal(t, "i-0123456789abcdef0", targetsA.AssociationExecutionTargets[0].ResourceID)
	assert.Equal(t, execID, targetsA.AssociationExecutionTargets[0].ExecutionID)

	targetsB, err := b.DescribeAssociationExecutionTargets(
		ctx,
		&ssm.DescribeAssociationExecutionTargetsInput{
			AssociationID: assocID,
			ExecutionID:   execID,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, targetsA.AssociationExecutionTargets, targetsB.AssociationExecutionTargets,
		"association execution targets must be stable across calls")

	// StartAssociationsOnce appends a new stable execution record.
	_, err = b.StartAssociationsOnce(ctx, &ssm.StartAssociationsOnceInput{
		AssociationIDs: []string{assocID},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, b.AssociationExecutionCount(assocID),
		"a one-time run must append a new execution record")
}

// ---------------------------------------------------------------------------
// 4. Sessions
// ---------------------------------------------------------------------------

func TestSessions_TerminatedEvictionBounded(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	// Create and terminate more sessions than the retained-history cap (200).
	const total = 260
	for range total {
		out, err := b.StartSession(ctx, &ssm.StartSessionInput{Target: "i-1"})
		require.NoError(t, err)

		_, err = b.TerminateSession(ctx, &ssm.TerminateSessionInput{SessionID: out.SessionID})
		require.NoError(t, err)
	}

	assert.LessOrEqual(t, b.SessionCount(), 200,
		"terminated sessions must be bounded to the retained-history cap")
}

func TestJanitor_SweepTerminatedSessions(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	nowUnix := float64(time.Now().Unix())
	oldEnd := nowUnix - (48 * 60 * 60) // 48h ago, past 24h retention
	recentEnd := nowUnix - 60          // 1 min ago, within retention

	b.AddTerminatedSessionInternal("session-old", oldEnd)
	b.AddTerminatedSessionInternal("session-recent", recentEnd)
	require.Equal(t, 2, b.SessionCount())

	j := ssm.NewJanitor(b, ssm.DefaultJanitorInterval)
	j.SweepOnce(context.TODO())

	assert.Equal(t, 1, b.SessionCount(),
		"the aged terminated session must be evicted, the recent one retained")

	sessions, err := b.DescribeSessions(context.TODO(), &ssm.DescribeSessionsInput{})
	require.NoError(t, err)
	require.Len(t, sessions.Sessions, 1)
	assert.Equal(t, "session-recent", sessions.Sessions[0].SessionID)
}

// ---------------------------------------------------------------------------
// 5. Patch / inventory
// ---------------------------------------------------------------------------

func TestGetDefaultPatchBaseline_StableRealID(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	first, err := b.GetDefaultPatchBaseline(ctx, &ssm.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(first.BaselineID, "pb-"),
		"default baseline ID must be well-shaped")
	assert.NotEqual(t, "pb-00000000000000000", first.BaselineID,
		"must not return the fabricated all-zeros ID")

	second, err := b.GetDefaultPatchBaseline(ctx, &ssm.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)
	assert.Equal(t, first.BaselineID, second.BaselineID,
		"default baseline ID must be stable for a given OS")

	// A registered default overrides the synthetic one.
	blID := createTestBaseline(t, b, "custom-baseline")
	_, err = b.RegisterDefaultPatchBaseline(ctx, &ssm.RegisterDefaultPatchBaselineInput{
		BaselineID: blID,
	})
	require.NoError(t, err)

	got, err := b.GetDefaultPatchBaseline(ctx, &ssm.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)
	assert.Equal(t, blID, got.BaselineID,
		"a registered default must be returned over the synthetic one")
}

func TestDescribeEffectivePatches_FromApprovedAndCatalog(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	create, err := b.CreatePatchBaseline(ctx, &ssm.CreatePatchBaselineInput{
		Name:                           "eff-baseline",
		OperatingSystem:                "AMAZON_LINUX_2",
		ApprovedPatches:                []string{"CVE-2024-0001", "CVE-2024-0002"},
		ApprovedPatchesComplianceLevel: "CRITICAL",
	})
	require.NoError(t, err)

	out, err := b.DescribeEffectivePatchesForPatchBaseline(
		ctx,
		&ssm.DescribeEffectivePatchesForPatchBaselineInput{BaselineID: create.BaselineID},
	)
	require.NoError(t, err)
	require.Len(t, out.EffectivePatches, 2,
		"effective patches must be derived from the baseline's approved patches")

	for _, ep := range out.EffectivePatches {
		require.NotNil(t, ep.Patch)
		require.NotNil(t, ep.PatchStatus)
		assert.Equal(t, "EXPLICIT_APPROVED", ep.PatchStatus.DeploymentStatus)
		assert.Equal(t, "CRITICAL", ep.PatchStatus.ComplianceLevel)
	}

	// Unknown baseline still errors distinctly.
	_, err = b.DescribeEffectivePatchesForPatchBaseline(
		ctx,
		&ssm.DescribeEffectivePatchesForPatchBaselineInput{BaselineID: "pb-unknown"},
	)
	require.ErrorIs(t, err, ssm.ErrPatchBaselineNotFound)
}

func TestInventory_DeletionJobRecorded(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	_, err := b.PutInventory(ctx, &ssm.PutInventoryInput{
		InstanceID: "i-1",
		Items: []ssm.InventoryItem{
			{TypeName: "AWS:Application", SchemaVersion: "1.0"},
		},
	})
	require.NoError(t, err)

	del, err := b.DeleteInventory(ctx, &ssm.DeleteInventoryInput{TypeName: "AWS:Application"})
	require.NoError(t, err)
	require.NotEmpty(t, del.DeletionID, "DeleteInventory must return a DeletionId")

	list, err := b.DescribeInventoryDeletions(ctx, &ssm.DescribeInventoryDeletionsInput{})
	require.NoError(t, err)
	require.Len(t, list.InventoryDeletions, 1,
		"the recorded deletion job must be returned")

	// Filtering by the returned DeletionId returns the same job.
	filtered, err := b.DescribeInventoryDeletions(ctx, &ssm.DescribeInventoryDeletionsInput{
		DeletionID: del.DeletionID,
	})
	require.NoError(t, err)
	assert.Len(t, filtered.InventoryDeletions, 1)
}
