package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/require"
)

// TestMarkAsArchived_LifeCycleStatePrecondition proves MarkAsArchived enforces
// api_op_MarkAsArchived.go's documented precondition ("This command only
// works for SourceServers with a lifecycle. state which equals DISCONNECTED
// or CUTOVER"), which the backend did not check before the fix.
func TestMarkAsArchived_LifeCycleStatePrecondition(t *testing.T) {
	t.Parallel()

	t.Run("blocked lifecycle state rejected", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "archive-blocked")
		serverID := aws.ToString(seeded.SourceServerID)

		// Fresh import leaves the server NOT_READY, not DISCONNECTED/CUTOVER.
		_, err := client.MarkAsArchived(ctx, &mgnsdk.MarkAsArchivedInput{SourceServerID: aws.String(serverID)})
		require.Error(t, err)

		var conflict *types.ConflictException
		require.ErrorAs(t, err, &conflict)
	})

	t.Run("cutover state allowed", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "archive-cutover")
		serverID := aws.ToString(seeded.SourceServerID)

		_, err := client.ChangeServerLifeCycleState(ctx, &mgnsdk.ChangeServerLifeCycleStateInput{
			SourceServerID: aws.String(serverID),
			LifeCycle: &types.ChangeServerLifeCycleStateSourceServerLifecycle{
				State: types.ChangeServerLifeCycleStateSourceServerLifecycleStateCutover,
			},
		})
		require.NoError(t, err)

		out, err := client.MarkAsArchived(ctx, &mgnsdk.MarkAsArchivedInput{SourceServerID: aws.String(serverID)})
		require.NoError(t, err)
		require.True(t, aws.ToBool(out.IsArchived))
	})

	t.Run("disconnected state allowed", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "archive-disconnected")
		serverID := aws.ToString(seeded.SourceServerID)

		_, err := client.DisconnectFromService(ctx, &mgnsdk.DisconnectFromServiceInput{
			SourceServerID: aws.String(serverID),
		})
		require.NoError(t, err)

		out, err := client.MarkAsArchived(ctx, &mgnsdk.MarkAsArchivedInput{SourceServerID: aws.String(serverID)})
		require.NoError(t, err)
		require.True(t, aws.ToBool(out.IsArchived))
	})
}

// TestTerminateTargetInstances_LifeCycleStatePrecondition proves
// TerminateTargetInstances enforces api_op_TerminateTargetInstances.go's
// documented block list ("This command will not work for any Source Server
// with a lifecycle.state of TESTING, CUTTING_OVER, or CUTOVER"), which the
// backend did not check before the fix.
func TestTerminateTargetInstances_LifeCycleStatePrecondition(t *testing.T) {
	t.Parallel()

	t.Run("testing state rejected", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "terminate-testing")
		serverID := aws.ToString(seeded.SourceServerID)

		_, err := client.ChangeServerLifeCycleState(ctx, &mgnsdk.ChangeServerLifeCycleStateInput{
			SourceServerID: aws.String(serverID),
			LifeCycle: &types.ChangeServerLifeCycleStateSourceServerLifecycle{
				State: types.ChangeServerLifeCycleStateSourceServerLifecycleStateReadyForTest,
			},
		})
		require.NoError(t, err)

		// StartTest moves the server straight to TESTING (synchronous).
		_, err = client.StartTest(ctx, &mgnsdk.StartTestInput{SourceServerIDs: []string{serverID}})
		require.NoError(t, err)

		_, err = client.TerminateTargetInstances(ctx, &mgnsdk.TerminateTargetInstancesInput{
			SourceServerIDs: []string{serverID},
		})
		require.Error(t, err)

		var conflict *types.ConflictException
		require.ErrorAs(t, err, &conflict)
	})

	t.Run("cutting over state rejected", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "terminate-cutting-over")
		serverID := aws.ToString(seeded.SourceServerID)

		_, err := client.ChangeServerLifeCycleState(ctx, &mgnsdk.ChangeServerLifeCycleStateInput{
			SourceServerID: aws.String(serverID),
			LifeCycle: &types.ChangeServerLifeCycleStateSourceServerLifecycle{
				State: types.ChangeServerLifeCycleStateSourceServerLifecycleStateReadyForCutover,
			},
		})
		require.NoError(t, err)

		// StartCutover moves the server straight to CUTTING_OVER (synchronous).
		_, err = client.StartCutover(ctx, &mgnsdk.StartCutoverInput{SourceServerIDs: []string{serverID}})
		require.NoError(t, err)

		_, err = client.TerminateTargetInstances(ctx, &mgnsdk.TerminateTargetInstancesInput{
			SourceServerIDs: []string{serverID},
		})
		require.Error(t, err)

		var conflict *types.ConflictException
		require.ErrorAs(t, err, &conflict)
	})

	t.Run("cutover state rejected", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "terminate-cutover")
		serverID := aws.ToString(seeded.SourceServerID)

		_, err := client.ChangeServerLifeCycleState(ctx, &mgnsdk.ChangeServerLifeCycleStateInput{
			SourceServerID: aws.String(serverID),
			LifeCycle: &types.ChangeServerLifeCycleStateSourceServerLifecycle{
				State: types.ChangeServerLifeCycleStateSourceServerLifecycleStateCutover,
			},
		})
		require.NoError(t, err)

		_, err = client.TerminateTargetInstances(ctx, &mgnsdk.TerminateTargetInstancesInput{
			SourceServerIDs: []string{serverID},
		})
		require.Error(t, err)

		var conflict *types.ConflictException
		require.ErrorAs(t, err, &conflict)
	})

	t.Run("non-blocked state allowed", func(t *testing.T) {
		t.Parallel()

		h, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		seeded := seedSourceServerViaImport(t, h, client, "terminate-allowed")
		serverID := aws.ToString(seeded.SourceServerID)

		// Fresh import leaves the server NOT_READY -- not in the block list.
		_, err := client.TerminateTargetInstances(ctx, &mgnsdk.TerminateTargetInstancesInput{
			SourceServerIDs: []string{serverID},
		})
		require.NoError(t, err)
	})
}
