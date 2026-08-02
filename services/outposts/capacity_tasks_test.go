package outposts_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

// capacityTaskTransitionWait is long enough for the backend's 100ms
// simulated REQUESTED -> COMPLETED transition to have fired.
const capacityTaskTransitionWait = 250 * time.Millisecond

// waitForCapacityTaskCompletion starts a capacity task for one InstanceType
// pool against assetID, waits for it to complete, and returns once
// GetCapacityTask confirms COMPLETED.
func waitForCapacityTaskCompletion(
	t *testing.T, client *outpostssdk.Client, outpostID, assetID *string, instanceType string, count int32,
) {
	t.Helper()

	start, err := client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: outpostID,
		AssetId:           assetID,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String(instanceType), Count: count},
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.CapacityTaskStatusRequested, start.CapacityTaskStatus)

	time.Sleep(capacityTaskTransitionWait)

	got, err := client.GetCapacityTask(t.Context(), &outpostssdk.GetCapacityTaskInput{
		OutpostIdentifier: outpostID,
		CapacityTaskId:    start.CapacityTaskId,
	})
	require.NoError(t, err)
	require.Equal(t, types.CapacityTaskStatusCompleted, got.CapacityTaskStatus)
}

func TestStartCapacityTask_DryRunDoesNotMutateCapacity(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)
	require.Len(t, assets.Assets, 1)

	out, err := client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		DryRun:            true,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String("m5.xlarge"), Count: 2},
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.CapacityTaskStatusCompleted, out.CapacityTaskStatus, "a dry run completes synchronously")

	time.Sleep(capacityTaskTransitionWait)

	instTypes, err := client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
		OutpostId: created.OutpostId,
	})
	require.NoError(t, err)
	require.Empty(t, instTypes.InstanceTypes, "a dry run must not mutate real capacity")
}

func TestStartCapacityTask_OnlyOneActivePerOutpostOrderPair(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)

	pools := []types.InstanceTypeCapacity{{InstanceType: aws.String("c5.2xlarge"), Count: 1}}

	_, err = client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools:     pools,
	})
	require.NoError(t, err)

	_, err = client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools:     pools,
	})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}

func TestCancelCapacityTask(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)

	start, err := client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String("m5.xlarge"), Count: 1},
		},
	})
	require.NoError(t, err)

	_, err = client.CancelCapacityTask(t.Context(), &outpostssdk.CancelCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		CapacityTaskId:    start.CapacityTaskId,
	})
	require.NoError(t, err)

	got, err := client.GetCapacityTask(t.Context(), &outpostssdk.GetCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		CapacityTaskId:    start.CapacityTaskId,
	})
	require.NoError(t, err)
	require.Equal(t, types.CapacityTaskStatusCancelled, got.CapacityTaskStatus)

	// Cancelling an already-terminal task is rejected.
	_, err = client.CancelCapacityTask(t.Context(), &outpostssdk.CancelCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		CapacityTaskId:    start.CapacityTaskId,
	})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}

func TestListCapacityTasks_FiltersByStatus(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)

	_, err = client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String("m5.xlarge"), Count: 1},
		},
	})
	require.NoError(t, err)

	out, err := client.ListCapacityTasks(t.Context(), &outpostssdk.ListCapacityTasksInput{
		OutpostIdentifierFilter:  created.OutpostId,
		CapacityTaskStatusFilter: []types.CapacityTaskStatus{types.CapacityTaskStatusRequested},
	})
	require.NoError(t, err)
	require.Len(t, out.CapacityTasks, 1)

	out, err = client.ListCapacityTasks(t.Context(), &outpostssdk.ListCapacityTasksInput{
		OutpostIdentifierFilter:  created.OutpostId,
		CapacityTaskStatusFilter: []types.CapacityTaskStatus{types.CapacityTaskStatusCompleted},
	})
	require.NoError(t, err)
	require.Empty(t, out.CapacityTasks)
}

func TestListBlockingInstancesForCapacityTask_EmptyButValidated(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{OutpostIdentifier: created.OutpostId})
	require.NoError(t, err)

	start, err := client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: created.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String("m5.xlarge"), Count: 1},
		},
	})
	require.NoError(t, err)

	out, err := client.ListBlockingInstancesForCapacityTask(
		t.Context(),
		&outpostssdk.ListBlockingInstancesForCapacityTaskInput{
			OutpostIdentifier: created.OutpostId,
			CapacityTaskId:    start.CapacityTaskId,
		},
	)
	require.NoError(t, err)
	require.Empty(t, out.BlockingInstances)

	_, err = client.ListBlockingInstancesForCapacityTask(
		t.Context(),
		&outpostssdk.ListBlockingInstancesForCapacityTaskInput{
			OutpostIdentifier: created.OutpostId,
			CapacityTaskId:    aws.String("ct-does-not-exist"),
		},
	)
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}
