package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mgn"
)

// seedTwoSourceServersViaImport drives StartImport with a two-row CSV,
// waiting until both rows have produced a SourceServer. Unlike
// seedSourceServerViaImport (one row -> one item), this returns both
// SourceServerIDs so a caller can drive per-item assertions across a real
// collection response.
func seedTwoSourceServersViaImport(t *testing.T, h *mgn.Handler, client *mgnsdk.Client, hostA, hostB string) []string {
	t.Helper()

	ctx := t.Context()

	s3 := newMockS3()
	s3.put("mgn-import-bucket", "servers.csv", "mgn:server:hostname\n"+hostA+"\n"+hostB+"\n")
	h.Backend.SetS3Backend(s3)

	_, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
		S3BucketSource: &types.S3BucketSource{
			S3Bucket: aws.String("mgn-import-bucket"), S3Key: aws.String("servers.csv"),
		},
	})
	require.NoError(t, err)

	var ids []string

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
		if describeErr != nil || len(out.Items) != 2 {
			return false
		}

		ids = []string{aws.ToString(out.Items[0].SourceServerID), aws.ToString(out.Items[1].SourceServerID)}

		return true
	}, defaultAsyncWait, defaultAsyncPoll, "StartImport never created both source servers")

	return ids
}

// TestSDKRoundTrip_ReplicationNestedLists is a typed round-trip test
// (gopherstack-21my) for the two nested lists inside DescribeSourceServers'
// per-item DataReplicationInfo: ReplicatedDisks[] and
// DataReplicationInitiation.Steps[]. Both are populated by this backend's
// own deterministic replication-progression timer (sourceservers.go), not
// invented for the test. Seeds two source servers so the collection itself
// has more than one item, waits for both to reach DataReplicationState
// CONTINUOUS, then asserts every nested field via the real aws-sdk-go-v2
// client.
func TestSDKRoundTrip_ReplicationNestedLists(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	ids := seedTwoSourceServersViaImport(t, h, client, "web-01", "db-01")

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
		if describeErr != nil || len(out.Items) != 2 {
			return false
		}

		for _, item := range out.Items {
			if item.DataReplicationInfo == nil ||
				item.DataReplicationInfo.DataReplicationState != types.DataReplicationStateContinuous {
				return false
			}
		}

		return true
	}, defaultAsyncWait, defaultAsyncPoll, "source servers never reached CONTINUOUS")

	out, err := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
	require.NoError(t, err)
	require.Len(t, out.Items, 2)

	seenIDs := map[string]bool{}
	for _, item := range out.Items {
		id := aws.ToString(item.SourceServerID)
		require.Contains(t, ids, id)
		seenIDs[id] = true

		require.NotNil(t, item.DataReplicationInfo)
		dri := item.DataReplicationInfo
		require.Equal(t, types.DataReplicationStateContinuous, dri.DataReplicationState)

		require.Len(t, dri.ReplicatedDisks, 1, "one replicated disk per source server")
		disk := dri.ReplicatedDisks[0]
		require.Equal(t, "/dev/sda1", aws.ToString(disk.DeviceName))
		require.Positive(t, disk.TotalStorageBytes)
		require.Equal(t, disk.TotalStorageBytes, disk.ReplicatedStorageBytes,
			"CONTINUOUS state must show the disk fully replicated")
		require.Zero(t, disk.BackloggedStorageBytes)

		require.NotNil(t, dri.DataReplicationInitiation)
		steps := dri.DataReplicationInitiation.Steps
		require.Len(t, steps, 12, "all 12 initiation steps must round-trip")
		for _, step := range steps {
			require.Equal(t, types.DataReplicationInitiationStepStatusSucceeded, step.Status)
			require.NotEmpty(t, step.Name)
		}

		require.NotNil(t, item.LifeCycle)
		require.Equal(t, types.LifeCycleStateReadyForTest, item.LifeCycle.State)
	}
	require.Len(t, seenIDs, 2)
}

// TestSDKRoundTrip_JobParticipatingServers is a typed round-trip test
// (gopherstack-21my) for DescribeJobs' per-item ParticipatingServers[] --
// a nested list whose members must be individually distinguishable by
// SourceServerID and carry a real LaunchedEc2InstanceID once the job
// completes.
func TestSDKRoundTrip_JobParticipatingServers(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	ids := seedTwoSourceServersViaImport(t, h, client, "app-a", "app-b")

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
		if describeErr != nil || len(out.Items) != 2 {
			return false
		}

		for _, item := range out.Items {
			if item.LifeCycle == nil || item.LifeCycle.State != types.LifeCycleStateReadyForTest {
				return false
			}
		}

		return true
	}, defaultAsyncWait, defaultAsyncPoll, "source servers never reached READY_FOR_TEST")

	testOut, err := client.StartTest(ctx, &mgnsdk.StartTestInput{SourceServerIDs: ids})
	require.NoError(t, err)
	jobID := aws.ToString(testOut.Job.JobID)

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeJobs(ctx, &mgnsdk.DescribeJobsInput{
			Filters: &types.DescribeJobsRequestFilters{JobIDs: []string{jobID}},
		})

		return describeErr == nil && len(out.Items) == 1 && out.Items[0].Status == types.JobStatusCompleted
	}, defaultAsyncWait, defaultAsyncPoll, "job never reached COMPLETED")

	out, err := client.DescribeJobs(ctx, &mgnsdk.DescribeJobsInput{
		Filters: &types.DescribeJobsRequestFilters{JobIDs: []string{jobID}},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.Len(t, out.Items[0].ParticipatingServers, 2, "both participants must round-trip")

	seenIDs := map[string]bool{}
	for _, p := range out.Items[0].ParticipatingServers {
		id := aws.ToString(p.SourceServerID)
		require.Contains(t, ids, id)
		seenIDs[id] = true
		require.Equal(t, types.LaunchStatusLaunched, p.LaunchStatus)
		require.NotEmpty(t, aws.ToString(p.LaunchedEc2InstanceID))
	}
	require.Len(t, seenIDs, 2)
}
