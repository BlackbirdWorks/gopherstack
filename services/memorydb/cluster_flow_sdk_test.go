package memorydb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func newClockedSDKClient(t *testing.T, delay time.Duration) (*memorydbsdk.Client, *time.Time) {
	t.Helper()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	now := time.Date(2024, 3, 1, 10, 0, 0, 0, time.UTC)
	b.SetClock(func() time.Time { return now })
	b.SetLifecycleDelay(delay)

	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return newMemorydbSDKClient(t, h), &now
}

func TestDeleteCluster_ReportsDeleting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		final string
	}{
		{name: "plain"},
		{name: "final snapshot", final: "final-snap"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newClockedSDKClient(t, 0)
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
				ClusterName: aws.String("del-cluster"),
				NodeType:    aws.String("db.r6g.large"),
				ACLName:     aws.String("open-access"),
			})
			require.NoError(t, err)

			in := &memorydbsdk.DeleteClusterInput{ClusterName: aws.String("del-cluster")}
			if tt.final != "" {
				in.FinalSnapshotName = aws.String(tt.final)
			}

			out, err := client.DeleteCluster(ctx, in)
			require.NoError(t, err)
			assert.Equal(t, "deleting", aws.ToString(out.Cluster.Status))
		})
	}
}

func TestDeleteCluster_FinalSnapshotNameConflict(t *testing.T) {
	t.Parallel()

	client, _ := newClockedSDKClient(t, 0)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("dup-cluster"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
		ClusterName:  aws.String("dup-cluster"),
		SnapshotName: aws.String("taken"),
	})
	require.NoError(t, err)

	_, err = client.DeleteCluster(ctx, &memorydbsdk.DeleteClusterInput{
		ClusterName:       aws.String("dup-cluster"),
		FinalSnapshotName: aws.String("taken"),
	})
	require.ErrorContains(t, err, "SnapshotAlreadyExistsFault")

	got, err := client.DescribeClusters(ctx, &memorydbsdk.DescribeClustersInput{ClusterName: aws.String("dup-cluster")})
	require.NoError(t, err)
	assert.Len(t, got.Clusters, 1, "rejected delete must leave the cluster in place")
}

func TestDescribeClusters_NodeCreateTimeStable(t *testing.T) {
	t.Parallel()

	client, now := newClockedSDKClient(t, 0)
	ctx := t.Context()
	created := *now

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("ct-cluster"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	*now = now.Add(3 * time.Hour)

	out, err := client.DescribeClusters(ctx, &memorydbsdk.DescribeClustersInput{
		ClusterName:      aws.String("ct-cluster"),
		ShowShardDetails: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.Clusters, 1)
	require.NotEmpty(t, out.Clusters[0].Shards)

	for _, node := range out.Clusters[0].Shards[0].Nodes {
		assert.Equal(t, created.Unix(), node.CreateTime.Unix(), "node %s", aws.ToString(node.Name))
	}
}

func TestCreateCluster_SnapshotRestoreHonoursExplicitInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nodeType string
		shards   *int32
		want     string
		wantSh   int32
	}{
		{name: "inherits", want: "db.r6g.large", wantSh: 3},
		{name: "explicit node type", nodeType: "db.t4g.small", want: "db.t4g.small", wantSh: 3},
		{name: "explicit shards", nodeType: "db.t4g.small", shards: aws.Int32(2), want: "db.t4g.small", wantSh: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newClockedSDKClient(t, 0)
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
				ClusterName: aws.String("src"),
				NodeType:    aws.String("db.r6g.large"),
				ACLName:     aws.String("open-access"),
				NumShards:   aws.Int32(3),
			})
			require.NoError(t, err)

			_, err = client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
				ClusterName: aws.String("src"), SnapshotName: aws.String("snap"),
			})
			require.NoError(t, err)

			in := &memorydbsdk.CreateClusterInput{
				ClusterName:  aws.String("restored"),
				ACLName:      aws.String("open-access"),
				SnapshotName: aws.String("snap"),
				NumShards:    tt.shards,
				NodeType:     aws.String("db.r6g.large"),
			}
			if tt.nodeType != "" {
				in.NodeType = aws.String(tt.nodeType)
			}

			out, err := client.CreateCluster(ctx, in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.Cluster.NodeType))
			assert.Equal(t, tt.wantSh, aws.ToInt32(out.Cluster.NumberOfShards))
		})
	}
}

func TestUpdateCluster_TopologyChangeReportsUpdating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		update *memorydbsdk.UpdateClusterInput
		want   string
	}{
		{
			name:   "node type",
			update: &memorydbsdk.UpdateClusterInput{NodeType: aws.String("db.r6g.xlarge")},
			want:   "updating",
		},
		{
			name: "replica count",
			update: &memorydbsdk.UpdateClusterInput{
				ReplicaConfiguration: &memorydbtypes.ReplicaConfigurationRequest{ReplicaCount: 2},
			},
			want: "updating",
		},
		{
			name:   "description only",
			update: &memorydbsdk.UpdateClusterInput{Description: aws.String("x")},
			want:   "available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, now := newClockedSDKClient(t, time.Minute)
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
				ClusterName: aws.String("upd"),
				NodeType:    aws.String("db.r6g.large"),
				ACLName:     aws.String("open-access"),
			})
			require.NoError(t, err)

			*now = now.Add(2 * time.Minute)

			tt.update.ClusterName = aws.String("upd")
			out, err := client.UpdateCluster(ctx, tt.update)
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.Cluster.Status))

			*now = now.Add(2 * time.Minute)

			desc, err := client.DescribeClusters(
				ctx,
				&memorydbsdk.DescribeClustersInput{ClusterName: aws.String("upd")},
			)
			require.NoError(t, err)
			assert.Equal(t, "available", aws.ToString(desc.Clusters[0].Status))
		})
	}
}
