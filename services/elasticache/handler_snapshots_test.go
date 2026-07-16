package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		snapshotName       string
		clusterID          string
		replicationGroupID string
		wantStatus         string
		wantErr            bool
	}{
		{
			name:         "create_from_cluster",
			snapshotName: "my-snap",
			clusterID:    "snap-cluster",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("snap-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantStatus: "available",
		},
		{
			name:               "create_from_replication_group",
			snapshotName:       "rg-snap",
			replicationGroupID: "rg-for-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-for-snap"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
			wantStatus: "available",
		},
		{
			name:         "create_snapshot_already_exists",
			snapshotName: "dup-snap",
			clusterID:    "snap-cluster2",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("snap-cluster2"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("dup-snap"),
					CacheClusterId: aws.String("snap-cluster2"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name:         "create_cluster_not_found",
			snapshotName: "no-snap",
			clusterID:    "does-not-exist",
			wantErr:      true,
		},
		{
			name:               "invalid_both_sources",
			snapshotName:       "both-snap",
			clusterID:          "some-cluster",
			replicationGroupID: "some-rg",
			wantErr:            true,
		},
		{
			name:         "invalid_no_source",
			snapshotName: "no-source-snap",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.CreateSnapshotInput{
				SnapshotName: aws.String(tt.snapshotName),
			}
			if tt.clusterID != "" {
				input.CacheClusterId = aws.String(tt.clusterID)
			}
			if tt.replicationGroupID != "" {
				input.ReplicationGroupId = aws.String(tt.replicationGroupID)
			}

			out, err := client.CreateSnapshot(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Snapshot)
			assert.Equal(t, tt.snapshotName, aws.ToString(out.Snapshot.SnapshotName))
			assert.Equal(t, tt.wantStatus, aws.ToString(out.Snapshot.SnapshotStatus))
		})
	}
}

func TestDescribeSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		snapshotName    string
		filterClusterID string
		filterRGID      string
		wantCount       int
		wantErr         bool
	}{
		{
			name:      "describe_all",
			wantCount: 3,
		},
		{
			name:            "describe_by_cluster",
			filterClusterID: "desc-snap-cluster",
			wantCount:       2,
		},
		{
			name:       "describe_by_replication_group",
			filterRGID: "desc-snap-rg",
			wantCount:  1,
		},
		{
			name:         "describe_specific",
			snapshotName: "snap-a",
			wantCount:    1,
		},
		{
			name:         "describe_not_found",
			snapshotName: "does-not-exist",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			// Create a cluster and two snapshots for describe tests.
			_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("desc-snap-cluster"),
				Engine:         aws.String("redis"),
			})
			require.NoError(t, err)

			for _, sname := range []string{"snap-a", "snap-b"} {
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String(sname),
					CacheClusterId: aws.String("desc-snap-cluster"),
				})
				require.NoError(t, err)
			}

			// Create a replication group and one snapshot for RG-filter tests.
			_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String("desc-snap-rg"),
				ReplicationGroupDescription: aws.String("test"),
			})
			require.NoError(t, err)
			_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
				SnapshotName:       aws.String("rg-snap-x"),
				ReplicationGroupId: aws.String("desc-snap-rg"),
			})
			require.NoError(t, err)

			input := &elasticachesdk.DescribeSnapshotsInput{}
			if tt.snapshotName != "" {
				input.SnapshotName = aws.String(tt.snapshotName)
			}
			if tt.filterClusterID != "" {
				input.CacheClusterId = aws.String(tt.filterClusterID)
			}
			if tt.filterRGID != "" {
				input.ReplicationGroupId = aws.String(tt.filterRGID)
			}

			out, err := client.DescribeSnapshots(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Snapshots, tt.wantCount)
		})
	}
}

func TestDeleteSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, client *elasticachesdk.Client)
		name         string
		snapshotName string
		wantErr      bool
	}{
		{
			name:         "success",
			snapshotName: "del-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("del-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("del-snap"),
					CacheClusterId: aws.String("del-cluster"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:         "not_found",
			snapshotName: "ghost-snap",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteSnapshot(t.Context(), &elasticachesdk.DeleteSnapshotInput{
				SnapshotName: aws.String(tt.snapshotName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Snapshot)
			assert.Equal(t, tt.snapshotName, aws.ToString(out.Snapshot.SnapshotName))

			// Verify it is actually gone.
			_, descErr := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{
				SnapshotName: aws.String(tt.snapshotName),
			})
			require.Error(t, descErr)
		})
	}
}

func TestCopySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		sourceSnapshotName string
		targetSnapshotName string
		wantErr            bool
	}{
		{
			name:               "success",
			sourceSnapshotName: "source-snap",
			targetSnapshotName: "target-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("copy-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("source-snap"),
					CacheClusterId: aws.String("copy-cluster"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:               "source_not_found",
			sourceSnapshotName: "does-not-exist",
			targetSnapshotName: "target",
			wantErr:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
				SourceSnapshotName: aws.String(tt.sourceSnapshotName),
				TargetSnapshotName: aws.String(tt.targetSnapshotName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Snapshot)
			assert.Equal(t, tt.targetSnapshotName, aws.ToString(out.Snapshot.SnapshotName))
		})
	}
}

func TestHandler_CopySnapshot_WithKmsKeyId(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("kms-copy-rg"),
		ReplicationGroupDescription: aws.String("for kms copy"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
		SnapshotName:       aws.String("kms-src-snap"),
		ReplicationGroupId: aws.String("kms-copy-rg"),
	})
	require.NoError(t, err)

	out, err := client.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("kms-src-snap"),
		TargetSnapshotName: aws.String("kms-dst-snap"),
		KmsKeyId:           aws.String("arn:aws:kms:us-east-1:000000000000:key/kms-test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "kms-dst-snap", aws.ToString(out.Snapshot.SnapshotName))
}

// ----------------------------------------
// ServerlessCache with UserGroupId in describe response
// ----------------------------------------

func TestHandler_CopySnapshot_CreatesNewSnapshot(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("copy-src-cluster"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
		SnapshotName:   aws.String("copy-src-snap"),
		CacheClusterId: aws.String("copy-src-cluster"),
	})
	require.NoError(t, err)

	out, err := client.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("copy-src-snap"),
		TargetSnapshotName: aws.String("copy-dst-snap"),
	})
	require.NoError(t, err)
	assert.Equal(t, "copy-dst-snap", aws.ToString(out.Snapshot.SnapshotName))

	// Both should exist now.
	desc, err := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{})
	require.NoError(t, err)
	snapNames := make([]string, 0, len(desc.Snapshots))
	for _, s := range desc.Snapshots {
		snapNames = append(snapNames, aws.ToString(s.SnapshotName))
	}
	assert.Contains(t, snapNames, "copy-src-snap")
	assert.Contains(t, snapNames, "copy-dst-snap")
}

// ----------------------------------------
// User — describe filter by ID
// ----------------------------------------

func TestHandler_DescribeSnapshots_FilterByClusterID(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create two clusters with snapshots.
	for _, id := range []string{"snap-cluster-a", "snap-cluster-b"} {
		_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
			CacheClusterId: aws.String(id),
			Engine:         aws.String("redis"),
		})
		require.NoError(t, err)

		_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
			SnapshotName:   aws.String("snap-" + id),
			CacheClusterId: aws.String(id),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{
		CacheClusterId: aws.String("snap-cluster-a"),
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	assert.Equal(t, "snap-cluster-a", aws.ToString(out.Snapshots[0].CacheClusterId))
}

// ----------------------------------------
// Persistence: UserGroupIds survive snapshot/restore
// ----------------------------------------

func TestHandler_DescribeSnapshots_SnapshotSource_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend)
		name           string
		snapshotSource string
		wantNames      []string
		wantCount      int
	}{
		{
			name:           "no_filter_returns_all",
			snapshotSource: "",
			wantCount:      2,
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("manual-snap"),
				})
				require.NoError(t, err)
				elasticache.AddSnapshotInternal(b, "auto-snap", "snap-src-rg", "automated")
			},
		},
		{
			name:           "user_filter_returns_only_manual",
			snapshotSource: "user",
			wantCount:      1,
			wantNames:      []string{"manual-snap"},
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("manual-snap"),
				})
				require.NoError(t, err)
				elasticache.AddSnapshotInternal(b, "auto-snap", "snap-src-rg", "automated")
			},
		},
		{
			name:           "system_filter_returns_only_automated",
			snapshotSource: "system",
			wantCount:      1,
			wantNames:      []string{"auto-snap"},
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("manual-snap"),
				})
				require.NoError(t, err)
				elasticache.AddSnapshotInternal(b, "auto-snap", "snap-src-rg", "automated")
			},
		},
		{
			name:           "system_filter_empty_when_no_automated",
			snapshotSource: "system",
			wantCount:      0,
			wantNames:      []string{},
			setup: func(t *testing.T, client *elasticachesdk.Client, _ *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("only-manual"),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			client := newTestStackSeeded(t, b)

			if tt.setup != nil {
				tt.setup(t, client, b)
			}

			var src *string
			if tt.snapshotSource != "" {
				src = aws.String(tt.snapshotSource)
			}

			out, err := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{
				SnapshotSource: src,
			})
			require.NoError(t, err)
			assert.Len(t, out.Snapshots, tt.wantCount)

			if len(tt.wantNames) > 0 {
				names := make([]string, 0, len(out.Snapshots))
				for _, s := range out.Snapshots {
					names = append(names, aws.ToString(s.SnapshotName))
				}
				assert.ElementsMatch(t, tt.wantNames, names)
			}
		})
	}
}
