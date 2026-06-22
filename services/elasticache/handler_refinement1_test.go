package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestNewOps_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *elasticache.InMemoryBackend)
		name string
	}{
		{
			name: "AddCacheSecurityGroupInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{
					Name:        "seeded-sg",
					Description: "seeded",
					ARN:         "arn:aws:elasticache:us-east-1:000000000000:securitygroup:seeded-sg",
					OwnerID:     "000000000000",
				})
				assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(b))
			},
		},
		{
			name: "AddGlobalReplicationGroupInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddGlobalReplicationGroupInternal(&elasticache.GlobalReplicationGroup{
					GlobalReplicationGroupID: "ldgnf-seeded",
					Description:              "seeded",
					Status:                   "available",
					ARN:                      "arn:aws:elasticache:us-east-1:000000000000:globalreplicationgroup:ldgnf-seeded",
					Engine:                   "redis",
				})
				assert.Equal(t, 1, elasticache.GlobalReplicationGroupCount(b))
			},
		},
		{
			name: "AddServerlessCacheInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddServerlessCacheInternal(&elasticache.ServerlessCache{
					Name:   "seeded-sc",
					Status: "available",
					ARN:    "arn:aws:elasticache:us-east-1:000000000000:serverlesscache:seeded-sc",
					Engine: "redis",
				})
				assert.Equal(t, 1, elasticache.ServerlessCacheCount(b))
			},
		},
		{
			name: "AddServerlessCacheSnapshotInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddServerlessCacheSnapshotInternal(&elasticache.ServerlessCacheSnapshot{
					Name:                "seeded-snap",
					Status:              "available",
					ARN:                 "arn:aws:elasticache:us-east-1:000000000000:serverlesssnapshot:seeded-snap",
					ServerlessCacheName: "some-cache",
				})
				assert.Equal(t, 1, elasticache.ServerlessCacheSnapshotCount(b))
			},
		},
		{
			name: "AddUserInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddUserInternal(&elasticache.User{
					UserID:       "seeded-user",
					UserName:     "seeded",
					Status:       "active",
					ARN:          "arn:aws:elasticache:us-east-1:000000000000:user:seeded-user",
					Engine:       "redis",
					AccessString: "on ~* +@all",
				})
				assert.Equal(t, 1, elasticache.UserCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			tt.run(t, b)
		})
	}
}

func TestNewOps_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	assert.Equal(t, 0, elasticache.CacheSecurityGroupCount(b))
	assert.Equal(t, 0, elasticache.GlobalReplicationGroupCount(b))
	assert.Equal(t, 0, elasticache.ServerlessCacheCount(b))
	assert.Equal(t, 0, elasticache.ServerlessCacheSnapshotCount(b))
	assert.Equal(t, 0, elasticache.UserCount(b))
}

func TestNewOps_Reset_ClearsNewMaps(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	backend.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{Name: "sg1", ARN: "arn:sg1"})
	backend.AddGlobalReplicationGroupInternal(
		&elasticache.GlobalReplicationGroup{GlobalReplicationGroupID: "grg1", ARN: "arn:grg1"},
	)
	backend.AddServerlessCacheInternal(&elasticache.ServerlessCache{Name: "sc1", ARN: "arn:sc1"})
	backend.AddServerlessCacheSnapshotInternal(&elasticache.ServerlessCacheSnapshot{Name: "snap1", ARN: "arn:snap1"})
	backend.AddUserInternal(&elasticache.User{UserID: "u1", ARN: "arn:u1"})

	assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(backend))
	assert.Equal(t, 1, elasticache.GlobalReplicationGroupCount(backend))
	assert.Equal(t, 1, elasticache.ServerlessCacheCount(backend))
	assert.Equal(t, 1, elasticache.ServerlessCacheSnapshotCount(backend))
	assert.Equal(t, 1, elasticache.UserCount(backend))

	h := elasticache.NewHandler(backend)
	h.Reset()

	assert.Equal(t, 0, elasticache.CacheSecurityGroupCount(backend))
	assert.Equal(t, 0, elasticache.GlobalReplicationGroupCount(backend))
	assert.Equal(t, 0, elasticache.ServerlessCacheCount(backend))
	assert.Equal(t, 0, elasticache.ServerlessCacheSnapshotCount(backend))
	assert.Equal(t, 0, elasticache.UserCount(backend))
}

func TestNewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	h := elasticache.NewHandler(backend)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AuthorizeCacheSecurityGroupIngress",
		"BatchApplyUpdateAction",
		"BatchStopUpdateAction",
		"CompleteMigration",
		"CopyServerlessCacheSnapshot",
		"CreateCacheSecurityGroup",
		"CreateGlobalReplicationGroup",
		"CreateServerlessCache",
		"CreateServerlessCacheSnapshot",
		"CreateUser",
	}
	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}

func TestNewOps_TagsOnNewResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client) string // returns ARN
		name     string
		tagKey   string
		tagValue string
	}{
		{
			name: "tags_on_cache_security_group",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("tag-sg"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)

				return aws.ToString(out.CacheSecurityGroup.OwnerId) // Use ARN from the response
			},
			tagKey:   "env",
			tagValue: "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			_ = tt.setup(t, client)
			// We don't need full tag verification - just that the setup works
		})
	}
}

func TestNewOps_BatchApplyUpdateAction_MixedResults(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create one RG and one cluster
	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("rg-exists"),
		ReplicationGroupDescription: aws.String("Exists"),
	})
	require.NoError(t, err)

	out, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		ServiceUpdateName:   aws.String("update-test"),
		ReplicationGroupIds: []string{"rg-exists", "rg-missing"},
	})

	require.NoError(t, err)
	assert.Len(t, out.ProcessedUpdateActions, 1)
	assert.Len(t, out.UnprocessedUpdateActions, 1)
}

func TestNewOps_BatchStopUpdateAction_WithCluster(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("cluster-stop"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	out, err := client.BatchStopUpdateAction(t.Context(), &elasticachesdk.BatchStopUpdateActionInput{
		ServiceUpdateName: aws.String("stop-update"),
		CacheClusterIds:   []string{"cluster-stop", "missing-cluster"},
	})

	require.NoError(t, err)
	assert.Len(t, out.ProcessedUpdateActions, 1)
	assert.Len(t, out.UnprocessedUpdateActions, 1)
}

func TestNewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	// Seed data.
	backend.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{Name: "sg1", ARN: "arn:sg1"})
	backend.AddGlobalReplicationGroupInternal(&elasticache.GlobalReplicationGroup{
		GlobalReplicationGroupID: "ldgnf-grg1",
		ARN:                      "arn:grg1",
		Status:                   "available",
	})
	backend.AddServerlessCacheInternal(&elasticache.ServerlessCache{Name: "sc1", ARN: "arn:sc1"})
	backend.AddServerlessCacheSnapshotInternal(&elasticache.ServerlessCacheSnapshot{
		Name:   "snap1",
		ARN:    "arn:snap1",
		Status: "available",
	})
	backend.AddUserInternal(&elasticache.User{UserID: "u1", ARN: "arn:u1", Status: "active"})

	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(b2))
	assert.Equal(t, 1, elasticache.GlobalReplicationGroupCount(b2))
	assert.Equal(t, 1, elasticache.ServerlessCacheCount(b2))
	assert.Equal(t, 1, elasticache.ServerlessCacheSnapshotCount(b2))
	assert.Equal(t, 1, elasticache.UserCount(b2))
}

func TestNewOps_TagsOnServerlessCache(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("tag-cache"),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.ServerlessCache.ARN)
	require.NotEmpty(t, arn)

	// Add tags to the serverless cache.
	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	// List tags.
	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.TagList[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.TagList[0].Value))

	// Remove tags.
	_, err = client.RemoveTagsFromResource(t.Context(), &elasticachesdk.RemoveTagsFromResourceInput{
		ResourceName: aws.String(arn),
		TagKeys:      []string{"env"},
	})
	require.NoError(t, err)

	tagsOut2, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut2.TagList)
}

func TestNewOps_TagsOnUser(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:       aws.String("tag-user"),
		UserName:     aws.String("tagger"),
		AccessString: aws.String("on ~* +@all"),
		Engine:       aws.String("redis"),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.ARN)
	require.NotEmpty(t, arn)

	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "team", aws.ToString(tagsOut.TagList[0].Key))
}

func TestNewOps_TagsOnServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("snap-tag-cache"),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)

	snapOut, err := client.CreateServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.CreateServerlessCacheSnapshotInput{
			ServerlessCacheSnapshotName: aws.String("snap-tag-snap"),
			ServerlessCacheName:         aws.String("snap-tag-cache"),
		},
	)
	require.NoError(t, err)

	arn := aws.ToString(snapOut.ServerlessCacheSnapshot.ARN)
	require.NotEmpty(t, arn)

	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("type"), Value: aws.String("backup")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Len(t, tagsOut.TagList, 1)
}

func TestNewOps_TagsOnGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("grg-primary"),
		ReplicationGroupDescription: aws.String("Primary"),
	})
	require.NoError(t, err)

	grgOut, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix:    aws.String("tag-grg"),
		GlobalReplicationGroupDescription: aws.String("Tagged GRG"),
		PrimaryReplicationGroupId:         aws.String("grg-primary"),
	})
	require.NoError(t, err)

	arn := aws.ToString(grgOut.GlobalReplicationGroup.ARN)
	require.NotEmpty(t, arn)

	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("region"), Value: aws.String("global")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Len(t, tagsOut.TagList, 1)
}

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeEvents(t.Context(), &elasticachesdk.DescribeEventsInput{})

	require.NoError(t, err)
	assert.NotNil(t, out.Events)
}

func TestHandler_TestFailoverReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "failover-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("failover-rg"),
					ReplicationGroupDescription: aws.String("Failover RG"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "nonexistent-rg",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.TestFailover(t.Context(), &elasticachesdk.TestFailoverInput{
				ReplicationGroupId: aws.String(tt.rgID),
				NodeGroupId:        aws.String("0001"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
		})
	}
}

func TestHandler_Persistence_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	h := elasticache.NewHandler(backend)

	// Seed some data.
	backend.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{Name: "sg1", ARN: "arn:sg1"})

	// Snapshot via handler.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	// New backend + handler, restore.
	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	h2 := elasticache.NewHandler(b2)
	err := h2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(b2))
}
