package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// CreateCacheSecurityGroup
// ----------------------------------------

func TestCreateCacheSecurityGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		groupName   string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			groupName:   "my-sg",
			description: "My security group",
		},
		{
			name:      "already_exists",
			groupName: "dup-sg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("dup-sg"),
					Description:            aws.String("first"),
				})
				require.NoError(t, err)
			},
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

			out, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
				CacheSecurityGroupName: aws.String(tt.groupName),
				Description:            aws.String(tt.description),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheSecurityGroup)
			assert.Equal(t, tt.groupName, aws.ToString(out.CacheSecurityGroup.CacheSecurityGroupName))
			assert.Equal(t, tt.description, aws.ToString(out.CacheSecurityGroup.Description))
			assert.NotEmpty(t, aws.ToString(out.CacheSecurityGroup.OwnerId))
		})
	}
}

// ----------------------------------------
// AuthorizeCacheSecurityGroupIngress
// ----------------------------------------

func TestAuthorizeCacheSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		groupName string
		ec2Group  string
		ownerID   string
		wantErr   bool
	}{
		{
			name:      "success",
			groupName: "my-sg",
			ec2Group:  "default",
			ownerID:   "123456789012",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("my-sg"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			ec2Group:  "default",
			ownerID:   "123456789012",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.AuthorizeCacheSecurityGroupIngress(
				t.Context(),
				&elasticachesdk.AuthorizeCacheSecurityGroupIngressInput{
					CacheSecurityGroupName:  aws.String(tt.groupName),
					EC2SecurityGroupName:    aws.String(tt.ec2Group),
					EC2SecurityGroupOwnerId: aws.String(tt.ownerID),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheSecurityGroup)
			assert.Equal(t, tt.groupName, aws.ToString(out.CacheSecurityGroup.CacheSecurityGroupName))
		})
	}
}

// ----------------------------------------
// CreateGlobalReplicationGroup
// ----------------------------------------

func TestCreateGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		suffix      string
		description string
		primaryRGID string
		wantErr     bool
	}{
		{
			name:        "success",
			suffix:      "my-group",
			description: "Global RG",
			primaryRGID: "primary-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("primary-rg"),
					ReplicationGroupDescription: aws.String("Primary"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:        "success_without_primary",
			suffix:      "standalone-group",
			description: "Standalone global RG",
			primaryRGID: "nonexistent-primary",
		},
		{
			name:        "duplicate",
			suffix:      "dup-group",
			description: "First",
			primaryRGID: "",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix:    aws.String("dup-group"),
						GlobalReplicationGroupDescription: aws.String("first"),
						PrimaryReplicationGroupId:         aws.String("primary-rg"),
					},
				)
				require.NoError(t, err)
			},
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

			out, err := client.CreateGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.CreateGlobalReplicationGroupInput{
					GlobalReplicationGroupIdSuffix:    aws.String(tt.suffix),
					GlobalReplicationGroupDescription: aws.String(tt.description),
					PrimaryReplicationGroupId:         aws.String(tt.primaryRGID),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.GlobalReplicationGroup)
			assert.Contains(t, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId), tt.suffix)
			assert.Equal(t, tt.description, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupDescription))
		})
	}
}

// ----------------------------------------
// CreateServerlessCache
// ----------------------------------------

func TestCreateServerlessCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		cacheName   string
		description string
		engine      string
		wantErr     bool
	}{
		{
			name:        "success_redis",
			cacheName:   "my-serverless",
			description: "My serverless cache",
			engine:      "redis",
		},
		{
			name:      "success_valkey",
			cacheName: "my-valkey",
			engine:    "valkey",
		},
		{
			name:      "already_exists",
			cacheName: "dup-serverless",
			engine:    "redis",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("dup-serverless"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
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

			out, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
				ServerlessCacheName: aws.String(tt.cacheName),
				Description:         aws.String(tt.description),
				Engine:              aws.String(tt.engine),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ServerlessCache)
			assert.Equal(t, tt.cacheName, aws.ToString(out.ServerlessCache.ServerlessCacheName))
			assert.NotEmpty(t, aws.ToString(out.ServerlessCache.ARN))
		})
	}
}

// ----------------------------------------
// CreateServerlessCacheSnapshot
// ----------------------------------------

func TestCreateServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup               func(t *testing.T, client *elasticachesdk.Client)
		name                string
		snapshotName        string
		serverlessCacheName string
		wantErr             bool
	}{
		{
			name:                "success",
			snapshotName:        "my-snap",
			serverlessCacheName: "my-serverless",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("my-serverless"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:                "cache_not_found",
			snapshotName:        "snap-fail",
			serverlessCacheName: "nonexistent-cache",
			wantErr:             true,
		},
		{
			name:                "snapshot_already_exists",
			snapshotName:        "dup-snap",
			serverlessCacheName: "my-serverless2",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("my-serverless2"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("dup-snap"),
						ServerlessCacheName:         aws.String("my-serverless2"),
					},
				)
				require.NoError(t, err)
			},
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

			out, err := client.CreateServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.CreateServerlessCacheSnapshotInput{
					ServerlessCacheSnapshotName: aws.String(tt.snapshotName),
					ServerlessCacheName:         aws.String(tt.serverlessCacheName),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ServerlessCacheSnapshot)
			assert.Equal(t, tt.snapshotName, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
			assert.NotEmpty(t, aws.ToString(out.ServerlessCacheSnapshot.ARN))
		})
	}
}

// ----------------------------------------
// CopyServerlessCacheSnapshot
// ----------------------------------------

func TestCopyServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, client *elasticachesdk.Client)
		name           string
		sourceSnapshot string
		targetSnapshot string
		wantErr        bool
	}{
		{
			name:           "success",
			sourceSnapshot: "source-snap",
			targetSnapshot: "target-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("src-cache"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("source-snap"),
						ServerlessCacheName:         aws.String("src-cache"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:           "source_not_found",
			sourceSnapshot: "nonexistent-snap",
			targetSnapshot: "target",
			wantErr:        true,
		},
		{
			name:           "target_already_exists",
			sourceSnapshot: "src-snap",
			targetSnapshot: "existing-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("cache-for-copy"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("src-snap"),
						ServerlessCacheName:         aws.String("cache-for-copy"),
					},
				)
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("existing-snap"),
						ServerlessCacheName:         aws.String("cache-for-copy"),
					},
				)
				require.NoError(t, err)
			},
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

			out, err := client.CopyServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.CopyServerlessCacheSnapshotInput{
					SourceServerlessCacheSnapshotName: aws.String(tt.sourceSnapshot),
					TargetServerlessCacheSnapshotName: aws.String(tt.targetSnapshot),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ServerlessCacheSnapshot)
			assert.Equal(t, tt.targetSnapshot, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
		})
	}
}

// ----------------------------------------
// CreateUser
// ----------------------------------------

func TestCreateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		userID             string
		userName           string
		accessString       string
		engine             string
		noPasswordRequired bool
		wantErr            bool
	}{
		{
			name:         "success",
			userID:       "user1",
			userName:     "alice",
			accessString: "on ~* +@all",
			engine:       "redis",
		},
		{
			name:               "success_no_password",
			userID:             "user2",
			userName:           "bob",
			accessString:       "on ~* +@read",
			engine:             "redis",
			noPasswordRequired: true,
		},
		{
			name:   "already_exists",
			userID: "dup-user",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("dup-user"),
					UserName:           aws.String("charlie"),
					AccessString:       aws.String("on ~* +@all"),
					Engine:             aws.String("redis"),
					NoPasswordRequired: aws.Bool(false),
				})
				require.NoError(t, err)
			},
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

			out, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
				UserId:             aws.String(tt.userID),
				UserName:           aws.String(tt.userName),
				AccessString:       aws.String(tt.accessString),
				Engine:             aws.String(tt.engine),
				NoPasswordRequired: aws.Bool(tt.noPasswordRequired),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userID, aws.ToString(out.UserId))
			assert.Equal(t, tt.userName, aws.ToString(out.UserName))
			assert.NotEmpty(t, aws.ToString(out.ARN))
		})
	}
}

// ----------------------------------------
// BatchApplyUpdateAction
// ----------------------------------------

func TestBatchApplyUpdateAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                  func(t *testing.T, client *elasticachesdk.Client)
		name                   string
		serviceUpdateName      string
		replicationGroupIDs    []string
		cacheClusterIDs        []string
		wantProcessedRGCount   int
		wantUnprocessedRGCount int
		wantProcessedCCCount   int
		wantUnprocessedCCCount int
	}{
		{
			name:                "apply_to_existing_rg",
			serviceUpdateName:   "update-2024-01",
			replicationGroupIDs: []string{"my-rg"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("RG for update"),
				})
				require.NoError(t, err)
			},
			wantProcessedRGCount:   1,
			wantUnprocessedRGCount: 0,
		},
		{
			name:                   "apply_to_nonexistent_rg",
			serviceUpdateName:      "update-2024-01",
			replicationGroupIDs:    []string{"nonexistent-rg"},
			wantProcessedRGCount:   0,
			wantUnprocessedRGCount: 1,
		},
		{
			name:              "apply_to_existing_cluster",
			serviceUpdateName: "update-2024-02",
			cacheClusterIDs:   []string{"my-cluster"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("my-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantProcessedCCCount:   1,
			wantUnprocessedCCCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
				ServiceUpdateName:   aws.String(tt.serviceUpdateName),
				ReplicationGroupIds: tt.replicationGroupIDs,
				CacheClusterIds:     tt.cacheClusterIDs,
			})

			require.NoError(t, err)
			assert.Len(t, out.ProcessedUpdateActions, tt.wantProcessedRGCount+tt.wantProcessedCCCount)
			assert.Len(t, out.UnprocessedUpdateActions, tt.wantUnprocessedRGCount+tt.wantUnprocessedCCCount)
		})
	}
}

// ----------------------------------------
// BatchStopUpdateAction
// ----------------------------------------

func TestBatchStopUpdateAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(t *testing.T, client *elasticachesdk.Client)
		name                 string
		serviceUpdateName    string
		replicationGroupIDs  []string
		cacheClusterIDs      []string
		wantProcessedCount   int
		wantUnprocessedCount int
	}{
		{
			name:                "stop_existing_rg",
			serviceUpdateName:   "update-2024-01",
			replicationGroupIDs: []string{"rg-for-stop"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-for-stop"),
					ReplicationGroupDescription: aws.String("RG for stop"),
				})
				require.NoError(t, err)
			},
			wantProcessedCount:   1,
			wantUnprocessedCount: 0,
		},
		{
			name:                 "stop_nonexistent_rg",
			serviceUpdateName:    "update-2024-01",
			replicationGroupIDs:  []string{"nonexistent-rg"},
			wantProcessedCount:   0,
			wantUnprocessedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.BatchStopUpdateAction(t.Context(), &elasticachesdk.BatchStopUpdateActionInput{
				ServiceUpdateName:   aws.String(tt.serviceUpdateName),
				ReplicationGroupIds: tt.replicationGroupIDs,
				CacheClusterIds:     tt.cacheClusterIDs,
			})

			require.NoError(t, err)
			assert.Len(t, out.ProcessedUpdateActions, tt.wantProcessedCount)
			assert.Len(t, out.UnprocessedUpdateActions, tt.wantUnprocessedCount)
		})
	}
}

// ----------------------------------------
// CompleteMigration
// ----------------------------------------

func TestCompleteMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		replicationGroupID string
		wantStatus         string
		wantErr            bool
	}{
		{
			name:               "success",
			replicationGroupID: "migration-rg",
			wantStatus:         "available",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("migration-rg"),
					ReplicationGroupDescription: aws.String("RG for migration"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:               "not_found",
			replicationGroupID: "nonexistent-rg",
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

			out, err := client.CompleteMigration(t.Context(), &elasticachesdk.CompleteMigrationInput{
				ReplicationGroupId: aws.String(tt.replicationGroupID),
				Force:              aws.Bool(false),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.wantStatus, aws.ToString(out.ReplicationGroup.Status))
		})
	}
}
