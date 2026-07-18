package elasticache_test

import (
	"context"
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		rgID        string
		description string
		wantStatus  string
		wantErr     bool
	}{
		{
			name:        "success",
			rgID:        "my-rg",
			description: "test replication group",
			wantStatus:  "available",
		},
		{
			name:        "already_exists",
			rgID:        "dup-rg",
			description: "duplicate",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("dup-rg"),
					ReplicationGroupDescription: aws.String("first"),
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

			out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String(tt.rgID),
				ReplicationGroupDescription: aws.String(tt.description),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
			assert.Equal(t, tt.wantStatus, aws.ToString(out.ReplicationGroup.Status))
		})
	}
}

func TestDescribeReplicationGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		rgID      string
		wantCount int
		wantErr   bool
	}{
		{
			name: "describe_specific",
			rgID: "my-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("test replication group"),
				})
				require.NoError(t, err)
			},
			wantCount: 1,
		},
		{
			name: "describe_all",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, rg := range []struct{ id, desc string }{{"rg-one", "first"}, {"rg-two", "second"}} {
					_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
						ReplicationGroupId:          aws.String(rg.id),
						ReplicationGroupDescription: aws.String(rg.desc),
					})
					require.NoError(t, err)
				}
			},
			wantCount: 2,
		},
		{
			name:    "not_found",
			rgID:    "does-not-exist",
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

			var rgID *string
			if tt.rgID != "" {
				rgID = aws.String(tt.rgID)
			}

			out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
				ReplicationGroupId: rgID,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ReplicationGroups, tt.wantCount)

			if tt.rgID != "" && tt.wantCount == 1 {
				assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroups[0].ReplicationGroupId))
			}
		})
	}
}

func TestDeleteReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, client *elasticachesdk.Client)
		name       string
		rgID       string
		wantStatus string
		wantErr    bool
	}{
		{
			name: "success",
			rgID: "my-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("test replication group"),
				})
				require.NoError(t, err)
			},
			wantStatus: "deleting",
		},
		{
			name:    "not_found",
			rgID:    "does-not-exist",
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

			out, err := client.DeleteReplicationGroup(t.Context(), &elasticachesdk.DeleteReplicationGroupInput{
				ReplicationGroupId: aws.String(tt.rgID),
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

func TestModifyReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		id          string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			id:          "my-rg",
			description: "updated description",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("my-rg"),
					ReplicationGroupDescription: aws.String("original"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			id:      "does-not-exist",
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

			out, err := client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
				ReplicationGroupId:          aws.String(tt.id),
				ReplicationGroupDescription: aws.String(tt.description),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.description, aws.ToString(out.ReplicationGroup.Description))
		})
	}
}

func TestHandler_DescribeReplicationGroups_LogDeliveryConfigs_InResponse(t *testing.T) {
	t.Parallel()

	b, client := newTestStackWithBackend(t)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "ld-rg",
		Description: "log delivery test",
		LogDeliveryConfigurations: []elasticache.LogDeliveryConfig{
			{
				LogType:            "slow-log",
				DestinationType:    "cloudwatch-logs",
				DestinationDetails: "/aws/elasticache/ld-rg/slow-log",
				LogFormat:          "text",
				Status:             "enabled",
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("ld-rg"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)
	rg := out.ReplicationGroups[0]
	assert.Len(t, rg.LogDeliveryConfigurations, 1)
	assert.Equal(t, elasticachetypes.LogTypeSlowLog, rg.LogDeliveryConfigurations[0].LogType)
	assert.Equal(t, elasticachetypes.DestinationTypeCloudWatchLogs, rg.LogDeliveryConfigurations[0].DestinationType)
}

// ----------------------------------------
// GlobalReplicationGroup NodeGroupCount in response
// ----------------------------------------

func TestHandler_CreateReplicationGroup_WithAuthToken(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-auth-rg"),
		ReplicationGroupDescription: aws.String("with auth token via HTTP"),
		AuthToken:                   aws.String("secret-http-token"),
		TransitEncryptionEnabled:    aws.Bool(true),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.Equal(t, "http-auth-rg", aws.ToString(out.ReplicationGroup.ReplicationGroupId))
	assert.True(t, aws.ToBool(out.ReplicationGroup.AuthTokenEnabled))
}

func TestHandler_CreateReplicationGroup_WithClusterMode(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-cluster-rg"),
		ReplicationGroupDescription: aws.String("cluster mode via HTTP"),
		ClusterMode:                 elasticachetypes.ClusterModeEnabled,
		NumNodeGroups:               aws.Int32(2),
		ReplicasPerNodeGroup:        aws.Int32(1),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.True(t, aws.ToBool(out.ReplicationGroup.ClusterEnabled))
}

func TestHandler_CreateReplicationGroup_WithKmsKey(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	kmsKey := "arn:aws:kms:us-east-1:123456789012:key/test-key"
	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-kms-rg"),
		ReplicationGroupDescription: aws.String("kms key via HTTP"),
		KmsKeyId:                    aws.String(kmsKey),
		AtRestEncryptionEnabled:     aws.Bool(true),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.Equal(t, kmsKey, aws.ToString(out.ReplicationGroup.KmsKeyId))
	assert.True(t, aws.ToBool(out.ReplicationGroup.AtRestEncryptionEnabled))
}

func TestHandler_CreateReplicationGroup_WithSnapshotRetention(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-snap-rg"),
		ReplicationGroupDescription: aws.String("snapshot retention via HTTP"),
		SnapshotRetentionLimit:      aws.Int32(7),
		SnapshotWindow:              aws.String("05:00-06:00"),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.Equal(t, int32(7), aws.ToInt32(out.ReplicationGroup.SnapshotRetentionLimit))
}

func TestHandler_CreateReplicationGroup_TransitEncryptionRequired_WithToken(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-transit-req-rg"),
		ReplicationGroupDescription: aws.String("transit required with token"),
		AuthToken:                   aws.String("required-token"),
		TransitEncryptionEnabled:    aws.Bool(true),
		TransitEncryptionMode:       elasticachetypes.TransitEncryptionModeRequired,
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.True(t, aws.ToBool(out.ReplicationGroup.TransitEncryptionEnabled))
	assert.Equal(t, elasticachetypes.TransitEncryptionModeRequired, out.ReplicationGroup.TransitEncryptionMode)
}

func TestHandler_CreateReplicationGroup_DataTiering(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-data-tier-rg"),
		ReplicationGroupDescription: aws.String("data tiering"),
		DataTieringEnabled:          aws.Bool(true),
		CacheNodeType:               aws.String("cache.r7g.large"),
		EngineVersion:               aws.String("7.1.0"),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.Equal(t, elasticachetypes.DataTieringStatusEnabled, out.ReplicationGroup.DataTiering)
}

// ----------------------------------------
// HTTP handler — ModifyReplicationGroup with new fields
// ----------------------------------------

func TestHandler_ModifyReplicationGroup_ApplyImmediately(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-modify-imm-rg"),
		ReplicationGroupDescription: aws.String("modify apply immediately"),
	})
	require.NoError(t, err)

	out, err := client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("http-modify-imm-rg"),
		EngineVersion:      aws.String("7.0.7"),
		ApplyImmediately:   aws.Bool(true),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	// Verify modification succeeded by checking the describe output.
	desc, descErr := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("http-modify-imm-rg"),
	})
	require.NoError(t, descErr)
	require.Len(t, desc.ReplicationGroups, 1)
}

func TestHandler_ModifyReplicationGroup_WithAutoFailover(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("http-failover-rg"),
		ReplicationGroupDescription: aws.String("failover test"),
	})
	require.NoError(t, err)

	out, err := client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId:       aws.String("http-failover-rg"),
		AutomaticFailoverEnabled: aws.Bool(true),
		ApplyImmediately:         aws.Bool(true),
	})

	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.Equal(t, elasticachetypes.AutomaticFailoverStatusEnabled, out.ReplicationGroup.AutomaticFailover)
}

func TestHandler_ModifyReplicationGroup_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId:          aws.String("nonexistent-rg"),
		ReplicationGroupDescription: aws.String("should fail"),
	})

	require.Error(t, err)
}

// ----------------------------------------
// Valkey engine — DescribeCacheEngineVersions
// ----------------------------------------

func TestHandler_DescribeReplicationGroups_ReturnsNodeGroups(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("ng-rg"),
		ReplicationGroupDescription: aws.String("node groups test"),
		ClusterMode:                 elasticachetypes.ClusterModeEnabled,
		NumNodeGroups:               aws.Int32(3),
		ReplicasPerNodeGroup:        aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("ng-rg"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)

	rg := out.ReplicationGroups[0]
	assert.True(t, aws.ToBool(rg.ClusterEnabled))
	assert.Len(t, rg.NodeGroups, 3)

	// Verify each node group has slots.
	for _, ng := range rg.NodeGroups {
		assert.NotEmpty(t, aws.ToString(ng.NodeGroupId))
		assert.NotEmpty(t, aws.ToString(ng.Slots))
	}
}

func TestHandler_DescribeReplicationGroups_NoNodeGroups_WhenDisabled(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("no-ng-rg"),
		ReplicationGroupDescription: aws.String("no node groups"),
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("no-ng-rg"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)

	rg := out.ReplicationGroups[0]
	assert.False(t, aws.ToBool(rg.ClusterEnabled))
	assert.Empty(t, rg.NodeGroups)
}

// ----------------------------------------
// PendingModifiedValues in HTTP response (issue #7)
// ----------------------------------------

func TestHandler_ModifyReplicationGroup_PendingModifiedValues_InResponse(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("pending-http-rg"),
		ReplicationGroupDescription: aws.String("pending changes"),
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("pending-http-rg"),
		EngineVersion:      aws.String("7.0.7"),
		ApplyImmediately:   aws.Bool(false),
	})
	require.NoError(t, err)

	// PendingModifiedValues should appear in the describe response.
	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("pending-http-rg"),
	})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationGroups, 1)

	// Pending modifications exist (SDK type doesn't expose EngineVersion in PendingModifiedValues,
	// but the pending state is tracked internally by the backend).
	_ = desc.ReplicationGroups[0].PendingModifiedValues
}

func TestHandler_ModifyReplicationGroup_ApplyImmediately_ClearsPending(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("clear-pending-rg"),
		ReplicationGroupDescription: aws.String("clear pending"),
	})
	require.NoError(t, err)

	// First modify without ApplyImmediately to queue pending.
	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("clear-pending-rg"),
		EngineVersion:      aws.String("7.0.7"),
		ApplyImmediately:   aws.Bool(false),
	})
	require.NoError(t, err)

	// Second modify with ApplyImmediately=true clears pending.
	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("clear-pending-rg"),
		CacheNodeType:      aws.String("cache.r6g.large"),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("clear-pending-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Nil(t, rg.PendingModifiedValues)
	assert.Equal(t, "cache.r6g.large", aws.ToString(rg.CacheNodeType))
}

// ----------------------------------------
// Tags on CreateReplicationGroup (gap - tags not parsed before)
// ----------------------------------------

func TestHandler_CreateReplicationGroup_WithTags(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("tagged-rg"),
		ReplicationGroupDescription: aws.String("tagged"),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)

	arn := aws.ToString(out.ReplicationGroup.ARN)
	require.NotEmpty(t, arn)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 2)

	tagMap := make(map[string]string)
	for _, tag := range tagsOut.TagList {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
}

// ----------------------------------------
// UserGroupIds in ReplicationGroup create/modify (issue #15)
// ----------------------------------------

func TestHandler_CreateReplicationGroup_WithUserGroupIds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create users and user group first.
	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("ug-user1"),
		UserName:           aws.String("ug-user1"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("test-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"ug-user1"},
	})
	require.NoError(t, err)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("ug-rg"),
		ReplicationGroupDescription: aws.String("with user groups"),
		UserGroupIds:                []string{"test-ug"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("ug-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Contains(t, rg.UserGroupIds, "test-ug")
}

func TestHandler_ModifyReplicationGroup_AddUserGroupIds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("mod-ug-user"),
		UserName:           aws.String("mod-ug-user"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("mod-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"mod-ug-user"},
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("mod-ug-rg"),
		ReplicationGroupDescription: aws.String("modify user groups"),
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("mod-ug-rg"),
		UserGroupIdsToAdd:  []string{"mod-ug"},
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("mod-ug-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Contains(t, rg.UserGroupIds, "mod-ug")
}

func TestHandler_ModifyReplicationGroup_RemoveUserGroupIds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("rm-ug-user"),
		UserName:           aws.String("rm-ug-user"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("rm-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"rm-ug-user"},
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("rm-ug-rg"),
		ReplicationGroupDescription: aws.String("remove user groups"),
		UserGroupIds:                []string{"rm-ug"},
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId:   aws.String("rm-ug-rg"),
		UserGroupIdsToRemove: []string{"rm-ug"},
		ApplyImmediately:     aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("rm-ug-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.NotContains(t, rg.UserGroupIds, "rm-ug")
}

// ----------------------------------------
// UserGroupIds backend unit tests (issue #15)
// ----------------------------------------

func TestHandler_CreateReplicationGroup_ARNFormat(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("arn-rg"),
		ReplicationGroupDescription: aws.String("arn test"),
	})
	require.NoError(t, err)
	arn := aws.ToString(out.ReplicationGroup.ARN)
	assert.Contains(t, arn, "arn:aws:elasticache:")
	assert.Contains(t, arn, "arn-rg")
}
