package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOpsWithTags_CreationTimeTagsRoundTrip drives six Create* ops whose
// real ElastiCache Input struct accepts Tags (CreateCacheCluster.go,
// CreateCacheSecurityGroup.go, CreateUser.go, CreateUserGroup.go,
// CreateSnapshot.go, CreateServerlessCacheSnapshot.go in the pinned
// aws-sdk-go-v2/service/elasticache) but whose handler never decoded the
// field at all, unlike CreateReplicationGroup/CreateServerlessCache/
// CreateCacheParameterGroup/CreateCacheSubnetGroup which already apply
// parseFormTags via AddTagsToResource (gopherstack-2mwl).
func TestCreateOpsWithTags_CreationTimeTagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *elasticachesdk.Client) string
		name  string
	}{
		{
			name: "cache cluster",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("tagged-cluster"),
					Engine:         aws.String("redis"),
					Tags:           []elasticachetypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.CacheCluster.ARN)
			},
		},
		{
			name: "cache security group",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("tagged-sg"),
					Description:            aws.String("test"),
					Tags:                   []elasticachetypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.CacheSecurityGroup.ARN)
			},
		},
		{
			name: "user",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:       aws.String("tagged-user"),
					UserName:     aws.String("tagged-user"),
					Engine:       aws.String("redis"),
					AccessString: aws.String("on ~* &* +@all"),
					Passwords:    []string{"averylongpassword1"},
					Tags:         []elasticachetypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ARN)
			},
		},
		{
			name: "user group",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("tagged-ug"),
					Engine:      aws.String("redis"),
					Tags:        []elasticachetypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ARN)
			},
		},
		{
			name: "snapshot",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("snap-source-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)

				out, err := client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					SnapshotName:   aws.String("tagged-snap"),
					CacheClusterId: aws.String("snap-source-cluster"),
					Tags:           []elasticachetypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.Snapshot.ARN)
			},
		},
		{
			name: "serverless cache snapshot",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("snap-source-serverless"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)

				out, err := client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("tagged-serverless-snap"),
						ServerlessCacheName:         aws.String("snap-source-serverless"),
						Tags: []elasticachetypes.Tag{
							{Key: aws.String("env"), Value: aws.String("prod")},
						},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.ServerlessCacheSnapshot.ARN)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			out, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
				ResourceName: aws.String(resourceARN),
			})
			require.NoError(t, err)

			require.Len(t, out.TagList, 1)
			assert.Equal(t, "env", aws.ToString(out.TagList[0].Key))
			assert.Equal(t, "prod", aws.ToString(out.TagList[0].Value))
		})
	}
}
