package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client) string
		name    string
		arn     string
		wantErr bool
	}{
		{
			name: "cluster_no_tags",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("tag-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)

				return aws.ToString(out.CacheCluster.ARN)
			},
		},
		{
			name: "replication_group_no_tags",
			setup: func(t *testing.T, client *elasticachesdk.Client) string {
				t.Helper()
				out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-tags"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ReplicationGroup.ARN)
			},
		},
		{
			name:    "not_found",
			arn:     "arn:aws:elasticache:us-east-1:000000000000:cluster:does-not-exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			resourceARN := tt.arn
			if tt.setup != nil {
				resourceARN = tt.setup(t, client)
			}

			out, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
				ResourceName: aws.String(resourceARN),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, out)
			assert.Empty(t, out.TagList)
		})
	}
}

func TestHandler_Tags_OnGlobalReplicationGroup(t *testing.T) {
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
