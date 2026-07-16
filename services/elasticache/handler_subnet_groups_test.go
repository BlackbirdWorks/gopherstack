package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		sgName    string
		desc      string
		subnetIDs []string
		wantErr   bool
		wantCount int
	}{
		{
			name:      "create_success",
			sgName:    "my-sg",
			desc:      "test subnet group",
			subnetIDs: []string{"subnet-1", "subnet-2"},
		},
		{
			name:      "create_already_exists",
			sgName:    "dup-sg",
			desc:      "duplicate",
			subnetIDs: []string{"subnet-1"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String("dup-sg"),
					CacheSubnetGroupDescription: aws.String("first"),
					SubnetIds:                   []string{"subnet-1"},
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name: "describe_all",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, nm := range []string{"sg-one", "sg-two"} {
					_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
						CacheSubnetGroupName:        aws.String(nm),
						CacheSubnetGroupDescription: aws.String("desc"),
						SubnetIds:                   []string{"subnet-1"},
					})
					require.NoError(t, err)
				}
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			if tt.sgName != "" {
				out, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String(tt.sgName),
					CacheSubnetGroupDescription: aws.String(tt.desc),
					SubnetIds:                   tt.subnetIDs,
				})

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, out.CacheSubnetGroup)
				assert.Equal(t, tt.sgName, aws.ToString(out.CacheSubnetGroup.CacheSubnetGroupName))

				return
			}

			if tt.wantCount > 0 {
				out, err := client.DescribeCacheSubnetGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheSubnetGroupsInput{},
				)
				require.NoError(t, err)
				assert.Len(t, out.CacheSubnetGroups, tt.wantCount)
			}
		})
	}
}

func TestDeleteCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "my-sg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String("my-sg"),
					CacheSubnetGroupDescription: aws.String("test"),
					SubnetIds:                   []string{"subnet-1"},
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "does-not-exist",
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

			_, err := client.DeleteCacheSubnetGroup(t.Context(), &elasticachesdk.DeleteCacheSubnetGroupInput{
				CacheSubnetGroupName: aws.String(tt.sgName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestModifyCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "my-sg",
		},
		{
			name:    "not_found",
			sgName:  "does-not-exist",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if !tt.wantErr {
				_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
					CacheSubnetGroupName:        aws.String(tt.sgName),
					CacheSubnetGroupDescription: aws.String("original"),
					SubnetIds:                   []string{"subnet-1"},
				})
				require.NoError(t, err)
			}

			out, err := client.ModifyCacheSubnetGroup(t.Context(), &elasticachesdk.ModifyCacheSubnetGroupInput{
				CacheSubnetGroupName:        aws.String(tt.sgName),
				CacheSubnetGroupDescription: aws.String("updated"),
				SubnetIds:                   []string{"subnet-1", "subnet-2"},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.CacheSubnetGroup)
			assert.Equal(t, "updated", aws.ToString(out.CacheSubnetGroup.CacheSubnetGroupDescription))
			assert.Len(t, out.CacheSubnetGroup.Subnets, 2)
		})
	}
}

func TestHandler_CreateCacheSubnetGroup_Tags(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
		CacheSubnetGroupName:        aws.String("tagged-sng"),
		CacheSubnetGroupDescription: aws.String("tagged subnet group"),
		SubnetIds:                   []string{"subnet-abc123"},
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("purpose"), Value: aws.String("testing")},
		},
	})
	require.NoError(t, err)
	arn := aws.ToString(out.CacheSubnetGroup.ARN)
	require.NotEmpty(t, arn)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "purpose", aws.ToString(tagsOut.TagList[0].Key))
	assert.Equal(t, "testing", aws.ToString(tagsOut.TagList[0].Value))
}
