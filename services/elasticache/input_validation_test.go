package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/require"
)

func TestInputValidation_ErrorCodes(t *testing.T) {
	t.Parallel()

	cluster := func(id, engine, nodeType string, nodes *int32) *elasticachesdk.CreateCacheClusterInput {
		return &elasticachesdk.CreateCacheClusterInput{
			CacheClusterId: aws.String(id), Engine: aws.String(engine),
			CacheNodeType: aws.String(nodeType), NumCacheNodes: nodes,
		}
	}

	tests := []struct {
		call     func(t *testing.T, c *elasticachesdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "cluster id underscore", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateCacheCluster(t.Context(), cluster("bad_id", "redis", "cache.t3.micro", aws.Int32(1)))

				return err
			},
		},
		{
			name: "cluster id double hyphen", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateCacheCluster(t.Context(), cluster("bad--id", "redis", "cache.t3.micro", aws.Int32(1)))

				return err
			},
		},
		{
			name: "unknown engine", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateCacheCluster(t.Context(), cluster("c1", "nosuch", "cache.t3.micro", aws.Int32(1)))

				return err
			},
		},
		{
			name: "bad node type", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateCacheCluster(t.Context(), cluster("c2", "redis", "bogus", aws.Int32(1)))

				return err
			},
		},
		{
			name: "zero nodes", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateCacheCluster(t.Context(), cluster("c3", "memcached", "cache.t3.micro", aws.Int32(0)))

				return err
			},
		},
		{
			name: "short auth token", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId: aws.String("rg1"), ReplicationGroupDescription: aws.String("d"),
					AuthToken: aws.String("short"), TransitEncryptionEnabled: aws.Bool(true),
				})

				return err
			},
		},
		{
			name: "auth token without transit encryption", wantCode: "InvalidParameterCombination",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId: aws.String("rg2"), ReplicationGroupDescription: aws.String("d"),
					AuthToken: aws.String("a-valid-token-0123456789"),
				})

				return err
			},
		},
		{
			name: "bad marker", wantCode: "InvalidParameterValue",
			call: func(t *testing.T, c *elasticachesdk.Client) error {
				t.Helper()

				_, err := c.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
					Marker: aws.String("garbage"),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			err := tc.call(t, client)
			require.Error(t, err)
			requireErrCode(t, err, tc.wantCode)
		})
	}
}
