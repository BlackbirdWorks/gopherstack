//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	memorydbSDK "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMemoryDBClient returns a MemoryDB client pointed at the shared test container.
func createMemoryDBClient(t *testing.T) *memorydbSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return memorydbSDK.NewFromConfig(cfg, func(o *memorydbSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MemoryDB_ClusterLifecycle tests cluster creation, description, and deletion.
func TestIntegration_MemoryDB_ClusterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
	}{
		{
			name:        "full_lifecycle",
			clusterName: "int-test-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMemoryDBClient(t)

			uniqueName := tt.clusterName + "-" + t.Name()

			// CreateCluster.
			createOut, err := client.CreateCluster(ctx, &memorydbSDK.CreateClusterInput{
				ClusterName: aws.String(uniqueName),
				NodeType:    aws.String("db.r6g.large"),
				ACLName:     aws.String("open-access"),
			})
			require.NoError(t, err, "CreateCluster should succeed")
			require.NotNil(t, createOut.Cluster)
			assert.Equal(t, uniqueName, aws.ToString(createOut.Cluster.Name))
			assert.NotEmpty(t, aws.ToString(createOut.Cluster.ARN))
			assert.Equal(t, "available", aws.ToString(createOut.Cluster.Status))

			clusterARN := aws.ToString(createOut.Cluster.ARN)

			t.Cleanup(func() {
				_, _ = client.DeleteCluster(ctx, &memorydbSDK.DeleteClusterInput{
					ClusterName: aws.String(uniqueName),
				})
			})

			// DescribeClusters by name.
			descOut, err := client.DescribeClusters(ctx, &memorydbSDK.DescribeClustersInput{
				ClusterName: aws.String(uniqueName),
			})
			require.NoError(t, err, "DescribeClusters should succeed")
			require.Len(t, descOut.Clusters, 1)
			assert.Equal(t, uniqueName, aws.ToString(descOut.Clusters[0].Name))

			// DescribeClusters (list all) — created cluster should appear.
			listOut, err := client.DescribeClusters(ctx, &memorydbSDK.DescribeClustersInput{})
			require.NoError(t, err, "DescribeClusters (all) should succeed")

			found := false

			for _, c := range listOut.Clusters {
				if aws.ToString(c.ARN) == clusterARN {
					found = true

					break
				}
			}

			assert.True(t, found, "created cluster should appear in list")

			// DeleteCluster.
			delOut, err := client.DeleteCluster(ctx, &memorydbSDK.DeleteClusterInput{
				ClusterName: aws.String(uniqueName),
			})
			require.NoError(t, err, "DeleteCluster should succeed")
			assert.Equal(t, uniqueName, aws.ToString(delOut.Cluster.Name))

			// DescribeClusters should return not-found after delete.
			_, err = client.DescribeClusters(ctx, &memorydbSDK.DescribeClustersInput{
				ClusterName: aws.String(uniqueName),
			})
			require.Error(t, err, "DescribeClusters should fail after deletion")
		})
	}
}

// TestIntegration_MemoryDB_ACLLifecycle tests ACL CRUD operations.
func TestIntegration_MemoryDB_ACLLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{
			name:    "full_lifecycle",
			aclName: "int-test-acl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMemoryDBClient(t)

			uniqueName := tt.aclName + "-" + t.Name()

			// CreateACL.
			createOut, err := client.CreateACL(ctx, &memorydbSDK.CreateACLInput{
				ACLName: aws.String(uniqueName),
			})
			require.NoError(t, err, "CreateACL should succeed")
			require.NotNil(t, createOut.ACL)
			assert.Equal(t, uniqueName, aws.ToString(createOut.ACL.Name))

			t.Cleanup(func() {
				_, _ = client.DeleteACL(ctx, &memorydbSDK.DeleteACLInput{
					ACLName: aws.String(uniqueName),
				})
			})

			// DescribeACLs by name.
			descOut, err := client.DescribeACLs(ctx, &memorydbSDK.DescribeACLsInput{
				ACLName: aws.String(uniqueName),
			})
			require.NoError(t, err, "DescribeACLs should succeed")
			require.Len(t, descOut.ACLs, 1)

			// DeleteACL.
			_, err = client.DeleteACL(ctx, &memorydbSDK.DeleteACLInput{
				ACLName: aws.String(uniqueName),
			})
			require.NoError(t, err, "DeleteACL should succeed")
		})
	}
}

// TestIntegration_MemoryDB_Tags tests tag operations.
func TestIntegration_MemoryDB_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		tags        []memorydbSDK.Tag
	}{
		{
			name:        "tag_cluster",
			clusterName: "int-tag-cluster",
			tags: []memorydbSDK.Tag{
				{Key: aws.String("Env"), Value: aws.String("test")},
				{Key: aws.String("Team"), Value: aws.String("ops")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMemoryDBClient(t)

			uniqueName := tt.clusterName + "-" + t.Name()

			createOut, err := client.CreateCluster(ctx, &memorydbSDK.CreateClusterInput{
				ClusterName: aws.String(uniqueName),
				NodeType:    aws.String("db.r6g.large"),
				ACLName:     aws.String("open-access"),
			})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteCluster(ctx, &memorydbSDK.DeleteClusterInput{
					ClusterName: aws.String(uniqueName),
				})
			})

			clusterARN := aws.ToString(createOut.Cluster.ARN)

			// TagResource.
			_, err = client.TagResource(ctx, &memorydbSDK.TagResourceInput{
				ResourceArn: aws.String(clusterARN),
				Tags:        tt.tags,
			})
			require.NoError(t, err, "TagResource should succeed")

			// ListTags.
			tagsOut, err := client.ListTags(ctx, &memorydbSDK.ListTagsInput{
				ResourceArn: aws.String(clusterARN),
			})
			require.NoError(t, err, "ListTags should succeed")
			assert.NotEmpty(t, tagsOut.TagList)

			// UntagResource.
			_, err = client.UntagResource(ctx, &memorydbSDK.UntagResourceInput{
				ResourceArn: aws.String(clusterARN),
				TagKeys:     []string{"Team"},
			})
			require.NoError(t, err, "UntagResource should succeed")

			// Verify tag removed.
			tagsAfter, err := client.ListTags(ctx, &memorydbSDK.ListTagsInput{
				ResourceArn: aws.String(clusterARN),
			})
			require.NoError(t, err)

			for _, tg := range tagsAfter.TagList {
				assert.NotEqual(t, "Team", aws.ToString(tg.Key), "Team tag should have been removed")
			}
		})
	}
}

// TestIntegration_MemoryDB_OpenAccessACL verifies the pre-seeded open-access ACL exists.
func TestIntegration_MemoryDB_OpenAccessACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{
			name:    "open_access_preseeded",
			aclName: "open-access",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMemoryDBClient(t)

			out, err := client.DescribeACLs(ctx, &memorydbSDK.DescribeACLsInput{
				ACLName: aws.String(tt.aclName),
			})
			require.NoError(t, err, "DescribeACLs for open-access should succeed")
			require.Len(t, out.ACLs, 1)
			assert.Equal(t, tt.aclName, aws.ToString(out.ACLs[0].Name))
		})
	}
}
