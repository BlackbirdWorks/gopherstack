package emr //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testReleaseLabel6 is a stable EMR 6.x release label used across isolation tests.
const testReleaseLabel6 = "emr-6.0.0"

func emrCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestEMRClusterRegionIsolation proves that EMR clusters with the same name
// created in two different regions are fully isolated: each region sees only
// its own cluster, ARNs embed the correct region, and terminating in one
// region leaves the other untouched.
func TestEMRClusterRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := emrCtxRegion("us-east-1")
	ctxWest := emrCtxRegion("us-west-2")

	// Create a cluster with the SAME name in both regions.
	eastCluster, err := backend.RunJobFlow(ctxEast, RunJobFlowParams{
		Name:         "shared-cluster",
		ReleaseLabel: testReleaseLabel6,
	})
	require.NoError(t, err)
	assert.Contains(t, eastCluster.ARN, "us-east-1")

	westCluster, err := backend.RunJobFlow(ctxWest, RunJobFlowParams{
		Name:         "shared-cluster",
		ReleaseLabel: "emr-7.0.0",
	})
	require.NoError(t, err)
	assert.Contains(t, westCluster.ARN, "us-west-2")

	// IDs and ARNs differ (region-qualified) even though names match.
	assert.NotEqual(t, eastCluster.ID, westCluster.ID)
	assert.NotEqual(t, eastCluster.ARN, westCluster.ARN)

	// Each region reads back only its own cluster, with its own release label.
	eastDesc, err := backend.DescribeCluster(ctxEast, eastCluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "emr-6.0.0", eastDesc.ReleaseLabel)

	westDesc, err := backend.DescribeCluster(ctxWest, westCluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "emr-7.0.0", westDesc.ReleaseLabel)

	// The west cluster ID is not resolvable from the east region.
	_, err = backend.DescribeCluster(ctxEast, westCluster.ID)
	require.Error(t, err)

	// ListClusters returns exactly one cluster per region.
	eastList, _ := backend.ListClusters(ctxEast, ListClustersParams{})
	require.Len(t, eastList, 1)
	assert.Equal(t, eastCluster.ID, eastList[0].ID)

	westList, _ := backend.ListClusters(ctxWest, ListClustersParams{})
	require.Len(t, westList, 1)
	assert.Equal(t, westCluster.ID, westList[0].ID)

	// Terminating the east cluster must not affect the west cluster.
	require.NoError(t, backend.TerminateJobFlows(ctxEast, []string{eastCluster.ID}))

	eastAfter, err := backend.DescribeCluster(ctxEast, eastCluster.ID)
	require.NoError(t, err)
	assert.Equal(t, StateTerminated, eastAfter.Status.State)

	westAfter, err := backend.DescribeCluster(ctxWest, westCluster.ID)
	require.NoError(t, err)
	assert.NotEqual(t, StateTerminated, westAfter.Status.State)
}

// TestEMRResourceRegionIsolation proves that EMR studios, security
// configurations, and tags (resolved by ARN) are scoped to the request region.
func TestEMRResourceRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := emrCtxRegion("us-east-1")
	ctxWest := emrCtxRegion("us-west-2")

	// Same-named studio in both regions.
	eastStudio, err := backend.CreateStudio(
		ctxEast, "shared-studio", "", "IAM", "s3://east", "sg-east", "role-east", "vpc-east", "sg-w-east", nil, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastStudio.StudioArn, "us-east-1")

	westStudio, err := backend.CreateStudio(
		ctxWest, "shared-studio", "", "IAM", "s3://west", "sg-west", "role-west", "vpc-west", "sg-w-west", nil, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westStudio.StudioArn, "us-west-2")

	// Each region sees exactly one studio.
	eastStudios, _ := backend.ListStudios(ctxEast, "")
	require.Len(t, eastStudios, 1)
	assert.Equal(t, "s3://east", eastStudios[0].DefaultS3Location)

	westStudios, _ := backend.ListStudios(ctxWest, "")
	require.Len(t, westStudios, 1)
	assert.Equal(t, "s3://west", westStudios[0].DefaultS3Location)

	// Same-named security configuration in both regions, isolated.
	_, err = backend.CreateSecurityConfiguration(ctxEast, "shared-sc", `{"k":"east"}`)
	require.NoError(t, err)
	_, err = backend.CreateSecurityConfiguration(ctxWest, "shared-sc", `{"k":"west"}`)
	require.NoError(t, err)

	eastSC, err := backend.DescribeSecurityConfiguration(ctxEast, "shared-sc")
	require.NoError(t, err)
	assert.JSONEq(t, `{"k":"east"}`, eastSC.SecurityConfig)

	westSC, err := backend.DescribeSecurityConfiguration(ctxWest, "shared-sc")
	require.NoError(t, err)
	assert.JSONEq(t, `{"k":"west"}`, westSC.SecurityConfig)

	// Tags addressed by a bare cluster ID are scoped to the request region.
	eastCluster, err := backend.RunJobFlow(ctxEast, RunJobFlowParams{Name: "tag-c", ReleaseLabel: testReleaseLabel6})
	require.NoError(t, err)

	require.NoError(t, backend.AddTags(ctxEast, eastCluster.ID, []Tag{{Key: "env", Value: "prod"}}))

	eastTags, err := backend.ListTagsForResource(ctxEast, eastCluster.ID)
	require.NoError(t, err)
	require.Len(t, eastTags, 1)
	assert.Equal(t, "prod", eastTags[0].Value)

	// The east cluster ID must not resolve in the west region (bare IDs fall
	// back to the ctx region rather than embedding one like an ARN).
	_, err = backend.ListTagsForResource(ctxWest, eastCluster.ID)
	require.Error(t, err, "east cluster ID must not be tag-resolvable from the west region")

	// Addressing the same cluster by its (region-qualified) ARN resolves
	// regardless of the ctx region, because the ARN carries its own region.
	arnTags, err := backend.ListTagsForResource(ctxWest, eastCluster.ARN)
	require.NoError(t, err, "ARN carries its region and must resolve from any ctx")
	require.Len(t, arnTags, 1)
	assert.Equal(t, "prod", arnTags[0].Value)

	// Deleting the east studio leaves the west studio intact.
	require.NoError(t, backend.DeleteStudio(ctxEast, eastStudio.StudioID))
	require.NoError(t, backend.DeleteStudio(ctxWest, westStudio.StudioID)) // west still exists
}

// TestEMRDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region (single-region behavior is
// unchanged).
func TestEMRDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	cluster, err := backend.RunJobFlow(context.Background(), RunJobFlowParams{
		Name:         "def-cluster",
		ReleaseLabel: testReleaseLabel6,
	})
	require.NoError(t, err)
	assert.Contains(t, cluster.ARN, "eu-central-1")

	// Reading via the explicit default region sees it.
	list, _ := backend.ListClusters(emrCtxRegion("eu-central-1"), ListClustersParams{})
	require.Len(t, list, 1)

	// A different region sees nothing.
	other, _ := backend.ListClusters(emrCtxRegion("ap-south-1"), ListClustersParams{})
	assert.Empty(t, other)
}
