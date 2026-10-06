package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	dsqltypes "github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createDSQLClient(t *testing.T, region string) *dsqlsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return dsqlsdk.NewFromConfig(cfg, func(o *dsqlsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func waitDSQLClusterStatus(t *testing.T, client *dsqlsdk.Client, id *string, want dsqltypes.ClusterStatus) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := client.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: id})

		return err == nil && out.Status == want
	}, 15*time.Second, 100*time.Millisecond)
}

// TestIntegration_DSQL_ClusterLifecycle covers create/get/list/update/tag/
// policy/delete on a single-Region cluster through the real SDK client.
func TestIntegration_DSQL_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createDSQLClient(t, "us-east-1")
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		DeletionProtectionEnabled: aws.Bool(true),
		Tags:                      map[string]string{"env": "it"},
	})
	require.NoError(t, err)
	assert.Equal(t, dsqltypes.ClusterStatusCreating, created.Status)
	waitDSQLClusterStatus(t, client, created.Identifier, dsqltypes.ClusterStatusActive)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.UpdateCluster(cleanupCtx, &dsqlsdk.UpdateClusterInput{
			Identifier:                created.Identifier,
			DeletionProtectionEnabled: aws.Bool(false),
		})
		_, _ = client.DeleteCluster(cleanupCtx, &dsqlsdk.DeleteClusterInput{Identifier: created.Identifier})
	})

	got, err := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Arn), aws.ToString(got.Arn))
	assert.True(t, aws.ToBool(got.DeletionProtectionEnabled))
	assert.Equal(t, map[string]string{"env": "it"}, got.Tags)

	_, err = client.DeleteCluster(ctx, &dsqlsdk.DeleteClusterInput{Identifier: created.Identifier})
	require.Error(t, err, "deletion protection must block delete")
	assert.True(t, hasAPIErrorCode(err, "ValidationException"))

	_, err = client.UpdateCluster(ctx, &dsqlsdk.UpdateClusterInput{
		Identifier:                created.Identifier,
		DeletionProtectionEnabled: aws.Bool(false),
	})
	require.NoError(t, err)
	waitDSQLClusterStatus(t, client, created.Identifier, dsqltypes.ClusterStatusActive)

	_, err = client.TagResource(ctx, &dsqlsdk.TagResourceInput{
		ResourceArn: created.Arn,
		Tags:        map[string]string{"team": "core"},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(ctx, &dsqlsdk.ListTagsForResourceInput{ResourceArn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "it", "team": "core"}, tags.Tags)

	put, err := client.PutClusterPolicy(ctx, &dsqlsdk.PutClusterPolicyInput{
		Identifier: created.Identifier,
		Policy:     aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(put.PolicyVersion))

	policy, err := client.GetClusterPolicy(ctx, &dsqlsdk.GetClusterPolicyInput{Identifier: created.Identifier})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(put.PolicyVersion), aws.ToString(policy.PolicyVersion))

	listed, err := client.ListClusters(ctx, &dsqlsdk.ListClustersInput{})
	require.NoError(t, err)

	var found bool

	for _, c := range listed.Clusters {
		found = found || aws.ToString(c.Identifier) == aws.ToString(created.Identifier)
	}

	assert.True(t, found, "cluster must appear in ListClusters")

	_, err = client.DeleteCluster(ctx, &dsqlsdk.DeleteClusterInput{Identifier: created.Identifier})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, getErr := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})

		return hasAPIErrorCode(getErr, "ResourceNotFoundException")
	}, 15*time.Second, 100*time.Millisecond)
}

// TestIntegration_DSQL_MultiRegionPeering peers two clusters through the
// witness Region handshake: PENDING_SETUP until both sides list each other.
func TestIntegration_DSQL_MultiRegionPeering(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	east := createDSQLClient(t, "us-east-1")
	west := createDSQLClient(t, "us-east-2")
	ctx := t.Context()
	witness := aws.String("us-west-2")

	a, err := east.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		MultiRegionProperties: &dsqltypes.MultiRegionProperties{WitnessRegion: witness},
	})
	require.NoError(t, err)

	b, err := west.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		MultiRegionProperties: &dsqltypes.MultiRegionProperties{
			WitnessRegion: witness,
			Clusters:      []string{aws.ToString(a.Arn)},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = east.DeleteCluster(cleanupCtx, &dsqlsdk.DeleteClusterInput{Identifier: a.Identifier})
		_, _ = west.DeleteCluster(cleanupCtx, &dsqlsdk.DeleteClusterInput{Identifier: b.Identifier})
	})

	waitDSQLClusterStatus(t, east, a.Identifier, dsqltypes.ClusterStatusPendingSetup)
	waitDSQLClusterStatus(t, west, b.Identifier, dsqltypes.ClusterStatusPendingSetup)

	_, err = east.UpdateCluster(ctx, &dsqlsdk.UpdateClusterInput{
		Identifier: a.Identifier,
		MultiRegionProperties: &dsqltypes.MultiRegionProperties{
			WitnessRegion: witness,
			Clusters:      []string{aws.ToString(b.Arn)},
		},
	})
	require.NoError(t, err)

	waitDSQLClusterStatus(t, east, a.Identifier, dsqltypes.ClusterStatusActive)
	waitDSQLClusterStatus(t, west, b.Identifier, dsqltypes.ClusterStatusActive)
}
