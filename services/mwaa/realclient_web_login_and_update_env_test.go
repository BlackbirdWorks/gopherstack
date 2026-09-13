package mwaa_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mwaasdk "github.com/aws/aws-sdk-go-v2/service/mwaa"
	mwaatypes "github.com/aws/aws-sdk-go-v2/service/mwaa/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateWebLoginTokenAndUpdateEnvironment_RealClient covers mwaa's last
// two typed-client-uncovered ops (gopherstack-n3zi). Reuses
// newMWAAHostPrefixTestClient (host_prefix_reachability_test.go) with the
// redial fix: every mwaa op carries a client-side "api."/"env." host-prefix
// rewrite that a plain httptest client can't dial without it.
func TestCreateWebLoginTokenAndUpdateEnvironment_RealClient(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	client := newMWAAHostPrefixTestClient(t, true)

	_, err := client.CreateEnvironment(ctx, &mwaasdk.CreateEnvironmentInput{
		Name:                 aws.String("slice17-env"),
		DagS3Path:            aws.String("dags/"),
		ExecutionRoleArn:     aws.String("arn:aws:iam::123456789012:role/mwaa-role"),
		SourceBucketArn:      aws.String("arn:aws:s3:::slice17-bucket"),
		NetworkConfiguration: mwaaNetworkConfig(),
	})
	require.NoError(t, err)

	// GetEnvironment promotes the mock-only CREATING state to AVAILABLE on
	// the stored environment (environments.go's promoteTransientStatus), but
	// returns a snapshot taken before that promotion -- so the FIRST call
	// still reports CREATING and a second call is needed to observe
	// AVAILABLE, matching real AWS's eventual-consistency lifecycle.
	_, err = client.GetEnvironment(ctx, &mwaasdk.GetEnvironmentInput{Name: aws.String("slice17-env")})
	require.NoError(t, err)

	got, err := client.GetEnvironment(ctx, &mwaasdk.GetEnvironmentInput{Name: aws.String("slice17-env")})
	require.NoError(t, err)
	require.NotNil(t, got.Environment)
	assert.Equal(t, mwaatypes.EnvironmentStatusAvailable, got.Environment.Status)

	token, err := client.CreateWebLoginToken(ctx, &mwaasdk.CreateWebLoginTokenInput{
		Name: aws.String("slice17-env"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(token.WebToken))
	assert.NotEmpty(t, aws.ToString(token.WebServerHostname))

	updated, err := client.UpdateEnvironment(ctx, &mwaasdk.UpdateEnvironmentInput{
		Name:      aws.String("slice17-env"),
		DagS3Path: aws.String("dags-v2/"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(updated.Arn), "slice17-env")

	afterUpdate, err := client.GetEnvironment(ctx, &mwaasdk.GetEnvironmentInput{Name: aws.String("slice17-env")})
	require.NoError(t, err)
	require.NotNil(t, afterUpdate.Environment)
	assert.Equal(t, "dags-v2/", aws.ToString(afterUpdate.Environment.DagS3Path))
}
