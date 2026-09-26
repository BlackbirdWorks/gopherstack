package dsql_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input dsqlsdk.CreateClusterInput
		name  string
	}{
		{name: "minimal", input: dsqlsdk.CreateClusterInput{}},
		{
			name: "with deletion protection and tags",
			input: dsqlsdk.CreateClusterInput{
				DeletionProtectionEnabled: aws.Bool(true),
				Tags:                      map[string]string{"env": "test"},
			},
		},
		{
			name: "with multi-region properties",
			input: dsqlsdk.CreateClusterInput{
				MultiRegionProperties: &types.MultiRegionProperties{
					WitnessRegion: aws.String("us-west-2"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())

			out, err := client.CreateCluster(t.Context(), &tt.input)
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.Arn), "arn:aws:dsql:"+testRegion+":"+testAccountID+":cluster/")
			assert.Equal(t, types.ClusterStatusCreating, out.Status)
			assert.Contains(t, aws.ToString(out.Endpoint), ".dsql."+testRegion+".on.aws")
			assert.NotEmpty(t, aws.ToString(out.Identifier))
		})
	}
}

func TestCreateCluster_WitnessRegionEqualsClusterRegion(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
		MultiRegionProperties: &types.MultiRegionProperties{WitnessRegion: aws.String(testRegion)},
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ValidationException")
}

func TestCreateCluster_QuotaExceeded(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	const quota = 20

	for range quota {
		_, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
		require.NoError(t, err)
	}

	_, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ServiceQuotaExceededException")
}

func TestGetCluster(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	out, err := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Identifier), aws.ToString(out.Identifier))
	assert.Equal(t, aws.ToString(created.Arn), aws.ToString(out.Arn))
	assert.Equal(t, map[string]string{"env": "test"}, out.Tags)
	assert.NotZero(t, aws.ToTime(out.CreationTime))
	require.NotNil(t, out.EncryptionDetails)
	assert.Equal(t, types.EncryptionTypeAwsOwnedKmsKey, out.EncryptionDetails.EncryptionType)
}

func TestGetCluster_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: aws.String("does-not-exist")})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestListClusters(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	const n = 3

	for range n {
		_, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
		require.NoError(t, err)
	}

	out, err := client.ListClusters(ctx, &dsqlsdk.ListClustersInput{})
	require.NoError(t, err)
	assert.Len(t, out.Clusters, n)
}

func TestUpdateCluster(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	out, err := client.UpdateCluster(ctx, &dsqlsdk.UpdateClusterInput{
		Identifier:                created.Identifier,
		DeletionProtectionEnabled: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.Identifier), aws.ToString(out.Identifier))
	assert.Equal(t, types.ClusterStatusUpdating, out.Status)

	got, err := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(got.DeletionProtectionEnabled))
}

func TestUpdateCluster_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.UpdateCluster(t.Context(), &dsqlsdk.UpdateClusterInput{Identifier: aws.String("nope")})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestDeleteCluster(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	out, err := client.DeleteCluster(ctx, &dsqlsdk.DeleteClusterInput{Identifier: created.Identifier})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterStatusDeleting, out.Status)

	require.Eventually(t, func() bool {
		_, getErr := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})

		var apiErr smithy.APIError

		return getErr != nil && errors.As(getErr, &apiErr) && apiErr.ErrorCode() == "ResourceNotFoundException"
	}, waitTimeout, pollInterval)
}

func TestDeleteCluster_DeletionProtectionBlocks(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{
		DeletionProtectionEnabled: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DeleteCluster(ctx, &dsqlsdk.DeleteClusterInput{Identifier: created.Identifier})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ValidationException")
}

func TestGetVpcEndpointServiceName(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	out, err := client.GetVpcEndpointServiceName(ctx, &dsqlsdk.GetVpcEndpointServiceNameInput{
		Identifier: created.Identifier,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.ServiceName))
	assert.NotEmpty(t, aws.ToString(out.ClusterVpcEndpoint))
}

func TestGetVpcEndpointServiceName_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.GetVpcEndpointServiceName(t.Context(), &dsqlsdk.GetVpcEndpointServiceNameInput{
		Identifier: aws.String("nope"),
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}
