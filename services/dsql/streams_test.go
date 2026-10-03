package dsql_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dsql"
)

func testKinesisTarget() types.TargetDefinition {
	return &types.TargetDefinitionMemberKinesis{
		Value: types.KinesisTargetDefinition{
			RoleArn:   aws.String("arn:aws:iam::123456789012:role/dsql-stream-role"),
			StreamArn: aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/dsql-cdc"),
		},
	}
}

func TestCreateStream(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	out, err := client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
		ClusterIdentifier: cluster.Identifier,
		TargetDefinition:  testKinesisTarget(),
		Format:            types.StreamFormatJson,
		Ordering:          types.StreamOrderingUnordered,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(cluster.Identifier), aws.ToString(out.ClusterIdentifier))
	assert.Equal(t, types.StreamStatusCreating, out.Status)
	assert.NotEmpty(t, aws.ToString(out.StreamIdentifier))
}

func TestCreateStream_ClusterNotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.CreateStream(t.Context(), &dsqlsdk.CreateStreamInput{
		ClusterIdentifier: aws.String("nope"),
		TargetDefinition:  testKinesisTarget(),
		Format:            types.StreamFormatJson,
		Ordering:          types.StreamOrderingUnordered,
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestCreateStream_MissingTargetIsValidationError(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	_, err = client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{ClusterIdentifier: cluster.Identifier})
	require.Error(t, err)
}

func TestGetStream(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	created, err := client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
		ClusterIdentifier: cluster.Identifier,
		TargetDefinition:  testKinesisTarget(),
		Format:            types.StreamFormatJson,
		Ordering:          types.StreamOrderingUnordered,
		Tags:              map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	out, err := client.GetStream(ctx, &dsqlsdk.GetStreamInput{
		ClusterIdentifier: cluster.Identifier,
		StreamIdentifier:  created.StreamIdentifier,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.StreamIdentifier), aws.ToString(out.StreamIdentifier))
	assert.Equal(t, map[string]string{"env": "test"}, out.Tags)
	require.NotNil(t, out.TargetDefinition)
}

func TestGetStream_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	_, err = client.GetStream(ctx, &dsqlsdk.GetStreamInput{
		ClusterIdentifier: cluster.Identifier,
		StreamIdentifier:  aws.String("nope"),
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestListStreams(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	const n = 3

	for range n {
		_, streamErr := client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
			ClusterIdentifier: cluster.Identifier,
			TargetDefinition:  testKinesisTarget(),
			Format:            types.StreamFormatJson,
			Ordering:          types.StreamOrderingUnordered,
		})
		require.NoError(t, streamErr)
	}

	out, err := client.ListStreams(ctx, &dsqlsdk.ListStreamsInput{ClusterIdentifier: cluster.Identifier})
	require.NoError(t, err)
	assert.Len(t, out.Streams, n)
}

func TestDeleteStream(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	created, err := client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
		ClusterIdentifier: cluster.Identifier,
		TargetDefinition:  testKinesisTarget(),
		Format:            types.StreamFormatJson,
		Ordering:          types.StreamOrderingUnordered,
	})
	require.NoError(t, err)

	del, err := client.DeleteStream(ctx, &dsqlsdk.DeleteStreamInput{
		ClusterIdentifier: cluster.Identifier,
		StreamIdentifier:  created.StreamIdentifier,
	})
	require.NoError(t, err)
	assert.Equal(t, types.StreamStatusDeleting, del.Status)

	got, err := client.GetStream(ctx, &dsqlsdk.GetStreamInput{
		ClusterIdentifier: cluster.Identifier,
		StreamIdentifier:  created.StreamIdentifier,
	})
	require.NoError(t, err)
	assert.Equal(t, types.StreamStatusDeleting, got.Status)

	require.Eventually(t, func() bool {
		_, getErr := client.GetStream(ctx, &dsqlsdk.GetStreamInput{
			ClusterIdentifier: cluster.Identifier,
			StreamIdentifier:  created.StreamIdentifier,
		})

		return getErr != nil
	}, waitTimeout, pollInterval)

	_, err = client.GetStream(ctx, &dsqlsdk.GetStreamInput{
		ClusterIdentifier: cluster.Identifier,
		StreamIdentifier:  created.StreamIdentifier,
	})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	list, err := client.ListStreams(ctx, &dsqlsdk.ListStreamsInput{ClusterIdentifier: cluster.Identifier})
	require.NoError(t, err)
	assert.Empty(t, list.Streams)
}

func TestDeleteCluster_RemovesOwnedStreams(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := dsql.NewInMemoryBackend()

		c, err := b.CreateCluster(testAccountID, testRegion, dsql.CreateClusterInput{})
		require.NoError(t, err)

		s, err := b.CreateStream(c.Identifier, dsql.CreateStreamInput{
			Target: &dsql.StreamTarget{
				RoleArn:   "arn:aws:iam::123456789012:role/r",
				StreamArn: "arn:aws:kinesis:us-east-1:123456789012:stream/k",
			},
		})
		require.NoError(t, err)

		_, err = b.DeleteCluster(c.Identifier)
		require.NoError(t, err)

		time.Sleep(time.Second)

		_, err = b.GetCluster(c.Identifier)
		require.ErrorIs(t, err, dsql.ErrClusterNotFound)

		_, err = b.GetStream(c.Identifier, s.StreamIdentifier)
		require.ErrorIs(t, err, dsql.ErrStreamNotFound)
	})
}

func TestCreateStream_QuotaExceeded(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
	require.NoError(t, err)

	const quota = 20

	for range quota {
		_, streamErr := client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
			ClusterIdentifier: cluster.Identifier,
			TargetDefinition:  testKinesisTarget(),
			Format:            types.StreamFormatJson,
			Ordering:          types.StreamOrderingUnordered,
		})
		require.NoError(t, streamErr)
	}

	_, err = client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
		ClusterIdentifier: cluster.Identifier,
		TargetDefinition:  testKinesisTarget(),
		Format:            types.StreamFormatJson,
		Ordering:          types.StreamOrderingUnordered,
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ServiceQuotaExceededException")
}
