package cloudtrail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestDescribeQuery_AliasOptionalQueryID covers gopherstack-2wvq.
// DescribeQueryInput marks neither QueryId nor QueryAlias required
// (cloudtrail@v1.58.4 api_op_DescribeQuery.go:12-16): "You must specify
// either QueryId or QueryAlias. Specifying the QueryAlias parameter returns
// information about the last query run for the alias." The handler
// previously rejected any request without QueryId with a 400, even though
// StartQuery's own QueryAlias was already accepted (and silently dropped) on
// the wire.
func TestDescribeQuery_AliasOptionalQueryID(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	t.Run("alias alone resolves to the query started under it", func(t *testing.T) {
		t.Parallel()

		start, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
			QueryStatement: aws.String("SELECT 1 -- 2wvq-alias-single"),
			QueryAlias:     aws.String("2wvq-alias-single"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(start.QueryId))

		desc, err := client.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{
			QueryAlias: aws.String("2wvq-alias-single"),
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(start.QueryId), aws.ToString(desc.QueryId))
		assert.Equal(t, "SELECT 1 -- 2wvq-alias-single", aws.ToString(desc.QueryString))
	})

	t.Run("query id alone still works", func(t *testing.T) {
		t.Parallel()

		start, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
			QueryStatement: aws.String("SELECT 1 -- 2wvq-by-id"),
		})
		require.NoError(t, err)

		desc, err := client.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{
			QueryId: start.QueryId,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(start.QueryId), aws.ToString(desc.QueryId))
	})

	t.Run("alias reused by multiple queries resolves to the last one started", func(t *testing.T) {
		t.Parallel()

		first, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
			QueryStatement: aws.String("SELECT 1 -- 2wvq-alias-multi-first"),
			QueryAlias:     aws.String("2wvq-alias-multi"),
		})
		require.NoError(t, err)

		last, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
			QueryStatement: aws.String("SELECT 1 -- 2wvq-alias-multi-last"),
			QueryAlias:     aws.String("2wvq-alias-multi"),
		})
		require.NoError(t, err)
		require.NotEqual(t, aws.ToString(first.QueryId), aws.ToString(last.QueryId))

		desc, err := client.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{
			QueryAlias: aws.String("2wvq-alias-multi"),
		})
		require.NoError(t, err)
		assert.Equal(
			t,
			aws.ToString(last.QueryId),
			aws.ToString(desc.QueryId),
			"DescribeQuery by alias must return the last query run for the alias, not the first",
		)
		assert.Equal(t, "SELECT 1 -- 2wvq-alias-multi-last", aws.ToString(desc.QueryString))
	})

	t.Run("unknown alias errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{
			QueryAlias: aws.String("2wvq-no-such-alias"),
		})
		require.Error(t, err)
	})

	t.Run("neither query id nor alias errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{})
		require.Error(t, err)
	})
}
