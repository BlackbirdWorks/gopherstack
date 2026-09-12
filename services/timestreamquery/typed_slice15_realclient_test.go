package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice15RealClient drives timestreamquery's typed-coverage-blind
// ops (gopherstack-n3zi slice 15) through the real aws-sdk-go-v2 client.
func TestTypedSlice15RealClient(t *testing.T) {
	t.Parallel()

	t.Run("cancel and prepare query", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		queryOut, err := client.Query(ctx, &tqsdk.QueryInput{
			QueryString: aws.String("SELECT 1"),
		})
		require.NoError(t, err)
		queryID := aws.ToString(queryOut.QueryId)
		require.NotEmpty(t, queryID)

		_, err = client.CancelQuery(ctx, &tqsdk.CancelQueryInput{QueryId: aws.String(queryID)})
		require.NoError(t, err)

		_, err = client.CancelQuery(ctx, &tqsdk.CancelQueryInput{QueryId: aws.String("nonexistent-query-id")})
		require.Error(t, err)

		// gopherstack's PrepareQuery only infers "?" positional markers, not
		// real Timestream's "@identifier" named-parameter syntax (see this
		// backend's own inferColumnsFromSQL doc comment) -- a disclosed
		// simplification, not a wire-shape bug, so the query here uses "?"
		// to match what this implementation actually supports.
		prepOut, err := client.PrepareQuery(ctx, &tqsdk.PrepareQueryInput{
			QueryString: aws.String("SELECT device_id, measure_value::double FROM tbl WHERE device_id = ?"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, prepOut.Columns)
		require.NotEmpty(t, prepOut.Parameters)
		assert.Equal(t, "param1", aws.ToString(prepOut.Parameters[0].Name))
	})

	t.Run("account settings", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		_, err := client.UpdateAccountSettings(ctx, &tqsdk.UpdateAccountSettingsInput{
			MaxQueryTCU:       aws.Int32(8),
			QueryPricingModel: types.QueryPricingModelComputeUnits,
		})
		require.NoError(t, err)

		descOut, err := client.DescribeAccountSettings(ctx, &tqsdk.DescribeAccountSettingsInput{})
		require.NoError(t, err)
		assert.EqualValues(t, 8, aws.ToInt32(descOut.MaxQueryTCU))
		assert.Equal(t, types.QueryPricingModelComputeUnits, descOut.QueryPricingModel)
	})

	t.Run("scheduled query update and tags", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		created, err := client.CreateScheduledQuery(ctx, &tqsdk.CreateScheduledQueryInput{
			Name:                           aws.String("s15-scheduled-query"),
			QueryString:                    aws.String("SELECT 1"),
			ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq-role"),
			ScheduleConfiguration: &types.ScheduleConfiguration{
				ScheduleExpression: aws.String("rate(1 hour)"),
			},
			NotificationConfiguration: &types.NotificationConfiguration{
				SnsConfiguration: &types.SnsConfiguration{
					TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:tsq-topic"),
				},
			},
			ErrorReportConfiguration: &types.ErrorReportConfiguration{
				S3Configuration: &types.S3Configuration{
					BucketName: aws.String("s15-error-bucket"),
				},
			},
			Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
		})
		require.NoError(t, err)
		sqARN := aws.ToString(created.Arn)
		require.NotEmpty(t, sqARN)

		_, err = client.UpdateScheduledQuery(ctx, &tqsdk.UpdateScheduledQueryInput{
			ScheduledQueryArn: aws.String(sqARN),
			State:             types.ScheduledQueryStateDisabled,
		})
		require.NoError(t, err)

		descOut, err := client.DescribeScheduledQuery(ctx, &tqsdk.DescribeScheduledQueryInput{
			ScheduledQueryArn: aws.String(sqARN),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ScheduledQueryStateDisabled, descOut.ScheduledQuery.State)

		_, err = client.TagResource(ctx, &tqsdk.TagResourceInput{
			ResourceARN: aws.String(sqARN),
			Tags:        []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(ctx, &tqsdk.ListTagsForResourceInput{
			ResourceARN: aws.String(sqARN),
		})
		require.NoError(t, err)
		tags := map[string]string{}
		for _, tg := range tagsOut.Tags {
			tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		assert.Equal(t, "test", tags["env"])
		assert.Equal(t, "platform", tags["team"])

		_, err = client.UntagResource(ctx, &tqsdk.UntagResourceInput{
			ResourceARN: aws.String(sqARN),
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		afterTagsOut, err := client.ListTagsForResource(ctx, &tqsdk.ListTagsForResourceInput{
			ResourceARN: aws.String(sqARN),
		})
		require.NoError(t, err)
		afterTags := map[string]string{}
		for _, tg := range afterTagsOut.Tags {
			afterTags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		assert.Equal(t, "test", afterTags["env"])
		assert.NotContains(t, afterTags, "team")
	})
}
