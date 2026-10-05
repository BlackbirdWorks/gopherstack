package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ListTagsForResourcePaging covers MaxResults/NextToken (body-bound,
// awsjson1.0); the shared tag store behind it is timestreamwrite's handler.
func TestRealClient_ListTagsForResourcePaging(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	created, err := client.CreateScheduledQuery(t.Context(), &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("paged-tags"),
		QueryString:                    aws.String("SELECT 1"),
		ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq-role"),
		ScheduleConfiguration:          &types.ScheduleConfiguration{ScheduleExpression: aws.String("rate(1 hour)")},
		NotificationConfiguration: &types.NotificationConfiguration{
			SnsConfiguration: &types.SnsConfiguration{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t")},
		},
		ErrorReportConfiguration: &types.ErrorReportConfiguration{
			S3Configuration: &types.S3Configuration{BucketName: aws.String("b")},
		},
		Tags: []types.Tag{
			{Key: aws.String("a"), Value: aws.String("1")},
			{Key: aws.String("b"), Value: aws.String("2")},
			{Key: aws.String("c"), Value: aws.String("3")},
		},
	})
	require.NoError(t, err)

	var token *string

	got, pages := 0, 0

	for {
		out, listErr := client.ListTagsForResource(t.Context(), &tqsdk.ListTagsForResourceInput{
			ResourceARN: created.Arn, MaxResults: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, listErr)

		got += len(out.Tags)
		pages++

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	require.Equal(t, 3, got)
	require.Equal(t, 2, pages)

	_, err = client.ListTagsForResource(t.Context(), &tqsdk.ListTagsForResourceInput{
		ResourceARN: created.Arn, NextToken: aws.String("%%%"),
	})

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
}
