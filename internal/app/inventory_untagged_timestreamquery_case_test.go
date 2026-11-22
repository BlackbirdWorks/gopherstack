package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/stretchr/testify/require"
)

func timestreamQueryInventoryCase() inventoryCase {
	return inventoryCase{
		name: "timestreamquery",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &tqsdk.CreateScheduledQueryInput{
				Name:                           aws.String(name),
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
					S3Configuration: &types.S3Configuration{BucketName: aws.String("tsq-error-bucket")},
				},
			}

			for k, v := range inventoryTagMap(tagged) {
				in.Tags = append(in.Tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
			}

			out, err := tqsdk.NewFromConfig(cfg).CreateScheduledQuery(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.Arn)
		},
	}
}
