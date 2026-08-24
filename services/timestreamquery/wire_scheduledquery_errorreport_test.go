package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_SDKRoundTrip_ScheduledQuery_ErrorReportS3Configuration drives
// CreateScheduledQuery with ErrorReportConfiguration.S3Configuration.
// EncryptionOption/ObjectKeyPrefix set (both optional members of
// types.S3Configuration, api_op_CreateScheduledQuery.go) through the real SDK
// client, then DescribeScheduledQuery and asserts both round-trip. Proves the
// fix for the silent-drop where only BucketName was parsed/echoed.
func Test_SDKRoundTrip_ScheduledQuery_ErrorReportS3Configuration(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateScheduledQuery(ctx, &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("sq-errreport-" + uuid.NewString()[:8]),
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
				BucketName:       aws.String("tsq-error-bucket"),
				EncryptionOption: types.S3EncryptionOptionSseKms,
				ObjectKeyPrefix:  aws.String("errors/tsq/"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Arn)

	desc, err := client.DescribeScheduledQuery(ctx, &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
	})
	require.NoError(t, err)

	require.NotNil(t, desc.ScheduledQuery.ErrorReportConfiguration)
	s3Config := desc.ScheduledQuery.ErrorReportConfiguration.S3Configuration
	require.NotNil(t, s3Config)

	assert.Equal(t, "tsq-error-bucket", aws.ToString(s3Config.BucketName))
	assert.Equal(t, types.S3EncryptionOptionSseKms, s3Config.EncryptionOption)
	assert.Equal(t, "errors/tsq/", aws.ToString(s3Config.ObjectKeyPrefix))
}
