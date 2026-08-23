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

// Test_SDKRoundTrip_DescribeScheduledQuery_TargetConfiguration proves
// gopherstack-r80d batch 25's finding: DescribeScheduledQueryOutput.ScheduledQuery
// (types.ScheduledQueryDescription) is required to carry TargetConfiguration
// once present, and once TargetConfiguration.TimestreamConfiguration is
// present its own DatabaseName/DimensionMappings/TableName/TimeColumn are ALL
// required (aws-sdk-go-v2/service/timestreamquery@v1.39.4/validators.go's
// validateTimestreamConfiguration, lines 651-666). Before the fix,
// gopherstack's CreateScheduledQuery request parsing only read
// DatabaseName/TableName from TargetConfiguration.TimestreamConfiguration and
// silently discarded TimeColumn/DimensionMappings entirely (no backing struct
// field), so a real client's valid CreateScheduledQuery (which the SDK's own
// client-side validator requires to include all four members once
// TargetConfiguration is set at all) would get back a DescribeScheduledQuery
// response missing two required TimestreamConfiguration members.
func Test_SDKRoundTrip_DescribeScheduledQuery_TargetConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateScheduledQuery(ctx, &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("sq-" + uuid.NewString()[:8]),
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
				BucketName: aws.String("tsq-error-bucket"),
			},
		},
		TargetConfiguration: &types.TargetConfiguration{
			TimestreamConfiguration: &types.TimestreamConfiguration{
				DatabaseName: aws.String("dest-db"),
				TableName:    aws.String("dest-table"),
				TimeColumn:   aws.String("time"),
				DimensionMappings: []types.DimensionMapping{
					{
						Name:               aws.String("region"),
						DimensionValueType: types.DimensionValueTypeVarchar,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeScheduledQuery(ctx, &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ScheduledQuery)
	require.NotNil(
		t, described.ScheduledQuery.TargetConfiguration,
		"TargetConfiguration must be present once the create request set one",
	)
	tc := described.ScheduledQuery.TargetConfiguration.TimestreamConfiguration
	require.NotNil(t, tc, "TargetConfiguration.TimestreamConfiguration must be present")

	require.NotNil(t, tc.DatabaseName)
	assert.Equal(t, "dest-db", *tc.DatabaseName)
	require.NotNil(t, tc.TableName)
	assert.Equal(t, "dest-table", *tc.TableName)
	require.NotNil(
		t, tc.TimeColumn,
		"TimeColumn is required once TimestreamConfiguration is present",
	)
	assert.Equal(t, "time", *tc.TimeColumn)
	require.Len(
		t, tc.DimensionMappings, 1,
		"DimensionMappings is required once TimestreamConfiguration is present",
	)
	require.NotNil(t, tc.DimensionMappings[0].Name)
	assert.Equal(t, "region", *tc.DimensionMappings[0].Name)
	assert.Equal(t, types.DimensionValueTypeVarchar, tc.DimensionMappings[0].DimensionValueType)
}
