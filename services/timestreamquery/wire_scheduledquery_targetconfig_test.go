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

// Test_SDKRoundTrip_ScheduledQuery_TargetConfigurationDetail drives
// CreateScheduledQuery with a full TargetConfiguration.TimestreamConfiguration
// (including DimensionMappings and TimeColumn, both required subfields per
// aws-sdk-go-v2/service/timestreamquery@v1.39.4's api_op_CreateScheduledQuery.go
// TimestreamConfiguration struct whenever TargetConfiguration is set) through
// the real SDK client, then DescribeScheduledQuery and asserts every field
// round-trips. Proves the fix for the silent-drop where DimensionMappings/
// TimeColumn/MeasureNameColumn/MixedMeasureMappings/MultiMeasureMappings were
// accepted nowhere on create and echoed nowhere on describe.
func Test_SDKRoundTrip_ScheduledQuery_TargetConfigurationDetail(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateScheduledQuery(ctx, &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("sq-target-" + uuid.NewString()[:8]),
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
				DatabaseName: aws.String("target-db"),
				TableName:    aws.String("target-table"),
				TimeColumn:   aws.String("time"),
				DimensionMappings: []types.DimensionMapping{
					{Name: aws.String("region"), DimensionValueType: types.DimensionValueTypeVarchar},
				},
				MeasureNameColumn: aws.String("measure_name"),
				MixedMeasureMappings: []types.MixedMeasureMapping{
					{
						MeasureName:       aws.String("cpu"),
						SourceColumn:      aws.String("cpu_value"),
						MeasureValueType:  types.MeasureValueTypeDouble,
						TargetMeasureName: aws.String("cpu_utilization"),
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Arn)

	desc, err := client.DescribeScheduledQuery(ctx, &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
	})
	require.NoError(t, err)

	require.NotNil(t, desc.ScheduledQuery.TargetConfiguration)
	tsConfig := desc.ScheduledQuery.TargetConfiguration.TimestreamConfiguration
	require.NotNil(t, tsConfig)

	assert.Equal(t, "target-db", aws.ToString(tsConfig.DatabaseName))
	assert.Equal(t, "target-table", aws.ToString(tsConfig.TableName))
	assert.Equal(t, "time", aws.ToString(tsConfig.TimeColumn))
	assert.Equal(t, "measure_name", aws.ToString(tsConfig.MeasureNameColumn))

	require.Len(t, tsConfig.DimensionMappings, 1)
	assert.Equal(t, "region", aws.ToString(tsConfig.DimensionMappings[0].Name))
	assert.Equal(t, types.DimensionValueTypeVarchar, tsConfig.DimensionMappings[0].DimensionValueType)

	require.Len(t, tsConfig.MixedMeasureMappings, 1)
	mm := tsConfig.MixedMeasureMappings[0]
	assert.Equal(t, "cpu", aws.ToString(mm.MeasureName))
	assert.Equal(t, "cpu_value", aws.ToString(mm.SourceColumn))
	assert.Equal(t, types.MeasureValueTypeDouble, mm.MeasureValueType)
	assert.Equal(t, "cpu_utilization", aws.ToString(mm.TargetMeasureName))
}
