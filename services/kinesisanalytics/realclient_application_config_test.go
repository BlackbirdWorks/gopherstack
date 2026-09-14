package kinesisanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kasdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	katypes "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ApplicationConfig drives kinesisanalytics's application
// configuration op families (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client: AddApplicationCloudWatchLoggingOption,
// AddApplicationInputProcessingConfiguration, AddApplicationOutput,
// AddApplicationReferenceDataSource, DeleteApplicationCloudWatchLoggingOption,
// DeleteApplicationInputProcessingConfiguration, DeleteApplicationOutput,
// DeleteApplicationReferenceDataSource, DiscoverInputSchema,
// ListTagsForResource, StartApplication, TagResource, UntagResource,
// UpdateApplication.
func TestRealClient_ApplicationConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "full application configuration lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newTestHandlerWithBackend(t)
				client := newTestKASDKClient(t, h)
				ctx := t.Context()
				appName := "s23-lifecycle-app"

				_, err := client.CreateApplication(ctx, &kasdk.CreateApplicationInput{
					ApplicationName: aws.String(appName),
				})
				require.NoError(t, err)

				currentVersion := func() int64 {
					out, describeErr := client.DescribeApplication(ctx, &kasdk.DescribeApplicationInput{
						ApplicationName: aws.String(appName),
					})
					require.NoError(t, describeErr)

					return aws.ToInt64(out.ApplicationDetail.ApplicationVersionId)
				}

				// --- CloudWatch logging option ---
				_, err = client.AddApplicationCloudWatchLoggingOption(
					ctx,
					&kasdk.AddApplicationCloudWatchLoggingOptionInput{
						ApplicationName:             aws.String(appName),
						CurrentApplicationVersionId: aws.Int64(currentVersion()),
						CloudWatchLoggingOption: &katypes.CloudWatchLoggingOption{
							LogStreamARN: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s"),
							RoleARN:      aws.String("arn:aws:iam::000000000000:role/logs-role"),
						},
					},
				)
				require.NoError(t, err)

				descAfterLog, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				require.Len(t, descAfterLog.ApplicationDetail.CloudWatchLoggingOptionDescriptions, 1)
				logOptID := descAfterLog.ApplicationDetail.CloudWatchLoggingOptionDescriptions[0].CloudWatchLoggingOptionId
				assert.Equal(
					t, "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
					aws.ToString(descAfterLog.ApplicationDetail.CloudWatchLoggingOptionDescriptions[0].LogStreamARN),
				)

				_, err = client.DeleteApplicationCloudWatchLoggingOption(
					ctx,
					&kasdk.DeleteApplicationCloudWatchLoggingOptionInput{
						ApplicationName:             aws.String(appName),
						CurrentApplicationVersionId: aws.Int64(currentVersion()),
						CloudWatchLoggingOptionId:   logOptID,
					},
				)
				require.NoError(t, err)

				descAfterLogDelete, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				assert.Empty(t, descAfterLogDelete.ApplicationDetail.CloudWatchLoggingOptionDescriptions)

				// --- Input + input processing configuration ---
				_, err = client.AddApplicationInput(ctx, &kasdk.AddApplicationInputInput{
					ApplicationName:             aws.String(appName),
					CurrentApplicationVersionId: aws.Int64(currentVersion()),
					Input: &katypes.Input{
						NamePrefix: aws.String("SOURCE_SQL_STREAM"),
						KinesisStreamsInput: &katypes.KinesisStreamsInput{
							ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/test"),
							RoleARN:     aws.String("arn:aws:iam::000000000000:role/role"),
						},
						InputSchema: &katypes.SourceSchema{
							RecordFormat: &katypes.RecordFormat{RecordFormatType: katypes.RecordFormatTypeJson},
							RecordColumns: []katypes.RecordColumn{
								{Name: aws.String("COL1"), SqlType: aws.String("VARCHAR(4)")},
							},
						},
					},
				})
				require.NoError(t, err)

				descAfterInput, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				require.Len(t, descAfterInput.ApplicationDetail.InputDescriptions, 1)
				inputID := descAfterInput.ApplicationDetail.InputDescriptions[0].InputId
				assert.Nil(
					t,
					descAfterInput.ApplicationDetail.InputDescriptions[0].InputProcessingConfigurationDescription,
				)

				_, err = client.AddApplicationInputProcessingConfiguration(
					ctx,
					&kasdk.AddApplicationInputProcessingConfigurationInput{
						ApplicationName:             aws.String(appName),
						CurrentApplicationVersionId: aws.Int64(currentVersion()),
						InputId:                     inputID,
						InputProcessingConfiguration: &katypes.InputProcessingConfiguration{
							InputLambdaProcessor: &katypes.InputLambdaProcessor{
								ResourceARN: aws.String("arn:aws:lambda:us-east-1:000000000000:function:fn"),
								RoleARN:     aws.String("arn:aws:iam::000000000000:role/lambda-role"),
							},
						},
					},
				)
				require.NoError(t, err)

				descAfterProc, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				require.NotNil(
					t,
					descAfterProc.ApplicationDetail.InputDescriptions[0].InputProcessingConfigurationDescription,
				)
				assert.Equal(
					t, "arn:aws:lambda:us-east-1:000000000000:function:fn",
					aws.ToString(descAfterProc.ApplicationDetail.InputDescriptions[0].
						InputProcessingConfigurationDescription.InputLambdaProcessorDescription.ResourceARN),
				)

				_, err = client.DeleteApplicationInputProcessingConfiguration(
					ctx, &kasdk.DeleteApplicationInputProcessingConfigurationInput{
						ApplicationName:             aws.String(appName),
						CurrentApplicationVersionId: aws.Int64(currentVersion()),
						InputId:                     inputID,
					},
				)
				require.NoError(t, err)

				descAfterProcDelete, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				assert.Nil(
					t,
					descAfterProcDelete.ApplicationDetail.InputDescriptions[0].InputProcessingConfigurationDescription,
				)

				// --- Output ---
				_, err = client.AddApplicationOutput(ctx, &kasdk.AddApplicationOutputInput{
					ApplicationName:             aws.String(appName),
					CurrentApplicationVersionId: aws.Int64(currentVersion()),
					Output: &katypes.Output{
						Name:              aws.String("DEST_SQL_STREAM"),
						DestinationSchema: &katypes.DestinationSchema{RecordFormatType: katypes.RecordFormatTypeJson},
						KinesisStreamsOutput: &katypes.KinesisStreamsOutput{
							ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/dest"),
							RoleARN:     aws.String("arn:aws:iam::000000000000:role/role"),
						},
					},
				})
				require.NoError(t, err)

				descAfterOutput, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				require.Len(t, descAfterOutput.ApplicationDetail.OutputDescriptions, 1)
				outputID := descAfterOutput.ApplicationDetail.OutputDescriptions[0].OutputId
				assert.Equal(
					t,
					"DEST_SQL_STREAM",
					aws.ToString(descAfterOutput.ApplicationDetail.OutputDescriptions[0].Name),
				)

				_, err = client.DeleteApplicationOutput(ctx, &kasdk.DeleteApplicationOutputInput{
					ApplicationName:             aws.String(appName),
					CurrentApplicationVersionId: aws.Int64(currentVersion()),
					OutputId:                    outputID,
				})
				require.NoError(t, err)

				descAfterOutputDelete, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				assert.Empty(t, descAfterOutputDelete.ApplicationDetail.OutputDescriptions)

				// --- Reference data source ---
				_, err = client.AddApplicationReferenceDataSource(ctx, &kasdk.AddApplicationReferenceDataSourceInput{
					ApplicationName:             aws.String(appName),
					CurrentApplicationVersionId: aws.Int64(currentVersion()),
					ReferenceDataSource: &katypes.ReferenceDataSource{
						TableName: aws.String("REF_TABLE"),
						ReferenceSchema: &katypes.SourceSchema{
							RecordFormat: &katypes.RecordFormat{RecordFormatType: katypes.RecordFormatTypeJson},
							RecordColumns: []katypes.RecordColumn{
								{Name: aws.String("ID"), SqlType: aws.String("INTEGER")},
							},
						},
						S3ReferenceDataSource: &katypes.S3ReferenceDataSource{
							BucketARN:        aws.String("arn:aws:s3:::s23-ref-bucket"),
							FileKey:          aws.String("ref.json"),
							ReferenceRoleARN: aws.String("arn:aws:iam::000000000000:role/ref-role"),
						},
					},
				})
				require.NoError(t, err)

				descAfterRef, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				require.Len(t, descAfterRef.ApplicationDetail.ReferenceDataSourceDescriptions, 1)
				refID := descAfterRef.ApplicationDetail.ReferenceDataSourceDescriptions[0].ReferenceId
				assert.Equal(
					t,
					"REF_TABLE",
					aws.ToString(descAfterRef.ApplicationDetail.ReferenceDataSourceDescriptions[0].TableName),
				)

				_, err = client.DeleteApplicationReferenceDataSource(
					ctx,
					&kasdk.DeleteApplicationReferenceDataSourceInput{
						ApplicationName:             aws.String(appName),
						CurrentApplicationVersionId: aws.Int64(currentVersion()),
						ReferenceId:                 refID,
					},
				)
				require.NoError(t, err)

				descAfterRefDelete, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				assert.Empty(t, descAfterRefDelete.ApplicationDetail.ReferenceDataSourceDescriptions)

				// --- UpdateApplication ---
				_, err = client.UpdateApplication(ctx, &kasdk.UpdateApplicationInput{
					ApplicationName:             aws.String(appName),
					CurrentApplicationVersionId: aws.Int64(currentVersion()),
					ApplicationUpdate: &katypes.ApplicationUpdate{
						ApplicationCodeUpdate: aws.String("SELECT 1;"),
					},
				})
				require.NoError(t, err)

				descAfterUpdate, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				assert.Equal(t, "SELECT 1;", aws.ToString(descAfterUpdate.ApplicationDetail.ApplicationCode))

				// --- StartApplication ---
				_, err = client.StartApplication(ctx, &kasdk.StartApplicationInput{
					ApplicationName: aws.String(appName),
					InputConfigurations: []katypes.InputConfiguration{{
						Id: inputID,
						InputStartingPositionConfiguration: &katypes.InputStartingPositionConfiguration{
							InputStartingPosition: katypes.InputStartingPositionNow,
						},
					}},
				})
				require.NoError(t, err)

				descAfterStart, err := client.DescribeApplication(
					ctx,
					&kasdk.DescribeApplicationInput{ApplicationName: aws.String(appName)},
				)
				require.NoError(t, err)
				assert.NotEqual(t, katypes.ApplicationStatusReady, descAfterStart.ApplicationDetail.ApplicationStatus)

				// --- Tags ---
				appARN := descAfterStart.ApplicationDetail.ApplicationARN

				_, err = client.TagResource(ctx, &kasdk.TagResourceInput{
					ResourceARN: appARN,
					Tags:        []katypes.Tag{{Key: aws.String("team"), Value: aws.String("data")}},
				})
				require.NoError(t, err)

				tagsOut, err := client.ListTagsForResource(ctx, &kasdk.ListTagsForResourceInput{ResourceARN: appARN})
				require.NoError(t, err)
				require.Len(t, tagsOut.Tags, 1)
				assert.Equal(t, "team", aws.ToString(tagsOut.Tags[0].Key))
				assert.Equal(t, "data", aws.ToString(tagsOut.Tags[0].Value))

				_, err = client.UntagResource(ctx, &kasdk.UntagResourceInput{
					ResourceARN: appARN,
					TagKeys:     []string{"team"},
				})
				require.NoError(t, err)

				tagsOut2, err := client.ListTagsForResource(ctx, &kasdk.ListTagsForResourceInput{ResourceARN: appARN})
				require.NoError(t, err)
				assert.Empty(t, tagsOut2.Tags)
			},
		},
		{
			name: "DiscoverInputSchema samples a real Kinesis reader",
			run: func(t *testing.T) {
				t.Helper()

				h, backend := newTestHandlerWithBackend(t)
				backend.SetKinesisStreamReader(&fakeKinesisReader{
					records: [][]byte{
						[]byte(`{"id":1,"name":"a"}`),
						[]byte(`{"id":2,"name":"bb"}`),
					},
				})
				client := newTestKASDKClient(t, h)
				ctx := t.Context()

				out, err := client.DiscoverInputSchema(ctx, &kasdk.DiscoverInputSchemaInput{
					ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/mystream"),
					RoleARN:     aws.String("arn:aws:iam::000000000000:role/role"),
				})
				require.NoError(t, err)
				require.NotNil(t, out.InputSchema)
				require.Len(t, out.InputSchema.RecordColumns, 2)
				assert.Equal(t, "id", aws.ToString(out.InputSchema.RecordColumns[0].Name))
				assert.Equal(t, "name", aws.ToString(out.InputSchema.RecordColumns[1].Name))
				require.Len(t, out.RawInputRecords, 2)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
