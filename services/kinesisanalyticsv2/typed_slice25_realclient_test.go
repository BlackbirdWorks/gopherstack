package kinesisanalyticsv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisanalyticsv2sdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// createTestSQLApp creates a minimal SQL application via the real typed
// client and returns its current ApplicationVersionId.
func createTestSQLApp(t *testing.T, client *kinesisanalyticsv2sdk.Client, name string) int64 {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
		ApplicationName:      aws.String(name),
		RuntimeEnvironment:   kav2types.RuntimeEnvironmentSql10,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/kav2-role"),
	})
	require.NoError(t, err)

	return describeTestAppVersion(t, client, name)
}

func describeTestAppVersion(t *testing.T, client *kinesisanalyticsv2sdk.Client, name string) int64 {
	t.Helper()

	out, err := client.DescribeApplication(
		t.Context(),
		&kinesisanalyticsv2sdk.DescribeApplicationInput{
			ApplicationName: aws.String(name),
		},
	)
	require.NoError(t, err)

	return aws.ToInt64(out.ApplicationDetail.ApplicationVersionId)
}

// TestKinesisAnalyticsV2_TypedSlice25 drives every remaining uncovered op
// through a real aws-sdk-go-v2 kinesisanalyticsv2 client:
// AddApplicationCloudWatchLoggingOption,
// AddApplicationInputProcessingConfiguration, AddApplicationOutput,
// AddApplicationVpcConfiguration, CreateApplicationPresignedUrl,
// DeleteApplicationCloudWatchLoggingOption,
// DeleteApplicationInputProcessingConfiguration, DeleteApplicationOutput,
// DeleteApplicationReferenceDataSource, DeleteApplicationVpcConfiguration,
// DescribeApplicationOperation, DescribeApplicationSnapshot,
// DescribeApplicationVersion, DiscoverInputSchema, ListApplicationOperations,
// ListApplicationVersions, RollbackApplication,
// UpdateApplicationMaintenanceConfiguration.
func TestKinesisAnalyticsV2_TypedSlice25(t *testing.T) {
	t.Parallel()

	t.Run("cloudwatch logging and vpc config, operations", func(t *testing.T) {
		t.Parallel()

		h := kinesisanalyticsv2.NewHandler(
			kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
		)
		client := newTestKAV2SDKClient(t, h)
		ctx := t.Context()

		appName := "slice25-cwl-vpc-app"
		ver := createTestSQLApp(t, client, appName)

		cwl, err := client.AddApplicationCloudWatchLoggingOption(
			ctx,
			&kinesisanalyticsv2sdk.AddApplicationCloudWatchLoggingOptionInput{
				ApplicationName:             aws.String(appName),
				CurrentApplicationVersionId: aws.Int64(ver),
				CloudWatchLoggingOption: &kav2types.CloudWatchLoggingOption{
					LogStreamARN: aws.String(
						"arn:aws:logs:us-east-1:000000000000:log-group:/kav2:log-stream:s1",
					),
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, cwl.CloudWatchLoggingOptionDescriptions, 1)
		cwlID := aws.ToString(cwl.CloudWatchLoggingOptionDescriptions[0].CloudWatchLoggingOptionId)
		require.NotEmpty(t, cwlID)
		ver = aws.ToInt64(cwl.ApplicationVersionId)

		_, err = client.DeleteApplicationCloudWatchLoggingOption(
			ctx,
			&kinesisanalyticsv2sdk.DeleteApplicationCloudWatchLoggingOptionInput{
				ApplicationName:             aws.String(appName),
				CloudWatchLoggingOptionId:   aws.String(cwlID),
				CurrentApplicationVersionId: aws.Int64(ver),
			},
		)
		require.NoError(t, err)
		ver = describeTestAppVersion(t, client, appName)

		vpc, err := client.AddApplicationVpcConfiguration(
			ctx,
			&kinesisanalyticsv2sdk.AddApplicationVpcConfigurationInput{
				ApplicationName:             aws.String(appName),
				CurrentApplicationVersionId: aws.Int64(ver),
				VpcConfiguration: &kav2types.VpcConfiguration{
					SubnetIds:        []string{"subnet-1"},
					SecurityGroupIds: []string{"sg-1"},
				},
			},
		)
		require.NoError(t, err)
		require.NotNil(t, vpc.VpcConfigurationDescription)
		vpcID := aws.ToString(vpc.VpcConfigurationDescription.VpcConfigurationId)
		require.NotEmpty(t, vpcID)
		opID := aws.ToString(vpc.OperationId)
		require.NotEmpty(t, opID)
		ver = aws.ToInt64(vpc.ApplicationVersionId)

		_, err = client.DeleteApplicationVpcConfiguration(
			ctx,
			&kinesisanalyticsv2sdk.DeleteApplicationVpcConfigurationInput{
				ApplicationName:             aws.String(appName),
				VpcConfigurationId:          aws.String(vpcID),
				CurrentApplicationVersionId: aws.Int64(ver),
			},
		)
		require.NoError(t, err)

		opDetails, err := client.DescribeApplicationOperation(
			ctx,
			&kinesisanalyticsv2sdk.DescribeApplicationOperationInput{
				ApplicationName: aws.String(appName),
				OperationId:     aws.String(opID),
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"AddApplicationVpcConfiguration",
			aws.ToString(opDetails.ApplicationOperationInfoDetails.Operation),
		)

		opsList, err := client.ListApplicationOperations(
			ctx,
			&kinesisanalyticsv2sdk.ListApplicationOperationsInput{
				ApplicationName: aws.String(appName),
			},
		)
		require.NoError(t, err)
		var found bool
		for _, op := range opsList.ApplicationOperationInfoList {
			if aws.ToString(op.OperationId) == opID {
				found = true
			}
		}
		assert.True(t, found, "recorded operation must appear in ListApplicationOperations")
	})

	t.Run("input processing configuration", func(t *testing.T) {
		t.Parallel()

		h := kinesisanalyticsv2.NewHandler(
			kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
		)
		client := newTestKAV2SDKClient(t, h)
		ctx := t.Context()

		appName := "slice25-input-proc-app"
		ver := createTestSQLApp(t, client, appName)

		_, err := client.AddApplicationInput(ctx, &kinesisanalyticsv2sdk.AddApplicationInputInput{
			ApplicationName:             aws.String(appName),
			CurrentApplicationVersionId: aws.Int64(ver),
			Input: &kav2types.Input{
				NamePrefix: aws.String("SOURCE"),
				KinesisStreamsInput: &kav2types.KinesisStreamsInput{
					ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/src"),
				},
				InputSchema: &kav2types.SourceSchema{
					RecordEncoding: aws.String("UTF-8"),
					RecordFormat: &kav2types.RecordFormat{
						RecordFormatType: kav2types.RecordFormatTypeJson,
					},
					RecordColumns: []kav2types.RecordColumn{
						{
							Name:    aws.String("col1"),
							SqlType: aws.String("VARCHAR(4)"),
							Mapping: aws.String("$.col1"),
						},
					},
				},
			},
		})
		require.NoError(t, err)

		got, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
			ApplicationName: aws.String(appName),
		})
		require.NoError(t, err)
		sqlDesc := got.ApplicationDetail.ApplicationConfigurationDescription.SqlApplicationConfigurationDescription
		require.NotNil(t, sqlDesc)
		require.Len(t, sqlDesc.InputDescriptions, 1)
		inputID := aws.ToString(sqlDesc.InputDescriptions[0].InputId)
		require.NotEmpty(t, inputID)
		ver = aws.ToInt64(got.ApplicationDetail.ApplicationVersionId)

		procOut, err := client.AddApplicationInputProcessingConfiguration(
			ctx,
			&kinesisanalyticsv2sdk.AddApplicationInputProcessingConfigurationInput{
				ApplicationName:             aws.String(appName),
				CurrentApplicationVersionId: aws.Int64(ver),
				InputId:                     aws.String(inputID),
				InputProcessingConfiguration: &kav2types.InputProcessingConfiguration{
					InputLambdaProcessor: &kav2types.InputLambdaProcessor{
						ResourceARN: aws.String(
							"arn:aws:lambda:us-east-1:000000000000:function:preprocess",
						),
					},
				},
			},
		)
		require.NoError(t, err)
		require.NotNil(t, procOut.InputProcessingConfigurationDescription)
		ver = aws.ToInt64(procOut.ApplicationVersionId)

		_, err = client.DeleteApplicationInputProcessingConfiguration(
			ctx,
			&kinesisanalyticsv2sdk.DeleteApplicationInputProcessingConfigurationInput{
				ApplicationName:             aws.String(appName),
				InputId:                     aws.String(inputID),
				CurrentApplicationVersionId: aws.Int64(ver),
			},
		)
		require.NoError(t, err)

		got, err = client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
			ApplicationName: aws.String(appName),
		})
		require.NoError(t, err)
		sqlDesc = got.ApplicationDetail.ApplicationConfigurationDescription.SqlApplicationConfigurationDescription
		require.Len(t, sqlDesc.InputDescriptions, 1)
		assert.Nil(t, sqlDesc.InputDescriptions[0].InputProcessingConfigurationDescription)
	})

	t.Run("output and reference data source", func(t *testing.T) {
		t.Parallel()

		h := kinesisanalyticsv2.NewHandler(
			kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
		)
		client := newTestKAV2SDKClient(t, h)
		ctx := t.Context()

		appName := "slice25-output-ref-app"
		ver := createTestSQLApp(t, client, appName)

		outOut, err := client.AddApplicationOutput(
			ctx,
			&kinesisanalyticsv2sdk.AddApplicationOutputInput{
				ApplicationName:             aws.String(appName),
				CurrentApplicationVersionId: aws.Int64(ver),
				Output: &kav2types.Output{
					Name: aws.String("DESTINATION"),
					DestinationSchema: &kav2types.DestinationSchema{
						RecordFormatType: kav2types.RecordFormatTypeJson,
					},
					KinesisStreamsOutput: &kav2types.KinesisStreamsOutput{
						ResourceARN: aws.String(
							"arn:aws:kinesis:us-east-1:000000000000:stream/dst",
						),
					},
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, outOut.OutputDescriptions, 1)
		outputID := aws.ToString(outOut.OutputDescriptions[0].OutputId)
		require.NotEmpty(t, outputID)
		ver = aws.ToInt64(outOut.ApplicationVersionId)

		_, err = client.DeleteApplicationOutput(
			ctx,
			&kinesisanalyticsv2sdk.DeleteApplicationOutputInput{
				ApplicationName:             aws.String(appName),
				OutputId:                    aws.String(outputID),
				CurrentApplicationVersionId: aws.Int64(ver),
			},
		)
		require.NoError(t, err)
		ver = describeTestAppVersion(t, client, appName)

		refOut, err := client.AddApplicationReferenceDataSource(
			ctx,
			&kinesisanalyticsv2sdk.AddApplicationReferenceDataSourceInput{
				ApplicationName:             aws.String(appName),
				CurrentApplicationVersionId: aws.Int64(ver),
				ReferenceDataSource: &kav2types.ReferenceDataSource{
					TableName: aws.String("REF_TABLE"),
					ReferenceSchema: &kav2types.SourceSchema{
						RecordEncoding: aws.String("UTF-8"),
						RecordFormat: &kav2types.RecordFormat{
							RecordFormatType: kav2types.RecordFormatTypeJson,
						},
						RecordColumns: []kav2types.RecordColumn{
							{
								Name:    aws.String("k"),
								SqlType: aws.String("VARCHAR(4)"),
								Mapping: aws.String("$.k"),
							},
						},
					},
					S3ReferenceDataSource: &kav2types.S3ReferenceDataSource{
						BucketARN: aws.String("arn:aws:s3:::slice25-ref-bucket"),
						FileKey:   aws.String("ref.json"),
					},
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, refOut.ReferenceDataSourceDescriptions, 1)
		refID := aws.ToString(refOut.ReferenceDataSourceDescriptions[0].ReferenceId)
		require.NotEmpty(t, refID)
		ver = aws.ToInt64(refOut.ApplicationVersionId)

		_, err = client.DeleteApplicationReferenceDataSource(
			ctx,
			&kinesisanalyticsv2sdk.DeleteApplicationReferenceDataSourceInput{
				ApplicationName:             aws.String(appName),
				ReferenceId:                 aws.String(refID),
				CurrentApplicationVersionId: aws.Int64(ver),
			},
		)
		require.NoError(t, err)

		got, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
			ApplicationName: aws.String(appName),
		})
		require.NoError(t, err)
		// With no inputs/outputs/reference-data-sources and no non-SQL config
		// left, both ApplicationConfigurationDescription and its nested
		// SqlApplicationConfigurationDescription are correctly absent (matches
		// real AWS: only present when there's config to describe), so their
		// absence -- not empty sub-slices -- is the proof both deletes took
		// effect.
		cfgDesc := got.ApplicationDetail.ApplicationConfigurationDescription
		if cfgDesc != nil {
			assert.Nil(t, cfgDesc.SqlApplicationConfigurationDescription)
		}
	})

	t.Run("versions and rollback", func(t *testing.T) {
		t.Parallel()

		h := kinesisanalyticsv2.NewHandler(
			kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
		)
		client := newTestKAV2SDKClient(t, h)
		ctx := t.Context()

		appName := "slice25-versions-app"
		ver1 := createTestSQLApp(t, client, appName)

		_, err := client.AddApplicationInput(ctx, &kinesisanalyticsv2sdk.AddApplicationInputInput{
			ApplicationName:             aws.String(appName),
			CurrentApplicationVersionId: aws.Int64(ver1),
			Input: &kav2types.Input{
				NamePrefix: aws.String("SOURCE"),
				KinesisStreamsInput: &kav2types.KinesisStreamsInput{
					ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/src"),
				},
				InputSchema: &kav2types.SourceSchema{
					RecordEncoding: aws.String("UTF-8"),
					RecordFormat: &kav2types.RecordFormat{
						RecordFormatType: kav2types.RecordFormatTypeJson,
					},
					RecordColumns: []kav2types.RecordColumn{
						{
							Name:    aws.String("col1"),
							SqlType: aws.String("VARCHAR(4)"),
							Mapping: aws.String("$.col1"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		ver2 := describeTestAppVersion(t, client, appName)
		require.Greater(t, ver2, ver1)

		listOut, err := client.ListApplicationVersions(
			ctx,
			&kinesisanalyticsv2sdk.ListApplicationVersionsInput{
				ApplicationName: aws.String(appName),
			},
		)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(listOut.ApplicationVersionSummaries), 2)

		verDetail, err := client.DescribeApplicationVersion(
			ctx,
			&kinesisanalyticsv2sdk.DescribeApplicationVersionInput{
				ApplicationName:      aws.String(appName),
				ApplicationVersionId: aws.Int64(ver1),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, ver1, aws.ToInt64(verDetail.ApplicationVersionDetail.ApplicationVersionId))

		rolledBack, err := client.RollbackApplication(
			ctx,
			&kinesisanalyticsv2sdk.RollbackApplicationInput{
				ApplicationName:             aws.String(appName),
				CurrentApplicationVersionId: aws.Int64(ver2),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, rolledBack.ApplicationDetail)
		assert.Equal(
			t,
			ver1,
			aws.ToInt64(rolledBack.ApplicationDetail.ApplicationVersionRolledBackTo),
		)
	})

	t.Run(
		"presigned url, maintenance config, discover schema, snapshot describe",
		func(t *testing.T) {
			t.Parallel()

			h := kinesisanalyticsv2.NewHandler(
				kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
			)
			client := newTestKAV2SDKClient(t, h)
			ctx := t.Context()

			appName := "slice25-misc-app"
			createTestSQLApp(t, client, appName)

			presigned, err := client.CreateApplicationPresignedUrl(
				ctx,
				&kinesisanalyticsv2sdk.CreateApplicationPresignedUrlInput{
					ApplicationName: aws.String(appName),
					UrlType:         kav2types.UrlTypeFlinkDashboardUrl,
				},
			)
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(presigned.AuthorizedUrl), "FLINK_DASHBOARD_URL")

			maint, err := client.UpdateApplicationMaintenanceConfiguration(
				ctx,
				&kinesisanalyticsv2sdk.UpdateApplicationMaintenanceConfigurationInput{
					ApplicationName: aws.String(appName),
					ApplicationMaintenanceConfigurationUpdate: &kav2types.ApplicationMaintenanceConfigurationUpdate{
						ApplicationMaintenanceWindowStartTimeUpdate: aws.String("06:00"),
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, maint.ApplicationMaintenanceConfigurationDescription)
			assert.Equal(
				t,
				"06:00",
				aws.ToString(
					maint.ApplicationMaintenanceConfigurationDescription.ApplicationMaintenanceWindowStartTime,
				),
			)

			discovered, err := client.DiscoverInputSchema(
				ctx,
				&kinesisanalyticsv2sdk.DiscoverInputSchemaInput{
					ResourceARN: aws.String(
						"arn:aws:kinesis:us-east-1:000000000000:stream/src",
					),
					ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/kav2-role"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, discovered.InputSchema)
			require.Len(t, discovered.InputSchema.RecordColumns, 3)

			_, err = client.StartApplication(ctx, &kinesisanalyticsv2sdk.StartApplicationInput{
				ApplicationName: aws.String(appName),
			})
			require.NoError(t, err)

			_, err = client.CreateApplicationSnapshot(
				ctx,
				&kinesisanalyticsv2sdk.CreateApplicationSnapshotInput{
					ApplicationName: aws.String(appName),
					SnapshotName:    aws.String("slice25-snapshot"),
				},
			)
			require.NoError(t, err)

			snap, err := client.DescribeApplicationSnapshot(
				ctx,
				&kinesisanalyticsv2sdk.DescribeApplicationSnapshotInput{
					ApplicationName: aws.String(appName),
					SnapshotName:    aws.String("slice25-snapshot"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, snap.SnapshotDetails)
			assert.Equal(t, "slice25-snapshot", aws.ToString(snap.SnapshotDetails.SnapshotName))
		},
	)
}
