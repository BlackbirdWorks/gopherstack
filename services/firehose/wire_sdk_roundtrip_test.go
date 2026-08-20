package firehose_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/require"
)

// TestUpdateDestination_S3BucketWireKey_SDKRoundTrip proves, against the real
// aws-sdk-go-v2 firehose client's own serializer/deserializer, that every
// destination family whose Create and Update variants name their single S3
// bucket differently on the wire (aws-sdk-go-v2/service/firehose@v1.46.4
// serializers.go: "S3Configuration"/"S3BackupConfiguration" on the
// *Configuration Create type vs "S3Update"/"S3BackupUpdate" on the *Update
// Update type) actually round-trips through UpdateDestination. Before this
// fix, gopherstack's shared parsing struct only recognized the Create-side
// key, so an UpdateDestination call carrying a real client's "S3Update"/
// "S3BackupUpdate" payload silently left the backup bucket unchanged.
func TestUpdateDestination_S3BucketWireKey_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(client *firehosesdk.Client, streamName string) error
		update func(client *firehosesdk.Client, streamName, versionID, destinationID string) error
		verify func(t *testing.T, desc *types.DeliveryStreamDescription)
		name   string
	}{
		{
			name: "http endpoint",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						HttpEndpointDestinationConfiguration: &types.HttpEndpointDestinationConfiguration{
							EndpointConfiguration: &types.HttpEndpointConfiguration{
								Url: aws.String("https://original.example.com"),
							},
							S3Configuration: &types.S3DestinationConfiguration{
								BucketARN: aws.String("arn:aws:s3:::http-original-bucket"),
								RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							},
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					HttpEndpointDestinationUpdate: &types.HttpEndpointDestinationUpdate{
						S3Update: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::http-updated-bucket"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				httpDest := desc.Destinations[0].HttpEndpointDestinationDescription
				require.NotNil(t, httpDest)
				require.NotNil(t, httpDest.S3DestinationDescription)
				require.Equal(
					t,
					"arn:aws:s3:::http-updated-bucket",
					aws.ToString(httpDest.S3DestinationDescription.BucketARN),
				)
			},
		},
		{
			name: "opensearch",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						AmazonopensearchserviceDestinationConfiguration: &types.AmazonopensearchserviceDestinationConfiguration{
							DomainARN: aws.String("arn:aws:es:us-east-1:000000000000:domain/d"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							IndexName: aws.String("idx"),
							S3Configuration: &types.S3DestinationConfiguration{
								BucketARN: aws.String("arn:aws:s3:::os-original-bucket"),
								RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							},
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					AmazonopensearchserviceDestinationUpdate: &types.AmazonopensearchserviceDestinationUpdate{
						S3Update: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::os-updated-bucket"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				osDest := desc.Destinations[0].AmazonopensearchserviceDestinationDescription
				require.NotNil(t, osDest)
				require.NotNil(t, osDest.S3DestinationDescription)
				require.Equal(
					t,
					"arn:aws:s3:::os-updated-bucket",
					aws.ToString(osDest.S3DestinationDescription.BucketARN),
				)
			},
		},
		{
			name: "splunk",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						SplunkDestinationConfiguration: &types.SplunkDestinationConfiguration{
							HECEndpoint:     aws.String("https://splunk.example.com"),
							HECEndpointType: types.HECEndpointTypeRaw,
							HECToken:        aws.String("tok"),
							S3Configuration: &types.S3DestinationConfiguration{
								BucketARN: aws.String("arn:aws:s3:::splunk-original-bucket"),
								RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							},
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					SplunkDestinationUpdate: &types.SplunkDestinationUpdate{
						S3Update: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::splunk-updated-bucket"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				spDest := desc.Destinations[0].SplunkDestinationDescription
				require.NotNil(t, spDest)
				require.NotNil(t, spDest.S3DestinationDescription)
				require.Equal(
					t,
					"arn:aws:s3:::splunk-updated-bucket",
					aws.ToString(spDest.S3DestinationDescription.BucketARN),
				)
			},
		},
		{
			name: "elasticsearch",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						ElasticsearchDestinationConfiguration: &types.ElasticsearchDestinationConfiguration{
							DomainARN: aws.String(
								"arn:aws:es:us-east-1:000000000000:domain/legacy",
							),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							IndexName: aws.String("idx"),
							S3Configuration: &types.S3DestinationConfiguration{
								BucketARN: aws.String("arn:aws:s3:::es-original-bucket"),
								RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							},
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					ElasticsearchDestinationUpdate: &types.ElasticsearchDestinationUpdate{
						S3Update: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::es-updated-bucket"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				esDest := desc.Destinations[0].ElasticsearchDestinationDescription
				require.NotNil(t, esDest)
				require.NotNil(t, esDest.S3DestinationDescription)
				require.Equal(
					t,
					"arn:aws:s3:::es-updated-bucket",
					aws.ToString(esDest.S3DestinationDescription.BucketARN),
				)
			},
		},
		{
			name: "snowflake",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						SnowflakeDestinationConfiguration: &types.SnowflakeDestinationConfiguration{
							AccountUrl: aws.String("https://acct.snowflakecomputing.com"),
							RoleARN:    aws.String("arn:aws:iam::000000000000:role/r"),
							Database:   aws.String("db"),
							Schema:     aws.String("schema"),
							Table:      aws.String("tbl"),
							S3Configuration: &types.S3DestinationConfiguration{
								BucketARN: aws.String("arn:aws:s3:::sf-original-bucket"),
								RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							},
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					SnowflakeDestinationUpdate: &types.SnowflakeDestinationUpdate{
						S3Update: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::sf-updated-bucket"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				sfDest := desc.Destinations[0].SnowflakeDestinationDescription
				require.NotNil(t, sfDest)
				require.NotNil(t, sfDest.S3DestinationDescription)
				require.Equal(
					t,
					"arn:aws:s3:::sf-updated-bucket",
					aws.ToString(sfDest.S3DestinationDescription.BucketARN),
				)
			},
		},
		{
			name: "redshift staging and backup",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						RedshiftDestinationConfiguration: &types.RedshiftDestinationConfiguration{
							ClusterJDBCURL: aws.String("jdbc:redshift://cluster:5439/db"),
							RoleARN:        aws.String("arn:aws:iam::000000000000:role/r"),
							Username:       aws.String("u"),
							Password:       aws.String("p"),
							CopyCommand: &types.CopyCommand{
								DataTableName: aws.String("t"),
							},
							S3Configuration: &types.S3DestinationConfiguration{
								BucketARN: aws.String("arn:aws:s3:::rs-original-staging"),
								RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
							},
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					RedshiftDestinationUpdate: &types.RedshiftDestinationUpdate{
						S3Update: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::rs-updated-staging"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
						S3BackupUpdate: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::rs-updated-backup"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				rsDest := desc.Destinations[0].RedshiftDestinationDescription
				require.NotNil(t, rsDest)
				require.NotNil(t, rsDest.S3DestinationDescription)
				require.Equal(
					t,
					"arn:aws:s3:::rs-updated-staging",
					aws.ToString(rsDest.S3DestinationDescription.BucketARN),
				)
				require.NotNil(t, rsDest.S3BackupDescription)
				require.Equal(
					t,
					"arn:aws:s3:::rs-updated-backup",
					aws.ToString(rsDest.S3BackupDescription.BucketARN),
				)
			},
		},
		{
			name: "extended s3 backup",
			create: func(client *firehosesdk.Client, streamName string) error {
				_, err := client.CreateDeliveryStream(
					t.Context(),
					&firehosesdk.CreateDeliveryStreamInput{
						DeliveryStreamName: aws.String(streamName),
						ExtendedS3DestinationConfiguration: &types.ExtendedS3DestinationConfiguration{
							BucketARN: aws.String("arn:aws:s3:::ext-original-bucket"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				)

				return err
			},
			update: func(client *firehosesdk.Client, streamName, versionID, destinationID string) error {
				_, err := client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
					DeliveryStreamName:             aws.String(streamName),
					CurrentDeliveryStreamVersionId: aws.String(versionID),
					DestinationId:                  aws.String(destinationID),
					ExtendedS3DestinationUpdate: &types.ExtendedS3DestinationUpdate{
						BucketARN:    aws.String("arn:aws:s3:::ext-original-bucket"),
						RoleARN:      aws.String("arn:aws:iam::000000000000:role/r"),
						S3BackupMode: types.S3BackupModeEnabled,
						S3BackupUpdate: &types.S3DestinationUpdate{
							BucketARN: aws.String("arn:aws:s3:::ext-updated-backup"),
							RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
						},
					},
				})

				return err
			},
			verify: func(t *testing.T, desc *types.DeliveryStreamDescription) {
				t.Helper()
				require.Len(t, desc.Destinations, 1)
				s3Dest := desc.Destinations[0].ExtendedS3DestinationDescription
				require.NotNil(t, s3Dest)
				require.NotNil(t, s3Dest.S3BackupDescription)
				require.Equal(
					t,
					"arn:aws:s3:::ext-updated-backup",
					aws.ToString(s3Dest.S3BackupDescription.BucketARN),
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			streamName := "roundtrip-" + tc.name

			require.NoError(t, tc.create(client, streamName))

			pre, err := client.DescribeDeliveryStream(
				t.Context(),
				&firehosesdk.DescribeDeliveryStreamInput{
					DeliveryStreamName: aws.String(streamName),
				},
			)
			require.NoError(t, err)
			require.Len(t, pre.DeliveryStreamDescription.Destinations, 1)
			versionID := aws.ToString(pre.DeliveryStreamDescription.VersionId)
			destinationID := aws.ToString(
				pre.DeliveryStreamDescription.Destinations[0].DestinationId,
			)

			require.NoError(t, tc.update(client, streamName, versionID, destinationID))

			post, err := client.DescribeDeliveryStream(
				t.Context(),
				&firehosesdk.DescribeDeliveryStreamInput{
					DeliveryStreamName: aws.String(streamName),
				},
			)
			require.NoError(t, err)

			tc.verify(t, post.DeliveryStreamDescription)
		})
	}
}

// TestRedshiftDestination_CloudWatchLoggingAndSecretsManager_SDKRoundTrip proves that
// RedshiftDestinationConfiguration's CloudWatchLoggingOptions and
// SecretsManagerConfiguration fields (real, optional SDK fields --
// aws-sdk-go-v2/service/firehose/types@v1.46.4 types.go) round-trip through
// CreateDeliveryStream -> DescribeDeliveryStream. Both were previously absent from
// gopherstack's Redshift destination entirely.
func TestRedshiftDestination_CloudWatchLoggingAndSecretsManager_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	const streamName = "redshift-cwl-secrets"

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String(streamName),
		RedshiftDestinationConfiguration: &types.RedshiftDestinationConfiguration{
			ClusterJDBCURL: aws.String("jdbc:redshift://cluster:5439/db"),
			RoleARN:        aws.String("arn:aws:iam::000000000000:role/r"),
			CopyCommand: &types.CopyCommand{
				DataTableName: aws.String("t"),
			},
			S3Configuration: &types.S3DestinationConfiguration{
				BucketARN: aws.String("arn:aws:s3:::rs-bucket"),
				RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
			},
			CloudWatchLoggingOptions: &types.CloudWatchLoggingOptions{
				Enabled:      aws.Bool(true),
				LogGroupName: aws.String("/aws/firehose/redshift"),
			},
			SecretsManagerConfiguration: &types.SecretsManagerConfiguration{
				Enabled: aws.Bool(true),
				SecretARN: aws.String(
					"arn:aws:secretsmanager:us-east-1:000000000000:secret:rs-creds",
				),
				RoleARN: aws.String("arn:aws:iam::000000000000:role/r"),
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDeliveryStream(
		t.Context(),
		&firehosesdk.DescribeDeliveryStreamInput{
			DeliveryStreamName: aws.String(streamName),
		},
	)
	require.NoError(t, err)
	require.Len(t, desc.DeliveryStreamDescription.Destinations, 1)

	rsDest := desc.DeliveryStreamDescription.Destinations[0].RedshiftDestinationDescription
	require.NotNil(t, rsDest)

	require.NotNil(t, rsDest.CloudWatchLoggingOptions)
	require.True(t, aws.ToBool(rsDest.CloudWatchLoggingOptions.Enabled))
	require.Equal(
		t,
		"/aws/firehose/redshift",
		aws.ToString(rsDest.CloudWatchLoggingOptions.LogGroupName),
	)

	require.NotNil(t, rsDest.SecretsManagerConfiguration)
	require.True(t, aws.ToBool(rsDest.SecretsManagerConfiguration.Enabled))
	require.Equal(t,
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:rs-creds",
		aws.ToString(rsDest.SecretsManagerConfiguration.SecretARN))
}

// TestExtendedS3_OrcSerDePaddingTolerance_SDKRoundTrip proves that OrcSerDe's
// PaddingTolerance field (a real, optional field on
// aws-sdk-go-v2/service/firehose/types.OrcSerDe) round-trips through
// CreateDeliveryStream -> DescribeDeliveryStream. It was previously absent from
// gopherstack's OrcSerDe entirely.
func TestExtendedS3_OrcSerDePaddingTolerance_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	const streamName = "ext-s3-orc-padding"

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String(streamName),
		ExtendedS3DestinationConfiguration: &types.ExtendedS3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::orc-bucket"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
			DataFormatConversionConfiguration: &types.DataFormatConversionConfiguration{
				Enabled: aws.Bool(true),
				SchemaConfiguration: &types.SchemaConfiguration{
					DatabaseName: aws.String("db"),
					TableName:    aws.String("tbl"),
				},
				InputFormatConfiguration: &types.InputFormatConfiguration{
					Deserializer: &types.Deserializer{
						OpenXJsonSerDe: &types.OpenXJsonSerDe{},
					},
				},
				OutputFormatConfiguration: &types.OutputFormatConfiguration{
					Serializer: &types.Serializer{
						OrcSerDe: &types.OrcSerDe{
							PaddingTolerance: aws.Float64(0.25),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDeliveryStream(
		t.Context(),
		&firehosesdk.DescribeDeliveryStreamInput{
			DeliveryStreamName: aws.String(streamName),
		},
	)
	require.NoError(t, err)
	require.Len(t, desc.DeliveryStreamDescription.Destinations, 1)

	s3Dest := desc.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription
	require.NotNil(t, s3Dest)
	require.NotNil(t, s3Dest.DataFormatConversionConfiguration)
	require.NotNil(t, s3Dest.DataFormatConversionConfiguration.OutputFormatConfiguration)
	orc := s3Dest.DataFormatConversionConfiguration.OutputFormatConfiguration.Serializer.OrcSerDe
	require.NotNil(t, orc)
	require.InDelta(t, 0.25, aws.ToFloat64(orc.PaddingTolerance), 0.0001)
}

// TestListDeliveryStreams_DeliveryStreamTypeFilter_SDKRoundTrip proves that all 4 real
// DeliveryStreamType enum values (aws-sdk-go-v2/service/firehose/types/
// enums.go@v1.46.4: DirectPut, KinesisStreamAsSource, MSKAsSource, DatabaseAsSource) are
// accepted as a ListDeliveryStreams filter. MSKAsSource/DatabaseAsSource previously
// errored with ErrValidation instead of returning a (possibly empty) filtered list.
func TestListDeliveryStreams_DeliveryStreamTypeFilter_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("direct-put-stream"),
	})
	require.NoError(t, err)

	tests := []struct {
		streamType types.DeliveryStreamType
		name       string
	}{
		{name: "direct put", streamType: types.DeliveryStreamTypeDirectPut},
		{name: "kinesis stream as source", streamType: types.DeliveryStreamTypeKinesisStreamAsSource},
		{name: "msk as source", streamType: types.DeliveryStreamTypeMSKAsSource},
		{name: "database as source", streamType: types.DeliveryStreamTypeDatabaseAsSource},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, listErr := client.ListDeliveryStreams(t.Context(), &firehosesdk.ListDeliveryStreamsInput{
				DeliveryStreamType: tc.streamType,
			})
			require.NoError(t, listErr)
		})
	}
}
