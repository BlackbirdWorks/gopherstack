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

type configTokenFixture struct {
	client *kinesisanalyticsv2sdk.Client
	name   string
	cwlID  string
	vpcID  string
}

func newConfigTokenFixture(t *testing.T) configTokenFixture {
	t.Helper()

	h := kinesisanalyticsv2.NewHandler(kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"))
	c := newTestKAV2SDKClient(t, h)
	name := "token-app"
	ver := createTestSQLApp(t, c, name)

	cwl, err := c.AddApplicationCloudWatchLoggingOption(
		t.Context(), &kinesisanalyticsv2sdk.AddApplicationCloudWatchLoggingOptionInput{
			ApplicationName:             aws.String(name),
			CurrentApplicationVersionId: aws.Int64(ver),
			CloudWatchLoggingOption: &kav2types.CloudWatchLoggingOption{
				LogStreamARN: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s"),
			},
		})
	require.NoError(t, err)

	vpc, err := c.AddApplicationVpcConfiguration(
		t.Context(), &kinesisanalyticsv2sdk.AddApplicationVpcConfigurationInput{
			ApplicationName:             aws.String(name),
			CurrentApplicationVersionId: cwl.ApplicationVersionId,
			VpcConfiguration: &kav2types.VpcConfiguration{
				SubnetIds: []string{"subnet-1"}, SecurityGroupIds: []string{"sg-1"},
			},
		})
	require.NoError(t, err)

	return configTokenFixture{
		client: c,
		name:   name,
		cwlID:  aws.ToString(cwl.CloudWatchLoggingOptionDescriptions[0].CloudWatchLoggingOptionId),
		vpcID:  aws.ToString(vpc.VpcConfigurationDescription.VpcConfigurationId),
	}
}

func (f configTokenFixture) token(t *testing.T) string {
	t.Helper()

	out, err := f.client.DescribeApplication(t.Context(), &kinesisanalyticsv2sdk.DescribeApplicationInput{
		ApplicationName: aws.String(f.name),
	})
	require.NoError(t, err)

	return aws.ToString(out.ApplicationDetail.ConditionalToken)
}

func TestSDK_ConditionalTokenGuardsConfigOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(f configTokenFixture, token string) error
		name string
	}{
		{
			name: "add cloudwatch logging option",
			call: func(f configTokenFixture, token string) error {
				_, err := f.client.AddApplicationCloudWatchLoggingOption(
					t.Context(), &kinesisanalyticsv2sdk.AddApplicationCloudWatchLoggingOptionInput{
						ApplicationName:  aws.String(f.name),
						ConditionalToken: aws.String(token),
						CloudWatchLoggingOption: &kav2types.CloudWatchLoggingOption{
							LogStreamARN: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s2"),
						},
					})

				return err
			},
		},
		{
			name: "delete cloudwatch logging option",
			call: func(f configTokenFixture, token string) error {
				_, err := f.client.DeleteApplicationCloudWatchLoggingOption(
					t.Context(), &kinesisanalyticsv2sdk.DeleteApplicationCloudWatchLoggingOptionInput{
						ApplicationName:           aws.String(f.name),
						ConditionalToken:          aws.String(token),
						CloudWatchLoggingOptionId: aws.String(f.cwlID),
					})

				return err
			},
		},
		{
			name: "add vpc configuration",
			call: func(f configTokenFixture, token string) error {
				_, err := f.client.AddApplicationVpcConfiguration(
					t.Context(), &kinesisanalyticsv2sdk.AddApplicationVpcConfigurationInput{
						ApplicationName:  aws.String(f.name),
						ConditionalToken: aws.String(token),
						VpcConfiguration: &kav2types.VpcConfiguration{
							SubnetIds: []string{"subnet-2"}, SecurityGroupIds: []string{"sg-2"},
						},
					})

				return err
			},
		},
		{
			name: "delete vpc configuration",
			call: func(f configTokenFixture, token string) error {
				_, err := f.client.DeleteApplicationVpcConfiguration(
					t.Context(), &kinesisanalyticsv2sdk.DeleteApplicationVpcConfigurationInput{
						ApplicationName:    aws.String(f.name),
						ConditionalToken:   aws.String(token),
						VpcConfigurationId: aws.String(f.vpcID),
					})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newConfigTokenFixture(t)
			before := describeTestAppVersion(t, f.client, f.name)

			var cme *kav2types.ConcurrentModificationException

			require.ErrorAs(t, tt.call(f, "stale-token"), &cme)
			assert.Equal(t, before, describeTestAppVersion(t, f.client, f.name))

			require.NoError(t, tt.call(f, f.token(t)))
			assert.Greater(t, describeTestAppVersion(t, f.client, f.name), before)
		})
	}
}

func TestSDK_DiscoverInputSchemaS3Configuration(t *testing.T) {
	t.Parallel()

	role := aws.String("arn:aws:iam::000000000000:role/kav2-role")

	tests := []struct {
		in      *kinesisanalyticsv2sdk.DiscoverInputSchemaInput
		name    string
		wantErr bool
	}{
		{
			name: "s3 object",
			in: &kinesisanalyticsv2sdk.DiscoverInputSchemaInput{
				ServiceExecutionRole: role,
				S3Configuration: &kav2types.S3Configuration{
					BucketARN: aws.String("arn:aws:s3:::bucket"), FileKey: aws.String("data.json"),
				},
			},
		},
		{
			name: "stream arn",
			in: &kinesisanalyticsv2sdk.DiscoverInputSchemaInput{
				ServiceExecutionRole: role,
				ResourceARN:          aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/s"),
			},
		},
		{
			name:    "no source",
			in:      &kinesisanalyticsv2sdk.DiscoverInputSchemaInput{ServiceExecutionRole: role},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := kinesisanalyticsv2.NewHandler(kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"))
			out, err := newTestKAV2SDKClient(t, h).DiscoverInputSchema(t.Context(), tt.in)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.InputSchema.RecordColumns)
		})
	}
}
