package awsconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func newOpenItemsClient(t *testing.T) (*awsconfig.InMemoryBackend, *configservicesdk.Client) {
	t.Helper()

	b := awsconfig.NewInMemoryBackendWithMeta("000000000000", "us-east-1")

	return b, newTestAWSConfigSDKClient(t, awsconfig.NewHandler(b))
}

func putResources(t *testing.T, client *configservicesdk.Client, resourceType string, ids ...string) {
	t.Helper()

	for _, id := range ids {
		_, err := client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
			ResourceType:    aws.String(resourceType),
			ResourceId:      aws.String(id),
			Configuration:   aws.String(`{}`),
			SchemaVersionId: aws.String("1.0"),
		})
		require.NoError(t, err)
	}
}

func TestRealClient_DiscoveredResourceCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		types     []string
		wantCount []types.ResourceCount
		limit     int32
		wantTotal int64
	}{
		{
			name: "all_types", wantTotal: 3,
			wantCount: []types.ResourceCount{
				{ResourceType: "AWS::EC2::Instance", Count: 2},
				{ResourceType: "AWS::S3::Bucket", Count: 1},
			},
		},
		{
			name: "type_filter", types: []string{"AWS::S3::Bucket"}, wantTotal: 1,
			wantCount: []types.ResourceCount{{ResourceType: "AWS::S3::Bucket", Count: 1}},
		},
		{
			name: "limit_pages", limit: 1, wantTotal: 3,
			wantCount: []types.ResourceCount{{ResourceType: "AWS::EC2::Instance", Count: 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newOpenItemsClient(t)
			putResources(t, client, "AWS::EC2::Instance", "i-1", "i-2")
			putResources(t, client, "AWS::S3::Bucket", "b-1")

			out, err := client.GetDiscoveredResourceCounts(
				t.Context(),
				&configservicesdk.GetDiscoveredResourceCountsInput{
					ResourceTypes: tt.types, Limit: tt.limit,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTotal, out.TotalDiscoveredResources)
			assert.Equal(t, tt.wantCount, out.ResourceCounts)

			if tt.limit > 0 {
				require.NotNil(t, out.NextToken)

				next, nerr := client.GetDiscoveredResourceCounts(
					t.Context(), &configservicesdk.GetDiscoveredResourceCountsInput{Limit: 1, NextToken: out.NextToken},
				)
				require.NoError(t, nerr)
				assert.Equal(t, []types.ResourceCount{{ResourceType: "AWS::S3::Bucket", Count: 1}}, next.ResourceCounts)
			}
		})
	}
}

func TestRealClient_AggregateDiscoveredResourceCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters   *types.ResourceCountFilters
		name      string
		groupBy   types.ResourceCountGroupKey
		want      []types.GroupedResourceCount
		wantTotal int64
		wantErr   bool
	}{
		{
			name: "by_type", groupBy: types.ResourceCountGroupKeyResourceType, wantTotal: 3,
			want: []types.GroupedResourceCount{
				{GroupName: aws.String("AWS::EC2::Instance"), ResourceCount: 2},
				{GroupName: aws.String("AWS::S3::Bucket"), ResourceCount: 1},
			},
		},
		{
			name: "by_account", groupBy: types.ResourceCountGroupKeyAccountId, wantTotal: 3,
			want: []types.GroupedResourceCount{{GroupName: aws.String("000000000000"), ResourceCount: 3}},
		},
		{
			name: "by_region", groupBy: types.ResourceCountGroupKeyAwsRegion, wantTotal: 3,
			want: []types.GroupedResourceCount{{GroupName: aws.String("us-east-1"), ResourceCount: 3}},
		},
		{
			name: "type_filter", groupBy: types.ResourceCountGroupKeyResourceType, wantTotal: 1,
			filters: &types.ResourceCountFilters{ResourceType: "AWS::S3::Bucket"},
			want:    []types.GroupedResourceCount{{GroupName: aws.String("AWS::S3::Bucket"), ResourceCount: 1}},
		},
		{
			name: "other_region", groupBy: types.ResourceCountGroupKeyResourceType, wantTotal: 0,
			filters: &types.ResourceCountFilters{Region: aws.String("eu-west-1")},
			want:    nil,
		},
		{name: "bad_group_key", groupBy: types.ResourceCountGroupKey("BOGUS"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newOpenItemsClient(t)
			require.NoError(t, b.PutConfigurationAggregator("agg", nil, nil, nil))
			putResources(t, client, "AWS::EC2::Instance", "i-1", "i-2")
			putResources(t, client, "AWS::S3::Bucket", "b-1")

			out, err := client.GetAggregateDiscoveredResourceCounts(
				t.Context(), &configservicesdk.GetAggregateDiscoveredResourceCountsInput{
					ConfigurationAggregatorName: aws.String("agg"), GroupByKey: tt.groupBy, Filters: tt.filters,
				},
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTotal, out.TotalDiscoveredResources)
			assert.Equal(t, tt.want, out.GroupedResourceCounts)
			assert.Equal(t, string(tt.groupBy), aws.ToString(out.GroupByKey))
		})
	}
}

func TestRealClient_PutValidationExceptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *configservicesdk.Client) error
		want any
		name string
	}{
		{
			name: "recording_group_all_supported_with_types",
			want: new(*types.InvalidRecordingGroupException),
			run: func(t *testing.T, client *configservicesdk.Client) error {
				t.Helper()

				_, err := client.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
					ConfigurationRecorder: &types.ConfigurationRecorder{
						Name:    aws.String("rec"),
						RoleARN: aws.String("arn:aws:iam::000000000000:role/r"),
						RecordingGroup: &types.RecordingGroup{
							AllSupported:  true,
							ResourceTypes: []types.ResourceType{types.ResourceTypeInstance},
						},
					},
				})

				return err
			},
		},
		{
			name: "sns_topic_not_an_arn",
			want: new(*types.InvalidSNSTopicARNException),
			run: func(t *testing.T, client *configservicesdk.Client) error {
				t.Helper()

				_, err := client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
					DeliveryChannel: &types.DeliveryChannel{
						Name: aws.String("ch"), S3BucketName: aws.String("b"), SnsTopicARN: aws.String("not-an-arn"),
					},
				})

				return err
			},
		},
		{
			name: "kms_key_not_a_kms_arn",
			want: new(*types.InvalidS3KmsKeyArnException),
			run: func(t *testing.T, client *configservicesdk.Client) error {
				t.Helper()

				_, err := client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
					DeliveryChannel: &types.DeliveryChannel{
						Name: aws.String("ch"), S3BucketName: aws.String("b"),
						S3KmsKeyArn: aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
					},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newOpenItemsClient(t)
			err := tt.run(t, client)
			require.Error(t, err)

			switch want := tt.want.(type) {
			case **types.InvalidRecordingGroupException:
				require.ErrorAs(t, err, want)
			case **types.InvalidSNSTopicARNException:
				require.ErrorAs(t, err, want)
			case **types.InvalidS3KmsKeyArnException:
				require.ErrorAs(t, err, want)
			}
		})
	}
}

func TestRealClient_DeliveryChannelKmsKeyRoundTrip(t *testing.T) {
	t.Parallel()

	_, client := newOpenItemsClient(t)
	kms := "arn:aws:kms:us-east-1:000000000000:key/abc"
	sns := "arn:aws:sns:us-east-1:000000000000:topic"

	_, err := client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
		DeliveryChannel: &types.DeliveryChannel{
			Name: aws.String(
				"ch",
			), S3BucketName: aws.String("b"), S3KmsKeyArn: aws.String(kms), SnsTopicARN: aws.String(sns),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDeliveryChannels(t.Context(), &configservicesdk.DescribeDeliveryChannelsInput{})
	require.NoError(t, err)
	require.Len(t, out.DeliveryChannels, 1)
	assert.Equal(t, kms, aws.ToString(out.DeliveryChannels[0].S3KmsKeyArn))
	assert.Equal(t, sns, aws.ToString(out.DeliveryChannels[0].SnsTopicARN))
}
