package s3_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ListBucketConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	const total = 101

	type listFn func(ctx context.Context, c *sdk_s3.Client, bucket string, token *string) ([]string, *string, bool, error)

	tests := []struct {
		put  func(ctx context.Context, c *sdk_s3.Client, bucket, id string) error
		list listFn
		name string
	}{
		{
			name: "analytics",
			put: func(ctx context.Context, c *sdk_s3.Client, bucket, id string) error {
				_, err := c.PutBucketAnalyticsConfiguration(ctx, &sdk_s3.PutBucketAnalyticsConfigurationInput{
					Bucket: aws.String(bucket),
					Id:     aws.String(id),
					AnalyticsConfiguration: &types.AnalyticsConfiguration{
						Id:                   aws.String(id),
						StorageClassAnalysis: &types.StorageClassAnalysis{},
					},
				})

				return err
			},
			list: func(ctx context.Context, c *sdk_s3.Client, bucket string, token *string) ([]string, *string, bool, error) {
				out, err := c.ListBucketAnalyticsConfigurations(ctx, &sdk_s3.ListBucketAnalyticsConfigurationsInput{
					Bucket: aws.String(bucket), ContinuationToken: token,
				})
				if err != nil {
					return nil, nil, false, err
				}
				ids := make([]string, 0, len(out.AnalyticsConfigurationList))
				for _, cfg := range out.AnalyticsConfigurationList {
					ids = append(ids, aws.ToString(cfg.Id))
				}

				return ids, out.NextContinuationToken, aws.ToBool(out.IsTruncated), nil
			},
		},
		{
			name: "metrics",
			put: func(ctx context.Context, c *sdk_s3.Client, bucket, id string) error {
				_, err := c.PutBucketMetricsConfiguration(ctx, &sdk_s3.PutBucketMetricsConfigurationInput{
					Bucket:               aws.String(bucket),
					Id:                   aws.String(id),
					MetricsConfiguration: &types.MetricsConfiguration{Id: aws.String(id)},
				})

				return err
			},
			list: func(ctx context.Context, c *sdk_s3.Client, bucket string, token *string) ([]string, *string, bool, error) {
				out, err := c.ListBucketMetricsConfigurations(ctx, &sdk_s3.ListBucketMetricsConfigurationsInput{
					Bucket: aws.String(bucket), ContinuationToken: token,
				})
				if err != nil {
					return nil, nil, false, err
				}
				ids := make([]string, 0, len(out.MetricsConfigurationList))
				for _, cfg := range out.MetricsConfigurationList {
					ids = append(ids, aws.ToString(cfg.Id))
				}

				return ids, out.NextContinuationToken, aws.ToBool(out.IsTruncated), nil
			},
		},
		{
			name: "inventory",
			put: func(ctx context.Context, c *sdk_s3.Client, bucket, id string) error {
				_, err := c.PutBucketInventoryConfiguration(ctx, &sdk_s3.PutBucketInventoryConfigurationInput{
					Bucket: aws.String(bucket),
					Id:     aws.String(id),
					InventoryConfiguration: &types.InventoryConfiguration{
						Id:                     aws.String(id),
						IsEnabled:              aws.Bool(true),
						IncludedObjectVersions: types.InventoryIncludedObjectVersionsAll,
						Schedule:               &types.InventorySchedule{Frequency: types.InventoryFrequencyDaily},
						Destination: &types.InventoryDestination{
							S3BucketDestination: &types.InventoryS3BucketDestination{
								Bucket: aws.String("arn:aws:s3:::dest"),
								Format: types.InventoryFormatCsv,
							},
						},
					},
				})

				return err
			},
			list: func(ctx context.Context, c *sdk_s3.Client, bucket string, token *string) ([]string, *string, bool, error) {
				out, err := c.ListBucketInventoryConfigurations(ctx, &sdk_s3.ListBucketInventoryConfigurationsInput{
					Bucket: aws.String(bucket), ContinuationToken: token,
				})
				if err != nil {
					return nil, nil, false, err
				}
				ids := make([]string, 0, len(out.InventoryConfigurationList))
				for _, cfg := range out.InventoryConfigurationList {
					ids = append(ids, aws.ToString(cfg.Id))
				}

				return ids, out.NextContinuationToken, aws.ToBool(out.IsTruncated), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()
			bucket := "cfg-page-" + tt.name

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			for i := range total {
				require.NoError(t, tt.put(ctx, client, bucket, fmt.Sprintf("id-%03d", i)))
			}

			first, next, truncated, err := tt.list(ctx, client, bucket, nil)
			require.NoError(t, err)
			assert.Len(t, first, 100)
			assert.True(t, truncated)
			require.NotNil(t, next)
			assert.Equal(t, "id-000", first[0])
			assert.Equal(t, "id-099", first[99])

			second, next2, truncated2, err := tt.list(ctx, client, bucket, next)
			require.NoError(t, err)
			assert.Equal(t, []string{"id-100"}, second)
			assert.False(t, truncated2)
			assert.Nil(t, next2)
		})
	}
}
