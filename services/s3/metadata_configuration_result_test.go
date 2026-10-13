package s3_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketMetadataConfiguration_ComputedResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		inventory      *types.InventoryTableConfiguration
		update         *types.InventoryTableConfigurationUpdates
		name           string
		wantInvState   types.InventoryConfigurationState
		wantInvTable   bool
		wantInvPresent bool
	}{
		{name: "journal only"},
		{
			name: "inventory enabled",
			inventory: &types.InventoryTableConfiguration{
				ConfigurationState: types.InventoryConfigurationStateEnabled,
			},
			wantInvPresent: true,
			wantInvState:   types.InventoryConfigurationStateEnabled,
			wantInvTable:   true,
		},
		{
			name: "inventory disabled by update",
			inventory: &types.InventoryTableConfiguration{
				ConfigurationState: types.InventoryConfigurationStateEnabled,
			},
			update: &types.InventoryTableConfigurationUpdates{
				ConfigurationState: types.InventoryConfigurationStateDisabled,
			},
			wantInvPresent: true,
			wantInvState:   types.InventoryConfigurationStateDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()
			bucket := "md-result"

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			_, err = client.CreateBucketMetadataConfiguration(ctx, &sdk_s3.CreateBucketMetadataConfigurationInput{
				Bucket: aws.String(bucket),
				MetadataConfiguration: &types.MetadataConfiguration{
					InventoryTableConfiguration: tt.inventory,
					JournalTableConfiguration: &types.JournalTableConfiguration{
						RecordExpiration: &types.RecordExpiration{
							Expiration: types.ExpirationStateEnabled, Days: aws.Int32(30),
						},
					},
				},
			})
			require.NoError(t, err)

			if tt.update != nil {
				_, err = client.UpdateBucketMetadataInventoryTableConfiguration(ctx,
					&sdk_s3.UpdateBucketMetadataInventoryTableConfigurationInput{
						Bucket:                      aws.String(bucket),
						InventoryTableConfiguration: tt.update,
					})
				require.NoError(t, err)
			}

			out, err := client.GetBucketMetadataConfiguration(ctx,
				&sdk_s3.GetBucketMetadataConfigurationInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			res := out.GetBucketMetadataConfigurationResult.MetadataConfigurationResult
			require.NotNil(t, res.DestinationResult)
			assert.Equal(t, types.S3TablesBucketTypeAws, res.DestinationResult.TableBucketType)
			assert.Equal(t, "b_"+bucket, aws.ToString(res.DestinationResult.TableNamespace))
			assert.Contains(t, aws.ToString(res.DestinationResult.TableBucketArn), ":bucket/aws-s3")

			require.NotNil(t, res.JournalTableConfigurationResult)
			assert.Equal(t, "journal", aws.ToString(res.JournalTableConfigurationResult.TableName))
			assert.Equal(t, "ACTIVE", aws.ToString(res.JournalTableConfigurationResult.TableStatus))
			assert.EqualValues(t, 30, aws.ToInt32(res.JournalTableConfigurationResult.RecordExpiration.Days))

			if !tt.wantInvPresent {
				assert.Nil(t, res.InventoryTableConfigurationResult)

				return
			}

			require.NotNil(t, res.InventoryTableConfigurationResult)
			assert.Equal(t, tt.wantInvState, res.InventoryTableConfigurationResult.ConfigurationState)
			assert.Equal(t, tt.wantInvTable, res.InventoryTableConfigurationResult.TableName != nil)
		})
	}
}
