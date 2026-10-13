package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestList_OptionalRestoreStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		optional  []types.OptionalObjectAttributes
		restore   bool
		wantShown bool
	}{
		{
			name:      "requested and restored",
			optional:  []types.OptionalObjectAttributes{types.OptionalObjectAttributesRestoreStatus},
			restore:   true,
			wantShown: true,
		},
		{
			name:     "requested not restored",
			optional: []types.OptionalObjectAttributes{types.OptionalObjectAttributesRestoreStatus},
		},
		{name: "not requested", restore: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()
			bucket := "restore-list"

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)
			_, err = client.PutObject(ctx, &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("d"),
				StorageClass: types.StorageClassGlacier,
			})
			require.NoError(t, err)

			if tt.restore {
				_, err = client.RestoreObject(ctx, &sdk_s3.RestoreObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("k"),
					RestoreRequest: &types.RestoreRequest{Days: aws.Int32(3)},
				})
				require.NoError(t, err)
			}

			v2, err := client.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
				Bucket: aws.String(bucket), OptionalObjectAttributes: tt.optional,
			})
			require.NoError(t, err)
			require.Len(t, v2.Contents, 1)

			v1, err := client.ListObjects(ctx, &sdk_s3.ListObjectsInput{
				Bucket: aws.String(bucket), OptionalObjectAttributes: tt.optional,
			})
			require.NoError(t, err)
			require.Len(t, v1.Contents, 1)

			vers, err := client.ListObjectVersions(ctx, &sdk_s3.ListObjectVersionsInput{
				Bucket: aws.String(bucket), OptionalObjectAttributes: tt.optional,
			})
			require.NoError(t, err)
			require.Len(t, vers.Versions, 1)

			statuses := []*types.RestoreStatus{
				v2.Contents[0].RestoreStatus, v1.Contents[0].RestoreStatus, vers.Versions[0].RestoreStatus,
			}

			for _, rs := range statuses {
				if !tt.wantShown {
					assert.Nil(t, rs)

					continue
				}

				require.NotNil(t, rs)
				assert.False(t, aws.ToBool(rs.IsRestoreInProgress))
				assert.NotNil(t, rs.RestoreExpiryDate)
			}
		})
	}
}
